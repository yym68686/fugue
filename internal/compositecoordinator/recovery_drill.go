package compositecoordinator

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

const (
	RecoveryDrillAPIVersion = "release-domain-recovery-drill.fugue.dev/v1"
	RecoveryDrillKind       = "CompositeNoopRecoveryDrill"
	RecoveryDrillPolicy     = "two-domain-current-then-reverse-v1"
)

type RecoveryDrillEvent struct {
	Sequence       string `json:"sequence"`
	Action         string `json:"action"`
	StepID         string `json:"stepId"`
	EvidenceDigest string `json:"evidenceDigest"`
}

// RecoveryDrillResult is deterministic evidence from one in-memory no-op
// canary. ProductionWrite is required to remain false.
type RecoveryDrillResult struct {
	APIVersion          string               `json:"apiVersion"`
	Kind                string               `json:"kind"`
	Policy              string               `json:"policy"`
	RecordID            string               `json:"recordId"`
	InitialRecordDigest string               `json:"initialRecordDigest"`
	FinalRecordDigest   string               `json:"finalRecordDigest"`
	RecordRevision      string               `json:"recordRevision"`
	PlanDigest          string               `json:"planDigest"`
	AuthorizationDigest string               `json:"authorizationDigest"`
	FailureStepID       string               `json:"failureStepId"`
	Events              []RecoveryDrillEvent `json:"events"`
	FinalState          State                `json:"finalState"`
	ProductionWrite     bool                 `json:"productionWrite"`
	Digest              string               `json:"digest"`
}

// RunControlledNoopRecoveryDrill executes only the coordinator state machine.
// It completes the first step, induces failure while observing the second,
// and lets Fugue reverse the current and completed steps in strict order.
func RunControlledNoopRecoveryDrill(
	record Record,
	authorization NoopAuthorization,
	failureStepID string,
	now time.Time,
) (Record, RecoveryDrillResult, error) {
	if VerifyRecord(record) != nil || record.State != StatePrepared || authorization.Verify(record) != nil ||
		len(record.Plan.Steps) != 2 || failureStepID != record.Plan.Steps[1].ID || now.IsZero() || now.Before(record.UpdatedAt) {
		return Record{}, RecoveryDrillResult{}, ErrInvalidAuthorization
	}

	initial := record
	current := record
	events := make([]RecoveryDrillEvent, 0, 7)
	sequence := 0
	transitionTime := now.UTC()
	nextTime := func() time.Time {
		transitionTime = transitionTime.Add(time.Second)
		return transitionTime
	}
	appendEvent := func(action, stepID string) string {
		sequence++
		digest := recoveryDrillEvidenceDigest(initial, authorization, action, stepID)
		events = append(events, RecoveryDrillEvent{
			Sequence: strconv.Itoa(sequence), Action: action, StepID: stepID, EvidenceDigest: digest,
		})
		return digest
	}

	var err error
	current, err = ApplyTransition(current, Transition{Kind: TransitionBeginApply}, nextTime())
	if err != nil {
		return Record{}, RecoveryDrillResult{}, err
	}
	for stepIndex, step := range initial.Plan.Steps {
		applyDigest := appendEvent("apply-noop", step.ID)
		current, err = ApplyTransition(current, Transition{
			Kind: TransitionBeginObservation, EvidenceDigest: applyDigest,
		}, nextTime())
		if err != nil {
			return Record{}, RecoveryDrillResult{}, err
		}
		observationDigest := appendEvent("observe-noop", step.ID)
		if stepIndex == 0 {
			current, err = ApplyTransition(current, Transition{
				Kind: TransitionCompleteObservation, EvidenceDigest: observationDigest,
			}, nextTime())
			if err != nil {
				return Record{}, RecoveryDrillResult{}, err
			}
			continue
		}

		appendEvent("induce-failure", step.ID)
		current, err = ApplyTransition(current, Transition{
			Kind: TransitionBeginRevert, Reason: "controlled-noop-failure:" + step.ID,
		}, nextTime())
		if err != nil {
			return Record{}, RecoveryDrillResult{}, err
		}
		for current.State == StateReverting {
			reverseStepID := current.Steps[current.CurrentStep].ID
			reverseDigest := appendEvent("reverse-noop", reverseStepID)
			current, err = ApplyTransition(current, Transition{
				Kind: TransitionCompleteRevert, EvidenceDigest: reverseDigest,
			}, nextTime())
			if err != nil {
				return Record{}, RecoveryDrillResult{}, err
			}
		}
	}

	result := RecoveryDrillResult{
		APIVersion: RecoveryDrillAPIVersion, Kind: RecoveryDrillKind, Policy: RecoveryDrillPolicy,
		RecordID: initial.ID, InitialRecordDigest: initial.Digest, FinalRecordDigest: current.Digest,
		RecordRevision: strconv.FormatInt(initial.Revision, 10), PlanDigest: initial.Plan.Digest,
		AuthorizationDigest: authorization.EnvelopeDigest(), FailureStepID: failureStepID,
		Events: events, FinalState: current.State, ProductionWrite: false,
	}
	result.Digest = DigestRecoveryDrillResult(result)
	if err := VerifyRecoveryDrillResult(initial, authorization, current, result); err != nil {
		return Record{}, RecoveryDrillResult{}, err
	}
	return current, result, nil
}

func VerifyRecoveryDrillResult(initial Record, authorization NoopAuthorization, final Record, result RecoveryDrillResult) error {
	if VerifyRecord(initial) != nil || initial.State != StatePrepared || authorization.Verify(initial) != nil ||
		VerifyRecord(final) != nil || len(initial.Plan.Steps) != 2 || final.State != StateReverted ||
		result.APIVersion != RecoveryDrillAPIVersion || result.Kind != RecoveryDrillKind || result.Policy != RecoveryDrillPolicy ||
		result.RecordID != initial.ID || result.InitialRecordDigest != initial.Digest || result.FinalRecordDigest != final.Digest ||
		result.RecordRevision != strconv.FormatInt(initial.Revision, 10) || result.PlanDigest != initial.Plan.Digest ||
		result.AuthorizationDigest != authorization.EnvelopeDigest() || result.FailureStepID != initial.Plan.Steps[1].ID ||
		result.FinalState != StateReverted || result.ProductionWrite || len(result.Events) != 7 {
		return fmt.Errorf("invalid controlled no-op recovery drill result")
	}
	wantedActions := []string{
		"apply-noop", "observe-noop", "apply-noop", "observe-noop",
		"induce-failure", "reverse-noop", "reverse-noop",
	}
	wantedSteps := []string{
		initial.Plan.Steps[0].ID, initial.Plan.Steps[0].ID,
		initial.Plan.Steps[1].ID, initial.Plan.Steps[1].ID, initial.Plan.Steps[1].ID,
		initial.Plan.Steps[1].ID, initial.Plan.Steps[0].ID,
	}
	for index := range result.Events {
		event := result.Events[index]
		if event.Sequence != strconv.Itoa(index+1) || event.Action != wantedActions[index] || event.StepID != wantedSteps[index] ||
			event.EvidenceDigest != recoveryDrillEvidenceDigest(initial, authorization, event.Action, event.StepID) {
			return fmt.Errorf("invalid controlled no-op recovery drill event")
		}
	}
	if final.ID != initial.ID || final.Plan.Digest != initial.Plan.Digest || final.Revision != initial.Revision+7 ||
		final.RollbackStartStep != 1 || final.CurrentStep != -1 ||
		final.FailureReason != "controlled-noop-failure:"+initial.Plan.Steps[1].ID || len(final.Steps) != 2 {
		return fmt.Errorf("invalid controlled no-op recovery final record")
	}
	for index := range final.Steps {
		step := final.Steps[index]
		if step.State != StepReverted || step.ApplyEvidenceDigest != result.Events[index*2].EvidenceDigest ||
			step.ReverseEvidenceDigest != result.Events[6-index].EvidenceDigest {
			return fmt.Errorf("invalid controlled no-op recovery step evidence")
		}
	}
	if final.Steps[0].ObservationEvidenceDigest != result.Events[1].EvidenceDigest ||
		final.Steps[1].ObservationEvidenceDigest != "" || result.Digest != DigestRecoveryDrillResult(result) {
		return fmt.Errorf("invalid controlled no-op recovery evidence digest")
	}
	return nil
}

func DigestRecoveryDrillResult(result RecoveryDrillResult) string {
	result.Events = append([]RecoveryDrillEvent(nil), result.Events...)
	result.Digest = ""
	encoded, err := json.Marshal(result)
	if err != nil {
		panic(fmt.Sprintf("marshal recovery drill result: %v", err))
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func recoveryDrillEvidenceDigest(record Record, authorization NoopAuthorization, action, stepID string) string {
	digest := sha256.Sum256([]byte(
		"fugue-controlled-noop-recovery-evidence-v1\x00" + record.ID + "\x00" + record.Digest + "\x00" +
			record.Plan.Digest + "\x00" + authorization.EnvelopeDigest() + "\x00" + action + "\x00" + stepID,
	))
	return "sha256:" + hex.EncodeToString(digest[:])
}
