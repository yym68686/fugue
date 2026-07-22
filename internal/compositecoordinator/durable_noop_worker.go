package compositecoordinator

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

const (
	DurableNoopRunAPIVersion = "release-domain-durable-noop.fugue.dev/v1"
	DurableNoopRunKind       = "CompositeDurableNoopRun"
	DurableNoopRunPolicy     = "serial-cas-no-adapter-v1"
)

var ErrDurableNoopRun = errors.New("invalid durable composite no-op run")

// DurableNoopTransactionStore is the smallest durable boundary used by the
// no-op worker. The production store satisfies this interface without being
// imported or wired into this package.
type DurableNoopTransactionStore interface {
	GetCompositeReleaseTransaction(id string) (Record, error)
	AdvanceCompositeReleaseTransaction(
		id string,
		expectedRevision int64,
		expectedPlanDigest string,
		expectedFencingEpoch string,
		transition Transition,
	) (Record, error)
}

type DurableNoopRunEvent struct {
	Sequence       string `json:"sequence"`
	Action         string `json:"action"`
	StepID         string `json:"stepId"`
	EvidenceDigest string `json:"evidenceDigest"`
}

// DurableNoopRunResult seals the store-backed coordinator transitions. The
// worker has no adapter capability and ProductionWrite must remain false.
type DurableNoopRunResult struct {
	APIVersion          string                `json:"apiVersion"`
	Kind                string                `json:"kind"`
	Policy              string                `json:"policy"`
	RecordID            string                `json:"recordId"`
	InitialRecordDigest string                `json:"initialRecordDigest"`
	FinalRecordDigest   string                `json:"finalRecordDigest"`
	InitialRevision     string                `json:"initialRevision"`
	FinalRevision       string                `json:"finalRevision"`
	PlanDigest          string                `json:"planDigest"`
	AuthorizationDigest string                `json:"authorizationDigest"`
	FailureStepID       string                `json:"failureStepId"`
	Events              []DurableNoopRunEvent `json:"events"`
	FinalState          State                 `json:"finalState"`
	ProductionWrite     bool                  `json:"productionWrite"`
	Digest              string                `json:"digest"`
}

// RunDurableNoop advances one exact prepared record through serial no-op
// apply and observation transitions. A non-empty failureStepID induces one
// controlled observation failure and makes the coordinator reverse the
// current and completed steps. It never receives or invokes an adapter.
func RunDurableNoop(
	store DurableNoopTransactionStore,
	recordID string,
	authorization NoopAuthorization,
	failureStepID string,
) (Record, DurableNoopRunResult, error) {
	if store == nil || recordID == "" || authorization.RecordID() != recordID {
		return Record{}, DurableNoopRunResult{}, ErrDurableNoopRun
	}
	initial, err := store.GetCompositeReleaseTransaction(recordID)
	if err != nil {
		return Record{}, DurableNoopRunResult{}, fmt.Errorf("%w: get prepared record: %v", ErrDurableNoopRun, err)
	}
	if authorization.Verify(initial) != nil {
		return Record{}, DurableNoopRunResult{}, ErrDurableNoopRun
	}
	failureIndex := -1
	if failureStepID != "" {
		for index := range initial.Plan.Steps {
			if initial.Plan.Steps[index].ID == failureStepID {
				failureIndex = index
				break
			}
		}
		if failureIndex < 0 {
			return Record{}, DurableNoopRunResult{}, ErrDurableNoopRun
		}
	}

	current := initial
	events := make([]DurableNoopRunEvent, 0, len(initial.Plan.Steps)*2)
	appendEvent := func(action, stepID string) string {
		digest := durableNoopEvidenceDigest(initial, authorization, action, stepID)
		events = append(events, DurableNoopRunEvent{
			Sequence: strconv.Itoa(len(events) + 1), Action: action, StepID: stepID, EvidenceDigest: digest,
		})
		return digest
	}
	advance := func(transition Transition) error {
		previous := current
		next, advanceErr := store.AdvanceCompositeReleaseTransaction(
			previous.ID, previous.Revision, previous.Plan.Digest, previous.Plan.FencingEpoch, transition,
		)
		if advanceErr != nil {
			return fmt.Errorf("%w: advance %s: %v", ErrDurableNoopRun, transition.Kind, advanceErr)
		}
		expected, expectedErr := ApplyTransition(previous, transition, next.UpdatedAt)
		if expectedErr != nil || VerifyRecord(next) != nil || next.Digest != expected.Digest ||
			next.ID != previous.ID || next.Plan.Digest != previous.Plan.Digest ||
			next.Plan.FencingEpoch != previous.Plan.FencingEpoch || next.Revision != previous.Revision+1 {
			return fmt.Errorf("%w: store returned an unbound transition result", ErrDurableNoopRun)
		}
		current = next
		return nil
	}

	if err := advance(Transition{Kind: TransitionBeginApply}); err != nil {
		return Record{}, DurableNoopRunResult{}, err
	}
	for index, step := range initial.Plan.Steps {
		applyDigest := appendEvent("apply-noop", step.ID)
		if err := advance(Transition{Kind: TransitionBeginObservation, EvidenceDigest: applyDigest}); err != nil {
			return Record{}, DurableNoopRunResult{}, err
		}
		observationDigest := appendEvent("observe-noop", step.ID)
		if index != failureIndex {
			if err := advance(Transition{Kind: TransitionCompleteObservation, EvidenceDigest: observationDigest}); err != nil {
				return Record{}, DurableNoopRunResult{}, err
			}
			continue
		}

		appendEvent("induce-failure", step.ID)
		if err := advance(Transition{
			Kind: TransitionBeginRevert, Reason: "controlled-durable-noop-failure:" + step.ID,
		}); err != nil {
			return Record{}, DurableNoopRunResult{}, err
		}
		for current.State == StateReverting {
			reverseStepID := current.Steps[current.CurrentStep].ID
			reverseDigest := appendEvent("reverse-noop", reverseStepID)
			if err := advance(Transition{Kind: TransitionCompleteRevert, EvidenceDigest: reverseDigest}); err != nil {
				return Record{}, DurableNoopRunResult{}, err
			}
		}
		break
	}

	result := DurableNoopRunResult{
		APIVersion: DurableNoopRunAPIVersion, Kind: DurableNoopRunKind, Policy: DurableNoopRunPolicy,
		RecordID: initial.ID, InitialRecordDigest: initial.Digest, FinalRecordDigest: current.Digest,
		InitialRevision: strconv.FormatInt(initial.Revision, 10), FinalRevision: strconv.FormatInt(current.Revision, 10),
		PlanDigest: initial.Plan.Digest, AuthorizationDigest: authorization.EnvelopeDigest(),
		FailureStepID: failureStepID, Events: events, FinalState: current.State, ProductionWrite: false,
	}
	result.Digest = DigestDurableNoopRunResult(result)
	if err := VerifyDurableNoopRunResult(initial, authorization, current, result); err != nil {
		return Record{}, DurableNoopRunResult{}, err
	}
	return current, result, nil
}

func VerifyDurableNoopRunResult(
	initial Record,
	authorization NoopAuthorization,
	final Record,
	result DurableNoopRunResult,
) error {
	if VerifyRecord(initial) != nil || initial.State != StatePrepared || authorization.Verify(initial) != nil ||
		VerifyRecord(final) != nil || result.APIVersion != DurableNoopRunAPIVersion || result.Kind != DurableNoopRunKind ||
		result.Policy != DurableNoopRunPolicy || result.RecordID != initial.ID ||
		result.InitialRecordDigest != initial.Digest || result.FinalRecordDigest != final.Digest ||
		result.InitialRevision != strconv.FormatInt(initial.Revision, 10) ||
		result.FinalRevision != strconv.FormatInt(final.Revision, 10) || result.PlanDigest != initial.Plan.Digest ||
		result.AuthorizationDigest != authorization.EnvelopeDigest() || result.FinalState != final.State ||
		result.ProductionWrite || result.Digest != DigestDurableNoopRunResult(result) {
		return ErrDurableNoopRun
	}

	failureIndex := -1
	if result.FailureStepID != "" {
		for index := range initial.Plan.Steps {
			if initial.Plan.Steps[index].ID == result.FailureStepID {
				failureIndex = index
				break
			}
		}
		if failureIndex < 0 || final.State != StateReverted || final.RollbackStartStep != failureIndex ||
			final.FailureReason != "controlled-durable-noop-failure:"+result.FailureStepID {
			return ErrDurableNoopRun
		}
	} else if final.State != StateCommitted || final.RollbackStartStep != -1 || final.FailureReason != "" {
		return ErrDurableNoopRun
	}

	wantedActions := make([]string, 0, len(result.Events))
	wantedSteps := make([]string, 0, len(result.Events))
	lastForwardIndex := len(initial.Plan.Steps) - 1
	if failureIndex >= 0 {
		lastForwardIndex = failureIndex
	}
	for index := 0; index <= lastForwardIndex; index++ {
		stepID := initial.Plan.Steps[index].ID
		wantedActions = append(wantedActions, "apply-noop", "observe-noop")
		wantedSteps = append(wantedSteps, stepID, stepID)
	}
	if failureIndex >= 0 {
		wantedActions = append(wantedActions, "induce-failure")
		wantedSteps = append(wantedSteps, result.FailureStepID)
		for index := failureIndex; index >= 0; index-- {
			wantedActions = append(wantedActions, "reverse-noop")
			wantedSteps = append(wantedSteps, initial.Plan.Steps[index].ID)
		}
	}
	expectedTransitions := len(result.Events)
	if failureIndex < 0 {
		expectedTransitions++
	}
	if len(result.Events) != len(wantedActions) || final.Revision != initial.Revision+int64(expectedTransitions) {
		return ErrDurableNoopRun
	}
	for index, event := range result.Events {
		if event.Sequence != strconv.Itoa(index+1) || event.Action != wantedActions[index] || event.StepID != wantedSteps[index] ||
			event.EvidenceDigest != durableNoopEvidenceDigest(initial, authorization, event.Action, event.StepID) {
			return ErrDurableNoopRun
		}
	}
	for index := range initial.Plan.Steps {
		step := final.Steps[index]
		if index <= lastForwardIndex {
			if step.ApplyEvidenceDigest != result.Events[index*2].EvidenceDigest {
				return ErrDurableNoopRun
			}
			if index == failureIndex {
				if step.ObservationEvidenceDigest != "" || step.State != StepReverted {
					return ErrDurableNoopRun
				}
			} else if step.ObservationEvidenceDigest != result.Events[index*2+1].EvidenceDigest {
				return ErrDurableNoopRun
			}
		}
		if failureIndex >= 0 && index <= failureIndex {
			reverseEvent := result.Events[len(result.Events)-1-index]
			if step.State != StepReverted || step.ReverseEvidenceDigest != reverseEvent.EvidenceDigest {
				return ErrDurableNoopRun
			}
		} else if failureIndex < 0 && step.State != StepCompleted {
			return ErrDurableNoopRun
		} else if index > lastForwardIndex && step.State != StepPending {
			return ErrDurableNoopRun
		}
	}
	return nil
}

func DigestDurableNoopRunResult(result DurableNoopRunResult) string {
	result.Events = append([]DurableNoopRunEvent(nil), result.Events...)
	result.Digest = ""
	encoded, err := json.Marshal(result)
	if err != nil {
		panic(fmt.Sprintf("marshal durable no-op run result: %v", err))
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func durableNoopEvidenceDigest(record Record, authorization NoopAuthorization, action, stepID string) string {
	digest := sha256.Sum256([]byte(
		"fugue-durable-noop-evidence-v1\x00" + record.ID + "\x00" + record.Digest + "\x00" +
			record.Plan.Digest + "\x00" + authorization.EnvelopeDigest() + "\x00" + action + "\x00" + stepID,
	))
	return "sha256:" + hex.EncodeToString(digest[:])
}
