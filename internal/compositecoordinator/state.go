// Package compositecoordinator defines the dormant durable state carried by a
// future composite release worker. It validates and advances records but does
// not authorize a release or invoke an adapter.
package compositecoordinator

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"fugue/internal/releasedomain"
)

const (
	RecordAPIVersion = "release-domain.fugue.dev/v2"
	RecordKind       = "CompositeCoordinatorRecord"
	RecordPolicy     = "durable-serial-saga-v1"
)

type State string

const (
	StatePrepared  State = "prepared"
	StateApplying  State = "applying"
	StateObserving State = "observing"
	StateCommitted State = "committed"
	StateReverting State = "reverting"
	StateReverted  State = "reverted"
	StateFrozen    State = "frozen"
)

type StepState string

const (
	StepPending   StepState = "pending"
	StepApplying  StepState = "applying"
	StepObserving StepState = "observing"
	StepCompleted StepState = "completed"
	StepReverting StepState = "reverting"
	StepReverted  StepState = "reverted"
	StepFrozen    StepState = "frozen"
)

type TransitionKind string

const (
	TransitionBeginApply          TransitionKind = "begin-apply"
	TransitionBeginObservation    TransitionKind = "begin-observation"
	TransitionCompleteObservation TransitionKind = "complete-observation"
	TransitionBeginRevert         TransitionKind = "begin-revert"
	TransitionCompleteRevert      TransitionKind = "complete-revert"
	TransitionFreeze              TransitionKind = "freeze"
)

var (
	ErrInvalidRecord     = errors.New("invalid composite coordinator record")
	ErrInvalidTransition = errors.New("invalid composite coordinator transition")
	recordIDPattern      = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)
	digestPattern        = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

// Record embeds the complete verified plan so every forward/reverse digest,
// observation policy and rollback budget is durable before any future write.
type Record struct {
	APIVersion        string                             `json:"apiVersion"`
	Kind              string                             `json:"kind"`
	Policy            string                             `json:"policy"`
	ID                string                             `json:"id"`
	Plan              releasedomain.CompositeReleasePlan `json:"plan"`
	State             State                              `json:"state"`
	CurrentStep       int                                `json:"currentStep"`
	RollbackStartStep int                                `json:"rollbackStartStep"`
	Steps             []StepProgress                     `json:"steps"`
	Revision          int64                              `json:"revision"`
	FailureReason     string                             `json:"failureReason"`
	FreezeReason      string                             `json:"freezeReason"`
	CreatedAt         time.Time                          `json:"createdAt"`
	UpdatedAt         time.Time                          `json:"updatedAt"`
	Digest            string                             `json:"digest"`
}

type StepProgress struct {
	ID                        string     `json:"id"`
	State                     StepState  `json:"state"`
	StartedAt                 *time.Time `json:"startedAt"`
	ApplyEvidenceDigest       string     `json:"applyEvidenceDigest"`
	ObservationStartedAt      *time.Time `json:"observationStartedAt"`
	ObservationEvidenceDigest string     `json:"observationEvidenceDigest"`
	CompletedAt               *time.Time `json:"completedAt"`
	ReverseEvidenceDigest     string     `json:"reverseEvidenceDigest"`
	RevertedAt                *time.Time `json:"revertedAt"`
}

type Transition struct {
	Kind           TransitionKind
	EvidenceDigest string
	Reason         string
}

func (record *Record) UnmarshalJSON(data []byte) error {
	if err := rejectDuplicateJSONFields(data); err != nil {
		return err
	}
	type recordAlias Record
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var decoded recordAlias
	if err := decoder.Decode(&decoded); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("composite coordinator record has trailing JSON")
	}
	*record = Record(decoded)
	return nil
}

func rejectDuplicateJSONFields(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	var consume func() error
	consume = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delimiter {
		case '{':
			seen := map[string]struct{}{}
			for decoder.More() {
				nameToken, err := decoder.Token()
				if err != nil {
					return err
				}
				name, ok := nameToken.(string)
				if !ok {
					return fmt.Errorf("composite coordinator JSON field name is invalid")
				}
				if _, duplicate := seen[name]; duplicate {
					return fmt.Errorf("composite coordinator JSON field %q is duplicated", name)
				}
				seen[name] = struct{}{}
				if err := consume(); err != nil {
					return err
				}
			}
		case '[':
			for decoder.More() {
				if err := consume(); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("composite coordinator JSON delimiter is invalid")
		}
		closing, err := decoder.Token()
		if err != nil {
			return err
		}
		if closing != json.Delim(map[json.Delim]rune{'{': '}', '[': ']'}[delimiter]) {
			return fmt.Errorf("composite coordinator JSON is not closed")
		}
		return nil
	}
	if err := consume(); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("composite coordinator JSON has trailing content")
	}
	return nil
}

func NewRecord(id string, plan releasedomain.CompositeReleasePlan, now time.Time) (Record, error) {
	if !recordIDPattern.MatchString(id) || releasedomain.VerifyCompositeReleasePlan(plan) != nil || now.IsZero() {
		return Record{}, ErrInvalidRecord
	}
	now = now.UTC()
	steps := make([]StepProgress, len(plan.Steps))
	for index, step := range plan.Steps {
		steps[index] = StepProgress{ID: step.ID, State: StepPending}
	}
	record := Record{
		APIVersion: RecordAPIVersion, Kind: RecordKind, Policy: RecordPolicy,
		ID: id, Plan: plan, State: StatePrepared, CurrentStep: 0,
		RollbackStartStep: -1, Steps: steps, Revision: 1,
		CreatedAt: now, UpdatedAt: now,
	}
	record.Digest = recordDigest(record)
	if err := VerifyRecord(record); err != nil {
		return Record{}, err
	}
	return record, nil
}

func VerifyRecord(record Record) error {
	if record.APIVersion != RecordAPIVersion || record.Kind != RecordKind || record.Policy != RecordPolicy ||
		!recordIDPattern.MatchString(record.ID) || releasedomain.VerifyCompositeReleasePlan(record.Plan) != nil ||
		record.Revision < 1 || record.CreatedAt.IsZero() || record.UpdatedAt.Before(record.CreatedAt) ||
		len(record.Steps) != len(record.Plan.Steps) {
		return ErrInvalidRecord
	}
	for index := range record.Steps {
		if record.Steps[index].ID != record.Plan.Steps[index].ID || !validStepProgress(record.Steps[index]) {
			return ErrInvalidRecord
		}
	}
	if err := verifyStateShape(record); err != nil {
		return err
	}
	if record.Digest != recordDigest(record) {
		return ErrInvalidRecord
	}
	return nil
}

func ApplyTransition(record Record, transition Transition, now time.Time) (Record, error) {
	if VerifyRecord(record) != nil || now.IsZero() || now.Before(record.UpdatedAt) {
		return Record{}, ErrInvalidRecord
	}
	if (transition.EvidenceDigest != "" && !digestPattern.MatchString(transition.EvidenceDigest)) ||
		strings.TrimSpace(transition.Reason) != transition.Reason || len(transition.Reason) > 1024 {
		return Record{}, ErrInvalidTransition
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return Record{}, ErrInvalidRecord
	}
	var next Record
	if err := json.Unmarshal(encoded, &next); err != nil {
		return Record{}, ErrInvalidRecord
	}
	now = now.UTC()
	current := func() *StepProgress {
		if next.CurrentStep < 0 || next.CurrentStep >= len(next.Steps) {
			return nil
		}
		return &next.Steps[next.CurrentStep]
	}

	switch transition.Kind {
	case TransitionBeginApply:
		if next.State != StatePrepared || transition.EvidenceDigest != "" || transition.Reason != "" {
			return Record{}, ErrInvalidTransition
		}
		next.State = StateApplying
		current().State = StepApplying
		current().StartedAt = timePointer(now)
	case TransitionBeginObservation:
		if next.State != StateApplying || transition.EvidenceDigest == "" || transition.Reason != "" {
			return Record{}, ErrInvalidTransition
		}
		next.State = StateObserving
		current().State = StepObserving
		current().ApplyEvidenceDigest = transition.EvidenceDigest
		current().ObservationStartedAt = timePointer(now)
	case TransitionCompleteObservation:
		if next.State != StateObserving || transition.EvidenceDigest == "" || transition.Reason != "" {
			return Record{}, ErrInvalidTransition
		}
		current().State = StepCompleted
		current().ObservationEvidenceDigest = transition.EvidenceDigest
		current().CompletedAt = timePointer(now)
		if next.CurrentStep == len(next.Steps)-1 {
			next.State = StateCommitted
			next.CurrentStep = len(next.Steps)
		} else {
			next.CurrentStep++
			next.State = StateApplying
			current().State = StepApplying
			current().StartedAt = timePointer(now)
		}
	case TransitionBeginRevert:
		if (next.State != StateApplying && next.State != StateObserving) || transition.EvidenceDigest != "" || transition.Reason == "" {
			return Record{}, ErrInvalidTransition
		}
		next.State = StateReverting
		next.RollbackStartStep = next.CurrentStep
		next.FailureReason = transition.Reason
		current().State = StepReverting
	case TransitionCompleteRevert:
		if next.State != StateReverting || transition.EvidenceDigest == "" || transition.Reason != "" {
			return Record{}, ErrInvalidTransition
		}
		current().State = StepReverted
		current().ReverseEvidenceDigest = transition.EvidenceDigest
		current().RevertedAt = timePointer(now)
		if next.CurrentStep == 0 {
			next.CurrentStep = -1
			next.State = StateReverted
		} else {
			next.CurrentStep--
			if current().State != StepCompleted {
				return Record{}, ErrInvalidTransition
			}
			current().State = StepReverting
		}
	case TransitionFreeze:
		if next.State != StateReverting || transition.EvidenceDigest != "" || transition.Reason == "" {
			return Record{}, ErrInvalidTransition
		}
		next.State = StateFrozen
		next.FreezeReason = transition.Reason
		current().State = StepFrozen
	default:
		return Record{}, ErrInvalidTransition
	}

	next.Revision++
	next.UpdatedAt = now
	next.Digest = recordDigest(next)
	if err := VerifyRecord(next); err != nil {
		return Record{}, fmt.Errorf("%w: %v", ErrInvalidTransition, err)
	}
	return next, nil
}

func verifyStateShape(record Record) error {
	if record.RollbackStartStep < -1 || record.RollbackStartStep >= len(record.Steps) {
		return ErrInvalidRecord
	}
	expect := func(index int, state StepState) bool { return record.Steps[index].State == state }
	switch record.State {
	case StatePrepared, StateApplying, StateObserving:
		if record.CurrentStep < 0 || record.CurrentStep >= len(record.Steps) || record.RollbackStartStep != -1 || record.FailureReason != "" || record.FreezeReason != "" {
			return ErrInvalidRecord
		}
		currentState := StepPending
		if record.State == StateApplying {
			currentState = StepApplying
		} else if record.State == StateObserving {
			currentState = StepObserving
		}
		for index := range record.Steps {
			wanted := StepPending
			if index < record.CurrentStep {
				wanted = StepCompleted
			} else if index == record.CurrentStep {
				wanted = currentState
			}
			if !expect(index, wanted) {
				return ErrInvalidRecord
			}
		}
	case StateCommitted:
		if record.CurrentStep != len(record.Steps) || record.RollbackStartStep != -1 || record.FailureReason != "" || record.FreezeReason != "" {
			return ErrInvalidRecord
		}
		for index := range record.Steps {
			if !expect(index, StepCompleted) {
				return ErrInvalidRecord
			}
		}
	case StateReverting, StateFrozen:
		if record.CurrentStep < 0 || record.CurrentStep > record.RollbackStartStep || record.FailureReason == "" ||
			(record.State == StateFrozen) != (record.FreezeReason != "") {
			return ErrInvalidRecord
		}
		for index := range record.Steps {
			wanted := StepPending
			switch {
			case index < record.CurrentStep:
				wanted = StepCompleted
			case index == record.CurrentStep && record.State == StateReverting:
				wanted = StepReverting
			case index == record.CurrentStep:
				wanted = StepFrozen
			case index <= record.RollbackStartStep:
				wanted = StepReverted
			}
			if !expect(index, wanted) {
				return ErrInvalidRecord
			}
		}
	case StateReverted:
		if record.CurrentStep != -1 || record.RollbackStartStep < 0 || record.FailureReason == "" || record.FreezeReason != "" {
			return ErrInvalidRecord
		}
		for index := range record.Steps {
			wanted := StepPending
			if index <= record.RollbackStartStep {
				wanted = StepReverted
			}
			if !expect(index, wanted) {
				return ErrInvalidRecord
			}
		}
	default:
		return ErrInvalidRecord
	}
	return nil
}

func validStepProgress(step StepProgress) bool {
	for _, digest := range []string{step.ApplyEvidenceDigest, step.ObservationEvidenceDigest, step.ReverseEvidenceDigest} {
		if digest != "" && !digestPattern.MatchString(digest) {
			return false
		}
	}
	ordered := []*time.Time{step.StartedAt, step.ObservationStartedAt, step.CompletedAt, step.RevertedAt}
	var previous *time.Time
	for _, observed := range ordered {
		if observed == nil {
			continue
		}
		if observed.IsZero() || (previous != nil && observed.Before(*previous)) {
			return false
		}
		previous = observed
	}
	started := step.StartedAt != nil
	observing := step.ObservationStartedAt != nil
	completed := step.CompletedAt != nil
	reverted := step.RevertedAt != nil
	applyEvidence := step.ApplyEvidenceDigest != ""
	observationEvidence := step.ObservationEvidenceDigest != ""
	reverseEvidence := step.ReverseEvidenceDigest != ""
	if observing != applyEvidence || completed != observationEvidence || reverted != reverseEvidence ||
		(observing && !started) || (completed && !observing) {
		return false
	}
	switch step.State {
	case StepPending:
		return !started && !observing && !completed && !reverted && !applyEvidence && !observationEvidence && !reverseEvidence
	case StepApplying:
		return started && !observing && !completed && !reverted && !applyEvidence && !observationEvidence && !reverseEvidence
	case StepObserving:
		return started && observing && !completed && !reverted && applyEvidence && !observationEvidence && !reverseEvidence
	case StepCompleted:
		return started && observing && completed && !reverted && applyEvidence && observationEvidence && !reverseEvidence
	case StepReverting, StepFrozen:
		return started && !reverted && !reverseEvidence
	case StepReverted:
		return started && reverted && reverseEvidence
	default:
		return false
	}
}

func recordDigest(record Record) string {
	record.Digest = ""
	encoded, err := json.Marshal(record)
	if err != nil {
		panic(fmt.Sprintf("marshal composite coordinator record: %v", err))
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func timePointer(value time.Time) *time.Time {
	value = value.UTC()
	return &value
}
