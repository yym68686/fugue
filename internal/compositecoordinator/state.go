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
	"reflect"
	"regexp"
	"strings"
	"time"

	"fugue/internal/releasecontract"
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
	APIVersion        string                               `json:"apiVersion"`
	Kind              string                               `json:"kind"`
	Policy            string                               `json:"policy"`
	ID                string                               `json:"id"`
	Plan              releasecontract.CompositeReleasePlan `json:"plan"`
	State             State                                `json:"state"`
	CurrentStep       int                                  `json:"currentStep"`
	RollbackStartStep int                                  `json:"rollbackStartStep"`
	Steps             []StepProgress                       `json:"steps"`
	Revision          int64                                `json:"revision"`
	FailureReason     string                               `json:"failureReason"`
	FreezeReason      string                               `json:"freezeReason"`
	CreatedAt         time.Time                            `json:"createdAt"`
	UpdatedAt         time.Time                            `json:"updatedAt"`
	Digest            string                               `json:"digest"`
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
	if err := validateStrictRecordJSON(data); err != nil {
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
	candidate := Record(decoded)
	if err := VerifyRecord(candidate); err != nil {
		return err
	}
	*record = candidate
	return nil
}

func validateStrictRecordJSON(data []byte) error {
	if err := validateJSONUnicodeEscapes(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := validateJSONValueForType(decoder, reflect.TypeOf(Record{}), "composite coordinator record"); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("composite coordinator JSON has trailing content")
	}
	return nil
}

func validateJSONValueForType(decoder *json.Decoder, expected reflect.Type, path string) error {
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}

	switch expected.Kind() {
	case reflect.Pointer:
		if token == nil {
			return nil
		}
		return validateJSONTokenForType(decoder, token, expected.Elem(), path)
	default:
		return validateJSONTokenForType(decoder, token, expected, path)
	}
}

func validateJSONTokenForType(decoder *json.Decoder, token json.Token, expected reflect.Type, path string) error {
	switch expected.Kind() {
	case reflect.Struct:
		if expected == reflect.TypeOf(time.Time{}) {
			if _, ok := token.(string); !ok {
				return fmt.Errorf("%s must be a timestamp string", path)
			}
			return nil
		}
		opening, ok := token.(json.Delim)
		if !ok || opening != '{' {
			return fmt.Errorf("%s must be a non-null object", path)
		}
		fields := make(map[string]reflect.StructField, expected.NumField())
		required := make(map[string]bool, expected.NumField())
		for index := 0; index < expected.NumField(); index++ {
			field := expected.Field(index)
			name := jsonFieldName(field)
			if field.PkgPath != "" || name == "-" {
				continue
			}
			fields[name] = field
			_, options, _ := strings.Cut(field.Tag.Get("json"), ",")
			required[name] = !strings.Contains(","+options+",", ",omitempty,")
		}
		seen := make(map[string]struct{}, len(fields))
		for decoder.More() {
			nameToken, err := decoder.Token()
			if err != nil {
				return fmt.Errorf("%s field name: %w", path, err)
			}
			name, ok := nameToken.(string)
			if !ok {
				return fmt.Errorf("%s field name must be a string", path)
			}
			if _, duplicate := seen[name]; duplicate {
				return fmt.Errorf("%s contains duplicate field %q", path, name)
			}
			seen[name] = struct{}{}
			field, known := fields[name]
			if !known {
				return fmt.Errorf("%s contains unknown field %q", path, name)
			}
			if err := validateJSONValueForType(decoder, field.Type, joinJSONPath(path, name)); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("%s: close object: %w", path, err)
		}
		if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
			return fmt.Errorf("%s object is not closed", path)
		}
		for name, isRequired := range required {
			if _, present := seen[name]; isRequired && !present {
				return fmt.Errorf("%s is missing required field %q", path, name)
			}
		}
		return nil
	case reflect.Slice, reflect.Array:
		opening, ok := token.(json.Delim)
		if !ok || opening != '[' {
			return fmt.Errorf("%s must be an array", path)
		}
		index := 0
		for decoder.More() {
			if err := validateJSONValueForType(decoder, expected.Elem(), fmt.Sprintf("%s[%d]", path, index)); err != nil {
				return err
			}
			index++
		}
		closing, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("%s: close array: %w", path, err)
		}
		if delimiter, ok := closing.(json.Delim); !ok || delimiter != ']' {
			return fmt.Errorf("%s array is not closed", path)
		}
		return nil
	case reflect.String:
		if _, ok := token.(string); !ok {
			return fmt.Errorf("%s must be a string", path)
		}
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		number, ok := token.(json.Number)
		if !ok || strings.ContainsAny(number.String(), ".eE") {
			return fmt.Errorf("%s must be an integer", path)
		}
		if _, err := number.Int64(); err != nil {
			return fmt.Errorf("%s must be an integer", path)
		}
		return nil
	default:
		return fmt.Errorf("%s has unsupported persisted JSON type %s", path, expected)
	}
}

func jsonFieldName(field reflect.StructField) string {
	name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
	if name == "" {
		return field.Name
	}
	return name
}

func joinJSONPath(parent, child string) string {
	if parent == "" {
		return child
	}
	return parent + "." + child
}

// encoding/json replaces malformed UTF-16 surrogate escapes with U+FFFD.
// Durable release evidence rejects those bytes before decoding instead.
func validateJSONUnicodeEscapes(data []byte) error {
	for index := 0; index < len(data); index++ {
		if data[index] != '"' {
			continue
		}
		closed := false
		for index++; index < len(data); index++ {
			switch data[index] {
			case '"':
				closed = true
			case '\\':
				if index+1 >= len(data) {
					return fmt.Errorf("unterminated JSON escape")
				}
				escape := data[index+1]
				if escape != 'u' {
					if !strings.ContainsRune(`"\\/bfnrt`, rune(escape)) {
						return fmt.Errorf("invalid JSON escape \\%c", escape)
					}
					index++
					continue
				}
				codePoint, ok := decodeHexQuad(data, index+2)
				if !ok {
					return fmt.Errorf("invalid JSON Unicode escape")
				}
				switch {
				case codePoint >= 0xd800 && codePoint <= 0xdbff:
					low, lowOK := decodeFollowingLowSurrogate(data, index+6)
					if !lowOK || low < 0xdc00 || low > 0xdfff {
						return fmt.Errorf("isolated high surrogate in JSON string")
					}
					index += 11
				case codePoint >= 0xdc00 && codePoint <= 0xdfff:
					return fmt.Errorf("isolated low surrogate in JSON string")
				default:
					index += 5
				}
			case 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
				0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
				0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
				0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f:
				return fmt.Errorf("unescaped control character in JSON string")
			}
			if closed {
				break
			}
		}
		if !closed {
			return fmt.Errorf("unterminated JSON string")
		}
	}
	return nil
}

func decodeHexQuad(data []byte, start int) (uint16, bool) {
	if start < 0 || start+4 > len(data) {
		return 0, false
	}
	var value uint16
	for _, digit := range data[start : start+4] {
		value <<= 4
		switch {
		case digit >= '0' && digit <= '9':
			value |= uint16(digit - '0')
		case digit >= 'a' && digit <= 'f':
			value |= uint16(digit-'a') + 10
		case digit >= 'A' && digit <= 'F':
			value |= uint16(digit-'A') + 10
		default:
			return 0, false
		}
	}
	return value, true
}

func decodeFollowingLowSurrogate(data []byte, start int) (uint16, bool) {
	if start < 0 || start+6 > len(data) || data[start] != '\\' || data[start+1] != 'u' {
		return 0, false
	}
	return decodeHexQuad(data, start+2)
}

func NewRecord(id string, plan releasecontract.CompositeReleasePlan, now time.Time) (Record, error) {
	if !recordIDPattern.MatchString(id) || releasecontract.VerifyCompositeReleasePlan(plan) != nil || now.IsZero() {
		return Record{}, ErrInvalidRecord
	}
	plan.BaseVersions = append([]releasecontract.DomainVersion(nil), plan.BaseVersions...)
	plan.TargetVersions = append([]releasecontract.DomainVersion(nil), plan.TargetVersions...)
	plan.Steps = releasecontract.CloneCompositeSteps(plan.Steps)
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
		!recordIDPattern.MatchString(record.ID) || releasecontract.VerifyCompositeReleasePlan(record.Plan) != nil ||
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
