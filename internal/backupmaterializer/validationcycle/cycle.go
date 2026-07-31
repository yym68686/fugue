// Package validationcycle composes one prepared source reconciliation with
// zero or one server-side-dry-run candidate validation. It retains only
// secret-free status and owns no source, credential, transport, Kubernetes,
// filesystem, process, or live-mutation capability.
package validationcycle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"fugue/internal/backupmaterializer/dryrunreconciler"
	"fugue/internal/backupmaterializer/materialization"
	"fugue/internal/backupmaterializer/reconcile"
	"fugue/internal/backupmaterializer/reconciler"
)

const (
	APIVersion = "backup-materializer-validation-cycle.fugue.dev/v1"
	Kind       = "BackupObserverSecretValidationCycleStatus"
	Policy     = "single-read-zero-or-one-dry-run-v1"
)

var (
	ErrConfig    = errors.New("backup materializer validation cycle configuration invalid")
	ErrDisabled  = errors.New("backup materializer validation cycle disabled")
	ErrInvariant = errors.New("backup materializer validation cycle invariant failed")
)

type Source interface {
	Enabled() bool
	PrepareOnce(context.Context) (reconciler.PreparedCycle, error)
}

type CandidateValidator interface {
	Enabled() bool
	ValidateCandidateOnce(context.Context, materialization.Plan, reconciler.Status) (dryrunreconciler.Status, error)
}

type Config struct {
	Enabled   bool
	CellKey   string
	Source    Source
	Validator CandidateValidator
}

func (config Config) String() string {
	return "backup materializer validation cycle configuration [REDACTED]"
}

func (config Config) GoString() string { return config.String() }

type Cycle struct {
	enabled   bool
	cellKey   string
	cellID    string
	source    Source
	validator CandidateValidator
}

// Status is a public, secret-free result for one exact prepared cycle. The
// private candidate plan is consumed during the call and never retained here.
type Status struct {
	APIVersion                string                            `json:"apiVersion"`
	Kind                      string                            `json:"kind"`
	Policy                    string                            `json:"policy"`
	CellKey                   string                            `json:"cellKey"`
	CellID                    string                            `json:"cellId"`
	Action                    reconcile.Action                  `json:"action"`
	Reason                    string                            `json:"reason"`
	PreparedCycleDigest       string                            `json:"preparedCycleDigest"`
	PreparedCycle             *reconciler.PreparedCycleEvidence `json:"preparedCycle"`
	CandidatePlanDigest       string                            `json:"candidatePlanDigest,omitempty"`
	ValidationOutcome         dryrunreconciler.Outcome          `json:"validationOutcome,omitempty"`
	ValidationStatusDigest    string                            `json:"validationStatusDigest,omitempty"`
	ValidationStatus          *dryrunreconciler.Status          `json:"validationStatus,omitempty"`
	IdempotencyKey            string                            `json:"idempotencyKey"`
	EvaluatedAt               time.Time                         `json:"evaluatedAt"`
	ValidationAttemptedAt     *time.Time                        `json:"validationAttemptedAt,omitempty"`
	Ready                     bool                              `json:"ready"`
	Converged                 bool                              `json:"converged"`
	LastKnownGoodServing      bool                              `json:"lastKnownGoodServing"`
	Blocked                   bool                              `json:"blocked"`
	Retryable                 bool                              `json:"retryable"`
	MutationCandidate         bool                              `json:"mutationCandidate"`
	ValidationRequired        bool                              `json:"validationRequired"`
	ValidationAttempted       bool                              `json:"validationAttempted"`
	ValidationAccepted        bool                              `json:"validationAccepted"`
	ExistingObjectPreserved   bool                              `json:"existingObjectPreserved"`
	Persisted                 bool                              `json:"persisted"`
	DeleteAllowed             bool                              `json:"deleteAllowed"`
	ObservationOnly           bool                              `json:"observationOnly"`
	ExecutionAllowed          bool                              `json:"executionAllowed"`
	ProductionMutationAllowed bool                              `json:"productionMutationAllowed"`
	Digest                    string                            `json:"digest"`
}

// New performs no source or validation operation. Disabled construction
// ignores and retains none of the cell or injected capabilities.
func New(config Config) (*Cycle, error) {
	if !config.Enabled {
		return &Cycle{}, nil
	}
	identity, err := materialization.SecretIdentityForCell(config.CellKey)
	if err != nil || nilInterface(config.Source) || nilInterface(config.Validator) ||
		!config.Source.Enabled() || !config.Validator.Enabled() {
		return nil, ErrConfig
	}
	return &Cycle{
		enabled: true, cellKey: identity.CellKey, cellID: identity.CellID,
		source: config.Source, validator: config.Validator,
	}, nil
}

func (cycle *Cycle) Enabled() bool {
	return cycle != nil && cycle.enabled && cycle.cellKey != "" && cycle.cellID != "" &&
		cycle.source != nil && cycle.validator != nil && cycle.source.Enabled() && cycle.validator.Enabled()
}

func (cycle *Cycle) String() string {
	if cycle == nil {
		return "backup materializer validation cycle <nil>"
	}
	return "backup materializer validation cycle [REDACTED]"
}

func (cycle *Cycle) GoString() string { return cycle.String() }

// ReconcileOnce reads one prepared cycle. Stable, LKG, and blocked source
// outcomes use zero validator calls. A mutation candidate consumes its private
// plan and performs exactly one validation call. The plan is never copied into
// the returned status.
func (cycle *Cycle) ReconcileOnce(ctx context.Context) (Status, error) {
	if cycle == nil {
		return Status{}, ErrConfig
	}
	if !cycle.Enabled() {
		return Status{}, ErrDisabled
	}
	if ctx == nil {
		return Status{}, ErrConfig
	}
	if err := ctx.Err(); err != nil {
		return Status{}, err
	}
	prepared, err := cycle.source.PrepareOnce(ctx)
	if ctx.Err() != nil {
		return Status{}, ctx.Err()
	}
	if err != nil {
		return Status{}, ErrInvariant
	}
	if reconciler.ValidatePreparedCycle(prepared) != nil || prepared.CellKey != cycle.cellKey {
		return Status{}, ErrInvariant
	}
	evidence, err := prepared.Evidence()
	if err != nil || reconciler.ValidatePreparedCycleEvidence(evidence) != nil {
		return Status{}, ErrInvariant
	}
	status := statusFromPrepared(cycle.cellID, evidence)
	if !prepared.CandidateAvailable {
		status.Digest = DigestStatus(status)
		if ValidateStatus(status) != nil {
			return Status{}, ErrInvariant
		}
		return status, nil
	}
	plan, ok := prepared.CandidatePlan()
	if !ok {
		return Status{}, ErrInvariant
	}
	validation, err := cycle.validator.ValidateCandidateOnce(ctx, plan, prepared.Status)
	if ctx.Err() != nil {
		return Status{}, ctx.Err()
	}
	if err != nil || dryrunreconciler.ValidateStatus(validation) != nil || validation.CellKey != cycle.cellKey {
		return Status{}, ErrInvariant
	}
	validationCopy := cloneValidationStatus(validation)
	status.ValidationOutcome = validation.Outcome
	status.ValidationStatusDigest = validation.Digest
	status.ValidationStatus = &validationCopy
	status.ValidationAttemptedAt = timePointer(validation.AttemptedAt)
	status.ValidationAttempted = true
	status.ValidationAccepted = validation.ValidationAccepted
	status.ExistingObjectPreserved = validation.ExistingObjectPreserved
	status.Blocked = validation.Blocked
	status.Retryable = validation.Retryable
	status.Digest = DigestStatus(status)
	if ValidateStatus(status) != nil {
		return Status{}, ErrInvariant
	}
	return status, nil
}

func ValidateStatus(status Status) error {
	identity, err := materialization.SecretIdentityForCell(status.CellKey)
	if err != nil || status.APIVersion != APIVersion || status.Kind != Kind || status.Policy != Policy ||
		status.CellID != identity.CellID || status.PreparedCycle == nil ||
		reconciler.ValidatePreparedCycleEvidence(*status.PreparedCycle) != nil ||
		status.PreparedCycle.CellKey != status.CellKey || status.PreparedCycle.Digest != status.PreparedCycleDigest ||
		status.Action != status.PreparedCycle.Status.Action || status.Reason != status.PreparedCycle.Status.Reason ||
		status.EvaluatedAt != status.PreparedCycle.Status.EvaluatedAt ||
		status.MutationCandidate != status.PreparedCycle.Status.MutationCandidate ||
		status.ValidationRequired != status.PreparedCycle.Status.MutationCandidate ||
		status.CandidatePlanDigest != status.PreparedCycle.CandidatePlanDigest ||
		!validDigest(status.PreparedCycleDigest) || status.IdempotencyKey != idempotencyKey(status) ||
		status.Persisted || status.DeleteAllowed || !status.ObservationOnly || status.ExecutionAllowed ||
		status.ProductionMutationAllowed || status.Digest != DigestStatus(status) {
		return ErrInvariant
	}
	if !status.ValidationRequired {
		if status.CandidatePlanDigest != "" || status.ValidationOutcome != "" ||
			status.ValidationStatusDigest != "" || status.ValidationStatus != nil ||
			status.ValidationAttemptedAt != nil || status.ValidationAttempted || status.ValidationAccepted ||
			status.ExistingObjectPreserved || status.Ready != status.PreparedCycle.Status.Ready ||
			status.Converged != status.PreparedCycle.Status.Converged ||
			status.LastKnownGoodServing != status.PreparedCycle.Status.LastKnownGoodServing ||
			status.Blocked != status.PreparedCycle.Status.Blocked || status.Retryable != status.PreparedCycle.Status.Retryable {
			return ErrInvariant
		}
		return nil
	}
	if !validDigest(status.CandidatePlanDigest) || status.CandidatePlanDigest != status.PreparedCycle.Status.DesiredPlanDigest ||
		status.ValidationStatus == nil || !validDigest(status.ValidationStatusDigest) ||
		dryrunreconciler.ValidateStatus(*status.ValidationStatus) != nil ||
		status.ValidationStatus.Digest != status.ValidationStatusDigest || status.ValidationStatus.CellKey != status.CellKey ||
		status.ValidationStatus.Action != status.Action ||
		status.ValidationStatus.ReconcileStatusDigest != status.PreparedCycle.StatusDigest ||
		status.ValidationStatus.DesiredPlanDigest != status.CandidatePlanDigest ||
		status.ValidationOutcome != status.ValidationStatus.Outcome || status.ValidationAttemptedAt == nil ||
		!canonicalTime(*status.ValidationAttemptedAt) || *status.ValidationAttemptedAt != status.ValidationStatus.AttemptedAt ||
		!status.ValidationAttempted || status.ValidationAccepted != status.ValidationStatus.ValidationAccepted ||
		status.ExistingObjectPreserved != status.ValidationStatus.ExistingObjectPreserved ||
		status.Blocked != status.ValidationStatus.Blocked || status.Retryable != status.ValidationStatus.Retryable ||
		status.Ready || status.Converged || status.LastKnownGoodServing {
		return ErrInvariant
	}
	return nil
}

func DigestStatus(status Status) string {
	status.Digest = ""
	document, err := json.Marshal(status)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(document)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func (status Status) String() string {
	return fmt.Sprintf(
		"BackupObserverSecretValidationCycleStatus{cell=%q action=%q validation=%q ready=%t accepted=%t persisted=false executionAllowed=false digest=%q}",
		status.CellKey, status.Action, status.ValidationOutcome, status.Ready, status.ValidationAccepted, status.Digest,
	)
}

func (status Status) GoString() string { return status.String() }

func statusFromPrepared(cellID string, prepared reconciler.PreparedCycleEvidence) Status {
	preparedCopy := clonePreparedEvidence(prepared)
	status := Status{
		APIVersion: APIVersion, Kind: Kind, Policy: Policy,
		CellKey: prepared.CellKey, CellID: cellID, Action: prepared.Status.Action, Reason: prepared.Status.Reason,
		PreparedCycleDigest: prepared.Digest, PreparedCycle: &preparedCopy,
		CandidatePlanDigest: prepared.CandidatePlanDigest, EvaluatedAt: prepared.EvaluatedAt,
		Ready: prepared.Status.Ready, Converged: prepared.Status.Converged,
		LastKnownGoodServing: prepared.Status.LastKnownGoodServing, Blocked: prepared.Status.Blocked,
		Retryable: prepared.Status.Retryable, MutationCandidate: prepared.Status.MutationCandidate,
		ValidationRequired: prepared.CandidateAvailable, ObservationOnly: true,
	}
	if prepared.CandidateAvailable {
		status.Ready = false
		status.Converged = false
		status.LastKnownGoodServing = false
		status.Blocked = false
		status.Retryable = false
	}
	status.IdempotencyKey = idempotencyKey(status)
	return status
}

func clonePreparedEvidence(evidence reconciler.PreparedCycleEvidence) reconciler.PreparedCycleEvidence {
	cloned := evidence
	cloned.Status = cloneSourceStatus(evidence.Status)
	return cloned
}

func cloneSourceStatus(status reconciler.Status) reconciler.Status {
	cloned := status
	if status.Decision != nil {
		decision := *status.Decision
		cloned.Decision = &decision
	}
	return cloned
}

func cloneValidationStatus(status dryrunreconciler.Status) dryrunreconciler.Status {
	cloned := status
	if status.ReconcileStatus != nil {
		reconcileStatus := cloneSourceStatus(*status.ReconcileStatus)
		cloned.ReconcileStatus = &reconcileStatus
	}
	if status.Receipt != nil {
		receipt := *status.Receipt
		cloned.Receipt = &receipt
	}
	return cloned
}

func idempotencyKey(status Status) string {
	if status.CellID == "" || !validDigest(status.PreparedCycleDigest) {
		return ""
	}
	return "backup-materializer-validation-cycle/" + status.CellID + "/" +
		strings.TrimPrefix(status.PreparedCycleDigest, "sha256:")
}

func validDigest(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil && strings.ToLower(value) == value
}

func canonicalTime(value time.Time) bool {
	return !value.IsZero() && value.Location() == time.UTC && value.Nanosecond() == 0
}

func timePointer(value time.Time) *time.Time {
	copy := value
	return &copy
}

func nilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
