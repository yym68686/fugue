// Package dryrunreconciler validates one already-reconciled, cell-local
// Secret mutation candidate through an injected server-side-dry-run writer.
// It owns only orchestration and secret-free status: no credential,
// filesystem, TLS, Kubernetes client, process, or live-mutation capability.
package dryrunreconciler

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

	"fugue/internal/backupmaterializer/materialization"
	"fugue/internal/backupmaterializer/reconcile"
	"fugue/internal/backupmaterializer/reconciler"
	"fugue/internal/backupmaterializer/secretwriter"
)

const (
	APIVersion = "backup-materializer-secret-dry-run-cycle.fugue.dev/v1"
	Kind       = "BackupObserverSecretDryRunCycleStatus"
	Policy     = "single-cell-reconcile-bound-dry-run-v1"

	OutcomeAccepted              Outcome = "accepted"
	OutcomeConflict              Outcome = "conflict"
	OutcomeRejected              Outcome = "rejected"
	OutcomeCredentialUnavailable Outcome = "credential-unavailable"
	OutcomeUnavailable           Outcome = "unavailable"
	OutcomeResponseInvalid       Outcome = "response-invalid"
	OutcomeReconcileRequired     Outcome = "reconcile-required"
)

var (
	ErrConfig    = errors.New("backup materializer Secret dry-run cycle configuration invalid")
	ErrDisabled  = errors.New("backup materializer Secret dry-run cycle disabled")
	ErrInvariant = errors.New("backup materializer Secret dry-run cycle invariant failed")
)

type Outcome string

// Validator is deliberately narrower than an HTTP or Kubernetes client. The
// core Secret writer satisfies it, while its projected credential and TLS
// bootstrap remain outside this package.
type Validator interface {
	Enabled() bool
	DryRun(context.Context, materialization.Plan, reconcile.Decision) (secretwriter.Result, error)
}

type Config struct {
	Enabled   bool
	CellKey   string
	Validator Validator
	Now       func() time.Time
}

func (config Config) String() string {
	return "backup materializer Secret dry-run cycle configuration [REDACTED]"
}

func (config Config) GoString() string { return config.String() }

type Reconciler struct {
	enabled   bool
	cellKey   string
	cellID    string
	validator Validator
	now       func() time.Time
}

// Status is the secret-free result of attempting one exact mutation
// candidate. Even an accepted server-side dry run remains non-persisted and
// cannot authorize execution or a production mutation.
type Status struct {
	APIVersion                string               `json:"apiVersion"`
	Kind                      string               `json:"kind"`
	Policy                    string               `json:"policy"`
	CellKey                   string               `json:"cellKey"`
	CellID                    string               `json:"cellId"`
	Namespace                 string               `json:"namespace"`
	SecretName                string               `json:"secretName"`
	Action                    reconcile.Action     `json:"action"`
	Outcome                   Outcome              `json:"outcome"`
	ReconcileStatusDigest     string               `json:"reconcileStatusDigest"`
	DesiredPlanDigest         string               `json:"desiredPlanDigest"`
	DecisionDigest            string               `json:"decisionDigest"`
	ReconcileStatus           *reconciler.Status   `json:"reconcileStatus"`
	ReceiptDigest             string               `json:"receiptDigest,omitempty"`
	Receipt                   *secretwriter.Result `json:"receipt,omitempty"`
	IdempotencyKey            string               `json:"idempotencyKey"`
	EvaluatedAt               time.Time            `json:"evaluatedAt"`
	AttemptedAt               time.Time            `json:"attemptedAt"`
	MutationCandidate         bool                 `json:"mutationCandidate"`
	ValidationAccepted        bool                 `json:"validationAccepted"`
	ServerSideDryRunAccepted  bool                 `json:"serverSideDryRunAccepted"`
	ExistingObjectPreserved   bool                 `json:"existingObjectPreserved"`
	Blocked                   bool                 `json:"blocked"`
	Retryable                 bool                 `json:"retryable"`
	Persisted                 bool                 `json:"persisted"`
	DeleteAllowed             bool                 `json:"deleteAllowed"`
	ObservationOnly           bool                 `json:"observationOnly"`
	ExecutionAllowed          bool                 `json:"executionAllowed"`
	ProductionMutationAllowed bool                 `json:"productionMutationAllowed"`
	Digest                    string               `json:"digest"`
}

// New performs no validation request. Disabled construction ignores and
// retains none of the cell, validator, or clock capabilities.
func New(config Config) (*Reconciler, error) {
	if !config.Enabled {
		return &Reconciler{}, nil
	}
	identity, err := materialization.SecretIdentityForCell(config.CellKey)
	if err != nil || nilInterface(config.Validator) || !config.Validator.Enabled() {
		return nil, ErrConfig
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	return &Reconciler{
		enabled: true, cellKey: identity.CellKey, cellID: identity.CellID,
		validator: config.Validator, now: now,
	}, nil
}

func (controller *Reconciler) Enabled() bool {
	return controller != nil && controller.enabled && controller.cellKey != "" && controller.cellID != "" &&
		controller.validator != nil && controller.validator.Enabled() && controller.now != nil
}

func (controller *Reconciler) String() string {
	if controller == nil {
		return "backup materializer Secret dry-run cycle <nil>"
	}
	return "backup materializer Secret dry-run cycle [REDACTED]"
}

func (controller *Reconciler) GoString() string { return controller.String() }

// ValidateCandidateOnce attempts exactly one already-sealed mutation
// candidate. Known writer failures become bounded cell-local status so a
// supervisor can recover on a later fresh reconcile. Cancellation and broken
// caller/validator contracts remain explicit errors.
func (controller *Reconciler) ValidateCandidateOnce(
	ctx context.Context,
	plan materialization.Plan,
	cycle reconciler.Status,
) (Status, error) {
	if controller == nil {
		return Status{}, ErrConfig
	}
	if !controller.Enabled() {
		return Status{}, ErrDisabled
	}
	if ctx == nil {
		return Status{}, ErrConfig
	}
	if err := ctx.Err(); err != nil {
		return Status{}, err
	}
	if !validCandidateInput(controller.cellKey, plan, cycle) {
		return Status{}, ErrInvariant
	}
	attemptedAt := controller.now().UTC().Truncate(time.Second)
	if !canonicalTime(attemptedAt) || attemptedAt.Before(cycle.EvaluatedAt) {
		return Status{}, ErrInvariant
	}
	receipt, err := controller.validator.DryRun(ctx, plan, *cycle.Decision)
	if ctx.Err() != nil {
		return Status{}, ctx.Err()
	}
	status := newStatus(controller.cellID, cycle, attemptedAt)
	if err == nil {
		if !validReceiptForCycle(receipt, plan, cycle, attemptedAt) {
			return Status{}, ErrInvariant
		}
		status.Outcome = OutcomeAccepted
		status.ValidationAccepted = true
		status.ServerSideDryRunAccepted = true
		receiptCopy := receipt
		status.Receipt = &receiptCopy
		status.ReceiptDigest = receipt.Digest
	} else {
		status.Outcome, status.Retryable = classify(err)
		status.Blocked = true
	}
	status.Digest = DigestStatus(status)
	if ValidateStatus(status) != nil {
		return Status{}, ErrInvariant
	}
	return status, nil
}

func ValidateStatus(status Status) error {
	identity, err := materialization.SecretIdentityForCell(status.CellKey)
	if err != nil || status.APIVersion != APIVersion || status.Kind != Kind || status.Policy != Policy ||
		status.CellID != identity.CellID || status.Namespace != identity.Namespace || status.SecretName != identity.SecretName ||
		status.ReconcileStatus == nil || reconciler.ValidateStatus(*status.ReconcileStatus) != nil ||
		status.ReconcileStatus.CellKey != status.CellKey || status.ReconcileStatus.Digest != status.ReconcileStatusDigest ||
		status.ReconcileStatus.DesiredState != reconciler.DesiredAvailable || status.ReconcileStatus.Decision == nil ||
		!status.ReconcileStatus.MutationCandidate || status.ReconcileStatus.Blocked || status.ReconcileStatus.Ready ||
		status.ReconcileStatus.Action != status.Action || status.ReconcileStatus.DesiredPlanDigest != status.DesiredPlanDigest ||
		status.ReconcileStatus.DecisionDigest != status.DecisionDigest ||
		(status.Action != reconcile.ActionCreateIfAbsent && status.Action != reconcile.ActionReplaceResourceVersionCAS) ||
		!validDigest(status.DesiredPlanDigest) || !validDigest(status.DecisionDigest) ||
		status.EvaluatedAt != status.ReconcileStatus.EvaluatedAt || !canonicalTime(status.AttemptedAt) ||
		status.AttemptedAt.Before(status.EvaluatedAt) || !status.MutationCandidate ||
		status.ExistingObjectPreserved != (status.Action == reconcile.ActionReplaceResourceVersionCAS) ||
		status.Persisted || status.DeleteAllowed || !status.ObservationOnly || status.ExecutionAllowed ||
		status.ProductionMutationAllowed || status.IdempotencyKey != idempotencyKey(status) ||
		status.Digest != DigestStatus(status) {
		return ErrInvariant
	}
	switch status.Outcome {
	case OutcomeAccepted:
		if !status.ValidationAccepted || !status.ServerSideDryRunAccepted || status.Blocked || status.Retryable ||
			status.Receipt == nil || status.ReceiptDigest == "" || status.ReceiptDigest != status.Receipt.Digest ||
			!validReceiptForStatus(*status.Receipt, status) {
			return ErrInvariant
		}
	case OutcomeConflict, OutcomeCredentialUnavailable, OutcomeUnavailable, OutcomeResponseInvalid, OutcomeReconcileRequired:
		if status.ValidationAccepted || status.ServerSideDryRunAccepted || !status.Blocked || !status.Retryable ||
			status.Receipt != nil || status.ReceiptDigest != "" {
			return ErrInvariant
		}
	case OutcomeRejected:
		if status.ValidationAccepted || status.ServerSideDryRunAccepted || !status.Blocked || status.Retryable ||
			status.Receipt != nil || status.ReceiptDigest != "" {
			return ErrInvariant
		}
	default:
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
		"BackupObserverSecretDryRunCycleStatus{cell=%q action=%q outcome=%q accepted=%t persisted=false executionAllowed=false digest=%q}",
		status.CellKey, status.Action, status.Outcome, status.ValidationAccepted, status.Digest,
	)
}

func (status Status) GoString() string { return status.String() }

func validCandidateInput(cellKey string, plan materialization.Plan, cycle reconciler.Status) bool {
	return reconciler.ValidateStatus(cycle) == nil && cycle.CellKey == cellKey && cycle.DesiredState == reconciler.DesiredAvailable &&
		cycle.Decision != nil && cycle.MutationCandidate && !cycle.Blocked && !cycle.Ready &&
		(cycle.Action == reconcile.ActionCreateIfAbsent || cycle.Action == reconcile.ActionReplaceResourceVersionCAS) &&
		materialization.Validate(plan, cycle.EvaluatedAt) == nil && plan.CellKey == cellKey &&
		plan.Digest == cycle.DesiredPlanDigest && cycle.Decision.DesiredPlanDigest == plan.Digest
}

func validReceiptForCycle(
	receipt secretwriter.Result,
	plan materialization.Plan,
	cycle reconciler.Status,
	attemptedAt time.Time,
) bool {
	return secretwriter.ValidateResult(receipt) == nil && receipt.CellKey == cycle.CellKey && receipt.CellID == cycle.CellID &&
		receipt.Namespace == plan.Namespace && receipt.SecretName == plan.SecretName && receipt.Action == cycle.Action &&
		receipt.PlanDigest == plan.Digest && receipt.DecisionDigest == cycle.DecisionDigest &&
		!receipt.ValidatedAt.Before(attemptedAt) &&
		receipt.ValidatedAt.Sub(cycle.EvaluatedAt) <= secretwriter.MaximumDecisionAge
}

func validReceiptForStatus(receipt secretwriter.Result, status Status) bool {
	return secretwriter.ValidateResult(receipt) == nil && receipt.CellKey == status.CellKey && receipt.CellID == status.CellID &&
		receipt.Namespace == status.Namespace && receipt.SecretName == status.SecretName && receipt.Action == status.Action &&
		receipt.PlanDigest == status.DesiredPlanDigest && receipt.DecisionDigest == status.DecisionDigest &&
		!receipt.ValidatedAt.Before(status.AttemptedAt) &&
		receipt.ValidatedAt.Sub(status.EvaluatedAt) <= secretwriter.MaximumDecisionAge
}

func newStatus(cellID string, cycle reconciler.Status, attemptedAt time.Time) Status {
	cycleCopy := cloneReconcileStatus(cycle)
	status := Status{
		APIVersion: APIVersion, Kind: Kind, Policy: Policy,
		CellKey: cycle.CellKey, CellID: cellID, Namespace: cycle.Decision.Namespace, SecretName: cycle.Decision.SecretName,
		Action: cycle.Action, ReconcileStatusDigest: cycle.Digest, DesiredPlanDigest: cycle.DesiredPlanDigest,
		DecisionDigest: cycle.DecisionDigest, ReconcileStatus: &cycleCopy, EvaluatedAt: cycle.EvaluatedAt,
		AttemptedAt: attemptedAt, MutationCandidate: true,
		ExistingObjectPreserved: cycle.Action == reconcile.ActionReplaceResourceVersionCAS,
		ObservationOnly:         true,
	}
	status.IdempotencyKey = idempotencyKey(status)
	return status
}

func classify(err error) (Outcome, bool) {
	switch {
	case errors.Is(err, secretwriter.ErrConflict):
		return OutcomeConflict, true
	case errors.Is(err, secretwriter.ErrRejected):
		return OutcomeRejected, false
	case errors.Is(err, secretwriter.ErrCredentialUnavailable):
		return OutcomeCredentialUnavailable, true
	case errors.Is(err, secretwriter.ErrResponse):
		return OutcomeResponseInvalid, true
	case errors.Is(err, secretwriter.ErrIntent):
		return OutcomeReconcileRequired, true
	default:
		return OutcomeUnavailable, true
	}
}

func cloneReconcileStatus(status reconciler.Status) reconciler.Status {
	cloned := status
	if status.Decision != nil {
		decision := *status.Decision
		cloned.Decision = &decision
	}
	return cloned
}

func idempotencyKey(status Status) string {
	if status.CellID == "" || !validDigest(status.ReconcileStatusDigest) || !validDigest(status.DecisionDigest) {
		return ""
	}
	basis := status.ReconcileStatusDigest + ":" + status.DecisionDigest
	digest := sha256.Sum256([]byte(basis))
	return "backup-materializer-secret-dry-run-cycle/" + status.CellID + "/" + hex.EncodeToString(digest[:])
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
