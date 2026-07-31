// Package reconciler composes one desired observer-input source with one
// current Secret observation source and the pure cell-local CAS/LKG policy. It
// owns no filesystem, network, Kubernetes, datastore, signer, writer, timer,
// goroutine, or process capability.
package reconciler

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

	materializercontract "fugue/internal/backupmaterializer/contract"
	"fugue/internal/backupmaterializer/materialization"
	"fugue/internal/backupmaterializer/reconcile"
)

const (
	APIVersion              = "backup-materializer-reconciler.fugue.dev/v1"
	Kind                    = "BackupObserverSecretReconcileStatus"
	Policy                  = "cell-local-idempotent-shadow-loop-v1"
	PreparedCycleAPIVersion = "backup-materializer-candidate-handoff.fugue.dev/v1"
	PreparedCycleKind       = "BackupObserverSecretPreparedCycle"
	PreparedCyclePolicy     = "secret-free-status-private-candidate-v1"

	CurrentUnavailable = "unavailable"

	DesiredNotRead     DesiredState = "not-read"
	DesiredAvailable   DesiredState = "available"
	DesiredUnavailable DesiredState = "unavailable"
	DesiredInvalid     DesiredState = "invalid"

	ReasonCurrentObservationUnavailable = "current-observation-unavailable"
)

var (
	ErrConfig    = errors.New("backup materializer reconciler configuration invalid")
	ErrDisabled  = errors.New("backup materializer reconciler disabled")
	ErrInvariant = errors.New("backup materializer reconciler invariant failed")
)

type DesiredState string

type DesiredSource interface {
	Fetch(context.Context) (materializercontract.ObserverInputBundle, error)
}

type CurrentSource interface {
	Observe(context.Context) (reconcile.Observation, error)
}

type Config struct {
	Enabled       bool
	CellKey       string
	DesiredSource DesiredSource
	CurrentSource CurrentSource
	Now           func() time.Time
}

func (config Config) String() string {
	return "backup materializer reconciler configuration [REDACTED]"
}

func (config Config) GoString() string { return config.String() }

type Reconciler struct {
	enabled bool
	cellKey string
	cellID  string
	desired DesiredSource
	current CurrentSource
	now     func() time.Time
}

// Status is one digest-bound, public, secret-free shadow-loop result. A nested
// decision can describe a future mutation candidate, but every execution and
// production-mutation flag remains false.
type Status struct {
	APIVersion                string              `json:"apiVersion"`
	Kind                      string              `json:"kind"`
	Policy                    string              `json:"policy"`
	CellKey                   string              `json:"cellKey"`
	CellID                    string              `json:"cellId"`
	CurrentState              string              `json:"currentState"`
	DesiredState              DesiredState        `json:"desiredState"`
	Action                    reconcile.Action    `json:"action"`
	Reason                    string              `json:"reason"`
	CurrentObservationDigest  string              `json:"currentObservationDigest,omitempty"`
	DesiredPlanDigest         string              `json:"desiredPlanDigest,omitempty"`
	DecisionDigest            string              `json:"decisionDigest,omitempty"`
	Decision                  *reconcile.Decision `json:"decision,omitempty"`
	IdempotencyKey            string              `json:"idempotencyKey"`
	EvaluatedAt               time.Time           `json:"evaluatedAt"`
	Ready                     bool                `json:"ready"`
	Converged                 bool                `json:"converged"`
	LastKnownGoodServing      bool                `json:"lastKnownGoodServing"`
	Stable                    bool                `json:"stable"`
	Blocked                   bool                `json:"blocked"`
	Retryable                 bool                `json:"retryable"`
	MutationCandidate         bool                `json:"mutationCandidate"`
	DeleteAllowed             bool                `json:"deleteAllowed"`
	ObservationOnly           bool                `json:"observationOnly"`
	ExecutionAllowed          bool                `json:"executionAllowed"`
	ProductionMutationAllowed bool                `json:"productionMutationAllowed"`
	Digest                    string              `json:"digest"`
}

// PreparedCycle is an explicit in-process handoff between the pure source
// reconciler and a separately owned candidate validator. Its serialized and
// formatted forms contain only the public Status and digests. A private plan
// is retained only for a valid mutation candidate and is available solely
// through CandidatePlan.
type PreparedCycle struct {
	APIVersion                string    `json:"apiVersion"`
	Kind                      string    `json:"kind"`
	Policy                    string    `json:"policy"`
	CellKey                   string    `json:"cellKey"`
	CellID                    string    `json:"cellId"`
	StatusDigest              string    `json:"statusDigest"`
	Status                    Status    `json:"status"`
	CandidatePlanDigest       string    `json:"candidatePlanDigest,omitempty"`
	CandidateAvailable        bool      `json:"candidateAvailable"`
	IdempotencyKey            string    `json:"idempotencyKey"`
	EvaluatedAt               time.Time `json:"evaluatedAt"`
	DeleteAllowed             bool      `json:"deleteAllowed"`
	ObservationOnly           bool      `json:"observationOnly"`
	ExecutionAllowed          bool      `json:"executionAllowed"`
	ProductionMutationAllowed bool      `json:"productionMutationAllowed"`
	Digest                    string    `json:"digest"`

	candidatePlan materialization.Plan
}

// New ignores and retains none of the cell, sources, or clock while disabled.
// Enabled construction validates interfaces only and performs no source I/O.
func New(config Config) (*Reconciler, error) {
	if !config.Enabled {
		return &Reconciler{}, nil
	}
	identity, err := materialization.SecretIdentityForCell(config.CellKey)
	if err != nil || nilInterface(config.DesiredSource) || nilInterface(config.CurrentSource) {
		return nil, ErrConfig
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return &Reconciler{
		enabled: true,
		cellKey: identity.CellKey,
		cellID:  identity.CellID,
		desired: config.DesiredSource,
		current: config.CurrentSource,
		now:     config.Now,
	}, nil
}

func (reconciler *Reconciler) Enabled() bool {
	return reconciler != nil && reconciler.enabled && reconciler.cellKey != "" && reconciler.cellID != "" &&
		reconciler.desired != nil && reconciler.current != nil && reconciler.now != nil
}

func (reconciler *Reconciler) String() string {
	if reconciler == nil {
		return "backup materializer reconciler <nil>"
	}
	return "backup materializer reconciler [REDACTED]"
}

func (reconciler *Reconciler) GoString() string { return reconciler.String() }

// ReconcileOnce evaluates one cell exactly once. Runtime source failures are
// converted into secret-free, cell-local status. Context cancellation and
// internal contract violations remain explicit errors for the outer process.
func (reconciler *Reconciler) ReconcileOnce(ctx context.Context) (Status, error) {
	status, _, err := reconciler.evaluateOnce(ctx)
	return status, err
}

// PrepareOnce performs the same single source evaluation as ReconcileOnce and
// seals a private candidate-plan handoff only when the resulting status is a
// valid mutation candidate. It never invokes a writer or another capability.
func (reconciler *Reconciler) PrepareOnce(ctx context.Context) (PreparedCycle, error) {
	status, desiredPlan, err := reconciler.evaluateOnce(ctx)
	if err != nil {
		return PreparedCycle{}, err
	}
	prepared, err := prepareCycle(status, desiredPlan)
	if err != nil {
		return PreparedCycle{}, ErrInvariant
	}
	return prepared, nil
}

func (reconciler *Reconciler) evaluateOnce(ctx context.Context) (Status, *materialization.Plan, error) {
	if reconciler == nil {
		return Status{}, nil, ErrConfig
	}
	if !reconciler.Enabled() {
		return Status{}, nil, ErrDisabled
	}
	if ctx == nil {
		return Status{}, nil, ErrConfig
	}
	if err := ctx.Err(); err != nil {
		return Status{}, nil, err
	}
	now := reconciler.now().UTC().Truncate(time.Second)
	if now.IsZero() {
		return Status{}, nil, ErrConfig
	}
	current, err := reconciler.current.Observe(ctx)
	if ctx.Err() != nil {
		return Status{}, nil, ctx.Err()
	}
	if err != nil || reconcile.ValidateObservation(current) != nil || current.CellKey != reconciler.cellKey {
		status := newCurrentUnavailableStatus(reconciler.cellKey, reconciler.cellID, now)
		if ValidateStatus(status) != nil {
			return Status{}, nil, ErrInvariant
		}
		return status, nil, nil
	}

	var desiredPlan *materialization.Plan
	desiredState := DesiredNotRead
	if current.State != reconcile.StateForeign && current.State != reconcile.StateMalformed {
		bundle, fetchErr := reconciler.desired.Fetch(ctx)
		if ctx.Err() != nil {
			return Status{}, nil, ctx.Err()
		}
		if fetchErr != nil {
			desiredState = DesiredUnavailable
		} else {
			plan, buildErr := materialization.Build(bundle, now)
			if buildErr != nil || plan.CellKey != reconciler.cellKey {
				desiredState = DesiredInvalid
			} else {
				desiredState = DesiredAvailable
				desiredPlan = &plan
			}
		}
	}
	decision, err := reconcile.Decide(reconciler.cellKey, desiredPlan, current, now)
	if err != nil {
		return Status{}, nil, ErrInvariant
	}
	status := statusFromDecision(reconciler.cellID, current, desiredState, decision)
	if err := ValidateStatus(status); err != nil {
		return Status{}, nil, err
	}
	return status, desiredPlan, nil
}

func ValidatePreparedCycle(prepared PreparedCycle) error {
	identity, err := materialization.SecretIdentityForCell(prepared.CellKey)
	if err != nil || prepared.APIVersion != PreparedCycleAPIVersion || prepared.Kind != PreparedCycleKind ||
		prepared.Policy != PreparedCyclePolicy || prepared.CellID != identity.CellID || ValidateStatus(prepared.Status) != nil ||
		prepared.Status.CellKey != prepared.CellKey || prepared.Status.Digest != prepared.StatusDigest ||
		prepared.EvaluatedAt != prepared.Status.EvaluatedAt ||
		prepared.CandidateAvailable != prepared.Status.MutationCandidate || prepared.DeleteAllowed ||
		!prepared.ObservationOnly || prepared.ExecutionAllowed || prepared.ProductionMutationAllowed ||
		prepared.IdempotencyKey != preparedCycleIdempotencyKey(prepared) ||
		prepared.Digest != DigestPreparedCycle(prepared) {
		return ErrInvariant
	}
	if prepared.CandidateAvailable {
		if materialization.Validate(prepared.candidatePlan, prepared.EvaluatedAt) != nil ||
			prepared.candidatePlan.CellKey != prepared.CellKey || prepared.CandidatePlanDigest == "" ||
			prepared.CandidatePlanDigest != prepared.candidatePlan.Digest ||
			prepared.CandidatePlanDigest != prepared.Status.DesiredPlanDigest || prepared.Status.Decision == nil ||
			prepared.Status.Decision.DesiredPlanDigest != prepared.CandidatePlanDigest ||
			(prepared.Status.Action != reconcile.ActionCreateIfAbsent &&
				prepared.Status.Action != reconcile.ActionReplaceResourceVersionCAS) {
			return ErrInvariant
		}
		return nil
	}
	if prepared.CandidatePlanDigest != "" || !reflect.DeepEqual(prepared.candidatePlan, materialization.Plan{}) {
		return ErrInvariant
	}
	return nil
}

// CandidatePlan returns a copy of the private plan only after the complete
// prepared-cycle contract validates and only for a mutation candidate. The
// Plan itself keeps raw Secret data private and returns copies through Data.
func (prepared PreparedCycle) CandidatePlan() (materialization.Plan, bool) {
	if ValidatePreparedCycle(prepared) != nil || !prepared.CandidateAvailable {
		return materialization.Plan{}, false
	}
	return prepared.candidatePlan, true
}

func DigestPreparedCycle(prepared PreparedCycle) string {
	prepared.Digest = ""
	document, err := json.Marshal(prepared)
	if err != nil {
		return ""
	}
	return digestBytes(document)
}

func (prepared PreparedCycle) String() string {
	return fmt.Sprintf(
		"BackupObserverSecretPreparedCycle{cell=%q action=%q candidate=%t executionAllowed=false digest=%q}",
		prepared.CellKey, prepared.Status.Action, prepared.CandidateAvailable, prepared.Digest,
	)
}

func (prepared PreparedCycle) GoString() string { return prepared.String() }

func prepareCycle(status Status, desiredPlan *materialization.Plan) (PreparedCycle, error) {
	if ValidateStatus(status) != nil {
		return PreparedCycle{}, ErrInvariant
	}
	statusCopy := clonePreparedStatus(status)
	prepared := PreparedCycle{
		APIVersion: PreparedCycleAPIVersion, Kind: PreparedCycleKind, Policy: PreparedCyclePolicy,
		CellKey: status.CellKey, CellID: status.CellID, StatusDigest: status.Digest, Status: statusCopy,
		CandidateAvailable: status.MutationCandidate, EvaluatedAt: status.EvaluatedAt, ObservationOnly: true,
	}
	if status.MutationCandidate {
		if desiredPlan == nil || materialization.Validate(*desiredPlan, status.EvaluatedAt) != nil ||
			desiredPlan.CellKey != status.CellKey || desiredPlan.Digest != status.DesiredPlanDigest {
			return PreparedCycle{}, ErrInvariant
		}
		prepared.CandidatePlanDigest = desiredPlan.Digest
		prepared.candidatePlan = *desiredPlan
	}
	prepared.IdempotencyKey = preparedCycleIdempotencyKey(prepared)
	prepared.Digest = DigestPreparedCycle(prepared)
	if ValidatePreparedCycle(prepared) != nil {
		return PreparedCycle{}, ErrInvariant
	}
	return prepared, nil
}

func clonePreparedStatus(status Status) Status {
	cloned := status
	if status.Decision != nil {
		decision := *status.Decision
		cloned.Decision = &decision
	}
	return cloned
}

func preparedCycleIdempotencyKey(prepared PreparedCycle) string {
	if prepared.CellID == "" || !validDigest(prepared.StatusDigest) ||
		(prepared.CandidatePlanDigest != "" && !validDigest(prepared.CandidatePlanDigest)) {
		return ""
	}
	basis := struct {
		CellKey             string `json:"cellKey"`
		StatusDigest        string `json:"statusDigest"`
		CandidatePlanDigest string `json:"candidatePlanDigest,omitempty"`
	}{
		CellKey: prepared.CellKey, StatusDigest: prepared.StatusDigest,
		CandidatePlanDigest: prepared.CandidatePlanDigest,
	}
	document, err := json.Marshal(basis)
	if err != nil {
		return ""
	}
	return "backup-materializer-candidate-handoff/" + prepared.CellID + "/" +
		strings.TrimPrefix(digestBytes(document), "sha256:")
}

func ValidateStatus(status Status) error {
	identity, err := materialization.SecretIdentityForCell(status.CellKey)
	if err != nil || status.APIVersion != APIVersion || status.Kind != Kind || status.Policy != Policy ||
		status.CellID != identity.CellID || !canonicalTime(status.EvaluatedAt) ||
		status.DeleteAllowed || !status.ObservationOnly || status.ExecutionAllowed || status.ProductionMutationAllowed ||
		status.IdempotencyKey != idempotencyKey(status) || status.Digest != DigestStatus(status) {
		return ErrInvariant
	}
	if status.CurrentState == CurrentUnavailable {
		if status.DesiredState != DesiredNotRead || status.Action != reconcile.ActionBlock ||
			status.Reason != ReasonCurrentObservationUnavailable || status.CurrentObservationDigest != "" ||
			status.DesiredPlanDigest != "" || status.DecisionDigest != "" || status.Decision != nil ||
			status.Ready || status.Converged || status.LastKnownGoodServing || status.Stable || !status.Blocked ||
			!status.Retryable || status.MutationCandidate {
			return ErrInvariant
		}
		return nil
	}
	if (status.CurrentState != string(reconcile.StateAbsent) && status.CurrentState != string(reconcile.StateManaged) &&
		status.CurrentState != string(reconcile.StateForeign) && status.CurrentState != string(reconcile.StateMalformed)) ||
		!validDigest(status.CurrentObservationDigest) || status.Decision == nil ||
		reconcile.ValidateDecision(*status.Decision) != nil || status.Decision.DecidedAt != status.EvaluatedAt ||
		status.Decision.CellKey != status.CellKey || status.Action != status.Decision.Action ||
		status.Reason != string(status.Decision.Reason) || status.DesiredPlanDigest != status.Decision.DesiredPlanDigest ||
		status.DecisionDigest != status.Decision.Digest || status.Blocked != status.Decision.Blocked ||
		status.Stable != status.Decision.Stable || status.MutationCandidate != status.Decision.MutationCandidate {
		return ErrInvariant
	}
	if (status.CurrentState == string(reconcile.StateForeign) || status.CurrentState == string(reconcile.StateMalformed)) &&
		status.DesiredState != DesiredNotRead {
		return ErrInvariant
	}
	if !validStateDecision(status) {
		return ErrInvariant
	}
	switch status.DesiredState {
	case DesiredNotRead:
		if status.CurrentState != string(reconcile.StateForeign) && status.CurrentState != string(reconcile.StateMalformed) ||
			status.DesiredPlanDigest != "" || status.Retryable {
			return ErrInvariant
		}
	case DesiredAvailable:
		if !validDigest(status.DesiredPlanDigest) || status.Retryable {
			return ErrInvariant
		}
	case DesiredUnavailable, DesiredInvalid:
		if status.DesiredPlanDigest != "" || !status.Retryable {
			return ErrInvariant
		}
	default:
		return ErrInvariant
	}
	wantReady := status.Action == reconcile.ActionNoop || status.Action == reconcile.ActionRetainLastKnownGood
	wantConverged := status.Action == reconcile.ActionNoop
	wantLKG := status.Action == reconcile.ActionRetainLastKnownGood
	if status.Ready != wantReady || status.Converged != wantConverged || status.LastKnownGoodServing != wantLKG {
		return ErrInvariant
	}
	return nil
}

func validStateDecision(status Status) bool {
	switch status.CurrentState {
	case string(reconcile.StateAbsent):
		switch status.DesiredState {
		case DesiredAvailable:
			return status.Action == reconcile.ActionCreateIfAbsent && status.Reason == string(reconcile.ReasonDesiredGenerationReady)
		case DesiredUnavailable, DesiredInvalid:
			return status.Action == reconcile.ActionBlock && status.Reason == string(reconcile.ReasonSourceUnavailableNoLKG)
		}
	case string(reconcile.StateManaged):
		switch status.DesiredState {
		case DesiredAvailable:
			return status.Action == reconcile.ActionNoop || status.Action == reconcile.ActionReplaceResourceVersionCAS
		case DesiredUnavailable, DesiredInvalid:
			return status.Action == reconcile.ActionRetainLastKnownGood ||
				(status.Action == reconcile.ActionBlock &&
					(status.Reason == string(reconcile.ReasonLastKnownGoodExpired) ||
						status.Reason == string(reconcile.ReasonLastKnownGoodUnavailable)))
		}
	case string(reconcile.StateForeign):
		return status.DesiredState == DesiredNotRead && status.Action == reconcile.ActionBlock &&
			status.Reason == string(reconcile.ReasonCurrentObjectForeign)
	case string(reconcile.StateMalformed):
		return status.DesiredState == DesiredNotRead && status.Action == reconcile.ActionBlock &&
			status.Reason == string(reconcile.ReasonCurrentObjectMalformed)
	}
	return false
}

func DigestStatus(status Status) string {
	status.Digest = ""
	document, err := json.Marshal(status)
	if err != nil {
		return ""
	}
	return digestBytes(document)
}

func (status Status) String() string {
	return fmt.Sprintf(
		"BackupObserverSecretReconcileStatus{cell=%q current=%q desired=%q action=%q reason=%q ready=%t executionAllowed=false digest=%q}",
		status.CellKey, status.CurrentState, status.DesiredState, status.Action, status.Reason, status.Ready, status.Digest,
	)
}

func (status Status) GoString() string { return status.String() }

func newCurrentUnavailableStatus(cellKey, cellID string, now time.Time) Status {
	status := Status{
		APIVersion:                APIVersion,
		Kind:                      Kind,
		Policy:                    Policy,
		CellKey:                   cellKey,
		CellID:                    cellID,
		CurrentState:              CurrentUnavailable,
		DesiredState:              DesiredNotRead,
		Action:                    reconcile.ActionBlock,
		Reason:                    ReasonCurrentObservationUnavailable,
		EvaluatedAt:               now,
		Blocked:                   true,
		Retryable:                 true,
		ObservationOnly:           true,
		DeleteAllowed:             false,
		ExecutionAllowed:          false,
		ProductionMutationAllowed: false,
	}
	status.IdempotencyKey = idempotencyKey(status)
	status.Digest = DigestStatus(status)
	return status
}

func statusFromDecision(cellID string, current reconcile.Observation, desiredState DesiredState, decision reconcile.Decision) Status {
	decisionCopy := decision
	status := Status{
		APIVersion:                APIVersion,
		Kind:                      Kind,
		Policy:                    Policy,
		CellKey:                   current.CellKey,
		CellID:                    cellID,
		CurrentState:              string(current.State),
		DesiredState:              desiredState,
		Action:                    decision.Action,
		Reason:                    string(decision.Reason),
		CurrentObservationDigest:  current.Digest,
		DesiredPlanDigest:         decision.DesiredPlanDigest,
		DecisionDigest:            decision.Digest,
		Decision:                  &decisionCopy,
		EvaluatedAt:               decision.DecidedAt,
		Ready:                     decision.Action == reconcile.ActionNoop || decision.Action == reconcile.ActionRetainLastKnownGood,
		Converged:                 decision.Action == reconcile.ActionNoop,
		LastKnownGoodServing:      decision.Action == reconcile.ActionRetainLastKnownGood,
		Stable:                    decision.Stable,
		Blocked:                   decision.Blocked,
		Retryable:                 desiredState == DesiredUnavailable || desiredState == DesiredInvalid,
		MutationCandidate:         decision.MutationCandidate,
		ObservationOnly:           true,
		DeleteAllowed:             false,
		ExecutionAllowed:          false,
		ProductionMutationAllowed: false,
	}
	status.IdempotencyKey = idempotencyKey(status)
	status.Digest = DigestStatus(status)
	return status
}

func idempotencyKey(status Status) string {
	basis := struct {
		CellKey                  string           `json:"cellKey"`
		CurrentState             string           `json:"currentState"`
		DesiredState             DesiredState     `json:"desiredState"`
		Action                   reconcile.Action `json:"action"`
		Reason                   string           `json:"reason"`
		CurrentObservationDigest string           `json:"currentObservationDigest"`
		DesiredPlanDigest        string           `json:"desiredPlanDigest"`
	}{
		CellKey: status.CellKey, CurrentState: status.CurrentState, DesiredState: status.DesiredState,
		Action: status.Action, Reason: status.Reason, CurrentObservationDigest: status.CurrentObservationDigest,
		DesiredPlanDigest: status.DesiredPlanDigest,
	}
	document, err := json.Marshal(basis)
	if err != nil || status.CellID == "" {
		return ""
	}
	return "backup-materializer-cycle/" + status.CellID + "/" + strings.TrimPrefix(digestBytes(document), "sha256:")
}

func validDigest(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil && strings.ToLower(value) == value
}

func digestBytes(value []byte) string {
	digest := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(digest[:])
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
