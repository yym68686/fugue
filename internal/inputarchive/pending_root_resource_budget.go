package inputarchive

import (
	"context"
	"errors"
	"reflect"
	"time"
)

const PendingRootWholeOperationBudgetSchema = "fugue.compiler-input.pending-root-whole-operation-budget.v1"

var (
	ErrPendingRootWholeOperationBudget      = errors.New("compiler input pending root whole-operation budget invalid")
	ErrPendingRootWholeOperationBudgetUnset = errors.New("compiler input pending root whole-operation budget is not proven")
)

// PendingRootWholeOperationBounds are the six independent bounds that must be
// reserved before a pending-root operation can start. They cover the entire
// operation, including bounded waits and failure cleanup, rather than only the
// successful payload.
type PendingRootWholeOperationBounds struct {
	NewPhysicalAllocationBoundBytes uint64
	PeakWALBytes                    uint64
	PeakTempBytes                   uint64
	GrowthDuringBoundedWaitBytes    uint64
	FailureExitAllocationBytes      uint64
	WaitAndExitTimeBoundSeconds     uint64
}

// PendingRootWholeOperationBudget is a trusted, operation-specific proof
// returned by the admission provider. It is deliberately not derived from a
// caller's request body or from a point-in-time available-space observation.
type PendingRootWholeOperationBudget struct {
	Schema           string
	RequestSHA256    string
	ReservationEpoch uint64
	ExpiresAt        time.Time
	Proven           bool
	// TempFilesForbidden attests that the complete operation enforces the
	// transaction-local zero-temp guard. An omitted bound is not proven zero.
	TempFilesForbidden bool
	Bounds             PendingRootWholeOperationBounds
}

// PendingRootWholeOperationBudgetLimits are process policy, not capacity
// observations. A strict admission must configure every maximum explicitly.
type PendingRootWholeOperationBudgetLimits struct {
	MaxNewPhysicalAllocationBoundBytes uint64
	MaxPeakWALBytes                    uint64
	MaxPeakTempBytes                   uint64
	MaxGrowthDuringBoundedWaitBytes    uint64
	MaxFailureExitAllocationBytes      uint64
	MaxWaitAndExitTimeBoundSeconds     uint64
	MaxReservationLifetime             time.Duration
}

func (l PendingRootWholeOperationBudgetLimits) valid() bool {
	return l.MaxNewPhysicalAllocationBoundBytes > 0 &&
		l.MaxPeakWALBytes > 0 &&
		l.MaxGrowthDuringBoundedWaitBytes > 0 &&
		l.MaxFailureExitAllocationBytes > 0 &&
		l.MaxWaitAndExitTimeBoundSeconds > 0 &&
		l.MaxReservationLifetime > 0
}

func (b PendingRootWholeOperationBudget) validate(now time.Time, requestHash string, epoch uint64, limits PendingRootWholeOperationBudgetLimits) error {
	if !limits.valid() || b.Schema != PendingRootWholeOperationBudgetSchema || !b.Proven ||
		!pendingRootHash.MatchString(b.RequestSHA256) || b.RequestSHA256 != requestHash ||
		b.ReservationEpoch == 0 || b.ReservationEpoch != epoch || b.ExpiresAt.IsZero() || !b.ExpiresAt.After(now) ||
		b.ExpiresAt.Sub(now) > limits.MaxReservationLifetime {
		return ErrPendingRootWholeOperationBudget
	}
	if b.Bounds.NewPhysicalAllocationBoundBytes == 0 || b.Bounds.NewPhysicalAllocationBoundBytes > limits.MaxNewPhysicalAllocationBoundBytes ||
		b.Bounds.PeakWALBytes == 0 || b.Bounds.PeakWALBytes > limits.MaxPeakWALBytes ||
		b.Bounds.PeakTempBytes > limits.MaxPeakTempBytes ||
		(b.Bounds.PeakTempBytes == 0) != b.TempFilesForbidden ||
		b.Bounds.GrowthDuringBoundedWaitBytes == 0 || b.Bounds.GrowthDuringBoundedWaitBytes > limits.MaxGrowthDuringBoundedWaitBytes ||
		b.Bounds.FailureExitAllocationBytes == 0 || b.Bounds.FailureExitAllocationBytes > limits.MaxFailureExitAllocationBytes ||
		b.Bounds.WaitAndExitTimeBoundSeconds == 0 || b.Bounds.WaitAndExitTimeBoundSeconds > limits.MaxWaitAndExitTimeBoundSeconds {
		return ErrPendingRootWholeOperationBudget
	}
	return nil
}

// ValidatePendingRootWholeOperationBudget is also used by durable admission
// providers. It validates a trusted proof; it cannot establish its bounds or
// physical-resource provenance from observations or a caller assertion.
func ValidatePendingRootWholeOperationBudget(b PendingRootWholeOperationBudget, now time.Time, requestHash string, epoch uint64, limits PendingRootWholeOperationBudgetLimits) error {
	return b.validate(now, requestHash, epoch, limits)
}

// PendingRootWholeOperationBudgetReservation is an optional extension to the
// legacy reservation interface. StrictPendingRootAdmission requires it.
type PendingRootWholeOperationBudgetReservation interface {
	PendingRootReservation
	PendingRootWholeOperationBudget(context.Context) (PendingRootWholeOperationBudget, error)
}

// StrictPendingRootAdmission wraps a trusted admission provider and makes the
// six-bound proof mandatory. It is opt-in so existing synthetic/local
// providers cannot be mistaken for a production resource reservation.
type StrictPendingRootAdmission struct {
	Inner            PendingRootAdmission
	Limits           PendingRootWholeOperationBudgetLimits
	ReservationEpoch uint64
	Now              func() time.Time
}

func (a StrictPendingRootAdmission) ReservePendingRoot(ctx context.Context, request PendingRootRequest) (PendingRootReservation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if a.Inner == nil || a.ReservationEpoch == 0 || !a.Limits.valid() {
		return nil, ErrPendingRootWholeOperationBudgetUnset
	}
	now := time.Now
	if a.Now != nil {
		now = a.Now
	}
	_, requestHash, err := request.Canonical()
	if err != nil {
		return nil, err
	}
	reservation, err := a.Inner.ReservePendingRoot(ctx, request)
	if err != nil {
		return nil, err
	}
	if reservation == nil {
		return nil, ErrPendingRootWholeOperationBudgetUnset
	}
	budgeted, ok := reservation.(PendingRootWholeOperationBudgetReservation)
	if !ok {
		reservation.RecordPendingRootOutcome(PendingRootNotAttempted)
		return nil, ErrPendingRootWholeOperationBudgetUnset
	}
	budget, err := budgeted.PendingRootWholeOperationBudget(ctx)
	if err != nil {
		reservation.RecordPendingRootOutcome(PendingRootNotAttempted)
		return nil, err
	}
	if err := budget.validate(now(), requestHash, a.ReservationEpoch, a.Limits); err != nil {
		reservation.RecordPendingRootOutcome(PendingRootNotAttempted)
		return nil, err
	}
	return &strictPendingRootReservation{inner: budgeted, budget: budget, requestHash: requestHash, epoch: a.ReservationEpoch, limits: a.Limits, now: now}, nil
}

type strictPendingRootReservation struct {
	inner       PendingRootWholeOperationBudgetReservation
	budget      PendingRootWholeOperationBudget
	requestHash string
	epoch       uint64
	limits      PendingRootWholeOperationBudgetLimits
	now         func() time.Time
}

func (r *strictPendingRootReservation) CheckPendingRootReservation(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	budget, err := r.inner.PendingRootWholeOperationBudget(ctx)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(budget, r.budget) {
		return ErrPendingRootWholeOperationBudget
	}
	if err := budget.validate(r.now(), r.requestHash, r.epoch, r.limits); err != nil {
		return err
	}
	return r.inner.CheckPendingRootReservation(ctx)
}

func (r *strictPendingRootReservation) RecordPendingRootOutcome(outcome PendingRootOutcome) {
	r.inner.RecordPendingRootOutcome(outcome)
}

func (r *strictPendingRootReservation) PendingRootWholeOperationBudget(ctx context.Context) (PendingRootWholeOperationBudget, error) {
	if err := ctx.Err(); err != nil {
		return PendingRootWholeOperationBudget{}, err
	}
	budget, err := r.inner.PendingRootWholeOperationBudget(ctx)
	if err != nil {
		return PendingRootWholeOperationBudget{}, err
	}
	if !reflect.DeepEqual(budget, r.budget) {
		return PendingRootWholeOperationBudget{}, ErrPendingRootWholeOperationBudget
	}
	if err := budget.validate(r.now(), r.requestHash, r.epoch, r.limits); err != nil {
		return PendingRootWholeOperationBudget{}, err
	}
	return budget, nil
}
