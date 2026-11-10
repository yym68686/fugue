package inputarchive

import (
	"context"
	"errors"
	"testing"
	"time"
)

type budgetTestAdmission struct {
	reservation PendingRootReservation
}

func (a budgetTestAdmission) ReservePendingRoot(context.Context, PendingRootRequest) (PendingRootReservation, error) {
	return a.reservation, nil
}

type budgetTestReservation struct {
	budget  PendingRootWholeOperationBudget
	check   error
	outcome []PendingRootOutcome
}

type legacyBudgetTestReservation struct {
	outcome []PendingRootOutcome
}

func (r *legacyBudgetTestReservation) CheckPendingRootReservation(context.Context) error { return nil }
func (r *legacyBudgetTestReservation) RecordPendingRootOutcome(outcome PendingRootOutcome) {
	r.outcome = append(r.outcome, outcome)
}

func (r *budgetTestReservation) CheckPendingRootReservation(context.Context) error { return r.check }
func (r *budgetTestReservation) RecordPendingRootOutcome(outcome PendingRootOutcome) {
	r.outcome = append(r.outcome, outcome)
}
func (r *budgetTestReservation) PendingRootWholeOperationBudget(context.Context) (PendingRootWholeOperationBudget, error) {
	return r.budget, nil
}

func budgetTestRequest(t *testing.T) (PendingRootRequest, string) {
	t.Helper()
	request := PendingRootRequest{
		Schema:       PendingRootRequestSchema,
		Registration: PendingRootRegistration{Schema: PendingRootSchema, Owner: "owner", Generation: "generation", SourceDigest: "sha256:" + "a" + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", References: []HotReference{{Kind: "release", ID: "release"}}},
		SourceSchema: "synthetic-source",
		Source:       []byte(`{"source":"test"}`),
	}
	_, hash, err := request.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	return request, hash
}

func budgetTestLimits() PendingRootWholeOperationBudgetLimits {
	return PendingRootWholeOperationBudgetLimits{
		MaxNewPhysicalAllocationBoundBytes: 1 << 30,
		MaxPeakWALBytes:                    1 << 30,
		MaxPeakTempBytes:                   1 << 30,
		MaxGrowthDuringBoundedWaitBytes:    1 << 30,
		MaxFailureExitAllocationBytes:      1 << 30,
		MaxWaitAndExitTimeBoundSeconds:     540,
		MaxReservationLifetime:             5 * time.Minute,
	}
}

func budgetTestProof(hash string, expires time.Time) PendingRootWholeOperationBudget {
	return PendingRootWholeOperationBudget{
		Schema:           PendingRootWholeOperationBudgetSchema,
		RequestSHA256:    hash,
		ReservationEpoch: 7,
		ExpiresAt:        expires,
		Proven:           true,
		Bounds: PendingRootWholeOperationBounds{
			NewPhysicalAllocationBoundBytes: 512,
			PeakWALBytes:                    512,
			PeakTempBytes:                   512,
			GrowthDuringBoundedWaitBytes:    512,
			FailureExitAllocationBytes:      512,
			WaitAndExitTimeBoundSeconds:     120,
		},
	}
}

func TestPendingRootWholeOperationBudgetRequiresAllSixProvenBounds(t *testing.T) {
	request, hash := budgetTestRequest(t)
	now := time.Unix(1_700_000_000, 0)
	limits := budgetTestLimits()
	valid := budgetTestProof(hash, now.Add(time.Minute))
	if err := valid.validate(now, hash, 7, limits); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*PendingRootWholeOperationBudget){
		"unproven": func(b *PendingRootWholeOperationBudget) { b.Proven = false },
		"wrong-request": func(b *PendingRootWholeOperationBudget) {
			b.RequestSHA256 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		},
		"wrong-epoch":   func(b *PendingRootWholeOperationBudget) { b.ReservationEpoch = 8 },
		"expired":       func(b *PendingRootWholeOperationBudget) { b.ExpiresAt = now },
		"missing-bound": func(b *PendingRootWholeOperationBudget) { b.Bounds.PeakTempBytes = 0 },
		"over-cap":      func(b *PendingRootWholeOperationBudget) { b.Bounds.PeakWALBytes = limits.MaxPeakWALBytes + 1 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			mutate(&candidate)
			if err := candidate.validate(now, hash, 7, limits); !errors.Is(err, ErrPendingRootWholeOperationBudget) {
				t.Fatalf("validate error = %v", err)
			}
		})
	}
	_ = request
}

func TestStrictPendingRootAdmissionRejectsLegacyReservationAndRecordsNotAttempted(t *testing.T) {
	request, _ := budgetTestRequest(t)
	legacy := &legacyBudgetTestReservation{}
	strict := StrictPendingRootAdmission{Inner: budgetTestAdmission{reservation: legacy}, Limits: budgetTestLimits(), ReservationEpoch: 7, Now: func() time.Time { return time.Unix(1_700_000_000, 0) }}
	if _, err := strict.ReservePendingRoot(context.Background(), request); !errors.Is(err, ErrPendingRootWholeOperationBudgetUnset) {
		t.Fatalf("ReservePendingRoot error = %v", err)
	}
	if len(legacy.outcome) != 1 || legacy.outcome[0] != PendingRootNotAttempted {
		t.Fatalf("outcomes = %#v", legacy.outcome)
	}
}

func TestStrictPendingRootAdmissionRechecksBudgetBeforeUse(t *testing.T) {
	request, hash := budgetTestRequest(t)
	now := time.Unix(1_700_000_000, 0)
	clock := now
	reservation := &budgetTestReservation{budget: budgetTestProof(hash, now.Add(time.Minute))}
	strict := StrictPendingRootAdmission{Inner: budgetTestAdmission{reservation: reservation}, Limits: budgetTestLimits(), ReservationEpoch: 7, Now: func() time.Time { return clock }}
	wrapped, err := strict.ReservePendingRoot(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if err := wrapped.CheckPendingRootReservation(context.Background()); err != nil {
		t.Fatal(err)
	}
	clock = now.Add(2 * time.Minute)
	if err := wrapped.CheckPendingRootReservation(context.Background()); !errors.Is(err, ErrPendingRootWholeOperationBudget) {
		t.Fatalf("expired check error = %v", err)
	}
	if len(reservation.outcome) != 0 {
		t.Fatalf("wrapper recorded outcome before caller decided: %#v", reservation.outcome)
	}
}

func TestStrictPendingRootAdmissionRejectsBudgetDrift(t *testing.T) {
	request, hash := budgetTestRequest(t)
	now := time.Unix(1_700_000_000, 0)
	reservation := &budgetTestReservation{budget: budgetTestProof(hash, now.Add(time.Minute))}
	strict := StrictPendingRootAdmission{Inner: budgetTestAdmission{reservation: reservation}, Limits: budgetTestLimits(), ReservationEpoch: 7, Now: func() time.Time { return now }}
	wrapper, err := strict.ReservePendingRoot(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	reservation.budget.Bounds.PeakTempBytes++
	if err := wrapper.CheckPendingRootReservation(context.Background()); !errors.Is(err, ErrPendingRootWholeOperationBudget) {
		t.Fatalf("budget drift error = %v", err)
	}
	if len(reservation.outcome) != 0 {
		t.Fatalf("wrapper recorded outcome before caller decided: %#v", reservation.outcome)
	}
}

func TestPendingRootWholeOperationBudgetExplicitZeroTemp(t *testing.T) {
	_, hash := budgetTestRequest(t)
	now := time.Unix(1_700_000_000, 0)
	limits := budgetTestLimits()
	limits.MaxPeakTempBytes = 0
	proof := budgetTestProof(hash, now.Add(time.Minute))
	proof.Bounds.PeakTempBytes = 0
	if err := proof.validate(now, hash, 7, limits); err == nil {
		t.Fatal("missing temp bound accepted as proven zero")
	}
	proof.TempFilesForbidden = true
	if err := proof.validate(now, hash, 7, limits); err != nil {
		t.Fatal("explicit guarded zero-temp operation refused", err)
	}
	proof.Bounds.PeakTempBytes = 1
	limits.MaxPeakTempBytes = 10
	if err := proof.validate(now, hash, 7, limits); err == nil {
		t.Fatal("contradictory zero-temp assertion accepted")
	}
	proof.TempFilesForbidden = false
	if err := proof.validate(now, hash, 7, limits); err != nil {
		t.Fatal("bounded nonzero-temp operation refused", err)
	}
}
