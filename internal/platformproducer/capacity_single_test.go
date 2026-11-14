package platformproducer

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func singleRecoveryFixture(t *testing.T) (OfflineEdge, OfflineRecoveryObservation, time.Time) {
	t.Helper()
	h := capacityHold()
	h.Recovery.Capacity.Version, h.Recovery.Capacity.MinimumStableSeconds = CapacitySingleVersion, 0
	h.Recovery.SuccessThreshold, h.Recovery.FailureThreshold = 1, 1
	if err := h.Recovery.Validate(); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	e := capacityEvidence(at)
	o := OfflineRecoveryObservation{}.RecordCapacity(h, at, true, at.Add(time.Minute), &e, "capacity_verified")
	return h, o, at
}

func TestSingleRecoveryAdmitsOneVerifiedRoundAndSurvivesRestartAndCollectorGaps(t *testing.T) {
	h, o, at := singleRecoveryFixture(t)
	if !o.Ready(h, at) || o.Successes != 1 {
		t.Fatal("single complete round did not restore eligibility", o)
	}
	for _, result := range []string{"reference_unavailable", "evidence_unavailable", "code_observation_unavailable"} {
		later := at.Add(90 * 24 * time.Hour)
		got := o.RecordCapacity(h, later, false, time.Time{}, nil, result)
		raw, _ := json.Marshal(got)
		var restarted OfflineRecoveryObservation
		if err := json.Unmarshal(raw, &restarted); err != nil {
			t.Fatal(err)
		}
		if !restarted.Ready(h, later) || !restarted.ReviewOverdue(h, later) || !restarted.LastSuccess.Equal(at) || !restarted.ProofValidUntil.Equal(o.ProofValidUntil) {
			t.Fatal("collector gap erased recovery or renewed a proof", result, restarted)
		}
	}
	if replay := o.RecordCapacity(h, at, true, o.ProofValidUntil, o.Capacity, "capacity_verified"); !reflect.DeepEqual(replay, o) {
		t.Fatal("duplicate result changed checkpoint")
	}
}

func TestSingleRecoveryRetractsOnNegativeAndCannotBorrowAnotherIdentityOrDeclaration(t *testing.T) {
	h, o, at := singleRecoveryFixture(t)
	for _, result := range []string{"capacity_unavailable", "capacity_below_policy", "code_identity_conflict", "code_not_current", "route_unavailable", ""} {
		later := at.Add(time.Hour)
		failed := o.RecordCapacity(h, later, false, time.Time{}, nil, result)
		if failed.Ready(h, later) || failed.Admitted {
			t.Fatal("old success hid a new negative", result)
		}
		inconclusive := failed.RecordCapacity(h, later.Add(time.Hour), false, time.Time{}, nil, "reference_unavailable")
		if inconclusive.Ready(h, later.Add(time.Hour)) {
			t.Fatal("collector failure resurrected a retracted success")
		}
		e := capacityEvidence(later.Add(2 * time.Hour))
		e.Code.WorkerPodUID = "verified-replacement"
		recovered := inconclusive.RecordCapacity(h, e.CheckedAt, true, e.CheckedAt.Add(time.Minute), &e, "capacity_verified")
		if !recovered.Ready(h, e.CheckedAt) || recovered.Successes != 1 {
			t.Fatal("new complete round failed to recover replacement", recovered)
		}
	}
	changed := h
	changed.PublicIPv4 = "8.8.4.4"
	if o.Ready(changed, at) || o.Ready(h, at.Add(-time.Second)) {
		t.Fatal("recovery crossed endpoint identity or clock boundary")
	}
	for _, bad := range []func(*OfflineRecoveryObservation){
		func(x *OfflineRecoveryObservation) { x.Successes = 0 },
		func(x *OfflineRecoveryObservation) { x.FirstSuccess = time.Time{} },
		func(x *OfflineRecoveryObservation) { x.LastResult = "capacity_below_policy" },
		func(x *OfflineRecoveryObservation) { x.Capacity = nil },
	} {
		x := o
		bad(&x)
		if x.Ready(h, at) {
			t.Fatal("malformed recovery admitted")
		}
	}
}

func TestSingleRecoveryRejectsPartialTransferAndImplicitLegacyPolicyChanges(t *testing.T) {
	h, _, at := singleRecoveryFixture(t)
	e := capacityEvidence(at)
	e.Bytes--
	if o := (OfflineRecoveryObservation{}).RecordCapacity(h, at, true, at.Add(time.Minute), &e, "capacity_verified"); o.Ready(h, at) {
		t.Fatal("partial transfer admitted")
	}
	for _, version := range []string{CapacityRecoveryVersion, CapacityContinuityVersion} {
		legacy := capacityHold()
		legacy.Recovery.Capacity.Version = version
		if version == CapacityContinuityVersion {
			legacy.Recovery.Capacity.RetainAdmissionSeconds = 86400
		}
		legacy.Recovery.SuccessThreshold = 1
		if legacy.Recovery.Validate() == nil {
			t.Fatal("legacy multi-round semantics changed", version)
		}
	}
	h.Recovery.Capacity.MinimumStableSeconds = 600
	if h.Recovery.Validate() == nil {
		t.Fatal("single recovery accepted a hidden stable-window requirement")
	}
}

func TestSingleRecoveryDoesNotDelayNegativeEvidenceUntilNextScheduledRound(t *testing.T) {
	h, success, at := singleRecoveryFixture(t)
	failedAt := at.Add(time.Second)
	failed := success.RecordCapacity(h, failedAt, false, time.Time{}, nil, "code_identity_conflict")
	if failed.Admitted || failed.Ready(h, failedAt) || failed.Failures != 1 || !failed.LastAttempt.Equal(failedAt) {
		t.Fatal("probe cadence hid a current identity conflict", failed)
	}
	// A late completion from the previous successful round must not overwrite
	// the newer failure, even though its evidence was originally valid.
	late := failed.RecordCapacity(h, at, true, success.ProofValidUntil, success.Capacity, "capacity_verified")
	if !reflect.DeepEqual(late, failed) {
		t.Fatal("out-of-order success resurrected a failed endpoint", late)
	}
}
