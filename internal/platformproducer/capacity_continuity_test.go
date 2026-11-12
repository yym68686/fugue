package platformproducer

import (
	"encoding/json"
	"testing"
	"time"
)

func continuityRecoveryFixture() (OfflineEdge, OfflineRecoveryObservation, time.Time) {
	h := capacityHold()
	h.Recovery.Capacity.Version = CapacityContinuityVersion
	h.Recovery.Capacity.RetainAdmissionSeconds = 86400
	base := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	o := OfflineRecoveryObservation{}
	for i := 0; i < 3; i++ {
		at := base.Add(time.Duration(i) * 5 * time.Minute)
		e := capacityEvidence(at)
		o = o.RecordCapacity(h, at, true, at.Add(10*time.Minute), &e, "capacity_verified")
	}
	return h, o, base.Add(10 * time.Minute)
}

func TestRecoveryContinuitySurvivesObservationGapAndCheckpointRestart(t *testing.T) {
	h, original, success := continuityRecoveryFixture()
	if !original.Ready(h, success) {
		t.Fatal("recovery fixture did not complete", original)
	}
	for _, result := range []string{"reference_unavailable", "evidence_unavailable", "code_observation_unavailable"} {
		at := success.Add(2 * time.Hour)
		o := original.RecordCapacity(h, at, false, time.Time{}, nil, result)
		raw, _ := json.Marshal(o)
		var restarted OfflineRecoveryObservation
		if err := json.Unmarshal(raw, &restarted); err != nil {
			t.Fatal(err)
		}
		if !restarted.Ready(h, at) || !restarted.ReviewOverdue(h, at) || !restarted.LastSuccess.Equal(success) || !restarted.ProofValidUntil.Equal(original.ProofValidUntil) {
			t.Fatal("collector failure erased recovery or renewed old evidence", result, restarted)
		}
		if restarted.Ready(h, success.Add(24*time.Hour+time.Nanosecond)) {
			t.Fatal("recovered admission exceeded explicit retention")
		}
	}
}

func TestRecoveryContinuityRetractsForNewNegativeOrChangedIdentity(t *testing.T) {
	h, original, success := continuityRecoveryFixture()
	for _, result := range []string{"capacity_unavailable", "capacity_below_policy", "code_identity_conflict", "route_not_verified", ""} {
		at := success.Add(time.Hour)
		o := original.RecordCapacity(h, at, false, time.Time{}, nil, result)
		if o.Ready(h, at) || o.Admitted || o.Successes != 0 {
			t.Fatal("new negative hidden by last good evidence", result, o)
		}
	}
	at := success.Add(time.Hour)
	e := capacityEvidence(at)
	e.Code.WorkerPodUID = "replacement"
	o := original.RecordCapacity(h, at, true, at.Add(time.Hour), &e, "capacity_verified")
	if o.Admitted || o.Successes != 1 {
		t.Fatal("replacement inherited completed recovery", o)
	}
	partial := OfflineRecoveryObservation{}.RecordCapacity(h, at, true, at.Add(time.Hour), &e, "capacity_verified")
	partial = partial.RecordCapacity(h, at.Add(time.Hour), false, time.Time{}, nil, "reference_unavailable")
	if partial.Admitted || partial.Successes != 0 {
		t.Fatal("incomplete recovery gained continuity", partial)
	}
}

func TestRecoveryContinuityCannotExtendUndeclaredOrMalformedState(t *testing.T) {
	h, original, success := continuityRecoveryFixture()
	changed := h
	copy := *h.Recovery
	capacity := *copy.Capacity
	copy.Capacity, changed.Recovery = &capacity, &copy
	capacity.RetainAdmissionSeconds++
	if original.Ready(changed, success) {
		t.Fatal("changed signed policy reused earlier admission")
	}
	for _, result := range []string{"", "capacity_below_policy"} {
		o := original
		o.LastResult = result
		if o.Ready(h, success) {
			t.Fatal("malformed retained checkpoint accepted")
		}
	}
	h.Recovery.Capacity.RetainAdmissionSeconds = h.Recovery.FreshnessSeconds - 1
	if h.Recovery.Validate() == nil {
		t.Fatal("retention below freshness accepted")
	}
}
