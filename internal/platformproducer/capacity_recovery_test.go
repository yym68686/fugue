package platformproducer

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

func capacityHold() OfflineEdge {
	h := recoveryHold()
	h.Recovery.Capacity = &CapacityRecoveryPolicy{Version: CapacityRecoveryVersion, TrafficClass: "streaming", ReferenceEdgeIDs: []string{"edge-b"}, MinimumBytesPerSecond: 262144, MaxElapsedMilliseconds: 5000, MinimumStableSeconds: 600}
	return h
}

func capacityEvidence(at time.Time) CapacityRecoveryEvidence {
	return CapacityRecoveryEvidence{CheckedAt: at, ReportDigest: "sha256:" + strings.Repeat("a", 64), ReferenceEdgeID: "edge-b", Bytes: 1 << 20, ElapsedMilliseconds: 2000, BytesPerSecond: 524288,
		Code: CapacityCodeIdentity{NodeUID: "node-uid", FrontPodUID: "front-uid", WorkerPodUID: "worker-uid", ServiceUID: "service-uid", AuthorityUID: "authority-uid", AuthorityRecordDigest: "sha256:" + strings.Repeat("c", 64), WorkerSourceSHA: strings.Repeat("d", 40), WorkerImageDigest: "sha256:" + strings.Repeat("e", 64), ProxyImageDigest: "sha256:" + strings.Repeat("f", 64), FrontImageDigest: "sha256:" + strings.Repeat("f", 64), Slot: "a", Fence: 1, Bundle: "bundle-a"}, RouteDigest: "sha256:" + strings.Repeat("b", 64)}
}

func TestCapacityRecoveryRejectsHEADSuccessAndRequiresFreshStableTransfer(t *testing.T) {
	h := capacityHold()
	base := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	o := OfflineRecoveryObservation{}
	for i := 0; i < 5; i++ {
		at := base.Add(time.Duration(i) * 5 * time.Minute)
		o = o.Record(h, at, true, at.Add(time.Hour))
	}
	if o.Admitted || o.Ready(h, base.Add(20*time.Minute)) {
		t.Fatal("HEAD successes admitted a capacity-limited edge")
	}
	o = OfflineRecoveryObservation{}
	for i := 0; i < 3; i++ {
		at := base.Add(time.Duration(i) * 5 * time.Minute)
		e := capacityEvidence(at)
		o = o.RecordCapacity(h, at, true, at.Add(time.Hour), &e, "capacity_verified")
		if o.Ready(h, at) != (i == 2) {
			t.Fatal("capacity recovery stability violated", i, o)
		}
		duplicate := o.RecordCapacity(h, at, true, at.Add(time.Hour), &e, "capacity_verified")
		if duplicate.Successes != o.Successes {
			t.Fatal("duplicate probe grew recovery history")
		}
	}
	at := base.Add(15 * time.Minute)
	o = o.RecordCapacity(h, at, false, at.Add(time.Hour), nil, "capacity_below_policy")
	if o.Admitted || o.Successes != 0 || !o.FirstSuccess.IsZero() {
		t.Fatal("failed transfer retained recovery progress", o)
	}
	e := capacityEvidence(at.Add(5 * time.Minute))
	o = o.RecordCapacity(h, e.CheckedAt, true, e.CheckedAt.Add(time.Hour), &e, "capacity_verified")
	if o.Admitted || o.Successes != 1 {
		t.Fatal("flapping resumed the old success streak")
	}
}

func TestCapacityEvidenceCannotHideLatencyTransformOrCodeReplacement(t *testing.T) {
	h := capacityHold()
	at := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, scenario := range []string{"slow-total", "slow-rate", "partial", "invented-rate", "nan", "inf", "missing-code", "foreign-reference", "self-reference", "bad-digest"} {
		e := capacityEvidence(at)
		switch scenario {
		case "slow-total":
			e.ElapsedMilliseconds = 6000
			e.BytesPerSecond = float64(e.Bytes) / 6
		case "slow-rate":
			e.ElapsedMilliseconds = 4500
			e.BytesPerSecond = float64(e.Bytes) / 4.5
		case "partial":
			e.Bytes--
		case "invented-rate":
			e.BytesPerSecond++
		case "nan":
			e.ElapsedMilliseconds = math.NaN()
		case "inf":
			e.BytesPerSecond = math.Inf(1)
		case "missing-code":
			e.Code.WorkerPodUID = ""
		case "foreign-reference":
			e.ReferenceEdgeID = "foreign"
		case "self-reference":
			e.ReferenceEdgeID = h.EdgeID
		case "bad-digest":
			e.ReportDigest = "unverified"
		}
		if e.Passes(*h.Recovery.Capacity, h.EdgeID) {
			t.Fatal("invalid capacity evidence accepted", scenario)
		}
	}
	o := OfflineRecoveryObservation{}
	for i := 0; i < 2; i++ {
		e := capacityEvidence(at.Add(time.Duration(i) * 5 * time.Minute))
		o = o.RecordCapacity(h, e.CheckedAt, true, e.CheckedAt.Add(time.Hour), &e, "capacity_verified")
	}
	e := capacityEvidence(at.Add(10 * time.Minute))
	e.Code.WorkerPodUID = "replacement"
	o = o.RecordCapacity(h, e.CheckedAt, true, e.CheckedAt.Add(time.Hour), &e, "capacity_verified")
	if o.Successes != 1 || o.Admitted {
		t.Fatal("replaced code inherited previous recovery evidence")
	}
}

func TestCapacityRecoveryCannotCountCloselySpacedSuccessOrMalformedAdmission(t *testing.T) {
	h := capacityHold()
	base := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	o := OfflineRecoveryObservation{}
	for i := 0; i < 3; i++ {
		at := base.Add(time.Duration(i) * 5 * time.Minute)
		e := capacityEvidence(at)
		o = o.RecordCapacity(h, at, true, at.Add(time.Hour), &e, "capacity_verified")
	}
	if !o.Ready(h, base.Add(10*time.Minute)) {
		t.Fatal("fixture not recovered")
	}
	for _, scenario := range []string{"missing_result", "missing_successes", "retained_failures", "short_stability", "missing_code"} {
		changed := o
		switch scenario {
		case "missing_result":
			changed.LastResult = ""
		case "missing_successes":
			changed.Successes = 0
		case "retained_failures":
			changed.Failures = 1
		case "short_stability":
			changed.FirstSuccess = changed.LastSuccess
		case "missing_code":
			e := *changed.Capacity
			e.Code.WorkerPodUID = ""
			changed.Capacity = &e
		}
		if changed.Ready(h, base.Add(10*time.Minute)) {
			t.Fatal("malformed admission accepted", scenario)
		}
	}
	e := capacityEvidence(base.Add(10*time.Minute + time.Second))
	if got := o.RecordCapacity(h, e.CheckedAt, true, e.CheckedAt.Add(time.Hour), &e, "capacity_verified"); got.LastAttempt != o.LastAttempt {
		t.Fatal("early success advanced observation")
	}
	e.Code.WorkerPodUID = "replaced"
	if got := o.RecordCapacity(h, e.CheckedAt, true, e.CheckedAt.Add(time.Hour), &e, "capacity_verified"); got.Admitted || got.Successes != 0 {
		t.Fatal("early code replacement retained admission")
	}
}

func TestCapacityRecoveryDoesNotChangeLegacyObservationWire(t *testing.T) {
	raw, err := json.Marshal(OfflineRecoveryObservation{})
	if err != nil || strings.Contains(string(raw), "first_success") || strings.Contains(string(raw), "capacity") || strings.Contains(string(raw), "last_result") {
		t.Fatal("new fields changed legacy evidence encoding", string(raw), err)
	}
}

func TestCapacityRecoveryDeclarationDigestMatchesPublisher(t *testing.T) {
	if got := OfflineRecoveryDigest(capacityHold()); got != "sha256:cb5b318de682263959e35fd8548180d01ac29408af3777883036cc081b41ab03" {
		t.Fatal("capacity declaration wire differs from configuration publisher", got)
	}
}
