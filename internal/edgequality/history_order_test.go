package edgequality

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
)

func publishedOrderFixture(t *testing.T) (Snapshot, DNSBinding) {
	t.Helper()
	snapshot, binding := historicalFixture(t)
	previous := model.CloneDNSPhysicalSelection(snapshot.HistoricalEvidence.Selection)
	at := previous.CapturedAt
	previous.QualityState, previous.QualityObservedAt = "retained", &at
	snapshot.HistoricalEvidence = nil
	snapshot.PreviousSelection = previous
	snapshot.CurrentEdgeID, binding.CurrentEdgeID = previous.PrimaryEdgeID, previous.PrimaryEdgeID
	snapshot.HistoricalOrder = &PublishedOrderEvidence{SourceArtifactID: "verified-order-parent", InputDigest: binding.LoadedDigest, EvidenceDigest: binding.LoadedDigest, Selection: model.CloneDNSPhysicalSelection(previous)}
	return snapshot, binding
}

func TestHistoricalPublishedOrderSurvivesMissingTranscriptsAndReadmitsRecovery(t *testing.T) {
	snapshot, binding := publishedOrderFixture(t)
	original := *snapshot.PreviousSelection.QualityObservedAt
	for _, suspended := range []bool{true, false} {
		snapshot.Candidates[0].ServingBlockedReason = ""
		if suspended {
			snapshot.Candidates[0].ServingBlockedReason = "dns_admission_suspended"
		}
		receipt, err := Capture(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		selection, err := CompileSelection(receipt, binding, snapshot.CapturedAt)
		want := []string{"edge-b", "edge-a"}
		if suspended {
			want = want[:1]
		}
		if err != nil || !reflect.DeepEqual(selection.OrderedEdgeIDs, want) || selection.History == nil || receipt.Result.History.EvidenceKind != "published_order" || selection.History.EvidenceKind != "" || !selection.QualityObservedAt.Equal(original) {
			t.Fatal("published order lost, promoted as new measurement or recovery blocked", selection, err)
		}
		serving, _ := json.Marshal(selection)
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(serving, &fields)
		var history map[string]json.RawMessage
		_ = json.Unmarshal(fields["history"], &history)
		if history["evidence_kind"] != nil {
			t.Fatal("new diagnostic field leaked to strict existing DNS readers")
		}
		raw, _ := json.Marshal(receipt)
		var restarted Receipt
		if json.Unmarshal(raw, &restarted) != nil {
			t.Fatal("round trip")
		}
		if _, err := Replay(restarted); err != nil {
			t.Fatal("published order not replayable", err)
		}
	}
}

func TestHistoricalPublishedOrderCannotChangePrimaryOrBypassCurrentGates(t *testing.T) {
	for _, scenario := range []string{"forged predecessor", "new primary", "capacity pressure", "stale route", "suspended", "future quality time"} {
		t.Run(scenario, func(t *testing.T) {
			snapshot, binding := publishedOrderFixture(t)
			switch scenario {
			case "forged predecessor":
				snapshot.HistoricalOrder.Selection.OrderedEdgeIDs = []string{"edge-b"}
			case "new primary":
				snapshot.CurrentEdgeID = "edge-a"
			case "capacity pressure":
				value := 0.99
				snapshot.Observations = []Observation{{ID: "capacity", EdgeID: "edge-b", Hostname: snapshot.Hostname, TrafficClass: snapshot.TrafficClass, Scope: snapshot.Scope, RouteGeneration: snapshot.Candidates[1].RouteGeneration, ObservedAt: snapshot.CapturedAt, CapacitySource: "kubelet_node_allocatable_v1", CapacityUtilization: &value}}
			case "stale route":
				snapshot.Candidates[1].ProofObservedAt = timePointer(snapshot.CapturedAt.Add(-time.Hour))
			case "suspended":
				snapshot.Candidates[1].ServingBlockedReason = "dns_admission_suspended"
			case "future quality time":
				snapshot.HistoricalOrder.Selection.QualityObservedAt = timePointer(snapshot.CapturedAt.Add(time.Hour))
			}
			receipt, err := Capture(snapshot)
			if err == nil && receipt.Result.History != nil {
				t.Fatal("published history bypassed guard", receipt.Result)
			}
			if err == nil {
				if selection, err := CompileSelection(receipt, binding, snapshot.CapturedAt); err == nil && selection.History != nil {
					t.Fatal("invalid historical authority published")
				}
			}
		})
	}
}
