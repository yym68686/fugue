package edgequality

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

// Exercise the comparator directly here; complete report/signature validation
// is separately covered by clientmeasurement and ClientProbeObservations tests.
func pairedAlgorithmFixture() Snapshot {
	snapshot := failureAwareFixture()
	snapshot.Policy = DefaultPairedContinuityPolicy()
	snapshot.Policy.UncertaintyMS = 1
	for index := range snapshot.Observations {
		o := &snapshot.Observations[index]
		o.ClientSource = "authenticated_client_probe"
		o.ClientProbeRoundID = fmt.Sprintf("round-%d", o.ObservedAt.UnixNano())
		if o.EdgeID == "edge-a" {
			o.ClientNetworkMS = nil
		}
	}
	return snapshot
}

func TestPairedComparisonCountsConnectionFailuresAndThreeCompletedWindows(t *testing.T) {
	snapshot := pairedAlgorithmFixture()
	result := evaluateNetwork(snapshot)
	if result.Hypothesis != "switch" || result.ProposedEdgeID != "edge-b" || result.SustainedBuckets != 3 || result.QualityState != "measured" {
		t.Fatal("connection failures without RTT froze a complete-transfer winner", result)
	}
	for _, assessment := range result.Candidates {
		if assessment.Metrics["client_network_ms"].State != "unknown" || assessment.Metrics["service_network_ms"].ScoreCostMS == nil {
			t.Fatal("global assessment fabricated client latency or omitted score explanation", assessment)
		}
	}
	originalTime := *result.QualityObservedAt
	snapshot.CapturedAt = snapshot.CapturedAt.Add(time.Minute)
	if next := evaluateNetwork(snapshot); next.QualityObservedAt == nil || !next.QualityObservedAt.Equal(originalTime) {
		t.Fatal("reading the same measurement renewed its age", next)
	}
	for index := range snapshot.Observations {
		if snapshot.Observations[index].ObservedAt.Unix()/300 == snapshot.CapturedAt.Unix()/300-2 {
			snapshot.Observations[index].ClientProbeRoundID += "-missing-middle"
			if snapshot.Observations[index].EdgeID == "edge-a" {
				snapshot.Observations[index].ClientProbeRoundID += "-other"
			}
		}
	}
	if next := evaluateNetwork(snapshot); next.Hypothesis != "hold" {
		t.Fatal("missing completed middle window was ignored", next)
	}
	snapshot = pairedAlgorithmFixture()
	for index := range snapshot.Observations {
		if snapshot.Observations[index].EdgeID == "edge-b" {
			snapshot.Observations[index].ClientProbeRoundID += "-unpaired"
		}
	}
	if result := evaluateNetwork(snapshot); result.Hypothesis != "hold" || result.QualityState != "learning" {
		t.Fatal("unpaired rounds manufactured a comparison", result)
	}
}

func TestContinuityNeverUsesServiceOnlyOrPassiveClientWinner(t *testing.T) {
	snapshot := failureAwareFixture()
	snapshot.Policy = DefaultPairedContinuityPolicy()
	for _, passive := range []bool{true, false} {
		if !passive {
			for index := range snapshot.Observations {
				snapshot.Observations[index].ClientSource = ""
				snapshot.Observations[index].ClientNetworkMS = nil
			}
		}
		if result := evaluateTest(t, snapshot); result.Hypothesis != "hold" || result.QualityState != "learning" || result.QualityObservedAt != nil {
			t.Fatal("backend/passive evidence claimed a client winner", result)
		}
	}
}

func TestContinuityRetainsOriginalOrderTimeAcrossSerializationAndFreshCaptures(t *testing.T) {
	snapshot := networkFixture()
	snapshot.Policy = DefaultPairedContinuityPolicy()
	snapshot.Scope = "global"
	for index := range snapshot.Observations {
		snapshot.Observations[index].Scope = snapshot.Scope
	}
	digest := "sha256:" + strings.Repeat("a", 64)
	original := snapshot.CapturedAt.Add(-2 * time.Hour)
	selection := &model.DNSPhysicalSelection{Version: model.DNSPhysicalSelectionVersion, PrimaryEdgeID: "edge-a", OrderedEdgeIDs: []string{"edge-a", "edge-b"}, EvidenceDigest: digest, DNSReceiptID: "previous-dns-answer", LoadedDigest: digest, PolicyDigest: digest, Scope: snapshot.Scope, CapturedAt: original, QualityState: "measured"}
	snapshot.PreviousSelection = selection
	for iteration := 0; iteration < 2; iteration++ {
		receipt, err := Capture(snapshot)
		if err != nil || receipt.Result.QualityState != "retained" || receipt.Result.QualityObservedAt == nil || !receipt.Result.QualityObservedAt.Equal(original) {
			t.Fatal("capture erased or renewed previous quality", receipt.Result, err)
		}
		binding := DNSBinding{ReceiptID: "current-dns-answer", LoadedDigest: digest, PolicyDigest: digest, Hostname: snapshot.Hostname, Scope: snapshot.Scope, CurrentEdgeID: "edge-a", ObservedAt: snapshot.CapturedAt, ReplayMatched: true, WriteSucceeded: true}
		compiled, err := CompileSelection(receipt, binding, snapshot.CapturedAt)
		if err != nil || !reflect.DeepEqual(compiled.OrderedEdgeIDs, selection.OrderedEdgeIDs) {
			t.Fatal("fresh route proof did not preserve historical order", compiled, err)
		}
		raw, _ := json.Marshal(compiled)
		var restarted model.DNSPhysicalSelection
		if err := json.Unmarshal(raw, &restarted); err != nil {
			t.Fatal(err)
		}
		snapshot.PreviousSelection = &restarted
	}
	snapshot.PreviousSelection.QualityObservedAt = timePointer(snapshot.CapturedAt.Add(-25 * time.Hour))
	// Keep the signed selection internally well formed while testing expiry.
	snapshot.PreviousSelection.CapturedAt = snapshot.CapturedAt
	if result := evaluateTest(t, snapshot); result.QualityState != "learning" {
		t.Fatal("old order retained past explicit policy", result)
	}
	snapshot.PreviousSelection = selection
	snapshot.Candidates[0].RouteProofVerified = false
	receipt, err := Capture(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	binding := DNSBinding{ReceiptID: "current-dns-answer", LoadedDigest: digest, PolicyDigest: digest, Hostname: snapshot.Hostname, Scope: snapshot.Scope, CurrentEdgeID: "edge-a", ObservedAt: snapshot.CapturedAt, ReplayMatched: true, WriteSucceeded: true}
	if _, err := CompileSelection(receipt, binding, snapshot.CapturedAt); err == nil {
		t.Fatal("retention bypassed a missing current route/TLS proof")
	}
}

func timePointer(at time.Time) *time.Time { return &at }
