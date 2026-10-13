package api

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestUnifiedDNSUsesOneDecisionDespiteDifferentHistoricalBaselines(t *testing.T) {
	baselines := map[string]string{"dns-a": "edge-a", "dns-b": "edge-b"}
	first, route := businessPublicationFixtureForBaselines(t, nil, false, "dns-a", baselines)
	second, _ := businessPublicationFixtureForBaselines(t, nil, false, "dns-b", baselines)
	policy := route.BusinessExploration
	policy.Version = model.BusinessEdgeUnifiedVersion
	policy.MaximumCandidateBasisPoints = 10000
	policy.MinimumConnectionsPerBucket = 1
	policy.Unified = &model.BusinessUnifiedPolicy{MinimumConnectionsPerCohort: 5, RTTAdvantageMS: 20, PromotionMinimumConnections: 8, PromotionMinimumCoverageBasisPoints: 9000}
	compiled, err := captureUnifiedBusinessPublications([]edgequality.Receipt{second, first}, route, first.Snapshot.CapturedAt)
	if err != nil {
		t.Fatal(err)
	}
	left, right := compiled["dns-a\x00"+route.Hostname], compiled["dns-b\x00"+route.Hostname]
	if left.Selection.PrimaryEdgeID != "edge-a" || right.Selection.PrimaryEdgeID != "edge-b" || left.Selection.BusinessAllocation.ReceiptDigest != right.Selection.BusinessAllocation.ReceiptDigest || !reflect.DeepEqual(left.Selection.BusinessAllocation.State, right.Selection.BusinessAllocation.State) || left.Selection.BusinessAllocation.State.Unified.PrimaryEdgeID != "edge-a" {
		t.Fatal("DNS identity selected independent business decision")
	}
	if left.Selection.DNSReceiptID == right.Selection.DNSReceiptID {
		t.Fatal("execution predecessor identity was discarded")
	}
	var evidence businessPublicationEvidence
	if err := json.Unmarshal(left.Evidence, &evidence); err != nil {
		t.Fatal(err)
	}
	if _, err := compileBoundBusinessSelection(evidence, route, first.Snapshot.CapturedAt); err != nil {
		t.Fatal("unified evidence does not replay", err)
	}
	coverage, err := businessExplorationCoverageFact(route, "dns-a", evidence)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := platformconfig.RuntimeSnapshot{Facts: map[string]any{"business_exploration_coverage": []map[string]any{coverage}}}
	digest, err := platformconfig.RuntimeSnapshotDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var encoded bytes.Buffer
	if err := platformconfig.WriteRuntimeSnapshot(&encoded, snapshot); err != nil {
		t.Fatal(err)
	}
	stored, err := platformconfig.DecodeRuntimeSnapshotJSON(encoded.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	storedDigest, err := platformconfig.RuntimeSnapshotDigest(stored)
	if err != nil || storedDigest != digest {
		t.Fatal("unified coverage changed compiler input identity after storage", err)
	}
	evidence.Sources = evidence.Sources[:1]
	if _, err := compileBoundBusinessSelection(evidence, route, first.Snapshot.CapturedAt); err == nil {
		t.Fatal("partial DNS predecessor set accepted")
	}
	if _, err := captureBusinessPublication(first, route, first.Snapshot.CapturedAt); err == nil {
		t.Fatal("legacy single-DNS path bypassed unified publication")
	}
	if _, err := captureUnifiedBusinessPublications([]edgequality.Receipt{first, first}, route, first.Snapshot.CapturedAt); err == nil {
		t.Fatal("duplicate DNS substituted for required member")
	}
}
