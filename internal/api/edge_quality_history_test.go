package api

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"fugue/internal/dnsserver"
	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

// Exercise the real verified compiler-input storage/read boundary. A missing
// new observation must not make policy migration or process restart lose the
// original positively verified quality data.
func TestHistoricalQualityDoesNotUpgradeLearningToMeasured(t *testing.T) {
	receipt, _ := businessPublicationFixture(t)
	// This fixture has no client measurements: its learning order is NOT a
	// historical quality winner, even when stored under a verified parent.
	selection, err := compileBoundPhysicalQualitySelection(receipt, receipt.Snapshot.CapturedAt)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(receipt)
	input := platformconfig.RuntimeSnapshot{IntentGeneration: "intent-a", PolicyGeneration: "policy-a", DNSSelections: []platformconfig.DNSSelectionObservation{{NodeID: "dns-a", Hostname: receipt.Snapshot.Hostname, Type: "A", PhysicalSelection: selection, PhysicalEvidence: raw}}}
	s, _, _ := qualityRestoreInputFixture(t, input)
	current := receipt.Snapshot
	current.Policy = edgequality.DefaultPairedContinuityPolicy()
	current.CapturedAt = current.CapturedAt.Add(48 * time.Hour)
	s.bindHistoricalQualityEvidence(context.Background(), &current, "dns-a")
	if current.HistoricalEvidence != nil {
		t.Fatal("learning was upgraded to historical measurement")
	}
}

func historicalBoundReceiptFixture(t *testing.T) edgequality.Receipt {
	t.Helper()
	base, _ := businessPublicationFixture(t)
	snapshot := base.Snapshot
	snapshot.Policy = edgequality.DefaultComparableDeliveryPolicy()
	now := snapshot.CapturedAt
	for edgeIndex, candidate := range snapshot.Candidates {
		for bucket := 0; bucket < 3; bucket++ {
			for record := 0; record < 3; record++ {
				at := now.Add(-time.Duration(bucket*300+record+1) * time.Second)
				sample, _ := networkWitnessFixture(at)
				sample.ID = fmt.Sprintf("network-%d-%d-%d", edgeIndex, bucket, record)
				sample.EdgeID, sample.EdgeGroupID, sample.TrafficClass = candidate.EdgeID, candidate.EdgeGroupID, snapshot.TrafficClass
				sample.ObservedAt = at
				snapshot.NetworkSamples = append(snapshot.NetworkSamples, sample)
				capacity := model.EdgeNetworkNodeCapacity{Source: "kubelet_node_allocatable_v1", NodeUID: "node-" + candidate.EdgeID, ObservedAt: at, ValidUntil: at.Add(2 * time.Minute), CPUObservedAt: at, MemoryObservedAt: at,
					CPUUsageNanoCores: 10000000, CPUAllocatableMilliCores: 1000, MemoryWorkingSetBytes: 100, MemoryAllocatableBytes: 1000, Pressure: []string{}}
				sample.ID = "witness-" + sample.ID
				sample.Source = "route_tls_witness_v1"
				sample.ServiceRTTMS = nil
				sample.ServiceTarget = ""
				sample.RouteWitness = &model.EdgeNetworkRouteWitness{Address: []string{"8.8.8.8", "9.9.9.9"}[edgeIndex], ValidUntil: at.Add(time.Minute), NodeCapacity: &capacity}
				snapshot.NetworkSamples = append(snapshot.NetworkSamples, sample)
			}
		}
	}
	var actual dnsserver.DNSDecisionReceipt
	if json.Unmarshal(snapshot.ActualDNSReceipt, &actual) != nil {
		t.Fatal("actual DNS")
	}
	snapshot.Observations = nil
	if err := bindPhysicalQualityDNS(&snapshot, actual); err != nil {
		t.Fatal(err)
	}
	receipt, err := edgequality.Capture(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	selection, err := compileBoundPhysicalQualitySelection(receipt, now)
	if err != nil || selection.QualityState != "measured" {
		t.Fatal("historical fixture not measured", selection, err)
	}
	return receipt
}

func TestHistoricalQualityLoadsVerifiedOriginalAcrossPolicyMigration(t *testing.T) {
	receipt := historicalBoundReceiptFixture(t)
	selection, err := compileBoundPhysicalQualitySelection(receipt, receipt.Snapshot.CapturedAt)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(receipt)
	input := platformconfig.RuntimeSnapshot{IntentGeneration: "intent-a", PolicyGeneration: "policy-a", DNSSelections: []platformconfig.DNSSelectionObservation{{NodeID: "dns-a", Hostname: receipt.Snapshot.Hostname, Type: "A", PhysicalSelection: selection, PhysicalEvidence: raw}}}
	s, _, _ := qualityRestoreInputFixture(t, input)
	for _, scope := range []string{"global", "tcp_peer:198.51.100.0/24"} {
		current := receipt.Snapshot
		current.Scope = scope
		current.Policy = edgequality.DefaultPairedContinuityPolicy()
		current.CapturedAt = current.CapturedAt.Add(48 * time.Hour)
		s.bindHistoricalQualityEvidence(context.Background(), &current, "dns-a")
		if (current.HistoricalEvidence != nil) != (scope == "global") {
			t.Fatal("history lost on policy migration or crossed scope", scope)
		}
		if current.HistoricalEvidence != nil && (current.HistoricalEvidence.Receipt.Digest != receipt.Digest || !current.HistoricalEvidence.Selection.CapturedAt.Equal(selection.CapturedAt)) {
			t.Fatal("original evidence renewed")
		}
	}
}
