package api

import (
	"testing"
	"time"

	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestHistoricalPublishedOrderRequiresExactActualPredecessorAndOriginalTime(t *testing.T) {
	receipt := historicalBoundReceiptFixture(t)
	previous, err := compileBoundPhysicalQualitySelection(receipt, receipt.Snapshot.CapturedAt)
	if err != nil {
		t.Fatal(err)
	}
	at := previous.CapturedAt.Add(-48 * time.Hour)
	previous.QualityState, previous.QualityObservedAt = "retained", &at
	for _, scenario := range []string{"match", "order changed", "quality time changed", "missing time", "business", "scope changed"} {
		t.Run(scenario, func(t *testing.T) {
			order := &model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: append([]string(nil), previous.OrderedEdgeIDs...), QualityObservedAt: &at}
			current := model.CloneDNSPhysicalSelection(previous)
			switch scenario {
			case "order changed":
				order.OrderedEdgeIDs = []string{"different"}
			case "quality time changed":
				changed := at.Add(time.Second)
				order.QualityObservedAt = &changed
			case "missing time":
				order.QualityObservedAt = nil
			case "business":
				current.BusinessAllocation = &model.DNSBusinessAllocation{}
			case "scope changed":
				current.Scope = "tcp_peer:198.51.100.0/24"
			}
			record := model.EdgeDNSRecord{AnswerPolicy: model.DNSAnswerPolicy{PolicyKind: model.DNSAnswerPolicyKindPhysicalOrder, PhysicalOrder: order}}
			if got := publishedQualityOrderMatches(current, record); got != (scenario == "match") {
				t.Fatal("unrelated order lent history", got)
			}
		})
	}
}

func TestHistoricalPublishedOrderFallbackPreservesOldClockOnlyFromVerifiedBaseline(t *testing.T) {
	for _, verified := range []bool{false, true} {
		projection, nodes, policy, now := directQueryFixture()
		at := time.Now().UTC().Add(-48 * time.Hour)
		policy.ECSEnabled, policy.ExplorationPercent = false, 0
		policy.OrderedProjection = &platformconfig.DNSOrderedProjection{DefaultOrder: model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-b", "edge-a"}, QualityObservedAt: &at}}
		policy.DynamicQuality = &platformconfig.DynamicQualityPolicy{Mode: "all_dynamic", Policy: edgequality.DefaultPairedContinuityPolicy(), RefreshQueriesPerCycle: 4, RefreshConcurrency: 2}
		availability := model.DNSAvailabilityPolicy{Version: model.DNSAvailabilityVersion, OrderedEdgeIDs: []string{"edge-a", "edge-b"}, RecoverySuccesses: 3, RecoveryMinSeconds: 60}
		policy.AvailabilityRoutes = []platformconfig.DNSAvailabilityRoute{{Hostname: "app.example.test", Policy: availability}}
		if err := projectDirectDNSQueries(&projection, policy, nodes, now); err != nil {
			t.Fatal(err)
		}
		for i := range projection.Policy.DNSAnswerRules {
			if rule := &projection.Policy.DNSAnswerRules[i]; rule.Hostname == "app.example.test" {
				rule.PhysicalOrder = &model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-b", "edge-a"}, QualityObservedAt: &at}
			}
		}
		if verified {
			for i := range projection.RuntimeSnapshot.DNSSelections {
				projection.RuntimeSnapshot.DNSSelections[i].Reason = "physical_quality_learning_preserved_verified_order"
			}
		}
		if err := applyDNSAvailabilityBaseline(&projection, "app.example.test", &availability); err != nil {
			t.Fatal(err)
		}
		for _, rule := range projection.Policy.DNSAnswerRules {
			if rule.Hostname != "app.example.test" {
				continue
			}
			want := "edge-a"
			if verified {
				want = "edge-b"
			}
			if rule.PhysicalOrder == nil || rule.PhysicalOrder.OrderedEdgeIDs[0] != want || (rule.PhysicalOrder.QualityObservedAt != nil) != verified {
				t.Fatal("old verified order expired or unauthenticated timestamp bypassed retention", verified, rule.PhysicalOrder)
			}
			if verified && !rule.PhysicalOrder.QualityObservedAt.Equal(at) {
				t.Fatal("fallback renewed old timestamp")
			}
		}
	}
}
