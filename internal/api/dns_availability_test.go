package api

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestDNSAvailabilityBaselineRetainsIndependentConstraintsAndNoQualityEvidence(t *testing.T) {
	projection, nodes, strategy, now := directQueryFixture()
	strategy.ECSEnabled, strategy.ExplorationPercent = false, 0
	strategy.OrderedProjection = &platformconfig.DNSOrderedProjection{DefaultOrder: model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-a", "edge-b"}}}
	policy := &model.DNSAvailabilityPolicy{Version: model.DNSAvailabilityVersion, OrderedEdgeIDs: []string{"edge-b", "edge-a", "foreign"}, RecoverySuccesses: 3, RecoveryMinSeconds: 60}
	strategy.AvailabilityRoutes = []platformconfig.DNSAvailabilityRoute{{Hostname: "app.example.test", Policy: *policy}}
	if err := projectDirectDNSQueries(&projection, strategy, nodes, now); err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(projection.Intent)
	if err := applyDNSAvailabilityBaseline(&projection, "app.example.test", policy); err != nil {
		t.Fatal(err)
	}
	for _, rule := range projection.Policy.DNSAnswerRules {
		if rule.Hostname != "app.example.test" {
			continue
		}
		if rule.PhysicalOrder == nil || !reflect.DeepEqual(rule.PhysicalOrder.OrderedEdgeIDs, []string{"edge-b", "edge-a"}) {
			t.Fatal("availability added an unauthorized node", rule)
		}
	}
	for _, fact := range projection.RuntimeSnapshot.DNSSelections {
		if fact.PhysicalSelection != nil || len(fact.PhysicalEvidence) != 0 || fact.Reason != "availability_only_quality_unavailable" {
			t.Fatal("manufactured quality success", fact)
		}
	}
	after, _ := json.Marshal(projection.Intent)
	if string(before) != string(after) {
		t.Fatal("availability changed route, tenant or static DNS intent")
	}
	// Unknown or non-owned names are rejected atomically.
	before, _ = json.Marshal(projection)
	if applyDNSAvailabilityBaseline(&projection, "foreign.example.test", policy) == nil {
		t.Fatal("unowned fallback accepted")
	}
	after, _ = json.Marshal(projection)
	if string(before) != string(after) {
		t.Fatal("failed fallback partially changed projection")
	}
}

func TestContinuityAvailabilityRetainsBoundedVerifiedOrderWithoutRenewal(t *testing.T) {
	for _, expired := range []bool{false, true} {
		projection, nodes, strategy, now := directQueryFixture()
		strategy.ECSEnabled, strategy.ExplorationPercent = false, 0
		strategy.OrderedProjection = &platformconfig.DNSOrderedProjection{DefaultOrder: model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-a", "edge-b"}}}
		strategy.DynamicQuality = &platformconfig.DynamicQualityPolicy{Mode: "all_dynamic", Policy: edgequality.DefaultPairedContinuityPolicy(), RefreshQueriesPerCycle: 16, RefreshConcurrency: 4}
		policy := &model.DNSAvailabilityPolicy{Version: model.DNSAvailabilityVersion, OrderedEdgeIDs: []string{"edge-a", "edge-b"}, RecoverySuccesses: 3, RecoveryMinSeconds: 60}
		strategy.AvailabilityRoutes = []platformconfig.DNSAvailabilityRoute{{Hostname: "app.example.test", Policy: *policy}}
		if err := projectDirectDNSQueries(&projection, strategy, nodes, now); err != nil {
			t.Fatal(err)
		}
		original := time.Now().UTC().Add(-2 * time.Hour)
		if expired {
			original = original.Add(-24 * time.Hour)
		}
		for index := range projection.Policy.DNSAnswerRules {
			rule := &projection.Policy.DNSAnswerRules[index]
			if rule.Hostname == "app.example.test" && rule.Type == "A" {
				rule.PhysicalOrder = &model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-b", "foreign", "edge-a"}, QualityObservedAt: &original}
			}
		}
		for iteration := 0; iteration < 2; iteration++ {
			if err := applyDNSAvailabilityBaseline(&projection, "app.example.test", policy); err != nil {
				t.Fatal(err)
			}
			for _, rule := range projection.Policy.DNSAnswerRules {
				if rule.Hostname != "app.example.test" || rule.Type != "A" {
					continue
				}
				want := []string{"edge-b", "edge-a"}
				if expired {
					want = []string{"edge-a", "edge-b"}
				}
				if !reflect.DeepEqual(rule.PhysicalOrder.OrderedEdgeIDs, want) || !expired && (rule.PhysicalOrder.QualityObservedAt == nil || !rule.PhysicalOrder.QualityObservedAt.Equal(original)) || expired && rule.PhysicalOrder.QualityObservedAt != nil {
					t.Fatal("fallback lost bounded order, renewed timestamp or admitted foreign node", expired, rule.PhysicalOrder)
				}
			}
			raw, _ := json.Marshal(projection)
			if err := json.Unmarshal(raw, &projection); err != nil {
				t.Fatal(err)
			}
		}
	}
}
