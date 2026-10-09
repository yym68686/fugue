package api

import (
	"encoding/json"
	"reflect"
	"testing"

	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestOrderedProjectionPreservesStaticAndOwnedAliasConstraints(t *testing.T) {
	projection, nodes, strategy, now := directQueryFixture()
	strategy.ECSEnabled, strategy.ExplorationPercent = false, 0
	strategy.OrderedProjection = &platformconfig.DNSOrderedProjection{
		DefaultOrder: model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-a", "edge-b"}},
		Overrides:    []platformconfig.DNSOrderOverride{{NodeID: "dns-a", Hostname: "alias.example.test", Type: "A", Order: model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-b", "edge-a"}}}},
	}
	alias := projection.Intent.DNS[0]
	alias.Hostname, alias.RecordKind = "alias.example.test", model.EdgeDNSRecordKindCustomDomainTarget
	static := platformconfig.DNSIntent{Hostname: "static.example.test", Type: "A", Values: []string{"1.1.1.1"}, TTL: 300}
	projection.Intent.DNS = append(projection.Intent.DNS, alias, static)
	before, _ := json.Marshal(projection.Intent)
	if err := projectDirectDNSQueries(&projection, strategy, nodes, now); err != nil {
		t.Fatal(err)
	}
	if len(projection.Policy.DNSAnswerRules) != 2 || projection.Policy.DNSQueryPolicy.OrderedProjection == nil {
		t.Fatal("explicit configuration or aliases lost")
	}
	for _, rule := range projection.Policy.DNSAnswerRules {
		want := []string{"edge-a", "edge-b"}
		if rule.Hostname == alias.Hostname {
			want = []string{"edge-b", "edge-a"}
		}
		if rule.SelectionMode != model.DNSAnswerPolicyKindPhysicalOrder || !reflect.DeepEqual(rule.PhysicalOrder.OrderedEdgeIDs, want) {
			t.Fatal("order not configuration-bound", rule)
		}
	}
	for _, fact := range projection.RuntimeSnapshot.DNSSelections {
		if fact.PhysicalSelection != nil || len(fact.PhysicalEvidence) != 0 || fact.SelectedEdgeGroupID != "" || len(fact.ScopedCandidates) != 0 {
			t.Fatal("invented measurement or legacy authority")
		}
		for _, candidate := range fact.Candidates {
			if candidate.Score != 0 || candidate.Weight != 0 || candidate.Country != "" || candidate.Priority != 0 {
				t.Fatal("legacy scoring leaked into inventory")
			}
		}
	}
	after, _ := json.Marshal(projection.Intent)
	if string(before) != string(after) {
		t.Fatal("static or route intent changed")
	}
}

func TestOrderedProjectionStillRequiresExactQualityOptIn(t *testing.T) {
	projection, nodes, strategy, now := directQueryFixture()
	strategy.ECSEnabled, strategy.ExplorationPercent = false, 0
	strategy.OrderedProjection = &platformconfig.DNSOrderedProjection{DefaultOrder: model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-a", "edge-b"}}}
	strategy.PhysicalRoutes = []platformconfig.PhysicalQualityRoute{{Hostname: "app.example.test", TrafficClass: "streaming", Policy: edgequality.DefaultNetworkPolicy()}}
	if err := projectDirectDNSQueries(&projection, strategy, nodes, now); err != nil {
		t.Fatal(err)
	}
	if projection.Policy.DNSAnswerRules[0].SelectionMode != model.DNSAnswerPolicyKindPhysicalQuality || projection.Policy.DNSAnswerRules[0].PhysicalOrder != nil || projection.RuntimeSnapshot.DNSSelections[0].PhysicalSelection != nil {
		t.Fatal("configured order bypassed missing quality evidence")
	}
}

func TestOrderedProjectionRejectsEmptyAuthorizedOrderAtomically(t *testing.T) {
	projection, nodes, strategy, now := directQueryFixture()
	strategy.ECSEnabled, strategy.ExplorationPercent = false, 0
	strategy.OrderedProjection = &platformconfig.DNSOrderedProjection{DefaultOrder: model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-foreign"}}}
	before, _ := json.Marshal(projection)
	if err := projectOrderedDNSQueries(&projection, strategy, nodes, now); err == nil {
		t.Fatal("unknown endpoint authorized")
	}
	after, _ := json.Marshal(projection)
	if string(before) != string(after) {
		t.Fatal("failure mutated projection")
	}
}

func TestOrderedProjectionPreservesPinnedRouteConstraint(t *testing.T) {
	projection, nodes, strategy, now := directQueryFixture()
	strategy.ECSEnabled, strategy.ExplorationPercent = false, 0
	strategy.OrderedProjection = &platformconfig.DNSOrderedProjection{DefaultOrder: model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-a", "edge-b"}}}
	projection.Intent.Routes[0].EdgeGroupMode = model.PlatformRouteEdgeGroupModePinned
	projection.Intent.Routes[0].EdgeGroupID = "edge-group-b"
	if err := projectOrderedDNSQueries(&projection, strategy, nodes, now); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(projection.Policy.DNSAnswerRules[0].PhysicalOrder.OrderedEdgeIDs, []string{"edge-b"}) || len(projection.RuntimeSnapshot.DNSSelections[0].Candidates) != 1 {
		t.Fatal("configured order overrode pinned constraint")
	}
}
