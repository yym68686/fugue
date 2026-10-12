package platformproducer

import (
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestBusinessExplorationConfigurationHasIndependentSingleRouteBoundary(t *testing.T) {
	prior := ProjectionPolicyInput{Generation: "before", Scope: "global", DNSQueryPolicy: &platformconfig.DNSQueryPolicy{RankingMode: "active"}}
	route := platformconfig.PhysicalQualityRoute{Hostname: "app.example.test", PathPrefix: "/", TrafficClass: "streaming", BusinessExploration: &model.BusinessEdgeExplorationPolicy{IntentID: "intent-a", Mode: "observe", CandidateEdgeIDs: []string{"edge-a", "edge-b"}}}
	next := cloneBusinessProjectionPolicy(prior)
	next.Generation = "after"
	next.DNSQueryPolicy.PhysicalRoutes = []platformconfig.PhysicalQualityRoute{route}
	if err := validateBusinessExplorationDelta(prior, next); err != nil {
		t.Fatal("independent enrollment rejected", err)
	}
	for _, mode := range []string{"adaptive", "paused", "remove"} {
		changed := cloneBusinessProjectionPolicy(next)
		changed.Generation = "next-" + mode
		if mode == "remove" {
			changed.DNSQueryPolicy.PhysicalRoutes = nil
		} else {
			changed.DNSQueryPolicy.PhysicalRoutes[0].BusinessExploration.Mode = mode
			if validateBusinessExplorationDelta(next, changed) == nil {
				t.Fatal("changed intent reused sticky-stop identity")
			}
			changed.DNSQueryPolicy.PhysicalRoutes[0].BusinessExploration.IntentID = "intent-" + mode
		}
		if err := validateBusinessExplorationDelta(next, changed); err != nil {
			t.Fatal("independent stage or recovery rejected", mode, err)
		}
	}
	for _, scenario := range []string{"unchanged", "two_routes", "unrelated_constraint", "legacy_only", "ownership"} {
		changed := cloneBusinessProjectionPolicy(next)
		changed.Generation = "changed"
		switch scenario {
		case "unchanged":
			changed = cloneBusinessProjectionPolicy(prior)
			changed.Generation = "changed"
		case "two_routes":
			other := route
			other.Hostname = "other.example.test"
			changed.DNSQueryPolicy.PhysicalRoutes = append(changed.DNSQueryPolicy.PhysicalRoutes, other)
		case "unrelated_constraint":
			changed.DNSQueryPolicy.ECSEnabled = true
		case "legacy_only":
			changed.DNSQueryPolicy.PhysicalRoutes[0].BusinessExploration = nil
		case "ownership":
			changed.Scope = "other"
		}
		if validateBusinessExplorationDelta(prior, changed) == nil {
			t.Fatal("accepted broad configuration change", scenario)
		}
	}
	if next.DNSQueryPolicy.PhysicalRoutes[0].BusinessExploration.Mode != "observe" {
		t.Fatal("validation mutated independent configuration")
	}
}
