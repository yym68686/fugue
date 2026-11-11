package api

import (
	"testing"
	"time"

	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestBusinessRetirementReturnsHostnameToDynamicLearningOnlyAfterExplicitRemoval(t *testing.T) {
	projection, nodes, policy, at := directQueryFixture()
	policy.ECSEnabled, policy.ExplorationPercent = false, 0
	policy.OrderedProjection = &platformconfig.DNSOrderedProjection{DefaultOrder: model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-a", "edge-b"}}}
	policy.DynamicQuality = &platformconfig.DynamicQualityPolicy{Mode: "all_dynamic", Policy: edgequality.DefaultDeliveryNetworkPolicy(), RefreshQueriesPerCycle: 8, RefreshConcurrency: 2}
	if err := projectDirectDNSQueries(&projection, policy, nodes, at); err != nil {
		t.Fatal(err)
	}
	for _, expires := range []time.Time{at.Add(time.Hour), at.Add(-time.Hour)} {
		policy.PhysicalRoutes = []platformconfig.PhysicalQualityRoute{{Hostname: "app.example.test", BusinessExploration: &model.BusinessEdgeExplorationPolicy{IntentID: "intent-a", ExpiresAt: expires}}}
		jobs, states, err := dynamicQualityRoutes(projection, policy)
		if err != nil || len(jobs) != 0 || states["app.example.test"] != "explicit_quality_policy" {
			t.Fatal("wall-clock expiry silently discarded explicit experiment authority", jobs, states, err)
		}
	}
	policy.PhysicalRoutes = nil // Applied only by the independently fenced configuration transaction.
	jobs, states, err := dynamicQualityRoutes(projection, policy)
	if err != nil || len(jobs) != 1 || states["app.example.test"] != "learning_queued" || jobs[0].Route.BusinessExploration != nil {
		t.Fatal("retired experiment continued to exclude dynamic learning", jobs, states, err)
	}
	projection.Intent.Routes[0].EdgeGroupMode = model.PlatformRouteEdgeGroupModePinned
	projection.Intent.Routes[0].EdgeGroupID = "edge-group-a"
	jobs, states, err = dynamicQualityRoutes(projection, policy)
	if err != nil || len(jobs) != 0 || states["app.example.test"] != "pinned_constraint" {
		t.Fatal("experiment retirement discarded an independent placement constraint", jobs, states, err)
	}
}
