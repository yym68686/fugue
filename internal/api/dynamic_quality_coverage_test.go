package api

import (
	"encoding/json"
	"testing"

	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestDynamicQualityCoverageOnlyQueuesEligibleQueries(t *testing.T) {
	for _, scenario := range []struct {
		name, state string
		jobs        int
	}{
		{"active", "learning_queued", 1},
		{"unavailable origin", "inactive_route_constraint", 0},
		{"disabled route", "inactive_route_constraint", 0},
		{"disabled record", "inactive_route_constraint", 0},
		{"permitted error page", "learning_queued", 1},
		{"unserved zone", "learning_no_eligible_query", 0},
		{"no endpoint", "learning_no_eligible_query", 0},
		{"IPv6 only", "learning_address_family_evidence_required", 0},
		{"dual stack", "learning_queued", 1},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			projection, nodes, policy, now := directQueryFixture()
			policy.ECSEnabled, policy.ExplorationPercent = false, 0
			policy.OrderedProjection = &platformconfig.DNSOrderedProjection{DefaultOrder: model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-a", "edge-b"}}}
			policy.DynamicQuality = &platformconfig.DynamicQualityPolicy{Mode: "all_dynamic", Policy: edgequality.DefaultComparableDeliveryPolicy(), RefreshQueriesPerCycle: 8, RefreshConcurrency: 2}
			switch scenario.name {
			case "unavailable origin":
				projection.RuntimeSnapshot.Origins[0].Status = model.EdgeRouteStatusUnavailable
			case "disabled route":
				projection.Intent.Routes[0].Enabled = false
			case "disabled record":
				projection.Intent.DNS[0].Status = model.EdgeRouteStatusDisabled
			case "permitted error page":
				projection.Intent.Routes[0].Enabled = false
				projection.Intent.DNS[0].RecordKind = model.EdgeDNSRecordKindCustomDomainTarget
				projection.Policy.DNSRouteStateConstraints = []platformconfig.DNSRouteStateConstraint{{RecordKind: model.EdgeDNSRecordKindCustomDomainTarget, InactiveBehavior: "serve_error_page"}}
			case "unserved zone":
				projection.Intent.DNSConsumers[0].Zones = []string{"other.test"}
			case "no endpoint":
				nodes = nil
			case "IPv6 only", "dual stack":
				nodes[0].PublicIPv6 = "2001:4860:4860::8888"
				projection.RuntimeSnapshot.DNSEdgeEndpoints[0].AAAA = []string{nodes[0].PublicIPv6}
				projection.Intent.DNS[0].Route.IPv4Policy, projection.Intent.DNS[0].Route.IPv6Policy = "auto", "auto"
				if scenario.name == "IPv6 only" {
					projection.Intent.DNS[0].Route.IPv4Policy, projection.Intent.DNS[0].Route.IPv6Policy = "ipv6_only", "ipv6_only"
				}
			}
			if err := projectDirectDNSQueries(&projection, policy, nodes, now); err != nil {
				t.Fatal(err)
			}
			before, err := json.Marshal(projection)
			if err != nil {
				t.Fatal(err)
			}
			jobs, states, err := dynamicQualityRoutes(projection, policy)
			if err != nil {
				t.Fatal(err)
			}
			if len(jobs) != scenario.jobs || states["app.example.test"] != scenario.state {
				t.Fatalf("coverage misrepresents eligible observation work: jobs=%d state=%s; want jobs=%d state=%s", len(jobs), states["app.example.test"], scenario.jobs, scenario.state)
			}
			after, err := json.Marshal(projection)
			if err != nil || string(after) != string(before) {
				t.Fatal("coverage changed desired configuration or runtime facts", err)
			}
			if scenario.jobs > 0 && jobs[0].Route.Hostname != "app.example.test" {
				t.Fatal("observation lost exact hostname")
			}
		})
	}
}
