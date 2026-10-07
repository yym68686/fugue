package platformconfig_test

import (
	"encoding/json"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/testfixture/celldns"
	"testing"
)

func TestCurrentSourceHardPolicyRestrictsPositivePublication(t *testing.T) {
	req, _, _ := celldns.SourceAuthorizedRequest(t, false)
	source := platformconfig.CellDNSPlanSource{Intent: req.Intent, Endpoints: req.RuntimeSnapshot.DNSEdgeEndpoints, CapturedAt: req.RuntimeSnapshot.CapturedAt}
	p := req.CellRoutePublications[0]
	pub := model.PlatformDNSRouteSourcePublication{Parent: p.Parent, Route: p.Route, TLS: p.TLS, Release: celldns.Publication(p)}
	for _, scenario := range []string{"unready-candidate", "exclusion", "higher-quorum", "disabled", "owner-mismatch"} {
		t.Run(scenario, func(t *testing.T) {
			plan, _, err := platformconfig.DNSRuntimeSourcePlan(source, req.Policy, pub, "cell-a")
			if err != nil {
				t.Fatal(err)
			}
			var current model.PlatformDNSRouteSourcePublication
			raw, _ := json.Marshal(pub)
			json.Unmarshal(raw, &current)
			var policy platformconfig.PolicySnapshot
			raw, _ = json.Marshal(current.Route.Content["policy"])
			json.Unmarshal(raw, &policy)
			rule := platformconfig.RoutePolicyConstraint{ID: "constraint", Hostname: "app.example.test", TenantID: "tenant-a", AppID: "app-a", RoutePolicy: "direct", Enabled: true}
			switch scenario {
			case "unready-candidate":
				current.Route.Content["routes"] = []any{}
			case "exclusion":
				rule.ExcludedEdgeIDs = []string{"edge-a"}
				policy.RouteConstraints = []platformconfig.RoutePolicyConstraint{rule}
			case "higher-quorum":
				policy.MinimumHealthyEdges = 3
			case "disabled":
				rule.Enabled = false
				policy.RouteConstraints = []platformconfig.RoutePolicyConstraint{rule}
			case "owner-mismatch":
				rule.TenantID = "tenant-other"
				policy.RouteConstraints = []platformconfig.RoutePolicyConstraint{rule}
			}
			current.Route.Content["policy"] = policy
			err = platformconfig.RestrictDNSRuntimeSourcePlan(plan, source, pub, []model.PlatformDNSRouteSourcePublication{current})
			if scenario == "owner-mismatch" {
				if err == nil {
					t.Fatal("foreign policy owner accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.Records) != 1 {
				t.Fatal("record was silently omitted")
			}
			if scenario == "exclusion" || scenario == "disabled" {
				if len(plan.Records[0].Targets) != 0 {
					t.Fatal("hard policy bypassed")
				}
			} else if len(plan.Records[0].Targets) != 1 {
				t.Fatal("unready candidate erased positive LKG")
			}
			if scenario == "higher-quorum" && plan.Records[0].MinimumHealthyEdges != 3 {
				t.Fatal("source quorum lowered")
			}
		})
	}
}
