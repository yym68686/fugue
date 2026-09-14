package platformconfig

import (
	"testing"
	"time"

	"fugue/internal/model"
)

func TestProjectPolicySnapshotSeparatesDesiredConstraintsFromFacts(t *testing.T) {
	expires := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	routes := []model.EdgeRoutePolicy{{ID: "route-policy", Hostname: "App.Example.", AppID: "app", TenantID: "tenant", EdgeGroupID: "edge-group-a", ExcludedEdgeIDs: []string{"edge-1"}, ExclusionReason: "maintenance", ExclusionExpiresAt: &expires, MinHealthyEdgeNodes: 2, RoutePolicy: model.EdgeRoutePolicyCanary, Enabled: true, ExclusionEvidenceFresh: true, ExclusionEvidenceCheckedAt: &expires, ExclusionOwnerDigest: "runtime-fact"}}
	traffic := []model.AppTrafficPolicy{{ID: "traffic-policy", AppID: "app", Mode: model.AppTrafficModeCanary, StableReleaseID: "stable", CandidateReleaseID: "candidate", StableWeight: 90, CandidateWeight: 10, StickyHeader: "X-Release", StickyCookie: "release"}}
	policy, err := ProjectPolicySnapshot(PolicySnapshot{RequireTLSReady: true, RequireRouteReady: true, MinimumHealthyEdges: 2, MaxStaleSeconds: 90}, routes, traffic, "policy-projected")
	if err != nil {
		t.Fatal(err)
	}
	if policy.RouteConstraints[0].Hostname != "app.example" || policy.RouteConstraints[0].MinHealthyEdgeNodes != 2 || policy.TrafficConstraints[0].CandidateWeight != 10 || policy.Generation != "policy-projected" {
		t.Fatalf("policy projection lost desired fields or leaked facts: %+v", policy)
	}
	if err := ValidatePolicySnapshot(policy); err != nil {
		t.Fatal(err)
	}
}

func TestPolicyRuleValidationRejectsAmbiguousConstraints(t *testing.T) {
	base := PolicySnapshot{Generation: "policy", RouteConstraints: []RoutePolicyConstraint{{ID: "a", Hostname: "a.example", RoutePolicy: model.EdgeRoutePolicyEnabled}}, TrafficConstraints: []TrafficPolicyConstraint{{ID: "t", AppID: "app", Mode: model.AppTrafficModeCanary, StableWeight: 90, CandidateWeight: 10}}}
	for _, mutate := range []func(*PolicySnapshot){
		func(p *PolicySnapshot) {
			p.RouteConstraints = append(p.RouteConstraints, RoutePolicyConstraint{ID: "b", Hostname: "a.example", RoutePolicy: model.EdgeRoutePolicyEnabled})
		},
		func(p *PolicySnapshot) { p.TrafficConstraints[0].CandidateWeight = 20 },
		func(p *PolicySnapshot) { p.TrafficConstraints[0].Mode = "script" },
		func(p *PolicySnapshot) { p.RouteConstraints[0].RoutePolicy = "unknown" },
	} {
		invalid := base
		invalid.RouteConstraints = append([]RoutePolicyConstraint(nil), base.RouteConstraints...)
		invalid.TrafficConstraints = append([]TrafficPolicyConstraint(nil), base.TrafficConstraints...)
		mutate(&invalid)
		if err := ValidatePolicySnapshot(invalid); err == nil {
			t.Fatal("invalid policy constraint accepted")
		}
	}
}

func TestApplyRoutePolicyConstraintsChangesArtifactOnly(t *testing.T) {
	routes := []CompiledRoute{{RouteIntent: RouteIntent{Hostname: "app.example", UpstreamURL: "http://origin", Enabled: true, RoutePolicy: model.EdgeRoutePolicyEnabled}}}
	expires := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	policy := PolicySnapshot{RouteConstraints: []RoutePolicyConstraint{{ID: "route", Hostname: "app.example", RoutePolicy: model.EdgeRoutePolicyRouteAOnly, Enabled: false, MinHealthyEdgeNodes: 2, ExcludedEdgeIDs: []string{"edge-a"}, ExclusionReason: "maintenance", ExclusionExpiresAt: &expires}}}
	got, err := ApplyRoutePolicyConstraints(routes, policy)
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Enabled || got[0].UpstreamURL != "http://origin" || got[0].Status != model.EdgeRouteStatusDisabled || got[0].RoutePolicy != model.EdgeRoutePolicyRouteAOnly || got[0].MinHealthyEdgeNodes != 2 || len(got[0].ExcludedEdgeIDs) != 1 || got[0].ExclusionReason != "maintenance" {
		t.Fatalf("route policy was not applied: %+v", got[0])
	}
	if routes[0].Status != "" || routes[0].UpstreamURL != "http://origin" || routes[0].Enabled != true {
		t.Fatal("policy application mutated caller route")
	}
}

func TestApplyRoutePolicyConstraintsForPlacementMaterializesEdgeGroup(t *testing.T) {
	routes := []CompiledRoute{{RouteIntent: RouteIntent{Hostname: "app.example", UpstreamURL: "http://origin", Enabled: true, RoutePolicy: model.EdgeRoutePolicyEnabled}}}
	policy := PolicySnapshot{RouteConstraints: []RoutePolicyConstraint{{ID: "route", Hostname: "app.example", EdgeGroupID: "edge-group-a", MinHealthyEdgeNodes: 2, RoutePolicy: model.EdgeRoutePolicyEnabled, Enabled: true}}}
	got, err := ApplyRoutePolicyConstraintsForPlacement(routes, policy)
	if err != nil {
		t.Fatal(err)
	}
	if got[0].EdgeGroupMode != model.PlatformRouteEdgeGroupModePinned || got[0].EdgeGroupID != "edge-group-a" || got[0].MinHealthyEdgeNodes != 2 {
		t.Fatalf("placement constraint was not materialized: %+v", got[0])
	}
	if _, err := ApplyRoutePolicyConstraints(routes, policy); err == nil {
		t.Fatal("strict serving compiler accepted unresolved edge-group constraint")
	}
}
