package platformproducer

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/edgetopology"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func placementFixture(t *testing.T) (Policy, platformconfig.PolicySnapshot, StaticIntentInput) {
	t.Helper()
	previous := edgetopology.Intent{SchemaVersion: edgetopology.SchemaVersion,
		Cells: []edgetopology.AuthorityCell{{ID: "cell-a", LegacyGroupID: "edge-group-first"}, {ID: "cell-b", LegacyGroupID: "edge-group-second"}},
		Pools: []edgetopology.ServingPool{{ID: "pool-public"}},
		Edges: []edgetopology.Edge{
			{ID: "node-a", AuthorityCellID: "cell-a", ServingPoolIDs: []string{"pool-public"}, Capabilities: []string{"http", "tls"}, FailureDomains: map[string]string{"host": "node-a", "provider": "provider-a"}, Labels: map[string]string{"country": "us"}},
			{ID: "node-b", AuthorityCellID: "cell-b", ServingPoolIDs: []string{"pool-public"}, Capabilities: []string{"http", "tls"}, FailureDomains: map[string]string{"host": "node-b", "provider": "provider-b"}, Labels: map[string]string{"country": "us"}},
		}}
	next := previous.Clone()
	for i := range next.Cells {
		next.Cells[i].LegacyGroupID = ""
	}
	expired := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	rule := normalizedRouteConstraint(platformconfig.RoutePolicyConstraint{ID: "policy-a", Hostname: "app.example.test", AppID: "app-a", TenantID: "tenant-a", MatchScope: "tenant_hostname", EdgeGroupID: "edge-group-second", ExcludedEdgeIDs: []string{"node-a"}, ExcludedEdgeGroupIDs: []string{"edge-group-first"}, ExclusionReason: "retained hold", ExclusionExpiresAt: &expired, ExclusionOwnerDigest: "sha256:" + strings.Repeat("b", 64), ExclusionGeneration: 9007199254740993, ExclusionFence: "fence-a", MinHealthyEdgeNodes: 1, RoutePolicy: model.EdgeRoutePolicyEnabled, Enabled: true})
	digest, err := RouteConstraintDigest(rule)
	if err != nil {
		t.Fatal(err)
	}
	p := Policy{SchemaVersion: Schema, Generation: "producer", PublicationRole: platformconfig.PublicationRoleCellRoutes, AuthorityCellID: "cell-a", TargetScope: "authority-cell:cell-a", Mode: "shadow", InputSource: "business-static-intent", StaticIntentArtifactID: "static", StaticIntentDigest: "sha256:" + strings.Repeat("a", 64), DNSPolicyArtifactID: "projection", DNSPolicyDigest: "sha256:" + strings.Repeat("b", 64), RequireRouteDefaults: true, RequireApplicationDomains: true, IntervalSeconds: 30, RefreshSeconds: 120,
		RoutePlacementTransition: &RoutePlacementTransition{PreviousTopology: previous, NextTopology: next, Constraints: []RouteConstraintTransition{{Source: rule, SourceDigest: digest}}}}
	policy := platformconfig.NormalizePolicySnapshot(platformconfig.PolicySnapshot{SchemaVersion: platformconfig.SchemaVersion, Generation: "source", Scope: p.TargetScope, PublicationRole: p.PublicationRole, AuthorityCellID: p.AuthorityCellID, ConsumerTopologyDigest: "sha256:" + strings.Repeat("c", 64), MinimumHealthyEdges: 1, MaxStaleSeconds: 300, TLSReadiness: &platformconfig.ReadinessProbePolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 5, FactFreshnessSeconds: 120, MaxConcurrency: 8, MaxProbes: 4096}, RouteConstraints: []platformconfig.RoutePolicyConstraint{rule}})
	cohort := next.Clone()
	cohort.Cells, cohort.Edges = cohort.Cells[:1], cohort.Edges[:1]
	static := StaticIntentInput{PublicationRole: p.PublicationRole, Scope: p.TargetScope, AuthorityCellID: p.AuthorityCellID, EdgeTopology: &cohort}
	return p, policy, static
}

func TestPlacementTransitionPreservesPhysicalEligibilityAndExpiredHold(t *testing.T) {
	p, policy, static := placementFixture(t)
	before, _ := json.Marshal(policy)
	pinBefore, _ := json.Marshal(p)
	if err := validatePlacementTransitionTopology(p, static); err != nil {
		t.Fatal(err)
	}
	got, err := ApplyRoutePlacementTransition(p, policy)
	if err != nil || ValidateRoutePlacementOutput(p, got) != nil {
		t.Fatal("transition failed", err)
	}
	r := got.RouteConstraints[0]
	if r.EdgeGroupID != "cell-b" || !reflect.DeepEqual(r.ExcludedEdgeGroupIDs, []string{"cell-a"}) || got.Generation == policy.Generation {
		t.Fatalf("neutral placement or lineage missing: %+v", r)
	}
	restored := r
	restored.EdgeGroupID, restored.ExcludedEdgeGroupIDs = policy.RouteConstraints[0].EdgeGroupID, policy.RouteConstraints[0].ExcludedEdgeGroupIDs
	if !reflect.DeepEqual(restored, policy.RouteConstraints[0]) {
		t.Fatal("authority rewrite changed ownership, physical exclusions, lease metadata or minimums")
	}
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	routes := []platformconfig.CompiledRoute{{RouteIntent: platformconfig.RouteIntent{Hostname: r.Hostname, TenantID: r.TenantID, AppID: "sibling-app", Enabled: true}}}
	oldRoutes, err := platformconfig.ApplyRoutePolicyConstraints(routes, policy, &at)
	if err != nil {
		t.Fatal(err)
	}
	newRoutes, err := platformconfig.ApplyRoutePolicyConstraints(routes, got, &at)
	if err != nil || newRoutes[0].ExclusionLifecycle != model.EdgeExclusionLifecycleExpiredHold {
		t.Fatal("expired exclusion hold was removed", err)
	}
	for i, edge := range p.RoutePlacementTransition.PreviousTopology.Edges {
		oldGroup := p.RoutePlacementTransition.PreviousTopology.Cells[i].ServingGroupID()
		newGroup := p.RoutePlacementTransition.NextTopology.Cells[i].ServingGroupID()
		oldAllowed := platformconfig.DNSPlacementAllowsEdge(platformconfig.DNSIntent{}, oldRoutes, edge.ID, oldGroup)
		newAllowed := platformconfig.DNSPlacementAllowsEdge(platformconfig.DNSIntent{}, newRoutes, edge.ID, newGroup)
		if oldAllowed != newAllowed || newAllowed != (edge.ID == "node-b") {
			t.Fatal("physical candidate eligibility changed")
		}
	}
	after, _ := json.Marshal(policy)
	pinAfter, _ := json.Marshal(p)
	if string(before) != string(after) || string(pinBefore) != string(pinAfter) {
		t.Fatal("transition mutated immutable input")
	}
}

func TestPlacementTransitionRejectsTopologyAndScopeChanges(t *testing.T) {
	for _, scenario := range []string{"no change", "rename", "rebind", "remove node", "pool", "capability", "country", "risk", "source digest", "source mutation", "duplicate", "global", "complete", "static identity"} {
		t.Run(scenario, func(t *testing.T) {
			p, _, static := placementFixture(t)
			transition := p.RoutePlacementTransition
			switch scenario {
			case "no change":
				transition.NextTopology = transition.PreviousTopology.Clone()
			case "rename":
				transition.NextTopology.Cells[0].ID = "cell-other"
			case "rebind":
				transition.NextTopology.Edges[0].AuthorityCellID = "cell-b"
			case "remove node":
				transition.NextTopology.Edges = transition.NextTopology.Edges[1:]
			case "pool":
				transition.NextTopology.Edges[0].ServingPoolIDs = []string{"pool-other"}
			case "capability":
				transition.NextTopology.Edges[0].Capabilities = []string{"http"}
			case "country":
				transition.NextTopology.Edges[0].Labels["country"] = "de"
			case "risk":
				transition.NextTopology.Edges[0].FailureDomains["provider"] = "provider-b"
			case "source digest":
				transition.Constraints[0].SourceDigest = "sha256:" + strings.Repeat("f", 64)
			case "source mutation":
				transition.Constraints[0].Source.MinHealthyEdgeNodes++
			case "duplicate":
				transition.Constraints = append(transition.Constraints, transition.Constraints[0])
			case "global":
				p.AuthorityCellID, p.TargetScope = "", "global"
			case "complete":
				p.PublicationRole = ""
			case "static identity":
				static.EdgeTopology.Edges[0].FailureDomains["provider"] = "other"
			}
			if validatePlacementTransitionTopology(p, static) == nil {
				t.Fatal("unauthorized transition accepted")
			}
		})
	}
}

func TestPlacementTransitionRequiresExactCaptureAndPublication(t *testing.T) {
	for _, output := range []bool{false, true} {
		for _, scenario := range []string{"exact", "owner", "hostname", "policy identity", "minimum", "physical exclusion", "group", "fence", "expiry", "disabled", "missing", "unlisted", "scope"} {
			t.Run(fmtTestName(output, scenario), func(t *testing.T) {
				p, input, _ := placementFixture(t)
				if output {
					var err error
					input, err = ApplyRoutePlacementTransition(p, input)
					if err != nil {
						t.Fatal(err)
					}
				}
				r := &input.RouteConstraints[0]
				switch scenario {
				case "owner":
					r.TenantID = "other-tenant"
				case "hostname":
					r.Hostname = "other.example.test"
				case "policy identity":
					r.ID = "other-policy"
				case "minimum":
					r.MinHealthyEdgeNodes = 0
				case "physical exclusion":
					r.ExcludedEdgeIDs = nil
				case "group":
					r.EdgeGroupID = "cell-other"
				case "fence":
					r.ExclusionFence = "changed"
				case "expiry":
					r.ExclusionExpiresAt = nil
				case "disabled":
					r.Enabled = false
				case "missing":
					input.RouteConstraints = nil
				case "unlisted":
					extra := p.RoutePlacementTransition.Constraints[0].Source
					extra.Hostname, extra.ID = "other.example.test", "other-policy"
					input.RouteConstraints = append(input.RouteConstraints, extra)
				case "scope":
					input.Scope = "authority-cell:cell-other"
				}
				_, err := transitionRouteConstraints(p, input, output)
				if (err == nil) != (scenario == "exact") {
					t.Fatal(scenario, err)
				}
			})
		}
	}
}

func fmtTestName(output bool, name string) string {
	if output {
		return "publication/" + name
	}
	return "capture/" + name
}

func TestPlacementTransitionDecodeIsOptInAndStrict(t *testing.T) {
	p, _, _ := placementFixture(t)
	raw, _ := json.Marshal(p)
	a := model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, ScopeKey: Scope + ":cell-a", Generation: p.Generation}
	if json.Unmarshal(raw, &a.Content) != nil {
		t.Fatal("fixture")
	}
	// Preserve large exclusion generations when emulating the artifact store.
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.UseNumber()
	if err := d.Decode(&a.Content); err != nil {
		t.Fatal(err)
	}
	if _, err := Decode(a); err != nil {
		t.Fatal(err)
	}
	a.Content["route_placement_transition"] = nil
	if _, err := Decode(a); err == nil {
		t.Fatal("explicit null accepted")
	}
	delete(a.Content, "route_placement_transition")
	if _, err := Decode(a); err != nil {
		t.Fatal("unchanged producer rejected", err)
	}
}
