package api

import (
	"context"
	"testing"
	"time"

	"fugue/internal/edgetopology"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
)

func TestTrafficReleaseScopeSeparatesGlobalAndDeclaredCells(t *testing.T) {
	_, s, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	s.auth.EdgeRouteIntentIdentityKeyring = edgeRouteIntentTestKeyring()
	s.auth.PlatformComponentIdentityKeyring = edgeRouteIntentTestKeyring()
	publish := func(generation, cell string) platformConfigCompileResponse {
		t.Helper()
		scope, group := "global", "edge-group-old"
		if cell != "" {
			scope, group = platformconfig.AuthorityCellScope(cell), cell
		}
		now := time.Now().UTC()
		req := platformConfigCompileRequest{Intent: platformconfig.PlatformIntent{Generation: generation, Scope: scope, Routes: []platformconfig.RouteIntent{{Hostname: generation + ".example.test", UpstreamURL: "http://origin:8080", Enabled: true}}}, Policy: platformconfig.PolicySnapshot{Generation: generation + "-policy", Scope: scope, TrafficRolloutCohorts: []platformconfig.TrafficRolloutCohort{{ID: "first", EdgeGroupIDs: []string{group}}}}}
		if cell != "" {
			req.Intent.AuthorityCellID, req.Policy.AuthorityCellID = cell, cell
			req.Intent.EdgeTopology = &edgetopology.Intent{SchemaVersion: edgetopology.SchemaVersion, Cells: []edgetopology.AuthorityCell{{ID: cell}}, Pools: []edgetopology.ServingPool{{ID: "pool-public"}}, Edges: []edgetopology.Edge{{ID: "node-a", AuthorityCellID: cell, ServingPoolIDs: []string{"pool-public"}, Capabilities: []string{"http", "tls"}, FailureDomains: map[string]string{"host": "node-a"}}}}
			req.Intent.DNSConsumers = []platformconfig.DNSConsumerIntent{{NodeID: "dns-a", EdgeGroupID: cell, Zones: []string{"example.test"}, ProbeLabel: "probe", ProbeTTL: 60}}
			topology, err := platformconfig.TrafficConsumerTopologyFromIntent(req.Intent)
			if err != nil {
				t.Fatal(err)
			}
			req.Policy.ConsumerTopologyDigest, _ = platformconfig.Digest(topology)
			req.RuntimeSnapshot = platformconfig.RuntimeSnapshot{CapturedAt: &now, DNSConsumers: []platformconfig.DNSConsumerObservation{{NodeID: "dns-a", EdgeGroupID: cell, ObservedAt: now, A: []string{"8.8.8.8"}}}}
		}
		r := performJSONRequest(t, s, "POST", "/v1/admin/platform-config/compile", admin, req)
		if r.Code != 201 {
			t.Fatal(r.Code, r.Body.String())
		}
		var compiled platformConfigCompileResponse
		mustDecodeJSON(t, r, &compiled)
		r = performJSONRequest(t, s, "POST", "/v1/admin/artifacts/"+compiled.ReleaseArtifact.ID+"/release", admin, model.PlatformArtifactReleaseRequest{ReleaseChannel: "gray", CanaryRuleRef: "cohort=first", Reason: "independent publication fixture"})
		if r.Code != 200 {
			t.Fatal(r.Code, r.Body.String())
		}
		var released model.PlatformArtifactReleaseResponse
		mustDecodeJSON(t, r, &released)
		if cell != "" {
			if _, found, err := s.edgeRouteIntentSnapshotFromTrafficRelease(cell); !found || err == nil {
				t.Fatal("unprepared cell returned serving configuration")
			}
			if _, err := s.preparePlatformReleaseSetConsumers(context.Background(), model.Principal{}, compiled.ReleaseArtifact, released.Release); err != nil {
				t.Fatal(err)
			}
		}
		return compiled
	}
	legacy := publish("legacy-one", "")
	if _, _, found, err := s.selectTrafficRouteRelease("cell-a"); err != nil || found {
		t.Fatal("missing cell borrowed global publication", err)
	}
	cell := publish("cell-one", "cell-a")
	check := func() {
		t.Helper()
		parent, _, found, err := s.selectTrafficRouteRelease("cell-a")
		if err != nil || !found || parent.ID != cell.ReleaseArtifact.ID {
			t.Fatal("cell selection changed", err)
		}
		parent, _, found, err = s.selectTrafficRouteRelease("edge-group-old")
		if err != nil || !found || parent.ID != legacy.ReleaseArtifact.ID {
			t.Fatal("legacy selection changed", err)
		}
		claims := platformcontrol.PlatformComponentIdentityClaims{CredentialID: "kubernetes:test-system:worker:pod-a", Component: model.PlatformConsumerComponentEdgeWorker, NodeID: "node-a", AuthorityID: "cell-a", ScopeKey: platformconfig.AuthorityCellScope("cell-a"), ArtifactKinds: []string{model.PlatformArtifactKindEdgeRouteBundle}}
		token, err := platformcontrol.IssuePlatformComponentIdentity(edgeRouteIntentTestKeyring(), claims, time.Now().UTC(), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		r := performJSONRequest(t, s, "GET", "/v1/platform-state/consumers/assignment?serving_only=true", token, nil)
		if r.Code != 200 {
			t.Fatal("cell serving assignment unavailable", r.Code, r.Body.String())
		}
		var assignments model.PlatformConsumerAssignmentResponse
		mustDecodeJSON(t, r, &assignments)
		if len(assignments.Assignments) != 1 || assignments.Assignments[0].ReleaseSetID != cell.ReleaseArtifact.ID || assignments.Assignments[0].ScopeKey != claims.ScopeKey {
			t.Fatal("serving assignment crossed publication scopes")
		}
		for _, tc := range []struct {
			scope, authority, group string
			status                  int
		}{
			{platformconfig.AuthorityCellScope("cell-a"), "cell-a", "cell-a", 200},
			{platformconfig.AuthorityCellScope("cell-b"), "cell-a", "cell-a", 403},
			{platformconfig.AuthorityCellScope("cell-a"), "cell-a", "cell-b", 403},
			{platformconfig.AuthorityCellScope("cell-a"), "cell-a", "edge-group-old", 403},
			{"global", "cell-a", "cell-a", 503},
		} {
			claims := edgeRouteIntentTestClaims(model.PlatformConsumerComponentEdgeControl, tc.scope, []string{model.PlatformArtifactKindEdgeRouteIntent})
			claims.AuthorityID = tc.authority
			token, err := platformcontrol.IssuePlatformComponentIdentity(edgeRouteIntentTestKeyring(), *claims, time.Now().UTC(), time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			r := performJSONRequest(t, s, "GET", "/v1/edge/route-intents?edge_group_id="+tc.group, token, nil)
			if r.Code != tc.status {
				t.Fatal(tc, r.Code, r.Body.String())
			}
			if r.Code == 200 {
				var snapshot model.EdgeRouteIntentSnapshot
				mustDecodeJSON(t, r, &snapshot)
				if snapshot.TrafficRelease == nil || snapshot.TrafficRelease.ScopeKey != tc.scope || snapshot.TrafficRelease.ReleaseSetID != cell.ReleaseArtifact.ID {
					t.Fatal("scoped response lost release binding")
				}
			}
		}
	}
	check()
	legacy = publish("legacy-two", "")
	check()
}

func TestPodPublicationScopeMustMatchExplicitAuthority(t *testing.T) {
	for _, component := range []string{"edge-control", "edge-worker", "dns-server"} {
		kind := "edge_route_bundle"
		if component == "edge-control" {
			kind = "edge_route_intent"
		}
		if component == "dns-server" {
			kind = "dns_answer_bundle"
		}
		for _, tc := range []struct {
			authority, scope string
			valid            bool
		}{
			{"cell-a", "authority-cell:cell-a", true}, {"cell-a", "global", true},
			{"cell-a", "authority-cell:cell-b", false}, {"", "authority-cell:cell-a", false},
		} {
			raw := `{"version":"v1","component":"` + component + `","scope_key":"` + tc.scope + `","artifact_kinds":["` + kind + `"],"authority_id":"` + tc.authority + `"}`
			if _, valid := decodePlatformConsumerIdentityPolicy(raw); valid != tc.valid {
				t.Fatal(component, tc, valid)
			}
		}
	}
}
