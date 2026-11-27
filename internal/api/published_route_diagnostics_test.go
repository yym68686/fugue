package api

import (
	"net/http"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
)

func routeDiagnosticFixture(t *testing.T) *Server {
	t.Helper()
	state := newStoreTestDatabase(t, "route-diagnostics")
	if err := state.Init(); err != nil {
		t.Fatal(err)
	}
	s := NewServer(state, auth.New(state, "diagnostic-admin"), nil, ServerConfig{BundleSigningKey: "synthetic-route-diagnostic", BundleSigningKeyID: "key"})
	now := time.Now().UTC()
	for i, suffix := range []string{"a", "b"} {
		if _, err := state.UpdateDNSHeartbeat(model.DNSNode{ID: "dns-" + suffix, EdgeGroupID: "edge-group-" + suffix, Zone: "example.test", PublicIPv4: []string{"8.8.8.8", "8.8.4.4"}[i], LastSeenAt: &now}); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func TestAdminRouteDiagnosticsDoNotCombineFullAndGrayPublications(t *testing.T) {
	s := routeDiagnosticFixture(t)
	seedVerifiedDNSDelegationFixture(t, s, "example.test")
	baseline, _, _, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", "gray")
	if err != nil {
		t.Fatal(err)
	}
	oldSets, err := s.currentReleaseSetExpectations(baseline)
	if err != nil {
		t.Fatal(err)
	}
	_, full, _, _, err := s.store.ReleasePlatformArtifact(baseline.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "full"}, platformProducerPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	for _, set := range oldSets {
		set.ID, set.ArtifactReleaseID, set.Revision = model.NewID("expectedset"), full.ID, set.Revision+1
		if _, err := s.store.CreatePlatformExpectedConsumerSet(set); err != nil {
			t.Fatal(err)
		}
	}
	// A newer full release of the same parent supersedes the old gray lane.
	if r := performJSONRequest(t, s, http.MethodGet, "/v1/admin/routes", "diagnostic-admin", nil); r.Code != 200 {
		t.Fatal(r.Code, r.Body.String())
	}
	r := performJSONRequest(t, s, http.MethodPost, "/v1/admin/platform-config/compile", "diagnostic-admin", platformConfigCompileRequest{
		Intent: platformconfig.PlatformIntent{Generation: "new-gray", Routes: []platformconfig.RouteIntent{{Hostname: "candidate.example.test", UpstreamURL: "http://candidate:8080", Enabled: true}}},
		Policy: platformconfig.PolicySnapshot{Generation: "new-policy", TrafficRolloutCohorts: []platformconfig.TrafficRolloutCohort{{ID: "first", EdgeGroupIDs: []string{"edge-group-a"}}, {ID: "complete", EdgeGroupIDs: []string{"edge-group-a", "edge-group-b"}}}},
	})
	if r.Code != 201 {
		t.Fatal(r.Code, r.Body.String())
	}
	var compiled platformConfigCompileResponse
	mustDecodeJSON(t, r, &compiled)
	_, gray, _, _, err := s.store.ReleasePlatformArtifact(compiled.ReleaseArtifact.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "gray", CanaryRuleRef: "cohort=first"}, platformProducerPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	topology := platformcontrol.ExpectedConsumerTopology{EdgeNodes: []model.EdgeNode{{ID: "edge-a", EdgeGroupID: "edge-group-a"}, {ID: "edge-b", EdgeGroupID: "edge-group-b"}}, DNSNodes: []model.DNSNode{{ID: "dns-a", PhysicalNodeID: "dns-a", EdgeGroupID: "edge-group-a", Zone: "example.test"}, {ID: "dns-b", PhysicalNodeID: "dns-b", EdgeGroupID: "edge-group-b", Zone: "example.test"}}}
	prepare := func(release model.PlatformArtifactRelease, revision int64) {
		t.Helper()
		for i, child := range []model.PlatformArtifact{compiled.RouteArtifact, compiled.DNSArtifact, compiled.TLSArtifact} {
			set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{ReleaseSetID: compiled.ReleaseArtifact.ID, ArtifactReleaseID: release.ID, ArtifactKind: child.ArtifactKind, ScopeKey: "global", Generation: child.Generation, Revision: revision + int64(i), Topology: topology})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.store.CreatePlatformExpectedConsumerSet(set); err != nil {
				t.Fatal(err)
			}
		}
	}
	prepare(gray, 100)
	if r := performJSONRequest(t, s, http.MethodGet, "/v1/admin/routes", "diagnostic-admin", nil); r.Code != 409 {
		t.Fatal(r.Code, r.Body.String())
	}
	for group, want := range map[string]string{"edge-group-a": gray.ID, "edge-group-b": full.ID} {
		r := performJSONRequest(t, s, http.MethodGet, "/v1/admin/routes?edge_group_id="+group, "diagnostic-admin", nil)
		var result publishedRouteServingModesResponse
		mustDecodeJSON(t, r, &result)
		if r.Code != 200 || result.TrafficRelease == nil || result.TrafficRelease.ReleaseID != want {
			t.Fatal(group, r.Code, r.Body.String())
		}
	}
}
