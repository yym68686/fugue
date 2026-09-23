package api

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/store"
)

func routeDiagnosticFixture(t *testing.T) (string, *Server) {
	t.Helper()
	path := t.TempDir() + "/state.json"
	state := store.New(path)
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
	return path, s
}

func TestAdminRouteDiagnosticsUsePublishedArtifactsAndFreshFacts(t *testing.T) {
	path, s := routeDiagnosticFixture(t)
	s.platformRoutes = []model.PlatformRoute{{Hostname: "ambient.example.test", UpstreamURL: "http://ambient:8080"}}
	for _, endpoint := range []string{"/v1/admin/routes", "/v1/admin/routes/explain/ambient.example.test"} {
		r := performJSONRequest(t, s, http.MethodGet, endpoint, "diagnostic-admin", nil)
		if r.Code != 503 || strings.Contains(r.Body.String(), "http://ambient") {
			t.Fatal("missing publication used ambient input", r.Code, r.Body.String())
		}
	}
	seedVerifiedDNSDelegationFixture(t, s, "example.test", platformconfig.RouteIntent{Hostname: "published.example.test", UpstreamURL: "http://published:8080", Enabled: true})
	if _, err := s.store.PutEdgeRoutePolicy(model.EdgeRoutePolicy{Hostname: "published.example.test", AppID: "synthetic-app", TenantID: "synthetic-tenant", EdgeGroupID: "edge-group-unpublished", RoutePolicy: model.EdgeRoutePolicyEnabled}); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	list := performJSONRequest(t, s, http.MethodGet, "/v1/admin/routes", "diagnostic-admin", nil)
	var result publishedRouteServingModesResponse
	mustDecodeJSON(t, list, &result)
	if list.Code != 200 || result.TrafficRelease == nil || len(result.Routes) != 2 || len(result.HealthyEdgeGroups) != 2 || !result.HealthyEdgeGroups["edge-group-a"] || !result.HealthyEdgeGroups["edge-group-b"] {
		t.Fatal(list.Code, list.Body.String())
	}
	for _, suffix := range []string{"", "?edge_group_id=edge-group-a"} {
		r := performJSONRequest(t, s, http.MethodGet, "/v1/admin/routes/explain/published.example.test"+suffix, "diagnostic-admin", nil)
		var response struct {
			Explain publishedRouteExplainResponse `json:"explain"`
		}
		mustDecodeJSON(t, r, &response)
		if r.Code != 200 || response.Explain.TrafficRelease.ReleaseID != result.TrafficRelease.ReleaseID || response.Explain.ServingMode != "edge" || response.Explain.Route.UpstreamURL != "http://published:8080" || r.Header().Get("Cache-Control") != "private, no-store" {
			t.Fatal(r.Code, r.Body.String())
		}
	}
	missing := performJSONRequest(t, s, http.MethodGet, "/v1/admin/routes/explain/ambient.example.test", "diagnostic-admin", nil)
	if missing.Code != 200 || !strings.Contains(missing.Body.String(), `"serving_mode":"unrouted"`) {
		t.Fatal(missing.Code, missing.Body.String())
	}
	for _, query := range []string{"?edge_group_id=", "?edge_group_id=BAD", "?edge_group_id=edge-group-a&edge_group_id=edge-group-b"} {
		if r := performJSONRequest(t, s, http.MethodGet, "/v1/admin/routes"+query, "diagnostic-admin", nil); r.Code != 400 {
			t.Fatal(query, r.Code)
		}
	}
	after, _ := os.ReadFile(path)
	if !reflect.DeepEqual(raw, after) {
		t.Fatal("read-only diagnostic changed persistent configuration")
	}
	for _, scenario := range []string{"stale facts", "foreign fence", "release_set", "edge_route_bundle", "dns_answer_bundle", "caddy_route_config", "missing membership"} {
		t.Run(scenario, func(t *testing.T) {
			var st model.State
			if err := json.Unmarshal(raw, &st); err != nil {
				t.Fatal(err)
			}
			for i := range st.PlatformArtifacts {
				if st.PlatformArtifacts[i].ArtifactKind == scenario {
					st.PlatformArtifacts[i].Provenance.Signature = "corrupt"
				}
			}
			for i := range st.PlatformConsumerInstances {
				if scenario == "stale facts" {
					st.PlatformConsumerInstances[i].LastHeartbeatAt = time.Now().Add(-time.Hour)
				}
				if scenario == "foreign fence" {
					st.PlatformConsumerInstances[i].FencingToken++
				}
			}
			if scenario == "missing membership" {
				st.ExpectedConsumerSets = nil
			}
			damaged, _ := json.Marshal(st)
			if err := os.WriteFile(path, damaged, 0600); err != nil {
				t.Fatal(err)
			}
			r := performJSONRequest(t, s, http.MethodGet, "/v1/admin/routes/explain/published.example.test", "diagnostic-admin", nil)
			if scenario == "stale facts" || scenario == "foreign fence" {
				var response struct {
					Explain publishedRouteExplainResponse `json:"explain"`
				}
				mustDecodeJSON(t, r, &response)
				if r.Code != 200 || response.Explain.ServingMode != "degraded" || response.Explain.Route.UpstreamURL != "" {
					t.Fatal(r.Code, r.Body.String())
				}
			} else if r.Code != 503 {
				t.Fatal(r.Code, r.Body.String())
			}
			after, _ := os.ReadFile(path)
			if !reflect.DeepEqual(damaged, after) {
				t.Fatal("diagnostic repaired or changed persistent data")
			}
		})
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := s.publishedRouteDiagnostics(ctx, ""); err != context.Canceled {
		t.Fatal(err)
	}
}

func TestAdminRouteDiagnosticsDoNotCombineFullAndGrayPublications(t *testing.T) {
	_, s := routeDiagnosticFixture(t)
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
