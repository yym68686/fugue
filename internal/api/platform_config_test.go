package api

import (
	"net/http"
	"testing"

	"fugue/internal/model"
)

func TestCompilePlatformConfigPersistsLineageAndReleaseSet(t *testing.T) {
	_, server, tenantKey, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	request := map[string]any{
		"intent": map[string]any{
			"generation": "intent-api-0001",
			"scope":      "global",
			"routes": []any{map[string]any{
				"hostname": "app.fugue.pro", "upstream_url": "http://app:8080", "enabled": true,
			}},
		},
		"policy": map[string]any{
			"generation":            "policy-api-0001",
			"scope":                 "global",
			"require_tls_ready":     true,
			"minimum_healthy_edges": 1,
			"dependency_order":      []string{"route", "tls", "dns"},
		},
		"input_snapshot": map[string]any{"topology_revision": "topology-api-1"},
	}
	compiled := performJSONRequest(t, server, http.MethodPost, "/v1/admin/platform-config/compile", platformAdminKey, request)
	if compiled.Code != http.StatusCreated {
		t.Fatalf("expected compile status %d, got %d body=%s", http.StatusCreated, compiled.Code, compiled.Body.String())
	}
	var response struct {
		Lineage         map[string]any         `json:"lineage"`
		ReleaseSet      map[string]any         `json:"release_set"`
		RouteArtifact   model.PlatformArtifact `json:"route_artifact"`
		ReleaseArtifact model.PlatformArtifact `json:"release_artifact"`
	}
	mustDecodeJSON(t, compiled, &response)
	if response.Lineage["intent_digest"] == "" || response.Lineage["policy_digest"] == "" || response.Lineage["compiler_version"] == "" {
		t.Fatalf("lineage missing from compile response: %+v", response.Lineage)
	}
	if response.RouteArtifact.Metadata["intent_digest"] == "" || response.RouteArtifact.Metadata["policy_digest"] == "" {
		t.Fatalf("route artifact missing lineage metadata: %+v", response.RouteArtifact.Metadata)
	}
	if response.ReleaseArtifact.ArtifactKind != model.PlatformArtifactKindReleaseSet {
		t.Fatalf("expected release set artifact, got %+v", response.ReleaseArtifact)
	}
	if len(response.ReleaseSet) == 0 {
		t.Fatal("release set response is empty")
	}

	lineage := performJSONRequest(t, server, http.MethodGet, "/v1/admin/artifacts/"+response.RouteArtifact.ID+"/lineage", platformAdminKey, nil)
	if lineage.Code != http.StatusOK {
		t.Fatalf("expected lineage status %d, got %d body=%s", http.StatusOK, lineage.Code, lineage.Body.String())
	}
	if forbidden := performJSONRequest(t, server, http.MethodPost, "/v1/admin/platform-config/compile", tenantKey, request); forbidden.Code != http.StatusForbidden {
		t.Fatalf("tenant key must not compile platform config, got %d body=%s", forbidden.Code, forbidden.Body.String())
	}
}
