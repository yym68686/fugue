package api

import (
	"net/http"
	"strings"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/store"
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
	releaseLineage := performJSONRequest(t, server, http.MethodGet, "/v1/admin/artifacts/"+response.ReleaseArtifact.ID+"/lineage", platformAdminKey, nil)
	if releaseLineage.Code != http.StatusOK || !strings.Contains(releaseLineage.Body.String(), `"dependencies"`) {
		t.Fatalf("release set lineage must expose child dependencies: %d %s", releaseLineage.Code, releaseLineage.Body.String())
	}
	if forbidden := performJSONRequest(t, server, http.MethodPost, "/v1/admin/platform-config/compile", tenantKey, request); forbidden.Code != http.StatusForbidden {
		t.Fatalf("tenant key must not compile platform config, got %d body=%s", forbidden.Code, forbidden.Body.String())
	}
}

func TestReleaseSetInvariantRequiresTrafficArtifactGroup(t *testing.T) {
	valid := model.PlatformArtifact{
		ArtifactKind: model.PlatformArtifactKindReleaseSet,
		Content: map[string]any{
			"artifact_ids":   []any{"route-1", "dns-1", "tls-1"},
			"artifact_kinds": []any{model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindDNSAnswerBundle, model.PlatformArtifactKindCaddyRouteConfig},
			"lineage":        map[string]any{"intent_digest": "sha256:intent", "policy_digest": "sha256:policy"},
		},
	}
	if result := platformArtifactInvariantValidation(valid); !result.Pass {
		t.Fatalf("valid traffic release set rejected: %+v", result)
	}
	valid.Content["artifact_kinds"] = []any{model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindDNSAnswerBundle}
	if result := platformArtifactInvariantValidation(valid); result.Pass {
		t.Fatalf("incomplete traffic release set unexpectedly passed: %+v", result)
	}
}

func TestReleaseSetReferenceValidationRequiresExistingValidatedChildren(t *testing.T) {
	s := store.New(t.TempDir() + "/store.json")
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	artifact := model.PlatformArtifact{
		ArtifactKind: model.PlatformArtifactKindReleaseSet,
		Scope:        model.PlatformArtifactScope{ScopeType: "global", Key: "global"},
		Generation:   "release-invalid-reference",
		Content: map[string]any{
			"artifact_ids":   []any{"missing-route"},
			"artifact_kinds": []any{model.PlatformArtifactKindEdgeRouteBundle},
		},
		Metadata: map[string]string{"intent_digest": "sha256:intent", "policy_digest": "sha256:policy"},
	}
	result := server.validateReleaseSetReferences(artifact)
	if result.Pass || !strings.Contains(result.Message, "unknown artifact") {
		t.Fatalf("expected missing child rejection, got %+v", result)
	}
}

func TestReleaseSetPromotionRejectsMissingChildren(t *testing.T) {
	stateStore, server, _, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	artifact, err := stateStore.CreatePlatformArtifact(model.PlatformArtifact{
		ArtifactKind: model.PlatformArtifactKindReleaseSet,
		Scope:        model.PlatformArtifactScope{ScopeType: "global", Key: "global"},
		Generation:   "release-promotion-invalid-reference",
		Content: map[string]any{
			"artifact_ids":   []any{"missing-route"},
			"artifact_kinds": []any{model.PlatformArtifactKindEdgeRouteBundle},
		},
		Metadata: map[string]string{"intent_digest": "sha256:intent", "policy_digest": "sha256:policy"},
	})
	if err != nil {
		t.Fatalf("create release set artifact: %v", err)
	}
	response := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts/"+artifact.ID+"/release", platformAdminKey, map[string]any{
		"release_channel": "full",
		"idempotency_key": "release-invalid-reference",
	})
	if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), "unknown artifact") {
		t.Fatalf("expected missing child rejection at promotion, got %d body=%s", response.Code, response.Body.String())
	}
}

func TestCompilePlatformConfigFailurePreservesPreviousImmutableArtifacts(t *testing.T) {
	stateStore, server, _, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	request := map[string]any{
		"intent": map[string]any{
			"generation": "compile-recovery-intent-1",
			"scope":      "global",
			"routes": []any{map[string]any{
				"hostname": "stable.fugue.pro", "upstream_url": "http://stable:8080", "enabled": true,
			}},
		},
		"policy": map[string]any{
			"generation":            "compile-recovery-policy-1",
			"scope":                 "global",
			"require_tls_ready":     true,
			"minimum_healthy_edges": 1,
			"dependency_order":      []string{"route", "tls", "dns"},
		},
		"input_snapshot": map[string]any{"topology_revision": "compile-recovery-topology-1"},
	}
	first := performJSONRequest(t, server, http.MethodPost, "/v1/admin/platform-config/compile", platformAdminKey, request)
	if first.Code != http.StatusCreated {
		t.Fatalf("initial compile failed: %d %s", first.Code, first.Body.String())
	}
	var firstResponse struct {
		RouteArtifact model.PlatformArtifact `json:"route_artifact"`
	}
	mustDecodeJSON(t, first, &firstResponse)

	request["intent"].(map[string]any)["routes"] = []any{map[string]any{
		"hostname": "changed.fugue.pro", "upstream_url": "http://changed:8080", "enabled": true,
	}}
	failed := performJSONRequest(t, server, http.MethodPost, "/v1/admin/platform-config/compile", platformAdminKey, request)
	if failed.Code != http.StatusConflict {
		t.Fatalf("changed immutable generation must fail with conflict: %d %s", failed.Code, failed.Body.String())
	}
	retained, err := stateStore.GetPlatformArtifact(firstResponse.RouteArtifact.ID)
	if err != nil {
		t.Fatalf("load retained route artifact: %v", err)
	}
	if retained.Content["routes"].([]any)[0].(map[string]any)["hostname"] != "stable.fugue.pro" {
		t.Fatalf("failed compile changed retained artifact: %+v", retained.Content)
	}
}

func TestPolicyArtifactValidationUsesTypedPolicySchema(t *testing.T) {
	artifact := model.PlatformArtifact{
		ArtifactKind: model.PlatformArtifactKindPolicySnapshot,
		Content:      map[string]any{"generation": "policy-invalid", "dependency_order": []any{"route", "route"}},
		Metadata:     map[string]string{},
	}
	if err := validatePlatformPolicyArtifact(artifact); err == nil || !strings.Contains(err.Error(), "policy snapshot is invalid") {
		t.Fatalf("expected typed policy validation failure, got %v", err)
	}
}

func TestPlatformPolicyLKGBindsVerifiedTypedArtifact(t *testing.T) {
	t.Parallel()
	_, server, tenantKey, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	create := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts", platformAdminKey, model.PlatformArtifactCreateRequest{
		ArtifactKind: model.PlatformArtifactKindPolicySnapshot,
		Scope:        model.PlatformArtifactScope{ScopeType: "global"},
		Generation:   "policy-lkg-api-1",
		Content: map[string]any{
			"generation": "policy-lkg-api-1", "scope": "global", "require_route_ready": true,
			"minimum_healthy_edges": 1, "dependency_order": []any{"route", "tls", "dns"},
		},
	})
	if create.Code != http.StatusCreated {
		t.Fatalf("create policy artifact: %d %s", create.Code, create.Body.String())
	}
	var created model.PlatformArtifactResponse
	mustDecodeJSON(t, create, &created)
	validated := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts/"+created.Artifact.ID+"/validate", platformAdminKey, map[string]any{"dry_run": false})
	if validated.Code != http.StatusOK {
		t.Fatalf("validate policy artifact: %d %s", validated.Code, validated.Body.String())
	}
	seedVerifiedPlatformArtifactAPI(t, server, platformAdminKey, created.Artifact.ID)
	releaseAndVerifyFullPlatformArtifactAPI(t, server, platformAdminKey, created.Artifact.ID)
	response := performJSONRequest(t, server, http.MethodGet, "/v1/admin/platform-config/policy-lkg", platformAdminKey, nil)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), created.Artifact.ID) || !strings.Contains(response.Body.String(), "policy-lkg-api-1") {
		t.Fatalf("unexpected policy LKG response: %d %s", response.Code, response.Body.String())
	}
	forbidden := performJSONRequest(t, server, http.MethodGet, "/v1/admin/platform-config/policy-lkg", tenantKey, nil)
	if forbidden.Code != http.StatusForbidden {
		t.Fatalf("tenant key must not read platform policy LKG, got %d body=%s", forbidden.Code, forbidden.Body.String())
	}
}

func TestPlatformIntentArtifactValidationUsesTypedIntentSchema(t *testing.T) {
	artifact := model.PlatformArtifact{
		ArtifactKind: model.PlatformArtifactKindPlatformIntent,
		Content:      map[string]any{"generation": "intent-invalid", "routes": []any{map[string]any{"hostname": "duplicate", "upstream_url": "http://one"}, map[string]any{"hostname": "duplicate", "upstream_url": "http://two"}}},
		Metadata:     map[string]string{},
	}
	if err := validatePlatformIntentArtifact(artifact); err == nil || !strings.Contains(err.Error(), "platform intent is invalid") {
		t.Fatalf("expected typed intent validation failure, got %v", err)
	}
}

func TestReleaseSetConvergenceBlocksRequiredConsumers(t *testing.T) {
	s := store.New(t.TempDir() + "/store.json")
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{
		ReleaseSetID: "release-set-convergence", ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle,
		Scope: model.PlatformArtifactScope{ScopeType: "global"}, ScopeKey: "global", Generation: "route-1", Revision: 1,
		Topology: platformcontrol.ExpectedConsumerTopology{EdgeNodes: []model.EdgeNode{{ID: "edge-1", EdgeGroupID: "group-1", Country: "US"}}},
	})
	if err != nil {
		t.Fatalf("build expected set: %v", err)
	}
	if _, err := s.CreatePlatformExpectedConsumerSet(set); err != nil {
		t.Fatalf("create expected set: %v", err)
	}
	result := server.validateReleaseSetConvergence(model.PlatformArtifact{ID: "release-set-convergence"})
	if result.Pass || !strings.Contains(result.Message, "have not converged") {
		t.Fatalf("expected convergence block, got %+v", result)
	}
}

func TestPlatformArtifactContentContainsHostname(t *testing.T) {
	content := map[string]any{"routes": []any{map[string]any{"hostname": "Example.Fugue.Pro."}}, "nested": map[string]any{"certificates": []any{map[string]any{"hostname": "tls.fugue.pro"}}}}
	if !platformArtifactContentContainsHostname(content, "example.fugue.pro") || !platformArtifactContentContainsHostname(content, "tls.fugue.pro") || platformArtifactContentContainsHostname(content, "missing.fugue.pro") {
		t.Fatalf("hostname lineage content scan returned an unexpected result")
	}
}
