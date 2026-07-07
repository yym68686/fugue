package api

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestReleaseGuardStatusDetectsPlatformConsumerDrift(t *testing.T) {
	t.Parallel()

	_, server, _, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")

	create := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts", platformAdminKey, model.PlatformArtifactCreateRequest{
		ArtifactKind: model.PlatformArtifactKindTrafficSafetyPolicy,
		Scope:        model.PlatformArtifactScope{ScopeType: "global"},
		Generation:   "traffic_safety_gen_1",
		Content:      map[string]any{"min_healthy_edges": 1},
	})
	if create.Code != http.StatusCreated {
		t.Fatalf("expected create status %d, got %d body=%s", http.StatusCreated, create.Code, create.Body.String())
	}
	var created model.PlatformArtifactResponse
	mustDecodeJSON(t, create, &created)

	validate := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts/"+created.Artifact.ID+"/validate", platformAdminKey, model.PlatformArtifactValidateRequest{DryRun: false})
	if validate.Code != http.StatusOK {
		t.Fatalf("expected validate status %d, got %d body=%s", http.StatusOK, validate.Code, validate.Body.String())
	}
	release := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts/"+created.Artifact.ID+"/release", platformAdminKey, model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelFull,
	})
	if release.Code != http.StatusOK {
		t.Fatalf("expected release status %d, got %d body=%s", http.StatusOK, release.Code, release.Body.String())
	}

	heartbeat := performJSONRequest(t, server, http.MethodPost, "/v1/platform-state/consumers/heartbeat", platformAdminKey, model.PlatformConsumerHeartbeatRequest{
		ConsumerID:        "edge-worker-drift-test",
		Component:         "edge-worker",
		ArtifactKind:      model.PlatformArtifactKindTrafficSafetyPolicy,
		ScopeKey:          "global",
		DesiredGeneration: created.Artifact.Generation,
		ActualGeneration:  "traffic_safety_old",
		ApplyStatus:       "applied",
		ProbeStatus:       "passed",
	})
	if heartbeat.Code != http.StatusOK {
		t.Fatalf("expected heartbeat status %d, got %d body=%s", http.StatusOK, heartbeat.Code, heartbeat.Body.String())
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/admin/release-guard/status", platformAdminKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected release guard status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response model.ReleaseGuardStatusResponse
	mustDecodeJSON(t, recorder, &response)
	if response.Status.PlatformConsumerDrift != 1 || !response.Status.BlockRollout {
		t.Fatalf("expected consumer drift to block rollout, got %+v", response.Status)
	}
	if !stringSliceContains(response.Status.BlockedReasons, "platform consumer generation drift: 1") {
		t.Fatalf("expected drift blocked reason, got %+v", response.Status.BlockedReasons)
	}
}

func TestReleaseGuardStatusBlocksInvalidActivePlatformArtifact(t *testing.T) {
	t.Parallel()

	_, server, _, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")

	create := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts", platformAdminKey, model.PlatformArtifactCreateRequest{
		ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle,
		Scope:        model.PlatformArtifactScope{ScopeType: "global"},
		Generation:   "route_bundle_invalid",
		Content:      map[string]any{"metadata": "missing routes"},
	})
	if create.Code != http.StatusCreated {
		t.Fatalf("expected create status %d, got %d body=%s", http.StatusCreated, create.Code, create.Body.String())
	}
	var created model.PlatformArtifactResponse
	mustDecodeJSON(t, create, &created)

	release := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts/"+created.Artifact.ID+"/release", platformAdminKey, model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelFull,
		ForcePublish:   true,
		Reason:         "test guard must catch invalid active artifact",
	})
	if release.Code != http.StatusOK {
		t.Fatalf("expected force release status %d, got %d body=%s", http.StatusOK, release.Code, release.Body.String())
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/admin/release-guard/status", platformAdminKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected release guard status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response model.ReleaseGuardStatusResponse
	mustDecodeJSON(t, recorder, &response)
	if response.Status.PlatformArtifactFailures != 1 || !response.Status.BlockRollout {
		t.Fatalf("expected artifact validation failure to block rollout, got %+v", response.Status)
	}
	if !stringSliceContains(response.Status.BlockedReasons, "platform artifact validation failed: 1") {
		t.Fatalf("expected artifact failure blocked reason, got %+v", response.Status.BlockedReasons)
	}
}

func TestReleaseGuardStatusIncludesExplicitReleaseSignals(t *testing.T) {
	t.Parallel()

	_, server, _, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")

	create := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts", platformAdminKey, model.PlatformArtifactCreateRequest{
		ArtifactKind: model.PlatformArtifactKindReleaseGuardPolicy,
		Scope:        model.PlatformArtifactScope{ScopeType: "global"},
		Generation:   "release_guard_policy_signal_test",
		Content: map[string]any{
			"version": "v1",
			"signals": []any{map[string]any{
				"id":          "sig_missing_app",
				"enabled":     true,
				"owner_scope": "tenant_workload",
				"gate_scope":  "control_plane",
				"mode":        "hard_gate",
				"subject":     "app:missing-app",
				"check_name":  "app_continuity_invariant",
				"reason":      "test signal blocks if not observable",
			}},
		},
	})
	if create.Code != http.StatusCreated {
		t.Fatalf("expected create status %d, got %d body=%s", http.StatusCreated, create.Code, create.Body.String())
	}
	var created model.PlatformArtifactResponse
	mustDecodeJSON(t, create, &created)

	validate := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts/"+created.Artifact.ID+"/validate", platformAdminKey, model.PlatformArtifactValidateRequest{DryRun: false})
	if validate.Code != http.StatusOK {
		t.Fatalf("expected validate status %d, got %d body=%s", http.StatusOK, validate.Code, validate.Body.String())
	}
	var validation model.PlatformArtifactValidationResponse
	mustDecodeJSON(t, validate, &validation)
	if !validation.Pass {
		t.Fatalf("expected release guard policy validation to pass, got %+v", validation.Results)
	}

	release := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts/"+created.Artifact.ID+"/release", platformAdminKey, model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelFull,
		Reason:         "activate test release signal",
	})
	if release.Code != http.StatusOK {
		t.Fatalf("expected release status %d, got %d body=%s", http.StatusOK, release.Code, release.Body.String())
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/admin/release-guard/status", platformAdminKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected release guard status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response model.ReleaseGuardStatusResponse
	mustDecodeJSON(t, recorder, &response)
	if len(response.Status.ReleaseSignals) != 1 || response.Status.ReleaseSignals[0].ID != "sig_missing_app" {
		t.Fatalf("expected release guard status to include active signal, got %+v", response.Status.ReleaseSignals)
	}
	if !response.Status.BlockRollout {
		t.Fatalf("expected missing hard-gated release signal to block rollout, got %+v", response.Status)
	}
	if !stringSliceContains(response.Status.BlockedReasons, "release_signal_observed: release signal sig_missing_app did not match any current robustness check") {
		t.Fatalf("expected release signal blocked reason, got %+v", response.Status.BlockedReasons)
	}
}

func TestTrafficSafetyExplainReportsUnroutedHostname(t *testing.T) {
	t.Parallel()

	_, server, _, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/admin/traffic-safety/explain/missing.fugue.pro", platformAdminKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected traffic safety status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response model.TrafficSafetyExplainResponse
	mustDecodeJSON(t, recorder, &response)
	if response.State.Pass || response.State.RouteExplain.ServingMode != "unrouted" {
		t.Fatalf("expected unrouted traffic safety failure, got %+v", response.State)
	}
	if !stringSliceContains(response.State.Blockers, "hostname has no generated edge route") {
		t.Fatalf("expected unrouted blocker, got %+v", response.State.Blockers)
	}
}

func TestRequestExplainAttributesBodyReadErrorAndNetworkSignals(t *testing.T) {
	t.Parallel()

	storeState, server, _, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	now := time.Now().UTC()
	if err := storeState.RecordEdgePerformanceSamples([]model.EdgePerformanceSample{{
		ID:                   "req_explain_body_read",
		EdgeID:               "edge-hk-1",
		EdgeGroupID:          "edge-group-country-hk",
		Hostname:             "api.fugue.pro",
		PathPrefix:           "/v1",
		Method:               "POST",
		TrafficClass:         "large_body_api",
		RouteGeneration:      "routegen_42",
		StatusCode:           http.StatusServiceUnavailable,
		SampleCount:          1,
		ErrorCount:           1,
		BodyReadBlockMS:      12_000,
		UploadEffectiveBPS:   8 * 1024,
		MinWindowBPS:         2 * 1024,
		MaxReadGapMS:         3_500,
		RequestBodyBytes:     64 * 1024,
		RequestBodyReadBytes: 32 * 1024,
		BodyReadErrorCount:   1,
		OriginDNSMS:          15,
		OriginConnectMS:      40,
		OriginResponseWaitMS: 900,
		OriginTTFBMS:         950,
		OriginTotalMS:        1_100,
		ClientTCPRTTMS:       88.5,
		ClientTCPRetransRate: 0.03,
		ClientTCPRTORate:     0.02,
		ClientTCPDeliveryBPS: 512 * 1024,
		DNSPolicy:            "latency_aware",
		CacheStatus:          "miss",
		SampledAt:            now.Add(-time.Minute),
	}}, time.Time{}); err != nil {
		t.Fatalf("record edge sample: %v", err)
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/admin/requests/req_explain_body_read/explain?since=2h", platformAdminKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected request explain status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response model.RequestExplainResponseEnvelope
	mustDecodeJSON(t, recorder, &response)
	explain := response.Explain
	if !explain.Found || explain.ErrorClass != "edge.body_read_error" || !explain.SecretSafe {
		t.Fatalf("expected secret-safe body read attribution, got %+v", explain)
	}
	for _, want := range []string{"client_to_edge_body_read", "client_to_edge_tcp", "edge_to_origin_dns", "edge_to_origin_connect", "origin_response_wait"} {
		if !stringSliceContains(explain.Attribution, want) {
			t.Fatalf("expected attribution %q in %+v", want, explain.Attribution)
		}
	}
	if explain.Evidence["request_body_read_complete"] != "false" || strings.TrimSpace(explain.Evidence["dns_policy"]) != "latency_aware" {
		t.Fatalf("expected request evidence without body/secrets, got %+v", explain.Evidence)
	}
}
