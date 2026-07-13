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
	seedVerifiedPlatformArtifactAPI(t, server, platformAdminKey, created.Artifact.ID)
	releaseAndVerifyFullPlatformArtifactAPI(t, server, platformAdminKey, created.Artifact.ID)

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

func TestReleaseGuardAutonomyBlockedReasonsExposeHardFailures(t *testing.T) {
	t.Parallel()

	autonomy := &model.PlatformAutonomyStatus{
		BlockRollout: true,
		Checks: []model.StoreInvariantCheck{
			{Name: "node_policy", Pass: false, Message: "soft degradation"},
			{Name: "registry", Pass: false, Message: "connection refused"},
			{Name: "headscale", Pass: false, Message: "context deadline exceeded"},
		},
	}
	reasons := releaseGuardAutonomyBlockedReasons(autonomy)
	if !stringSliceContains(reasons, "platform autonomy registry failed: error_class=connection_refused") {
		t.Fatalf("expected registry blocker, got %+v", reasons)
	}
	if !stringSliceContains(reasons, "platform autonomy headscale failed: error_class=timeout") {
		t.Fatalf("expected headscale blocker, got %+v", reasons)
	}
	for _, reason := range reasons {
		if strings.Contains(reason, "node_policy") {
			t.Fatalf("soft autonomy failures must not be reported as rollout blockers: %+v", reasons)
		}
	}
	steps := releaseGuardRecommendedSteps(true, reasons)
	for _, step := range steps {
		if strings.Contains(step, "release guard passed") {
			t.Fatalf("blocked release guard must not recommend continuing rollout: %+v", steps)
		}
	}
}

func TestReleaseGuardAutonomyBlockedReasonsClassifyStoreInvariants(t *testing.T) {
	t.Parallel()

	autonomy := &model.PlatformAutonomyStatus{
		BlockRollout: true,
		ControlPlaneStore: model.ControlPlaneStoreStatus{
			BlockRollout: true,
			GateReason:   "do not copy raw store detail secret-value",
			Invariants: []model.StoreInvariantCheck{
				{Name: "permission_verification", Pass: false, Message: "secret-value"},
				{Name: "schema", Pass: true},
			},
		},
	}
	reasons := releaseGuardAutonomyBlockedReasons(autonomy)
	if !stringSliceContains(reasons, "control plane store blocked rollout: failed_invariants=permission_verification") {
		t.Fatalf("expected exact failed store invariant, got %+v", reasons)
	}
	if strings.Contains(strings.Join(reasons, " "), "secret-value") {
		t.Fatalf("blocked reasons must not copy raw store messages: %+v", reasons)
	}
}

func TestReleaseGuardAutonomyBlockedReasonsFailClosedOnInconsistentStatus(t *testing.T) {
	t.Parallel()

	reasons := releaseGuardAutonomyBlockedReasons(&model.PlatformAutonomyStatus{BlockRollout: true})
	if !stringSliceContains(reasons, "platform autonomy reported block_rollout=true without a failing blocking check") {
		t.Fatalf("expected classified invariant failure, got %+v", reasons)
	}
	steps := releaseGuardRecommendedSteps(true, reasons)
	if len(steps) == 0 || strings.Contains(strings.Join(steps, " "), "continue normal rollout") {
		t.Fatalf("inconsistent blocked status must remain fail closed, got %+v", steps)
	}
}

func TestPlatformSafetyKernelTreatsForcePublishAsBoundedSoftOverride(t *testing.T) {
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
		ReleaseChannel: model.PlatformArtifactReleaseChannelShadow,
		ForcePublish:   true,
		Reason:         "test legacy force publish is bounded soft override",
	})
	if release.Code != http.StatusOK {
		t.Fatalf("force_publish compatibility alias should bypass ordinary validation status, got %d body=%s", release.Code, release.Body.String())
	}
	var released model.PlatformArtifactReleaseResponse
	mustDecodeJSON(t, release, &released)
	if released.Release.OverrideMode != model.PlatformArtifactOverrideModeSoft ||
		len(released.Release.BypassedInvariants) != 1 ||
		released.Release.BypassedInvariants[0] != "artifact.validated" {
		t.Fatalf("unexpected bounded soft override ledger: %+v", released.Release)
	}

	hardInvariant := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts/"+created.Artifact.ID+"/release", platformAdminKey, model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelGray,
		CanaryRuleRef:  "*",
		ForcePublish:   true,
		Reason:         "test legacy force publish cannot bypass canary isolation",
	})
	if hardInvariant.Code != http.StatusConflict {
		t.Fatalf("force_publish must not bypass immutable canary isolation, got %d body=%s", hardInvariant.Code, hardInvariant.Body.String())
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

	seedVerifiedPlatformArtifactAPI(t, server, platformAdminKey, created.Artifact.ID)
	releaseAndVerifyFullPlatformArtifactAPI(t, server, platformAdminKey, created.Artifact.ID)

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

func TestGatePolicyRegistryPromotionAndReleaseGuard(t *testing.T) {
	t.Parallel()

	_, server, _, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")

	listRecorder := performJSONRequest(t, server, http.MethodGet, "/v1/admin/gates", platformAdminKey, nil)
	if listRecorder.Code != http.StatusOK {
		t.Fatalf("expected gate list status %d, got %d body=%s", http.StatusOK, listRecorder.Code, listRecorder.Body.String())
	}
	var listed model.GatePolicyListResponse
	mustDecodeJSON(t, listRecorder, &listed)
	if _, ok := gatePolicyByID(listed.Policies, "node.kubernetes_service_dns"); !ok {
		t.Fatalf("expected default node.kubernetes_service_dns gate policy, got %+v", listed.Policies)
	}

	rejected := performJSONRequest(t, server, http.MethodPost, "/v1/admin/gates/node.kube_proxy_rules/promote", platformAdminKey, model.GatePolicyPromoteRequest{
		Mode: model.GatePolicyModeEnforced,
	})
	if rejected.Code != http.StatusBadRequest {
		t.Fatalf("expected enforced promotion without reason to be rejected, got %d body=%s", rejected.Code, rejected.Body.String())
	}

	promoted := performJSONRequest(t, server, http.MethodPost, "/v1/admin/gates/node.kube_proxy_rules/promote", platformAdminKey, model.GatePolicyPromoteRequest{
		Mode:         model.GatePolicyModeCanary,
		Reason:       "test canary promotion",
		CanaryScopes: []string{"node:test-canary"},
	})
	if promoted.Code != http.StatusOK {
		t.Fatalf("expected gate promotion status %d, got %d body=%s", http.StatusOK, promoted.Code, promoted.Body.String())
	}
	var promotion model.GatePolicyPromotionResponse
	mustDecodeJSON(t, promoted, &promotion)
	if promotion.Policy.Mode != model.GatePolicyModeCanary || promotion.Artifact.ArtifactKind != model.PlatformArtifactKindGatePolicyRegistry || promotion.Release.ID == "" {
		t.Fatalf("unexpected gate promotion response: %+v", promotion)
	}
	if promotion.Release.ReleaseChannel != model.PlatformArtifactReleaseChannelShadow {
		t.Fatalf("first gate policy artifact must bootstrap through shadow, got %+v", promotion.Release)
	}
	verifyPlatformArtifactReleaseAPI(t, server, platformAdminKey, promotion.Release, true)
	activeGate := performJSONRequest(t, server, http.MethodGet, "/v1/admin/gates/node.kube_proxy_rules", platformAdminKey, nil)
	if activeGate.Code != http.StatusOK {
		t.Fatalf("expected active gate status %d, got %d body=%s", http.StatusOK, activeGate.Code, activeGate.Body.String())
	}
	var activeGateResponse model.GatePolicyResponse
	mustDecodeJSON(t, activeGate, &activeGateResponse)
	if activeGateResponse.Policy.Mode != model.GatePolicyModeCanary {
		t.Fatalf("verified gate policy did not become active: %+v", activeGateResponse.Policy)
	}

	statusRecorder := performJSONRequest(t, server, http.MethodGet, "/v1/admin/release-guard/status", platformAdminKey, nil)
	if statusRecorder.Code != http.StatusOK {
		t.Fatalf("expected release guard status %d, got %d body=%s", http.StatusOK, statusRecorder.Code, statusRecorder.Body.String())
	}
	var status model.ReleaseGuardStatusResponse
	mustDecodeJSON(t, statusRecorder, &status)
	if status.Status.GatePolicyCount == 0 || status.Status.EnforcedGateCount == 0 {
		t.Fatalf("expected release guard to include gate policy counts, got %+v", status.Status)
	}
	if len(status.Status.GatePolicyViolations) != 0 {
		t.Fatalf("expected valid gate policy registry, got violations %+v", status.Status.GatePolicyViolations)
	}
}

func TestEdgeRouteInventoryBlastRadiusKeepsLastHealthyGroups(t *testing.T) {
	t.Parallel()

	beforeHealthy := map[string]bool{
		"edge-group-country-us": true,
		"edge-group-country-jp": true,
	}
	beforeIDs := map[string][]string{
		"edge-group-country-us": []string{"edge-us-1"},
		"edge-group-country-jp": []string{"edge-jp-1"},
	}
	afterHealthy := map[string]bool{
		"edge-group-country-us": false,
		"edge-group-country-jp": false,
	}
	afterIDs := map[string][]string{}

	healthy, ids := applyEdgeRouteInventoryBlastRadiusCap(beforeHealthy, beforeIDs, afterHealthy, afterIDs)
	if !healthy["edge-group-country-us"] || !healthy["edge-group-country-jp"] {
		t.Fatalf("blast-radius cap must preserve previously healthy edge groups, got healthy=%v ids=%v", healthy, ids)
	}
	if !stringSliceContains(ids["edge-group-country-us"], "edge-us-1") || !stringSliceContains(ids["edge-group-country-jp"], "edge-jp-1") {
		t.Fatalf("blast-radius cap must preserve previous healthy edge ids, got %+v", ids)
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
