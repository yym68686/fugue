package api

import (
	"net/http"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
)

func TestPlatformExpectedConsumerSetListAPIIsReadOnlyAndAdminScoped(t *testing.T) {
	t.Parallel()

	storeState, server, tenantKey, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{
		ReleaseSetID:      "release-set-api-test",
		ArtifactReleaseID: "artifact-release-api-test",
		ArtifactKind:      model.PlatformArtifactKindCaddyRouteConfig,
		Scope:             model.PlatformArtifactScope{ScopeType: "global"},
		ScopeKey:          "global",
		Generation:        "generation-api-test",
		Revision:          1,
		PreparedAt:        time.Date(2026, 7, 10, 10, 0, 0, 0, time.UTC),
		Topology: platformcontrol.ExpectedConsumerTopology{
			EdgeNodes: []model.EdgeNode{{ID: "edge-api-test", EdgeGroupID: "edge-group-api-test", Country: "US"}},
		},
	})
	if err != nil {
		t.Fatalf("build expected consumer set: %v", err)
	}
	if _, err := storeState.CreatePlatformExpectedConsumerSet(set); err != nil {
		t.Fatalf("persist expected consumer set: %v", err)
	}

	response := performJSONRequest(t, server, http.MethodGet,
		"/v1/admin/expected-consumer-sets?release_set_id=release-set-api-test&artifact_kind=caddy_route_config&scope_key=GLOBAL&limit=1",
		platformAdminKey, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("expected list status %d, got %d body=%s", http.StatusOK, response.Code, response.Body.String())
	}
	var listed model.PlatformExpectedConsumerSetListResponse
	mustDecodeJSON(t, response, &listed)
	if len(listed.ExpectedConsumerSets) != 1 || listed.ExpectedConsumerSets[0].ID != set.ID || listed.GeneratedAt.IsZero() {
		t.Fatalf("unexpected expected consumer set response: %+v", listed)
	}
	invalidLimit := performJSONRequest(t, server, http.MethodGet, "/v1/admin/expected-consumer-sets?limit=0", platformAdminKey, nil)
	if invalidLimit.Code != http.StatusBadRequest {
		t.Fatalf("invalid limit must be rejected, got %d body=%s", invalidLimit.Code, invalidLimit.Body.String())
	}

	forbidden := performJSONRequest(t, server, http.MethodGet, "/v1/admin/expected-consumer-sets", tenantKey, nil)
	if forbidden.Code != http.StatusForbidden {
		t.Fatalf("tenant key must not list expected consumer topology, got %d body=%s", forbidden.Code, forbidden.Body.String())
	}
}

func TestPlatformArtifactAPIReleaseConsumerAndFailureContracts(t *testing.T) {
	t.Parallel()

	_, server, tenantKey, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")

	create := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts", platformAdminKey, model.PlatformArtifactCreateRequest{
		ArtifactKind: model.PlatformArtifactKindEdgeRankingPolicy,
		Scope:        model.PlatformArtifactScope{ScopeType: "global"},
		Generation:   "rank_api_gen_1",
		Content:      map[string]any{"weights": map[string]any{"ttfb": 1, "error_rate": 2}},
	})
	if create.Code != http.StatusCreated {
		t.Fatalf("expected create status %d, got %d body=%s", http.StatusCreated, create.Code, create.Body.String())
	}
	var created model.PlatformArtifactResponse
	mustDecodeJSON(t, create, &created)
	if created.Artifact.ContentHash == "" || created.Artifact.ScopeKey != "global" {
		t.Fatalf("unexpected created artifact: %+v", created.Artifact)
	}

	validate := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts/"+created.Artifact.ID+"/validate", platformAdminKey, map[string]any{"dry_run": false})
	if validate.Code != http.StatusOK {
		t.Fatalf("expected validate status %d, got %d body=%s", http.StatusOK, validate.Code, validate.Body.String())
	}
	var validation model.PlatformArtifactValidationResponse
	mustDecodeJSON(t, validate, &validation)
	if !validation.Pass || validation.Artifact.Status != model.PlatformArtifactStatusValidated {
		t.Fatalf("expected persisted validation pass, got %+v", validation)
	}

	seeded := seedVerifiedPlatformArtifactAPI(t, server, platformAdminKey, created.Artifact.ID)
	released := releaseAndVerifyFullPlatformArtifactAPI(t, server, platformAdminKey, created.Artifact.ID)
	if released.Release.Generation != created.Artifact.Generation ||
		released.Release.VerificationState != model.PlatformArtifactVerificationStateVerified ||
		released.LKG == nil ||
		released.LKG.Generation != created.Artifact.Generation ||
		seeded.LKG == nil {
		t.Fatalf("expected explicitly verified full release and LKG, seeded=%+v released=%+v", seeded, released)
	}

	pull := performJSONRequest(t, server, http.MethodGet, "/v1/platform-state/artifacts/edge_ranking_policy?scope_key=global", platformAdminKey, nil)
	if pull.Code != http.StatusOK {
		t.Fatalf("expected pull status %d, got %d body=%s", http.StatusOK, pull.Code, pull.Body.String())
	}
	var pulled model.PlatformStateArtifactResponse
	mustDecodeJSON(t, pull, &pulled)
	if pulled.Artifact == nil || pulled.Artifact.Generation != created.Artifact.Generation || pulled.LKG == nil {
		t.Fatalf("expected active artifact and LKG in pull response, got %+v", pulled)
	}

	heartbeat := performJSONRequest(t, server, http.MethodPost, "/v1/platform-state/consumers/heartbeat", platformAdminKey, model.PlatformConsumerHeartbeatRequest{
		ConsumerID:        "edge-worker-api-test",
		Component:         "edge-worker",
		ArtifactKind:      model.PlatformArtifactKindEdgeRankingPolicy,
		ScopeKey:          "global",
		DesiredGeneration: created.Artifact.Generation,
		ActualGeneration:  created.Artifact.Generation,
		ApplyStatus:       "applied",
		ProbeStatus:       "passed",
	})
	if heartbeat.Code != http.StatusOK {
		t.Fatalf("expected heartbeat status %d, got %d body=%s", http.StatusOK, heartbeat.Code, heartbeat.Body.String())
	}
	consumers := performJSONRequest(t, server, http.MethodGet, "/v1/admin/artifacts/"+created.Artifact.ID+"/consumers", platformAdminKey, nil)
	if consumers.Code != http.StatusOK {
		t.Fatalf("expected consumers status %d, got %d body=%s", http.StatusOK, consumers.Code, consumers.Body.String())
	}
	var consumerResponse model.PlatformArtifactConsumersResponse
	mustDecodeJSON(t, consumers, &consumerResponse)
	if len(consumerResponse.Consumers) != 1 || consumerResponse.Consumers[0].ConsumerID != "edge-worker-api-test" {
		t.Fatalf("expected heartbeat consumer, got %+v", consumerResponse.Consumers)
	}
	rejectedHeartbeat := performJSONRequest(t, server, http.MethodPost, "/v1/platform-state/consumers/heartbeat", tenantKey, model.PlatformConsumerHeartbeatRequest{
		ConsumerID:   "forged-edge-worker",
		ArtifactKind: model.PlatformArtifactKindEdgeRankingPolicy,
		ScopeKey:     "global",
	})
	if rejectedHeartbeat.Code != http.StatusForbidden {
		t.Fatalf("ordinary tenant key must not write platform heartbeat, got %d body=%s", rejectedHeartbeat.Code, rejectedHeartbeat.Body.String())
	}

	contracts := performJSONRequest(t, server, http.MethodGet, "/v1/admin/failure-contracts", platformAdminKey, nil)
	if contracts.Code != http.StatusOK {
		t.Fatalf("expected contracts status %d, got %d body=%s", http.StatusOK, contracts.Code, contracts.Body.String())
	}
	var contractList model.SubsystemFailureContractListResponse
	mustDecodeJSON(t, contracts, &contractList)
	if len(contractList.Contracts) < 16 {
		t.Fatalf("expected critical subsystem contracts, got %d", len(contractList.Contracts))
	}
}

func TestPlatformKernelBreakGlassRequiresExplicitScopeAndDualConfirmation(t *testing.T) {
	t.Parallel()

	s, server, _, platformAdminKey, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	create := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts", platformAdminKey, model.PlatformArtifactCreateRequest{
		ArtifactKind: model.PlatformArtifactKindTrafficSafetyPolicy,
		Scope:        model.PlatformArtifactScope{ScopeType: "global"},
		Generation:   "kernel-break-glass-api",
		Content:      map[string]any{"min_healthy_edges": 1},
	})
	if create.Code != http.StatusCreated {
		t.Fatalf("expected create status %d, got %d body=%s", http.StatusCreated, create.Code, create.Body.String())
	}
	var created model.PlatformArtifactResponse
	mustDecodeJSON(t, create, &created)

	validRequest := model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelFull,
		KernelBreakGlass: &model.PlatformKernelBreakGlassRequest{
			ExpiresAt:          time.Now().UTC().Add(5 * time.Minute),
			Confirmation:       platformsafety.KernelBreakGlassConfirmation,
			TargetConfirmation: created.Artifact.ID,
		},
		Reason: "test API emergency release",
	}
	forbidden := performJSONRequest(
		t,
		server,
		http.MethodPost,
		"/v1/admin/artifacts/"+created.Artifact.ID+"/release",
		platformAdminKey,
		validRequest,
	)
	if forbidden.Code != http.StatusForbidden {
		t.Fatalf("platform.admin without explicit break-glass scope must be rejected, got %d body=%s", forbidden.Code, forbidden.Body.String())
	}

	_, breakGlassKey, err := s.CreateAPIKey(app.TenantID, "kernel-break-glass", []string{
		"platform.admin",
		"artifact.kernel_break_glass",
	})
	if err != nil {
		t.Fatalf("create explicit break-glass key: %v", err)
	}

	badConfirmation := validRequest
	badConfirmation.KernelBreakGlass = &model.PlatformKernelBreakGlassRequest{
		ExpiresAt:          time.Now().UTC().Add(5 * time.Minute),
		Confirmation:       "BYPASS",
		TargetConfirmation: created.Artifact.ID,
	}
	rejected := performJSONRequest(
		t,
		server,
		http.MethodPost,
		"/v1/admin/artifacts/"+created.Artifact.ID+"/release",
		breakGlassKey,
		badConfirmation,
	)
	if rejected.Code != http.StatusBadRequest {
		t.Fatalf("invalid safety confirmation must be rejected, got %d body=%s", rejected.Code, rejected.Body.String())
	}

	tooLong := validRequest
	tooLong.KernelBreakGlass = &model.PlatformKernelBreakGlassRequest{
		ExpiresAt:          time.Now().UTC().Add(platformsafety.KernelBreakGlassMaxTTL + time.Minute),
		Confirmation:       platformsafety.KernelBreakGlassConfirmation,
		TargetConfirmation: created.Artifact.ID,
	}
	rejected = performJSONRequest(
		t,
		server,
		http.MethodPost,
		"/v1/admin/artifacts/"+created.Artifact.ID+"/release",
		breakGlassKey,
		tooLong,
	)
	if rejected.Code != http.StatusBadRequest {
		t.Fatalf("overlong break-glass TTL must be rejected, got %d body=%s", rejected.Code, rejected.Body.String())
	}

	accepted := performJSONRequest(
		t,
		server,
		http.MethodPost,
		"/v1/admin/artifacts/"+created.Artifact.ID+"/release",
		breakGlassKey,
		validRequest,
	)
	if accepted.Code != http.StatusOK {
		t.Fatalf("valid explicitly authorized break-glass release failed, got %d body=%s", accepted.Code, accepted.Body.String())
	}
	var released model.PlatformArtifactReleaseResponse
	mustDecodeJSON(t, accepted, &released)
	if released.Release.OverrideMode != model.PlatformArtifactOverrideModeKernelBreakGlass ||
		released.Release.OverrideExpiresAt == nil ||
		!stringSliceContains(released.Release.BypassedInvariants, platformsafety.InvariantArtifactValidated) ||
		!stringSliceContains(released.Release.BypassedInvariants, platformsafety.InvariantFullPinnedRollback) {
		t.Fatalf("unexpected break-glass release ledger: %+v", released.Release)
	}
}

func seedVerifiedPlatformArtifactAPI(t *testing.T, server *Server, platformAdminKey, artifactID string) model.PlatformArtifactReleaseResponse {
	t.Helper()
	release := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts/"+artifactID+"/release", platformAdminKey, model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelShadow,
		IdempotencyKey: "seed-" + artifactID,
		Reason:         "test initial shadow seed",
	})
	if release.Code != http.StatusOK {
		t.Fatalf("expected initial shadow release status %d, got %d body=%s", http.StatusOK, release.Code, release.Body.String())
	}
	var released model.PlatformArtifactReleaseResponse
	mustDecodeJSON(t, release, &released)
	return verifyPlatformArtifactReleaseAPI(t, server, platformAdminKey, released.Release, true)
}

func releaseAndVerifyFullPlatformArtifactAPI(t *testing.T, server *Server, platformAdminKey, artifactID string) model.PlatformArtifactReleaseResponse {
	t.Helper()
	release := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts/"+artifactID+"/release", platformAdminKey, model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelFull,
		IdempotencyKey: "full-" + artifactID,
		Reason:         "test full release",
	})
	if release.Code != http.StatusOK {
		t.Fatalf("expected full release status %d, got %d body=%s", http.StatusOK, release.Code, release.Body.String())
	}
	var released model.PlatformArtifactReleaseResponse
	mustDecodeJSON(t, release, &released)
	return verifyPlatformArtifactReleaseAPI(t, server, platformAdminKey, released.Release, false)
}

func verifyPlatformArtifactReleaseAPI(t *testing.T, server *Server, platformAdminKey string, release model.PlatformArtifactRelease, allowInitial bool) model.PlatformArtifactReleaseResponse {
	t.Helper()
	verify := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifact-releases/"+release.ID+"/verify-lkg", platformAdminKey, model.PlatformArtifactVerifyLKGRequest{
		FencingToken:    release.FencingToken,
		Reason:          "test verification completed",
		AllowInitialLKG: allowInitial,
		Evidence: model.PlatformArtifactVerificationEvidence{
			ConsumerConvergence:        true,
			LocalProbe:                 true,
			PublicSynthetic:            true,
			WatchWindow:                true,
			BaselineMonotonic:          true,
			DatabaseRollbackCompatible: true,
			EvidenceRefs:               []string{"test:evidence"},
		},
	})
	if verify.Code != http.StatusOK {
		t.Fatalf("expected LKG verification status %d, got %d body=%s", http.StatusOK, verify.Code, verify.Body.String())
	}
	var verified model.PlatformArtifactReleaseResponse
	mustDecodeJSON(t, verify, &verified)
	return verified
}
