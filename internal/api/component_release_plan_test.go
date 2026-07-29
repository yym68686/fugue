package api

import (
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"fugue/internal/componentmanifest"
	"fugue/internal/model"
	"fugue/internal/platformsafety"
)

func TestComponentReleasePlanArtifactAPIIsDurableAndShadowOnly(t *testing.T) {
	t.Parallel()

	stateStore, server, _, platformAdminKey, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	observerKey, observerSecret, err := stateStore.CreateAPIKey(app.TenantID, "release-control-observer", []string{
		"artifact.read",
		"artifact.release_shadow",
		model.PlatformComponentPlanObserveScope,
	})
	if err != nil {
		t.Fatalf("create release-control observer key: %v", err)
	}
	_, missingReadSecret, err := stateStore.CreateAPIKey(app.TenantID, "release-control-missing-read", []string{
		"artifact.release_shadow",
		model.PlatformComponentPlanObserveScope,
	})
	if err != nil {
		t.Fatalf("create observer key without read scope: %v", err)
	}
	_, missingObserveSecret, err := stateStore.CreateAPIKey(app.TenantID, "release-control-missing-observe", []string{
		"artifact.read",
		"artifact.release_shadow",
	})
	if err != nil {
		t.Fatalf("create observer key without observe scope: %v", err)
	}
	envelope, identity := testComponentReleasePlanEnvelope(t)
	content, err := envelope.Content()
	if err != nil {
		t.Fatalf("encode component release plan content: %v", err)
	}

	create := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts", platformAdminKey, model.PlatformArtifactCreateRequest{
		ArtifactKind: model.PlatformArtifactKindComponentReleasePlan,
		Scope: model.PlatformArtifactScope{
			ScopeType: identity.ScopeType,
			Key:       identity.ScopeKey,
		},
		Generation:         identity.Generation,
		Content:            content,
		CompatibilityFloor: componentmanifest.ShadowArtifactSchemaVersionV1,
	})
	if create.Code != http.StatusCreated {
		t.Fatalf("create component release plan: status=%d body=%s", create.Code, create.Body.String())
	}
	var created model.PlatformArtifactResponse
	mustDecodeJSON(t, create, &created)
	if created.Artifact.ScopeKey != identity.ScopeKey ||
		created.Artifact.Generation != identity.Generation ||
		created.Artifact.ArtifactKind != model.PlatformArtifactKindComponentReleasePlan {
		t.Fatalf("created component release plan identity drifted: %+v", created.Artifact)
	}
	read := performJSONRequest(t, server, http.MethodGet, "/v1/admin/artifacts/"+created.Artifact.ID, observerSecret, nil)
	if read.Code != http.StatusOK {
		t.Fatalf("scoped observer could not read component plan: status=%d body=%s", read.Code, read.Body.String())
	}

	validate := performJSONRequest(
		t,
		server,
		http.MethodPost,
		"/v1/admin/artifacts/"+created.Artifact.ID+"/validate",
		platformAdminKey,
		model.PlatformArtifactValidateRequest{DryRun: false},
	)
	if validate.Code != http.StatusOK {
		t.Fatalf("validate component release plan: status=%d body=%s", validate.Code, validate.Body.String())
	}
	var validation model.PlatformArtifactValidationResponse
	mustDecodeJSON(t, validate, &validation)
	if !validation.Pass || validation.Artifact.Status != model.PlatformArtifactStatusValidated {
		t.Fatalf("component release plan did not persist as validated: %+v", validation)
	}

	release := performJSONRequest(
		t,
		server,
		http.MethodPost,
		"/v1/admin/artifacts/"+created.Artifact.ID+"/release",
		observerSecret,
		model.PlatformArtifactReleaseRequest{
			ReleaseChannel: model.PlatformArtifactReleaseChannelShadow,
			Reason:         model.PlatformComponentPlanObservationReason,
			IdempotencyKey: envelope.CoordinationPlan.IdempotencyKey,
		},
	)
	if release.Code != http.StatusOK {
		t.Fatalf("release component plan to shadow: status=%d body=%s", release.Code, release.Body.String())
	}
	var released model.PlatformArtifactReleaseResponse
	mustDecodeJSON(t, release, &released)
	if released.Release.ReleaseChannel != model.PlatformArtifactReleaseChannelShadow ||
		released.Release.ReleasedByType != model.ActorTypeAPIKey ||
		released.Release.ReleasedByID != observerKey.ID ||
		released.Release.LaneKey != platformsafety.ReleaseLaneKey(
			model.PlatformArtifactKindComponentReleasePlan,
			identity.ScopeKey,
			model.PlatformArtifactReleaseChannelShadow,
		) {
		t.Fatalf("component plan used an unexpected shadow lane: %+v", released.Release)
	}
	for name, secret := range map[string]string{
		"missing read":    missingReadSecret,
		"missing observe": missingObserveSecret,
	} {
		t.Run(name, func(t *testing.T) {
			blocked := performJSONRequest(
				t,
				server,
				http.MethodPost,
				"/v1/admin/artifacts/"+created.Artifact.ID+"/release",
				secret,
				model.PlatformArtifactReleaseRequest{
					ReleaseChannel: model.PlatformArtifactReleaseChannelShadow,
					Reason:         model.PlatformComponentPlanObservationReason,
					IdempotencyKey: envelope.CoordinationPlan.IdempotencyKey,
				},
			)
			if blocked.Code != http.StatusForbidden {
				t.Fatalf("incomplete observer was authorized: status=%d body=%s", blocked.Code, blocked.Body.String())
			}
		})
	}

	for name, request := range map[string]model.PlatformArtifactReleaseRequest{
		"reason drift": {
			ReleaseChannel: model.PlatformArtifactReleaseChannelShadow,
			Reason:         "different",
			IdempotencyKey: envelope.CoordinationPlan.IdempotencyKey,
		},
		"idempotency drift": {
			ReleaseChannel: model.PlatformArtifactReleaseChannelShadow,
			Reason:         model.PlatformComponentPlanObservationReason,
			IdempotencyKey: "component-shadow/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
		"override": {
			ReleaseChannel: model.PlatformArtifactReleaseChannelShadow,
			Reason:         model.PlatformComponentPlanObservationReason,
			IdempotencyKey: envelope.CoordinationPlan.IdempotencyKey,
			SoftOverride:   true,
		},
	} {
		t.Run("scoped "+name, func(t *testing.T) {
			blocked := performJSONRequest(
				t,
				server,
				http.MethodPost,
				"/v1/admin/artifacts/"+created.Artifact.ID+"/release",
				observerSecret,
				request,
			)
			if blocked.Code != http.StatusForbidden {
				t.Fatalf("scoped observer escaped exact request: status=%d body=%s", blocked.Code, blocked.Body.String())
			}
		})
	}

	for _, request := range []model.PlatformArtifactReleaseRequest{
		{ReleaseChannel: model.PlatformArtifactReleaseChannelGray, CanaryRuleRef: "node:test-node", Reason: "must stay shadow"},
		{ReleaseChannel: model.PlatformArtifactReleaseChannelFull, Reason: "must stay shadow"},
	} {
		blocked := performJSONRequest(
			t,
			server,
			http.MethodPost,
			"/v1/admin/artifacts/"+created.Artifact.ID+"/release",
			platformAdminKey,
			request,
		)
		if blocked.Code != http.StatusConflict {
			t.Fatalf("component release plan escaped shadow through %s: status=%d body=%s", request.ReleaseChannel, blocked.Code, blocked.Body.String())
		}
	}
}

func TestComponentReleasePlanArtifactAPIRejectsFalseLedgerIdentity(t *testing.T) {
	t.Parallel()

	_, server, _, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	envelope, identity := testComponentReleasePlanEnvelope(t)
	content, err := envelope.Content()
	if err != nil {
		t.Fatalf("encode component release plan content: %v", err)
	}
	create := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts", platformAdminKey, model.PlatformArtifactCreateRequest{
		ArtifactKind: model.PlatformArtifactKindComponentReleasePlan,
		Scope: model.PlatformArtifactScope{
			ScopeType: identity.ScopeType,
			Key:       identity.ScopeKey,
		},
		Generation: "git-3333333333333333333333333333333333333333",
		Content:    content,
	})
	if create.Code != http.StatusCreated {
		t.Fatalf("create malformed component release plan draft: status=%d body=%s", create.Code, create.Body.String())
	}
	var created model.PlatformArtifactResponse
	mustDecodeJSON(t, create, &created)
	validate := performJSONRequest(
		t,
		server,
		http.MethodPost,
		"/v1/admin/artifacts/"+created.Artifact.ID+"/validate",
		platformAdminKey,
		model.PlatformArtifactValidateRequest{DryRun: false},
	)
	if validate.Code != http.StatusOK {
		t.Fatalf("validate malformed component release plan: status=%d body=%s", validate.Code, validate.Body.String())
	}
	var validation model.PlatformArtifactValidationResponse
	mustDecodeJSON(t, validate, &validation)
	if validation.Pass || validation.Artifact.Status != model.PlatformArtifactStatusRejected {
		t.Fatalf("false component release plan identity was accepted: %+v", validation)
	}
}

func TestComponentPlanObservationAuthorizationIsExact(t *testing.T) {
	t.Parallel()

	envelope, _ := testComponentReleasePlanEnvelope(t)
	content, err := envelope.Content()
	if err != nil {
		t.Fatalf("encode component plan: %v", err)
	}
	principal := model.Principal{
		ActorType: model.ActorTypeAPIKey,
		ActorID:   "observer-key",
		Scopes: map[string]struct{}{
			"artifact.read":                         {},
			"artifact.release_shadow":               {},
			model.PlatformComponentPlanObserveScope: {},
		},
	}
	artifact := model.PlatformArtifact{
		ArtifactKind: model.PlatformArtifactKindComponentReleasePlan,
		Status:       model.PlatformArtifactStatusValidated,
		Content:      content,
	}
	request := model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelShadow,
		Reason:         model.PlatformComponentPlanObservationReason,
		IdempotencyKey: envelope.CoordinationPlan.IdempotencyKey,
	}
	if !componentPlanObservationAuthorized(principal, artifact, request) {
		t.Fatal("exact least-privilege observation was rejected")
	}

	for name, mutate := range map[string]func(*model.Principal, *model.PlatformArtifact, *model.PlatformArtifactReleaseRequest){
		"missing observe scope": func(principal *model.Principal, _ *model.PlatformArtifact, _ *model.PlatformArtifactReleaseRequest) {
			principal.Scopes = map[string]struct{}{"artifact.read": {}, "artifact.release_shadow": {}}
		},
		"missing read scope": func(principal *model.Principal, _ *model.PlatformArtifact, _ *model.PlatformArtifactReleaseRequest) {
			principal.Scopes = map[string]struct{}{
				"artifact.release_shadow": {}, model.PlatformComponentPlanObserveScope: {},
			}
		},
		"missing release scope": func(principal *model.Principal, _ *model.PlatformArtifact, _ *model.PlatformArtifactReleaseRequest) {
			principal.Scopes = map[string]struct{}{"artifact.read": {}, model.PlatformComponentPlanObserveScope: {}}
		},
		"additional scope": func(principal *model.Principal, _ *model.PlatformArtifact, _ *model.PlatformArtifactReleaseRequest) {
			principal.Scopes = map[string]struct{}{
				"artifact.read": {}, "artifact.release_shadow": {},
				model.PlatformComponentPlanObserveScope: {}, "app.read": {},
			}
		},
		"wrong actor type": func(principal *model.Principal, _ *model.PlatformArtifact, _ *model.PlatformArtifactReleaseRequest) {
			principal.ActorType = model.ActorTypeSystem
		},
		"wrong kind": func(_ *model.Principal, artifact *model.PlatformArtifact, _ *model.PlatformArtifactReleaseRequest) {
			artifact.ArtifactKind = model.PlatformArtifactKindEdgeRouteBundle
		},
		"unvalidated artifact": func(_ *model.Principal, artifact *model.PlatformArtifact, _ *model.PlatformArtifactReleaseRequest) {
			artifact.Status = model.PlatformArtifactStatusDraft
		},
		"full channel": func(_ *model.Principal, _ *model.PlatformArtifact, request *model.PlatformArtifactReleaseRequest) {
			request.ReleaseChannel = model.PlatformArtifactReleaseChannelFull
		},
		"canary": func(_ *model.Principal, _ *model.PlatformArtifact, request *model.PlatformArtifactReleaseRequest) {
			request.CanaryRuleRef = "node:other"
		},
		"soft override": func(_ *model.Principal, _ *model.PlatformArtifact, request *model.PlatformArtifactReleaseRequest) {
			request.SoftOverride = true
		},
		"force publish": func(_ *model.Principal, _ *model.PlatformArtifact, request *model.PlatformArtifactReleaseRequest) {
			request.ForcePublish = true
		},
		"break glass": func(_ *model.Principal, _ *model.PlatformArtifact, request *model.PlatformArtifactReleaseRequest) {
			request.KernelBreakGlass = &model.PlatformKernelBreakGlassRequest{}
		},
		"reason drift": func(_ *model.Principal, _ *model.PlatformArtifact, request *model.PlatformArtifactReleaseRequest) {
			request.Reason = "different"
		},
		"idempotency drift": func(_ *model.Principal, _ *model.PlatformArtifact, request *model.PlatformArtifactReleaseRequest) {
			request.IdempotencyKey = "different"
		},
		"malformed envelope": func(_ *model.Principal, artifact *model.PlatformArtifact, _ *model.PlatformArtifactReleaseRequest) {
			artifact.Content = map[string]any{"observationOnly": true}
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidatePrincipal := principal
			candidateArtifact := artifact
			candidateRequest := request
			mutate(&candidatePrincipal, &candidateArtifact, &candidateRequest)
			if componentPlanObservationAuthorized(candidatePrincipal, candidateArtifact, candidateRequest) {
				t.Fatal("unsafe observation authorization passed")
			}
		})
	}
}

func testComponentReleasePlanEnvelope(t *testing.T) (componentmanifest.ShadowArtifactEnvelope, componentmanifest.ShadowArtifactIdentity) {
	t.Helper()
	manifestFile, err := os.Open(filepath.Join("..", "..", "docs", "architecture", "component-ownership-v1.yaml"))
	if err != nil {
		t.Fatalf("open component ownership manifest: %v", err)
	}
	defer manifestFile.Close()
	manifest, err := componentmanifest.Load(manifestFile)
	if err != nil {
		t.Fatalf("load component ownership manifest: %v", err)
	}
	changePlan, err := componentmanifest.PlanChanges(manifest, []string{"internal/componentmanifest/artifact.go"})
	if err != nil {
		t.Fatalf("plan component release change: %v", err)
	}
	coordinationPlan, err := componentmanifest.BuildShadowCoordinationPlan(changePlan)
	if err != nil {
		t.Fatalf("build component release coordination: %v", err)
	}
	envelope, err := componentmanifest.BuildShadowArtifactEnvelope(
		manifest,
		changePlan,
		coordinationPlan,
		"1111111111111111111111111111111111111111",
		"2222222222222222222222222222222222222222",
	)
	if err != nil {
		t.Fatalf("build component release plan envelope: %v", err)
	}
	identity, err := envelope.ArtifactIdentity()
	if err != nil {
		t.Fatalf("derive component release plan identity: %v", err)
	}
	return envelope, identity
}
