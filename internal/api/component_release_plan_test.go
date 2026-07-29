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
		platformAdminKey,
		model.PlatformArtifactReleaseRequest{
			ReleaseChannel: model.PlatformArtifactReleaseChannelShadow,
			Reason:         "record exact migration observation",
			IdempotencyKey: "component-plan-shadow-1",
		},
	)
	if release.Code != http.StatusOK {
		t.Fatalf("release component plan to shadow: status=%d body=%s", release.Code, release.Body.String())
	}
	var released model.PlatformArtifactReleaseResponse
	mustDecodeJSON(t, release, &released)
	if released.Release.ReleaseChannel != model.PlatformArtifactReleaseChannelShadow ||
		released.Release.LaneKey != platformsafety.ReleaseLaneKey(
			model.PlatformArtifactKindComponentReleasePlan,
			identity.ScopeKey,
			model.PlatformArtifactReleaseChannelShadow,
		) {
		t.Fatalf("component plan used an unexpected shadow lane: %+v", released.Release)
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
