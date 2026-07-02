package api

import (
	"context"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestSyncAppImageReturnsActiveOperationWhenTrackedDigestMatchesDesiredSource(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, app, digest := setupAppImageSyncTestServer(t)
	desiredSpec := cloneAppSpec(app.Spec)
	deployOp, err := stateStore.CreateOperation(model.Operation{
		TenantID:        app.TenantID,
		AppID:           app.ID,
		Type:            model.OperationTypeDeploy,
		RequestedByType: model.ActorTypeBootstrap,
		RequestedByID:   model.OperationRequestedByImageTracking,
		DesiredSpec:     &desiredSpec,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/image-sync", apiKey, nil)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	var response appImageSyncResponse
	mustDecodeJSON(t, recorder, &response)
	if !response.AlreadyCurrent {
		t.Fatal("expected sync response to remain already_current")
	}
	if !response.RolloutPending {
		t.Fatal("expected sync response to mark rollout pending")
	}
	if response.Operation == nil || response.Operation.ID != deployOp.ID {
		t.Fatalf("expected active deploy operation %s, got %+v", deployOp.ID, response.Operation)
	}
	if response.Digest != digest {
		t.Fatalf("expected digest %q, got %q", digest, response.Digest)
	}
	if strings.Contains(strings.ToLower(response.Message), "already deployed") {
		t.Fatalf("message must not claim deployment completed, got %q", response.Message)
	}
}

func TestGetAppImageTrackingHistoryReturnsRecordedChecks(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, app, _ := setupAppImageSyncTestServer(t)
	tracking, err := stateStore.GetAppImageTracking(app.TenantID, true, app.ID)
	if err != nil {
		t.Fatalf("get tracking: %v", err)
	}
	recorded, err := stateStore.CreateAppImageTrackingCheck(model.AppImageTrackingCheck{
		TenantID:         app.TenantID,
		AppID:            app.ID,
		TrackingID:       tracking.ID,
		ImageRef:         tracking.ImageRef,
		ObservedDigest:   "sha256:new",
		CurrentAppDigest: "sha256:old",
		Decision:         model.AppImageTrackingDecisionActiveOperation,
		SkipReason:       "app has an active operation",
	})
	if err != nil {
		t.Fatalf("create tracking check: %v", err)
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/image-tracking/history?limit=5", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response appImageTrackingHistoryResponse
	mustDecodeJSON(t, recorder, &response)
	if response.Tracking == nil || response.Tracking.ID != tracking.ID {
		t.Fatalf("expected tracking in history response, got %+v", response.Tracking)
	}
	if len(response.Checks) != 1 || response.Checks[0].ID != recorded.ID {
		t.Fatalf("expected recorded check, got %+v", response.Checks)
	}
}

func TestGetAppImageTrackingDiagnosisResolvesRemoteDigest(t *testing.T) {
	t.Parallel()

	_, server, apiKey, app, digest := setupAppImageSyncTestServer(t)
	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/image-tracking/diagnosis", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response appImageTrackingDiagnosisResponse
	mustDecodeJSON(t, recorder, &response)
	if response.Diagnosis.Tracking == nil {
		t.Fatalf("expected tracking diagnosis, got %+v", response.Diagnosis)
	}
	if response.Diagnosis.RemoteDigest != digest {
		t.Fatalf("expected remote digest %q, got %+v", digest, response.Diagnosis)
	}
}

func setupAppImageSyncTestServer(t *testing.T) (*store.Store, *Server, string, model.App, string) {
	t.Helper()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Image Sync Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "image-sync", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	raiseManagedTestCap(t, stateStore, tenant.ID)
	_, apiKey, err := stateStore.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.deploy"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	imageRef := "ghcr.io/acme/api:main"
	digest := "sha256:abc123456789"
	source := model.AppSource{
		Type:             model.AppSourceTypeDockerImage,
		ImageRef:         imageRef,
		ResolvedImageRef: "registry.fugue.internal:5000/fugue-apps/acme-api:image-abc123456789",
		ImageNameSuffix:  "api",
		ComposeService:   "api",
		DetectedProvider: "docker-image",
	}
	app, err := stateStore.CreateImportedAppWithoutRoute(tenant.ID, project.ID, "api", "", model.AppSpec{
		Image:     source.ResolvedImageRef,
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, source)
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}
	if _, err := stateStore.UpsertAppImageTracking(model.AppImageTracking{
		TenantID: tenant.ID,
		AppID:    app.ID,
		ImageRef: imageRef,
		Enabled:  true,
	}); err != nil {
		t.Fatalf("upsert tracking: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	server.resolveRemoteImageDigest = func(context.Context, string) (string, error) {
		return digest, nil
	}
	return stateStore, server, apiKey, app, digest
}
