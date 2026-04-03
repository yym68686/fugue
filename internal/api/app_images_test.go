package api

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestHandleGetAppImagesReturnsCurrentAndHistoricalVersions(t *testing.T) {
	t.Parallel()

	_, server, apiKey, _, project, app, _, oldImageRef, newImageRef, _ := setupAppImagesTestServer(t)

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/images", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response appImageInventoryResponse
	mustDecodeJSON(t, recorder, &response)

	if response.AppID != app.ID {
		t.Fatalf("expected app id %q, got %q", app.ID, response.AppID)
	}
	if !response.RegistryConfigured {
		t.Fatal("expected registry inventory to be configured")
	}
	if response.Summary.VersionCount != 2 {
		t.Fatalf("expected two image versions, got %#v", response.Summary)
	}
	if response.Summary.CurrentVersionCount != 1 {
		t.Fatalf("expected one current version, got %#v", response.Summary)
	}
	if response.Summary.StaleVersionCount != 1 {
		t.Fatalf("expected one stale version, got %#v", response.Summary)
	}
	if response.Summary.TotalSizeBytes != 240 {
		t.Fatalf("expected total size 240, got %d", response.Summary.TotalSizeBytes)
	}
	if response.Summary.CurrentSizeBytes != 180 {
		t.Fatalf("expected current size 180, got %d", response.Summary.CurrentSizeBytes)
	}
	if response.Summary.StaleSizeBytes != 160 {
		t.Fatalf("expected stale size 160, got %d", response.Summary.StaleSizeBytes)
	}
	if response.Summary.ReclaimableSizeBytes != 60 {
		t.Fatalf("expected reclaimable size 60, got %d", response.Summary.ReclaimableSizeBytes)
	}
	if len(response.Versions) != 2 {
		t.Fatalf("expected two versions in response, got %#v", response.Versions)
	}

	versionByImageRef := make(map[string]appImageVersion, len(response.Versions))
	for _, version := range response.Versions {
		versionByImageRef[version.ImageRef] = version
	}

	currentVersion, ok := versionByImageRef[newImageRef]
	if !ok {
		t.Fatalf("expected current image %q in response", newImageRef)
	}
	if !currentVersion.Current {
		t.Fatalf("expected %q to be current: %#v", newImageRef, currentVersion)
	}
	if currentVersion.Status != appImageStatusAvailable {
		t.Fatalf("expected current version to be available, got %#v", currentVersion)
	}
	if currentVersion.DeleteSupported {
		t.Fatalf("expected current version to be non-deletable, got %#v", currentVersion)
	}
	if !currentVersion.RedeploySupported {
		t.Fatalf("expected current version to be redeployable, got %#v", currentVersion)
	}

	staleVersion, ok := versionByImageRef[oldImageRef]
	if !ok {
		t.Fatalf("expected stale image %q in response", oldImageRef)
	}
	if staleVersion.Current {
		t.Fatalf("expected %q to be stale: %#v", oldImageRef, staleVersion)
	}
	if staleVersion.ReclaimableSizeBytes != 60 {
		t.Fatalf("expected stale reclaimable size 60, got %#v", staleVersion)
	}
	if !staleVersion.DeleteSupported {
		t.Fatalf("expected stale version to be deletable, got %#v", staleVersion)
	}
	if staleVersion.Source == nil || staleVersion.Source.CommitSHA == "" {
		t.Fatalf("expected stale version source metadata, got %#v", staleVersion)
	}
	if response.ReclaimNote == "" {
		t.Fatalf("expected reclaim note for project %s", project.ID)
	}
}

func TestHandleListProjectImageUsageReturnsProjectSummary(t *testing.T) {
	t.Parallel()

	_, server, apiKey, _, project, app, _, _, _, _ := setupAppImagesTestServer(t)

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/projects/image-usage", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response projectImageUsageResponse
	mustDecodeJSON(t, recorder, &response)

	if !response.RegistryConfigured {
		t.Fatal("expected registry inventory to be configured")
	}
	if len(response.Projects) != 1 {
		t.Fatalf("expected one project summary, got %#v", response.Projects)
	}

	summary := response.Projects[0]
	if summary.ProjectID != project.ID {
		t.Fatalf("expected project id %q, got %#v", project.ID, summary)
	}
	if summary.VersionCount != 2 || summary.StaleVersionCount != 1 {
		t.Fatalf("expected one stale and two total versions, got %#v", summary)
	}
	if summary.TotalSizeBytes != 240 || summary.ReclaimableSizeBytes != 60 {
		t.Fatalf("expected project summary sizes 240/60, got %#v", summary)
	}
	if len(summary.Apps) != 1 {
		t.Fatalf("expected one app summary, got %#v", summary.Apps)
	}
	if summary.Apps[0].AppID != app.ID {
		t.Fatalf("expected app summary for %q, got %#v", app.ID, summary.Apps[0])
	}
}

func TestHandleRedeployAppImageQueuesHistoricalDeploy(t *testing.T) {
	t.Parallel()

	s, server, apiKey, _, _, app, _, oldImageRef, _, oldRuntimeImageRef := setupAppImagesTestServer(t)

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/images/redeploy", apiKey, map[string]any{
		"image_ref": oldImageRef,
	})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	var response appImageRedeployResponse
	mustDecodeJSON(t, recorder, &response)

	op, err := s.GetOperation(response.Operation.ID)
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}
	if op.Type != model.OperationTypeDeploy {
		t.Fatalf("expected deploy operation, got %#v", op)
	}
	if op.DesiredSpec == nil || op.DesiredSpec.Image != oldRuntimeImageRef {
		t.Fatalf("expected desired runtime image %q, got %#v", oldRuntimeImageRef, op.DesiredSpec)
	}
	if op.DesiredSource == nil || op.DesiredSource.ResolvedImageRef != oldImageRef {
		t.Fatalf("expected desired source resolved image ref %q, got %#v", oldImageRef, op.DesiredSource)
	}
}

func TestHandleDeleteAppImageDeletesHistoricalRegistryVersion(t *testing.T) {
	t.Parallel()

	_, server, apiKey, _, _, app, fakeRegistry, oldImageRef, _, _ := setupAppImagesTestServer(t)

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/images/delete", apiKey, map[string]any{
		"image_ref": oldImageRef,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response appImageDeleteResponse
	mustDecodeJSON(t, recorder, &response)

	if !response.Deleted {
		t.Fatalf("expected delete response to mark deleted, got %#v", response)
	}
	if response.ReclaimedSizeBytes != 60 {
		t.Fatalf("expected reclaimed size estimate 60, got %#v", response)
	}
	if len(fakeRegistry.deleted) != 1 || fakeRegistry.deleted[0] != oldImageRef {
		t.Fatalf("expected fake registry delete for %q, got %#v", oldImageRef, fakeRegistry.deleted)
	}
}

type fakeAppImageRegistry struct {
	deleted []string
	images  map[string]appImageRegistryInspectResult
}

func (f *fakeAppImageRegistry) InspectImage(_ context.Context, imageRef string) (appImageRegistryInspectResult, error) {
	if result, ok := f.images[imageRef]; ok {
		return cloneAppImageRegistryInspectResult(result), nil
	}
	return appImageRegistryInspectResult{
		ImageRef: imageRef,
		Exists:   false,
	}, nil
}

func (f *fakeAppImageRegistry) DeleteImage(_ context.Context, imageRef string) (appImageRegistryDeleteResult, error) {
	result, ok := f.images[imageRef]
	if !ok {
		return appImageRegistryDeleteResult{
			ImageRef:       imageRef,
			AlreadyMissing: true,
		}, nil
	}
	delete(f.images, imageRef)
	f.deleted = append(f.deleted, imageRef)
	return appImageRegistryDeleteResult{
		ImageRef: imageRef,
		Digest:   result.Digest,
		Deleted:  true,
	}, nil
}

func setupAppImagesTestServer(t *testing.T) (*store.Store, *Server, string, model.Tenant, model.Project, model.App, *fakeAppImageRegistry, string, string, string) {
	t.Helper()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Images Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "gallery", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.deploy", "app.write", "app.delete"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	const (
		pushBase      = "registry.push.example"
		pullBase      = "registry.pull.example"
		oldCommit     = "111111111111aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		newCommit     = "222222222222bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		imageRepoPath = "example-demo-web"
	)
	oldImageRef := pushBase + "/fugue-apps/" + imageRepoPath + ":git-111111111111"
	newImageRef := pushBase + "/fugue-apps/" + imageRepoPath + ":git-222222222222"
	oldRuntimeImageRef := pullBase + "/fugue-apps/" + imageRepoPath + ":git-111111111111"
	newRuntimeImageRef := pullBase + "/fugue-apps/" + imageRepoPath + ":git-222222222222"

	oldSource := model.AppSource{
		Type:              model.AppSourceTypeGitHubPublic,
		RepoURL:           "https://github.com/example/demo",
		RepoBranch:        "main",
		BuildStrategy:     model.AppBuildStrategyStaticSite,
		CommitSHA:         oldCommit,
		CommitCommittedAt: "2026-03-01T08:00:00Z",
		ImageNameSuffix:   "web",
	}
	oldSpec := model.AppSpec{
		Image:     oldRuntimeImageRef,
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}
	app, err := s.CreateImportedApp(tenant.ID, project.ID, "demo", "", oldSpec, oldSource, model.AppRoute{
		Hostname:    "demo.apps.example.com",
		BaseDomain:  "apps.example.com",
		PublicURL:   "https://demo.apps.example.com",
		ServicePort: 80,
	})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	oldDeployOp, err := s.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeDeploy,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "tester",
		AppID:           app.ID,
		DesiredSpec:     &oldSpec,
		DesiredSource:   &oldSource,
	})
	if err != nil {
		t.Fatalf("create old deploy operation: %v", err)
	}
	if _, err := s.CompleteManagedOperationWithResult(oldDeployOp.ID, "/tmp/old.yaml", "old deployed", &oldSpec, &oldSource); err != nil {
		t.Fatalf("complete old deploy operation: %v", err)
	}

	newSource := oldSource
	newSource.CommitSHA = newCommit
	newSource.CommitCommittedAt = "2026-03-02T08:00:00Z"
	newSpec := oldSpec
	newSpec.Image = newRuntimeImageRef

	newDeployOp, err := s.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeDeploy,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "tester",
		AppID:           app.ID,
		DesiredSpec:     &newSpec,
		DesiredSource:   &newSource,
	})
	if err != nil {
		t.Fatalf("create new deploy operation: %v", err)
	}
	if _, err := s.CompleteManagedOperationWithResult(newDeployOp.ID, "/tmp/new.yaml", "new deployed", &newSpec, &newSource); err != nil {
		t.Fatalf("complete new deploy operation: %v", err)
	}

	app, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("reload app: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{
		RegistryPushBase: pushBase,
		RegistryPullBase: pullBase,
	})
	fakeRegistry := &fakeAppImageRegistry{
		images: map[string]appImageRegistryInspectResult{
			oldImageRef: {
				ImageRef:  oldImageRef,
				Digest:    "sha256:oldmanifest",
				Exists:    true,
				SizeBytes: 160,
				BlobSizes: map[string]int64{
					"sha256:manifest-old": 10,
					"sha256:config-old":   20,
					"sha256:layer-base":   100,
					"sha256:layer-old":    30,
				},
			},
			newImageRef: {
				ImageRef:  newImageRef,
				Digest:    "sha256:newmanifest",
				Exists:    true,
				SizeBytes: 180,
				BlobSizes: map[string]int64{
					"sha256:manifest-new": 10,
					"sha256:config-new":   20,
					"sha256:layer-base":   100,
					"sha256:layer-new":    50,
				},
			},
		},
	}
	server.appImageRegistry = fakeRegistry

	return s, server, apiKey, tenant, project, app, fakeRegistry, oldImageRef, newImageRef, oldRuntimeImageRef
}
