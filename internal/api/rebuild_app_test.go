package api

import (
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestRebuildAppQueuesImportForGitHubSource(t *testing.T) {
	t.Parallel()

	s, server, apiKey, tenant, project := setupRebuildAppTestServer(t)
	app := createImportedAppForRebuildTest(t, s, tenant.ID, project.ID, "demo-github", model.AppSource{
		Type:            model.AppSourceTypeGitHubPublic,
		RepoURL:         "https://github.com/example/demo",
		RepoBranch:      "main",
		SourceDir:       "public",
		BuildStrategy:   model.AppBuildStrategyStaticSite,
		ImageNameSuffix: "web",
	})

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/rebuild", apiKey, map[string]any{
		"branch":     "release",
		"source_dir": "dist",
	})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	var response struct {
		Operation model.Operation `json:"operation"`
		Build     struct {
			SourceType    string `json:"source_type"`
			Branch        string `json:"branch"`
			SourceDir     string `json:"source_dir"`
			BuildStrategy string `json:"build_strategy"`
		} `json:"build"`
	}
	mustDecodeJSON(t, recorder, &response)

	if response.Operation.ID == "" {
		t.Fatal("expected operation id in response")
	}
	if response.Build.SourceType != model.AppSourceTypeGitHubPublic {
		t.Fatalf("expected build source type %q, got %q", model.AppSourceTypeGitHubPublic, response.Build.SourceType)
	}
	if response.Build.Branch != "release" {
		t.Fatalf("expected build branch release, got %q", response.Build.Branch)
	}
	if response.Build.SourceDir != "dist" {
		t.Fatalf("expected build source_dir dist, got %q", response.Build.SourceDir)
	}

	op, err := s.GetOperation(response.Operation.ID)
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}
	if op.Type != model.OperationTypeImport {
		t.Fatalf("expected import operation, got %q", op.Type)
	}
	if op.DesiredSource == nil {
		t.Fatal("expected desired source on queued operation")
	}
	if op.DesiredSource.Type != model.AppSourceTypeGitHubPublic {
		t.Fatalf("expected queued source type %q, got %q", model.AppSourceTypeGitHubPublic, op.DesiredSource.Type)
	}
	if op.DesiredSource.RepoBranch != "release" {
		t.Fatalf("expected queued branch release, got %q", op.DesiredSource.RepoBranch)
	}
	if op.DesiredSource.SourceDir != "dist" {
		t.Fatalf("expected queued source_dir dist, got %q", op.DesiredSource.SourceDir)
	}
	if op.DesiredSource.ImageNameSuffix != "web" {
		t.Fatalf("expected queued image name suffix web, got %q", op.DesiredSource.ImageNameSuffix)
	}
}

func TestRebuildAppQueuesImportForUploadSource(t *testing.T) {
	t.Parallel()

	s, server, apiKey, tenant, project := setupRebuildAppTestServer(t)
	archiveBytes := mustTarGz(t, map[string]string{
		"index.html": "<h1>demo</h1>\n",
	})
	upload, err := s.CreateSourceUpload(tenant.ID, "demo-upload.tgz", "application/gzip", archiveBytes)
	if err != nil {
		t.Fatalf("create source upload: %v", err)
	}
	source, err := buildQueuedUploadSource(upload, "public", "", "", model.AppBuildStrategyStaticSite, "site", "")
	if err != nil {
		t.Fatalf("build queued upload source: %v", err)
	}
	app := createImportedAppForRebuildTest(t, s, tenant.ID, project.ID, "demo-upload", source)

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/rebuild", apiKey, map[string]any{
		"source_dir": "dist",
	})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	var response struct {
		Operation model.Operation `json:"operation"`
		Build     struct {
			SourceType    string `json:"source_type"`
			UploadID      string `json:"upload_id"`
			SourceDir     string `json:"source_dir"`
			BuildStrategy string `json:"build_strategy"`
		} `json:"build"`
	}
	mustDecodeJSON(t, recorder, &response)

	if response.Build.SourceType != model.AppSourceTypeUpload {
		t.Fatalf("expected build source type %q, got %q", model.AppSourceTypeUpload, response.Build.SourceType)
	}
	if response.Build.UploadID != upload.ID {
		t.Fatalf("expected build upload_id %q, got %q", upload.ID, response.Build.UploadID)
	}
	if response.Build.SourceDir != "dist" {
		t.Fatalf("expected build source_dir dist, got %q", response.Build.SourceDir)
	}

	op, err := s.GetOperation(response.Operation.ID)
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}
	if op.Type != model.OperationTypeImport {
		t.Fatalf("expected import operation, got %q", op.Type)
	}
	if op.DesiredSource == nil {
		t.Fatal("expected desired source on queued operation")
	}
	if op.DesiredSource.Type != model.AppSourceTypeUpload {
		t.Fatalf("expected queued source type %q, got %q", model.AppSourceTypeUpload, op.DesiredSource.Type)
	}
	if op.DesiredSource.UploadID != upload.ID {
		t.Fatalf("expected queued upload_id %q, got %q", upload.ID, op.DesiredSource.UploadID)
	}
	if op.DesiredSource.UploadFilename != upload.Filename {
		t.Fatalf("expected queued upload filename %q, got %q", upload.Filename, op.DesiredSource.UploadFilename)
	}
	if op.DesiredSource.ArchiveSHA256 != upload.SHA256 {
		t.Fatalf("expected queued archive sha256 %q, got %q", upload.SHA256, op.DesiredSource.ArchiveSHA256)
	}
	if op.DesiredSource.SourceDir != "dist" {
		t.Fatalf("expected queued source_dir dist, got %q", op.DesiredSource.SourceDir)
	}
	if op.DesiredSource.ImageNameSuffix != "site" {
		t.Fatalf("expected queued image name suffix site, got %q", op.DesiredSource.ImageNameSuffix)
	}
}

func TestRebuildAppQueuesImportForDockerImageSource(t *testing.T) {
	t.Parallel()

	s, server, apiKey, tenant, project := setupRebuildAppTestServer(t)
	app := createImportedAppForRebuildTest(t, s, tenant.ID, project.ID, "demo-image", model.AppSource{
		Type:             model.AppSourceTypeDockerImage,
		ImageRef:         "ghcr.io/example/demo:1.2.3",
		ResolvedImageRef: "registry.example.com/fugue-apps/demo-image:image-abc123",
	})

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/rebuild", apiKey, nil)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	var response struct {
		Operation model.Operation `json:"operation"`
		Build     struct {
			SourceType       string `json:"source_type"`
			ImageRef         string `json:"image_ref"`
			ResolvedImageRef string `json:"resolved_image_ref"`
		} `json:"build"`
	}
	mustDecodeJSON(t, recorder, &response)

	if response.Build.SourceType != model.AppSourceTypeDockerImage {
		t.Fatalf("expected build source type %q, got %q", model.AppSourceTypeDockerImage, response.Build.SourceType)
	}
	if response.Build.ImageRef != "ghcr.io/example/demo:1.2.3" {
		t.Fatalf("expected build image_ref to match saved source, got %q", response.Build.ImageRef)
	}
	if response.Build.ResolvedImageRef != "" {
		t.Fatalf("expected queued rebuild resolved image ref to be empty, got %q", response.Build.ResolvedImageRef)
	}

	op, err := s.GetOperation(response.Operation.ID)
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}
	if op.Type != model.OperationTypeImport {
		t.Fatalf("expected import operation, got %q", op.Type)
	}
	if op.DesiredSource == nil {
		t.Fatal("expected desired source on queued operation")
	}
	if op.DesiredSource.Type != model.AppSourceTypeDockerImage {
		t.Fatalf("expected queued source type %q, got %q", model.AppSourceTypeDockerImage, op.DesiredSource.Type)
	}
	if op.DesiredSource.ImageRef != "ghcr.io/example/demo:1.2.3" {
		t.Fatalf("expected queued image_ref ghcr.io/example/demo:1.2.3, got %q", op.DesiredSource.ImageRef)
	}
}

func TestRebuildAppReturnsNotFoundWhenUploadArchiveMetadataIsMissing(t *testing.T) {
	t.Parallel()

	s, server, apiKey, tenant, project := setupRebuildAppTestServer(t)
	app := createImportedAppForRebuildTest(t, s, tenant.ID, project.ID, "demo-missing-upload", model.AppSource{
		Type:            model.AppSourceTypeUpload,
		UploadID:        "upload_missing",
		UploadFilename:  "missing.tgz",
		ArchiveSHA256:   strings.Repeat("a", 64),
		BuildStrategy:   model.AppBuildStrategyStaticSite,
		ImageNameSuffix: "site",
	})

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/rebuild", apiKey, nil)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusNotFound, recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "resource not found") {
		t.Fatalf("expected not found error body, got %s", recorder.Body.String())
	}

	ops, err := s.ListOperations("", true)
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("expected no queued operations, got %d", len(ops))
	}
}

func TestRebuildAppRefreshesWorkspaceResetToken(t *testing.T) {
	t.Parallel()

	s, server, apiKey, tenant, project := setupRebuildAppTestServer(t)
	runtimeObj, _, err := s.CreateRuntime(tenant.ID, "worker-1", model.RuntimeTypeManagedOwned, "https://runtime.example.com", nil)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}

	app, err := s.CreateImportedApp(tenant.ID, project.ID, "demo-workspace", "", model.AppSpec{
		Image:     "registry.example.com/demo-workspace:current",
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: runtimeObj.ID,
		Workspace: &model.AppWorkspaceSpec{
			ResetToken: "workspace-reset-old",
		},
	}, model.AppSource{
		Type:          model.AppSourceTypeGitHubPublic,
		RepoURL:       "https://github.com/example/demo",
		RepoBranch:    "main",
		BuildStrategy: model.AppBuildStrategyStaticSite,
	}, model.AppRoute{
		Hostname:    "demo-workspace.apps.example.com",
		BaseDomain:  "apps.example.com",
		PublicURL:   "https://demo-workspace.apps.example.com",
		ServicePort: 80,
	})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/rebuild", apiKey, nil)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	var response struct {
		Operation model.Operation `json:"operation"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Operation.DesiredSpec == nil || response.Operation.DesiredSpec.Workspace == nil {
		t.Fatal("expected desired spec workspace on rebuild operation")
	}
	if response.Operation.DesiredSpec.Workspace.ResetToken == "" {
		t.Fatal("expected workspace reset token to be refreshed")
	}
	if response.Operation.DesiredSpec.Workspace.ResetToken == "workspace-reset-old" {
		t.Fatal("expected rebuild to generate a fresh workspace reset token")
	}
}

func setupRebuildAppTestServer(t *testing.T) (*store.Store, *Server, string, model.Tenant, model.Project) {
	t.Helper()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Rebuild Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "deployer", []string{"app.deploy"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	return s, server, apiKey, tenant, project
}

func createImportedAppForRebuildTest(t *testing.T, s *store.Store, tenantID, projectID, name string, source model.AppSource) model.App {
	t.Helper()

	app, err := s.CreateImportedApp(tenantID, projectID, name, "", model.AppSpec{
		Image:     "registry.example.com/" + name + ":current",
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, source, model.AppRoute{
		Hostname:    name + ".apps.example.com",
		BaseDomain:  "apps.example.com",
		PublicURL:   "https://" + name + ".apps.example.com",
		ServicePort: 80,
	})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}
	return app
}
