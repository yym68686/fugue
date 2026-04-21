package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestPatchAppSourceRebindsOriginWithoutChangingBuildSource(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Source Repair Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "repair", []string{"app.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	upload, err := s.CreateSourceUpload(tenant.ID, "demo.tgz", "application/gzip", []byte("archive-bytes"))
	if err != nil {
		t.Fatalf("create source upload: %v", err)
	}
	app, err := s.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:        "registry.pull.example/fugue-apps/demo:upload-current",
		Replicas:     1,
		RuntimeID:    "runtime_managed_shared",
		RestartToken: "restart_old",
	}, model.AppSource{
		Type:             model.AppSourceTypeUpload,
		UploadID:         upload.ID,
		UploadFilename:   upload.Filename,
		ArchiveSHA256:    upload.SHA256,
		ArchiveSizeBytes: upload.SizeBytes,
		BuildStrategy:    model.AppBuildStrategyDockerfile,
		DockerfilePath:   "Dockerfile",
		BuildContextDir:  ".",
		ImageNameSuffix:  "gateway",
		ComposeService:   "gateway",
		ComposeDependsOn: []string{"runtime"},
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{
		AppBaseDomain: "apps.example.com",
	})

	requestBody, err := json.Marshal(map[string]any{
		"origin_source": map[string]any{
			"repo_url":    "https://github.com/example/demo",
			"repo_branch": "main",
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPatch, "/v1/apps/"+app.ID+"/source", bytes.NewReader(requestBody))
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response struct {
		App            model.App `json:"app"`
		AlreadyCurrent bool      `json:"already_current,omitempty"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.AlreadyCurrent {
		t.Fatal("expected origin repair to report already_current=false")
	}
	if got := model.AppBuildSource(response.App); got == nil || got.Type != model.AppSourceTypeUpload {
		t.Fatalf("expected response build source to remain upload-backed, got %+v", got)
	}
	if got := model.AppOriginSource(response.App); got == nil || got.Type != model.AppSourceTypeGitHubPublic {
		t.Fatalf("expected response origin source to switch to github-public, got %+v", got)
	}

	persistedApp, err := s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get updated app: %v", err)
	}
	if persistedApp.Source == nil || persistedApp.Source.Type != model.AppSourceTypeUpload {
		t.Fatalf("expected persisted current build source to remain upload-backed, got %+v", persistedApp.Source)
	}
	if persistedApp.BuildSource == nil || persistedApp.BuildSource.Type != model.AppSourceTypeUpload {
		t.Fatalf("expected persisted build source to remain upload-backed, got %+v", persistedApp.BuildSource)
	}
	if persistedApp.OriginSource == nil {
		t.Fatal("expected persisted origin source to be recorded")
	}
	if got := persistedApp.OriginSource.Type; got != model.AppSourceTypeGitHubPublic {
		t.Fatalf("expected persisted origin source type %q, got %q", model.AppSourceTypeGitHubPublic, got)
	}
	if got := persistedApp.OriginSource.RepoURL; got != "https://github.com/example/demo" {
		t.Fatalf("expected persisted origin repo url, got %q", got)
	}
	if got := persistedApp.OriginSource.RepoBranch; got != "main" {
		t.Fatalf("expected persisted origin branch main, got %q", got)
	}
	if got := persistedApp.OriginSource.BuildStrategy; got != model.AppBuildStrategyDockerfile {
		t.Fatalf("expected persisted origin build strategy to be preserved, got %q", got)
	}
	if got := persistedApp.OriginSource.DockerfilePath; got != "Dockerfile" {
		t.Fatalf("expected persisted origin dockerfile path to be preserved, got %q", got)
	}
	if got := persistedApp.OriginSource.ComposeService; got != "gateway" {
		t.Fatalf("expected persisted origin compose service to be preserved, got %q", got)
	}
	if len(persistedApp.OriginSource.ComposeDependsOn) != 1 || persistedApp.OriginSource.ComposeDependsOn[0] != "runtime" {
		t.Fatalf("expected persisted origin compose dependencies to be preserved, got %+v", persistedApp.OriginSource.ComposeDependsOn)
	}
}
