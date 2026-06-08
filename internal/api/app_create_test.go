package api

import (
	"net/http"
	"path/filepath"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestCreateAppStagesGitHubSource(t *testing.T) {
	t.Parallel()

	s, server, apiKey, existingApp := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps", apiKey, map[string]any{
		"tenant_id":  existingApp.TenantID,
		"project_id": existingApp.ProjectID,
		"name":       "worker",
		"spec": map[string]any{
			"runtime_id":   "runtime_managed_shared",
			"replicas":     1,
			"network_mode": "background",
		},
		"source": map[string]any{
			"repo_url":       "example/worker",
			"repo_branch":    "main",
			"build_strategy": "buildpacks",
		},
	})
	if recorder.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusCreated, recorder.Code, recorder.Body.String())
	}

	var response struct {
		App model.App `json:"app"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.App.Name != "worker" {
		t.Fatalf("expected app name worker, got %q", response.App.Name)
	}
	if response.App.Status.Phase != "importing" {
		t.Fatalf("expected importing phase, got %q", response.App.Status.Phase)
	}
	if response.App.Source == nil {
		t.Fatal("expected staged source on created app")
	}
	if response.App.Source.Type != model.AppSourceTypeGitHubPublic {
		t.Fatalf("expected github public source, got %+v", response.App.Source)
	}
	if response.App.Source.RepoURL != "example/worker" || response.App.Source.RepoBranch != "main" || response.App.Source.BuildStrategy != model.AppBuildStrategyBuildpacks {
		t.Fatalf("unexpected created source %+v", response.App.Source)
	}

	storedApp, err := s.GetApp(response.App.ID)
	if err != nil {
		t.Fatalf("get stored app: %v", err)
	}
	if storedApp.Source == nil || storedApp.Source.Type != model.AppSourceTypeGitHubPublic {
		t.Fatalf("expected stored staged source, got %+v", storedApp.Source)
	}
	if storedApp.Status.Phase != "importing" {
		t.Fatalf("expected stored app phase importing, got %q", storedApp.Status.Phase)
	}
	if storedApp.Route != nil {
		t.Fatalf("expected no auto route without app base domain, got %+v", storedApp.Route)
	}
}

func TestCreateAppDefaultsManagedPostgresStorageClass(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Managed Postgres Storage Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "demo", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	raiseManagedTestCap(t, s, tenant.ID)
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{ManagedPostgresStorageClass: "fugue-postgres-rwo"})

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps", apiKey, map[string]any{
		"tenant_id":  tenant.ID,
		"project_id": project.ID,
		"name":       "api",
		"spec": map[string]any{
			"image":    "ghcr.io/example/api:latest",
			"replicas": 1,
			"postgres": map[string]any{
				"database": "api",
				"user":     "api",
				"password": "secret",
			},
		},
	})
	if recorder.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusCreated, recorder.Code, recorder.Body.String())
	}

	var response struct {
		App model.App `json:"app"`
	}
	mustDecodeJSON(t, recorder, &response)
	postgres := store.OwnedManagedPostgresSpec(response.App)
	if postgres == nil {
		t.Fatalf("expected managed postgres spec, got %+v", response.App)
	}
	if got := postgres.StorageClassName; got != "fugue-postgres-rwo" {
		t.Fatalf("expected default managed postgres storage class, got %q", got)
	}
}
