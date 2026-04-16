package api

import (
	"net/http"
	"path/filepath"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
	"fugue/internal/workloadidentity"
)

func TestWorkloadTokenImportsIntoOwnProjectAndFiltersAppList(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Workload Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	projectA, err := s.CreateProject(tenant.ID, "Project A", "")
	if err != nil {
		t.Fatalf("create project A: %v", err)
	}
	projectB, err := s.CreateProject(tenant.ID, "Project B", "")
	if err != nil {
		t.Fatalf("create project B: %v", err)
	}
	gatewayApp, err := s.CreateApp(tenant.ID, projectA.ID, "gateway", "", model.AppSpec{
		Image:     "ghcr.io/example/gateway:latest",
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatalf("create gateway app: %v", err)
	}
	if _, err := s.CreateApp(tenant.ID, projectB.ID, "other-project-app", "", model.AppSpec{
		Image:     "ghcr.io/example/other:latest",
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}); err != nil {
		t.Fatalf("create other project app: %v", err)
	}

	authenticator := auth.New(s, "signing-secret")
	server := NewServer(s, authenticator, nil, ServerConfig{
		AppBaseDomain:    "apps.example.com",
		RegistryPushBase: "registry.internal.example",
	})
	token, err := workloadidentity.Issue("signing-secret", workloadidentity.Claims{
		TenantID:  tenant.ID,
		ProjectID: projectA.ID,
		AppID:     gatewayApp.ID,
		Scopes:    []string{"app.write", "app.deploy", "app.delete"},
	})
	if err != nil {
		t.Fatalf("issue workload token: %v", err)
	}

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/import-image", token, map[string]any{
		"image_ref":    "ghcr.io/example/runtime:1.2.3",
		"name":         "session-runtime",
		"service_port": 7777,
		"network_mode": model.AppNetworkModeInternal,
	})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	var importResponse struct {
		App model.App `json:"app"`
	}
	mustDecodeJSON(t, recorder, &importResponse)
	if importResponse.App.ProjectID != projectA.ID {
		t.Fatalf("expected imported app to stay in project A, got %q", importResponse.App.ProjectID)
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/apps", token, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var listResponse struct {
		Apps []model.App `json:"apps"`
	}
	mustDecodeJSON(t, recorder, &listResponse)
	if len(listResponse.Apps) != 2 {
		t.Fatalf("expected 2 visible apps in project A, got %d", len(listResponse.Apps))
	}
	for _, app := range listResponse.Apps {
		if app.ProjectID != projectA.ID {
			t.Fatalf("expected only project A apps, got app %s in project %s", app.ID, app.ProjectID)
		}
	}
}

func TestWorkloadTokenRejectsOtherProjectAndForbiddenEndpoint(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Workload Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	projectA, err := s.CreateProject(tenant.ID, "Project A", "")
	if err != nil {
		t.Fatalf("create project A: %v", err)
	}
	projectB, err := s.CreateProject(tenant.ID, "Project B", "")
	if err != nil {
		t.Fatalf("create project B: %v", err)
	}
	gatewayApp, err := s.CreateApp(tenant.ID, projectA.ID, "gateway", "", model.AppSpec{
		Image:     "ghcr.io/example/gateway:latest",
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatalf("create gateway app: %v", err)
	}

	authenticator := auth.New(s, "signing-secret")
	server := NewServer(s, authenticator, nil, ServerConfig{
		AppBaseDomain:    "apps.example.com",
		RegistryPushBase: "registry.internal.example",
	})
	token, err := workloadidentity.Issue("signing-secret", workloadidentity.Claims{
		TenantID:  tenant.ID,
		ProjectID: projectA.ID,
		AppID:     gatewayApp.ID,
		Scopes:    []string{"app.write", "app.deploy", "app.delete"},
	})
	if err != nil {
		t.Fatalf("issue workload token: %v", err)
	}

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/import-image", token, map[string]any{
		"project_id":   projectB.ID,
		"image_ref":    "ghcr.io/example/runtime:1.2.3",
		"name":         "bad-runtime",
		"service_port": 7777,
		"network_mode": model.AppNetworkModeInternal,
	})
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusForbidden, recorder.Code, recorder.Body.String())
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/projects", token, nil)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected status %d for forbidden endpoint, got %d body=%s", http.StatusForbidden, recorder.Code, recorder.Body.String())
	}
}
