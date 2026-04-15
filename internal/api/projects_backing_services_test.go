package api

import (
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestPatchProjectUpdatesNameAndDescription(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Projects Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "web", "legacy description")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "project-admin", []string{"project.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	recorder := performJSONRequest(t, server, http.MethodPatch, "/v1/projects/"+project.ID, apiKey, map[string]any{
		"name":        "backend core",
		"description": "core services",
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response struct {
		Project model.Project `json:"project"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Project.Name != "backend core" {
		t.Fatalf("expected updated name, got %q", response.Project.Name)
	}
	if response.Project.Description != "core services" {
		t.Fatalf("expected updated description, got %q", response.Project.Description)
	}
	if response.Project.Slug != "backend-core" {
		t.Fatalf("expected updated slug backend-core, got %q", response.Project.Slug)
	}
}

func TestDeleteProjectCascadeQueuesDeleteAndFinalizesProject(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Project Cascade Delete Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	raiseManagedTestCap(t, s, tenant.ID)
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.deploy", "app.write", "project.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/backing-services", apiKey, map[string]any{
		"project_id": project.ID,
		"name":       "main-db",
		"spec": model.BackingServiceSpec{
			Postgres: &model.AppPostgresSpec{Database: "demo", User: "demo", Password: "secret"},
		},
	})
	if recorder.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusCreated, recorder.Code, recorder.Body.String())
	}

	var createServiceResponse struct {
		BackingService model.BackingService `json:"backing_service"`
	}
	mustDecodeJSON(t, recorder, &createServiceResponse)

	recorder = performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/bindings", apiKey, map[string]any{
		"service_id": createServiceResponse.BackingService.ID,
	})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	recorder = performJSONRequest(t, server, http.MethodDelete, "/v1/projects/"+project.ID+"?cascade=true", apiKey, nil)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	var deleteResponse struct {
		DeleteRequested bool              `json:"delete_requested"`
		Deleted         bool              `json:"deleted"`
		Operations      []model.Operation `json:"operations"`
		Project         model.Project     `json:"project"`
	}
	mustDecodeJSON(t, recorder, &deleteResponse)
	if !deleteResponse.DeleteRequested {
		t.Fatal("expected delete_requested=true")
	}
	if deleteResponse.Deleted {
		t.Fatal("expected project delete to remain in progress")
	}
	if len(deleteResponse.Operations) != 1 {
		t.Fatalf("expected one queued delete operation, got %+v", deleteResponse.Operations)
	}
	if deleteResponse.Operations[0].Type != model.OperationTypeDelete {
		t.Fatalf("expected delete operation, got %q", deleteResponse.Operations[0].Type)
	}

	app, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app after project delete request: %v", err)
	}
	if got := strings.TrimSpace(strings.ToLower(app.Status.Phase)); !strings.Contains(got, "deleting") {
		t.Fatalf("expected deleting phase after cascade delete request, got %q", app.Status.Phase)
	}

	completeNextManagedOperation(t, s)
	completeNextManagedOperation(t, s)

	if _, err := s.GetProject(project.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected project to be deleted after delete operation completed, got %v", err)
	}
	if _, err := s.GetBackingService(createServiceResponse.BackingService.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected backing service to be deleted with project cleanup, got %v", err)
	}
}

func TestBackingServiceLifecycleAndBindingsQueueDeploy(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Backing Services Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	raiseManagedTestCap(t, s, tenant.ID)
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.write", "app.deploy"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	if _, err := s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{
		CPUMilliCores:    750,
		MemoryMebibytes:  1536,
		StorageGibibytes: 1,
	}); err != nil {
		t.Fatalf("raise billing cap: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/backing-services", apiKey, map[string]any{
		"project_id":  project.ID,
		"name":        "main-db",
		"description": "primary database",
		"spec":        model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{Database: "demo", User: "demo", Password: "secret"}},
	})
	if recorder.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusCreated, recorder.Code, recorder.Body.String())
	}

	var createServiceResponse struct {
		BackingService model.BackingService `json:"backing_service"`
	}
	mustDecodeJSON(t, recorder, &createServiceResponse)
	service := createServiceResponse.BackingService
	if service.ID == "" {
		t.Fatal("expected backing service id")
	}

	recorder = performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/bindings", apiKey, map[string]any{
		"service_id": service.ID,
	})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	var bindResponse struct {
		Binding   model.ServiceBinding `json:"binding"`
		Operation model.Operation      `json:"operation"`
	}
	mustDecodeJSON(t, recorder, &bindResponse)
	if bindResponse.Binding.ID == "" {
		t.Fatal("expected binding id")
	}
	if bindResponse.Operation.Type != model.OperationTypeDeploy {
		t.Fatalf("expected deploy operation, got %q", bindResponse.Operation.Type)
	}

	completeNextManagedOperation(t, s)

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/env", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var envResponse struct {
		Env map[string]string `json:"env"`
	}
	mustDecodeJSON(t, recorder, &envResponse)
	if got := envResponse.Env["DB_HOST"]; got != model.PostgresRWServiceName(service.Spec.Postgres.ServiceName) {
		t.Fatalf("expected DB_HOST=%q, got %q", model.PostgresRWServiceName(service.Spec.Postgres.ServiceName), got)
	}
	if got := envResponse.Env["DB_NAME"]; got != "demo" {
		t.Fatalf("expected DB_NAME=demo, got %q", got)
	}

	recorder = performJSONRequest(t, server, http.MethodDelete, "/v1/apps/"+app.ID+"/bindings/"+bindResponse.Binding.ID, apiKey, nil)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}
	var unbindResponse struct {
		Operation model.Operation `json:"operation"`
	}
	mustDecodeJSON(t, recorder, &unbindResponse)
	if unbindResponse.Operation.Type != model.OperationTypeDeploy {
		t.Fatalf("expected deploy operation for unbind, got %q", unbindResponse.Operation.Type)
	}

	completeNextManagedOperation(t, s)

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/env", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	envResponse = struct {
		Env map[string]string `json:"env"`
	}{}
	mustDecodeJSON(t, recorder, &envResponse)
	if _, ok := envResponse.Env["DB_HOST"]; ok {
		t.Fatalf("expected DB_HOST to be removed after unbind, got %v", envResponse.Env)
	}

	recorder = performJSONRequest(t, server, http.MethodDelete, "/v1/backing-services/"+service.ID, apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	if _, err := s.GetBackingService(service.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected deleted backing service to be gone, got %v", err)
	}
}

func TestGetAppIncludesStructuredTechStack(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Tech Stack Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	raiseManagedTestCap(t, s, tenant.ID)
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "viewer", []string{"app.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	app, err := s.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Postgres: &model.AppPostgresSpec{
			Database: "demo",
			User:     "demo",
			Password: "secret",
		},
	}, model.AppSource{
		Type:             model.AppSourceTypeGitHubPublic,
		RepoURL:          "https://github.com/example/demo",
		BuildStrategy:    model.AppBuildStrategyDockerfile,
		DetectedProvider: model.AppBuildStrategyDockerfile,
		DetectedStack:    "nextjs",
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID, apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response struct {
		App model.App `json:"app"`
	}
	mustDecodeJSON(t, recorder, &response)
	if len(response.App.TechStack) != 1 {
		t.Fatalf("expected one tech_stack entry, got %+v", response.App.TechStack)
	}
	if tech := response.App.TechStack[0]; tech.Kind != "stack" || tech.Slug != "nextjs" || tech.Name != "Next.js" {
		t.Fatalf("expected nextjs tech stack entry, got %+v", tech)
	}
	if response.App.TechStack[0].Source != "detected" {
		t.Fatalf("expected nextjs source=detected, got %q", response.App.TechStack[0].Source)
	}
}

func TestImportGitHubInlineProjectRollbackOnSynchronousFailure(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Import Rollback Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "importer", []string{"app.write", "app.deploy"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{
		AppBaseDomain:    "apps.example.com",
		RegistryPushBase: "registry.internal.example",
	})
	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/import-github", apiKey, map[string]any{
		"tenant_id":       tenant.ID,
		"repo_url":        "https://github.com/example/demo",
		"project":         map[string]any{"name": "import-target", "description": "temporary import project"},
		"build_strategy":  model.AppBuildStrategyDockerfile,
		"dockerfile_path": "Dockerfile",
		"runtime_id":      "runtime_missing",
	})
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusNotFound, recorder.Code, recorder.Body.String())
	}

	projects, err := s.ListProjects(tenant.ID)
	if err != nil {
		t.Fatalf("list projects: %v", err)
	}
	if len(projects) != 0 {
		t.Fatalf("expected no project residue after failed import, got %+v", projects)
	}

	apps, err := s.ListApps(tenant.ID, false)
	if err != nil {
		t.Fatalf("list apps: %v", err)
	}
	if len(apps) != 0 {
		t.Fatalf("expected no app residue after failed import, got %+v", apps)
	}

	ops, err := s.ListOperations(tenant.ID, false)
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("expected no operation residue after failed import, got %+v", ops)
	}
}
