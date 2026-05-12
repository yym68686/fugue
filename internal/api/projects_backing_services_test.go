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

func TestPatchProjectDefaultRuntime(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Project Runtime Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	raiseManagedTestCap(t, s, tenant.ID)
	runtimeObj, _, err := s.CreateRuntime(tenant.ID, "primary-vps", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "project-admin", []string{"project.write", "app.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	recorder := performJSONRequest(t, server, http.MethodPatch, "/v1/projects/"+project.ID, apiKey, map[string]any{
		"default_runtime_id": runtimeObj.ID,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response struct {
		Project model.Project `json:"project"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Project.DefaultRuntimeID != runtimeObj.ID {
		t.Fatalf("expected default runtime %q, got %q", runtimeObj.ID, response.Project.DefaultRuntimeID)
	}

	app, err := s.CreateApp(tenant.ID, project.ID, "defaulted", "", model.AppSpec{
		Image:    "nginx:1.27",
		Ports:    []int{80},
		Replicas: 1,
	})
	if err != nil {
		t.Fatalf("create defaulted app: %v", err)
	}
	if app.Spec.RuntimeID != runtimeObj.ID {
		t.Fatalf("expected app runtime %q, got %q", runtimeObj.ID, app.Spec.RuntimeID)
	}

	recorder = performJSONRequest(t, server, http.MethodPatch, "/v1/projects/"+project.ID, apiKey, map[string]any{
		"clear_default_runtime_id": true,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected clear status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	response = struct {
		Project model.Project `json:"project"`
	}{}
	mustDecodeJSON(t, recorder, &response)
	if response.Project.DefaultRuntimeID != "" {
		t.Fatalf("expected cleared default runtime, got %q", response.Project.DefaultRuntimeID)
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

	events, err := s.ListAuditEvents(tenant.ID, false, 0)
	if err != nil {
		t.Fatalf("list audit events: %v", err)
	}
	var sawDeleteRequest bool
	var sawFinalDelete bool
	for _, event := range events {
		if event.TargetID != project.ID {
			continue
		}
		switch event.Action {
		case "project.delete_request":
			sawDeleteRequest = true
		case "project.delete":
			if event.ActorType == model.ActorTypeSystem &&
				event.ActorID == "project-delete-finalizer" &&
				event.Metadata["finalized_from_request"] == "true" {
				sawFinalDelete = true
			}
		}
	}
	if !sawDeleteRequest {
		t.Fatalf("expected project.delete_request audit event for %s, got %+v", project.ID, events)
	}
	if !sawFinalDelete {
		t.Fatalf("expected finalizer project.delete audit event for %s, got %+v", project.ID, events)
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

func TestMigrateBackingServiceQueuesManagedPostgresSwitchover(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Backing Service Move Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	raiseManagedTestCap(t, s, tenant.ID)
	sourceRuntime, _, err := s.CreateRuntime(tenant.ID, "source-vps", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create source runtime: %v", err)
	}
	targetRuntime, _, err := s.CreateRuntime(tenant.ID, "target-vps", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create target runtime: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: sourceRuntime.ID,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "project-admin", []string{"project.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	service, err := s.CreateBackingService(tenant.ID, project.ID, "main-db", "", model.BackingServiceSpec{
		Postgres: &model.AppPostgresSpec{
			RuntimeID:                        sourceRuntime.ID,
			FailoverTargetRuntimeID:          targetRuntime.ID,
			PrimaryNodeName:                  "old-node",
			PrimaryPlacementPendingRebalance: true,
			Database:                         "demo",
			User:                             "demo",
			Password:                         "secret",
		},
	})
	if err != nil {
		t.Fatalf("create backing service: %v", err)
	}
	if _, err := s.BindBackingService(tenant.ID, app.ID, service.ID, "", nil); err != nil {
		t.Fatalf("bind backing service: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/backing-services/"+service.ID+"/migrate", apiKey, map[string]any{
		"target_runtime_id": targetRuntime.ID,
	})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}
	var response struct {
		BackingService model.BackingService `json:"backing_service"`
		Operation      model.Operation      `json:"operation"`
	}
	mustDecodeJSON(t, recorder, &response)
	postgres := response.BackingService.Spec.Postgres
	if postgres == nil {
		t.Fatalf("expected postgres spec, got %+v", response.BackingService)
	}
	if postgres.RuntimeID != sourceRuntime.ID {
		t.Fatalf("expected service response to remain on source runtime %q before switchover, got %q", sourceRuntime.ID, postgres.RuntimeID)
	}
	if response.Operation.Type != model.OperationTypeDatabaseSwitchover {
		t.Fatalf("expected database switchover operation, got %+v", response.Operation)
	}
	if response.Operation.AppID != app.ID || response.Operation.ServiceID != service.ID {
		t.Fatalf("expected operation scoped to app %s service %s, got app=%s service=%s", app.ID, service.ID, response.Operation.AppID, response.Operation.ServiceID)
	}
	if response.Operation.SourceRuntimeID != sourceRuntime.ID || response.Operation.TargetRuntimeID != targetRuntime.ID {
		t.Fatalf("expected operation runtimes %s -> %s, got %s -> %s", sourceRuntime.ID, targetRuntime.ID, response.Operation.SourceRuntimeID, response.Operation.TargetRuntimeID)
	}
	persisted, err := s.GetBackingService(service.ID)
	if err != nil {
		t.Fatalf("get persisted service: %v", err)
	}
	if persisted.Spec.Postgres == nil || persisted.Spec.Postgres.RuntimeID != sourceRuntime.ID {
		t.Fatalf("expected persisted service to stay on source runtime until controller switchover, got %+v", persisted.Spec.Postgres)
	}
}

func TestLocalizeBackingServiceQueuesManagedPostgresLocalize(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Backing Service Localize Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	raiseManagedTestCap(t, s, tenant.ID)
	sourceRuntime, _, err := s.CreateRuntime(tenant.ID, "source-vps", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create source runtime: %v", err)
	}
	targetRuntime, _, err := s.CreateRuntime(tenant.ID, "target-vps", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create target runtime: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: sourceRuntime.ID,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "project-admin", []string{"project.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	service, err := s.CreateBackingService(tenant.ID, project.ID, "main-db", "", model.BackingServiceSpec{
		Postgres: &model.AppPostgresSpec{
			RuntimeID:                        sourceRuntime.ID,
			FailoverTargetRuntimeID:          targetRuntime.ID,
			PrimaryNodeName:                  "old-node",
			PrimaryPlacementPendingRebalance: true,
			Instances:                        2,
			SynchronousReplicas:              1,
			Database:                         "demo",
			User:                             "demo",
			Password:                         "secret",
		},
	})
	if err != nil {
		t.Fatalf("create backing service: %v", err)
	}
	if _, err := s.BindBackingService(tenant.ID, app.ID, service.ID, "", nil); err != nil {
		t.Fatalf("bind backing service: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/backing-services/"+service.ID+"/localize", apiKey, map[string]any{
		"target_runtime_id": targetRuntime.ID,
		"target_node_name":  "target-node",
	})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	var response struct {
		BackingService model.BackingService `json:"backing_service"`
		Operation      model.Operation      `json:"operation"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.BackingService.Spec.Postgres == nil || response.BackingService.Spec.Postgres.RuntimeID != sourceRuntime.ID {
		t.Fatalf("expected response service to remain on source runtime before controller apply, got %+v", response.BackingService.Spec.Postgres)
	}
	op := response.Operation
	if op.Type != model.OperationTypeDatabaseLocalize {
		t.Fatalf("expected database localize operation, got %+v", op)
	}
	if op.AppID != app.ID || op.ServiceID != service.ID {
		t.Fatalf("expected operation scoped to app %s service %s, got app=%s service=%s", app.ID, service.ID, op.AppID, op.ServiceID)
	}
	if op.SourceRuntimeID != sourceRuntime.ID || op.TargetRuntimeID != targetRuntime.ID {
		t.Fatalf("expected operation runtimes %s -> %s, got %s -> %s", sourceRuntime.ID, targetRuntime.ID, op.SourceRuntimeID, op.TargetRuntimeID)
	}
	if op.DesiredSpec == nil || op.DesiredSpec.Postgres == nil {
		t.Fatalf("expected desired postgres spec, got %+v", op.DesiredSpec)
	}
	postgres := op.DesiredSpec.Postgres
	if postgres.RuntimeID != targetRuntime.ID || postgres.FailoverTargetRuntimeID != "" || postgres.PrimaryNodeName != "target-node" {
		t.Fatalf("unexpected desired postgres placement: %+v", postgres)
	}
	if postgres.Instances != 1 || postgres.SynchronousReplicas != 0 || postgres.PrimaryPlacementPendingRebalance {
		t.Fatalf("expected single localized postgres spec, got %+v", postgres)
	}

	persisted, err := s.GetBackingService(service.ID)
	if err != nil {
		t.Fatalf("get persisted service: %v", err)
	}
	if persisted.Spec.Postgres == nil || persisted.Spec.Postgres.RuntimeID != sourceRuntime.ID {
		t.Fatalf("expected persisted service to stay on source runtime until controller localize, got %+v", persisted.Spec.Postgres)
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
