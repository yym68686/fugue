package api

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestBuildConsoleProjectLifecycleUsesUpdatingForMixedLiveAndPending(t *testing.T) {
	t.Parallel()

	lifecycle := buildConsoleProjectLifecycle(
		[]string{"importing", "building"},
		1,
		2,
		true,
		true,
		true,
	)

	if lifecycle.Label != "Updating" {
		t.Fatalf("expected mixed live/pending lifecycle to be Updating, got %q", lifecycle.Label)
	}
	if !lifecycle.Live {
		t.Fatal("expected updating lifecycle to stay live")
	}
	if lifecycle.SyncMode != "active" {
		t.Fatalf("expected active sync mode, got %q", lifecycle.SyncMode)
	}
	if lifecycle.Tone != "info" {
		t.Fatalf("expected info tone, got %q", lifecycle.Tone)
	}
}

func TestBuildConsoleProjectLifecycleKeepsBuildingForPendingOnly(t *testing.T) {
	t.Parallel()

	lifecycle := buildConsoleProjectLifecycle(
		[]string{"building"},
		1,
		1,
		true,
		false,
		true,
	)

	if lifecycle.Label != "Building" {
		t.Fatalf("expected pending-only lifecycle to stay Building, got %q", lifecycle.Label)
	}
}

func TestReadConsoleActiveReleaseOperationIgnoresPendingDeployForFailedApp(t *testing.T) {
	t.Parallel()

	operation := &model.Operation{
		ID:     "op_demo",
		Type:   model.OperationTypeDeploy,
		Status: model.OperationStatusRunning,
	}
	app := model.App{
		Status: model.AppStatus{
			Phase: "failed",
		},
	}

	if got := readConsoleActiveReleaseOperation(operation, app); got != nil {
		t.Fatalf("expected failed app to ignore active deploy operation, got %+v", got)
	}
}

func TestScopeConsoleActiveOperationsKeepsTenantOwnedOperations(t *testing.T) {
	t.Parallel()

	principal := model.Principal{
		TenantID: "tenant_a",
		Scopes:   map[string]struct{}{},
	}
	operations := []model.Operation{
		{ID: "op_owned", TenantID: "tenant_a", AppID: "app_a"},
		{ID: "op_other", TenantID: "tenant_b", AppID: "app_b"},
	}

	filtered := scopeConsoleActiveOperations(principal, operations)
	if len(filtered) != 1 {
		t.Fatalf("expected 1 tenant-scoped operation, got %d", len(filtered))
	}
	if filtered[0].ID != "op_owned" {
		t.Fatalf("expected owned operation to remain, got %q", filtered[0].ID)
	}
}

func TestScopeConsoleActiveOperationsKeepsPlatformAdminView(t *testing.T) {
	t.Parallel()

	principal := model.Principal{
		TenantID: "tenant_a",
		Scopes: map[string]struct{}{
			"platform.admin": {},
		},
	}
	operations := []model.Operation{
		{ID: "op_owned", TenantID: "tenant_a", AppID: "app_a"},
		{ID: "op_other", TenantID: "tenant_b", AppID: "app_b"},
	}

	filtered := scopeConsoleActiveOperations(principal, operations)
	if len(filtered) != 2 {
		t.Fatalf("expected platform admin to keep all active operations, got %d", len(filtered))
	}
}

func TestConsoleGallerySkipsLiveStatusOverlayByDefault(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Console Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "demo", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "viewer", []string{"project.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	if _, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: tenantSharedRuntimeID,
	}); err != nil {
		t.Fatalf("create app: %v", err)
	}

	managedStatusCalls := 0

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	server.newManagedAppStatusClient = func() (*managedAppStatusClient, error) {
		managedStatusCalls++
		return nil, errors.New("unexpected managed status lookup")
	}

	recorder := performJSONRequest(
		t,
		server,
		http.MethodGet,
		"/v1/console/gallery",
		apiKey,
		nil,
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	if managedStatusCalls != 0 {
		t.Fatalf("expected console gallery to skip live status overlay by default, got %d calls", managedStatusCalls)
	}
}

func TestConsoleGallerySkipsResourceOverlayByDefault(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Console Cache Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "demo", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "viewer", []string{"project.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	if _, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: tenantSharedRuntimeID,
	}); err != nil {
		t.Fatalf("create app: %v", err)
	}

	clusterInventoryCalls := 0

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		clusterInventoryCalls++
		return nil, errors.New("unexpected cluster inventory lookup")
	}

	first := performJSONRequest(
		t,
		server,
		http.MethodGet,
		"/v1/console/gallery",
		apiKey,
		nil,
	)
	if first.Code != http.StatusOK {
		t.Fatalf("expected first status %d, got %d body=%s", http.StatusOK, first.Code, first.Body.String())
	}

	second := performJSONRequest(
		t,
		server,
		http.MethodGet,
		"/v1/console/gallery",
		apiKey,
		nil,
	)
	if second.Code != http.StatusOK {
		t.Fatalf("expected second status %d, got %d body=%s", http.StatusOK, second.Code, second.Body.String())
	}

	if clusterInventoryCalls != 0 {
		t.Fatalf("expected console gallery summary to skip resource overlay by default, got %d lookups", clusterInventoryCalls)
	}
}

func TestConsoleGalleryHashIgnoresInactiveDatabaseSwitchoverOperations(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Console Hash Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "demo", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	appRuntime, _, err := s.CreateRuntime(tenant.ID, "app-runtime", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create app runtime: %v", err)
	}
	databaseSource, _, err := s.CreateRuntime(tenant.ID, "db-source", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create database source runtime: %v", err)
	}
	databaseTarget, _, err := s.CreateRuntime(tenant.ID, "db-target", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create database target runtime: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: appRuntime.ID,
		Postgres: &model.AppPostgresSpec{
			Database:  "demo",
			RuntimeID: databaseSource.ID,
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	deploySpec := app.Spec
	deployOp, err := s.CreateOperation(model.Operation{
		TenantID:    tenant.ID,
		Type:        model.OperationTypeDeploy,
		AppID:       app.ID,
		DesiredSpec: &deploySpec,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}
	if _, err := s.CompleteManagedOperation(deployOp.ID, "/tmp/demo.yaml", "deployed"); err != nil {
		t.Fatalf("complete deploy operation: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	server.consoleGalleryCache = newExpiringResponseCache[consoleGalleryResponse](0)
	principal := model.Principal{TenantID: tenant.ID}

	initialHash, err := server.buildConsoleGalleryHash(context.Background(), principal, false)
	if err != nil {
		t.Fatalf("build initial hash: %v", err)
	}

	op, err := s.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeDatabaseSwitchover,
		AppID:           app.ID,
		TargetRuntimeID: databaseTarget.ID,
	})
	if err != nil {
		t.Fatalf("create database switchover operation: %v", err)
	}
	if _, err := s.FailOperation(op.ID, "database switchover unavailable"); err != nil {
		t.Fatalf("fail database switchover operation: %v", err)
	}

	nextHash, err := server.buildConsoleGalleryHash(context.Background(), principal, false)
	if err != nil {
		t.Fatalf("build next hash: %v", err)
	}

	if initialHash != nextHash {
		t.Fatalf("expected hash to ignore inactive database switchover operations, got %q then %q", initialHash, nextHash)
	}
}
