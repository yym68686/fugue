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

func TestMigrateAppRejectsStatefulFailoverBlockers(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Migrate Test Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "demo", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	runtimeObj, _, err := s.CreateRuntime(tenant.ID, "tenant-owned-1", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.write", "app.migrate"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		RuntimeID: runtimeObj.ID,
		Replicas:  1,
		Workspace: &model.AppWorkspaceSpec{MountPath: "/workspace"},
		Postgres: &model.AppPostgresSpec{
			Database: "demo",
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/migrate", apiKey, map[string]any{
		"target_runtime_id": "runtime_owned_1",
	})
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusBadRequest, recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	for _, want := range []string{
		"live transfer is blocked by persistent storage",
		"replicated operator-backed storage before failover",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("expected response to contain %q, got %s", want, body)
		}
	}
}

func TestMigrateAppAllowsManagedPostgresWithoutPersistentStorage(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Migrate Test Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "demo", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	sourceRuntime, _, err := s.CreateRuntime(tenant.ID, "tenant-owned-1", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create source runtime: %v", err)
	}
	targetRuntime, _, err := s.CreateRuntime(tenant.ID, "tenant-owned-2", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create target runtime: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.write", "app.migrate"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		RuntimeID: sourceRuntime.ID,
		Replicas:  1,
		Postgres: &model.AppPostgresSpec{
			Database: "demo",
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/migrate", apiKey, map[string]any{
		"target_runtime_id": targetRuntime.ID,
	})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "\"operation\"") {
		t.Fatalf("expected operation response body, got %s", recorder.Body.String())
	}
	ops, err := s.ListOperationsByApp(tenant.ID, true, app.ID)
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	if len(ops) != 1 || ops[0].DesiredSpec == nil || ops[0].DesiredSpec.Postgres == nil {
		t.Fatalf("expected migrate operation with desired postgres spec, got %+v", ops)
	}
	if got := ops[0].DesiredSpec.Postgres.RuntimeID; got != targetRuntime.ID {
		t.Fatalf("expected desired postgres runtime %q, got %q", targetRuntime.ID, got)
	}
	if got := ops[0].DesiredSpec.Postgres.FailoverTargetRuntimeID; got != "" {
		t.Fatalf("expected postgres failover target to be cleared, got %q", got)
	}
}

func TestMigrateAppAllowsSharedProjectRWXPersistentStorage(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Migrate Test Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "demo", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	sourceRuntime, _, err := s.CreateRuntime(tenant.ID, "tenant-owned-1", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create source runtime: %v", err)
	}
	targetRuntime, _, err := s.CreateRuntime(tenant.ID, "tenant-owned-2", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create target runtime: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.write", "app.migrate"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		RuntimeID: sourceRuntime.ID,
		Replicas:  1,
		PersistentStorage: &model.AppPersistentStorageSpec{
			Mode:        model.AppPersistentStorageModeSharedProjectRWX,
			StorageSize: "1Gi",
			Mounts: []model.AppPersistentStorageMount{
				{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/workspace"},
			},
			SharedSubPath: "argus/sessions/demo",
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/migrate", apiKey, map[string]any{
		"target_runtime_id": targetRuntime.ID,
	})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "\"operation\"") {
		t.Fatalf("expected operation response body, got %s", recorder.Body.String())
	}
}

func TestMigrateAppAllowsMovableRWOPersistentStorage(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Migrate Test Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "demo", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	sourceRuntime, _, err := s.CreateRuntime(tenant.ID, "tenant-owned-1", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create source runtime: %v", err)
	}
	targetRuntime, _, err := s.CreateRuntime(tenant.ID, "tenant-owned-2", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create target runtime: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.write", "app.migrate"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		RuntimeID: sourceRuntime.ID,
		Replicas:  1,
		PersistentStorage: &model.AppPersistentStorageSpec{
			Mode:             model.AppPersistentStorageModeMovableRWO,
			StorageClassName: "fast-rwo",
			StorageSize:      "1Gi",
			Mounts: []model.AppPersistentStorageMount{
				{Kind: model.AppPersistentStorageMountKindDirectory, Path: "/workspace"},
			},
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/migrate", apiKey, map[string]any{
		"target_runtime_id": targetRuntime.ID,
	})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "\"operation\"") {
		t.Fatalf("expected operation response body, got %s", recorder.Body.String())
	}
}

func TestMigrateAppRejectsExternalRuntimeForManagedPostgres(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Migrate Test Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "demo", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	sourceRuntime, _, err := s.CreateRuntime(tenant.ID, "tenant-owned-1", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create source runtime: %v", err)
	}
	targetRuntime, _, err := s.CreateRuntime(tenant.ID, "tenant-external-1", model.RuntimeTypeExternalOwned, "", nil)
	if err != nil {
		t.Fatalf("create target runtime: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.write", "app.migrate"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		RuntimeID: sourceRuntime.ID,
		Replicas:  1,
		Postgres: &model.AppPostgresSpec{
			Database: "demo",
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/migrate", apiKey, map[string]any{
		"target_runtime_id": targetRuntime.ID,
	})
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusBadRequest, recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "invalid input") {
		t.Fatalf("expected invalid input response body, got %s", recorder.Body.String())
	}
}

func TestMigrateAppRecoversFailedImportedAppBaseline(t *testing.T) {
	t.Parallel()

	s, server, _, app, recoveredImage, recoveredSource := setupFailedImportedAppRecoveryServer(t)
	targetRuntime, _, err := s.CreateRuntime(app.TenantID, "tenant-vps-1", model.RuntimeTypeExternalOwned, "https://vps.example.com", nil)
	if err != nil {
		t.Fatalf("create target runtime: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(app.TenantID, "tenant-admin", []string{"app.write", "app.migrate"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/migrate", apiKey, map[string]any{
		"target_runtime_id": targetRuntime.ID,
	})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}

	ops, err := s.ListOperationsByApp(app.TenantID, false, app.ID)
	if err != nil {
		t.Fatalf("list app operations: %v", err)
	}

	var migrateOp model.Operation
	found := false
	for _, op := range ops {
		if op.Type != model.OperationTypeMigrate || op.Status != model.OperationStatusPending {
			continue
		}
		migrateOp = op
		found = true
	}
	if !found {
		t.Fatal("expected pending migrate operation")
	}
	if migrateOp.DesiredSpec == nil {
		t.Fatal("expected migrate operation desired spec")
	}
	if migrateOp.DesiredSource == nil {
		t.Fatal("expected migrate operation desired source")
	}
	if got := migrateOp.DesiredSpec.Image; got != recoveredImage {
		t.Fatalf("expected recovered image %q, got %q", recoveredImage, got)
	}
	if got := migrateOp.DesiredSpec.RuntimeID; got != targetRuntime.ID {
		t.Fatalf("expected desired runtime %q, got %q", targetRuntime.ID, got)
	}
	if got := migrateOp.TargetRuntimeID; got != targetRuntime.ID {
		t.Fatalf("expected target runtime %q, got %q", targetRuntime.ID, got)
	}
	if got := migrateOp.DesiredSource.ResolvedImageRef; got != recoveredSource.ResolvedImageRef {
		t.Fatalf("expected recovered source image ref %q, got %q", recoveredSource.ResolvedImageRef, got)
	}
}
