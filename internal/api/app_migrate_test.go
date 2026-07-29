package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestMigrateAppDryRunChecksRuntimeVisibility(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	owner, err := s.CreateTenant("Runtime Owner")
	if err != nil {
		t.Fatalf("create owner tenant: %v", err)
	}
	consumer, err := s.CreateTenant("Runtime Consumer")
	if err != nil {
		t.Fatalf("create consumer tenant: %v", err)
	}
	project, err := s.CreateProject(consumer.ID, "demo", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	targetRuntime, _, err := s.CreateRuntime(owner.ID, "owner-private-runtime", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create target runtime: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(consumer.ID, "tenant-admin", []string{"app.write", "app.migrate"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	app, err := s.CreateApp(consumer.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		RuntimeID: model.DefaultManagedRuntimeID,
		Replicas:  1,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/migrate", apiKey, map[string]any{
		"target_runtime_id": targetRuntime.ID,
		"dry_run":           true,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response struct {
		Impact model.AppMoveImpact `json:"impact"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode dry-run response: %v", err)
	}
	if response.Impact.Pass {
		t.Fatalf("expected dry-run impact to fail for hidden runtime, got %+v", response.Impact)
	}
	foundAccessCheck := false
	for _, check := range response.Impact.Checks {
		if check.Name != "target_runtime_access" {
			continue
		}
		foundAccessCheck = true
		if check.Pass || !strings.Contains(check.Message, "not visible") {
			t.Fatalf("unexpected access check: %+v", check)
		}
	}
	if !foundAccessCheck {
		t.Fatalf("expected target_runtime_access check, got %+v", response.Impact.Checks)
	}
}

func TestMigrateAppDryRunRejectsManifestOnlyTrackedImage(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Image Integrity Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "demo", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	source, _, err := s.CreateRuntime(tenant.ID, "source", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create source runtime: %v", err)
	}
	target, _, err := s.CreateRuntime(tenant.ID, "target", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create target runtime: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.write", "app.migrate"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	imageRef := "registry.example/demo:latest"
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     imageRef,
		RuntimeID: source.ID,
		Replicas:  1,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	if _, err := s.UpsertImage(model.Image{
		TenantID:        tenant.ID,
		AppID:           app.ID,
		ImageRef:        imageRef,
		CanonicalDigest: "sha256:" + strings.Repeat("a", 64),
		LifecycleState:  model.ImageLifecycleAvailable,
	}); err != nil {
		t.Fatalf("upsert manifest-only image: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/migrate", apiKey, map[string]any{
		"target_runtime_id": target.ID,
		"dry_run":           true,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response struct {
		Impact model.AppMoveImpact `json:"impact"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode dry-run response: %v", err)
	}
	if response.Impact.Pass {
		t.Fatalf("expected manifest-only image to block move, got %+v", response.Impact)
	}
	found := false
	for _, check := range response.Impact.Checks {
		if check.Name != "image_blob_integrity" {
			continue
		}
		found = true
		if check.Pass || !strings.Contains(check.Message, "config/layer") {
			t.Fatalf("unexpected image integrity check: %+v", check)
		}
	}
	if !found {
		t.Fatalf("expected image_blob_integrity check, got %+v", response.Impact.Checks)
	}
}

func TestMigrateAppRejectsUnlocalizedManagedPostgres(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Database Dependency Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "demo", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	source, _, err := s.CreateRuntime(tenant.ID, "source", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create source runtime: %v", err)
	}
	target, _, err := s.CreateRuntime(tenant.ID, "target", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create target runtime: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.write", "app.migrate"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		RuntimeID: source.ID,
		Replicas:  1,
		Postgres: &model.AppPostgresSpec{
			Database:    "demo",
			User:        "demo",
			Password:    "secret",
			ServiceName: "demo-postgres",
			RuntimeID:   source.ID,
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})

	dryRun := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/migrate", apiKey, map[string]any{
		"target_runtime_id": target.ID,
		"dry_run":           true,
	})
	if dryRun.Code != http.StatusOK {
		t.Fatalf("expected dry-run status %d, got %d body=%s", http.StatusOK, dryRun.Code, dryRun.Body.String())
	}
	if !strings.Contains(dryRun.Body.String(), "managed Postgres must be localized") {
		t.Fatalf("expected localization dependency in dry-run, got %s", dryRun.Body.String())
	}

	actual := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/migrate", apiKey, map[string]any{
		"target_runtime_id": target.ID,
	})
	if actual.Code != http.StatusBadRequest {
		t.Fatalf("expected direct app move to be rejected until database localization, got %d body=%s", actual.Code, actual.Body.String())
	}
	if !strings.Contains(actual.Body.String(), "managed Postgres must be localized") {
		t.Fatalf("expected actionable localization error, got %s", actual.Body.String())
	}
}

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

func TestMigrateAppRequiresManagedPostgresLocalization(t *testing.T) {
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
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusBadRequest, recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "managed Postgres must be localized") {
		t.Fatalf("expected localization dependency response, got %s", recorder.Body.String())
	}
	ops, err := s.ListOperationsByApp(tenant.ID, true, app.ID)
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	if len(ops) != 0 {
		t.Fatalf("expected no app operation before database localization, got %+v", ops)
	}
}

func TestCreateAppRejectsSharedProjectRWXPersistentStorage(t *testing.T) {
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
	_, err = s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
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
	if !errors.Is(err, store.ErrInvalidInput) {
		t.Fatalf("expected invalid input for disabled shared_project_rwx, got %v", err)
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
