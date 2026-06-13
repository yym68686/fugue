package api

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestAppBackupStatusReportsDisabledByDefault(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Backup Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	runtime, _, err := stateStore.CreateRuntime(tenant.ID, "tenant-owned", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "postgres-app", "", model.AppSpec{
		Image:     "ghcr.io/example/app:latest",
		RuntimeID: runtime.ID,
		Replicas:  1,
		Postgres: &model.AppPostgresSpec{
			Database: "appdb",
		},
		Workspace: &model.AppWorkspaceSpec{MountPath: "/workspace"},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	_, apiKey, err := stateStore.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.read", "backup.read"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/backups/status", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response struct {
		Policies []model.BackupPolicy  `json:"policies"`
		Posture  []model.BackupPosture `json:"posture"`
	}
	mustDecodeJSON(t, recorder, &response)
	if len(response.Policies) != 0 {
		t.Fatalf("expected no app backup policies by default, got %+v", response.Policies)
	}
	if len(response.Posture) != 2 {
		t.Fatalf("expected database and storage posture, got %+v", response.Posture)
	}
	for _, posture := range response.Posture {
		if posture.Status != "disabled" {
			t.Fatalf("expected app backup target %s to be disabled by default, got %+v", posture.Target.Type, posture)
		}
		if posture.PolicyID != "" {
			t.Fatalf("expected no policy id for disabled default target, got %+v", posture)
		}
	}
}

func TestCreateAppBackupPolicyResolvesManagedPostgresTarget(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Backup Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	runtime, _, err := stateStore.CreateRuntime(tenant.ID, "tenant-owned", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "postgres-app", "", model.AppSpec{
		Image:     "ghcr.io/example/app:latest",
		RuntimeID: runtime.ID,
		Replicas:  1,
		Postgres: &model.AppPostgresSpec{
			Database: "appdb",
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	_, apiKey, err := stateStore.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.read", "backup.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/backups/policies", apiKey, map[string]any{
		"target": map[string]any{"type": model.BackupTargetAppDatabase},
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response struct {
		Policy model.BackupPolicy `json:"policy"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Policy.Target.Database != "appdb" {
		t.Fatalf("expected app database target to be resolved, got %+v", response.Policy.Target)
	}
	if response.Policy.Target.RuntimeID != runtime.ID {
		t.Fatalf("expected app database runtime %q, got %+v", runtime.ID, response.Policy.Target)
	}
	if response.Policy.Target.ServiceName == "" {
		t.Fatalf("expected app database service name to be resolved, got %+v", response.Policy.Target)
	}
	if response.Policy.Status != model.BackupPolicyStatusBlockedNoBackend {
		t.Fatalf("expected app backup policy without backend to be blocked, got %+v", response.Policy)
	}
}

func TestCreateAppBackupPolicyRejectsAppWithoutManagedPostgres(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Backup Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	runtime, _, err := stateStore.CreateRuntime(tenant.ID, "tenant-owned", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "stateless-app", "", model.AppSpec{
		Image:     "ghcr.io/example/app:latest",
		RuntimeID: runtime.ID,
		Replicas:  1,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	_, apiKey, err := stateStore.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.read", "backup.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/backups/policies", apiKey, map[string]any{
		"target": map[string]any{"type": model.BackupTargetAppDatabase},
	})
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusBadRequest, recorder.Code, recorder.Body.String())
	}
}

func TestControlPlaneBackupRunCreatesRestorePlan(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	backend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		Name:     "r2",
		Provider: model.DataBackendProviderCloudflareR2,
		Bucket:   "bucket",
		Endpoint: "https://example.r2.cloudflarestorage.com",
	})
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}
	policy, err := stateStore.UpsertBackupPolicy(model.BackupPolicy{
		Name:      "control-plane",
		Target:    model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase},
		BackendID: backend.ID,
		Enabled:   true,
		Status:    model.BackupPolicyStatusActive,
		Schedule:  model.BackupDefaultSchedule,
		Retention: model.BackupRetentionPolicy{RetainCount: model.BackupDefaultRetainCount},
	})
	if err != nil {
		t.Fatalf("create policy: %v", err)
	}
	server := NewServer(stateStore, auth.New(stateStore, "bootstrap-secret"), nil, ServerConfig{})
	artifactCh := make(chan model.BackupArtifact, 1)
	server.backupRunner = func(ctx context.Context, run model.BackupRun) ([]model.BackupArtifact, error) {
		artifact, err := stateStore.CreateBackupArtifact(model.BackupArtifact{
			RunID:             run.ID,
			PolicyID:          run.PolicyID,
			Target:            run.Target,
			BackendID:         run.BackendID,
			Kind:              model.BackupArtifactKindControlPlanePGDump,
			Version:           "test-control-plane",
			ObjectKey:         "control-plane/postgres.dump",
			ManifestObjectKey: "control-plane/manifest.json",
			SHA256:            strings.Repeat("a", 64),
			SizeBytes:         1024,
			LogicalBytes:      2048,
			Status:            model.BackupArtifactStatusActive,
		})
		if err != nil {
			return nil, err
		}
		artifactCh <- artifact
		return []model.BackupArtifact{artifact}, nil
	}

	runRecorder := performJSONRequest(t, server, http.MethodPost, "/v1/backups/runs", "bootstrap-secret", map[string]any{
		"policy_id": policy.ID,
		"wait":      true,
	})
	if runRecorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, runRecorder.Code, runRecorder.Body.String())
	}
	var runResponse struct {
		Run model.BackupRun `json:"run"`
	}
	mustDecodeJSON(t, runRecorder, &runResponse)
	if runResponse.Run.Status != model.BackupRunStatusSucceeded {
		t.Fatalf("expected succeeded run, got %+v", runResponse.Run)
	}
	if runResponse.Run.BytesWritten != 1024 || runResponse.Run.ArtifactCount != 1 {
		t.Fatalf("expected backup run output counters, got %+v", runResponse.Run)
	}

	var artifact model.BackupArtifact
	select {
	case artifact = <-artifactCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for control-plane backup artifact")
	}
	planRecorder := performJSONRequest(t, server, http.MethodPost, "/v1/backups/restore-plans", "bootstrap-secret", map[string]any{
		"artifact_id": artifact.ID,
		"mode":        "offline",
	})
	if planRecorder.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusCreated, planRecorder.Code, planRecorder.Body.String())
	}
	var planResponse struct {
		Plan model.BackupRestorePlan `json:"plan"`
	}
	mustDecodeJSON(t, planRecorder, &planResponse)
	if planResponse.Plan.ArtifactID != artifact.ID || planResponse.Plan.Target.Type != model.BackupTargetControlPlaneDatabase {
		t.Fatalf("unexpected control-plane restore plan: %+v", planResponse.Plan)
	}
	if planResponse.Plan.Mode != model.BackupRestoreModeOfflineControlPlane {
		t.Fatalf("expected offline control-plane restore mode, got %+v", planResponse.Plan)
	}
	if len(planResponse.Plan.Phases) == 0 || planResponse.Plan.Phases[0].Name != "download-artifact" {
		t.Fatalf("expected offline restore phases, got %+v", planResponse.Plan.Phases)
	}
}

func TestAdminBackupRunUsesDefaultControlPlanePolicy(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	_, err := stateStore.CreateBackupBackend(model.BackupBackend{
		ID:       stateStore.DefaultBackupBackendID(),
		Name:     "fugue-default-r2",
		Provider: model.DataBackendProviderCloudflareR2,
		Bucket:   "bucket",
		Endpoint: "https://example.r2.cloudflarestorage.com",
		Status:   "active",
	})
	if err != nil {
		t.Fatalf("create default backend: %v", err)
	}
	if err := stateStore.EnsureDefaultBackupPolicy(); err != nil {
		t.Fatalf("ensure default policy: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, "bootstrap-secret"), nil, ServerConfig{})
	seenRun := make(chan model.BackupRun, 1)
	server.backupRunner = func(ctx context.Context, run model.BackupRun) ([]model.BackupArtifact, error) {
		seenRun <- run
		return nil, nil
	}

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/backups/runs", "bootstrap-secret", map[string]any{
		"wait": true,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response struct {
		Run model.BackupRun `json:"run"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Run.Status != model.BackupRunStatusSucceeded {
		t.Fatalf("expected succeeded default backup run, got %+v", response.Run)
	}
	if response.Run.PolicyID != stateStore.DefaultControlPlaneBackupPolicyID() {
		t.Fatalf("expected default control-plane policy, got %+v", response.Run)
	}
	if response.Run.BackendID != stateStore.DefaultBackupBackendID() {
		t.Fatalf("expected default backup backend, got %+v", response.Run)
	}

	select {
	case run := <-seenRun:
		if run.PolicyID != stateStore.DefaultControlPlaneBackupPolicyID() || run.BackendID != stateStore.DefaultBackupBackendID() {
			t.Fatalf("expected runner to receive default policy/backend, got %+v", run)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for default backup run execution")
	}
}

func TestAppDatabaseBackupRunCreatesCloneRestorePlan(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Backup Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	runtime, _, err := stateStore.CreateRuntime(tenant.ID, "tenant-owned", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "postgres-app", "", model.AppSpec{
		Image:     "ghcr.io/example/app:latest",
		RuntimeID: runtime.ID,
		Replicas:  1,
		Postgres:  &model.AppPostgresSpec{Database: "appdb"},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	backend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		TenantID: tenant.ID,
		Name:     "tenant-r2",
		Provider: model.DataBackendProviderCloudflareR2,
		Bucket:   "tenant-bucket",
		Endpoint: "https://tenant.r2.cloudflarestorage.com",
	})
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}
	_, apiKey, err := stateStore.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.read", "backup.read", "backup.write", "backup.restore"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	artifactCh := make(chan model.BackupArtifact, 1)
	server.backupRunner = func(ctx context.Context, run model.BackupRun) ([]model.BackupArtifact, error) {
		artifact, err := stateStore.CreateBackupArtifact(model.BackupArtifact{
			RunID:             run.ID,
			PolicyID:          run.PolicyID,
			TenantID:          run.TenantID,
			ProjectID:         run.ProjectID,
			AppID:             run.AppID,
			Target:            run.Target,
			BackendID:         run.BackendID,
			Kind:              model.BackupArtifactKindAppPGDump,
			Version:           "before-migration",
			ObjectKey:         "apps/app/database.dump",
			ManifestObjectKey: "apps/app/manifest.json",
			SHA256:            strings.Repeat("b", 64),
			SizeBytes:         2048,
			LogicalBytes:      4096,
			Status:            model.BackupArtifactStatusActive,
		})
		if err != nil {
			return nil, err
		}
		artifactCh <- artifact
		return []model.BackupArtifact{artifact}, nil
	}

	policyRecorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/backups/policies", apiKey, map[string]any{
		"target":     map[string]any{"type": model.BackupTargetAppDatabase},
		"backend_id": backend.ID,
		"version":    "before-migration",
	})
	if policyRecorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, policyRecorder.Code, policyRecorder.Body.String())
	}
	var policyResponse struct {
		Policy model.BackupPolicy `json:"policy"`
	}
	mustDecodeJSON(t, policyRecorder, &policyResponse)
	if policyResponse.Policy.Status != model.BackupPolicyStatusActive || policyResponse.Policy.Target.Database != "appdb" {
		t.Fatalf("expected active resolved app database policy, got %+v", policyResponse.Policy)
	}

	runRecorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/backups/runs", apiKey, nil)
	if runRecorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, runRecorder.Code, runRecorder.Body.String())
	}
	var runResponse struct {
		Run model.BackupRun `json:"run"`
	}
	mustDecodeJSON(t, runRecorder, &runResponse)
	if runResponse.Run.AppID != app.ID || runResponse.Run.Target.Type != model.BackupTargetAppDatabase {
		t.Fatalf("unexpected app backup run target: %+v", runResponse.Run)
	}

	var artifact model.BackupArtifact
	select {
	case artifact = <-artifactCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for app database backup artifact")
	}
	finalRun, err := stateStore.GetBackupRun(runResponse.Run.ID, tenant.ID, false)
	if err != nil {
		t.Fatalf("get final app backup run: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for finalRun.Status != model.BackupRunStatusSucceeded && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		finalRun, err = stateStore.GetBackupRun(runResponse.Run.ID, tenant.ID, false)
		if err != nil {
			t.Fatalf("get final app backup run: %v", err)
		}
	}
	if finalRun.Status != model.BackupRunStatusSucceeded || finalRun.ArtifactCount != 1 {
		t.Fatalf("expected succeeded app backup run, got %+v", finalRun)
	}

	planRecorder := performJSONRequest(t, server, http.MethodPost, "/v1/backups/restore-plans", apiKey, map[string]any{
		"artifact_id": artifact.ID,
		"mode":        model.BackupRestoreModeClone,
	})
	if planRecorder.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusCreated, planRecorder.Code, planRecorder.Body.String())
	}
	var planResponse struct {
		Plan model.BackupRestorePlan `json:"plan"`
	}
	mustDecodeJSON(t, planRecorder, &planResponse)
	if planResponse.Plan.TenantID != tenant.ID || planResponse.Plan.AppID != app.ID {
		t.Fatalf("expected tenant-scoped app restore plan, got %+v", planResponse.Plan)
	}
	if planResponse.Plan.Mode != model.BackupRestoreModeClone || planResponse.Plan.Target.Type != model.BackupTargetAppDatabase {
		t.Fatalf("expected app database clone restore plan, got %+v", planResponse.Plan)
	}
	if len(planResponse.Plan.Phases) == 0 || planResponse.Plan.Phases[0].Name != "provision-clone" {
		t.Fatalf("expected clone restore phases, got %+v", planResponse.Plan.Phases)
	}
}

func TestPersistentStorageBackupWorkerArchivesMountedRoot(t *testing.T) {
	s3URL, stored := newBackupFakeS3(t)
	sourceRoot := t.TempDir()
	if err := os.MkdirAll(filepath.Join(sourceRoot, "data"), 0o755); err != nil {
		t.Fatalf("mkdir source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sourceRoot, "data", "hello.txt"), []byte("hello persistent storage\n"), 0o644); err != nil {
		t.Fatalf("write source file: %v", err)
	}
	t.Setenv("FUGUE_BACKUP_PERSISTENT_STORAGE_ROOT", sourceRoot)

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Storage Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	runtime, _, err := stateStore.CreateRuntime(tenant.ID, "tenant-owned", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "files-app", "", model.AppSpec{
		Image:     "ghcr.io/example/app:latest",
		RuntimeID: runtime.ID,
		Replicas:  1,
		PersistentStorage: &model.AppPersistentStorageSpec{
			Mode:             model.AppPersistentStorageModeMovableRWO,
			StorageClassName: "fugue-rwo",
			Mounts: []model.AppPersistentStorageMount{{
				Kind: model.AppPersistentStorageMountKindDirectory,
				Path: "/data",
			}},
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	backend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		TenantID: tenant.ID,
		Name:     "tenant-s3",
		Provider: model.DataBackendProviderS3,
		Bucket:   "bucket",
		Endpoint: s3URL,
		Region:   "us-east-1",
		Credentials: model.DataBackendCredentials{
			AccessKeyID:     "access",
			SecretAccessKey: "secret",
		},
	})
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}
	policy, err := stateStore.UpsertBackupPolicy(model.BackupPolicy{
		TenantID:  tenant.ID,
		ProjectID: project.ID,
		AppID:     app.ID,
		Name:      "persistent-storage",
		Target:    model.BackupTarget{Type: model.BackupTargetPersistentStorage, TenantID: tenant.ID, ProjectID: project.ID, AppID: app.ID},
		BackendID: backend.ID,
		Enabled:   true,
		Status:    model.BackupPolicyStatusActive,
	})
	if err != nil {
		t.Fatalf("create policy: %v", err)
	}
	run, err := stateStore.CreateBackupRun(model.BackupRun{
		PolicyID:  policy.ID,
		TenantID:  tenant.ID,
		ProjectID: project.ID,
		AppID:     app.ID,
		Target:    policy.Target,
		BackendID: backend.ID,
		Status:    model.BackupRunStatusPending,
	})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	artifacts, err := server.runPersistentStorageBackup(context.Background(), run)
	if err != nil {
		t.Fatalf("run persistent storage backup: %v", err)
	}
	if len(artifacts) != 1 {
		t.Fatalf("expected one artifact, got %+v", artifacts)
	}
	artifact := artifacts[0]
	if artifact.Kind != model.BackupArtifactKindFileArchive || artifact.Target.Type != model.BackupTargetPersistentStorage {
		t.Fatalf("unexpected persistent storage artifact: %+v", artifact)
	}
	if artifact.Manifest.Metadata["restore_target"] != "new-pvc" || artifact.Manifest.Metadata["cutover"] != "normal-deploy-operation" {
		t.Fatalf("expected storage restore metadata, got %+v", artifact.Manifest.Metadata)
	}
	if len(artifact.Manifest.Files) == 0 {
		t.Fatalf("expected manifest files, got %+v", artifact.Manifest)
	}
	if _, ok := stored[artifact.ObjectKey]; !ok {
		t.Fatalf("expected archive object %q in fake s3 keys=%v", artifact.ObjectKey, stored)
	}
	if _, ok := stored[artifact.ManifestObjectKey]; !ok {
		t.Fatalf("expected manifest object %q in fake s3 keys=%v", artifact.ManifestObjectKey, stored)
	}
}

func TestDataWorkspaceBackupWorkerCreatesSnapshotArtifact(t *testing.T) {
	t.Parallel()

	s3URL, stored := newBackupFakeS3(t)
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Data Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "data", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	workspace, err := stateStore.CreateDataWorkspace(model.DataWorkspace{
		TenantID:  tenant.ID,
		ProjectID: project.ID,
		Name:      "workspace",
	})
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	snapshot, err := stateStore.CreateDataSnapshot(model.DataSnapshot{
		WorkspaceID: workspace.ID,
		Version:     "dataset-v1",
		Manifest: model.DataManifest{Entries: []model.DataManifestEntry{{
			AssetName:    "data",
			RelativePath: "rows.jsonl",
			Kind:         model.DataManifestEntryKindFile,
			Size:         128,
			SHA256:       strings.Repeat("c", 64),
		}}},
	})
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	backend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		TenantID: tenant.ID,
		Name:     "tenant-s3",
		Provider: model.DataBackendProviderS3,
		Bucket:   "bucket",
		Endpoint: s3URL,
		Region:   "us-east-1",
		Credentials: model.DataBackendCredentials{
			AccessKeyID:     "access",
			SecretAccessKey: "secret",
		},
	})
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}
	run, err := stateStore.CreateBackupRun(model.BackupRun{
		TenantID:  tenant.ID,
		ProjectID: project.ID,
		Target:    model.BackupTarget{Type: model.BackupTargetDataWorkspace, TenantID: tenant.ID, ProjectID: project.ID, WorkspaceID: workspace.ID},
		BackendID: backend.ID,
		Status:    model.BackupRunStatusPending,
	})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	artifacts, err := server.runDataWorkspaceBackup(context.Background(), run)
	if err != nil {
		t.Fatalf("run data workspace backup: %v", err)
	}
	if len(artifacts) != 1 {
		t.Fatalf("expected one artifact, got %+v", artifacts)
	}
	artifact := artifacts[0]
	if artifact.Kind != model.BackupArtifactKindDataSnapshot || artifact.Target.WorkspaceID != workspace.ID {
		t.Fatalf("unexpected data workspace artifact: %+v", artifact)
	}
	if !artifact.Protected {
		t.Fatalf("expected data workspace snapshot artifact to be retention protected, got %+v", artifact)
	}
	if artifact.Manifest.Metadata["snapshot_id"] != snapshot.ID || artifact.Manifest.Metadata["manifest_digest"] != snapshot.ManifestDigest {
		t.Fatalf("expected snapshot metadata, got artifact=%+v snapshot=%+v", artifact, snapshot)
	}
	if _, ok := stored[artifact.ManifestObjectKey]; !ok {
		t.Fatalf("expected data workspace manifest object %q in fake s3 keys=%v", artifact.ManifestObjectKey, stored)
	}
}

func TestReplaceRestoreRunQueuesProtectiveBackup(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	backend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		Name:     "r2",
		Provider: model.DataBackendProviderCloudflareR2,
		Bucket:   "bucket",
		Endpoint: "https://example.r2.cloudflarestorage.com",
	})
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}
	artifact, err := stateStore.CreateBackupArtifact(model.BackupArtifact{
		Target:    model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase},
		BackendID: backend.ID,
		Kind:      model.BackupArtifactKindControlPlanePGDump,
		Status:    model.BackupArtifactStatusActive,
	})
	if err != nil {
		t.Fatalf("create artifact: %v", err)
	}
	plan, err := stateStore.CreateBackupRestorePlan(model.BackupRestorePlan{
		ArtifactID: artifact.ID,
		Mode:       model.BackupRestoreModeReplace,
	})
	if err != nil {
		t.Fatalf("create restore plan: %v", err)
	}
	server := NewServer(stateStore, auth.New(stateStore, "bootstrap-secret"), nil, ServerConfig{})
	protectiveCh := make(chan string, 1)
	server.backupRunner = func(ctx context.Context, run model.BackupRun) ([]model.BackupArtifact, error) {
		protectiveCh <- run.ID
		return []model.BackupArtifact{}, nil
	}

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/backups/restore-runs", "bootstrap-secret", map[string]any{
		"plan_id": plan.ID,
		"mode":    model.BackupRestoreModeReplace,
	})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}
	var response struct {
		Run model.BackupRestoreRun `json:"run"`
	}
	mustDecodeJSON(t, recorder, &response)
	if len(response.Run.Phases) == 0 || !strings.Contains(response.Run.Phases[0].Message, "queued protective backup run") {
		t.Fatalf("expected protective backup phase, got %+v", response.Run.Phases)
	}
	runs, err := stateStore.ListBackupRuns(store.BackupRunFilter{PlatformAdmin: true})
	if err != nil {
		t.Fatalf("list backup runs: %v", err)
	}
	if len(runs) != 1 || runs[0].Trigger != "pre-restore-protective" {
		t.Fatalf("expected protective backup run, got %+v", runs)
	}
	select {
	case <-protectiveCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for protective backup execution")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		latest, err := stateStore.GetBackupRun(runs[0].ID, "", true)
		if err != nil {
			t.Fatalf("get protective run: %v", err)
		}
		if latest.Status == model.BackupRunStatusSucceeded {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for protective backup run to succeed")
}

func TestPlatformBackupPostureReportsCNPGAndExternalizedComponents(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	server := NewServer(stateStore, auth.New(stateStore, "bootstrap-secret"), nil, ServerConfig{
		ControlPlaneCNPGBackupEnabled: true,
		ControlPlaneCNPGBackupName:    "fugue-postgres-backup",
		RegistryPushBase:              "registry.example.com",
		RegistryPullBase:              "registry.example.com",
		ClusterJoinRegistryEndpoint:   "registry.example.com",
		ClusterJoinMeshProvider:       "tailscale",
		ClusterJoinMeshLoginServer:    "https://mesh.example.com",
	})
	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/admin/backups/status", "bootstrap-secret", nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response struct {
		Posture []model.BackupPosture `json:"posture"`
	}
	mustDecodeJSON(t, recorder, &response)
	if !backupPostureHas(response.Posture, model.BackupTargetControlPlaneDatabase, "", func(posture model.BackupPosture) bool {
		return posture.CNPGBackupIntegrated
	}) {
		t.Fatalf("expected CNPG-integrated control-plane posture, got %+v", response.Posture)
	}
	if !backupPostureHas(response.Posture, model.BackupTargetRegistry, "registry", func(posture model.BackupPosture) bool {
		return posture.Externalized && posture.ExternallyBackedUp
	}) {
		t.Fatalf("expected externalized registry posture, got %+v", response.Posture)
	}
	if !backupPostureHas(response.Posture, model.BackupTargetPlatformComponent, "headscale", func(posture model.BackupPosture) bool {
		return posture.Externalized && posture.ExternallyBackedUp
	}) {
		t.Fatalf("expected externalized headscale posture, got %+v", response.Posture)
	}
}

func TestBackupMetricsExposePolicyAndBillableStorage(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	if _, err := stateStore.CreateBackupArtifact(model.BackupArtifact{
		TenantID:  "tenant_a",
		Target:    model.BackupTarget{Type: model.BackupTargetAppDatabase, TenantID: "tenant_a", AppID: "app_a"},
		Kind:      model.BackupArtifactKindAppPGDump,
		SizeBytes: 123,
		Status:    model.BackupArtifactStatusActive,
		Billable:  true,
	}); err != nil {
		t.Fatalf("create artifact: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	recorder := httptest.NewRecorder()
	server.MetricsHandler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recorder.Body.String()
	for _, want := range []string{
		"fugue_backup_policies",
		`target_type="control-plane-db"`,
		"fugue_backup_artifact_bytes",
		"fugue_backup_billable_bytes",
		"123.000000",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("expected metrics to contain %q, got:\n%s", want, body)
		}
	}
}

func TestScheduleBackupRetryCreatesPendingRetryRun(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	backend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		Name:     "r2",
		Provider: model.DataBackendProviderCloudflareR2,
		Bucket:   "bucket",
		Endpoint: "https://example.r2.cloudflarestorage.com",
	})
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}
	policy, err := stateStore.UpsertBackupPolicy(model.BackupPolicy{
		Name:      "control-plane",
		Target:    model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase},
		BackendID: backend.ID,
		Enabled:   true,
		Status:    model.BackupPolicyStatusActive,
		Schedule:  model.BackupDefaultSchedule,
	})
	if err != nil {
		t.Fatalf("create policy: %v", err)
	}
	failedRun, err := stateStore.CreateBackupRun(model.BackupRun{
		PolicyID:   policy.ID,
		Target:     policy.Target,
		BackendID:  backend.ID,
		Trigger:    model.BackupRunTriggerScheduled,
		Status:     model.BackupRunStatusFailed,
		RetryCount: 0,
		Attempt:    1,
	})
	if err != nil {
		t.Fatalf("create failed run: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	before := time.Now().UTC()
	server.scheduleBackupRetry(ctx, failedRun)

	runs, err := stateStore.ListBackupRuns(store.BackupRunFilter{Status: model.BackupRunStatusPending, PlatformAdmin: true})
	if err != nil {
		t.Fatalf("list pending runs: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("expected one retry run, got %+v", runs)
	}
	retry := runs[0]
	if retry.Trigger != model.BackupRunTriggerRetry || retry.RetryCount != 1 || retry.Attempt != 2 {
		t.Fatalf("unexpected retry run: %+v", retry)
	}
	if retry.NextRetryAt == nil || retry.NextRetryAt.Before(before.Add(4*time.Minute)) || retry.NextRetryAt.After(before.Add(6*time.Minute)) {
		t.Fatalf("expected retry next run about five minutes out, got %+v", retry.NextRetryAt)
	}
}

func TestRecoverStaleBackupRunMarksFailedAndSchedulesRetry(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	backend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		Name:     "r2",
		Provider: model.DataBackendProviderCloudflareR2,
		Bucket:   "bucket",
		Endpoint: "https://example.r2.cloudflarestorage.com",
	})
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}
	policy, err := stateStore.UpsertBackupPolicy(model.BackupPolicy{
		Name:      "control-plane",
		Target:    model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase},
		BackendID: backend.ID,
		Enabled:   true,
		Status:    model.BackupPolicyStatusActive,
		Schedule:  model.BackupDefaultSchedule,
	})
	if err != nil {
		t.Fatalf("create policy: %v", err)
	}
	staleHeartbeat := time.Now().UTC().Add(-10 * time.Minute)
	staleLock := time.Now().UTC().Add(-8 * time.Minute)
	run, err := stateStore.CreateBackupRun(model.BackupRun{
		PolicyID:    policy.ID,
		Target:      policy.Target,
		BackendID:   backend.ID,
		Trigger:     model.BackupRunTriggerScheduled,
		Status:      model.BackupRunStatusRunning,
		Attempt:     1,
		RetryCount:  0,
		LockedUntil: &staleLock,
		HeartbeatAt: &staleHeartbeat,
	})
	if err != nil {
		t.Fatalf("create stale run: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	server.recoverStaleBackupRuns(context.Background())

	recovered, err := stateStore.GetBackupRun(run.ID, "", true)
	if err != nil {
		t.Fatalf("get recovered run: %v", err)
	}
	if recovered.Status != model.BackupRunStatusFailed {
		t.Fatalf("expected stale run to fail, got %+v", recovered)
	}
	if recovered.ErrorCode != "backup_run_lost" || recovered.FinishedAt == nil {
		t.Fatalf("expected stale run recovery metadata, got %+v", recovered)
	}

	runs, err := stateStore.ListBackupRuns(store.BackupRunFilter{Status: model.BackupRunStatusPending, PlatformAdmin: true})
	if err != nil {
		t.Fatalf("list pending runs: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("expected one retry run, got %+v", runs)
	}
	retry := runs[0]
	if retry.Trigger != model.BackupRunTriggerRetry || retry.Attempt != 2 || retry.RetryCount != 1 {
		t.Fatalf("unexpected retry run: %+v", retry)
	}
	if retry.NextRetryAt == nil {
		t.Fatalf("expected retry next_retry_at, got %+v", retry)
	}
}

func newBackupFakeS3(t *testing.T) (string, map[string][]byte) {
	t.Helper()
	var mu sync.Mutex
	objects := map[string][]byte{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/")
		key = strings.TrimPrefix(key, "bucket/")
		switch r.Method {
		case http.MethodPut:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read fake s3 put body: %v", err)
			}
			mu.Lock()
			objects[key] = body
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			mu.Lock()
			body, ok := objects[key]
			mu.Unlock()
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			_, _ = w.Write(body)
		default:
			t.Fatalf("unexpected fake s3 request %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(server.Close)
	return server.URL, objects
}

func backupPostureHas(postures []model.BackupPosture, targetType, component string, match func(model.BackupPosture) bool) bool {
	for _, posture := range postures {
		if posture.Target.Type != targetType {
			continue
		}
		if component != "" && posture.Target.Component != component {
			continue
		}
		if match(posture) {
			return true
		}
	}
	return false
}
