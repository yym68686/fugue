package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
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
