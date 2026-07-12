package store

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestDefaultBackupPolicyIsControlPlaneHourlyRetainThree(t *testing.T) {
	clearDefaultDataBackendEnv(t)

	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	policy, err := stateStore.GetBackupPolicy(stateStore.DefaultControlPlaneBackupPolicyID(), "", true)
	if err != nil {
		t.Fatalf("get default backup policy: %v", err)
	}
	if policy.Target.Type != model.BackupTargetControlPlaneDatabase {
		t.Fatalf("expected control-plane DB target, got %+v", policy.Target)
	}
	if policy.Scope != model.BackupScopePlatform {
		t.Fatalf("expected platform scope, got %q", policy.Scope)
	}
	if policy.Schedule != model.BackupDefaultSchedule {
		t.Fatalf("expected default hourly schedule %q, got %q", model.BackupDefaultSchedule, policy.Schedule)
	}
	if policy.RetainCount != model.BackupDefaultRetainCount || policy.Retention.RetainCount != model.BackupDefaultRetainCount {
		t.Fatalf("expected retain count 3, got retain_count=%d retention=%+v", policy.RetainCount, policy.Retention)
	}
	if !policy.Enabled {
		t.Fatal("expected default control-plane backup policy to be enabled")
	}
	if policy.Status != model.BackupPolicyStatusBlockedNoBackend {
		t.Fatalf("expected missing backend to block policy, got %q", policy.Status)
	}
	if policy.BackendID != "" {
		t.Fatalf("expected no backend without R2 env, got %q", policy.BackendID)
	}
}

func TestDefaultBackupPolicyUsesConfiguredPlatformR2Backend(t *testing.T) {
	t.Setenv("FUGUE_DATA_BACKEND_PROVIDER", model.DataBackendProviderCloudflareR2)
	t.Setenv("FUGUE_DATA_R2_ACCOUNT_ID", "acct123")
	t.Setenv("FUGUE_DATA_BACKEND_BUCKET", "fugue-backups")
	t.Setenv("FUGUE_DATA_BACKEND_PREFIX", "prod")
	t.Setenv("FUGUE_DATA_BACKEND_ACCESS_KEY_ID", "access-key")
	t.Setenv("FUGUE_DATA_BACKEND_SECRET_ACCESS_KEY", "secret-key")
	t.Setenv("FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY", "test-encryption-key")

	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	backend, err := stateStore.GetBackupBackend(stateStore.DefaultBackupBackendID(), "", true)
	if err != nil {
		t.Fatalf("get default backup backend: %v", err)
	}
	if backend.Provider != model.DataBackendProviderCloudflareR2 {
		t.Fatalf("expected R2 backend, got %+v", backend)
	}
	if backend.Endpoint != "https://acct123.r2.cloudflarestorage.com" {
		t.Fatalf("unexpected R2 endpoint %q", backend.Endpoint)
	}
	if backend.Prefix != "prod/backups" {
		t.Fatalf("expected backup prefix to be nested under data prefix, got %q", backend.Prefix)
	}
	if !backend.FugueManaged || !backend.Billable {
		t.Fatalf("expected default R2 backup backend to be Fugue-managed and billable, got %+v", backend)
	}
	if backend.Credentials.AccessKeyID != "access-key" || backend.Credentials.SecretAccessKey != "" {
		t.Fatalf("expected redacted backend credentials, got %+v", backend.Credentials)
	}

	forUse, err := stateStore.GetBackupBackendForUse(stateStore.DefaultBackupBackendID(), "", true)
	if err != nil {
		t.Fatalf("get default backup backend for use: %v", err)
	}
	if forUse.Credentials.AccessKeyID != "access-key" || forUse.Credentials.SecretAccessKey != "secret-key" {
		t.Fatalf("expected unredacted credentials for backend use, got %+v", forUse.Credentials)
	}

	policy, err := stateStore.GetBackupPolicy(stateStore.DefaultControlPlaneBackupPolicyID(), "", true)
	if err != nil {
		t.Fatalf("get default backup policy: %v", err)
	}
	if policy.BackendID != backend.ID {
		t.Fatalf("expected default policy to use R2 backend %q, got %q", backend.ID, policy.BackendID)
	}
	if policy.Status != model.BackupPolicyStatusActive {
		t.Fatalf("expected default policy to be active with R2 backend, got %q", policy.Status)
	}
}

func TestUserAppBackupPolicyAbsentByDefault(t *testing.T) {
	clearDefaultDataBackendEnv(t)

	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	policies, err := stateStore.ListBackupPolicies(BackupPolicyFilter{
		AppID:           "app_test",
		TargetType:      model.BackupTargetAppDatabase,
		IncludeDisabled: true,
		PlatformAdmin:   true,
	})
	if err != nil {
		t.Fatalf("list app backup policies: %v", err)
	}
	if len(policies) != 0 {
		t.Fatalf("expected no app database backup policy by default, got %+v", policies)
	}
}

func TestBackupRetentionKeepsLatestSuccessfulArtifacts(t *testing.T) {
	clearDefaultDataBackendEnv(t)

	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
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
		Name:        "retain-two",
		Target:      model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase},
		BackendID:   backend.ID,
		Enabled:     true,
		Status:      model.BackupPolicyStatusActive,
		Schedule:    model.BackupDefaultSchedule,
		RetainCount: 2,
		Retention:   model.BackupRetentionPolicy{RetainCount: 2, ProtectLatest: 2},
	})
	if err != nil {
		t.Fatalf("create policy: %v", err)
	}
	base := time.Date(2026, 6, 13, 10, 0, 0, 0, time.UTC)
	var ids []string
	for i := 0; i < 4; i++ {
		artifact, err := stateStore.CreateBackupArtifact(model.BackupArtifact{
			RunID:     "run_" + string(rune('a'+i)),
			PolicyID:  policy.ID,
			Target:    policy.Target,
			BackendID: backend.ID,
			Kind:      model.BackupArtifactKindControlPlanePGDump,
			ObjectKey: "dump-" + string(rune('a'+i)),
			SizeBytes: int64(100 + i),
			Status:    model.BackupArtifactStatusActive,
			CreatedAt: base.Add(time.Duration(i) * time.Minute),
		})
		if err != nil {
			t.Fatalf("create artifact %d: %v", i, err)
		}
		ids = append(ids, artifact.ID)
	}

	active, err := stateStore.ListBackupArtifacts(BackupArtifactFilter{PolicyID: policy.ID, ActiveOnly: true, PlatformAdmin: true})
	if err != nil {
		t.Fatalf("list active artifacts: %v", err)
	}
	if len(active) != 2 {
		t.Fatalf("expected 2 active artifacts, got %d: %+v", len(active), active)
	}
	if active[0].ObjectKey != "dump-d" || active[1].ObjectKey != "dump-c" {
		t.Fatalf("expected latest artifacts to remain active, got %+v", active)
	}
	for _, id := range ids[:2] {
		artifact, err := stateStore.GetBackupArtifact(id, "", true)
		if err != nil {
			t.Fatalf("get expired artifact %s: %v", id, err)
		}
		if artifact.Status != model.BackupArtifactStatusExpired {
			t.Fatalf("expected artifact %s to expire, got %q", id, artifact.Status)
		}
	}
}

func TestBackupScheduleDueCalculation(t *testing.T) {
	after := time.Date(2026, 6, 13, 10, 17, 30, 0, time.UTC)
	tests := []struct {
		name     string
		schedule string
		want     time.Time
	}{
		{name: "default hourly", schedule: model.BackupDefaultSchedule, want: time.Date(2026, 6, 13, 11, 0, 0, 0, time.UTC)},
		{name: "hourly shortcut", schedule: "@hourly", want: time.Date(2026, 6, 13, 11, 0, 0, 0, time.UTC)},
		{name: "every fifteen", schedule: "*/15 * * * *", want: time.Date(2026, 6, 13, 10, 30, 0, 0, time.UTC)},
		{name: "fixed minute", schedule: "20 * * * *", want: time.Date(2026, 6, 13, 10, 20, 0, 0, time.UTC)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := nextBackupRunAfter(tt.schedule, after)
			if got == nil {
				t.Fatalf("expected next run for %q", tt.schedule)
			}
			if !got.Equal(tt.want) {
				t.Fatalf("expected %s, got %s", tt.want, *got)
			}
		})
	}
	if got := nextBackupRunAfter("bad schedule", after); got != nil {
		t.Fatalf("expected unsupported schedule to return nil, got %s", *got)
	}
}

func TestBackupUsageCountsBillableR2BytesWithMarkup(t *testing.T) {
	clearDefaultDataBackendEnv(t)

	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	for _, artifact := range []model.BackupArtifact{
		{
			TenantID:     "tenant_a",
			Target:       model.BackupTarget{Type: model.BackupTargetAppDatabase, TenantID: "tenant_a", AppID: "app_a"},
			Kind:         model.BackupArtifactKindAppPGDump,
			SizeBytes:    100,
			Status:       model.BackupArtifactStatusActive,
			Billable:     true,
			BillingClass: "r2-standard",
		},
		{
			TenantID:     "tenant_a",
			Target:       model.BackupTarget{Type: model.BackupTargetAppDatabase, TenantID: "tenant_a", AppID: "app_a"},
			Kind:         model.BackupArtifactKindAppPGDump,
			SizeBytes:    50,
			Status:       model.BackupArtifactStatusDeleted,
			Billable:     true,
			BillingClass: "r2-standard",
		},
		{
			TenantID:  "tenant_b",
			Target:    model.BackupTarget{Type: model.BackupTargetAppDatabase, TenantID: "tenant_b", AppID: "app_b"},
			Kind:      model.BackupArtifactKindAppPGDump,
			SizeBytes: 75,
			Status:    model.BackupArtifactStatusActive,
			Billable:  true,
		},
		{
			TenantID:  "tenant_a",
			Target:    model.BackupTarget{Type: model.BackupTargetAppDatabase, TenantID: "tenant_a", AppID: "app_a"},
			Kind:      model.BackupArtifactKindAppPGDump,
			SizeBytes: 25,
			Status:    model.BackupArtifactStatusActive,
			Billable:  false,
		},
	} {
		if _, err := stateStore.CreateBackupArtifact(artifact); err != nil {
			t.Fatalf("create usage artifact: %v", err)
		}
	}

	tenantUsage, err := stateStore.BackupUsage("tenant_a", false)
	if err != nil {
		t.Fatalf("tenant usage: %v", err)
	}
	if tenantUsage.BillableBytes != 100 {
		t.Fatalf("expected tenant billable bytes 100, got %d", tenantUsage.BillableBytes)
	}
	if tenantUsage.MarkupPercent != model.BackupR2MarkupPercent || tenantUsage.EffectiveMultiplier != 1.05 {
		t.Fatalf("expected 5%% markup and 1.05 multiplier, got %+v", tenantUsage)
	}

	platformUsage, err := stateStore.BackupUsage("", true)
	if err != nil {
		t.Fatalf("platform usage: %v", err)
	}
	if platformUsage.BillableBytes != 175 {
		t.Fatalf("expected platform billable bytes 175, got %d", platformUsage.BillableBytes)
	}
}

func TestBackupArtifactManifestIsSelfDescribing(t *testing.T) {
	clearDefaultDataBackendEnv(t)

	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	target := model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase, Component: "control-plane-postgres"}
	artifact, err := stateStore.CreateBackupArtifact(model.BackupArtifact{
		RunID:             "backup_run_123",
		PolicyID:          stateStore.DefaultControlPlaneBackupPolicyID(),
		Target:            target,
		Kind:              model.BackupArtifactKindControlPlanePGDump,
		Version:           "before-upgrade",
		ObjectKey:         "backups/control-plane/run/control-plane.dump",
		ManifestObjectKey: "backups/control-plane/run/manifest.json",
		SHA256:            "0123456789abcdef",
		SizeBytes:         1024,
		LogicalBytes:      2048,
		Status:            model.BackupArtifactStatusActive,
	})
	if err != nil {
		t.Fatalf("create artifact: %v", err)
	}
	if artifact.ManifestDigest == "" {
		t.Fatalf("expected manifest digest, got %+v", artifact)
	}
	if artifact.Manifest.SchemaVersion != "fugue.backup/v1" {
		t.Fatalf("expected manifest schema version, got %+v", artifact.Manifest)
	}
	if artifact.Manifest.RunID != artifact.RunID || artifact.Manifest.PolicyID != artifact.PolicyID {
		t.Fatalf("expected manifest to reference run and policy, got %+v", artifact.Manifest)
	}
	if artifact.Manifest.Target.Type != target.Type || artifact.Manifest.ObjectKey != artifact.ObjectKey || artifact.Manifest.SHA256 != artifact.SHA256 {
		t.Fatalf("expected self-describing manifest, got %+v", artifact.Manifest)
	}
}

func TestBackupBackendValidationRejectsInvalidProvider(t *testing.T) {
	clearDefaultDataBackendEnv(t)

	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	if _, err := stateStore.CreateBackupBackend(model.BackupBackend{Name: "bad", Provider: "nfs"}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected invalid provider error, got %v", err)
	}
	if _, err := stateStore.CreateBackupBackend(model.BackupBackend{Name: "missing-bucket", Provider: model.DataBackendProviderCloudflareR2}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected missing bucket error, got %v", err)
	}
}

func TestBackupListWithoutTargetTypeIncludesAppScopedItems(t *testing.T) {
	clearDefaultDataBackendEnv(t)

	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
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
	target := model.BackupTarget{Type: model.BackupTargetAppDatabase, TenantID: "tenant_a", ProjectID: "project_a", AppID: "app_a", Database: "appdb"}
	policy, err := stateStore.UpsertBackupPolicy(model.BackupPolicy{
		TenantID:  "tenant_a",
		ProjectID: "project_a",
		AppID:     "app_a",
		Name:      "app-db",
		Target:    target,
		BackendID: backend.ID,
		Enabled:   true,
		Status:    model.BackupPolicyStatusActive,
		Schedule:  model.BackupDefaultSchedule,
	})
	if err != nil {
		t.Fatalf("create policy: %v", err)
	}
	run, err := stateStore.CreateBackupRun(model.BackupRun{
		PolicyID:  policy.ID,
		TenantID:  "tenant_a",
		ProjectID: "project_a",
		AppID:     "app_a",
		Target:    target,
		BackendID: backend.ID,
		Trigger:   model.BackupRunTriggerManual,
		Status:    model.BackupRunStatusFailed,
	})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	artifact, err := stateStore.CreateBackupArtifact(model.BackupArtifact{
		RunID:     run.ID,
		PolicyID:  policy.ID,
		TenantID:  "tenant_a",
		ProjectID: "project_a",
		AppID:     "app_a",
		Target:    target,
		BackendID: backend.ID,
		Kind:      model.BackupArtifactKindAppPGDump,
		ObjectKey: "app.dump",
		Status:    model.BackupArtifactStatusActive,
	})
	if err != nil {
		t.Fatalf("create artifact: %v", err)
	}

	policies, err := stateStore.ListBackupPolicies(BackupPolicyFilter{AppID: "app_a", PlatformAdmin: true})
	if err != nil {
		t.Fatalf("list policies: %v", err)
	}
	if len(policies) != 1 || policies[0].ID != policy.ID {
		t.Fatalf("expected app policy without target type filter, got %+v", policies)
	}
	runs, err := stateStore.ListBackupRuns(BackupRunFilter{AppID: "app_a", PlatformAdmin: true})
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if len(runs) != 1 || runs[0].ID != run.ID {
		t.Fatalf("expected app run without target type filter, got %+v", runs)
	}
	artifacts, err := stateStore.ListBackupArtifacts(BackupArtifactFilter{AppID: "app_a", PlatformAdmin: true})
	if err != nil {
		t.Fatalf("list artifacts: %v", err)
	}
	if len(artifacts) != 1 || artifacts[0].ID != artifact.ID {
		t.Fatalf("expected app artifact without target type filter, got %+v", artifacts)
	}
}
