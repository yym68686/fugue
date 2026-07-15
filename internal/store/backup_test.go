package store

import (
	"errors"
	"fmt"
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

func TestDefaultBackupPolicyDisableSurvivesBackendSeedAndRestart(t *testing.T) {
	t.Setenv("FUGUE_DATA_BACKEND_PROVIDER", model.DataBackendProviderCloudflareR2)
	t.Setenv("FUGUE_DATA_R2_ACCOUNT_ID", "acct123")
	t.Setenv("FUGUE_DATA_BACKEND_BUCKET", "fugue-backups")
	t.Setenv("FUGUE_DATA_BACKEND_PREFIX", "prod")
	t.Setenv("FUGUE_DATA_BACKEND_ACCESS_KEY_ID", "access-key")
	t.Setenv("FUGUE_DATA_BACKEND_SECRET_ACCESS_KEY", "secret-key")
	t.Setenv("FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY", "test-encryption-key")

	storePath := filepath.Join(t.TempDir(), "store.json")
	stateStore := New(storePath)
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	const reason = "planned maintenance"
	disabled, err := stateStore.SetBackupPolicyEnabled(stateStore.DefaultControlPlaneBackupPolicyID(), "", true, false, reason)
	if err != nil {
		t.Fatalf("disable default backup policy: %v", err)
	}
	assertDisabled := func(stage string, policy model.BackupPolicy) {
		t.Helper()
		if policy.Enabled || policy.Status != model.BackupPolicyStatusDisabled || policy.NextRunAt != nil {
			t.Fatalf("expected default policy to remain disabled after %s, got %+v", stage, policy)
		}
		if policy.DisabledReason != reason {
			t.Fatalf("expected disable reason %q after %s, got %q", reason, stage, policy.DisabledReason)
		}
	}
	assertDisabled("disable", disabled)

	if err := stateStore.SeedDefaultBackupBackendFromEnv(); err != nil {
		t.Fatalf("reseed default backup backend: %v", err)
	}
	afterSeed, err := stateStore.GetBackupPolicy(stateStore.DefaultControlPlaneBackupPolicyID(), "", true)
	if err != nil {
		t.Fatalf("get default backup policy after backend seed: %v", err)
	}
	assertDisabled("backend seed", afterSeed)

	restarted := New(storePath)
	if err := restarted.Init(); err != nil {
		t.Fatalf("restart store: %v", err)
	}
	afterRestart, err := restarted.GetBackupPolicy(restarted.DefaultControlPlaneBackupPolicyID(), "", true)
	if err != nil {
		t.Fatalf("get default backup policy after restart: %v", err)
	}
	assertDisabled("restart", afterRestart)

	due, err := restarted.ListDueBackupPolicies(time.Now().UTC().Add(24*time.Hour), 10)
	if err != nil {
		t.Fatalf("list due backup policies after restart: %v", err)
	}
	for _, policy := range due {
		if policy.ID == restarted.DefaultControlPlaneBackupPolicyID() {
			t.Fatalf("disabled default policy became schedulable after restart: %+v", policy)
		}
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
		run, err := stateStore.CreateBackupRun(model.BackupRun{
			PolicyID:  policy.ID,
			Target:    policy.Target,
			BackendID: backend.ID,
			Trigger:   model.BackupRunTriggerManual,
			Status:    model.BackupRunStatusPending,
		})
		if err != nil {
			t.Fatalf("create backup run %d: %v", i, err)
		}
		claimedAt := time.Now().UTC()
		claimed, err := stateStore.ClaimBackupRun(run.ID, "retention-worker", claimedAt, 2*time.Minute)
		if err != nil {
			t.Fatalf("claim backup run %d: %v", i, err)
		}
		artifact, err := stateStore.CreateBackupArtifactForRun(model.BackupArtifact{
			RunID:     claimed.ID,
			PolicyID:  policy.ID,
			Target:    policy.Target,
			BackendID: backend.ID,
			Kind:      model.BackupArtifactKindControlPlanePGDump,
			ObjectKey: "dump-" + string(rune('a'+i)),
			SizeBytes: int64(100 + i),
			Status:    model.BackupArtifactStatusActive,
			CreatedAt: base.Add(time.Duration(i) * time.Minute),
		}, claimed.LeaseOwner)
		if err != nil {
			t.Fatalf("create artifact %d: %v", i, err)
		}
		ids = append(ids, artifact.ID)
		if _, err := stateStore.FinishBackupRun(claimed.ID, claimed.LeaseOwner, BackupRunFinish{
			Status:        model.BackupRunStatusSucceeded,
			BytesWritten:  artifact.SizeBytes,
			ArtifactCount: 1,
			FinishedAt:    claimedAt.Add(time.Second),
		}); err != nil {
			t.Fatalf("finish backup run %d: %v", i, err)
		}
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

func TestMarkBackupArtifactDeletedEnforcesTenantOwnership(t *testing.T) {
	t.Parallel()

	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	createArtifact := func(id, tenantID string) model.BackupArtifact {
		t.Helper()
		artifact, err := stateStore.CreateBackupArtifact(model.BackupArtifact{
			ID:        id,
			TenantID:  tenantID,
			Target:    model.BackupTarget{Type: model.BackupTargetAppDatabase, TenantID: tenantID},
			Kind:      model.BackupArtifactKindAppPGDump,
			ObjectKey: id + ".dump",
			Status:    model.BackupArtifactStatusActive,
		})
		if err != nil {
			t.Fatalf("create artifact %q: %v", id, err)
		}
		return artifact
	}
	attackerArtifact := createArtifact("artifact_attacker", "tenant_attacker")
	victimArtifact := createArtifact("artifact_victim", "tenant_victim")
	platformArtifact := createArtifact("artifact_platform", "")

	for _, artifact := range []model.BackupArtifact{victimArtifact, platformArtifact} {
		if _, err := stateStore.MarkBackupArtifactDeleted(artifact.ID, "tenant_attacker", false); !errors.Is(err, ErrNotFound) {
			t.Fatalf("expected tenant deletion of artifact %q to be hidden, got %v", artifact.ID, err)
		}
		unchanged, err := stateStore.GetBackupArtifact(artifact.ID, "", true)
		if err != nil {
			t.Fatalf("get artifact %q after rejected deletion: %v", artifact.ID, err)
		}
		if unchanged.Status != model.BackupArtifactStatusActive || unchanged.DeletedAt != nil {
			t.Fatalf("artifact %q changed after rejected deletion: %+v", artifact.ID, unchanged)
		}
	}
	deleted, err := stateStore.MarkBackupArtifactDeleted(attackerArtifact.ID, "tenant_attacker", false)
	if err != nil {
		t.Fatalf("delete tenant-owned artifact: %v", err)
	}
	if deleted.Status != model.BackupArtifactStatusDeleted || deleted.DeletedAt == nil {
		t.Fatalf("expected tenant-owned artifact to be deleted, got %+v", deleted)
	}
	if _, err := stateStore.MarkBackupArtifactDeleted(victimArtifact.ID, "", true); err != nil {
		t.Fatalf("platform admin delete victim artifact: %v", err)
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
		{name: "every six hours", schedule: "0 */6 * * *", want: time.Date(2026, 6, 13, 12, 0, 0, 0, time.UTC)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := nextBackupRunAfter(tt.schedule, after)
			if err != nil {
				t.Fatalf("expected next run for %q: %v", tt.schedule, err)
			}
			if !got.Equal(tt.want) {
				t.Fatalf("expected %s, got %s", tt.want, *got)
			}
		})
	}
	if got, err := nextBackupRunAfter("bad schedule", after); err == nil || got != nil {
		t.Fatalf("expected invalid schedule to fail closed, got %v err=%v", got, err)
	}
}

func TestUpsertBackupPolicyValidatesAndRecalculatesSchedule(t *testing.T) {
	t.Parallel()

	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	policy, err := stateStore.UpsertBackupPolicy(model.BackupPolicy{
		Name:     "six-hour",
		Target:   model.BackupTarget{Type: model.BackupTargetAppDatabase, AppID: "app_a"},
		Enabled:  true,
		Status:   model.BackupPolicyStatusActive,
		Schedule: "0 */6 * * *",
	})
	if err != nil {
		t.Fatalf("upsert six-hour policy: %v", err)
	}
	if policy.NextRunAt == nil || policy.NextRunAt.Minute() != 0 || policy.NextRunAt.Hour()%6 != 0 {
		t.Fatalf("expected aligned six-hour next run, got %+v", policy)
	}
	originalNext := *policy.NextRunAt
	policy.Schedule = "7 * * * *"
	updated, err := stateStore.UpsertBackupPolicy(policy)
	if err != nil {
		t.Fatalf("change policy schedule: %v", err)
	}
	if updated.NextRunAt == nil || updated.NextRunAt.Equal(originalNext) || updated.NextRunAt.Minute() != 7 {
		t.Fatalf("expected schedule change to recalculate next run, old=%s updated=%+v", originalNext, updated)
	}

	if _, err := stateStore.UpsertBackupPolicy(model.BackupPolicy{
		Name:     "invalid",
		Target:   model.BackupTarget{Type: model.BackupTargetAppDatabase, AppID: "app_b"},
		Enabled:  true,
		Status:   model.BackupPolicyStatusActive,
		Schedule: "not cron",
	}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected invalid schedule to return ErrInvalidInput, got %v", err)
	}

	defaulted, err := stateStore.UpsertBackupPolicy(model.BackupPolicy{
		Name:    "defaulted",
		Target:  model.BackupTarget{Type: model.BackupTargetAppDatabase, AppID: "app_c"},
		Enabled: true,
		Status:  model.BackupPolicyStatusActive,
	})
	if err != nil {
		t.Fatalf("upsert defaulted schedule: %v", err)
	}
	if defaulted.Schedule != model.BackupDefaultSchedule || defaulted.NextRunAt == nil {
		t.Fatalf("expected missing schedule to default safely, got %+v", defaulted)
	}

	invalidDisabled, err := stateStore.UpsertBackupPolicy(model.BackupPolicy{
		Name:     "invalid-disabled",
		Target:   model.BackupTarget{Type: model.BackupTargetAppDatabase, AppID: "app_d"},
		Enabled:  false,
		Status:   model.BackupPolicyStatusDisabled,
		Schedule: "not cron",
	})
	if err != nil {
		t.Fatalf("disabled invalid policy must remain operable: %v", err)
	}
	if _, err := stateStore.SetBackupPolicyEnabled(invalidDisabled.ID, "", true, true, ""); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected enabling invalid policy to fail validation, got %v", err)
	}
	if _, err := stateStore.SetBackupPolicyEnabled(invalidDisabled.ID, "", true, false, "disabled by user"); err != nil {
		t.Fatalf("expected invalid policy kill-switch to remain available: %v", err)
	}
}

func TestCreateScheduledBackupRunRechecksPolicyDueAndAllowsManualRun(t *testing.T) {
	t.Parallel()

	stateStore, backend, policy := newBackupClaimTestStore(t)
	if policy.NextRunAt == nil || !policy.NextRunAt.After(time.Now().UTC()) {
		t.Fatalf("expected newly created policy to be scheduled in the future, got %+v", policy)
	}
	if _, err := stateStore.CreateBackupRun(model.BackupRun{
		PolicyID:  policy.ID,
		Target:    policy.Target,
		BackendID: backend.ID,
		Trigger:   model.BackupRunTriggerScheduled,
		Status:    model.BackupRunStatusPending,
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected stale scheduled enqueue to fail when policy is not due, got %v", err)
	}

	manual, err := stateStore.CreateBackupRun(model.BackupRun{
		PolicyID:  policy.ID,
		Target:    policy.Target,
		BackendID: backend.ID,
		Trigger:   model.BackupRunTriggerManual,
		Status:    model.BackupRunStatusPending,
	})
	if err != nil {
		t.Fatalf("manual policy run must remain allowed before next_run_at: %v", err)
	}
	if manual.Trigger != model.BackupRunTriggerManual || manual.Status != model.BackupRunStatusPending {
		t.Fatalf("unexpected manual backup run: %+v", manual)
	}
}

func TestClaimBackupRunIsAtomicAndHonorsRetryDueTime(t *testing.T) {
	t.Parallel()

	t.Run("only one claimant transitions pending to running", func(t *testing.T) {
		stateStore, backend, policy := newBackupClaimTestStore(t)
		run, err := stateStore.CreateBackupRun(model.BackupRun{
			PolicyID:  policy.ID,
			Target:    policy.Target,
			BackendID: backend.ID,
			Trigger:   model.BackupRunTriggerManual,
			Status:    model.BackupRunStatusPending,
		})
		if err != nil {
			t.Fatalf("create pending backup run: %v", err)
		}
		now := time.Date(2026, time.July, 12, 12, 30, 0, 0, time.UTC)
		claimed, err := stateStore.ClaimBackupRun(run.ID, "worker-a", now, 2*time.Minute)
		if err != nil {
			t.Fatalf("claim pending backup run: %v", err)
		}
		wantLockedUntil := now.Add(2 * time.Minute)
		if claimed.Status != model.BackupRunStatusRunning || claimed.LeaseOwner != "worker-a" || claimed.LockedUntil == nil || !claimed.LockedUntil.Equal(wantLockedUntil) || claimed.StartedAt == nil || !claimed.StartedAt.Equal(now) {
			t.Fatalf("unexpected claimed run: %+v", claimed)
		}
		if _, err := stateStore.ClaimBackupRun(run.ID, "worker-b", now, 2*time.Minute); !errors.Is(err, ErrConflict) {
			t.Fatalf("expected second claimant to lose atomically, got %v", err)
		}
	})

	t.Run("future retry cannot be claimed early", func(t *testing.T) {
		stateStore, backend, policy := newBackupClaimTestStore(t)
		now := time.Date(2026, time.July, 12, 12, 30, 0, 0, time.UTC)
		nextRetryAt := now.Add(5 * time.Minute)
		run, err := stateStore.CreateBackupRun(model.BackupRun{
			PolicyID:    policy.ID,
			Target:      policy.Target,
			BackendID:   backend.ID,
			Trigger:     model.BackupRunTriggerRetry,
			Status:      model.BackupRunStatusPending,
			NextRetryAt: &nextRetryAt,
		})
		if err != nil {
			t.Fatalf("create pending retry run: %v", err)
		}
		if _, err := stateStore.ClaimBackupRun(run.ID, "worker-early", now, 2*time.Minute); !errors.Is(err, ErrConflict) {
			t.Fatalf("expected early retry claim to fail, got %v", err)
		}
		claimed, err := stateStore.ClaimBackupRun(run.ID, "worker-due", nextRetryAt, 2*time.Minute)
		if err != nil {
			t.Fatalf("claim due retry run: %v", err)
		}
		if claimed.Status != model.BackupRunStatusRunning || claimed.LeaseOwner != "worker-due" {
			t.Fatalf("unexpected due retry claim: %+v", claimed)
		}
	})

	t.Run("heartbeat and finish are fenced by owner", func(t *testing.T) {
		stateStore, backend, policy := newBackupClaimTestStore(t)
		run, err := stateStore.CreateBackupRun(model.BackupRun{
			PolicyID:  policy.ID,
			Target:    policy.Target,
			BackendID: backend.ID,
			Trigger:   model.BackupRunTriggerManual,
			Status:    model.BackupRunStatusPending,
		})
		if err != nil {
			t.Fatalf("create pending backup run: %v", err)
		}
		now := time.Date(2026, time.July, 12, 12, 30, 0, 0, time.UTC)
		if _, err := stateStore.ClaimBackupRun(run.ID, "worker-owner", now, 2*time.Minute); err != nil {
			t.Fatalf("claim backup run: %v", err)
		}
		if _, err := stateStore.HeartbeatBackupRun(run.ID, "worker-stale", now.Add(time.Minute), 2*time.Minute); !errors.Is(err, ErrConflict) {
			t.Fatalf("expected stale heartbeat to be fenced, got %v", err)
		}
		heartbeatAt := now.Add(time.Minute)
		heartbeat, err := stateStore.HeartbeatBackupRun(run.ID, "worker-owner", heartbeatAt, 2*time.Minute)
		if err != nil {
			t.Fatalf("heartbeat claimed backup run: %v", err)
		}
		if heartbeat.HeartbeatAt == nil || !heartbeat.HeartbeatAt.Equal(heartbeatAt) || heartbeat.LockedUntil == nil || !heartbeat.LockedUntil.Equal(heartbeatAt.Add(2*time.Minute)) {
			t.Fatalf("unexpected fenced heartbeat result: %+v", heartbeat)
		}
		finish := BackupRunFinish{Status: model.BackupRunStatusSucceeded, BytesWritten: 42, ArtifactCount: 1, FinishedAt: heartbeatAt.Add(time.Minute)}
		if _, err := stateStore.FinishBackupRun(run.ID, "worker-stale", finish); !errors.Is(err, ErrConflict) {
			t.Fatalf("expected stale finish to be fenced, got %v", err)
		}
		finished, err := stateStore.FinishBackupRun(run.ID, "worker-owner", finish)
		if err != nil {
			t.Fatalf("finish claimed backup run: %v", err)
		}
		if finished.Status != model.BackupRunStatusSucceeded || finished.BytesWritten != 42 || finished.ArtifactCount != 1 || finished.LockedUntil != nil {
			t.Fatalf("unexpected fenced finish result: %+v", finished)
		}
		if _, err := stateStore.FinishBackupRun(run.ID, "worker-owner", finish); !errors.Is(err, ErrConflict) {
			t.Fatalf("expected terminal run to reject repeated finish, got %v", err)
		}
	})

	t.Run("expired owner cannot renew or finish", func(t *testing.T) {
		stateStore, backend, policy := newBackupClaimTestStore(t)
		run, err := stateStore.CreateBackupRun(model.BackupRun{
			PolicyID:  policy.ID,
			Target:    policy.Target,
			BackendID: backend.ID,
			Trigger:   model.BackupRunTriggerManual,
			Status:    model.BackupRunStatusPending,
		})
		if err != nil {
			t.Fatalf("create pending backup run: %v", err)
		}
		claimedAt := time.Now().UTC()
		if _, err := stateStore.ClaimBackupRun(run.ID, "worker-expired", claimedAt, time.Minute); err != nil {
			t.Fatalf("claim backup run: %v", err)
		}
		afterExpiry := claimedAt.Add(time.Minute + time.Second)
		if _, err := stateStore.HeartbeatBackupRun(run.ID, "worker-expired", afterExpiry, time.Minute); !errors.Is(err, ErrConflict) {
			t.Fatalf("expected expired heartbeat to conflict, got %v", err)
		}
		if _, err := stateStore.FinishBackupRun(run.ID, "worker-expired", BackupRunFinish{Status: model.BackupRunStatusSucceeded, FinishedAt: afterExpiry}); !errors.Is(err, ErrConflict) {
			t.Fatalf("expected expired finish to conflict, got %v", err)
		}
	})
}

func TestManagedPostgresSuspendAndBackupRunAreMutuallyExclusive(t *testing.T) {
	t.Run("pending and running database backups block suspend without cancellation", func(t *testing.T) {
		for _, claim := range []bool{false, true} {
			name := "pending"
			if claim {
				name = "running"
			}
			t.Run(name, func(t *testing.T) {
				stateStore, app, service, backend := newManagedPostgresBackupInterlockTestStore(t)
				run, err := stateStore.CreateBackupRun(managedPostgresInterlockBackupRun(app, service, backend, model.BackupRunTriggerManual))
				if err != nil {
					t.Fatalf("create app database backup run: %v", err)
				}
				if claim {
					if _, err := stateStore.ClaimBackupRun(run.ID, "backup-worker", time.Now().UTC(), 2*time.Minute); err != nil {
						t.Fatalf("claim app database backup run: %v", err)
					}
				}
				if _, err := stateStore.CreateOperation(model.Operation{
					TenantID:  app.TenantID,
					Type:      model.OperationTypeDatabaseSuspend,
					AppID:     app.ID,
					ServiceID: service.ID,
				}); !errors.Is(err, ErrManagedPostgresBackupInProgressConflict) {
					t.Fatalf("expected %s database backup to block suspend, got %v", name, err)
				}
				stored, err := stateStore.GetBackupRun(run.ID, app.TenantID, false)
				if err != nil {
					t.Fatalf("get backup run after rejected suspend: %v", err)
				}
				wantStatus := model.BackupRunStatusPending
				if claim {
					wantStatus = model.BackupRunStatusRunning
				}
				if stored.Status != wantStatus {
					t.Fatalf("suspend must not cancel or mutate backup run: %+v", stored)
				}
				operations, err := stateStore.ListOperationsByApp(app.TenantID, false, app.ID)
				if err != nil {
					t.Fatalf("list app operations: %v", err)
				}
				for _, op := range operations {
					if op.Type == model.OperationTypeDatabaseSuspend && isActiveOperationStatus(op.Status) {
						t.Fatalf("rejected suspend leaked an active operation: %+v", op)
					}
				}
			})
		}
	})

	t.Run("active suspend rejects a new database backup", func(t *testing.T) {
		stateStore, app, service, backend := newManagedPostgresBackupInterlockTestStore(t)
		if _, err := stateStore.CreateOperation(model.Operation{
			TenantID:  app.TenantID,
			Type:      model.OperationTypeDatabaseSuspend,
			AppID:     app.ID,
			ServiceID: service.ID,
		}); err != nil {
			t.Fatalf("create database suspend operation: %v", err)
		}
		if _, err := stateStore.CreateBackupRun(managedPostgresInterlockBackupRun(app, service, backend, model.BackupRunTriggerManual)); !errors.Is(err, ErrManagedPostgresSuspendBackupConflict) {
			t.Fatalf("expected active suspend to reject database backup create, got %v", err)
		}
		runs, err := stateStore.ListBackupRuns(BackupRunFilter{TenantID: app.TenantID, PlatformAdmin: false})
		if err != nil {
			t.Fatalf("list backup runs: %v", err)
		}
		if len(runs) != 0 {
			t.Fatalf("rejected backup create leaked a run: %+v", runs)
		}
	})

	t.Run("claim atomically fails a legacy pending run during suspend", func(t *testing.T) {
		stateStore, app, service, backend := newManagedPostgresBackupInterlockTestStore(t)
		if _, err := stateStore.CreateOperation(model.Operation{
			TenantID:  app.TenantID,
			Type:      model.OperationTypeDatabaseSuspend,
			AppID:     app.ID,
			ServiceID: service.ID,
		}); err != nil {
			t.Fatalf("create database suspend operation: %v", err)
		}
		run := managedPostgresInterlockBackupRun(app, service, backend, model.BackupRunTriggerScheduled)
		run.ID = model.NewID("backup_run")
		run.CreatedAt = time.Now().UTC().Add(-time.Minute)
		run.UpdatedAt = run.CreatedAt
		if err := stateStore.withLockedState(true, func(state *model.State) error {
			state.BackupRuns = append(state.BackupRuns, model.NormalizeBackupRun(run))
			return nil
		}); err != nil {
			t.Fatalf("seed legacy pending backup run: %v", err)
		}

		failed, err := stateStore.ClaimBackupRun(run.ID, "scheduled-worker", time.Now().UTC(), 2*time.Minute)
		if !errors.Is(err, ErrManagedPostgresSuspendBackupConflict) {
			t.Fatalf("expected claim to fail deterministically during suspend, got run=%+v err=%v", failed, err)
		}
		if failed.Status != model.BackupRunStatusFailed ||
			failed.ErrorCode != ManagedPostgresSuspendBackupConflictCode ||
			failed.ErrorMessage != ManagedPostgresSuspendBackupConflictMessage ||
			failed.FinishedAt == nil {
			t.Fatalf("unexpected terminal suspended backup run: %+v", failed)
		}
		stored, err := stateStore.GetBackupRun(run.ID, app.TenantID, false)
		if err != nil {
			t.Fatalf("reread failed backup run: %v", err)
		}
		if stored.Status != model.BackupRunStatusFailed || stored.ErrorCode != ManagedPostgresSuspendBackupConflictCode {
			t.Fatalf("terminal failure was not persisted: %+v", stored)
		}
		if _, err := stateStore.ClaimBackupRun(run.ID, "second-worker", time.Now().UTC(), 2*time.Minute); !errors.Is(err, ErrConflict) {
			t.Fatalf("terminal failed backup run must not be claimable again, got %v", err)
		}
	})

	t.Run("other backup targets remain available during suspend", func(t *testing.T) {
		stateStore, app, service, backend := newManagedPostgresBackupInterlockTestStore(t)
		if _, err := stateStore.CreateOperation(model.Operation{
			TenantID:  app.TenantID,
			Type:      model.OperationTypeDatabaseSuspend,
			AppID:     app.ID,
			ServiceID: service.ID,
		}); err != nil {
			t.Fatalf("create database suspend operation: %v", err)
		}
		run, err := stateStore.CreateBackupRun(model.BackupRun{
			TenantID:  app.TenantID,
			ProjectID: app.ProjectID,
			AppID:     app.ID,
			Target: model.BackupTarget{
				Type:      model.BackupTargetPersistentStorage,
				TenantID:  app.TenantID,
				ProjectID: app.ProjectID,
				AppID:     app.ID,
				RuntimeID: app.Spec.RuntimeID,
			},
			BackendID: backend.ID,
			Trigger:   model.BackupRunTriggerScheduled,
			Status:    model.BackupRunStatusPending,
		})
		if err != nil {
			t.Fatalf("create non-database backup during suspend: %v", err)
		}
		claimed, err := stateStore.ClaimBackupRun(run.ID, "storage-worker", time.Now().UTC(), 2*time.Minute)
		if err != nil || claimed.Status != model.BackupRunStatusRunning {
			t.Fatalf("non-database backup was blocked by postgres suspend: run=%+v err=%v", claimed, err)
		}
	})

	t.Run("concurrent create admits exactly one side", func(t *testing.T) {
		stateStore, app, service, backend := newManagedPostgresBackupInterlockTestStore(t)
		start := make(chan struct{})
		results := make(chan error, 2)
		go func() {
			<-start
			_, err := stateStore.CreateOperation(model.Operation{
				TenantID:  app.TenantID,
				Type:      model.OperationTypeDatabaseSuspend,
				AppID:     app.ID,
				ServiceID: service.ID,
			})
			results <- err
		}()
		go func() {
			<-start
			_, err := stateStore.CreateBackupRun(managedPostgresInterlockBackupRun(app, service, backend, model.BackupRunTriggerScheduled))
			results <- err
		}()
		close(start)

		succeeded := 0
		conflicted := 0
		for range 2 {
			err := <-results
			switch {
			case err == nil:
				succeeded++
			case errors.Is(err, ErrManagedPostgresSuspendBackupConflict), errors.Is(err, ErrManagedPostgresBackupInProgressConflict):
				conflicted++
			default:
				t.Fatalf("unexpected concurrent create result: %v", err)
			}
		}
		if succeeded != 1 || conflicted != 1 {
			t.Fatalf("expected one winner and one interlock conflict, got succeeded=%d conflicted=%d", succeeded, conflicted)
		}

		operations, err := stateStore.ListOperationsByApp(app.TenantID, false, app.ID)
		if err != nil {
			t.Fatalf("list operations after race: %v", err)
		}
		activeSuspend := false
		for _, op := range operations {
			if op.Type == model.OperationTypeDatabaseSuspend && isActiveOperationStatus(op.Status) {
				activeSuspend = true
			}
		}
		runs, err := stateStore.ListBackupRuns(BackupRunFilter{TenantID: app.TenantID})
		if err != nil {
			t.Fatalf("list backup runs after race: %v", err)
		}
		activeBackup := false
		for _, run := range runs {
			if run.Target.Type == model.BackupTargetAppDatabase &&
				(run.Status == model.BackupRunStatusPending || run.Status == model.BackupRunStatusRunning) {
				activeBackup = true
			}
		}
		if activeSuspend == activeBackup {
			t.Fatalf("interlock invariant violated: active_suspend=%t active_backup=%t", activeSuspend, activeBackup)
		}
	})
}

func TestRecoverStaleBackupRunUsesObservedLeaseCAS(t *testing.T) {
	t.Parallel()

	t.Run("recovers an unchanged stale running observation", func(t *testing.T) {
		stateStore, backend, policy := newBackupClaimTestStore(t)
		staleHeartbeat := time.Now().UTC().Add(-10 * time.Minute)
		staleLock := staleHeartbeat.Add(time.Minute)
		run, err := stateStore.CreateBackupRun(model.BackupRun{
			PolicyID:    policy.ID,
			Target:      policy.Target,
			BackendID:   backend.ID,
			Trigger:     model.BackupRunTriggerManual,
			Status:      model.BackupRunStatusRunning,
			LeaseOwner:  "worker-owner",
			LockedUntil: &staleLock,
			HeartbeatAt: &staleHeartbeat,
		})
		if err != nil {
			t.Fatalf("create stale running backup run: %v", err)
		}
		recoveredAt := time.Now().UTC()
		recovered, err := stateStore.RecoverStaleBackupRun(run, recoveredAt, 2*time.Minute)
		if err != nil {
			t.Fatalf("recover unchanged stale backup run: %v", err)
		}
		if recovered.Status != model.BackupRunStatusFailed || recovered.ErrorCode != backupRunLostErrorCode || recovered.LockedUntil != nil || recovered.FinishedAt == nil || !recovered.FinishedAt.Equal(recoveredAt) {
			t.Fatalf("unexpected recovered backup run: %+v", recovered)
		}
	})

	t.Run("heartbeat renewal invalidates running observation", func(t *testing.T) {
		stateStore, backend, policy := newBackupClaimTestStore(t)
		staleHeartbeat := time.Now().UTC().Add(-10 * time.Minute)
		staleLock := staleHeartbeat.Add(time.Minute)
		observed, err := stateStore.CreateBackupRun(model.BackupRun{
			PolicyID:    policy.ID,
			Target:      policy.Target,
			BackendID:   backend.ID,
			Trigger:     model.BackupRunTriggerManual,
			Status:      model.BackupRunStatusRunning,
			LeaseOwner:  "worker-owner",
			LockedUntil: &staleLock,
			HeartbeatAt: &staleHeartbeat,
		})
		if err != nil {
			t.Fatalf("create stale running backup run: %v", err)
		}
		replica := New(stateStore.path)
		if err := replica.Init(); err != nil {
			t.Fatalf("init heartbeat replica store: %v", err)
		}
		recoveredAt := time.Now().UTC()
		// Model a heartbeat that started before the old lease expired but
		// committed after the recovery scanner read the stale observation.
		renewedAt := staleLock.Add(-time.Second)
		renewed, err := replica.HeartbeatBackupRun(observed.ID, observed.LeaseOwner, renewedAt, 15*time.Minute)
		if err != nil {
			t.Fatalf("renew backup run lease: %v", err)
		}
		if _, err := stateStore.RecoverStaleBackupRun(observed, recoveredAt, 2*time.Minute); !errors.Is(err, ErrConflict) {
			t.Fatalf("expected pre-heartbeat observation to conflict, got %v", err)
		}
		current, err := stateStore.GetBackupRun(observed.ID, "", true)
		if err != nil {
			t.Fatalf("get renewed backup run: %v", err)
		}
		if current.Status != model.BackupRunStatusRunning || current.HeartbeatAt == nil || !current.HeartbeatAt.Equal(renewedAt) || current.LockedUntil == nil || renewed.LockedUntil == nil || !current.LockedUntil.Equal(*renewed.LockedUntil) {
			t.Fatalf("stale recovery overwrote renewed backup run: %+v", current)
		}
	})

	t.Run("pending claim invalidates scanner observation", func(t *testing.T) {
		stateStore, backend, policy := newBackupClaimTestStore(t)
		observed, err := stateStore.CreateBackupRun(model.BackupRun{
			PolicyID:  policy.ID,
			Target:    policy.Target,
			BackendID: backend.ID,
			Trigger:   model.BackupRunTriggerManual,
			Status:    model.BackupRunStatusPending,
		})
		if err != nil {
			t.Fatalf("create pending backup run: %v", err)
		}
		replica := New(stateStore.path)
		if err := replica.Init(); err != nil {
			t.Fatalf("init claiming replica store: %v", err)
		}
		claimAt := observed.UpdatedAt.Add(2*time.Minute + time.Second)
		claimed, err := replica.ClaimBackupRun(observed.ID, "worker-claim", claimAt, 2*time.Minute)
		if err != nil {
			t.Fatalf("claim pending backup run: %v", err)
		}
		if _, err := stateStore.RecoverStaleBackupRun(observed, claimAt, 2*time.Minute); !errors.Is(err, ErrConflict) {
			t.Fatalf("expected pre-claim pending observation to conflict, got %v", err)
		}
		current, err := stateStore.GetBackupRun(observed.ID, "", true)
		if err != nil {
			t.Fatalf("get claimed backup run: %v", err)
		}
		if current.Status != model.BackupRunStatusRunning || current.LeaseOwner != claimed.LeaseOwner || current.LockedUntil == nil {
			t.Fatalf("stale recovery overwrote claimed backup run: %+v", current)
		}
	})
}

func TestCreateBackupArtifactForRunFencesLeaseAndDefersPolicySuccess(t *testing.T) {
	t.Parallel()

	t.Run("owned artifact is persisted but policy advances only after finish", func(t *testing.T) {
		stateStore, backend, policy := newBackupClaimTestStore(t)
		run, err := stateStore.CreateBackupRun(model.BackupRun{
			PolicyID:  policy.ID,
			Target:    policy.Target,
			BackendID: backend.ID,
			Trigger:   model.BackupRunTriggerManual,
			Status:    model.BackupRunStatusPending,
		})
		if err != nil {
			t.Fatalf("create backup run: %v", err)
		}
		claimedAt := time.Now().UTC()
		claimed, err := stateStore.ClaimBackupRun(run.ID, "artifact-worker", claimedAt, 2*time.Minute)
		if err != nil {
			t.Fatalf("claim backup run: %v", err)
		}
		artifactInput := model.BackupArtifact{
			RunID:        claimed.ID,
			PolicyID:     policy.ID,
			Target:       policy.Target,
			BackendID:    backend.ID,
			Kind:         model.BackupArtifactKindControlPlanePGDump,
			ObjectKey:    "fenced.dump",
			SizeBytes:    42,
			LogicalBytes: 84,
			Status:       model.BackupArtifactStatusActive,
		}
		if _, err := stateStore.CreateBackupArtifactForRun(artifactInput, "wrong-worker"); !errors.Is(err, ErrConflict) {
			t.Fatalf("expected wrong artifact owner to conflict, got %v", err)
		}
		artifact, err := stateStore.CreateBackupArtifactForRun(artifactInput, claimed.LeaseOwner)
		if err != nil {
			t.Fatalf("persist fenced backup artifact: %v", err)
		}
		beforeFinish, err := stateStore.GetBackupPolicy(policy.ID, "", true)
		if err != nil {
			t.Fatalf("get policy before finish: %v", err)
		}
		if beforeFinish.LastSuccessfulRunID != "" || beforeFinish.LastSuccessfulAt != nil {
			t.Fatalf("artifact insertion advanced policy success before finish: %+v", beforeFinish)
		}
		finishedAt := claimedAt.Add(time.Minute)
		if _, err := stateStore.FinishBackupRun(claimed.ID, claimed.LeaseOwner, BackupRunFinish{
			Status:        model.BackupRunStatusSucceeded,
			BytesWritten:  artifact.SizeBytes,
			ArtifactCount: 1,
			FinishedAt:    finishedAt,
		}); err != nil {
			t.Fatalf("finish backup run: %v", err)
		}
		afterFinish, err := stateStore.GetBackupPolicy(policy.ID, "", true)
		if err != nil {
			t.Fatalf("get policy after finish: %v", err)
		}
		if afterFinish.LastSuccessfulRunID != claimed.ID || afterFinish.LastSuccessfulAt == nil || !afterFinish.LastSuccessfulAt.Equal(finishedAt) {
			t.Fatalf("successful fenced finish did not advance policy: %+v", afterFinish)
		}
	})

	t.Run("expired owner cannot persist active artifact", func(t *testing.T) {
		stateStore, backend, policy := newBackupClaimTestStore(t)
		run, err := stateStore.CreateBackupRun(model.BackupRun{
			PolicyID:  policy.ID,
			Target:    policy.Target,
			BackendID: backend.ID,
			Trigger:   model.BackupRunTriggerManual,
			Status:    model.BackupRunStatusPending,
		})
		if err != nil {
			t.Fatalf("create backup run: %v", err)
		}
		claimedAt := time.Now().UTC().Add(-2 * time.Minute)
		claimed, err := stateStore.ClaimBackupRun(run.ID, "expired-artifact-worker", claimedAt, time.Minute)
		if err != nil {
			t.Fatalf("claim backup run with expired lease: %v", err)
		}
		if _, err := stateStore.CreateBackupArtifactForRun(model.BackupArtifact{
			RunID:     claimed.ID,
			PolicyID:  policy.ID,
			Target:    policy.Target,
			BackendID: backend.ID,
			Kind:      model.BackupArtifactKindControlPlanePGDump,
			ObjectKey: "must-not-persist.dump",
			Status:    model.BackupArtifactStatusActive,
		}, claimed.LeaseOwner); !errors.Is(err, ErrConflict) {
			t.Fatalf("expected expired artifact writer to conflict, got %v", err)
		}
		artifacts, err := stateStore.ListBackupArtifacts(BackupArtifactFilter{RunID: claimed.ID, PlatformAdmin: true})
		if err != nil {
			t.Fatalf("list artifacts after rejected write: %v", err)
		}
		if len(artifacts) != 0 {
			t.Fatalf("expired worker persisted active artifact: %+v", artifacts)
		}
	})
}

func TestBackupSchedulerQueriesFilterBeforeLimit(t *testing.T) {
	t.Parallel()

	t.Run("due retries are ordered by oldest due time before limit", func(t *testing.T) {
		stateStore := New(filepath.Join(t.TempDir(), "store.json"))
		if err := stateStore.Init(); err != nil {
			t.Fatalf("init store: %v", err)
		}
		now := time.Date(2026, time.July, 12, 14, 0, 0, 0, time.UTC)
		oldestDue := now.Add(-10 * time.Minute)
		newerDue := now.Add(-time.Minute)
		if err := stateStore.withLockedState(true, func(state *model.State) error {
			state.BackupRuns = append(state.BackupRuns,
				model.BackupRun{ID: "retry_oldest_due", Trigger: model.BackupRunTriggerRetry, Status: model.BackupRunStatusPending, NextRetryAt: &oldestDue, CreatedAt: now.Add(-2 * time.Hour), UpdatedAt: now.Add(-2 * time.Hour)},
				model.BackupRun{ID: "retry_newer_due", Trigger: model.BackupRunTriggerRetry, Status: model.BackupRunStatusPending, NextRetryAt: &newerDue, CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Hour)},
			)
			for idx := 0; idx < 150; idx++ {
				future := now.Add(time.Duration(idx+1) * time.Minute)
				state.BackupRuns = append(state.BackupRuns, model.BackupRun{
					ID:          fmt.Sprintf("retry_future_%03d", idx),
					Trigger:     model.BackupRunTriggerRetry,
					Status:      model.BackupRunStatusPending,
					NextRetryAt: &future,
					CreatedAt:   now.Add(time.Duration(idx) * time.Minute),
					UpdatedAt:   now.Add(time.Duration(idx) * time.Minute),
				})
			}
			return nil
		}); err != nil {
			t.Fatalf("seed retry runs: %v", err)
		}
		runs, err := stateStore.ListDueBackupRetryRuns(now, 1)
		if err != nil {
			t.Fatalf("list due retry runs: %v", err)
		}
		if len(runs) != 1 || runs[0].ID != "retry_oldest_due" {
			t.Fatalf("newer future retries starved oldest due retry: %+v", runs)
		}
	})

	t.Run("stale runs are ordered by oldest expiry before limit", func(t *testing.T) {
		stateStore := New(filepath.Join(t.TempDir(), "store.json"))
		if err := stateStore.Init(); err != nil {
			t.Fatalf("init store: %v", err)
		}
		now := time.Date(2026, time.July, 12, 14, 0, 0, 0, time.UTC)
		oldestLock := now.Add(-30 * time.Minute)
		if err := stateStore.withLockedState(true, func(state *model.State) error {
			state.BackupRuns = append(state.BackupRuns,
				model.BackupRun{ID: "run_oldest_stale", Trigger: model.BackupRunTriggerManual, Status: model.BackupRunStatusRunning, LeaseOwner: "worker-old", LockedUntil: &oldestLock, CreatedAt: now.Add(-2 * time.Hour), UpdatedAt: now.Add(-2 * time.Hour)},
				model.BackupRun{ID: "run_newer_stale", Trigger: model.BackupRunTriggerManual, Status: model.BackupRunStatusPending, CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-10 * time.Minute)},
			)
			for idx := 0; idx < 600; idx++ {
				healthyLock := now.Add(time.Duration(idx+1) * time.Minute)
				state.BackupRuns = append(state.BackupRuns, model.BackupRun{
					ID:          fmt.Sprintf("run_healthy_%03d", idx),
					Trigger:     model.BackupRunTriggerManual,
					Status:      model.BackupRunStatusRunning,
					LeaseOwner:  "worker-healthy",
					LockedUntil: &healthyLock,
					CreatedAt:   now.Add(time.Duration(idx) * time.Minute),
					UpdatedAt:   now,
				})
			}
			return nil
		}); err != nil {
			t.Fatalf("seed stale and healthy runs: %v", err)
		}
		runs, err := stateStore.ListStaleBackupRuns(now, 2*time.Minute, 1)
		if err != nil {
			t.Fatalf("list stale backup runs: %v", err)
		}
		if len(runs) != 1 || runs[0].ID != "run_oldest_stale" {
			t.Fatalf("newer healthy runs starved oldest stale run: %+v", runs)
		}
	})
}

func newBackupClaimTestStore(t *testing.T) (*Store, model.BackupBackend, model.BackupPolicy) {
	t.Helper()
	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	backend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		Name:     "claim-r2",
		Provider: model.DataBackendProviderCloudflareR2,
		Bucket:   "claim-bucket",
		Endpoint: "https://example.r2.cloudflarestorage.com",
	})
	if err != nil {
		t.Fatalf("create backup backend: %v", err)
	}
	policy, err := stateStore.UpsertBackupPolicy(model.BackupPolicy{
		Name:      "claim-policy",
		Target:    model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase},
		BackendID: backend.ID,
		Enabled:   true,
		Status:    model.BackupPolicyStatusActive,
		Schedule:  model.BackupDefaultSchedule,
	})
	if err != nil {
		t.Fatalf("create backup policy: %v", err)
	}
	return stateStore, backend, policy
}

func newManagedPostgresBackupInterlockTestStore(t *testing.T) (*Store, model.App, model.BackingService, model.BackupBackend) {
	t.Helper()

	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Backup Suspend Interlock")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	if _, err := stateStore.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{
		CPUMilliCores:    4000,
		MemoryMebibytes:  8192,
		StorageGibibytes: 20,
	}); err != nil {
		t.Fatalf("raise tenant capacity: %v", err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "postgres-app", "", model.AppSpec{
		Image:     "ghcr.io/example/app:latest",
		RuntimeID: model.DefaultManagedRuntimeID,
		Replicas:  1,
		Postgres: &model.AppPostgresSpec{
			Database:    "appdb",
			User:        "appuser",
			Password:    "secret",
			StorageSize: "1Gi",
		},
	})
	if err != nil {
		t.Fatalf("create app with managed postgres: %v", err)
	}
	zero := 0
	scale, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeScale,
		AppID:           app.ID,
		DesiredReplicas: &zero,
	})
	if err != nil {
		t.Fatalf("create scale-to-zero operation: %v", err)
	}
	if _, err := stateStore.CompleteManagedOperation(scale.ID, "", "app disabled"); err != nil {
		t.Fatalf("complete scale-to-zero operation: %v", err)
	}
	app, err = stateStore.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get disabled app: %v", err)
	}
	var service model.BackingService
	for idx := range app.BackingServices {
		candidate := app.BackingServices[idx]
		if candidate.Type == model.BackingServiceTypePostgres && candidate.Spec.Postgres != nil {
			service = candidate
			break
		}
	}
	if service.ID == "" {
		t.Fatal("managed postgres backing service not found")
	}
	backend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		TenantID: tenant.ID,
		Name:     "tenant-r2",
		Provider: model.DataBackendProviderCloudflareR2,
		Bucket:   "tenant-bucket",
		Endpoint: "https://tenant.example.r2.cloudflarestorage.com",
	})
	if err != nil {
		t.Fatalf("create backup backend: %v", err)
	}
	return stateStore, app, service, backend
}

func managedPostgresInterlockBackupRun(app model.App, service model.BackingService, backend model.BackupBackend, trigger string) model.BackupRun {
	return model.BackupRun{
		TenantID:  app.TenantID,
		ProjectID: app.ProjectID,
		AppID:     app.ID,
		Target: model.BackupTarget{
			Type:        model.BackupTargetAppDatabase,
			TenantID:    app.TenantID,
			ProjectID:   app.ProjectID,
			AppID:       app.ID,
			ServiceName: service.Spec.Postgres.ServiceName,
			Database:    service.Spec.Postgres.Database,
		},
		BackendID: backend.ID,
		Trigger:   trigger,
		Status:    model.BackupRunStatusPending,
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
