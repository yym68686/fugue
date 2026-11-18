package api

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func newPendingLonghornBackup(t *testing.T, trigger string) (*store.Store, model.BackupRun) {
	t.Helper()
	fixture := newBackupAuthorizationFixture(t)
	app := fixture.victimApp
	target := model.BackupTarget{
		Type: model.BackupTargetAppDatabase, Engine: model.BackupEngineLonghornSnapshot,
		TenantID: app.TenantID, ProjectID: app.ProjectID, AppID: app.ID,
	}
	policy, err := fixture.stateStore.UpsertBackupPolicy(model.BackupPolicy{
		Name: "snapshot-policy", TenantID: app.TenantID, ProjectID: app.ProjectID, AppID: app.ID,
		Target: target, BackendID: fixture.victimBackend.ID, Enabled: true, Schedule: "0 3 * * *",
	})
	if err != nil {
		t.Fatal(err)
	}
	due := time.Now().UTC().Add(-time.Minute)
	policy.NextRunAt = &due
	if _, err := fixture.stateStore.UpsertBackupPolicy(policy); err != nil {
		t.Fatal(err)
	}
	run, err := fixture.stateStore.CreateBackupRun(model.BackupRun{
		PolicyID: policy.ID, Target: target, BackendID: fixture.victimBackend.ID,
		TenantID: app.TenantID, ProjectID: app.ProjectID, AppID: app.ID, Trigger: trigger,
	})
	if err != nil {
		t.Fatal(err)
	}
	return fixture.stateStore, run
}

func waitPendingBackupStatus(t *testing.T, stateStore *store.Store, id, status string) model.BackupRun {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		run, err := stateStore.GetBackupRun(id, "", true)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status == status {
			return run
		}
		if time.Now().After(deadline) {
			t.Fatalf("backup did not reach %s: %+v", status, run)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPendingLonghornBackupResumesSameRunAfterCoordinationContention(t *testing.T) {
	for _, trigger := range []string{model.BackupRunTriggerScheduled, model.BackupRunTriggerManual} {
		t.Run(trigger, func(t *testing.T) {
			stateStore, run := newPendingLonghornBackup(t, trigger)
			backend := &testControlPlaneBackupCoordinationBackend{blocked: true}
			first := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
			configureTestControlPlaneBackupCoordination(first, backend)
			var calls atomic.Int32
			runner := func(_ context.Context, claimed model.BackupRun) ([]model.BackupArtifact, error) {
				calls.Add(1)
				if claimed.ID != run.ID || claimed.Status != model.BackupRunStatusRunning || claimed.LeaseOwner == "" {
					t.Errorf("resumed a different or unclaimed run: %+v", claimed)
				}
				return nil, nil
			}
			first.backupRunner = runner
			first.executeBackupRun(context.Background(), run.ID)
			pending, err := stateStore.GetBackupRun(run.ID, "", true)
			if err != nil || pending.Status != model.BackupRunStatusPending || pending.StartedAt != nil || pending.LeaseOwner != "" || calls.Load() != 0 {
				t.Fatalf("contended backup must stay pending without side effects: %+v %v", pending, err)
			}
			// A task older than the stale-worker deadline must not be failed or
			// replaced while it is still waiting to acquire coordination.
			old := time.Now().UTC().Add(-24 * time.Hour)
			if _, err := stateStore.UpdateBackupRun(run.ID, store.BackupRunUpdate{HeartbeatAt: timePtrPtr(&old)}); err != nil {
				t.Fatal(err)
			}
			first.recoverStaleBackupRuns(context.Background())
			pending, err = stateStore.GetBackupRun(run.ID, "", true)
			if err != nil || pending.Status != model.BackupRunStatusPending || pending.Attempt != run.Attempt {
				t.Fatalf("waiting task was incorrectly recovered as a lost worker: %+v %v", pending, err)
			}

			// Simulate a restart/second API replica. Normal scheduler ticks must
			// resume the original durable task, with the same fencing and claim.
			second := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
			configureTestControlPlaneBackupCoordination(second, backend)
			second.backupRunner = runner
			backend.setBlocked(false)
			first.enqueueDueBackups(context.Background())
			second.enqueueDueBackups(context.Background())
			finished := waitPendingBackupStatus(t, stateStore, run.ID, model.BackupRunStatusSucceeded)
			if calls.Load() != 1 || finished.Attempt != run.Attempt || finished.RetryCount != 0 {
				t.Fatalf("duplicate execution or invented retry: calls=%d run=%+v", calls.Load(), finished)
			}
			runs, err := stateStore.ListBackupRuns(store.BackupRunFilter{PolicyID: run.PolicyID, PlatformAdmin: true})
			if err != nil || len(runs) != 1 || runs[0].ID != run.ID {
				t.Fatalf("scheduler replaced the pending run: %+v %v", runs, err)
			}
		})
	}
}

func TestPendingLonghornBackupScannerHonorsDisabledPolicy(t *testing.T) {
	stateStore, run := newPendingLonghornBackup(t, model.BackupRunTriggerScheduled)
	if _, err := stateStore.SetBackupPolicyEnabled(run.PolicyID, "", true, false, "maintenance"); err != nil {
		t.Fatal(err)
	}
	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	configureTestControlPlaneBackupCoordination(server, nil)
	var calls atomic.Int32
	server.backupRunner = func(context.Context, model.BackupRun) ([]model.BackupArtifact, error) {
		calls.Add(1)
		return nil, nil
	}
	server.enqueuePendingCoordinatedBackupRuns(context.Background())
	finished := waitPendingBackupStatus(t, stateStore, run.ID, model.BackupRunStatusFailed)
	if calls.Load() != 0 || finished.ErrorCode != "backup_policy_disabled" {
		t.Fatalf("scanner bypassed disabled policy: calls=%d run=%+v", calls.Load(), finished)
	}
}

func TestPendingLonghornBackupScannerResumesRecoveredScheduledWorker(t *testing.T) {
	stateStore, run := newPendingLonghornBackup(t, model.BackupRunTriggerScheduled)
	now := time.Now().UTC()
	if _, err := stateStore.ClaimBackupRun(run.ID, "lost-worker", now.Add(-time.Hour), time.Minute); err != nil {
		t.Fatal(err)
	}
	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	configureTestControlPlaneBackupCoordination(server, nil)
	server.recoverStaleBackupRuns(context.Background())
	recovered, err := stateStore.GetBackupRun(run.ID, "", true)
	if err != nil || recovered.Status != model.BackupRunStatusPending || recovered.StartedAt == nil || recovered.NextRetryAt == nil {
		t.Fatalf("Longhorn worker not requeued for observation: %+v %v", recovered, err)
	}
	server.backupRunner = func(_ context.Context, observed model.BackupRun) ([]model.BackupArtifact, error) {
		if observed.ID != run.ID {
			t.Errorf("must observe the existing run's Longhorn objects: %+v", observed)
		}
		return nil, nil
	}
	server.enqueuePendingCoordinatedBackupRuns(context.Background())
	waitPendingBackupStatus(t, stateStore, run.ID, model.BackupRunStatusSucceeded)
}
