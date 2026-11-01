package store

import (
	"errors"
	"path/filepath"
	"regexp"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestBackupRetryCreationFencesConcurrentAndStaleWorkers(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "state.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	backend, err := s.CreateBackupBackend(model.BackupBackend{Name: "backup-store", Provider: model.DataBackendProviderCloudflareR2, Bucket: "backup-bucket"})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := s.UpsertBackupPolicy(model.BackupPolicy{Name: "database", Target: model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase}, BackendID: backend.ID, Enabled: true, Status: model.BackupPolicyStatusActive})
	if err != nil {
		t.Fatal(err)
	}
	failed, err := s.CreateBackupRun(model.BackupRun{PolicyID: policy.ID, Target: policy.Target, BackendID: backend.ID, Trigger: model.BackupRunTriggerRetry, Status: model.BackupRunStatusFailed})
	if err != nil {
		t.Fatal(err)
	}
	due := time.Now().UTC()
	retry := model.BackupRun{PolicyID: policy.ID, Target: policy.Target, BackendID: backend.ID, Trigger: model.BackupRunTriggerRetry, Status: model.BackupRunStatusPending, NextRetryAt: &due}
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := s.CreateBackupRetryRun(retry, failed.ID); results <- err }()
	}
	wg.Wait()
	close(results)
	success, conflict := 0, 0
	for err := range results {
		switch {
		case err == nil:
			success++
		case errors.Is(err, ErrConflict):
			conflict++
		default:
			t.Fatal(err)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatalf("concurrent retry results: success=%d conflict=%d", success, conflict)
	}
	runs, err := s.ListBackupRuns(BackupRunFilter{PolicyID: policy.ID, Status: model.BackupRunStatusPending, PlatformAdmin: true})
	if err != nil || len(runs) != 1 {
		t.Fatalf("pending retry inventory: %v %+v", err, runs)
	}
	succeeded := model.BackupRunStatusSucceeded
	if _, err := s.UpdateBackupRun(runs[0].ID, BackupRunUpdate{Status: &succeeded}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateBackupRetryRun(retry, failed.ID); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale worker retried after newer success: %v", err)
	}
}

func TestPGBackupRetryRejectsSupersededFailureUnderPolicyLock(t *testing.T) {
	s, mock := newBackupSchedulePGTestStore(t)
	policy := backupScheduleTestPolicy("database-policy")
	policy.LastRunID = "newer-success"
	query := regexp.QuoteMeta(backupPolicySelectSQL() + ` WHERE id = $1 FOR UPDATE`)
	mock.ExpectBegin()
	mock.ExpectQuery(query).WithArgs(policy.ID).WillReturnRows(backupSchedulePolicyRows(policy))
	mock.ExpectRollback()
	due := time.Now().UTC()
	_, err := s.CreateBackupRetryRun(model.BackupRun{PolicyID: policy.ID, Target: policy.Target, Trigger: model.BackupRunTriggerRetry, Status: model.BackupRunStatusPending, NextRetryAt: &due}, "old-failure")
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected superseded retry conflict, got %v", err)
	}
	assertBackupSchedulePGExpectations(t, mock)
}
