package api

import (
	"context"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestQueuedAutomaticBackupRechecksDisabledPolicy(t *testing.T) {
	for _, trigger := range []string{model.BackupRunTriggerScheduled, model.BackupRunTriggerRetry, model.BackupRunTriggerManual} {
		t.Run(trigger, func(t *testing.T) {
			stateStore, backend := newBackupCoordinationProofStore(t)
			due := time.Now().UTC().Add(-time.Minute)
			policy, err := stateStore.UpsertBackupPolicy(model.BackupPolicy{Name: "queued-policy", Target: model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase}, BackendID: backend, Enabled: true, Schedule: "@hourly", NextRunAt: &due})
			if err != nil {
				t.Fatal(err)
			}
			run, err := stateStore.CreateBackupRun(model.BackupRun{PolicyID: policy.ID, Target: policy.Target, BackendID: backend, Trigger: trigger})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := stateStore.SetBackupPolicyEnabled(policy.ID, "", true, false, "maintenance"); err != nil {
				t.Fatal(err)
			}
			server := &Server{store: stateStore}
			err = server.validateAutomaticBackupPolicy(run)
			if trigger == model.BackupRunTriggerManual {
				if err != nil {
					t.Fatalf("explicit manual run was blocked: %v", err)
				}
				return
			}
			if err != errBackupPolicyDisabled {
				t.Fatalf("disabled policy accepted: %v", err)
			}
			configureTestControlPlaneBackupCoordination(server, nil)
			called := false
			server.backupRunner = func(context.Context, model.BackupRun) ([]model.BackupArtifact, error) { called = true; return nil, nil }
			server.executeBackupRun(context.Background(), run.ID)
			if called {
				t.Fatal("disabled queued backup started an export")
			}
			stored, err := stateStore.GetBackupRun(run.ID, "", true)
			if err != nil || stored.Status != model.BackupRunStatusFailed || stored.ErrorCode != "backup_policy_disabled" {
				t.Fatalf("missing terminal explanation: %+v %v", stored, err)
			}
		})
	}
}
