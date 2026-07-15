package store

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestRepairBackupPolicySchedule(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.July, 12, 13, 30, 0, 0, time.UTC)
	lastRunAt := time.Date(2026, time.July, 12, 7, 15, 0, 0, time.UTC)
	policy := backupScheduleTestPolicy("policy_repair")
	policy.CreatedAt = now.Add(-48 * time.Hour)
	policy.LastRunAt = &lastRunAt
	policy.NextRunAt = nil

	repaired, changed := repairBackupPolicySchedule(policy, now)
	if !changed {
		t.Fatal("expected missing next_run_at to be repaired")
	}
	wantNext := time.Date(2026, time.July, 12, 12, 0, 0, 0, time.UTC)
	if repaired.NextRunAt == nil || !repaired.NextRunAt.Equal(wantNext) {
		t.Fatalf("expected repair to anchor after last run at %s, got %+v", wantNext, repaired.NextRunAt)
	}
	if !repaired.UpdatedAt.Equal(now) {
		t.Fatalf("expected repair timestamp %s, got %s", now, repaired.UpdatedAt)
	}

	alreadyScheduled := policy
	alreadyScheduled.NextRunAt = &wantNext
	repaired, changed = repairBackupPolicySchedule(alreadyScheduled, now)
	if changed || repaired.NextRunAt == nil || !repaired.NextRunAt.Equal(wantNext) {
		t.Fatalf("expected repair to be idempotent for an already scheduled policy, got changed=%t policy=%+v", changed, repaired)
	}

	invalid := policy
	invalid.ID = "policy_invalid"
	invalid.Schedule = "not cron"
	invalid.NextRunAt = nil
	repaired, changed = repairBackupPolicySchedule(invalid, now)
	if !changed || repaired.Status != model.BackupPolicyStatusError || repaired.NextRunAt != nil || !strings.Contains(repaired.DisabledReason, "invalid backup schedule") {
		t.Fatalf("expected invalid active policy to fail closed, got %+v", repaired)
	}

	disabled := invalid
	disabled.Enabled = false
	disabled.Status = model.BackupPolicyStatusDisabled
	repaired, changed = repairBackupPolicySchedule(disabled, now)
	if changed || repaired.Status != model.BackupPolicyStatusDisabled {
		t.Fatalf("expected disabled invalid policy to remain available for kill-switch operations, got changed=%t policy=%+v", changed, repaired)
	}
}

func TestPGStartupRepairsSixHourPolicyWithCAS(t *testing.T) {
	t.Parallel()

	s, mock := newBackupSchedulePGTestStore(t)
	createdAt := time.Date(2026, time.July, 12, 1, 10, 0, 0, time.UTC)
	policy := backupScheduleTestPolicy("policy_startup_repair")
	policy.CreatedAt = createdAt
	policy.UpdatedAt = createdAt
	policy.NextRunAt = nil
	wantNext := time.Date(2026, time.July, 12, 6, 0, 0, 0, time.UTC)

	mock.ExpectQuery(regexp.QuoteMeta(backupPolicySelectSQL() + ` WHERE enabled = TRUE AND status = 'active'`)).
		WillReturnRows(backupSchedulePolicyRows(policy))
	mock.ExpectExec(regexp.QuoteMeta(pgRepairBackupPolicyScheduleSQL)).
		WithArgs(policy.ID, policy.Schedule, model.BackupPolicyStatusActive, "", wantNext, sqlmock.AnyArg(), policy.Schedule, nil, nil).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := s.pgRepairBackupPolicySchedules(); err != nil {
		t.Fatalf("repair startup backup policies: %v", err)
	}
	assertBackupSchedulePGExpectations(t, mock)
}

func TestPGEnsureDefaultBackupPolicyPreservesLegacyNullForRepair(t *testing.T) {
	t.Parallel()

	s, mock := newBackupSchedulePGTestStore(t)
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id FROM fugue_backup_backends WHERE id = $1 AND status = 'active'`)).
		WithArgs(defaultBackupBackendID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(defaultBackupBackendID))
	mock.ExpectExec(`(?s)INSERT INTO fugue_backup_policies .*next_run_at = CASE.*ELSE fugue_backup_policies.next_run_at\s+END`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := s.pgEnsureDefaultBackupPolicy(); err != nil {
		t.Fatalf("ensure default backup policy: %v", err)
	}
	assertBackupSchedulePGExpectations(t, mock)
}

func TestPGEnsureDefaultBackupPolicyPreservesDisabledConflictState(t *testing.T) {
	t.Parallel()

	s, mock := newBackupSchedulePGTestStore(t)
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id FROM fugue_backup_backends WHERE id = $1 AND status = 'active'`)).
		WithArgs(defaultBackupBackendID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(defaultBackupBackendID))
	mock.ExpectExec(`(?s)ON CONFLICT \(id\) DO UPDATE SET.*enabled = CASE\s+WHEN fugue_backup_policies.enabled = FALSE OR fugue_backup_policies.status = 'disabled' THEN FALSE\s+ELSE TRUE\s+END,.*status = CASE\s+WHEN fugue_backup_policies.enabled = FALSE OR fugue_backup_policies.status = 'disabled' THEN 'disabled'.*next_run_at = CASE\s+WHEN fugue_backup_policies.enabled = FALSE OR fugue_backup_policies.status = 'disabled' THEN NULL\s+ELSE fugue_backup_policies.next_run_at\s+END,`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := s.pgEnsureDefaultBackupPolicy(); err != nil {
		t.Fatalf("ensure disabled default backup policy: %v", err)
	}
	assertBackupSchedulePGExpectations(t, mock)
}

func TestPGListDueBackupPoliciesRepairsNullThenRequeriesAuthoritatively(t *testing.T) {
	t.Parallel()

	t.Run("future repair is not returned", func(t *testing.T) {
		s, mock := newBackupSchedulePGTestStore(t)
		now := time.Date(2026, time.July, 12, 12, 30, 0, 0, time.UTC)
		policy := backupScheduleTestPolicy("policy_future")
		policy.CreatedAt = time.Date(2026, time.July, 12, 12, 1, 0, 0, time.UTC)
		policy.UpdatedAt = policy.CreatedAt
		wantNext := time.Date(2026, time.July, 12, 18, 0, 0, 0, time.UTC)

		expectNullBackupScheduleRepairQuery(mock, 25, policy)
		mock.ExpectExec(regexp.QuoteMeta(pgRepairBackupPolicyScheduleSQL)).
			WithArgs(policy.ID, policy.Schedule, model.BackupPolicyStatusActive, "", wantNext, now, policy.Schedule, nil, nil).
			WillReturnResult(sqlmock.NewResult(0, 1))
		expectDueBackupScheduleQuery(mock, now, 25, sqlmock.NewRows(backupSchedulePolicyColumns()))

		policies, err := s.pgListDueBackupPolicies(now, 25)
		if err != nil {
			t.Fatalf("list due backup policies: %v", err)
		}
		if len(policies) != 0 {
			t.Fatalf("expected future repair to be omitted, got %+v", policies)
		}
		assertBackupSchedulePGExpectations(t, mock)
	})

	t.Run("past repair is returned by due query", func(t *testing.T) {
		s, mock := newBackupSchedulePGTestStore(t)
		now := time.Date(2026, time.July, 12, 12, 30, 0, 0, time.UTC)
		policy := backupScheduleTestPolicy("policy_due")
		policy.CreatedAt = time.Date(2026, time.July, 12, 1, 1, 0, 0, time.UTC)
		policy.UpdatedAt = policy.CreatedAt
		wantNext := time.Date(2026, time.July, 12, 6, 0, 0, 0, time.UTC)

		expectNullBackupScheduleRepairQuery(mock, 25, policy)
		mock.ExpectExec(regexp.QuoteMeta(pgRepairBackupPolicyScheduleSQL)).
			WithArgs(policy.ID, policy.Schedule, model.BackupPolicyStatusActive, "", wantNext, now, policy.Schedule, nil, nil).
			WillReturnResult(sqlmock.NewResult(0, 1))
		repaired := policy
		repaired.NextRunAt = &wantNext
		repaired.UpdatedAt = now
		expectDueBackupScheduleQuery(mock, now, 25, backupSchedulePolicyRows(repaired))

		policies, err := s.pgListDueBackupPolicies(now, 25)
		if err != nil {
			t.Fatalf("list due backup policies: %v", err)
		}
		if len(policies) != 1 || policies[0].ID != policy.ID || policies[0].NextRunAt == nil || !policies[0].NextRunAt.Equal(wantNext) {
			t.Fatalf("expected repaired due policy, got %+v", policies)
		}
		assertBackupSchedulePGExpectations(t, mock)
	})

	t.Run("lost CAS never returns the stale candidate", func(t *testing.T) {
		s, mock := newBackupSchedulePGTestStore(t)
		now := time.Date(2026, time.July, 12, 12, 30, 0, 0, time.UTC)
		policy := backupScheduleTestPolicy("policy_cas_lost")
		policy.CreatedAt = time.Date(2026, time.July, 12, 1, 1, 0, 0, time.UTC)
		policy.UpdatedAt = policy.CreatedAt
		wantNext := time.Date(2026, time.July, 12, 6, 0, 0, 0, time.UTC)

		expectNullBackupScheduleRepairQuery(mock, 25, policy)
		mock.ExpectExec(regexp.QuoteMeta(pgRepairBackupPolicyScheduleSQL)).
			WithArgs(policy.ID, policy.Schedule, model.BackupPolicyStatusActive, "", wantNext, now, policy.Schedule, nil, nil).
			WillReturnResult(sqlmock.NewResult(0, 0))
		expectDueBackupScheduleQuery(mock, now, 25, sqlmock.NewRows(backupSchedulePolicyColumns()))

		policies, err := s.pgListDueBackupPolicies(now, 25)
		if err != nil {
			t.Fatalf("list due backup policies after CAS loss: %v", err)
		}
		if len(policies) != 0 {
			t.Fatalf("expected CAS loser to trust only the authoritative due query, got %+v", policies)
		}
		assertBackupSchedulePGExpectations(t, mock)
	})
}

func TestPGUpsertBackupPolicySchedulesSixHourPolicy(t *testing.T) {
	t.Parallel()

	s, mock := newBackupSchedulePGTestStore(t)
	policy := backupScheduleTestPolicy("policy_upsert_6h")
	policy.NextRunAt = nil
	returned := policy
	returnedNext := time.Date(2026, time.July, 12, 18, 0, 0, 0, time.UTC)
	returned.NextRunAt = &returnedNext

	args := backupScheduleAnyArgs(29)
	args[16] = policy.Schedule
	args[24] = backupSixHourTimeArgument{}
	args[28] = backupSixHourTimeArgument{}
	mock.ExpectQuery(backupPolicyUpsertQueryPattern()).
		WithArgs(args...).
		WillReturnRows(backupSchedulePolicyRows(returned))

	saved, err := s.UpsertBackupPolicy(policy)
	if err != nil {
		t.Fatalf("upsert six-hour backup policy: %v", err)
	}
	if saved.NextRunAt == nil || !saved.NextRunAt.Equal(returnedNext) {
		t.Fatalf("expected non-null six-hour next run, got %+v", saved)
	}
	assertBackupSchedulePGExpectations(t, mock)
}

func TestPGUpsertBackupPolicyScheduleChangeUsesFreshNextRun(t *testing.T) {
	t.Parallel()

	s, mock := newBackupSchedulePGTestStore(t)
	policy := backupScheduleTestPolicy("policy_schedule_change")
	oldNext := time.Date(2026, time.July, 12, 13, 0, 0, 0, time.UTC)
	policy.NextRunAt = &oldNext
	returned := policy
	returnedNext := time.Date(2026, time.July, 12, 18, 0, 0, 0, time.UTC)
	returned.NextRunAt = &returnedNext

	args := backupScheduleAnyArgs(29)
	args[16] = policy.Schedule
	args[24] = oldNext
	args[28] = backupSixHourTimeArgument{}
	mock.ExpectQuery(backupPolicyUpsertQueryPattern()).
		WithArgs(args...).
		WillReturnRows(backupSchedulePolicyRows(returned))

	saved, err := s.UpsertBackupPolicy(policy)
	if err != nil {
		t.Fatalf("upsert changed backup schedule: %v", err)
	}
	if saved.NextRunAt == nil || !saved.NextRunAt.Equal(returnedNext) {
		t.Fatalf("expected changed schedule to use fresh next run, got %+v", saved)
	}
	assertBackupSchedulePGExpectations(t, mock)
}

func TestPGCreateBackupRunAdvancesSixHourSchedule(t *testing.T) {
	t.Parallel()

	s, mock := newBackupSchedulePGTestStore(t)
	policy := backupScheduleTestPolicy("policy_create_run")
	dueAt := time.Now().UTC().Add(-time.Minute)
	policy.NextRunAt = &dueAt
	run := model.NormalizeBackupRun(model.BackupRun{
		ID:              "backup_run_schedule_advance",
		PolicyID:        policy.ID,
		Trigger:         model.BackupRunTriggerScheduled,
		Status:          model.BackupRunStatusPending,
		RequestedByType: "system",
		RequestedByID:   "backup-scheduler",
		CreatedAt:       time.Date(2026, time.July, 12, 12, 0, 5, 0, time.UTC),
	})
	returnedRun := run
	returnedRun.TenantID = policy.TenantID
	returnedRun.ProjectID = policy.ProjectID
	returnedRun.AppID = policy.AppID
	returnedRun.Target = policy.Target
	returnedRun.BackendID = policy.BackendID
	returnedRun.UpdatedAt = run.CreatedAt

	mock.ExpectBegin()
	mock.ExpectQuery(`(?s)SELECT .* FROM fugue_backup_policies WHERE id = \$1 FOR UPDATE`).
		WithArgs(policy.ID).
		WillReturnRows(backupSchedulePolicyRows(policy))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id FROM fugue_apps WHERE id = $1 FOR UPDATE`)).
		WithArgs(policy.AppID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(policy.AppID))
	mock.ExpectQuery(`(?s)SELECT EXISTS .*FROM fugue_operations AS operation.*operation.type = \$2`).
		WithArgs(
			policy.AppID,
			model.OperationTypeDatabaseSuspend,
			model.OperationStatusPending,
			model.OperationStatusRunning,
			model.OperationStatusWaitingAgent,
			policy.Target.ServiceName,
		).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT EXISTS (SELECT 1 FROM fugue_backup_runs WHERE status IN ('pending', 'running') AND target_type = $1 AND COALESCE(target_tenant_id, '') = $2 AND COALESCE(target_project_id, '') = $3 AND COALESCE(target_app_id, '') = $4)`)).
		WithArgs(policy.Target.Type, policy.Target.TenantID, policy.Target.ProjectID, policy.Target.AppID).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectQuery(`(?s)INSERT INTO fugue_backup_runs .*RETURNING`).
		WillReturnRows(backupScheduleRunRows(returnedRun))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE fugue_backup_policies SET last_run_id = $2, last_run_at = $3, next_run_at = $4, updated_at = $3 WHERE id = $1`)).
		WithArgs(policy.ID, run.ID, sqlmock.AnyArg(), backupSixHourTimeArgument{}).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	created, err := s.CreateBackupRun(run)
	if err != nil {
		t.Fatalf("create scheduled backup run: %v", err)
	}
	if created.ID != run.ID || created.PolicyID != policy.ID {
		t.Fatalf("unexpected created backup run: %+v", created)
	}
	assertBackupSchedulePGExpectations(t, mock)
}

func TestPGCreateScheduledBackupRunRechecksPolicyDueUnderLock(t *testing.T) {
	t.Parallel()

	s, mock := newBackupSchedulePGTestStore(t)
	policy := backupScheduleTestPolicy("policy_not_due")
	nextRunAt := time.Now().UTC().Add(time.Hour)
	policy.NextRunAt = &nextRunAt
	run := model.NormalizeBackupRun(model.BackupRun{
		ID:              "backup_run_not_due",
		PolicyID:        policy.ID,
		Target:          policy.Target,
		BackendID:       policy.BackendID,
		Trigger:         model.BackupRunTriggerScheduled,
		Status:          model.BackupRunStatusPending,
		RequestedByType: "system",
		RequestedByID:   "backup-scheduler",
	})

	mock.ExpectBegin()
	mock.ExpectQuery(`(?s)SELECT .* FROM fugue_backup_policies WHERE id = \$1 FOR UPDATE`).
		WithArgs(policy.ID).
		WillReturnRows(backupSchedulePolicyRows(policy))
	mock.ExpectRollback()

	if _, err := s.CreateBackupRun(run); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected scheduled run with future next_run_at to conflict, got %v", err)
	}
	assertBackupSchedulePGExpectations(t, mock)
}

func TestPGClaimBackupRunIsAtomicAndDueAware(t *testing.T) {
	t.Parallel()

	t.Run("claims one due pending run", func(t *testing.T) {
		s, mock := newBackupSchedulePGTestStore(t)
		now := time.Date(2026, time.July, 12, 12, 30, 0, 0, time.UTC)
		leaseTTL := 2 * time.Minute
		lockedUntil := now.Add(leaseTTL)
		run := model.NormalizeBackupRun(model.BackupRun{
			ID:              "backup_run_claim",
			PolicyID:        "policy_claim",
			Target:          model.BackupTarget{Type: model.BackupTargetAppDatabase, AppID: "app_claim"},
			BackendID:       "backup_backend_r2",
			Trigger:         model.BackupRunTriggerRetry,
			Status:          model.BackupRunStatusRunning,
			RequestedByType: "system",
			RequestedByID:   "backup-retry",
			LeaseOwner:      "worker-a",
			LockedUntil:     &lockedUntil,
			HeartbeatAt:     &now,
			StartedAt:       &now,
			CreatedAt:       now.Add(-5 * time.Minute),
			UpdatedAt:       now,
		})
		targetJSON, _ := json.Marshal(run.Target)
		mock.ExpectBegin()
		mock.ExpectQuery(`(?s)SELECT COALESCE\(NULLIF\(app_id, ''\), target_app_id, ''\), target_json.*FROM fugue_backup_runs.*WHERE id = \$1`).
			WithArgs(run.ID).
			WillReturnRows(sqlmock.NewRows([]string{"app_id", "target_json"}).AddRow(run.Target.AppID, targetJSON))
		mock.ExpectQuery(regexp.QuoteMeta(`SELECT id FROM fugue_apps WHERE id = $1 FOR UPDATE`)).
			WithArgs(run.Target.AppID).
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(run.Target.AppID))
		mock.ExpectQuery(`(?s)SELECT EXISTS .*FROM fugue_operations AS operation.*operation.type = \$2`).
			WithArgs(
				run.Target.AppID,
				model.OperationTypeDatabaseSuspend,
				model.OperationStatusPending,
				model.OperationStatusRunning,
				model.OperationStatusWaitingAgent,
				run.Target.ServiceName,
			).
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
		mock.ExpectQuery(`(?s)UPDATE fugue_backup_runs.*SET status = 'running'.*WHERE id = \$1 AND status = 'pending' AND \(next_retry_at IS NULL OR next_retry_at <= \$4\).*RETURNING`).
			WithArgs(run.ID, "worker-a", lockedUntil, now).
			WillReturnRows(backupScheduleRunRows(run))
		mock.ExpectCommit()

		claimed, err := s.ClaimBackupRun(run.ID, "worker-a", now, leaseTTL)
		if err != nil {
			t.Fatalf("claim pending backup run: %v", err)
		}
		if claimed.Status != model.BackupRunStatusRunning || claimed.LeaseOwner != "worker-a" || claimed.LockedUntil == nil || !claimed.LockedUntil.Equal(lockedUntil) {
			t.Fatalf("unexpected claimed backup run: %+v", claimed)
		}
		assertBackupSchedulePGExpectations(t, mock)
	})

	t.Run("losing claimant receives conflict", func(t *testing.T) {
		s, mock := newBackupSchedulePGTestStore(t)
		now := time.Date(2026, time.July, 12, 12, 30, 0, 0, time.UTC)
		leaseTTL := 2 * time.Minute
		target := model.BackupTarget{Type: model.BackupTargetAppDatabase, AppID: "app_claimed"}
		targetJSON, _ := json.Marshal(target)
		mock.ExpectBegin()
		mock.ExpectQuery(`(?s)SELECT COALESCE\(NULLIF\(app_id, ''\), target_app_id, ''\), target_json.*FROM fugue_backup_runs.*WHERE id = \$1`).
			WithArgs("backup_run_claimed").
			WillReturnRows(sqlmock.NewRows([]string{"app_id", "target_json"}).AddRow(target.AppID, targetJSON))
		mock.ExpectQuery(regexp.QuoteMeta(`SELECT id FROM fugue_apps WHERE id = $1 FOR UPDATE`)).
			WithArgs(target.AppID).
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(target.AppID))
		mock.ExpectQuery(`(?s)SELECT EXISTS .*FROM fugue_operations AS operation.*operation.type = \$2`).
			WithArgs(
				target.AppID,
				model.OperationTypeDatabaseSuspend,
				model.OperationStatusPending,
				model.OperationStatusRunning,
				model.OperationStatusWaitingAgent,
				target.ServiceName,
			).
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
		mock.ExpectQuery(`(?s)UPDATE fugue_backup_runs.*WHERE id = \$1 AND status = 'pending' AND \(next_retry_at IS NULL OR next_retry_at <= \$4\).*RETURNING`).
			WithArgs("backup_run_claimed", "worker-b", now.Add(leaseTTL), now).
			WillReturnRows(sqlmock.NewRows(backupScheduleRunColumns()))
		mock.ExpectRollback()

		if _, err := s.ClaimBackupRun("backup_run_claimed", "worker-b", now, leaseTTL); !errors.Is(err, ErrConflict) {
			t.Fatalf("expected losing claimant to receive conflict, got %v", err)
		}
		assertBackupSchedulePGExpectations(t, mock)
	})
}

func TestPGCreateDatabaseBackupRunLocksAppAndRejectsActiveSuspend(t *testing.T) {
	t.Parallel()

	s, mock := newBackupSchedulePGTestStore(t)
	run := model.NormalizeBackupRun(model.BackupRun{
		ID:        "backup_run_suspend_conflict",
		TenantID:  "tenant_backup",
		ProjectID: "project_backup",
		AppID:     "app_backup",
		Target: model.BackupTarget{
			Type:        model.BackupTargetAppDatabase,
			TenantID:    "tenant_backup",
			ProjectID:   "project_backup",
			AppID:       "app_backup",
			ServiceName: "postgres-app",
		},
		BackendID: "backup_backend_r2",
		Trigger:   model.BackupRunTriggerManual,
		Status:    model.BackupRunStatusPending,
	})

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id FROM fugue_apps WHERE id = $1 FOR UPDATE`)).
		WithArgs(run.AppID).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(run.AppID))
	mock.ExpectQuery(`(?s)SELECT EXISTS .*FROM fugue_operations AS operation.*operation.type = \$2`).
		WithArgs(
			run.AppID,
			model.OperationTypeDatabaseSuspend,
			model.OperationStatusPending,
			model.OperationStatusRunning,
			model.OperationStatusWaitingAgent,
			run.Target.ServiceName,
		).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectRollback()

	if _, err := s.CreateBackupRun(run); !errors.Is(err, ErrManagedPostgresSuspendBackupConflict) {
		t.Fatalf("expected active suspend to reject backup create under app lock, got %v", err)
	}
	assertBackupSchedulePGExpectations(t, mock)
}

func TestPGClaimDatabaseBackupRunPersistsTerminalFailureDuringSuspend(t *testing.T) {
	t.Parallel()

	s, mock := newBackupSchedulePGTestStore(t)
	now := time.Date(2026, time.July, 12, 12, 30, 0, 0, time.UTC)
	pending := model.NormalizeBackupRun(model.BackupRun{
		ID:        "backup_run_pending_during_suspend",
		TenantID:  "tenant_backup",
		ProjectID: "project_backup",
		AppID:     "app_backup",
		Target: model.BackupTarget{
			Type:        model.BackupTargetAppDatabase,
			TenantID:    "tenant_backup",
			ProjectID:   "project_backup",
			AppID:       "app_backup",
			ServiceName: "postgres-app",
		},
		BackendID: "backup_backend_r2",
		Trigger:   model.BackupRunTriggerScheduled,
		Status:    model.BackupRunStatusPending,
		CreatedAt: now.Add(-time.Minute),
		UpdatedAt: now.Add(-time.Minute),
	})
	failed := pending
	failed.Status = model.BackupRunStatusFailed
	failed.ErrorCode = ManagedPostgresSuspendBackupConflictCode
	failed.ErrorMessage = ManagedPostgresSuspendBackupConflictMessage
	failed.UpdatedAt = now
	failed.FinishedAt = &now
	targetJSON, _ := json.Marshal(pending.Target)

	expectCandidateAndSuspend := func() {
		mock.ExpectQuery(`(?s)SELECT COALESCE\(NULLIF\(app_id, ''\), target_app_id, ''\), target_json.*FROM fugue_backup_runs.*WHERE id = \$1`).
			WithArgs(pending.ID).
			WillReturnRows(sqlmock.NewRows([]string{"app_id", "target_json"}).AddRow(pending.AppID, targetJSON))
		mock.ExpectQuery(regexp.QuoteMeta(`SELECT id FROM fugue_apps WHERE id = $1 FOR UPDATE`)).
			WithArgs(pending.AppID).
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(pending.AppID))
		mock.ExpectQuery(`(?s)SELECT EXISTS .*FROM fugue_operations AS operation.*operation.type = \$2`).
			WithArgs(
				pending.AppID,
				model.OperationTypeDatabaseSuspend,
				model.OperationStatusPending,
				model.OperationStatusRunning,
				model.OperationStatusWaitingAgent,
				pending.Target.ServiceName,
			).
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	}

	mock.ExpectBegin()
	expectCandidateAndSuspend()
	mock.ExpectQuery(`(?s)UPDATE fugue_backup_runs.*SET status = \$2.*error_code = \$3.*error_message = \$4.*WHERE id = \$1.*AND status = \$6.*RETURNING`).
		WithArgs(
			pending.ID,
			model.BackupRunStatusFailed,
			ManagedPostgresSuspendBackupConflictCode,
			ManagedPostgresSuspendBackupConflictMessage,
			now,
			model.BackupRunStatusPending,
		).
		WillReturnRows(backupScheduleRunRows(failed))
	mock.ExpectCommit()

	observed, err := s.ClaimBackupRun(pending.ID, "scheduled-worker", now, 2*time.Minute)
	if !errors.Is(err, ErrManagedPostgresSuspendBackupConflict) {
		t.Fatalf("expected deterministic suspend conflict, got run=%+v err=%v", observed, err)
	}
	if observed.Status != model.BackupRunStatusFailed ||
		observed.ErrorCode != ManagedPostgresSuspendBackupConflictCode ||
		observed.ErrorMessage != ManagedPostgresSuspendBackupConflictMessage ||
		observed.FinishedAt == nil {
		t.Fatalf("unexpected terminal failed run: %+v", observed)
	}

	mock.ExpectQuery(regexp.QuoteMeta(backupRunSelectSQL()+` WHERE id = $1 AND tenant_id = $2`)).
		WithArgs(pending.ID, pending.TenantID).
		WillReturnRows(backupScheduleRunRows(failed))
	stored, err := s.GetBackupRun(pending.ID, pending.TenantID, false)
	if err != nil {
		t.Fatalf("reread terminal failed backup run: %v", err)
	}
	if stored.Status != model.BackupRunStatusFailed || stored.ErrorCode != ManagedPostgresSuspendBackupConflictCode {
		t.Fatalf("terminal failure was not durable: %+v", stored)
	}

	mock.ExpectBegin()
	expectCandidateAndSuspend()
	mock.ExpectQuery(`(?s)UPDATE fugue_backup_runs.*SET status = \$2.*WHERE id = \$1.*AND status = \$6.*RETURNING`).
		WithArgs(
			pending.ID,
			model.BackupRunStatusFailed,
			ManagedPostgresSuspendBackupConflictCode,
			ManagedPostgresSuspendBackupConflictMessage,
			now,
			model.BackupRunStatusPending,
		).
		WillReturnRows(sqlmock.NewRows(backupScheduleRunColumns()))
	mock.ExpectRollback()
	if _, err := s.ClaimBackupRun(pending.ID, "second-worker", now, 2*time.Minute); !errors.Is(err, ErrConflict) {
		t.Fatalf("terminal failed run must not be claimable again, got %v", err)
	}
	assertBackupSchedulePGExpectations(t, mock)
}

func TestPGBackupSchedulerQueriesFilterAndOrderBeforeLimit(t *testing.T) {
	t.Parallel()

	t.Run("due retries use next retry ordering", func(t *testing.T) {
		s, mock := newBackupSchedulePGTestStore(t)
		now := time.Date(2026, time.July, 12, 14, 0, 0, 0, time.UTC)
		dueAt := now.Add(-10 * time.Minute)
		run := model.NormalizeBackupRun(model.BackupRun{
			ID:          "retry_oldest_due",
			Trigger:     model.BackupRunTriggerRetry,
			Status:      model.BackupRunStatusPending,
			NextRetryAt: &dueAt,
			CreatedAt:   now.Add(-time.Hour),
			UpdatedAt:   now.Add(-time.Hour),
		})
		mock.ExpectQuery(`(?s)SELECT .* FROM fugue_backup_runs.*WHERE status = 'pending'.*AND trigger = 'retry'.*AND next_retry_at <= \$1.*ORDER BY next_retry_at ASC, created_at ASC.*LIMIT \$2`).
			WithArgs(now, 1).
			WillReturnRows(backupScheduleRunRows(run))

		runs, err := s.ListDueBackupRetryRuns(now, 1)
		if err != nil {
			t.Fatalf("list due backup retry runs: %v", err)
		}
		if len(runs) != 1 || runs[0].ID != run.ID {
			t.Fatalf("unexpected due retry runs: %+v", runs)
		}
		assertBackupSchedulePGExpectations(t, mock)
	})

	t.Run("stale runs use expiry ordering", func(t *testing.T) {
		s, mock := newBackupSchedulePGTestStore(t)
		now := time.Date(2026, time.July, 12, 14, 0, 0, 0, time.UTC)
		leaseTTL := 2 * time.Minute
		staleLock := now.Add(-10 * time.Minute)
		run := model.NormalizeBackupRun(model.BackupRun{
			ID:          "run_oldest_stale",
			Trigger:     model.BackupRunTriggerManual,
			Status:      model.BackupRunStatusRunning,
			LeaseOwner:  "worker-old",
			LockedUntil: &staleLock,
			CreatedAt:   now.Add(-time.Hour),
			UpdatedAt:   now.Add(-time.Hour),
		})
		mock.ExpectQuery(`(?s)SELECT .* FROM fugue_backup_runs.*WHERE status IN \('running', 'pending'\).*next_retry_at > \$1.*INTERVAL '1 microsecond'.*< \$1.*ORDER BY .* ASC, created_at ASC.*LIMIT \$3`).
			WithArgs(now, leaseTTL.Microseconds(), 1).
			WillReturnRows(backupScheduleRunRows(run))

		runs, err := s.ListStaleBackupRuns(now, leaseTTL, 1)
		if err != nil {
			t.Fatalf("list stale backup runs: %v", err)
		}
		if len(runs) != 1 || runs[0].ID != run.ID {
			t.Fatalf("unexpected stale backup runs: %+v", runs)
		}
		assertBackupSchedulePGExpectations(t, mock)
	})
}

func TestPGBackupRunHeartbeatAndFinishAreLeaseFenced(t *testing.T) {
	t.Parallel()

	s, mock := newBackupSchedulePGTestStore(t)
	now := time.Date(2026, time.July, 12, 12, 30, 0, 0, time.UTC)
	leaseTTL := 2 * time.Minute
	heartbeatAt := now.Add(time.Minute)
	lockedUntil := heartbeatAt.Add(leaseTTL)
	run := model.NormalizeBackupRun(model.BackupRun{
		ID:              "backup_run_fenced",
		Target:          model.BackupTarget{Type: model.BackupTargetAppDatabase, AppID: "app_fenced"},
		BackendID:       "backup_backend_r2",
		Trigger:         model.BackupRunTriggerScheduled,
		Status:          model.BackupRunStatusRunning,
		RequestedByType: "system",
		RequestedByID:   "backup-scheduler",
		LeaseOwner:      "worker-owner",
		LockedUntil:     &lockedUntil,
		HeartbeatAt:     &heartbeatAt,
		StartedAt:       &now,
		CreatedAt:       now.Add(-time.Minute),
		UpdatedAt:       heartbeatAt,
	})
	mock.ExpectQuery(`(?s)UPDATE fugue_backup_runs.*SET locked_until = \$3, heartbeat_at = \$4, updated_at = \$4.*WHERE id = \$1 AND status = 'running' AND lease_owner = \$2 AND locked_until IS NOT NULL AND locked_until >= \$4.*RETURNING`).
		WithArgs(run.ID, "worker-owner", lockedUntil, heartbeatAt).
		WillReturnRows(backupScheduleRunRows(run))

	heartbeat, err := s.HeartbeatBackupRun(run.ID, "worker-owner", heartbeatAt, leaseTTL)
	if err != nil {
		t.Fatalf("heartbeat claimed backup run: %v", err)
	}
	if heartbeat.LeaseOwner != "worker-owner" || heartbeat.LockedUntil == nil || !heartbeat.LockedUntil.Equal(lockedUntil) {
		t.Fatalf("unexpected heartbeat result: %+v", heartbeat)
	}

	finishedAt := heartbeatAt.Add(time.Minute)
	finish := BackupRunFinish{Status: model.BackupRunStatusSucceeded, BytesWritten: 42, ArtifactCount: 1, FinishedAt: finishedAt}
	finishPattern := `(?s)UPDATE fugue_backup_runs.*SET status = \$3, locked_until = NULL.*WHERE id = \$1 AND status = 'running' AND lease_owner = \$2 AND locked_until IS NOT NULL AND locked_until >= \$4.*RETURNING`
	mock.ExpectBegin()
	mock.ExpectQuery(finishPattern).
		WithArgs(run.ID, "worker-stale", finish.Status, finishedAt, finish.BytesWritten, finish.ArtifactCount, "", "").
		WillReturnRows(sqlmock.NewRows(backupScheduleRunColumns()))
	mock.ExpectRollback()
	if _, err := s.FinishBackupRun(run.ID, "worker-stale", finish); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected stale finish to conflict, got %v", err)
	}

	finishedRun := run
	finishedRun.Status = model.BackupRunStatusSucceeded
	finishedRun.LockedUntil = nil
	finishedRun.HeartbeatAt = &finishedAt
	finishedRun.BytesWritten = finish.BytesWritten
	finishedRun.ArtifactCount = finish.ArtifactCount
	finishedRun.FinishedAt = &finishedAt
	finishedRun.UpdatedAt = finishedAt
	mock.ExpectBegin()
	mock.ExpectQuery(finishPattern).
		WithArgs(run.ID, "worker-owner", finish.Status, finishedAt, finish.BytesWritten, finish.ArtifactCount, "", "").
		WillReturnRows(backupScheduleRunRows(finishedRun))
	mock.ExpectCommit()
	finished, err := s.FinishBackupRun(run.ID, "worker-owner", finish)
	if err != nil {
		t.Fatalf("finish claimed backup run: %v", err)
	}
	if finished.Status != model.BackupRunStatusSucceeded || finished.LockedUntil != nil || finished.FinishedAt == nil || !finished.FinishedAt.Equal(finishedAt) {
		t.Fatalf("unexpected finished backup run: %+v", finished)
	}
	assertBackupSchedulePGExpectations(t, mock)
}

func TestPGRecoverStaleBackupRunUsesObservedLeaseCAS(t *testing.T) {
	t.Parallel()

	recoveryPattern := `(?s)UPDATE fugue_backup_runs.*SET status = 'failed'.*WHERE id = \$1.*AND status = \$2.*AND lease_owner = \$3.*AND updated_at = \$4.*AND locked_until IS NOT DISTINCT FROM \$5.*AND heartbeat_at IS NOT DISTINCT FROM \$6.*AND next_retry_at IS NOT DISTINCT FROM \$7.*RETURNING`

	t.Run("recovers only the unchanged running observation", func(t *testing.T) {
		s, mock := newBackupSchedulePGTestStore(t)
		recoveredAt := time.Date(2026, time.July, 12, 13, 0, 0, 0, time.UTC)
		staleHeartbeat := recoveredAt.Add(-10 * time.Minute)
		staleLock := recoveredAt.Add(-8 * time.Minute)
		observed := model.NormalizeBackupRun(model.BackupRun{
			ID:              "backup_run_recover_stale",
			PolicyID:        "policy_recover_stale",
			Target:          model.BackupTarget{Type: model.BackupTargetAppDatabase, AppID: "app_recover_stale"},
			BackendID:       "backup_backend_r2",
			Trigger:         model.BackupRunTriggerScheduled,
			Status:          model.BackupRunStatusRunning,
			RequestedByType: "system",
			RequestedByID:   "backup-scheduler",
			LeaseOwner:      "worker-owner",
			LockedUntil:     &staleLock,
			HeartbeatAt:     &staleHeartbeat,
			CreatedAt:       recoveredAt.Add(-time.Hour),
			UpdatedAt:       staleHeartbeat,
			StartedAt:       &staleHeartbeat,
		})
		recovered := observed
		recovered.Status = model.BackupRunStatusFailed
		recovered.LockedUntil = nil
		recovered.HeartbeatAt = &recoveredAt
		recovered.ErrorCode = backupRunLostErrorCode
		recovered.ErrorMessage = backupRunLostErrorMessage
		recovered.UpdatedAt = recoveredAt
		recovered.FinishedAt = &recoveredAt
		mock.ExpectQuery(recoveryPattern).
			WithArgs(observed.ID, observed.Status, observed.LeaseOwner, observed.UpdatedAt, staleLock, staleHeartbeat, nil, recoveredAt, backupRunLostErrorCode, backupRunLostErrorMessage).
			WillReturnRows(backupScheduleRunRows(recovered))

		got, err := s.RecoverStaleBackupRun(observed, recoveredAt, 2*time.Minute)
		if err != nil {
			t.Fatalf("recover unchanged stale backup run: %v", err)
		}
		if got.Status != model.BackupRunStatusFailed || got.ErrorCode != backupRunLostErrorCode || got.LockedUntil != nil || got.FinishedAt == nil || !got.FinishedAt.Equal(recoveredAt) {
			t.Fatalf("unexpected recovered backup run: %+v", got)
		}
		assertBackupSchedulePGExpectations(t, mock)
	})

	t.Run("heartbeat after scan causes conflict", func(t *testing.T) {
		s, mock := newBackupSchedulePGTestStore(t)
		recoveredAt := time.Date(2026, time.July, 12, 13, 0, 0, 0, time.UTC)
		staleHeartbeat := recoveredAt.Add(-10 * time.Minute)
		staleLock := recoveredAt.Add(-8 * time.Minute)
		observed := model.NormalizeBackupRun(model.BackupRun{
			ID:          "backup_run_heartbeat_race",
			Target:      model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase},
			Trigger:     model.BackupRunTriggerManual,
			Status:      model.BackupRunStatusRunning,
			LeaseOwner:  "worker-owner",
			LockedUntil: &staleLock,
			HeartbeatAt: &staleHeartbeat,
			CreatedAt:   recoveredAt.Add(-time.Hour),
			UpdatedAt:   staleHeartbeat,
		})
		mock.ExpectQuery(recoveryPattern).
			WithArgs(observed.ID, observed.Status, observed.LeaseOwner, observed.UpdatedAt, staleLock, staleHeartbeat, nil, recoveredAt, backupRunLostErrorCode, backupRunLostErrorMessage).
			WillReturnRows(sqlmock.NewRows(backupScheduleRunColumns()))

		if _, err := s.RecoverStaleBackupRun(observed, recoveredAt, 2*time.Minute); !errors.Is(err, ErrConflict) {
			t.Fatalf("expected heartbeat race to conflict, got %v", err)
		}
		assertBackupSchedulePGExpectations(t, mock)
	})

	t.Run("pending claim after scan causes conflict", func(t *testing.T) {
		s, mock := newBackupSchedulePGTestStore(t)
		recoveredAt := time.Date(2026, time.July, 12, 13, 0, 0, 0, time.UTC)
		observed := model.NormalizeBackupRun(model.BackupRun{
			ID:        "backup_run_claim_race",
			Target:    model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase},
			Trigger:   model.BackupRunTriggerManual,
			Status:    model.BackupRunStatusPending,
			CreatedAt: recoveredAt.Add(-time.Hour),
			UpdatedAt: recoveredAt.Add(-10 * time.Minute),
		})
		mock.ExpectQuery(recoveryPattern).
			WithArgs(observed.ID, observed.Status, "", observed.UpdatedAt, nil, nil, nil, recoveredAt, backupRunLostErrorCode, backupRunLostErrorMessage).
			WillReturnRows(sqlmock.NewRows(backupScheduleRunColumns()))

		if _, err := s.RecoverStaleBackupRun(observed, recoveredAt, 2*time.Minute); !errors.Is(err, ErrConflict) {
			t.Fatalf("expected pending claim race to conflict, got %v", err)
		}
		assertBackupSchedulePGExpectations(t, mock)
	})
}

func TestPGCreateBackupArtifactForRunFencesLeaseInTransaction(t *testing.T) {
	t.Parallel()

	artifact := model.NormalizeBackupArtifact(model.BackupArtifact{
		ID:           "artifact_fenced",
		RunID:        "backup_run_artifact",
		PolicyID:     "policy_artifact",
		Target:       model.BackupTarget{Type: model.BackupTargetAppDatabase, AppID: "app_artifact"},
		BackendID:    "backup_backend_r2",
		Kind:         model.BackupArtifactKindAppPGDump,
		ObjectKey:    "artifact.dump",
		SizeBytes:    42,
		LogicalBytes: 84,
		Status:       model.BackupArtifactStatusActive,
		CreatedAt:    time.Date(2026, time.July, 12, 13, 0, 0, 0, time.UTC),
	})

	t.Run("owner inserts and updates run under one lease lock", func(t *testing.T) {
		s, mock := newBackupSchedulePGTestStore(t)
		mock.ExpectBegin()
		mock.ExpectQuery(`(?s)SELECT policy_id.*FROM fugue_backup_runs.*WHERE id = \$1 AND status = 'running' AND lease_owner = \$2 AND locked_until IS NOT NULL AND locked_until >= \$3.*FOR UPDATE`).
			WithArgs(artifact.RunID, "worker-owner", sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"policy_id"}).AddRow(artifact.PolicyID))
		mock.ExpectQuery(`(?s)INSERT INTO fugue_backup_artifacts .*RETURNING`).
			WillReturnRows(backupScheduleArtifactRows(artifact))
		mock.ExpectExec(`(?s)UPDATE fugue_backup_runs SET artifact_count = artifact_count \+ 1.*WHERE id = \$1 AND status = 'running' AND lease_owner = \$5 AND locked_until IS NOT NULL AND locked_until >= \$4`).
			WithArgs(artifact.RunID, artifact.SizeBytes, artifact.LogicalBytes, sqlmock.AnyArg(), "worker-owner").
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()

		created, err := s.CreateBackupArtifactForRun(artifact, "worker-owner")
		if err != nil {
			t.Fatalf("create fenced backup artifact: %v", err)
		}
		if created.ID != artifact.ID || created.RunID != artifact.RunID || created.PolicyID != artifact.PolicyID {
			t.Fatalf("unexpected fenced artifact: %+v", created)
		}
		assertBackupSchedulePGExpectations(t, mock)
	})

	t.Run("expired or lost owner cannot insert", func(t *testing.T) {
		s, mock := newBackupSchedulePGTestStore(t)
		mock.ExpectBegin()
		mock.ExpectQuery(`(?s)SELECT policy_id.*FROM fugue_backup_runs.*WHERE id = \$1 AND status = 'running' AND lease_owner = \$2 AND locked_until IS NOT NULL AND locked_until >= \$3.*FOR UPDATE`).
			WithArgs(artifact.RunID, "worker-stale", sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"policy_id"}))
		mock.ExpectRollback()

		if _, err := s.CreateBackupArtifactForRun(artifact, "worker-stale"); !errors.Is(err, ErrConflict) {
			t.Fatalf("expected stale artifact owner to conflict, got %v", err)
		}
		assertBackupSchedulePGExpectations(t, mock)
	})
}

func TestPGFinishBackupRunAdvancesPolicyAfterFencedSuccess(t *testing.T) {
	t.Parallel()

	s, mock := newBackupSchedulePGTestStore(t)
	finishedAt := time.Date(2026, time.July, 12, 13, 0, 0, 0, time.UTC)
	run := model.NormalizeBackupRun(model.BackupRun{
		ID:            "backup_run_finish_policy",
		PolicyID:      "policy_finish_success",
		Target:        model.BackupTarget{Type: model.BackupTargetAppDatabase, AppID: "app_finish"},
		BackendID:     "backup_backend_r2",
		Trigger:       model.BackupRunTriggerManual,
		Status:        model.BackupRunStatusSucceeded,
		LeaseOwner:    "worker-owner",
		HeartbeatAt:   &finishedAt,
		CreatedAt:     finishedAt.Add(-time.Minute),
		UpdatedAt:     finishedAt,
		StartedAt:     &finishedAt,
		FinishedAt:    &finishedAt,
		BytesWritten:  42,
		ArtifactCount: 1,
	})
	finish := BackupRunFinish{Status: model.BackupRunStatusSucceeded, BytesWritten: 42, ArtifactCount: 1, FinishedAt: finishedAt}
	mock.ExpectBegin()
	mock.ExpectQuery(`(?s)UPDATE fugue_backup_runs.*WHERE id = \$1 AND status = 'running' AND lease_owner = \$2 AND locked_until IS NOT NULL AND locked_until >= \$4.*RETURNING`).
		WithArgs(run.ID, run.LeaseOwner, finish.Status, finishedAt, finish.BytesWritten, finish.ArtifactCount, "", "").
		WillReturnRows(backupScheduleRunRows(run))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE fugue_backup_policies SET last_successful_run_id = $2, last_successful_at = $3, updated_at = $3 WHERE id = $1`)).
		WithArgs(run.PolicyID, run.ID, finishedAt).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	policy := backupScheduleTestPolicy(run.PolicyID)
	policy.Target = run.Target
	policy.RetainCount = 0
	policy.Retention = model.BackupRetentionPolicy{}
	expectGetBackupSchedulePolicy(mock, policy)

	finished, err := s.FinishBackupRun(run.ID, run.LeaseOwner, finish)
	if err != nil {
		t.Fatalf("finish backup run and advance policy: %v", err)
	}
	if finished.Status != model.BackupRunStatusSucceeded || finished.PolicyID != run.PolicyID {
		t.Fatalf("unexpected finished run: %+v", finished)
	}
	assertBackupSchedulePGExpectations(t, mock)
}

func TestPGSetBackupPolicyEnabledInvalidScheduleKillSwitch(t *testing.T) {
	t.Parallel()

	t.Run("enable rejects invalid schedule", func(t *testing.T) {
		s, mock := newBackupSchedulePGTestStore(t)
		policy := backupScheduleTestPolicy("policy_invalid_enable")
		policy.Enabled = false
		policy.Status = model.BackupPolicyStatusDisabled
		policy.Schedule = "not cron"
		policy.NextRunAt = nil
		expectGetBackupSchedulePolicy(mock, policy)

		if _, err := s.SetBackupPolicyEnabled(policy.ID, "", true, true, ""); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("expected invalid enable to return ErrInvalidInput, got %v", err)
		}
		assertBackupSchedulePGExpectations(t, mock)
	})

	t.Run("disable remains a kill switch", func(t *testing.T) {
		s, mock := newBackupSchedulePGTestStore(t)
		policy := backupScheduleTestPolicy("policy_invalid_disable")
		policy.Schedule = "not cron"
		policy.NextRunAt = nil
		expectGetBackupSchedulePolicy(mock, policy)
		returned := policy
		returned.Enabled = false
		returned.Status = model.BackupPolicyStatusDisabled
		returned.DisabledReason = "operator kill switch"

		args := backupScheduleAnyArgs(29)
		args[13] = false
		args[14] = model.BackupPolicyStatusDisabled
		args[15] = returned.DisabledReason
		args[16] = policy.Schedule
		args[24] = nil
		args[28] = nil
		mock.ExpectQuery(backupPolicyUpsertQueryPattern()).
			WithArgs(args...).
			WillReturnRows(backupSchedulePolicyRows(returned))

		saved, err := s.SetBackupPolicyEnabled(policy.ID, "", true, false, returned.DisabledReason)
		if err != nil {
			t.Fatalf("disable invalid backup policy: %v", err)
		}
		if saved.Enabled || saved.Status != model.BackupPolicyStatusDisabled || saved.NextRunAt != nil {
			t.Fatalf("expected disabled invalid policy, got %+v", saved)
		}
		assertBackupSchedulePGExpectations(t, mock)
	})
}

type backupSixHourTimeArgument struct{}

func (backupSixHourTimeArgument) Match(value driver.Value) bool {
	timestamp, ok := value.(time.Time)
	return ok && !timestamp.IsZero() && timestamp.Minute() == 0 && timestamp.Second() == 0 && timestamp.Hour()%6 == 0
}

func newBackupSchedulePGTestStore(t *testing.T) (*Store, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &Store{databaseURL: "postgres://example", db: db, dbReady: true}, mock
}

func backupScheduleTestPolicy(id string) model.BackupPolicy {
	createdAt := time.Date(2026, time.July, 12, 0, 0, 0, 0, time.UTC)
	return model.NormalizeBackupPolicy(model.BackupPolicy{
		ID:        id,
		TenantID:  "tenant_backup",
		ProjectID: "project_backup",
		AppID:     "app_backup",
		Name:      id,
		Target: model.BackupTarget{
			Type:      model.BackupTargetAppDatabase,
			TenantID:  "tenant_backup",
			ProjectID: "project_backup",
			AppID:     "app_backup",
			Database:  "appdb",
		},
		BackendID:   "backup_backend_r2",
		Enabled:     true,
		Status:      model.BackupPolicyStatusActive,
		Schedule:    "0 */6 * * *",
		RetainCount: 3,
		Retention: model.BackupRetentionPolicy{
			RetainCount:   3,
			ProtectLatest: 3,
		},
		CreatedBy: "test",
		CreatedAt: createdAt,
		UpdatedAt: createdAt,
	})
}

func backupSchedulePolicyColumns() []string {
	return []string{
		"id", "tenant_id", "project_id", "app_id", "name", "slug", "scope", "target_json", "backend_id",
		"enabled", "status", "disabled_reason", "schedule", "retain_count", "retention_json", "version",
		"last_run_id", "last_successful_run_id", "last_run_at", "last_successful_at", "next_run_at", "created_by",
		"created_at", "updated_at",
	}
}

func backupSchedulePolicyRows(policies ...model.BackupPolicy) *sqlmock.Rows {
	rows := sqlmock.NewRows(backupSchedulePolicyColumns())
	for _, policy := range policies {
		policy = model.NormalizeBackupPolicy(policy)
		targetJSON, _ := json.Marshal(policy.Target)
		retentionJSON, _ := json.Marshal(policy.Retention)
		rows.AddRow(
			policy.ID,
			backupScheduleNullableString(policy.TenantID),
			backupScheduleNullableString(policy.ProjectID),
			backupScheduleNullableString(policy.AppID),
			policy.Name,
			policy.Slug,
			policy.Scope,
			targetJSON,
			backupScheduleNullableString(policy.BackendID),
			policy.Enabled,
			policy.Status,
			policy.DisabledReason,
			policy.Schedule,
			policy.RetainCount,
			retentionJSON,
			policy.Version,
			policy.LastRunID,
			policy.LastSuccessfulRunID,
			backupScheduleNullableTime(policy.LastRunAt),
			backupScheduleNullableTime(policy.LastSuccessfulAt),
			backupScheduleNullableTime(policy.NextRunAt),
			policy.CreatedBy,
			policy.CreatedAt,
			policy.UpdatedAt,
		)
	}
	return rows
}

func backupScheduleRunRows(runs ...model.BackupRun) *sqlmock.Rows {
	rows := sqlmock.NewRows(backupScheduleRunColumns())
	for _, run := range runs {
		run = model.NormalizeBackupRun(run)
		targetJSON, _ := json.Marshal(run.Target)
		rows.AddRow(
			run.ID,
			backupScheduleNullableString(run.PolicyID),
			backupScheduleNullableString(run.TenantID),
			backupScheduleNullableString(run.ProjectID),
			backupScheduleNullableString(run.AppID),
			targetJSON,
			backupScheduleNullableString(run.BackendID),
			run.Trigger,
			run.Version,
			run.Status,
			run.Attempt,
			run.RetryCount,
			run.RequestedByType,
			run.RequestedByID,
			run.LeaseOwner,
			backupScheduleNullableTime(run.LockedUntil),
			backupScheduleNullableTime(run.HeartbeatAt),
			run.BytesWritten,
			run.LogicalBytes,
			run.ArtifactCount,
			run.ErrorCode,
			run.ErrorMessage,
			backupScheduleNullableTime(run.NextRetryAt),
			run.CreatedAt,
			run.UpdatedAt,
			backupScheduleNullableTime(run.StartedAt),
			backupScheduleNullableTime(run.FinishedAt),
		)
	}
	return rows
}

func backupScheduleArtifactRows(artifacts ...model.BackupArtifact) *sqlmock.Rows {
	columns := []string{
		"id", "run_id", "policy_id", "tenant_id", "project_id", "app_id", "target_json", "backend_id", "kind", "version",
		"object_key", "manifest_object_key", "sha256", "size_bytes", "logical_bytes", "status", "protected", "billable",
		"billing_class", "manifest_digest", "manifest_json", "created_at", "deleted_at",
	}
	rows := sqlmock.NewRows(columns)
	for _, artifact := range artifacts {
		artifact = model.NormalizeBackupArtifact(artifact)
		targetJSON, _ := json.Marshal(artifact.Target)
		manifestJSON, _ := json.Marshal(artifact.Manifest)
		rows.AddRow(
			artifact.ID,
			backupScheduleNullableString(artifact.RunID),
			backupScheduleNullableString(artifact.PolicyID),
			backupScheduleNullableString(artifact.TenantID),
			backupScheduleNullableString(artifact.ProjectID),
			backupScheduleNullableString(artifact.AppID),
			targetJSON,
			backupScheduleNullableString(artifact.BackendID),
			artifact.Kind,
			artifact.Version,
			artifact.ObjectKey,
			artifact.ManifestObjectKey,
			artifact.SHA256,
			artifact.SizeBytes,
			artifact.LogicalBytes,
			artifact.Status,
			artifact.Protected,
			artifact.Billable,
			artifact.BillingClass,
			artifact.ManifestDigest,
			manifestJSON,
			artifact.CreatedAt,
			backupScheduleNullableTime(artifact.DeletedAt),
		)
	}
	return rows
}

func backupScheduleRunColumns() []string {
	return []string{
		"id", "policy_id", "tenant_id", "project_id", "app_id", "target_json", "backend_id", "trigger", "version",
		"status", "attempt", "retry_count", "requested_by_type", "requested_by_id", "lease_owner", "locked_until",
		"heartbeat_at", "bytes_written", "logical_bytes", "artifact_count", "error_code", "error_message", "next_retry_at",
		"created_at", "updated_at", "started_at", "finished_at",
	}
}

func expectNullBackupScheduleRepairQuery(mock sqlmock.Sqlmock, limit int, policies ...model.BackupPolicy) {
	mock.ExpectQuery(`(?s)SELECT .* FROM fugue_backup_policies.*next_run_at IS NULL.*LIMIT \$1`).
		WithArgs(limit).
		WillReturnRows(backupSchedulePolicyRows(policies...))
}

func expectDueBackupScheduleQuery(mock sqlmock.Sqlmock, now time.Time, limit int, rows *sqlmock.Rows) {
	mock.ExpectQuery(`(?s)SELECT .* FROM fugue_backup_policies.*next_run_at IS NOT NULL AND next_run_at <= \$1.*LIMIT \$2`).
		WithArgs(now, limit).
		WillReturnRows(rows)
}

func expectGetBackupSchedulePolicy(mock sqlmock.Sqlmock, policy model.BackupPolicy) {
	mock.ExpectQuery(`(?s)SELECT .* FROM fugue_backup_policies WHERE \(id = \$1 OR name = \$1 OR slug = \$2\)`).
		WithArgs(policy.ID, sqlmock.AnyArg()).
		WillReturnRows(backupSchedulePolicyRows(policy))
}

func backupPolicyUpsertQueryPattern() string {
	return `(?s)INSERT INTO fugue_backup_policies .*next_run_at = CASE.*schedule IS DISTINCT FROM EXCLUDED.schedule THEN \$29.*RETURNING`
}

func backupScheduleAnyArgs(count int) []driver.Value {
	args := make([]driver.Value, count)
	for idx := range args {
		args[idx] = sqlmock.AnyArg()
	}
	return args
}

func backupScheduleNullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func backupScheduleNullableTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return *value
}

func assertBackupSchedulePGExpectations(t *testing.T, mock sqlmock.Sqlmock) {
	t.Helper()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}
