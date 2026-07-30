package store

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPGBackupArtifactPhysicalCleanupIsRestoreSafeAndDurable(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	stateStore := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	cutoff := time.Date(2026, time.July, 29, 8, 0, 0, 0, time.UTC)

	mock.ExpectQuery(`(?s)FROM fugue_backup_artifacts WHERE status IN \('deleted', 'expired'\).*physical_deleted_at IS NULL.*NOT EXISTS \(.*fugue_backup_restore_plans.*status NOT IN \('succeeded', 'failed', 'canceled'\).*NOT EXISTS \(.*fugue_backup_restore_runs.*ORDER BY physical_delete_attempted_at ASC NULLS FIRST, deleted_at ASC, id ASC LIMIT \$2`).
		WithArgs(cutoff, 20).
		WillReturnRows(sqlmock.NewRows(strings.Split(backupArtifactReturningColumns(), ", ")))
	candidates, err := stateStore.ListBackupArtifactCleanupCandidates(BackupArtifactCleanupFilter{Before: cutoff, Limit: 20})
	if err != nil {
		t.Fatalf("list physical cleanup candidates: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("unexpected candidates: %#v", candidates)
	}

	completedAt := cutoff.Add(time.Minute)
	mock.ExpectExec(`UPDATE fugue_backup_artifacts SET physical_deleted_at = COALESCE\(physical_deleted_at, \$2\), physical_delete_attempted_at = \$2, physical_delete_error = '' WHERE id = \$1 AND status IN \('deleted', 'expired'\) AND protected = FALSE`).
		WithArgs("artifact-1", completedAt).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := stateStore.MarkBackupArtifactPhysicalDeleted("artifact-1", completedAt); err != nil {
		t.Fatalf("mark physical cleanup: %v", err)
	}

	failureAt := completedAt.Add(time.Minute)
	mock.ExpectExec(`UPDATE fugue_backup_artifacts SET physical_delete_attempted_at = \$2, physical_delete_error = \$3 WHERE id = \$1 AND physical_deleted_at IS NULL AND status IN \('deleted', 'expired'\) AND protected = FALSE`).
		WithArgs("artifact-2", failureAt, "access denied").
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := stateStore.RecordBackupArtifactPhysicalCleanupFailure("artifact-2", failureAt, "access denied"); err != nil {
		t.Fatalf("record physical cleanup failure: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestPGFailedBackupRunObjectCleanupIsDurableAndArtifactSafe(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	stateStore := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	cutoff := time.Date(2026, time.July, 29, 8, 0, 0, 0, time.UTC)

	mock.ExpectQuery(`(?s)FROM fugue_backup_runs WHERE status IN \('failed', 'canceled'\).*COALESCE\(finished_at, updated_at\) <= \$1.*orphan_cleanup_at IS NULL.*NOT EXISTS \(.*fugue_backup_artifacts.*ORDER BY orphan_cleanup_attempted_at ASC NULLS FIRST`).
		WithArgs(cutoff, 20).
		WillReturnRows(sqlmock.NewRows(strings.Split(backupRunReturningColumns(), ", ")))
	runs, err := stateStore.ListFailedBackupRunObjectCleanupCandidates(BackupRunObjectCleanupFilter{Before: cutoff, Limit: 20})
	if err != nil {
		t.Fatalf("list failed-run cleanup candidates: %v", err)
	}
	if len(runs) != 0 {
		t.Fatalf("unexpected failed-run cleanup candidates: %#v", runs)
	}

	mock.ExpectQuery(`SELECT EXISTS \(SELECT 1 FROM fugue_backup_artifacts WHERE run_id = \$1\)`).
		WithArgs("run-no-artifact").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	exists, err := stateStore.BackupArtifactExistsForRun("run-no-artifact")
	if err != nil || exists {
		t.Fatalf("artifact existence check = %t, %v", exists, err)
	}

	completedAt := cutoff.Add(time.Minute)
	mock.ExpectExec(`UPDATE fugue_backup_runs SET orphan_cleanup_at = COALESCE\(orphan_cleanup_at, \$2\),.*NOT EXISTS \(.*fugue_backup_artifacts`).
		WithArgs("run-no-artifact", completedAt).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := stateStore.MarkBackupRunObjectsCleaned("run-no-artifact", completedAt); err != nil {
		t.Fatalf("mark failed-run cleanup: %v", err)
	}

	failureAt := completedAt.Add(time.Minute)
	mock.ExpectExec(`UPDATE fugue_backup_runs SET orphan_cleanup_attempted_at = \$2,.*orphan_cleanup_error = \$3.*NOT EXISTS \(.*fugue_backup_artifacts`).
		WithArgs("run-failed-cleanup", failureAt, "access denied").
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := stateStore.RecordBackupRunObjectCleanupFailure("run-failed-cleanup", failureAt, "access denied"); err != nil {
		t.Fatalf("record failed-run cleanup failure: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestPGListBackupUsageArtifactsIsUnpaginatedAndIncludesPhysicalState(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	stateStore := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	createdAt := time.Date(2026, time.July, 30, 1, 0, 0, 0, time.UTC)
	deletedAt := createdAt.Add(time.Hour)
	physicalDeletedAt := deletedAt.Add(time.Hour)
	physicalAttemptedAt := physicalDeletedAt.Add(-time.Minute)
	artifact := model.NormalizeBackupArtifact(model.BackupArtifact{
		ID:                "artifact_usage_pg",
		RunID:             "backup_run_usage_pg",
		TenantID:          "tenant_usage_pg",
		Target:            model.BackupTarget{Type: model.BackupTargetAppDatabase, TenantID: "tenant_usage_pg", AppID: "app_usage_pg"},
		BackendID:         "backend_usage_pg",
		Kind:              model.BackupArtifactKindAppPGDump,
		ObjectKey:         "apps/tenant_usage_pg/project/app_usage_pg/backup_run_usage_pg/database.dump",
		ManifestObjectKey: "apps/tenant_usage_pg/project/app_usage_pg/backup_run_usage_pg/manifest.json",
		SizeBytes:         123,
		Status:            model.BackupArtifactStatusDeleted,
		CreatedAt:         createdAt,
		DeletedAt:         &deletedAt,
	})

	mock.ExpectQuery(`(?s)SELECT .*physical_deleted_at, physical_delete_attempted_at, physical_delete_error FROM fugue_backup_artifacts WHERE tenant_id = \$1 ORDER BY created_at ASC, id ASC$`).
		WithArgs(artifact.TenantID).
		WillReturnRows(backupUsageArtifactRows(physicalDeletedAt, physicalAttemptedAt, "", artifact))

	artifacts, err := stateStore.ListBackupUsageArtifacts(artifact.TenantID, false)
	if err != nil {
		t.Fatalf("list backup usage artifacts: %v", err)
	}
	if len(artifacts) != 1 || artifacts[0].Artifact.ID != artifact.ID {
		t.Fatalf("unexpected artifacts: %+v", artifacts)
	}
	if artifacts[0].PhysicalDeletedAt == nil || !artifacts[0].PhysicalDeletedAt.Equal(physicalDeletedAt) || artifacts[0].PhysicalDeleteAttemptedAt == nil || !artifacts[0].PhysicalDeleteAttemptedAt.Equal(physicalAttemptedAt) {
		t.Fatalf("physical state was not scanned: %+v", artifacts[0])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestPGBackupArtifactRestoreMutationInterlockUsesArtifactRowLock(t *testing.T) {
	t.Parallel()

	createdAt := time.Date(2026, time.July, 29, 9, 0, 0, 0, time.UTC)
	artifact := model.NormalizeBackupArtifact(model.BackupArtifact{
		ID:        "artifact_restore_interlock",
		TenantID:  "tenant_restore_interlock",
		Target:    model.BackupTarget{Type: model.BackupTargetAppDatabase, TenantID: "tenant_restore_interlock", AppID: "app_restore_interlock"},
		Kind:      model.BackupArtifactKindAppPGDump,
		Status:    model.BackupArtifactStatusActive,
		CreatedAt: createdAt,
	})
	plan := model.NormalizeBackupRestorePlan(model.BackupRestorePlan{
		ID:         "restore_plan_interlock",
		ArtifactID: artifact.ID,
		TenantID:   artifact.TenantID,
		AppID:      artifact.Target.AppID,
		Target:     artifact.Target,
		Mode:       model.BackupRestoreModePlanOnly,
		Status:     model.BackupRestoreStatusPlanned,
		CreatedAt:  createdAt.Add(time.Minute),
		UpdatedAt:  createdAt.Add(time.Minute),
	})

	t.Run("restore plan locks active artifact before insert", func(t *testing.T) {
		s, mock := newBackupSchedulePGTestStore(t)
		mock.ExpectBegin()
		mock.ExpectQuery(`(?s)SELECT .* FROM fugue_backup_artifacts WHERE id = \$1 FOR UPDATE`).
			WithArgs(artifact.ID).
			WillReturnRows(backupScheduleArtifactRows(artifact))
		mock.ExpectQuery(`(?s)INSERT INTO fugue_backup_restore_plans .*RETURNING`).
			WillReturnRows(backupGCRestorePlanRows(plan))
		mock.ExpectCommit()

		created, err := s.CreateBackupRestorePlan(plan)
		if err != nil {
			t.Fatalf("create restore plan: %v", err)
		}
		if created.ID != plan.ID || created.ArtifactID != artifact.ID {
			t.Fatalf("unexpected restore plan: %+v", created)
		}
		assertBackupSchedulePGExpectations(t, mock)
	})

	t.Run("restore plan rejects deleted artifact under lock", func(t *testing.T) {
		s, mock := newBackupSchedulePGTestStore(t)
		deleted := artifact
		deleted.Status = model.BackupArtifactStatusDeleted
		deletedAt := createdAt.Add(time.Minute)
		deleted.DeletedAt = &deletedAt
		mock.ExpectBegin()
		mock.ExpectQuery(`(?s)SELECT .* FROM fugue_backup_artifacts WHERE id = \$1 FOR UPDATE`).
			WithArgs(artifact.ID).
			WillReturnRows(backupScheduleArtifactRows(deleted))
		mock.ExpectRollback()

		if _, err := s.CreateBackupRestorePlan(plan); !errors.Is(err, ErrConflict) {
			t.Fatalf("restore plan error = %v, want conflict", err)
		}
		assertBackupSchedulePGExpectations(t, mock)
	})

	t.Run("delete rejects in-flight restore after locking artifact", func(t *testing.T) {
		s, mock := newBackupSchedulePGTestStore(t)
		mock.ExpectBegin()
		mock.ExpectQuery(`(?s)SELECT .* FROM fugue_backup_artifacts WHERE id = \$1.*FOR UPDATE`).
			WithArgs(artifact.ID, artifact.TenantID).
			WillReturnRows(backupScheduleArtifactRows(artifact))
		mock.ExpectQuery(`(?s)SELECT EXISTS \(.*fugue_backup_restore_plans.*UNION ALL.*fugue_backup_restore_runs.*\)`).
			WithArgs(artifact.ID).
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
		mock.ExpectRollback()

		if _, err := s.MarkBackupArtifactDeleted(artifact.ID, artifact.TenantID, false); !errors.Is(err, ErrConflict) {
			t.Fatalf("delete artifact error = %v, want conflict", err)
		}
		assertBackupSchedulePGExpectations(t, mock)
	})

	t.Run("delete locks and updates artifact when no restore is active", func(t *testing.T) {
		s, mock := newBackupSchedulePGTestStore(t)
		deleted := artifact
		deleted.Status = model.BackupArtifactStatusDeleted
		deletedAt := createdAt.Add(2 * time.Minute)
		deleted.DeletedAt = &deletedAt
		mock.ExpectBegin()
		mock.ExpectQuery(`(?s)SELECT .* FROM fugue_backup_artifacts WHERE id = \$1.*FOR UPDATE`).
			WithArgs(artifact.ID, artifact.TenantID).
			WillReturnRows(backupScheduleArtifactRows(artifact))
		mock.ExpectQuery(`(?s)SELECT EXISTS \(.*fugue_backup_restore_plans.*UNION ALL.*fugue_backup_restore_runs.*\)`).
			WithArgs(artifact.ID).
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
		mock.ExpectQuery(`(?s)UPDATE fugue_backup_artifacts.*SET status = 'deleted'.*WHERE id = \$1 AND status = 'active' AND protected = FALSE.*RETURNING`).
			WithArgs(artifact.ID, sqlmock.AnyArg()).
			WillReturnRows(backupScheduleArtifactRows(deleted))
		mock.ExpectCommit()

		got, err := s.MarkBackupArtifactDeleted(artifact.ID, artifact.TenantID, false)
		if err != nil {
			t.Fatalf("delete artifact: %v", err)
		}
		if got.Status != model.BackupArtifactStatusDeleted {
			t.Fatalf("unexpected deleted artifact: %+v", got)
		}
		assertBackupSchedulePGExpectations(t, mock)
	})
}

func backupGCRestorePlanRows(plans ...model.BackupRestorePlan) *sqlmock.Rows {
	rows := sqlmock.NewRows(strings.Split(backupRestorePlanReturningColumns(), ", "))
	for _, plan := range plans {
		plan = model.NormalizeBackupRestorePlan(plan)
		targetJSON, _ := json.Marshal(plan.Target)
		warningsJSON, _ := json.Marshal(plan.Warnings)
		phasesJSON, _ := json.Marshal(plan.Phases)
		rows.AddRow(
			plan.ID,
			backupScheduleNullableString(plan.TenantID),
			backupScheduleNullableString(plan.ProjectID),
			backupScheduleNullableString(plan.AppID),
			plan.ArtifactID,
			targetJSON,
			plan.Mode,
			plan.Status,
			warningsJSON,
			phasesJSON,
			plan.CreatedByType,
			plan.CreatedByID,
			plan.CreatedAt,
			plan.UpdatedAt,
		)
	}
	return rows
}

func backupUsageArtifactRows(physicalDeletedAt, physicalAttemptedAt time.Time, physicalError string, artifacts ...model.BackupArtifact) *sqlmock.Rows {
	columns := []string{"id", "run_id", "tenant_id", "backend_id", "kind", "object_key", "manifest_object_key", "size_bytes", "status", "protected", "billable", "created_at", "deleted_at", "physical_deleted_at", "physical_delete_attempted_at", "physical_delete_error"}
	rows := sqlmock.NewRows(columns)
	for _, artifact := range artifacts {
		artifact = model.NormalizeBackupArtifact(artifact)
		rows.AddRow(
			artifact.ID,
			backupScheduleNullableString(artifact.RunID),
			backupScheduleNullableString(artifact.TenantID),
			backupScheduleNullableString(artifact.BackendID),
			artifact.Kind,
			artifact.ObjectKey,
			artifact.ManifestObjectKey,
			artifact.SizeBytes,
			artifact.Status,
			artifact.Protected,
			artifact.Billable,
			artifact.CreatedAt,
			backupScheduleNullableTime(artifact.DeletedAt),
			physicalDeletedAt,
			physicalAttemptedAt,
			physicalError,
		)
	}
	return rows
}
