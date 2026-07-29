package store

import (
	"strings"
	"testing"
	"time"

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
