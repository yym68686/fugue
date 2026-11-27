package store

import (
	"regexp"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestPostgresBackupListQueriesUseCursorTupleAndStableOrder(t *testing.T) {
	t.Run("runs", func(t *testing.T) {
		stateStore, mock := newBackupSchedulePGTestStore(t)
		cursor := BackupListCursor{
			CreatedAt: time.Date(2026, 7, 31, 1, 2, 3, 4000, time.UTC),
			ID:        "backup_run_cursor",
		}
		query := backupRunSelectSQL() + ` WHERE status = $1 AND (created_at < $2 OR (created_at = $2 AND id < $3)) ORDER BY created_at DESC, id DESC LIMIT $4`
		mock.ExpectQuery(regexp.QuoteMeta(query)).
			WithArgs(model.BackupRunStatusFailed, cursor.CreatedAt, cursor.ID, 2).
			WillReturnRows(backupScheduleRunRows())

		if _, err := stateStore.ListBackupRuns(BackupRunFilter{
			Status:        model.BackupRunStatusFailed,
			PlatformAdmin: true,
			Cursor:        &cursor,
			Limit:         2,
		}); err != nil {
			t.Fatalf("list postgres runs: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("postgres run pagination expectations: %v", err)
		}
	})

	t.Run("artifacts", func(t *testing.T) {
		stateStore, mock := newBackupSchedulePGTestStore(t)
		cursor := BackupListCursor{
			CreatedAt: time.Date(2026, 7, 31, 4, 5, 6, 7000, time.UTC),
			ID:        "backup_artifact_cursor",
		}
		query := backupArtifactSelectSQL() + ` WHERE status = 'active' AND (created_at < $1 OR (created_at = $1 AND id < $2)) ORDER BY created_at DESC, id DESC LIMIT $3`
		mock.ExpectQuery(regexp.QuoteMeta(query)).
			WithArgs(cursor.CreatedAt, cursor.ID, 3).
			WillReturnRows(backupScheduleArtifactRows())

		if _, err := stateStore.ListBackupArtifacts(BackupArtifactFilter{
			ActiveOnly:    true,
			PlatformAdmin: true,
			Cursor:        &cursor,
			Limit:         3,
		}); err != nil {
			t.Fatalf("list postgres artifacts: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("postgres artifact pagination expectations: %v", err)
		}
	})
}
