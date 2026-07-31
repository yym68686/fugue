package store

import (
	"fmt"
	"path/filepath"
	"reflect"
	"regexp"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestBackupListCursorPagesStableTimestampsWithoutDuplicates(t *testing.T) {
	t.Parallel()

	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	base := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)
	if err := stateStore.withLockedState(true, func(state *model.State) error {
		for i := 0; i < 6; i++ {
			createdAt := base.Add(time.Duration(i/2) * time.Minute)
			id := fmt.Sprintf("backup_page_%02d", i)
			state.BackupRuns = append(state.BackupRuns, model.BackupRun{
				ID:        "run_" + id,
				Status:    model.BackupRunStatusBlocked,
				CreatedAt: createdAt,
			})
			state.BackupArtifacts = append(state.BackupArtifacts, model.BackupArtifact{
				ID:        "artifact_" + id,
				Status:    model.BackupArtifactStatusActive,
				CreatedAt: createdAt,
			})
		}
		return nil
	}); err != nil {
		t.Fatalf("seed backups: %v", err)
	}

	wantRuns := []string{
		"run_backup_page_05", "run_backup_page_04",
		"run_backup_page_03", "run_backup_page_02",
		"run_backup_page_01", "run_backup_page_00",
	}
	var gotRuns []string
	var runCursor *BackupListCursor
	for {
		page, err := stateStore.ListBackupRuns(BackupRunFilter{
			PlatformAdmin: true,
			Cursor:        runCursor,
			Limit:         2,
		})
		if err != nil {
			t.Fatalf("list run page: %v", err)
		}
		if len(page) == 0 {
			break
		}
		for _, run := range page {
			gotRuns = append(gotRuns, run.ID)
		}
		last := page[len(page)-1]
		runCursor = &BackupListCursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}
	if !reflect.DeepEqual(gotRuns, wantRuns) {
		t.Fatalf("run order mismatch:\n got: %v\nwant: %v", gotRuns, wantRuns)
	}

	wantArtifacts := []string{
		"artifact_backup_page_05", "artifact_backup_page_04",
		"artifact_backup_page_03", "artifact_backup_page_02",
		"artifact_backup_page_01", "artifact_backup_page_00",
	}
	var gotArtifacts []string
	var artifactCursor *BackupListCursor
	for {
		page, err := stateStore.ListBackupArtifacts(BackupArtifactFilter{
			PlatformAdmin: true,
			Cursor:        artifactCursor,
			Limit:         2,
		})
		if err != nil {
			t.Fatalf("list artifact page: %v", err)
		}
		if len(page) == 0 {
			break
		}
		for _, artifact := range page {
			gotArtifacts = append(gotArtifacts, artifact.ID)
		}
		last := page[len(page)-1]
		artifactCursor = &BackupListCursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}
	if !reflect.DeepEqual(gotArtifacts, wantArtifacts) {
		t.Fatalf("artifact order mismatch:\n got: %v\nwant: %v", gotArtifacts, wantArtifacts)
	}
}

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
