package store

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestBackupArtifactRestoreDeleteInterlockPostgres(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("FUGUE_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set FUGUE_TEST_DATABASE_URL to run backup artifact restore/delete interlock integration test")
	}
	if !strings.Contains(databaseURL, "fugue-pgtest") && !strings.Contains(databaseURL, "fugue_test") {
		t.Fatalf("refusing to run backup artifact interlock test against non-test database URL %q", databaseURL)
	}

	parsedURL, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatalf("parse test database URL: %v", err)
	}
	testSuffix := model.NewID("backup_gc_interlock")
	applicationName := "fugue_backup_gc_interlock_" + testSuffix[len(testSuffix)-12:]
	query := parsedURL.Query()
	query.Set("application_name", applicationName)
	parsedURL.RawQuery = query.Encode()

	stateStore := New("", parsedURL.String())
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init postgres store: %v", err)
	}
	t.Cleanup(func() { _ = stateStore.db.Close() })

	artifactIDs := make([]string, 0, 2)
	t.Cleanup(func() {
		for _, artifactID := range artifactIDs {
			_, _ = stateStore.db.Exec(`DELETE FROM fugue_backup_artifacts WHERE id = $1`, artifactID)
		}
	})
	createArtifact := func(label string) model.BackupArtifact {
		t.Helper()
		artifact, err := stateStore.CreateBackupArtifact(model.BackupArtifact{
			ID:     testSuffix + "_" + label,
			Target: model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase, Component: "control-plane-postgres"},
			Kind:   model.BackupArtifactKindControlPlanePGDump,
			Status: model.BackupArtifactStatusActive,
		})
		if err != nil {
			t.Fatalf("create %s artifact: %v", label, err)
		}
		artifactIDs = append(artifactIDs, artifact.ID)
		return artifact
	}

	t.Run("committed restore plan makes waiting delete conflict", func(t *testing.T) {
		artifact := createArtifact("plan_wins")
		planID := testSuffix + "_plan"
		tx, err := stateStore.db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatalf("begin plan transaction: %v", err)
		}
		defer tx.Rollback()
		var lockedID string
		if err := tx.QueryRow(`SELECT id FROM fugue_backup_artifacts WHERE id = $1 FOR UPDATE`, artifact.ID).Scan(&lockedID); err != nil {
			t.Fatalf("lock artifact for plan: %v", err)
		}
		targetJSON, err := json.Marshal(artifact.Target)
		if err != nil {
			t.Fatalf("marshal plan target: %v", err)
		}
		now := time.Now().UTC()
		if _, err := tx.Exec(`
INSERT INTO fugue_backup_restore_plans (id, artifact_id, target_type, target_json, mode, status, warnings_json, phases_json, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, '[]'::jsonb, '[]'::jsonb, $7, $7)`,
			planID, artifact.ID, artifact.Target.Type, targetJSON, model.BackupRestoreModePlanOnly, model.BackupRestoreStatusPlanned, now); err != nil {
			t.Fatalf("insert restore plan under artifact lock: %v", err)
		}

		deleteResult := make(chan error, 1)
		go func() {
			_, err := stateStore.MarkBackupArtifactDeleted(artifact.ID, "", true)
			deleteResult <- err
		}()
		waitForBackupArtifactLockWait(t, stateStore, applicationName)
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit restore plan: %v", err)
		}
		if err := <-deleteResult; !errors.Is(err, ErrConflict) {
			t.Fatalf("waiting delete error = %v, want conflict", err)
		}
		persisted, err := stateStore.GetBackupArtifact(artifact.ID, "", true)
		if err != nil {
			t.Fatalf("get restore-protected artifact: %v", err)
		}
		if persisted.Status != model.BackupArtifactStatusActive || persisted.DeletedAt != nil {
			t.Fatalf("restore-protected artifact changed: %+v", persisted)
		}
	})

	t.Run("committed delete makes waiting restore plan conflict", func(t *testing.T) {
		artifact := createArtifact("delete_wins")
		tx, err := stateStore.db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatalf("begin delete transaction: %v", err)
		}
		defer tx.Rollback()
		deletedAt := time.Now().UTC()
		if _, err := tx.Exec(`
UPDATE fugue_backup_artifacts
SET status = 'deleted', deleted_at = $2
WHERE id = $1`, artifact.ID, deletedAt); err != nil {
			t.Fatalf("delete artifact under row lock: %v", err)
		}

		planResult := make(chan error, 1)
		go func() {
			_, err := stateStore.CreateBackupRestorePlan(model.BackupRestorePlan{
				ID:         testSuffix + "_rejected_plan",
				ArtifactID: artifact.ID,
				Target:     artifact.Target,
				Mode:       model.BackupRestoreModePlanOnly,
				Status:     model.BackupRestoreStatusPlanned,
			})
			planResult <- err
		}()
		waitForBackupArtifactLockWait(t, stateStore, applicationName)
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit artifact delete: %v", err)
		}
		if err := <-planResult; !errors.Is(err, ErrConflict) {
			t.Fatalf("waiting restore plan error = %v, want conflict", err)
		}
		var planCount int
		if err := stateStore.db.QueryRow(`SELECT COUNT(*) FROM fugue_backup_restore_plans WHERE artifact_id = $1`, artifact.ID).Scan(&planCount); err != nil {
			t.Fatalf("count rejected restore plans: %v", err)
		}
		if planCount != 0 {
			t.Fatalf("deleted artifact gained %d restore plans", planCount)
		}
	})
}

func waitForBackupArtifactLockWait(t *testing.T, stateStore *Store, applicationName string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int
		err := stateStore.db.QueryRow(`
SELECT COUNT(*)
FROM pg_stat_activity
WHERE application_name = $1
  AND pid <> pg_backend_pid()
  AND state = 'active'
  AND wait_event_type = 'Lock'
  AND query LIKE '%fugue_backup_artifacts%'`, applicationName).Scan(&waiting)
		if err != nil {
			t.Fatalf("inspect postgres lock wait: %v", err)
		}
		if waiting > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for backup artifact row-lock contention")
}
