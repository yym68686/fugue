package store

import (
	"regexp"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestPGListPendingCoordinatedBackupRunsFiltersBeforeLimit(t *testing.T) {
	s, mock := newBackupSchedulePGTestStore(t)
	now := time.Now().UTC()
	run := model.NormalizeBackupRun(model.BackupRun{
		ID: "pending-snapshot", Target: model.BackupTarget{Type: model.BackupTargetAppDatabase, Engine: model.BackupEngineLonghornSnapshot},
		Status: model.BackupRunStatusPending, Trigger: model.BackupRunTriggerScheduled, CreatedAt: now.Add(-time.Hour), UpdatedAt: now,
	})
	query := backupRunSelectSQL() + `
 WHERE status = 'pending'
   AND trigger <> 'retry'
   AND (target_type = $1 OR target_json->>'engine' = $2)
   AND (next_retry_at IS NULL OR next_retry_at <= $3)
 ORDER BY created_at ASC, id ASC
 LIMIT $4`
	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(model.BackupTargetControlPlaneDatabase, model.BackupEngineLonghornSnapshot, now, 1).
		WillReturnRows(backupScheduleRunRows(run))
	runs, err := s.ListPendingCoordinatedBackupRuns(now, 1)
	if err != nil || len(runs) != 1 || runs[0].ID != run.ID {
		t.Fatalf("unexpected eligible SQL runs: %+v %v", runs, err)
	}
	assertBackupSchedulePGExpectations(t, mock)
}
