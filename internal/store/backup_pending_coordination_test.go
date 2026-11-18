package store

import (
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestListPendingCoordinatedBackupRunsFiltersBeforeLimit(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "state.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	future := now.Add(time.Hour)
	past := now.Add(-time.Hour)
	base := model.BackupRun{
		Target: model.BackupTarget{Type: model.BackupTargetAppDatabase, Engine: model.BackupEngineLonghornSnapshot},
		Status: model.BackupRunStatusPending, Trigger: model.BackupRunTriggerScheduled,
		CreatedAt: past, UpdatedAt: past,
	}
	oldest := base
	oldest.ID = "a-oldest-snapshot"
	sameTime := base
	sameTime.ID = "b-same-time-snapshot"
	control := base
	control.ID = "control-plane"
	control.Target = model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase}
	control.Trigger = model.BackupRunTriggerManual
	control.CreatedAt = now
	control.NextRetryAt = &past

	err := s.withLockedState(true, func(state *model.State) error {
		state.BackupRuns = nil
		for _, excluded := range []string{"future", "retry-due", "retry-future", "logical", "running", "failed", "canceled", "blocked"} {
			run := base
			run.ID = excluded
			run.CreatedAt = past.Add(-time.Hour)
			switch excluded {
			case "future":
				run.NextRetryAt = &future
			case "retry-due", "retry-future":
				run.Trigger = model.BackupRunTriggerRetry
				run.NextRetryAt = &past
				if excluded == "retry-future" {
					run.NextRetryAt = &future
				}
			case "logical":
				run.Target.Engine = model.BackupEngineLogicalPGDump
			default:
				run.Status = excluded
			}
			state.BackupRuns = append(state.BackupRuns, run)
		}
		state.BackupRuns = append(state.BackupRuns, control, sameTime, oldest)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int{1, 2, 0} {
		runs, err := s.ListPendingCoordinatedBackupRuns(now, limit)
		if err != nil {
			t.Fatal(err)
		}
		want := []string{oldest.ID, sameTime.ID, control.ID}
		if limit > 0 {
			want = want[:limit]
		}
		if len(runs) != len(want) {
			t.Fatalf("limit=%d: unexpected runs %+v", limit, runs)
		}
		for i, id := range want {
			if runs[i].ID != id || !model.BackupRunNeedsCoordinationLease(runs[i]) {
				t.Fatalf("limit=%d: wrong ordered eligible run: %+v", limit, runs[i])
			}
		}
	}
}

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
