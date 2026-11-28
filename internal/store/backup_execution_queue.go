package store

import (
	"context"
	"time"

	"fugue/internal/model"
)

const maxUnclaimedBackupExecutionScan = 500

// ListUnclaimedBackupExecutionIDs discovers initial, ordinary backup runs that
// can be dispatched after the API has durably accepted a request. It does not
// claim or recover work. The executor must still call ClaimBackupRun before any
// side effect. Keep coordinated work and retries on their separate scans so an
// unavailable coordination Lease or a not-yet-due retry cannot block this lane.
func (s *Store) ListUnclaimedBackupExecutionIDs(ctx context.Context, now time.Time, limit int) ([]string, error) {
	if limit <= 0 || limit > maxUnclaimedBackupExecutionScan || now.IsZero() {
		return nil, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `
SELECT id
FROM fugue_backup_runs
WHERE status = 'pending'
  AND trigger <> 'retry'
  AND lease_owner = '' AND locked_until IS NULL AND started_at IS NULL
  AND target_type <> $1 AND target_json->>'engine' IS DISTINCT FROM $2
  AND (next_retry_at IS NULL OR next_retry_at <= $3)
ORDER BY created_at, id
LIMIT $4`, model.BackupTargetControlPlaneDatabase, model.BackupEngineLonghornSnapshot, now.UTC(), limit)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	ids := make([]string, 0, limit)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, mapDBErr(err)
	}
	return ids, nil
}

// ListExpiredBackupExecutions finds only previously claimed work. Pending
// requests may wait longer than the claim TTL during a worker outage. Do not
// mark them stale or let them fill the limited recovery projection. An expired
// execution is evidence to reconcile, not permission to repeat its effects.
func (s *Store) ListExpiredBackupExecutions(ctx context.Context, now time.Time, leaseTTL time.Duration, limit int) ([]model.BackupRun, error) {
	if now.IsZero() || leaseTTL < time.Microsecond || limit <= 0 || limit > maxUnclaimedBackupExecutionScan {
		return nil, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	deadline := `COALESCE(locked_until, COALESCE(heartbeat_at, updated_at) + ($2 * INTERVAL '1 microsecond'))`
	rows, err := s.db.QueryContext(ctx, backupRunSelectSQL()+`
WHERE status = 'running' AND `+deadline+` < $1
ORDER BY `+deadline+`, created_at, id
LIMIT $3`, now.UTC(), leaseTTL.Microseconds(), limit)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	runs := make([]model.BackupRun, 0, limit)
	for rows.Next() {
		run, err := scanBackupRun(rows)
		if err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, mapDBErr(err)
	}
	return runs, nil
}
