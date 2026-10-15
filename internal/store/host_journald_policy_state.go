package store

import (
	"context"
	"time"

	"fugue/internal/model"
)

// ReadHostJournaldPolicyTaskState reads only the scheduler's inputs. InFlight
// includes older tasks and inactive/missing updaters, preserving the fleet-wide
// mutation guard; Latest has deterministic created_at/id tie breaking.
func (s *Store) ReadHostJournaldPolicyTaskState(ctx context.Context) (latest map[string]model.NodeUpdateTask, inFlight bool, err error) {
	started := time.Now()
	defer func() { s.recordRead(journaldReadMetric, started, len(latest), err) }()
	latest = make(map[string]model.NodeUpdateTask)
	if err = ctx.Err(); err != nil {
		return
	}
	if s.usingDatabase() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		rows, queryErr := s.db.QueryContext(ctx, hostJournaldPolicyTaskStateSQL, model.NodeUpdateTaskTypeReconcileHostJournaldPolicy)
		if queryErr != nil {
			err = mapDBErr(queryErr)
			return
		}
		defer rows.Close()
		for rows.Next() {
			var task model.NodeUpdateTask
			var policyHash string
			if err = rows.Scan(&task.ID, &task.NodeUpdaterID, &task.Status, &policyHash, &task.RequestedByID, &task.CreatedAt, &task.UpdatedAt, &task.CompletedAt, &inFlight); err != nil {
				return
			}
			task.Type = model.NodeUpdateTaskTypeReconcileHostJournaldPolicy
			task.Payload = map[string]string{"policy_hash": policyHash}
			latest[task.NodeUpdaterID] = task
		}
		err = rows.Err()
		return
	}
	err = s.withLockedState(false, func(state *model.State) error {
		for _, task := range state.NodeUpdateTasks {
			if task.Type != model.NodeUpdateTaskTypeReconcileHostJournaldPolicy {
				continue
			}
			if task.Status == model.NodeUpdateTaskStatusPending || task.Status == model.NodeUpdateTaskStatusRunning {
				inFlight = true
			}
			previous, exists := latest[task.NodeUpdaterID]
			if !exists || task.CreatedAt.After(previous.CreatedAt) || (task.CreatedAt.Equal(previous.CreatedAt) && task.ID > previous.ID) {
				task.Payload = map[string]string{"policy_hash": task.Payload["policy_hash"]}
				task.Logs = nil
				latest[task.NodeUpdaterID] = task
			}
		}
		return ctx.Err()
	})
	return
}

const hostJournaldPolicyTaskStateSQL = `SELECT DISTINCT ON (node_updater_id)
 id, node_updater_id, status, COALESCE(payload_json->>'policy_hash', ''),
 requested_by_id, created_at, updated_at, completed_at,
 EXISTS (SELECT 1 FROM fugue_node_update_tasks
         WHERE task_type = $1 AND status IN ('pending', 'running'))
FROM fugue_node_update_tasks
WHERE task_type = $1
ORDER BY node_updater_id, created_at DESC, id DESC`
