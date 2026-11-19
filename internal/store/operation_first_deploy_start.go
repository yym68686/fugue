package store

import (
	"context"
	"fmt"
	"time"

	"fugue/internal/model"
)

// FirstDeployExecutionStarts reads the first recorded execution for already
// authorized operation IDs. Operation.StartedAt inside the store is a per-claim
// fencing token and must never be replaced with a historical timestamp.
func (s *Store) FirstDeployExecutionStarts(ctx context.Context, ids []string) (map[string]time.Time, error) {
	starts := make(map[string]time.Time)
	if len(ids) == 0 {
		return starts, nil
	}
	if s.usingDatabase() {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		rows, err := s.db.QueryContext(ctx, `
SELECT operation_id, MIN(started_at)
FROM fugue_release_steps
WHERE operation_id <> '' AND operation_id = ANY($1::text[]) AND step_type = $2 AND status = $3
GROUP BY operation_id`, ids, model.ReleaseStepTypeDeployApply, model.ReleaseStepStatusRunning)
		if err != nil {
			return nil, fmt.Errorf("read first deployment execution: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			var at time.Time
			if err := rows.Scan(&id, &at); err != nil {
				return nil, err
			}
			starts[id] = at.UTC()
		}
		return starts, rows.Err()
	}
	requested := make(map[string]bool, len(ids))
	for _, id := range ids {
		requested[id] = true
	}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, step := range state.ReleaseSteps {
			if !requested[step.OperationID] || step.Type != model.ReleaseStepTypeDeployApply || step.Status != model.ReleaseStepStatusRunning {
				continue
			}
			if previous := starts[step.OperationID]; previous.IsZero() || step.StartedAt.Before(previous) {
				starts[step.OperationID] = step.StartedAt.UTC()
			}
		}
		return nil
	})
	return starts, err
}
