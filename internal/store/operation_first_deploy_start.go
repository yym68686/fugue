package store

import (
	"context"
	"time"

	"fugue/internal/model"
	"fugue/internal/store/internal/operationread"
)

// FirstDeployExecutionStarts is the read-only, already-authorized timing port.
func (s *Store) FirstDeployExecutionStarts(ctx context.Context, ids []string) (map[string]time.Time, error) {
	if s.usingDatabase() {
		return operationread.FirstDeployExecutionStarts(ctx, s.db, nil, ids)
	}
	return operationread.FirstDeployExecutionStarts(ctx, nil, func(visit func([]model.ReleaseStep)) error {
		return s.withLockedState(false, func(state *model.State) error {
			visit(state.ReleaseSteps)
			return nil
		})
	}, ids)
}
