package store

import (
	"context"
	"time"

	"fugue/internal/store/internal/operationread"
)

// FirstDeployExecutionStarts is the read-only, already-authorized timing port.
func (s *Store) FirstDeployExecutionStarts(ctx context.Context, ids []string) (map[string]time.Time, error) {
	return operationread.FirstDeployExecutionStarts(ctx, s.db, ids)
}
