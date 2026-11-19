package api

import (
	"context"

	"fugue/internal/model"
)

// Apply timing evidence only to response copies, never to an operation that
// may be used to fence a controller write. The SQL implementation batches the
// authorized page rather than looking up each operation independently.
func (s *Server) projectFirstDeployStarts(ctx context.Context, ops []model.Operation) {
	ids := make([]string, 0, len(ops))
	for _, op := range ops {
		if op.Type == model.OperationTypeDeploy && op.ExecutionMode == model.ExecutionModeManaged {
			ids = append(ids, op.ID)
		}
	}
	starts, err := s.store.FirstDeployExecutionStarts(ctx, ids)
	if err != nil {
		if s.log != nil {
			s.log.Printf("operation timing evidence unavailable: %v", err)
		}
		return
	}
	for i := range ops {
		op := &ops[i]
		at := starts[op.ID]
		if !at.IsZero() && !at.Before(op.CreatedAt) && (op.StartedAt == nil || at.Before(*op.StartedAt)) &&
			(op.CompletedAt == nil || !at.After(*op.CompletedAt)) {
			op.StartedAt = &at
		}
	}
}
