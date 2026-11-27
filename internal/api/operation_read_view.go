package api

import (
	"context"

	"fugue/internal/model"
	"fugue/internal/operationview"
)

func (s *Server) projectFirstDeployStarts(ctx context.Context, ops []model.Operation) {
	if err := operationview.ProjectFirstDeployStarts(ctx, s.store.OperationViews(), ops); err != nil && s.log != nil {
		s.log.Printf("operation timing evidence unavailable: %v", err)
	}
}
