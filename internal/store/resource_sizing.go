package store

import (
	"context"
	"strings"

	"fugue/internal/model"
)

// RunAutomaticResourceSizing preserves the original PostgreSQL session lock
// across old and new API replicas. Callers cannot select a different key.
func (s *Store) RunAutomaticResourceSizing(fn func() error) (bool, error) {
	return s.WithAdvisoryLock(context.Background(), "background:right-sizing:auto-apply", fn)
}

func isAutomaticResourceSizingActor(op model.Operation) bool {
	if strings.TrimSpace(op.RequestedByType) != model.ActorTypeSystem {
		return false
	}
	switch strings.TrimSpace(op.RequestedByID) {
	case model.OperationRequestedByRightSizing, model.OperationRequestedByRightSizingDownscale:
		return true
	default:
		return false
	}
}

// EnqueueAutomaticResourceSizingRequest uses the same compare-and-swap and
// active-operation checks as an ordinary resize without letting a generic
// operation writer pose as the periodic resource-sizing job.
func (s *Store) EnqueueAutomaticResourceSizingRequest(op model.Operation, baseline model.AppSpec) (model.Operation, error) {
	if !isAutomaticResourceSizingActor(op) || !model.IsAppRequestResize(op.Type) || strings.TrimSpace(op.AppID) == "" {
		return model.Operation{}, ErrInvalidInput
	}
	created, _, err := s.createOperationWithPolicy(op, operationCreatePolicy{
		ExpectedAppSpecHash: model.AppSpecSHA256(baseline), AllowAutomaticResourceSizing: true,
	})
	return created, err
}
