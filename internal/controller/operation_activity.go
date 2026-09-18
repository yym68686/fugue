package controller

import (
	"errors"
	"fmt"

	"fugue/internal/model"
	"fugue/internal/store"
)

var errOperationNoLongerActive = errors.New("operation is no longer active")

func (s *Service) ensureOperationStillActive(id string) error {
	operation, err := s.Store.GetOperation(id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return errOperationNoLongerActive
		}
		return fmt.Errorf("load operation %s: %w", id, err)
	}

	switch operation.Status {
	case model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent:
		return nil
	default:
		return fmt.Errorf("%w: operation %s is %s", errOperationNoLongerActive, id, operation.Status)
	}
}

// A deletion must not wait for the readiness of the workload being removed.
// CancelOperation already restricts running cancellation to deploy operations;
// database transitions and migrations retain their existing safety gates.
func (s *Service) cancelDeploysSupersededByDeletion(operations []model.Operation) (bool, error) {
	deletes := map[string]model.Operation{}
	for _, op := range operations {
		if op.Type == model.OperationTypeDelete {
			deletes[op.AppID] = op
		}
	}
	changed := false
	for _, op := range operations {
		deletion, ok := deletes[op.AppID]
		if !ok || op.Type != model.OperationTypeDeploy || (op.Status != model.OperationStatusPending && op.Status != model.OperationStatusRunning) || deletion.CreatedAt.Before(op.CreatedAt) {
			continue
		}
		if _, err := s.Store.CancelOperation(op.ID, "deployment superseded by app deletion"); err != nil {
			if errors.Is(err, store.ErrConflict) || errors.Is(err, store.ErrNotFound) {
				continue
			}
			return changed, fmt.Errorf("cancel deploy before app deletion: %w", err)
		}
		changed = true
	}
	return changed, nil
}
