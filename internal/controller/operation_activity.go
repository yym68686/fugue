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
