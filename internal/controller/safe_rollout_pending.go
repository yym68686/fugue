package controller

import (
	"context"
	"errors"

	"fugue/internal/model"
	"fugue/internal/store"
)

// Requeue the same fenced operation. Its immutable candidate and promotion
// receipts are the resume cursor, not a new deploy or a new image import.
func (s *Service) deferSafeRolloutTrafficConfirmation(ctx context.Context, op model.Operation) error {
	err := retryOperationStore(ctx, 0, s.operationPersistencePending(op.ID, "traffic_publication_wait"), func() error {
		_, err := s.Store.RequeueManagedOperationGuarded(op, model.OperationResultTrafficConfirmationPending)
		if errors.Is(err, store.ErrConflict) || errors.Is(err, store.ErrNotFound) {
			return errOperationNoLongerActive
		}
		return err
	})
	if errors.Is(err, errOperationNoLongerActive) {
		return nil
	}
	if err != nil {
		return err
	}
	// The rollout wait already records the nonterminal attempt. Do not perform
	// an unfenced attempt update after requeue: cancellation may now own it.
	return nil
}
