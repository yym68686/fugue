package controller

import (
	"context"
	"errors"

	"fugue/internal/model"
	"fugue/internal/store"
)

var errSafeRolloutCanonicalPending = errors.New(model.OperationResultCanonicalHandoffPending)

func safeRolloutCanonicalPending(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return errSafeRolloutCanonicalPending
}

func safeRolloutPendingResult(err error) string {
	switch {
	case errors.Is(err, errSafeRolloutCanonicalPending):
		return model.OperationResultCanonicalHandoffPending
	case errors.Is(err, errSafeRolloutTrafficPending):
		return model.OperationResultTrafficConfirmationPending
	default:
		return ""
	}
}

// Requeue the same fenced operation. Its immutable candidate and promotion
// receipts are the resume cursor, not a new deploy or a new image import.
func (s *Service) deferSafeRolloutPending(ctx context.Context, op model.Operation, message string) error {
	err := retryOperationStore(ctx, 0, s.operationPersistencePending(op.ID, "safe_rollout_wait"), func() error {
		_, err := s.Store.RequeueManagedOperationGuarded(op, message)
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

func (s *Service) waitForSafeRolloutCanonicalHandoff(ctx context.Context, op model.Operation, app model.App, releaseID string, err error) error {
	s.recordSafeRolloutReleaseStep(op, app, "stable_alignment", model.ReleaseStepStatusRunning,
		"promoted release is serving; original connection drain or canonical handoff is not verified", releaseID, map[string]any{"error": err.Error()})
	return safeRolloutCanonicalPending(ctx)
}
