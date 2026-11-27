package store

import (
	"errors"

	"fugue/internal/model"
)

// OperationQueue is the Controller's durable queue port. The existing Store
// implementations still perform each transition atomically, including their
// original tenant checks, runtime assignment, and database interlocks.
// Return the restricted adapter; do not import the Controller's queue policy.
func (s *Store) OperationQueue() operationQueueRepository {
	return operationQueueRepository{store: s}
}

type operationQueueRepository struct {
	store *Store
}

func (r operationQueueRepository) ListActiveOperations() ([]model.Operation, error) {
	return r.store.ListActiveOperations()
}

func (r operationQueueRepository) ListAppsMetadataByIDs(ids []string) ([]model.App, error) {
	return r.store.ListAppsMetadataByIDs(ids)
}

func (r operationQueueRepository) ListAppsMetadataByProjectIDs(ids []string) ([]model.App, error) {
	return r.store.ListAppsMetadataByProjectIDs(ids)
}

func (r operationQueueRepository) TryClaimPendingOperation(id string) (model.Operation, bool, error) {
	return r.store.TryClaimPendingOperation(id)
}

func (r operationQueueRepository) FailBeforeClaim(expected model.Operation, reason string) (model.Operation, bool, error) {
	op, err := r.store.failOperationGuarded(expected.ID, reason, &operationFinalizationGuard{
		Operation: expected, PendingSnapshotOnly: true,
	})
	if errors.Is(err, ErrConflict) || errors.Is(err, ErrNotFound) {
		return model.Operation{}, false, nil
	}
	return op, err == nil, err
}

func (r operationQueueRepository) CancelSupersededDeploy(id, reason string) (bool, error) {
	_, err := r.store.pgCancelOperation(id, reason, nil)
	if errors.Is(err, ErrConflict) || errors.Is(err, ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}
