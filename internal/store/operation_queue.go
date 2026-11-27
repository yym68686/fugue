package store

import (
	"errors"

	"fugue/internal/model"
	"fugue/internal/operationqueue"
)

// OperationQueue is the Controller's durable queue port. The existing Store
// implementations still perform each transition atomically, including their
// original tenant checks, runtime assignment, and database interlocks.
func (s *Store) OperationQueue() operationqueue.Repository {
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

func (r operationQueueRepository) FailBeforeClaim(id, reason string) (model.Operation, bool, error) {
	op, err := r.store.FailOperation(id, reason)
	if errors.Is(err, ErrConflict) || errors.Is(err, ErrNotFound) {
		return model.Operation{}, false, nil
	}
	return op, err == nil, err
}

func (r operationQueueRepository) CancelSupersededDeploy(id, reason string) (bool, error) {
	_, err := r.store.CancelOperation(id, reason)
	if errors.Is(err, ErrConflict) || errors.Is(err, ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}
