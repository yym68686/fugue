package store

import (
	"context"

	"fugue/internal/buildadmission/contract"
)

type builderAdmissionCheckpointPort struct{ store *Store }

// BuilderAdmissionCheckpoints returns the Controller's scoped adapter without
// exposing the Store or a capability to write other observations.
func (s *Store) BuilderAdmissionCheckpoints() builderAdmissionCheckpointPort {
	return builderAdmissionCheckpointPort{store: s}
}

func isBuilderAdmissionCheckpoint(key string) bool { return contract.OwnsCheckpoint(key) }

func (p builderAdmissionCheckpointPort) Read(ctx context.Context, key string) ([]byte, error) {
	if !isBuilderAdmissionCheckpoint(key) {
		return nil, errBuilderAdmissionCheckpointScope
	}
	return p.store.ReadObservationCheckpoint(ctx, key)
}

func (p builderAdmissionCheckpointPort) Update(ctx context.Context, key string, fn func([]byte) ([]byte, error)) (bool, error) {
	if !isBuilderAdmissionCheckpoint(key) {
		return false, errBuilderAdmissionCheckpointScope
	}
	return p.store.updateObservationCheckpoint(ctx, key, fn, nil)
}
