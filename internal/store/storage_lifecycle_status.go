package store

import (
	"context"
	"sync"

	"fugue/internal/storageaudit"
)

type storageLifecycleState struct {
	mu          sync.Mutex
	observation storageaudit.Observation
}

func (s *Store) StorageLifecycleObservation() storageaudit.Observation {
	s.lifecycle.mu.Lock()
	defer s.lifecycle.mu.Unlock()
	o := s.lifecycle.observation
	o.Schema = storageaudit.ObservationSchema
	o.CapacitySemantics = storageaudit.CapacitySemantics
	if o.Sample != nil {
		sample := *o.Sample
		sample.Relations = append([]storageaudit.RelationAllocation(nil), sample.Relations...)
		o.Sample = &sample
	}
	return o
}

// The API-local policy refresh reads the worker's redacted status for diagnostics.
func (s *Store) refreshStorageLifecycleStatus(ctx context.Context) {
	if s.db == nil || ctx.Err() != nil {
		return
	}
	progress, err := storageaudit.ReadProgress(ctx, s.db)
	s.lifecycle.mu.Lock()
	defer s.lifecycle.mu.Unlock()
	if err != nil || progress.Owner != "worker" {
		s.lifecycle.observation.PolicyReadFailed = true
		s.lifecycle.observation.LastRunFailed = true
		s.lifecycle.observation.FailureCategory = "owner_status_unavailable"
		return
	}
	s.lifecycle.observation = progress.Observation()
}
