package store

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"fugue/internal/compositecoordinator"
	"fugue/internal/model"
	"fugue/internal/releasecontract"
)

func (s *Store) CreateCompositeReleaseTransaction(plan releasecontract.CompositeReleasePlan) (compositecoordinator.Record, error) {
	record, err := compositecoordinator.NewRecord(model.NewID("compositerelease"), plan, time.Now().UTC())
	if err != nil {
		return compositecoordinator.Record{}, fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	if s.usingDatabase() {
		return s.pgCreateCompositeReleaseTransaction(record)
	}
	err = s.withLockedState(true, func(state *model.State) error {
		if state.CompositeRuntimeLane != nil {
			if _, err := compositeRuntimeLaneFromState(state, record.CreatedAt); err != nil {
				return err
			}
			return ErrConflict
		}
		for _, existing := range state.CompositeTransactions {
			if err := compositecoordinator.VerifyRecord(existing); err != nil {
				return fmt.Errorf("stored composite release transaction: %w", err)
			}
			if existing.ID == record.ID || existing.Plan.Digest == record.Plan.Digest {
				return ErrConflict
			}
		}
		state.CompositeTransactions = append(state.CompositeTransactions, record)
		return nil
	})
	if err != nil {
		return compositecoordinator.Record{}, err
	}
	return record, nil
}

func (s *Store) GetCompositeReleaseTransaction(id string) (compositecoordinator.Record, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return compositecoordinator.Record{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetCompositeReleaseTransaction(id)
	}
	var record compositecoordinator.Record
	err := s.withLockedState(false, func(state *model.State) error {
		for _, candidate := range state.CompositeTransactions {
			if candidate.ID == id {
				if err := compositecoordinator.VerifyRecord(candidate); err != nil {
					return fmt.Errorf("stored composite release transaction: %w", err)
				}
				record = candidate
				return nil
			}
		}
		return ErrNotFound
	})
	return record, err
}

func (s *Store) AdvanceCompositeReleaseTransaction(
	id string,
	expectedRevision int64,
	expectedPlanDigest string,
	expectedFencingEpoch string,
	transition compositecoordinator.Transition,
) (compositecoordinator.Record, error) {
	id = strings.TrimSpace(id)
	if id == "" || expectedRevision < 1 || strings.TrimSpace(expectedPlanDigest) == "" || strings.TrimSpace(expectedFencingEpoch) == "" {
		return compositecoordinator.Record{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgAdvanceCompositeReleaseTransaction(id, expectedRevision, expectedPlanDigest, expectedFencingEpoch, transition)
	}
	var next compositecoordinator.Record
	err := s.withLockedState(true, func(state *model.State) error {
		for index, current := range state.CompositeTransactions {
			if current.ID != id {
				continue
			}
			if current.Revision != expectedRevision || current.Plan.Digest != expectedPlanDigest || current.Plan.FencingEpoch != expectedFencingEpoch {
				return ErrConflict
			}
			advanced, err := compositecoordinator.ApplyTransition(current, transition, time.Now().UTC())
			if err != nil {
				if errors.Is(err, compositecoordinator.ErrInvalidTransition) {
					return fmt.Errorf("%w: %v", ErrConflict, err)
				}
				return fmt.Errorf("%w: %v", ErrInvalidInput, err)
			}
			state.CompositeTransactions[index] = advanced
			next = advanced
			return nil
		}
		return ErrNotFound
	})
	return next, err
}
