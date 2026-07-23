package store

import (
	"errors"
	"fmt"
	"time"

	"fugue/internal/compositecoordinator"
	"fugue/internal/model"
	"fugue/internal/releasecontract"
)

const compositeRuntimeLaneAdvisoryLock = "fugue-composite-runtime-lane-v1"

// GetCompositeRuntimeLane returns the fixed global composite mutation lane.
// Legacy history is materialized as a read-only genesis view until the first
// reservation persists it atomically with a new record.
func (s *Store) GetCompositeRuntimeLane() (compositecoordinator.RuntimeLane, error) {
	if s.usingDatabase() {
		return s.pgGetCompositeRuntimeLane()
	}
	var lane compositecoordinator.RuntimeLane
	err := s.withLockedState(false, func(state *model.State) error {
		resolved, err := compositeRuntimeLaneFromState(state, time.Now().UTC())
		if err != nil {
			return err
		}
		lane = resolved
		return nil
	})
	return lane, err
}

// ReserveCompositeReleaseTransaction is intentionally dormant. It atomically
// creates one prepared record and reserves the fixed runtime lane, but no API,
// worker, adapter, or scheduler calls it in this checkpoint.
func (s *Store) ReserveCompositeReleaseTransaction(
	plan releasecontract.CompositeReleasePlan,
	expectedLaneVersion int64,
) (compositecoordinator.Record, compositecoordinator.RuntimeLane, error) {
	if s.usingDatabase() {
		return s.pgReserveCompositeReleaseTransaction(plan, expectedLaneVersion)
	}
	now := time.Now().UTC()
	record, err := compositecoordinator.NewRecord(model.NewID("compositerelease"), plan, now)
	if err != nil || expectedLaneVersion < 0 {
		return compositecoordinator.Record{}, compositecoordinator.RuntimeLane{}, ErrInvalidInput
	}
	var reserved compositecoordinator.RuntimeLane
	err = s.withLockedState(true, func(state *model.State) error {
		lane, err := compositeRuntimeLaneFromState(state, now)
		if err != nil {
			return err
		}
		for _, existing := range state.CompositeTransactions {
			if compositecoordinator.VerifyRecord(existing) != nil {
				return fmt.Errorf("stored composite release transaction is invalid")
			}
			if existing.ID == record.ID || existing.Plan.Digest == record.Plan.Digest {
				return ErrConflict
			}
		}
		next, err := compositecoordinator.ReserveRuntimeLane(lane, record, expectedLaneVersion, now)
		if err != nil {
			return mapCompositeRuntimeLaneError(err)
		}
		state.CompositeTransactions = append(state.CompositeTransactions, record)
		state.CompositeRuntimeLane = &next
		if err := compositecoordinator.VerifyRuntimeLaneHistory(next, state.CompositeTransactions); err != nil {
			return mapCompositeRuntimeLaneError(err)
		}
		reserved = next
		return nil
	})
	if err != nil {
		return compositecoordinator.Record{}, compositecoordinator.RuntimeLane{}, err
	}
	return record, reserved, nil
}

func compositeRuntimeLaneFromState(state *model.State, now time.Time) (compositecoordinator.RuntimeLane, error) {
	if state == nil {
		return compositecoordinator.RuntimeLane{}, fmt.Errorf("composite runtime lane state is nil")
	}
	if state.CompositeRuntimeLane == nil {
		lane, err := compositecoordinator.NewRuntimeLaneFromHistory(state.CompositeTransactions, now)
		if err != nil {
			return compositecoordinator.RuntimeLane{}, mapCompositeRuntimeLaneError(err)
		}
		return lane, nil
	}
	lane := *state.CompositeRuntimeLane
	if err := compositecoordinator.VerifyRuntimeLaneHistory(lane, state.CompositeTransactions); err != nil {
		return compositecoordinator.RuntimeLane{}, mapCompositeRuntimeLaneError(err)
	}
	return lane, nil
}

func mapCompositeRuntimeLaneError(err error) error {
	if errors.Is(err, compositecoordinator.ErrRuntimeLaneConflict) {
		return ErrConflict
	}
	if errors.Is(err, compositecoordinator.ErrInvalidRuntimeLane) {
		return fmt.Errorf("stored composite runtime lane is invalid")
	}
	return err
}
