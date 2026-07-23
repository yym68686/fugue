package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"fugue/internal/compositecoordinator"
)

func (s *Store) pgCreateCompositeReleaseTransaction(record compositecoordinator.Record) (compositecoordinator.Record, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return compositecoordinator.Record{}, mapDBErr(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, compositeRuntimeLaneAdvisoryLock); err != nil {
		return compositecoordinator.Record{}, mapDBErr(err)
	}
	if lane, err := pgReadCompositeRuntimeLane(ctx, tx, true); err == nil {
		records, historyErr := pgReadCompositeReleaseHistory(ctx, tx)
		if historyErr != nil {
			return compositecoordinator.Record{}, historyErr
		}
		if historyErr = compositecoordinator.VerifyRuntimeLaneHistory(lane, records); historyErr != nil {
			return compositecoordinator.Record{}, mapCompositeRuntimeLaneError(historyErr)
		}
		return compositecoordinator.Record{}, ErrConflict
	} else if !errors.Is(err, sql.ErrNoRows) {
		return compositecoordinator.Record{}, mapDBErr(err)
	}
	if err := pgInsertCompositeReleaseTransactionTx(ctx, tx, record); err != nil {
		return compositecoordinator.Record{}, err
	}
	if err := tx.Commit(); err != nil {
		return compositecoordinator.Record{}, mapDBErr(err)
	}
	return record, nil
}

func (s *Store) pgGetCompositeReleaseTransaction(id string) (compositecoordinator.Record, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var encoded []byte
	err := s.db.QueryRowContext(ctx, `
SELECT record_json
FROM fugue_composite_release_transactions
WHERE id = $1
`, id).Scan(&encoded)
	if err != nil {
		return compositecoordinator.Record{}, mapDBErr(err)
	}
	return decodeCompositeReleaseTransaction(encoded)
}

func (s *Store) pgAdvanceCompositeReleaseTransaction(
	id string,
	expectedRevision int64,
	expectedPlanDigest string,
	expectedFencingEpoch string,
	transition compositecoordinator.Transition,
) (compositecoordinator.Record, error) {
	current, err := s.pgGetCompositeReleaseTransaction(id)
	if err != nil {
		return compositecoordinator.Record{}, err
	}
	if current.Revision != expectedRevision || current.Plan.Digest != expectedPlanDigest || current.Plan.FencingEpoch != expectedFencingEpoch {
		return compositecoordinator.Record{}, ErrConflict
	}
	next, err := compositecoordinator.ApplyTransition(current, transition, time.Now().UTC())
	if err != nil {
		return compositecoordinator.Record{}, fmt.Errorf("%w: %v", ErrConflict, err)
	}
	encoded, err := json.Marshal(next)
	if err != nil {
		return compositecoordinator.Record{}, fmt.Errorf("marshal advanced composite release transaction: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, err := s.db.ExecContext(ctx, `
UPDATE fugue_composite_release_transactions
SET state = $2, current_step = $3, rollback_start_step = $4,
    record_revision = $5, record_json = $6, updated_at = $7
WHERE id = $1 AND plan_digest = $8 AND fencing_epoch = $9 AND record_revision = $10
`, id, next.State, next.CurrentStep, next.RollbackStartStep, next.Revision, encoded, next.UpdatedAt,
		expectedPlanDigest, expectedFencingEpoch, expectedRevision)
	if err != nil {
		return compositecoordinator.Record{}, mapDBErr(err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return compositecoordinator.Record{}, fmt.Errorf("read composite release CAS result: %w", err)
	}
	if rows != 1 {
		return compositecoordinator.Record{}, ErrConflict
	}
	return next, nil
}

func decodeCompositeReleaseTransaction(encoded []byte) (compositecoordinator.Record, error) {
	var record compositecoordinator.Record
	if len(encoded) == 0 || json.Unmarshal(encoded, &record) != nil || compositecoordinator.VerifyRecord(record) != nil {
		return compositecoordinator.Record{}, fmt.Errorf("stored composite release transaction is invalid")
	}
	return record, nil
}
