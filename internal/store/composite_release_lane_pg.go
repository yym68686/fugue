package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"fugue/internal/compositecoordinator"
	"fugue/internal/model"
	"fugue/internal/releasecontract"
)

func (s *Store) pgGetCompositeRuntimeLane() (compositecoordinator.RuntimeLane, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	lane, err := pgReadCompositeRuntimeLane(ctx, s.db, false)
	if err == nil {
		records, historyErr := pgReadCompositeReleaseHistory(ctx, s.db)
		if historyErr != nil {
			return compositecoordinator.RuntimeLane{}, historyErr
		}
		if historyErr = compositecoordinator.VerifyRuntimeLaneHistory(lane, records); historyErr != nil {
			return compositecoordinator.RuntimeLane{}, mapCompositeRuntimeLaneError(historyErr)
		}
		return lane, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return compositecoordinator.RuntimeLane{}, mapDBErr(err)
	}
	records, err := pgReadCompositeReleaseHistory(ctx, s.db)
	if err != nil {
		return compositecoordinator.RuntimeLane{}, err
	}
	lane, err = compositecoordinator.NewRuntimeLaneFromHistory(records, time.Now().UTC())
	if err != nil {
		return compositecoordinator.RuntimeLane{}, mapCompositeRuntimeLaneError(err)
	}
	return lane, nil
}

func (s *Store) pgReserveCompositeReleaseTransaction(
	plan releasecontract.CompositeReleasePlan,
	expectedLaneVersion int64,
) (compositecoordinator.Record, compositecoordinator.RuntimeLane, error) {
	now := time.Now().UTC().Truncate(time.Microsecond)
	record, err := compositecoordinator.NewRecord(model.NewID("compositerelease"), plan, now)
	if err != nil || expectedLaneVersion < 0 {
		return compositecoordinator.Record{}, compositecoordinator.RuntimeLane{}, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return compositecoordinator.Record{}, compositecoordinator.RuntimeLane{}, mapDBErr(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, compositeRuntimeLaneAdvisoryLock); err != nil {
		return compositecoordinator.Record{}, compositecoordinator.RuntimeLane{}, mapDBErr(err)
	}

	lane, err := pgLockOrCreateCompositeRuntimeLane(ctx, tx, now)
	if err != nil {
		return compositecoordinator.Record{}, compositecoordinator.RuntimeLane{}, err
	}
	next, err := compositecoordinator.ReserveRuntimeLane(lane, record, expectedLaneVersion, now)
	if err != nil {
		return compositecoordinator.Record{}, compositecoordinator.RuntimeLane{}, mapCompositeRuntimeLaneError(err)
	}
	if err := pgInsertCompositeReleaseTransactionTx(ctx, tx, record); err != nil {
		return compositecoordinator.Record{}, compositecoordinator.RuntimeLane{}, err
	}
	encoded, err := json.Marshal(next)
	if err != nil {
		return compositecoordinator.Record{}, compositecoordinator.RuntimeLane{}, fmt.Errorf("marshal composite runtime lane: %w", err)
	}
	result, err := tx.ExecContext(ctx, `
UPDATE fugue_composite_release_lanes
SET generation = $2, fencing_epoch = $3, lane_version = $4,
    active_record_id = $5, active_initial_record_digest = $6, active_plan_digest = $7,
    last_settled_record_id = $8, last_settled_record_digest = $9, last_settled_plan_digest = $10,
    frozen = $11, freeze_reason = $12, lane_json = $13, updated_at = $14
WHERE lane_key = $1 AND lane_version = $15 AND active_record_id = '' AND frozen = FALSE
`, next.Key, next.Generation, next.FencingEpoch, next.Version,
		next.ActiveRecordID, next.ActiveInitialRecordDigest, next.ActivePlanDigest,
		next.LastSettledRecordID, next.LastSettledRecordDigest, next.LastSettledPlanDigest,
		next.Frozen, next.FreezeReason, encoded, next.UpdatedAt, lane.Version)
	if err != nil {
		return compositecoordinator.Record{}, compositecoordinator.RuntimeLane{}, mapDBErr(err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return compositecoordinator.Record{}, compositecoordinator.RuntimeLane{}, fmt.Errorf("read composite lane CAS result: %w", err)
	}
	if rows != 1 {
		return compositecoordinator.Record{}, compositecoordinator.RuntimeLane{}, ErrConflict
	}
	if err := tx.Commit(); err != nil {
		return compositecoordinator.Record{}, compositecoordinator.RuntimeLane{}, mapDBErr(err)
	}
	return record, next, nil
}

func pgLockOrCreateCompositeRuntimeLane(ctx context.Context, tx *sql.Tx, now time.Time) (compositecoordinator.RuntimeLane, error) {
	lane, err := pgReadCompositeRuntimeLane(ctx, tx, true)
	if err == nil {
		records, historyErr := pgReadCompositeReleaseHistory(ctx, tx)
		if historyErr != nil {
			return compositecoordinator.RuntimeLane{}, historyErr
		}
		if historyErr = compositecoordinator.VerifyRuntimeLaneHistory(lane, records); historyErr != nil {
			return compositecoordinator.RuntimeLane{}, mapCompositeRuntimeLaneError(historyErr)
		}
		return lane, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return compositecoordinator.RuntimeLane{}, mapDBErr(err)
	}
	records, err := pgReadCompositeReleaseHistory(ctx, tx)
	if err != nil {
		return compositecoordinator.RuntimeLane{}, err
	}
	genesis, err := compositecoordinator.NewRuntimeLaneFromHistory(records, now)
	if err != nil {
		return compositecoordinator.RuntimeLane{}, mapCompositeRuntimeLaneError(err)
	}
	encoded, err := json.Marshal(genesis)
	if err != nil {
		return compositecoordinator.RuntimeLane{}, fmt.Errorf("marshal composite runtime lane genesis: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_composite_release_lanes (
  lane_key, generation, fencing_epoch, lane_version,
  active_record_id, active_initial_record_digest, active_plan_digest,
  last_settled_record_id, last_settled_record_digest, last_settled_plan_digest,
  frozen, freeze_reason, lane_json, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
ON CONFLICT (lane_key) DO NOTHING
`, genesis.Key, genesis.Generation, genesis.FencingEpoch, genesis.Version,
		genesis.ActiveRecordID, genesis.ActiveInitialRecordDigest, genesis.ActivePlanDigest,
		genesis.LastSettledRecordID, genesis.LastSettledRecordDigest, genesis.LastSettledPlanDigest,
		genesis.Frozen, genesis.FreezeReason, encoded, genesis.CreatedAt, genesis.UpdatedAt); err != nil {
		return compositecoordinator.RuntimeLane{}, mapDBErr(err)
	}
	lane, err = pgReadCompositeRuntimeLane(ctx, tx, true)
	if err != nil {
		return compositecoordinator.RuntimeLane{}, mapDBErr(err)
	}
	return lane, nil
}

type compositeRuntimeLaneQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func pgReadCompositeRuntimeLane(ctx context.Context, db compositeRuntimeLaneQuerier, forUpdate bool) (compositecoordinator.RuntimeLane, error) {
	query := `SELECT generation, fencing_epoch, lane_version,
	       active_record_id, active_initial_record_digest, active_plan_digest,
	       last_settled_record_id, last_settled_record_digest, last_settled_plan_digest,
	       frozen, freeze_reason, lane_json, created_at, updated_at
FROM fugue_composite_release_lanes
WHERE lane_key = $1`
	if forUpdate {
		query += " FOR UPDATE"
	}
	var generation, fencingEpoch string
	var activeRecordID, activeInitialRecordDigest, activePlanDigest string
	var lastSettledRecordID, lastSettledRecordDigest, lastSettledPlanDigest, freezeReason string
	var version int64
	var frozen bool
	var encoded []byte
	var createdAt, updatedAt time.Time
	err := db.QueryRowContext(ctx, query, compositecoordinator.RuntimeLaneKey).Scan(
		&generation, &fencingEpoch, &version,
		&activeRecordID, &activeInitialRecordDigest, &activePlanDigest,
		&lastSettledRecordID, &lastSettledRecordDigest, &lastSettledPlanDigest,
		&frozen, &freezeReason, &encoded, &createdAt, &updatedAt,
	)
	if err != nil {
		return compositecoordinator.RuntimeLane{}, err
	}
	lane, err := decodeCompositeRuntimeLane(encoded)
	if err != nil || lane.Generation != generation || lane.FencingEpoch != fencingEpoch ||
		lane.Version != version || lane.ActiveRecordID != activeRecordID ||
		lane.ActiveInitialRecordDigest != activeInitialRecordDigest || lane.ActivePlanDigest != activePlanDigest ||
		lane.LastSettledRecordID != lastSettledRecordID ||
		lane.LastSettledRecordDigest != lastSettledRecordDigest || lane.LastSettledPlanDigest != lastSettledPlanDigest ||
		lane.Frozen != frozen ||
		lane.FreezeReason != freezeReason || !lane.CreatedAt.Equal(createdAt) || !lane.UpdatedAt.Equal(updatedAt) {
		return compositecoordinator.RuntimeLane{}, fmt.Errorf("stored composite runtime lane columns do not match sealed JSON")
	}
	return lane, nil
}

type compositeReleaseHistoryQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func pgReadCompositeReleaseHistory(ctx context.Context, db compositeReleaseHistoryQuerier) ([]compositecoordinator.Record, error) {
	rows, err := db.QueryContext(ctx, `SELECT record_json FROM fugue_composite_release_transactions ORDER BY updated_at ASC, id ASC`)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	records := make([]compositecoordinator.Record, 0)
	for rows.Next() {
		var encoded []byte
		if err := rows.Scan(&encoded); err != nil {
			return nil, mapDBErr(err)
		}
		record, err := decodeCompositeReleaseTransaction(encoded)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, mapDBErr(err)
	}
	return records, nil
}

func pgInsertCompositeReleaseTransactionTx(ctx context.Context, tx *sql.Tx, record compositecoordinator.Record) error {
	encoded, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal reserved composite release transaction: %w", err)
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO fugue_composite_release_transactions (
  id, plan_digest, image_activation_plan_digest, generation, fencing_epoch,
  state, current_step, rollback_start_step, record_revision, record_json,
  created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
`, record.ID, record.Plan.Digest, record.Plan.ImageActivationPlanDigest, record.Plan.Generation,
		record.Plan.FencingEpoch, record.State, record.CurrentStep, record.RollbackStartStep,
		record.Revision, encoded, record.CreatedAt, record.UpdatedAt)
	if err != nil {
		return mapDBErr(err)
	}
	return nil
}

func decodeCompositeRuntimeLane(encoded []byte) (compositecoordinator.RuntimeLane, error) {
	var lane compositecoordinator.RuntimeLane
	if len(encoded) == 0 || json.Unmarshal(encoded, &lane) != nil || compositecoordinator.VerifyRuntimeLane(lane) != nil {
		return compositecoordinator.RuntimeLane{}, fmt.Errorf("stored composite runtime lane is invalid")
	}
	return lane, nil
}
