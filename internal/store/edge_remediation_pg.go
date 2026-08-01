package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"fugue/internal/model"
)

func (s *Store) pgAdvanceEdgeRemediation(advance model.EdgeRemediationAdvance) (model.EdgeActivationState, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.EdgeActivationState{}, fmt.Errorf("begin edge remediation transaction: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, pgEdgeAdvisoryLockKey("activation", "singleton")); err != nil {
		return model.EdgeActivationState{}, fmt.Errorf("lock edge remediation: %w", err)
	}
	current, err := pgReadEdgeActivation(ctx, tx, true)
	if err != nil {
		return model.EdgeActivationState{}, err
	}
	if current.Generation != advance.ExpectedActivationGeneration {
		return model.EdgeActivationState{}, ErrConflict
	}
	instances, err := pgReadEdgeNodeInstances(ctx, tx, "")
	if err != nil {
		return model.EdgeActivationState{}, err
	}
	epochs, err := pgReadEdgeActiveEpochs(ctx, tx, "")
	if err != nil {
		return model.EdgeActivationState{}, err
	}
	now, err := pgEdgeServerTime(ctx, tx)
	if err != nil {
		return model.EdgeActivationState{}, err
	}
	next, err := buildNextEdgeRemediation(current, advance, instances, epochs, now)
	if err != nil {
		return model.EdgeActivationState{}, err
	}
	payload, err := json.Marshal(next)
	if err != nil {
		return model.EdgeActivationState{}, fmt.Errorf("encode edge remediation: %w", err)
	}
	result, err := tx.ExecContext(ctx, `UPDATE fugue_edge_activation SET phase=$1,generation=$2,state_json=$3,updated_at=$4 WHERE singleton=true AND generation=$5`, next.Phase, next.Generation, payload, now, current.Generation)
	if err != nil {
		return model.EdgeActivationState{}, mapDBErr(err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return model.EdgeActivationState{}, ErrConflict
	}
	if err := tx.Commit(); err != nil {
		return model.EdgeActivationState{}, fmt.Errorf("commit edge remediation transaction: %w", err)
	}
	return next, nil
}
