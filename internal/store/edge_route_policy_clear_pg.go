package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func (s *Store) pgCheckEdgeRoutePolicyClearEvidence(hostname string, edgeIDs, edgeGroupIDs []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return fmt.Errorf("begin edge exclusion evidence snapshot: %w", err)
	}
	defer tx.Rollback()
	if _, err := scanEdgeRoutePolicy(tx.QueryRowContext(ctx, `SELECT `+edgeRoutePolicySelectColumns+` FROM fugue_edge_route_policies WHERE lower(hostname)=lower($1)`, hostname)); err != nil {
		return mapDBErr(err)
	}
	if err := s.pgValidateEdgeExclusionClearTx(ctx, tx, edgeIDs, edgeGroupIDs, false); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit edge exclusion evidence snapshot: %w", err)
	}
	return nil
}

func (s *Store) pgValidateEdgeExclusionClearTx(ctx context.Context, tx *sql.Tx, edgeIDs, edgeGroupIDs []string, lock bool) error {
	if lock {
		// SHARE conflicts with all heartbeat/activation RowExclusive writers and
		// closes evidence-to-policy-CAS TOCTOU without granting a write retry.
		if _, err := tx.ExecContext(ctx, `LOCK TABLE fugue_edge_activation, fugue_edge_active_epochs, fugue_edge_node_instances, fugue_edge_nodes IN SHARE MODE`); err != nil {
			return fmt.Errorf("lock edge exclusion clear evidence: %w", err)
		}
	}
	activation, err := pgReadEdgeActivation(ctx, tx, lock)
	if err != nil {
		return err
	}
	instances, err := pgReadEdgeNodeInstances(ctx, tx, "")
	if err != nil {
		return err
	}
	epochs, err := pgReadEdgeActiveEpochs(ctx, tx, "")
	if err != nil {
		return err
	}
	controls, err := pgReadEdgeControlNodes(ctx, tx)
	if err != nil {
		return err
	}
	if err := verifyEdgeInstanceMaterialForActivation(instances, epochs, controls, activation); err != nil {
		return err
	}
	now, err := pgEdgeServerTime(ctx, tx)
	if err != nil {
		return err
	}
	return validateEdgeExclusionClearMaterial(activation, instances, epochs, edgeIDs, edgeGroupIDs, now)
}
