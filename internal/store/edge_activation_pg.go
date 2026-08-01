package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"fugue/internal/model"
)

func (s *Store) pgGetEdgeActivationState() (model.EdgeActivationState, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return pgReadEdgeActivation(ctx, s.db, false)
}

func (s *Store) pgAdvanceEdgeActivation(advance model.EdgeActivationAdvance) (model.EdgeActivationState, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.EdgeActivationState{}, fmt.Errorf("begin edge activation transaction: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, pgEdgeAdvisoryLockKey("activation", "singleton")); err != nil {
		return model.EdgeActivationState{}, fmt.Errorf("lock edge activation: %w", err)
	}
	current, err := pgReadEdgeActivation(ctx, tx, true)
	if err != nil {
		return model.EdgeActivationState{}, err
	}
	if current.Generation != advance.ExpectedGeneration {
		return model.EdgeActivationState{}, ErrConflict
	}
	instances, err := pgReadEdgeNodeInstances(ctx, tx, "")
	if err != nil {
		return model.EdgeActivationState{}, err
	}
	currentEpochs, err := pgReadEdgeActiveEpochs(ctx, tx, "")
	if err != nil {
		return model.EdgeActivationState{}, err
	}
	now, err := pgEdgeServerTime(ctx, tx)
	if err != nil {
		return model.EdgeActivationState{}, err
	}
	next, err := buildNextEdgeActivation(current, advance, instances, currentEpochs, now)
	if err != nil {
		return model.EdgeActivationState{}, err
	}
	if advance.ToPhase == model.EdgeActivationPhaseActive {
		next.PreviousActiveEpochs = append([]model.EdgeActiveEpoch(nil), currentEpochs...)
		if _, err := tx.ExecContext(ctx, `DELETE FROM fugue_edge_active_epochs`); err != nil {
			return model.EdgeActivationState{}, fmt.Errorf("replace active edge epochs: %w", err)
		}
		for _, epoch := range current.CandidateEpochs {
			epoch, err = normalizeEdgeActiveEpoch(epoch)
			if err != nil {
				return model.EdgeActivationState{}, err
			}
			if epoch.ActivatedAt.IsZero() {
				epoch.ActivatedAt = now
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO fugue_edge_active_epochs (
				edge_group_id,slot,release_epoch,fence_sequence,min_healthy_instances,activated_at,created_at,updated_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$7)`, epoch.EdgeGroupID, epoch.Slot, epoch.ReleaseEpoch, epoch.FenceSequence, epoch.MinHealthyInstances, epoch.ActivatedAt, now); err != nil {
				return model.EdgeActivationState{}, mapDBErr(err)
			}
		}
	}
	if advance.ToPhase == model.EdgeActivationActionRollback && current.Rollback != nil {
		if _, err := tx.ExecContext(ctx, `DELETE FROM fugue_edge_active_epochs`); err != nil {
			return model.EdgeActivationState{}, fmt.Errorf("restore active edge epochs: %w", err)
		}
		for _, epoch := range current.Rollback.ActiveEpochs {
			if _, err := tx.ExecContext(ctx, `INSERT INTO fugue_edge_active_epochs (edge_group_id,slot,release_epoch,fence_sequence,min_healthy_instances,activated_at,created_at,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, epoch.EdgeGroupID, epoch.Slot, epoch.ReleaseEpoch, epoch.FenceSequence, epoch.MinHealthyInstances, epoch.ActivatedAt, epoch.CreatedAt, epoch.UpdatedAt); err != nil {
				return model.EdgeActivationState{}, mapDBErr(err)
			}
		}
	}
	stateJSON, err := json.Marshal(next)
	if err != nil {
		return model.EdgeActivationState{}, fmt.Errorf("encode edge activation: %w", err)
	}
	result, err := tx.ExecContext(ctx, `UPDATE fugue_edge_activation SET phase=$1,generation=$2,state_json=$3,updated_at=$4
		WHERE singleton=true AND generation=$5`, next.Phase, next.Generation, stateJSON, now, current.Generation)
	if err != nil {
		return model.EdgeActivationState{}, mapDBErr(err)
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return model.EdgeActivationState{}, ErrConflict
	}
	if next.Phase == model.EdgeActivationPhaseEnforced {
		if _, err := tx.ExecContext(ctx, `INSERT INTO fugue_meta (key,value,updated_at) VALUES ($1,$2,$3)
			ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value,updated_at=EXCLUDED.updated_at`, edgeFlatWriteFenceMetaKey, model.EdgeActivationPhaseEnforced, now); err != nil {
			return model.EdgeActivationState{}, fmt.Errorf("arm flat edge write fence: %w", err)
		}
	} else {
		if _, err := tx.ExecContext(ctx, `DELETE FROM fugue_meta WHERE key=$1`, edgeFlatWriteFenceMetaKey); err != nil {
			return model.EdgeActivationState{}, fmt.Errorf("disarm flat edge write fence: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return model.EdgeActivationState{}, fmt.Errorf("commit edge activation transaction: %w", err)
	}
	return next, nil
}

func (s *Store) pgVerifyEdgeActivationReadiness(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return fmt.Errorf("begin edge activation readiness snapshot: %w", err)
	}
	defer tx.Rollback()
	activation, err := pgReadEdgeActivation(ctx, tx, false)
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
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit edge activation readiness snapshot: %w", err)
	}
	return nil
}

func (s *Store) pgListRouteEdgeNodes(edgeGroupID string) ([]model.EdgeNode, []model.EdgeGroup, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, nil, fmt.Errorf("begin route edge inventory snapshot: %w", err)
	}
	defer tx.Rollback()
	activation, err := pgReadEdgeActivation(ctx, tx, false)
	if err != nil {
		return nil, nil, err
	}
	groups, err := pgReadEdgeGroupDefinitions(ctx, tx, edgeGroupID)
	if err != nil {
		return nil, nil, err
	}
	if !edgeActivationUsesInstanceRoutes(activation) {
		nodes, err := pgReadFullEdgeNodes(ctx, tx, edgeGroupID)
		if err != nil {
			return nil, nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, nil, fmt.Errorf("commit legacy route inventory snapshot: %w", err)
		}
		return nodes, edgeGroupSummaries(groups, nodes, edgeGroupID), nil
	}
	instances, err := pgReadEdgeNodeInstances(ctx, tx, edgeGroupID)
	if err != nil {
		return nil, nil, err
	}
	epochs, err := pgReadEdgeActiveEpochs(ctx, tx, edgeGroupID)
	if err != nil {
		return nil, nil, err
	}
	controls, err := pgReadEdgeControlNodes(ctx, tx)
	if err != nil {
		return nil, nil, err
	}
	if err := verifyEdgeInstanceMaterialForActivation(instances, epochs, controls, activation); err != nil {
		return nil, nil, err
	}
	nodes, summaries, err := aggregateActiveEdgeNodes(instances, epochs, groups, edgeGroupID)
	if err != nil {
		return nil, nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, nil, fmt.Errorf("commit active route inventory snapshot: %w", err)
	}
	return nodes, summaries, nil
}

func pgReadEdgeActivation(ctx context.Context, db sqlQueryer, forUpdate bool) (model.EdgeActivationState, error) {
	query := `SELECT phase,generation,state_json,created_at,updated_at FROM fugue_edge_activation WHERE singleton=true`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	var phase string
	var generation int64
	var payload []byte
	var createdAt, updatedAt time.Time
	if err := db.QueryRowContext(ctx, query).Scan(&phase, &generation, &payload, &createdAt, &updatedAt); err != nil {
		return model.EdgeActivationState{}, mapDBErr(err)
	}
	if generation <= 0 {
		return model.EdgeActivationState{}, ErrEdgeInstanceFencingNotReady
	}
	var state model.EdgeActivationState
	if err := json.Unmarshal(payload, &state); err != nil {
		return model.EdgeActivationState{}, fmt.Errorf("decode edge activation: %w", err)
	}
	if state.Phase != phase || state.Generation != uint64(generation) || !state.CreatedAt.Equal(createdAt.UTC()) || !state.UpdatedAt.Equal(updatedAt.UTC()) {
		return model.EdgeActivationState{}, ErrEdgeInstanceFencingNotReady
	}
	return normalizeStoredEdgeActivation(state)
}

func verifyEdgeInstanceMaterialForActivation(instances []model.EdgeNodeInstance, epochs []model.EdgeActiveEpoch, controls []model.EdgeNode, activation model.EdgeActivationState) error {
	state := model.State{
		EdgeInstanceFencingSchema: model.EdgeInstanceFencingSchemaV1,
		EdgeNodeInstances:         instances,
		EdgeActiveEpochs:          epochs,
		EdgeNodes:                 controls,
		EdgeActivation:            &activation,
	}
	return verifyEdgeActivationReadiness(&state)
}

func pgReadFullEdgeNodes(ctx context.Context, db sqlQueryer, edgeGroupID string) ([]model.EdgeNode, error) {
	query := `SELECT id, edge_group_id, workload_mode, canary_state, canary_weight, public_probe_status, public_probe_last_error, public_probe_last_at,
	region, country, public_hostname, public_ipv4, public_ipv6, mesh_ip,
	status, healthy, draining, route_bundle_version, dns_bundle_version, caddy_route_count,
	serving_generation, lkg_generation, caddy_applied_version, caddy_last_error, cache_status,
	tls_status, tls_last_message, tls_ready_at, last_error, token_prefix, token_hash, last_seen_at, last_heartbeat_at, created_at, updated_at
	FROM fugue_edge_nodes`
	args := []any{}
	if edgeGroupID != "" {
		query += ` WHERE edge_group_id=$1`
		args = append(args, edgeGroupID)
	}
	query += ` ORDER BY edge_group_id,id`
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list legacy route edge nodes: %w", err)
	}
	defer rows.Close()
	nodes := []model.EdgeNode{}
	for rows.Next() {
		node, err := scanEdgeNode(rows)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, redactEdgeNode(node))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate legacy route edge nodes: %w", err)
	}
	return nodes, nil
}

func pgReadEdgeGroupDefinitions(ctx context.Context, db sqlQueryer, edgeGroupID string) ([]model.EdgeGroup, error) {
	query := `SELECT id,region,country,status,created_at,updated_at FROM fugue_edge_groups`
	args := []any{}
	if edgeGroupID != "" {
		query += ` WHERE id=$1`
		args = append(args, edgeGroupID)
	}
	query += ` ORDER BY id`
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list edge group definitions: %w", err)
	}
	defer rows.Close()
	groups := []model.EdgeGroup{}
	for rows.Next() {
		var group model.EdgeGroup
		if err := rows.Scan(&group.ID, &group.Region, &group.Country, &group.Status, &group.CreatedAt, &group.UpdatedAt); err != nil {
			return nil, err
		}
		groups = append(groups, group)
	}
	return groups, rows.Err()
}
