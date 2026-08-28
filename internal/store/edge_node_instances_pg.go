package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"fugue/internal/model"
)

const edgeInstanceSelectColumns = `edge_id, edge_group_id, slot, instance_uid, release_epoch,
	node_json, failure_class, effective_healthy, consecutive_healthy, consecutive_unhealthy,
	health_state_since, last_heartbeat_at, created_at, updated_at`

const edgeActiveEpochSelectColumns = `edge_group_id, slot, release_epoch, fence_sequence,
	min_healthy_instances, activated_at, created_at, updated_at`

const edgeInstanceFencingMetaKey = "edge_instance_fencing_schema"

func (s *Store) pgEnsureEdgeInstanceFencing() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin edge instance migration: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "edge-instance-fencing-migration"); err != nil {
		return fmt.Errorf("lock edge instance migration: %w", err)
	}
	now, err := pgEdgeServerTime(ctx, tx)
	if err != nil {
		return err
	}
	defaultActivation := defaultEdgeActivationState(now)
	defaultActivationJSON, err := json.Marshal(defaultActivation)
	if err != nil {
		return fmt.Errorf("encode default edge activation: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO fugue_edge_activation (singleton, phase, generation, state_json, created_at, updated_at)
VALUES (true,$1,$2,$3,$4,$4) ON CONFLICT (singleton) DO NOTHING`, defaultActivation.Phase, defaultActivation.Generation, defaultActivationJSON, now); err != nil {
		return fmt.Errorf("ensure default edge activation: %w", err)
	}

	var schema string
	err = tx.QueryRowContext(ctx, `SELECT value FROM fugue_meta WHERE key=$1 FOR UPDATE`, edgeInstanceFencingMetaKey).Scan(&schema)
	if err == nil {
		if schema != model.EdgeInstanceFencingSchemaV1 {
			return fmt.Errorf("%w: unsupported postgres schema %q", ErrEdgeInstanceFencingNotReady, schema)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit edge instance migration check: %w", err)
		}
		return s.pgVerifyEdgeActivationReadiness(ctx)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read edge instance schema marker: %w", err)
	}
	var instanceCount, epochCount int
	if err := tx.QueryRowContext(ctx, `SELECT
		(SELECT count(*) FROM fugue_edge_node_instances),
		(SELECT count(*) FROM fugue_edge_active_epochs)`).Scan(&instanceCount, &epochCount); err != nil {
		return fmt.Errorf("inspect edge instance migration state: %w", err)
	}
	if instanceCount != 0 || epochCount != 0 {
		return fmt.Errorf("%w: partial postgres edge instance schema", ErrEdgeInstanceFencingNotReady)
	}

	rows, err := tx.QueryContext(ctx, `
SELECT id, edge_group_id, workload_mode, canary_state, canary_weight, public_probe_status, public_probe_last_error, public_probe_last_at,
	region, country, public_hostname, public_ipv4, public_ipv6, mesh_ip,
	status, healthy, draining, route_bundle_version, dns_bundle_version, caddy_route_count,
	serving_generation, lkg_generation, caddy_applied_version, caddy_last_error, cache_status,
	tls_status, tls_last_message, tls_ready_at, last_error, token_prefix, token_hash, last_seen_at, last_heartbeat_at, created_at, updated_at
FROM fugue_edge_nodes ORDER BY id FOR UPDATE`)
	if err != nil {
		return fmt.Errorf("list legacy edge nodes for migration: %w", err)
	}
	legacyNodes := []model.EdgeNode{}
	for rows.Next() {
		node, err := scanEdgeNode(rows)
		if err != nil {
			rows.Close()
			return err
		}
		legacyNodes = append(legacyNodes, node)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate legacy edge nodes for migration: %w", err)
	}
	rows.Close()
	for _, node := range legacyNodes {
		node.TokenHash = ""
		nodeJSON, err := json.Marshal(node)
		if err != nil {
			return fmt.Errorf("encode legacy edge node: %w", err)
		}
		lastHeartbeatAt := any(nil)
		if node.LastHeartbeatAt != nil {
			lastHeartbeatAt = node.LastHeartbeatAt.UTC()
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_edge_node_instances (
	edge_id, edge_group_id, slot, instance_uid, release_epoch, node_json, failure_class,
	effective_healthy, consecutive_healthy, consecutive_unhealthy, health_state_since,
	last_heartbeat_at, created_at, updated_at
) VALUES ($1,$2,$3,$4,$5,$6,'',false,0,0,NULL,$7,$8,$8)`,
			node.ID, node.EdgeGroupID, edgeLegacyMigrationSlot, legacyEdgeInstanceUID(node.ID), edgeLegacyMigrationEpoch,
			nodeJSON, lastHeartbeatAt, now); err != nil {
			return mapDBErr(err)
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO fugue_meta (key,value,updated_at)
VALUES ($1,$2,$3)`, edgeInstanceFencingMetaKey, model.EdgeInstanceFencingSchemaV1, now); err != nil {
		return fmt.Errorf("write edge instance schema marker: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit edge instance migration: %w", err)
	}
	return s.pgVerifyEdgeActivationReadiness(ctx)
}

func (s *Store) pgVerifyEdgeInstanceFencing(ctx context.Context) error {
	return s.pgVerifyEdgeActivationReadiness(ctx)
}

func (s *Store) pgPutEdgeActiveEpoch(epoch model.EdgeActiveEpoch) (model.EdgeActiveEpoch, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.EdgeActiveEpoch{}, fmt.Errorf("begin active edge epoch transaction: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, pgEdgeAdvisoryLockKey("active-epoch", epoch.EdgeGroupID)); err != nil {
		return model.EdgeActiveEpoch{}, fmt.Errorf("lock active edge epoch: %w", err)
	}

	now, err := pgEdgeServerTime(ctx, tx)
	if err != nil {
		return model.EdgeActiveEpoch{}, err
	}
	current, err := scanEdgeActiveEpoch(tx.QueryRowContext(ctx, `SELECT `+edgeActiveEpochSelectColumns+`
FROM fugue_edge_active_epochs WHERE edge_group_id = $1 FOR UPDATE`, epoch.EdgeGroupID))
	switch {
	case err == nil:
		if epoch.FenceSequence < current.FenceSequence || (epoch.FenceSequence == current.FenceSequence && !sameEdgeActiveEpochIdentity(current, epoch)) {
			return model.EdgeActiveEpoch{}, ErrConflict
		}
		if epoch.FenceSequence == current.FenceSequence {
			return current, nil
		}
		epoch.CreatedAt = current.CreatedAt
	case errors.Is(err, sql.ErrNoRows):
		epoch.CreatedAt = now
	default:
		return model.EdgeActiveEpoch{}, mapDBErr(err)
	}
	if epoch.ActivatedAt.IsZero() {
		epoch.ActivatedAt = now
	}
	epoch.UpdatedAt = now
	stored, err := scanEdgeActiveEpoch(tx.QueryRowContext(ctx, `
INSERT INTO fugue_edge_active_epochs (
	edge_group_id, slot, release_epoch, fence_sequence, min_healthy_instances,
	activated_at, created_at, updated_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT (edge_group_id) DO UPDATE SET
	slot = EXCLUDED.slot,
	release_epoch = EXCLUDED.release_epoch,
	fence_sequence = EXCLUDED.fence_sequence,
	min_healthy_instances = EXCLUDED.min_healthy_instances,
	activated_at = EXCLUDED.activated_at,
	updated_at = EXCLUDED.updated_at
WHERE fugue_edge_active_epochs.fence_sequence < EXCLUDED.fence_sequence
RETURNING `+edgeActiveEpochSelectColumns,
		epoch.EdgeGroupID, epoch.Slot, epoch.ReleaseEpoch, epoch.FenceSequence, epoch.MinHealthyInstances,
		epoch.ActivatedAt, epoch.CreatedAt, epoch.UpdatedAt))
	if errors.Is(err, sql.ErrNoRows) {
		return model.EdgeActiveEpoch{}, ErrConflict
	}
	if err != nil {
		return model.EdgeActiveEpoch{}, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.EdgeActiveEpoch{}, fmt.Errorf("commit active edge epoch transaction: %w", err)
	}
	return stored, nil
}

func (s *Store) pgGetEdgeActiveEpoch(edgeGroupID string) (model.EdgeActiveEpoch, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	epoch, err := scanEdgeActiveEpoch(s.db.QueryRowContext(ctx, `SELECT `+edgeActiveEpochSelectColumns+`
FROM fugue_edge_active_epochs WHERE edge_group_id = $1`, edgeGroupID))
	if err != nil {
		return model.EdgeActiveEpoch{}, mapDBErr(err)
	}
	return epoch, nil
}

func (s *Store) pgListEdgeNodeInstances(edgeGroupID string) ([]model.EdgeNodeInstance, []model.EdgeActiveEpoch, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, nil, fmt.Errorf("begin edge instance inventory snapshot: %w", err)
	}
	defer tx.Rollback()
	activation, err := pgReadEdgeActivation(ctx, tx, false)
	if err != nil {
		return nil, nil, err
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
	if err := tx.Commit(); err != nil {
		return nil, nil, fmt.Errorf("commit edge instance inventory snapshot: %w", err)
	}
	for index := range instances {
		instances[index].Node = redactEdgeNode(instances[index].Node)
	}
	return instances, epochs, nil
}

func (s *Store) pgUpdateEdgeInstanceHeartbeatAt(instance model.EdgeNodeInstance, _ time.Time) (model.EdgeNodeInstance, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.EdgeNodeInstance{}, fmt.Errorf("begin edge instance heartbeat transaction: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, pgEdgeAdvisoryLockKey("instance-heartbeat", edgeNodeInstanceKey(instance))); err != nil {
		return model.EdgeNodeInstance{}, fmt.Errorf("lock edge instance heartbeat: %w", err)
	}
	now, err := pgEdgeServerTime(ctx, tx)
	if err != nil {
		return model.EdgeNodeInstance{}, err
	}

	previous, err := scanEdgeNodeInstance(tx.QueryRowContext(ctx, `SELECT `+edgeInstanceSelectColumns+`
FROM fugue_edge_node_instances
WHERE edge_id=$1 AND edge_group_id=$2 AND slot=$3 AND instance_uid=$4 AND release_epoch=$5
FOR UPDATE`, instance.EdgeID, instance.EdgeGroupID, instance.Slot, instance.InstanceUID, instance.ReleaseEpoch))
	switch {
	case err == nil:
		instance.CreatedAt = previous.CreatedAt
	case errors.Is(err, sql.ErrNoRows):
		instance.CreatedAt = now
		previous = model.EdgeNodeInstance{}
	default:
		return model.EdgeNodeInstance{}, mapDBErr(err)
	}
	applyEdgeInstanceHealthTransition(&instance, previous, now)
	instance.LastHeartbeatAt = now
	instance.UpdatedAt = now
	instance.Node.LastSeenAt = edgeInstanceTimePtr(now)
	instance.Node.LastHeartbeatAt = edgeInstanceTimePtr(now)
	instance.Node.UpdatedAt = now
	if instance.Node.CreatedAt.IsZero() {
		instance.Node.CreatedAt = instance.CreatedAt
	}
	nodeJSON, err := json.Marshal(instance.Node)
	if err != nil {
		return model.EdgeNodeInstance{}, fmt.Errorf("encode edge instance node: %w", err)
	}
	stored, err := scanEdgeNodeInstance(tx.QueryRowContext(ctx, `
INSERT INTO fugue_edge_node_instances (
	edge_id, edge_group_id, slot, instance_uid, release_epoch, node_json, failure_class,
	effective_healthy, consecutive_healthy, consecutive_unhealthy, health_state_since,
	last_heartbeat_at, created_at, updated_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
ON CONFLICT (edge_id, edge_group_id, slot, instance_uid, release_epoch) DO UPDATE SET
	node_json=EXCLUDED.node_json,
	failure_class=EXCLUDED.failure_class,
	effective_healthy=EXCLUDED.effective_healthy,
	consecutive_healthy=EXCLUDED.consecutive_healthy,
	consecutive_unhealthy=EXCLUDED.consecutive_unhealthy,
	health_state_since=EXCLUDED.health_state_since,
	last_heartbeat_at=EXCLUDED.last_heartbeat_at,
	updated_at=EXCLUDED.updated_at
RETURNING `+edgeInstanceSelectColumns,
		instance.EdgeID, instance.EdgeGroupID, instance.Slot, instance.InstanceUID, instance.ReleaseEpoch,
		nodeJSON, instance.FailureClass, instance.EffectiveHealthy, instance.ConsecutiveHealthy,
		instance.ConsecutiveUnhealthy, nullTime(instance.HealthStateSince), nullTime(instance.LastHeartbeatAt),
		instance.CreatedAt, instance.UpdatedAt))
	if err != nil {
		return model.EdgeNodeInstance{}, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.EdgeNodeInstance{}, fmt.Errorf("commit edge instance heartbeat transaction: %w", err)
	}
	return stored, nil
}

func pgEdgeAdvisoryLockKey(scope, identity string) string {
	sum := sha256.Sum256([]byte(scope + "\x00" + identity))
	return fmt.Sprintf("fugue-edge:%s:%x", scope, sum[:])
}

func (s *Store) pgListActiveEdgeInstanceMaterial(edgeGroupID string) ([]model.EdgeNodeInstance, []model.EdgeActiveEpoch, []model.EdgeGroup, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	instances, epochs, err := s.pgListEdgeNodeInstances(edgeGroupID)
	if err != nil {
		return nil, nil, nil, err
	}
	groups, err := s.pgListEdgeGroups(ctx, edgeGroupID)
	if err != nil {
		return nil, nil, nil, err
	}
	return instances, epochs, groups, nil
}

func pgReadEdgeNodeInstances(ctx context.Context, db sqlQueryer, edgeGroupID string) ([]model.EdgeNodeInstance, error) {
	query := `SELECT ` + edgeInstanceSelectColumns + ` FROM fugue_edge_node_instances`
	args := []any{}
	if edgeGroupID != "" {
		query += ` WHERE edge_group_id=$1`
		args = append(args, edgeGroupID)
	}
	query += ` ORDER BY edge_group_id, edge_id, slot, instance_uid, release_epoch`
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list edge node instances: %w", err)
	}
	defer rows.Close()
	instances := []model.EdgeNodeInstance{}
	for rows.Next() {
		instance, err := scanEdgeNodeInstance(rows)
		if err != nil {
			return nil, err
		}
		instances = append(instances, instance)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate edge node instances: %w", err)
	}
	return instances, nil
}

func pgReadEdgeActiveEpochs(ctx context.Context, db sqlQueryer, edgeGroupID string) ([]model.EdgeActiveEpoch, error) {
	query := `SELECT ` + edgeActiveEpochSelectColumns + ` FROM fugue_edge_active_epochs`
	args := []any{}
	if edgeGroupID != "" {
		query += ` WHERE edge_group_id=$1`
		args = append(args, edgeGroupID)
	}
	query += ` ORDER BY edge_group_id`
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list active edge epochs: %w", err)
	}
	defer rows.Close()
	epochs := []model.EdgeActiveEpoch{}
	for rows.Next() {
		epoch, err := scanEdgeActiveEpoch(rows)
		if err != nil {
			return nil, err
		}
		epochs = append(epochs, epoch)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active edge epochs: %w", err)
	}
	return epochs, nil
}

func scanEdgeNodeInstance(scanner sqlScanner) (model.EdgeNodeInstance, error) {
	var instance model.EdgeNodeInstance
	var nodeJSON []byte
	var healthStateSince, lastHeartbeatAt sql.NullTime
	if err := scanner.Scan(
		&instance.EdgeID, &instance.EdgeGroupID, &instance.Slot, &instance.InstanceUID, &instance.ReleaseEpoch,
		&nodeJSON, &instance.FailureClass, &instance.EffectiveHealthy, &instance.ConsecutiveHealthy,
		&instance.ConsecutiveUnhealthy, &healthStateSince, &lastHeartbeatAt, &instance.CreatedAt, &instance.UpdatedAt,
	); err != nil {
		return model.EdgeNodeInstance{}, err
	}
	if err := json.Unmarshal(nodeJSON, &instance.Node); err != nil {
		return model.EdgeNodeInstance{}, fmt.Errorf("decode edge instance node: %w", err)
	}
	if healthStateSince.Valid {
		instance.HealthStateSince = healthStateSince.Time.UTC()
	}
	if lastHeartbeatAt.Valid {
		instance.LastHeartbeatAt = lastHeartbeatAt.Time.UTC()
	}
	instance.CreatedAt = instance.CreatedAt.UTC()
	instance.UpdatedAt = instance.UpdatedAt.UTC()
	return normalizeStoredEdgeNodeInstance(instance)
}

func scanEdgeActiveEpoch(scanner sqlScanner) (model.EdgeActiveEpoch, error) {
	var epoch model.EdgeActiveEpoch
	var fenceSequence int64
	if err := scanner.Scan(&epoch.EdgeGroupID, &epoch.Slot, &epoch.ReleaseEpoch, &fenceSequence,
		&epoch.MinHealthyInstances, &epoch.ActivatedAt, &epoch.CreatedAt, &epoch.UpdatedAt); err != nil {
		return model.EdgeActiveEpoch{}, err
	}
	if fenceSequence <= 0 {
		return model.EdgeActiveEpoch{}, ErrEdgeInstanceFencingNotReady
	}
	epoch.FenceSequence = uint64(fenceSequence)
	return normalizeEdgeActiveEpoch(epoch)
}

func verifyEdgeInstanceMaterial(instances []model.EdgeNodeInstance, epochs []model.EdgeActiveEpoch, controls []model.EdgeNode) error {
	state := model.State{
		EdgeInstanceFencingSchema: model.EdgeInstanceFencingSchemaV1,
		EdgeNodeInstances:         instances,
		EdgeActiveEpochs:          epochs,
		EdgeNodes:                 controls,
	}
	return verifyEdgeInstanceState(&state)
}

func pgReadEdgeControlNodes(ctx context.Context, db sqlQueryer) ([]model.EdgeNode, error) {
	rows, err := db.QueryContext(ctx, `SELECT id, edge_group_id FROM fugue_edge_nodes ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list edge control identities: %w", err)
	}
	defer rows.Close()
	controls := []model.EdgeNode{}
	for rows.Next() {
		var node model.EdgeNode
		if err := rows.Scan(&node.ID, &node.EdgeGroupID); err != nil {
			return nil, err
		}
		controls = append(controls, node)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate edge control identities: %w", err)
	}
	return controls, nil
}

func pgEdgeServerTime(ctx context.Context, tx *sql.Tx) (time.Time, error) {
	var now time.Time
	if err := tx.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return time.Time{}, fmt.Errorf("read postgres edge heartbeat time: %w", err)
	}
	return now.UTC(), nil
}
