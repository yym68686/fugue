package schemamigrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	edgeInstanceFencingPoll          = 250 * time.Millisecond
	edgeInstanceFencingLockKey       = "edge-instance-fencing-migration"
	edgeInstanceFencingMetaKey       = "edge_instance_fencing_schema"
	edgeInstanceFencingReceiptKey    = "edge_instance_fencing_receipt"
	edgeInstanceFencingSchema        = "edge-instance-fencing/v1"
	edgeActivationSchema             = "edge-activation/v1"
	edgeActivationLegacyPhase        = "legacy-authoritative"
	edgeActivationLegacyRoute        = "legacy"
	edgeLegacyMigrationSlot          = "legacy"
	edgeLegacyMigrationEpoch         = "legacy-flat-v0"
	edgeInstanceFencingReceiptSchema = "edge-instance-fencing/migration-receipt/v1"
)

var errEdgeInstanceFencingMigrationRequired = errors.New("edge-instance-fencing schema migration is required")

// EdgeInstanceFencingReceipt is an immutable record of the independent
// migration boundary. It deliberately records counts and authority state, but
// never contains edge credentials or node payloads.
type EdgeInstanceFencingReceipt struct {
	Schema               string    `json:"schema"`
	Marker               string    `json:"marker"`
	LegacyRowCount       int64     `json:"legacy_row_count"`
	MigratedRowCount     int64     `json:"migrated_row_count"`
	ActiveEpochCount     int64     `json:"active_epoch_count"`
	ActivationPhase      string    `json:"activation_phase"`
	ActivationGeneration int64     `json:"activation_generation"`
	InstanceUIDAlgorithm string    `json:"instance_uid_algorithm"`
	RecordedAt           time.Time `json:"recorded_at"`
}

// MigrateEdgeInstanceFencing performs the additive edge-instance migration in
// the schema-owned lane. It is safe to rerun and never advances activation.
func MigrateEdgeInstanceFencing(ctx context.Context, databaseURL string) error {
	databaseURL, err := normalizeDatabaseURL(databaseURL)
	if err != nil {
		return err
	}
	migrateCtx, cancel := boundedContext(ctx)
	defer cancel()
	database, err := openDatabase(databaseURL)
	if err != nil {
		return fmt.Errorf("open edge-instance schema database: %w", err)
	}
	defer database.Close()
	if err := waitForEdgeInstanceTables(migrateCtx, database, edgeInstanceFencingPoll); err != nil {
		return err
	}
	if err := applyEdgeInstanceFencing(migrateCtx, database); err != nil {
		return err
	}
	if err := verifyEdgeInstanceFencing(migrateCtx, database); err != nil {
		return err
	}
	return nil
}

func waitForEdgeInstanceTables(ctx context.Context, database *sql.DB, pollInterval time.Duration) error {
	if database == nil {
		return fmt.Errorf("edge-instance schema database is nil")
	}
	if pollInterval <= 0 {
		return fmt.Errorf("edge-instance schema poll interval must be positive")
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		var ready bool
		err := database.QueryRowContext(ctx, `
SELECT to_regclass('fugue_meta') IS NOT NULL
   AND to_regclass('fugue_edge_groups') IS NOT NULL
   AND to_regclass('fugue_edge_nodes') IS NOT NULL
   AND to_regclass('fugue_edge_node_instances') IS NOT NULL
   AND to_regclass('fugue_edge_active_epochs') IS NOT NULL
   AND to_regclass('fugue_edge_activation') IS NOT NULL`).Scan(&ready)
		if err == nil && ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for edge-instance bootstrap tables: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func applyEdgeInstanceFencing(ctx context.Context, database *sql.DB) error {
	tx, err := database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin edge-instance schema migration: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT set_config('lock_timeout', $1, true)`, edgeInstanceFencingLockLimit.String()); err != nil {
		return fmt.Errorf("set edge-instance schema lock timeout: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, edgeInstanceFencingLockKey); err != nil {
		return fmt.Errorf("lock edge-instance schema migration: %w", err)
	}
	if err := ensureEdgeActivationRow(ctx, tx); err != nil {
		return err
	}
	marker, err := readEdgeInstanceMarker(ctx, tx)
	if err != nil {
		return err
	}
	needsCopy := marker == ""
	if marker == "" {
		var instanceCount, epochCount int64
		if err := tx.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM fugue_edge_node_instances), (SELECT count(*) FROM fugue_edge_active_epochs)`).Scan(&instanceCount, &epochCount); err != nil {
			return fmt.Errorf("inspect edge-instance migration state: %w", err)
		}
		if instanceCount != 0 || epochCount != 0 {
			return fmt.Errorf("%w: partial edge-instance schema (instances=%d active_epochs=%d)", errEdgeInstanceFencingMigrationRequired, instanceCount, epochCount)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO fugue_meta (key,value,updated_at) VALUES ($1,$2,NOW())`, edgeInstanceFencingMetaKey, edgeInstanceFencingSchema); err != nil {
			return fmt.Errorf("write edge-instance schema marker: %w", err)
		}
	} else if marker != edgeInstanceFencingSchema {
		return fmt.Errorf("%w: unsupported edge-instance schema marker %q", errEdgeInstanceFencingMigrationRequired, marker)
	}
	if needsCopy {
		if err := copyLegacyEdgeInstances(ctx, tx); err != nil {
			return err
		}
		marker = edgeInstanceFencingSchema
	}
	if err := ensureEdgeInstanceReceipt(ctx, tx, marker); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit edge-instance schema migration: %w", err)
	}
	return nil
}

// edgeInstanceFencingLockLimit intentionally shares the platform migration's
// bounded lock behavior without coupling the two advisory-lock namespaces.
const edgeInstanceFencingLockLimit = 5 * time.Second

func ensureEdgeActivationRow(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
INSERT INTO fugue_edge_activation (singleton, phase, generation, state_json, created_at, updated_at)
WITH stamp AS (SELECT NOW() AS now)
SELECT true, $1::text, 1, jsonb_build_object(
  'schema', $2::text, 'phase', $1::text, 'route_authority', $3::text, 'generation', 1,
  'created_at', stamp.now, 'updated_at', stamp.now), stamp.now, stamp.now
FROM stamp
ON CONFLICT (singleton) DO NOTHING`, edgeActivationLegacyPhase, edgeActivationSchema, edgeActivationLegacyRoute)
	if err != nil {
		return fmt.Errorf("ensure edge activation row: %w", err)
	}
	return nil
}

func readEdgeInstanceMarker(ctx context.Context, queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) (string, error) {
	var marker string
	err := queryer.QueryRowContext(ctx, `SELECT value FROM fugue_meta WHERE key=$1 FOR UPDATE`, edgeInstanceFencingMetaKey).Scan(&marker)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read edge-instance schema marker: %w", err)
	}
	return strings.TrimSpace(marker), nil
}

func copyLegacyEdgeInstances(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `SELECT id, edge_group_id, to_jsonb(n), last_heartbeat_at FROM fugue_edge_nodes AS n ORDER BY id FOR UPDATE`)
	if err != nil {
		return fmt.Errorf("list legacy edge nodes for independent migration: %w", err)
	}
	type legacyRow struct {
		edgeID, groupID string
		nodeJSON        []byte
		lastHeartbeat   sql.NullTime
	}
	legacyRows := make([]legacyRow, 0)
	for rows.Next() {
		var row legacyRow
		if err := rows.Scan(&row.edgeID, &row.groupID, &row.nodeJSON, &row.lastHeartbeat); err != nil {
			return fmt.Errorf("scan legacy edge node for independent migration: %w", err)
		}
		legacyRows = append(legacyRows, row)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate legacy edge nodes for independent migration: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close legacy edge nodes for independent migration: %w", err)
	}
	for _, row := range legacyRows {
		edgeID, groupID, nodeJSON, lastHeartbeat := row.edgeID, row.groupID, row.nodeJSON, row.lastHeartbeat
		var node map[string]any
		if err := json.Unmarshal(nodeJSON, &node); err != nil {
			return fmt.Errorf("decode legacy edge node %q: %w", edgeID, err)
		}
		// Never carry the credential hash into the instance snapshot.
		node["token_hash"] = ""
		snapshot, err := json.Marshal(node)
		if err != nil {
			return fmt.Errorf("encode legacy edge node %q: %w", edgeID, err)
		}
		var heartbeat any
		if lastHeartbeat.Valid {
			heartbeat = lastHeartbeat.Time.UTC()
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_edge_node_instances (
  edge_id, edge_group_id, slot, instance_uid, release_epoch, node_json, failure_class,
  effective_healthy, consecutive_healthy, consecutive_unhealthy, health_state_since,
  last_heartbeat_at, created_at, updated_at
) VALUES ($1,$2,$3,$4,$5,$6,'',false,0,0,NULL,$7,NOW(),NOW())
ON CONFLICT (edge_id, edge_group_id, slot, instance_uid, release_epoch) DO NOTHING`,
			edgeID, groupID, edgeLegacyMigrationSlot, legacyInstanceUID(edgeID), edgeLegacyMigrationEpoch, snapshot, heartbeat); err != nil {
			return fmt.Errorf("copy legacy edge node %q: %w", edgeID, err)
		}
	}
	return nil
}

func legacyInstanceUID(edgeID string) string {
	digest := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(edgeID))))
	return "legacy-" + hex.EncodeToString(digest[:12])
}

func ensureEdgeInstanceReceipt(ctx context.Context, tx *sql.Tx, marker string) error {
	if marker != edgeInstanceFencingSchema {
		return fmt.Errorf("%w: migration marker is %q", errEdgeInstanceFencingMigrationRequired, marker)
	}
	var legacyRows, migratedRows, activeEpochs, activationGeneration int64
	var activationPhase string
	if err := tx.QueryRowContext(ctx, `
SELECT
  (SELECT count(*) FROM fugue_edge_nodes),
  (SELECT count(*) FROM fugue_edge_node_instances WHERE slot=$1 AND release_epoch=$2),
  (SELECT count(*) FROM fugue_edge_active_epochs),
  phase, generation
FROM fugue_edge_activation WHERE singleton=true`, edgeLegacyMigrationSlot, edgeLegacyMigrationEpoch).Scan(&legacyRows, &migratedRows, &activeEpochs, &activationPhase, &activationGeneration); err != nil {
		return fmt.Errorf("inspect edge-instance migration receipt state: %w", err)
	}
	var missing int64
	if err := tx.QueryRowContext(ctx, `
SELECT count(*) FROM fugue_edge_nodes AS n
LEFT JOIN fugue_edge_node_instances AS i
  ON i.edge_id=n.id AND i.edge_group_id=n.edge_group_id AND i.slot=$1 AND i.release_epoch=$2
WHERE i.edge_id IS NULL`, edgeLegacyMigrationSlot, edgeLegacyMigrationEpoch).Scan(&missing); err != nil {
		return fmt.Errorf("verify legacy edge instance mapping: %w", err)
	}
	if missing != 0 || migratedRows < legacyRows {
		return fmt.Errorf("%w: unmapped legacy rows=%d migrated=%d legacy=%d", errEdgeInstanceFencingMigrationRequired, missing, migratedRows, legacyRows)
	}
	receipt := EdgeInstanceFencingReceipt{
		Schema: edgeInstanceFencingReceiptSchema, Marker: marker, LegacyRowCount: legacyRows,
		MigratedRowCount: migratedRows, ActiveEpochCount: activeEpochs,
		ActivationPhase: activationPhase, ActivationGeneration: activationGeneration,
		InstanceUIDAlgorithm: "sha256(normalized-edge-id)-first-12-bytes", RecordedAt: time.Now().UTC(),
	}
	receiptJSON, err := json.Marshal(receipt)
	if err != nil {
		return fmt.Errorf("encode edge-instance migration receipt: %w", err)
	}
	var existing string
	err = tx.QueryRowContext(ctx, `SELECT value FROM fugue_meta WHERE key=$1 FOR UPDATE`, edgeInstanceFencingReceiptKey).Scan(&existing)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, err := tx.ExecContext(ctx, `INSERT INTO fugue_meta (key,value,updated_at) VALUES ($1,$2,NOW())`, edgeInstanceFencingReceiptKey, string(receiptJSON)); err != nil {
			return fmt.Errorf("write edge-instance migration receipt: %w", err)
		}
	case err != nil:
		return fmt.Errorf("read edge-instance migration receipt: %w", err)
	default:
		var prior EdgeInstanceFencingReceipt
		if err := json.Unmarshal([]byte(existing), &prior); err != nil {
			return fmt.Errorf("decode edge-instance migration receipt: %w", err)
		}
		if prior.Schema != edgeInstanceFencingReceiptSchema || prior.Marker != edgeInstanceFencingSchema ||
			prior.MigratedRowCount < prior.LegacyRowCount || strings.TrimSpace(prior.InstanceUIDAlgorithm) == "" || prior.RecordedAt.IsZero() {
			return fmt.Errorf("%w: invalid immutable migration receipt", errEdgeInstanceFencingMigrationRequired)
		}
	}
	return nil
}

func verifyEdgeInstanceFencing(ctx context.Context, database *sql.DB) error {
	var marker string
	if err := database.QueryRowContext(ctx, `SELECT value FROM fugue_meta WHERE key=$1`, edgeInstanceFencingMetaKey).Scan(&marker); err != nil {
		return fmt.Errorf("verify edge-instance schema marker: %w", err)
	}
	if strings.TrimSpace(marker) != edgeInstanceFencingSchema {
		return fmt.Errorf("%w: marker=%q", errEdgeInstanceFencingMigrationRequired, marker)
	}
	var receipt string
	if err := database.QueryRowContext(ctx, `SELECT value FROM fugue_meta WHERE key=$1`, edgeInstanceFencingReceiptKey).Scan(&receipt); err != nil {
		return fmt.Errorf("verify edge-instance migration receipt: %w", err)
	}
	var decoded EdgeInstanceFencingReceipt
	if err := json.Unmarshal([]byte(receipt), &decoded); err != nil || decoded.Schema != edgeInstanceFencingReceiptSchema || decoded.Marker != edgeInstanceFencingSchema {
		return fmt.Errorf("%w: invalid migration receipt", errEdgeInstanceFencingMigrationRequired)
	}
	var missing int64
	if err := database.QueryRowContext(ctx, `
SELECT count(*) FROM fugue_edge_nodes AS n
LEFT JOIN fugue_edge_node_instances AS i
  ON i.edge_id=n.id AND i.edge_group_id=n.edge_group_id AND i.slot=$1 AND i.release_epoch=$2
WHERE i.edge_id IS NULL`, edgeLegacyMigrationSlot, edgeLegacyMigrationEpoch).Scan(&missing); err != nil {
		return fmt.Errorf("verify edge-instance migration state: %w", err)
	}
	if missing != 0 {
		return fmt.Errorf("%w: current legacy rows missing instance mapping=%d", errEdgeInstanceFencingMigrationRequired, missing)
	}
	return nil
}
