package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"fugue/internal/model"

	"github.com/jackc/pgx/v5/pgconn"
)

const (
	postgresBootstrapLockID  int64  = 315609238744281
	PostgresOperationChannel string = "fugue_operations"
)

var postgresSchemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS fugue_meta (
		key TEXT PRIMARY KEY,
		value TEXT NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	)`,
	`CREATE TABLE IF NOT EXISTS fugue_tenants (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		slug TEXT NOT NULL UNIQUE,
		status TEXT NOT NULL,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS fugue_projects (
		id TEXT PRIMARY KEY,
		tenant_id TEXT REFERENCES fugue_tenants(id) ON DELETE CASCADE,
		name TEXT NOT NULL,
		slug TEXT NOT NULL,
		description TEXT NOT NULL DEFAULT '',
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL
	)`,
	`CREATE UNIQUE INDEX IF NOT EXISTS idx_fugue_projects_tenant_slug ON fugue_projects (tenant_id, slug)`,
	`CREATE TABLE IF NOT EXISTS fugue_api_keys (
		id TEXT PRIMARY KEY,
		tenant_id TEXT NULL REFERENCES fugue_tenants(id) ON DELETE CASCADE,
		label TEXT NOT NULL,
		prefix TEXT NOT NULL,
		hash TEXT NOT NULL UNIQUE,
		scopes_json JSONB NOT NULL,
		created_at TIMESTAMPTZ NOT NULL,
		last_used_at TIMESTAMPTZ NULL
	)`,
	`CREATE TABLE IF NOT EXISTS fugue_enrollment_tokens (
		id TEXT PRIMARY KEY,
		tenant_id TEXT NULL REFERENCES fugue_tenants(id) ON DELETE CASCADE,
		label TEXT NOT NULL,
		prefix TEXT NOT NULL,
		hash TEXT NOT NULL UNIQUE,
		expires_at TIMESTAMPTZ NOT NULL,
		used_at TIMESTAMPTZ NULL,
		created_at TIMESTAMPTZ NOT NULL,
		last_used_at TIMESTAMPTZ NULL
	)`,
	`CREATE TABLE IF NOT EXISTS fugue_node_keys (
		id TEXT PRIMARY KEY,
		tenant_id TEXT NULL REFERENCES fugue_tenants(id) ON DELETE CASCADE,
		label TEXT NOT NULL,
		prefix TEXT NOT NULL,
		hash TEXT NOT NULL UNIQUE,
		status TEXT NOT NULL,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		last_used_at TIMESTAMPTZ NULL,
		revoked_at TIMESTAMPTZ NULL
	)`,
	`CREATE TABLE IF NOT EXISTS fugue_machines (
		id TEXT PRIMARY KEY,
		tenant_id TEXT NULL REFERENCES fugue_tenants(id) ON DELETE CASCADE,
		name TEXT NOT NULL,
		connection_mode TEXT NOT NULL,
		status TEXT NOT NULL,
		endpoint TEXT NOT NULL DEFAULT '',
		labels_json JSONB NULL,
		node_key_id TEXT NULL REFERENCES fugue_node_keys(id) ON DELETE SET NULL,
		runtime_id TEXT NOT NULL DEFAULT '',
		runtime_name TEXT NOT NULL DEFAULT '',
		cluster_node_name TEXT NOT NULL DEFAULT '',
		fingerprint_prefix TEXT NOT NULL DEFAULT '',
		fingerprint_hash TEXT NOT NULL DEFAULT '',
		last_seen_at TIMESTAMPTZ NULL,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL
	)`,
	`CREATE UNIQUE INDEX IF NOT EXISTS idx_fugue_machines_tenant_fingerprint_hash ON fugue_machines ((COALESCE(tenant_id, '')), fingerprint_hash) WHERE fingerprint_hash <> ''`,
	`CREATE UNIQUE INDEX IF NOT EXISTS idx_fugue_machines_runtime_id ON fugue_machines (runtime_id) WHERE runtime_id <> ''`,
	`CREATE INDEX IF NOT EXISTS idx_fugue_machines_node_key_id ON fugue_machines (node_key_id) WHERE node_key_id IS NOT NULL`,
	`CREATE TABLE IF NOT EXISTS fugue_runtimes (
		id TEXT PRIMARY KEY,
		tenant_id TEXT NULL REFERENCES fugue_tenants(id) ON DELETE CASCADE,
		name TEXT NOT NULL,
		type TEXT NOT NULL,
		status TEXT NOT NULL,
		endpoint TEXT NOT NULL DEFAULT '',
		labels_json JSONB NULL,
		node_key_id TEXT NULL REFERENCES fugue_node_keys(id) ON DELETE SET NULL,
		agent_key_prefix TEXT NOT NULL DEFAULT '',
		agent_key_hash TEXT NOT NULL DEFAULT '',
		last_heartbeat_at TIMESTAMPTZ NULL,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL
	)`,
	`CREATE UNIQUE INDEX IF NOT EXISTS idx_fugue_runtimes_tenant_name_ci ON fugue_runtimes ((COALESCE(tenant_id, '')), lower(name))`,
	`CREATE UNIQUE INDEX IF NOT EXISTS idx_fugue_runtimes_agent_key_hash ON fugue_runtimes (agent_key_hash) WHERE agent_key_hash <> ''`,
	`CREATE TABLE IF NOT EXISTS fugue_apps (
		id TEXT PRIMARY KEY,
		tenant_id TEXT NOT NULL REFERENCES fugue_tenants(id) ON DELETE CASCADE,
		project_id TEXT NOT NULL REFERENCES fugue_projects(id) ON DELETE CASCADE,
		name TEXT NOT NULL,
		description TEXT NOT NULL DEFAULT '',
		source_json JSONB NULL,
		route_json JSONB NULL,
		spec_json JSONB NOT NULL,
		status_json JSONB NOT NULL,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL
	)`,
	`CREATE UNIQUE INDEX IF NOT EXISTS idx_fugue_apps_tenant_project_name_ci ON fugue_apps (tenant_id, project_id, lower(name))`,
	`CREATE UNIQUE INDEX IF NOT EXISTS idx_fugue_apps_route_hostname_ci ON fugue_apps (lower((route_json->>'hostname'))) WHERE route_json IS NOT NULL AND COALESCE(route_json->>'hostname', '') <> ''`,
	`CREATE TABLE IF NOT EXISTS fugue_idempotency_keys (
		scope TEXT NOT NULL,
		tenant_id TEXT NOT NULL REFERENCES fugue_tenants(id) ON DELETE CASCADE,
		key TEXT NOT NULL,
		request_hash TEXT NOT NULL,
		status TEXT NOT NULL,
		app_id TEXT NOT NULL DEFAULT '',
		operation_id TEXT NOT NULL DEFAULT '',
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (scope, tenant_id, key)
	)`,
	`CREATE TABLE IF NOT EXISTS fugue_operations (
		id TEXT PRIMARY KEY,
		tenant_id TEXT NOT NULL REFERENCES fugue_tenants(id) ON DELETE CASCADE,
		type TEXT NOT NULL,
		status TEXT NOT NULL,
		execution_mode TEXT NOT NULL,
		requested_by_type TEXT NOT NULL,
		requested_by_id TEXT NOT NULL,
		app_id TEXT NOT NULL REFERENCES fugue_apps(id) ON DELETE CASCADE,
		source_runtime_id TEXT NOT NULL DEFAULT '',
		target_runtime_id TEXT NOT NULL DEFAULT '',
		desired_replicas INTEGER NULL,
		desired_spec_json JSONB NULL,
		desired_source_json JSONB NULL,
		result_message TEXT NOT NULL DEFAULT '',
		manifest_path TEXT NOT NULL DEFAULT '',
		assigned_runtime_id TEXT NOT NULL DEFAULT '',
		error_message TEXT NOT NULL DEFAULT '',
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		started_at TIMESTAMPTZ NULL,
		completed_at TIMESTAMPTZ NULL
	)`,
	`CREATE INDEX IF NOT EXISTS idx_fugue_operations_status_created_at ON fugue_operations (status, created_at)`,
	`CREATE INDEX IF NOT EXISTS idx_fugue_operations_assigned_runtime_status_created_at ON fugue_operations (assigned_runtime_id, status, created_at)`,
	`CREATE TABLE IF NOT EXISTS fugue_audit_events (
		id TEXT PRIMARY KEY,
		tenant_id TEXT NULL,
		actor_type TEXT NOT NULL,
		actor_id TEXT NOT NULL,
		action TEXT NOT NULL,
		target_type TEXT NOT NULL,
		target_id TEXT NOT NULL DEFAULT '',
		metadata_json JSONB NULL,
		created_at TIMESTAMPTZ NOT NULL
	)`,
	`CREATE INDEX IF NOT EXISTS idx_fugue_audit_events_created_at ON fugue_audit_events (created_at DESC)`,
	`CREATE INDEX IF NOT EXISTS idx_fugue_audit_events_tenant_created_at ON fugue_audit_events (tenant_id, created_at DESC)`,
}

func (s *Store) ensureDatabaseReady() error {
	s.dbInitMu.Lock()
	defer s.dbInitMu.Unlock()

	if s.dbReady {
		return nil
	}
	if strings.TrimSpace(s.databaseURL) == "" {
		return fmt.Errorf("database url is empty")
	}

	if s.db == nil {
		db, err := sql.Open("pgx", s.databaseURL)
		if err != nil {
			return fmt.Errorf("open postgres: %w", err)
		}
		s.db = db
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := s.db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}
	if err := s.bootstrapDatabase(ctx); err != nil {
		return err
	}

	s.dbReady = true
	return nil
}

func (s *Store) bootstrapDatabase(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin bootstrap transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1)", postgresBootstrapLockID); err != nil {
		return fmt.Errorf("acquire postgres advisory lock: %w", err)
	}
	for _, stmt := range postgresSchemaStatements {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("apply postgres schema: %w", err)
		}
	}

	schemaVersion, exists, err := s.getMetaTx(ctx, tx, "schema_version")
	if err != nil {
		return err
	}
	if !exists || schemaVersion != "relational-v1" {
		state, source, err := s.loadBootstrapStateTx(ctx, tx)
		if err != nil {
			return err
		}
		ensureDefaults(&state)
		if err := s.importLegacyStateTx(ctx, tx, state); err != nil {
			return err
		}
		if err := s.upsertMetaTx(ctx, tx, "schema_version", "relational-v1"); err != nil {
			return err
		}
		if err := s.upsertMetaTx(ctx, tx, "legacy_import_source", source); err != nil {
			return err
		}
	}
	if err := s.ensureManagedRuntimeTx(ctx, tx); err != nil {
		return err
	}
	if err := s.ensureMachinesTx(ctx, tx); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit bootstrap transaction: %w", err)
	}
	return nil
}

func (s *Store) getMetaTx(ctx context.Context, tx *sql.Tx, key string) (string, bool, error) {
	var value string
	err := tx.QueryRowContext(ctx, `SELECT value FROM fugue_meta WHERE key = $1`, key).Scan(&value)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("query fugue_meta %s: %w", key, err)
	}
	return value, true, nil
}

func (s *Store) upsertMetaTx(ctx context.Context, tx *sql.Tx, key, value string) error {
	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_meta (key, value, updated_at)
VALUES ($1, $2, NOW())
ON CONFLICT (key) DO UPDATE
SET value = EXCLUDED.value, updated_at = NOW()
`, key, value); err != nil {
		return fmt.Errorf("upsert fugue_meta %s: %w", key, err)
	}
	return nil
}

func (s *Store) loadBootstrapStateTx(ctx context.Context, tx *sql.Tx) (model.State, string, error) {
	if state, ok, err := s.loadLegacyTableStateTx(ctx, tx); err != nil {
		return model.State{}, "", err
	} else if ok {
		return state, "fugue_state", nil
	}
	if state, ok, err := s.loadLegacyFileState(); err != nil {
		return model.State{}, "", err
	} else if ok {
		return state, "store.json", nil
	}
	return model.State{}, "empty", nil
}

func (s *Store) loadLegacyTableStateTx(ctx context.Context, tx *sql.Tx) (model.State, bool, error) {
	var exists bool
	if err := tx.QueryRowContext(ctx, `SELECT to_regclass('public.fugue_state') IS NOT NULL`).Scan(&exists); err != nil {
		return model.State{}, false, fmt.Errorf("check legacy fugue_state table: %w", err)
	}
	if !exists {
		return model.State{}, false, nil
	}

	var raw []byte
	err := tx.QueryRowContext(ctx, `SELECT state FROM fugue_state WHERE id = 1`).Scan(&raw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.State{}, false, nil
		}
		return model.State{}, false, fmt.Errorf("read legacy fugue_state row: %w", err)
	}
	if len(raw) == 0 {
		return model.State{}, false, nil
	}

	var state model.State
	if err := json.Unmarshal(raw, &state); err != nil {
		return model.State{}, false, fmt.Errorf("unmarshal legacy fugue_state row: %w", err)
	}
	return state, true, nil
}

func (s *Store) loadLegacyFileState() (model.State, bool, error) {
	if strings.TrimSpace(s.path) == "" {
		return model.State{}, false, nil
	}

	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return model.State{}, false, nil
		}
		return model.State{}, false, fmt.Errorf("read legacy state file: %w", err)
	}
	if len(data) == 0 {
		return model.State{}, false, nil
	}

	var state model.State
	if err := json.Unmarshal(data, &state); err != nil {
		return model.State{}, false, fmt.Errorf("unmarshal legacy state file: %w", err)
	}
	return state, true, nil
}

func (s *Store) importLegacyStateTx(ctx context.Context, tx *sql.Tx, state model.State) error {
	for _, tenant := range state.Tenants {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_tenants (id, name, slug, status, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (id) DO NOTHING
`, tenant.ID, tenant.Name, tenant.Slug, tenant.Status, tenant.CreatedAt, tenant.UpdatedAt); err != nil {
			return fmt.Errorf("import tenant %s: %w", tenant.ID, err)
		}
	}
	for _, project := range state.Projects {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_projects (id, tenant_id, name, slug, description, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (id) DO NOTHING
`, project.ID, nullIfEmpty(project.TenantID), project.Name, project.Slug, project.Description, project.CreatedAt, project.UpdatedAt); err != nil {
			return fmt.Errorf("import project %s: %w", project.ID, err)
		}
	}
	for _, key := range state.APIKeys {
		scopesJSON, err := marshalJSON(model.NormalizeScopes(key.Scopes))
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_api_keys (id, tenant_id, label, prefix, hash, scopes_json, created_at, last_used_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (id) DO NOTHING
`, key.ID, nullIfEmpty(key.TenantID), key.Label, key.Prefix, key.Hash, scopesJSON, key.CreatedAt, key.LastUsedAt); err != nil {
			return fmt.Errorf("import api key %s: %w", key.ID, err)
		}
	}
	for _, token := range state.EnrollmentTokens {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_enrollment_tokens (id, tenant_id, label, prefix, hash, expires_at, used_at, created_at, last_used_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (id) DO NOTHING
`, token.ID, nullIfEmpty(token.TenantID), token.Label, token.Prefix, token.Hash, token.ExpiresAt, token.UsedAt, token.CreatedAt, token.LastUsedAt); err != nil {
			return fmt.Errorf("import enrollment token %s: %w", token.ID, err)
		}
	}
	for _, key := range state.NodeKeys {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_node_keys (id, tenant_id, label, prefix, hash, status, created_at, updated_at, last_used_at, revoked_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (id) DO NOTHING
`, key.ID, nullIfEmpty(key.TenantID), key.Label, key.Prefix, key.Hash, key.Status, key.CreatedAt, key.UpdatedAt, key.LastUsedAt, key.RevokedAt); err != nil {
			return fmt.Errorf("import node key %s: %w", key.ID, err)
		}
	}
	for _, machine := range state.Machines {
		labelsJSON, err := marshalNullableJSON(machine.Labels)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_machines (id, tenant_id, name, connection_mode, status, endpoint, labels_json, node_key_id, runtime_id, runtime_name, cluster_node_name, fingerprint_prefix, fingerprint_hash, last_seen_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
ON CONFLICT (id) DO NOTHING
`, machine.ID, nullIfEmpty(machine.TenantID), machine.Name, machine.ConnectionMode, machine.Status, machine.Endpoint, labelsJSON, nullIfEmpty(machine.NodeKeyID), machine.RuntimeID, machine.RuntimeName, machine.ClusterNodeName, machine.FingerprintPrefix, machine.FingerprintHash, machine.LastSeenAt, machine.CreatedAt, machine.UpdatedAt); err != nil {
			return fmt.Errorf("import machine %s: %w", machine.ID, err)
		}
	}
	for _, runtime := range state.Runtimes {
		labelsJSON, err := marshalNullableJSON(runtime.Labels)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_runtimes (id, tenant_id, name, type, status, endpoint, labels_json, node_key_id, agent_key_prefix, agent_key_hash, last_heartbeat_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
ON CONFLICT (id) DO NOTHING
`, runtime.ID, nullIfEmpty(runtime.TenantID), runtime.Name, runtime.Type, runtime.Status, runtime.Endpoint, labelsJSON, nullIfEmpty(runtime.NodeKeyID), runtime.AgentKeyPrefix, runtime.AgentKeyHash, runtime.LastHeartbeatAt, runtime.CreatedAt, runtime.UpdatedAt); err != nil {
			return fmt.Errorf("import runtime %s: %w", runtime.ID, err)
		}
	}
	for _, app := range state.Apps {
		sourceJSON, err := marshalNullableJSON(app.Source)
		if err != nil {
			return err
		}
		routeJSON, err := marshalNullableJSON(app.Route)
		if err != nil {
			return err
		}
		specJSON, err := marshalJSON(app.Spec)
		if err != nil {
			return err
		}
		statusJSON, err := marshalJSON(app.Status)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_apps (id, tenant_id, project_id, name, description, source_json, route_json, spec_json, status_json, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (id) DO NOTHING
`, app.ID, app.TenantID, app.ProjectID, app.Name, app.Description, sourceJSON, routeJSON, specJSON, statusJSON, app.CreatedAt, app.UpdatedAt); err != nil {
			return fmt.Errorf("import app %s: %w", app.ID, err)
		}
	}
	for _, op := range state.Operations {
		desiredSpecJSON, err := marshalNullableJSON(op.DesiredSpec)
		if err != nil {
			return err
		}
		desiredSourceJSON, err := marshalNullableJSON(op.DesiredSource)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_operations (id, tenant_id, type, status, execution_mode, requested_by_type, requested_by_id, app_id, source_runtime_id, target_runtime_id, desired_replicas, desired_spec_json, desired_source_json, result_message, manifest_path, assigned_runtime_id, error_message, created_at, updated_at, started_at, completed_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
ON CONFLICT (id) DO NOTHING
`, op.ID, op.TenantID, op.Type, op.Status, op.ExecutionMode, op.RequestedByType, op.RequestedByID, op.AppID, op.SourceRuntimeID, op.TargetRuntimeID, intPointerValue(op.DesiredReplicas), desiredSpecJSON, desiredSourceJSON, op.ResultMessage, op.ManifestPath, op.AssignedRuntimeID, op.ErrorMessage, op.CreatedAt, op.UpdatedAt, op.StartedAt, op.CompletedAt); err != nil {
			return fmt.Errorf("import operation %s: %w", op.ID, err)
		}
	}
	for _, event := range state.AuditEvents {
		metadataJSON, err := marshalNullableJSON(event.Metadata)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_audit_events (id, tenant_id, actor_type, actor_id, action, target_type, target_id, metadata_json, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (id) DO NOTHING
`, event.ID, nullIfEmpty(event.TenantID), event.ActorType, event.ActorID, event.Action, event.TargetType, event.TargetID, metadataJSON, event.CreatedAt); err != nil {
			return fmt.Errorf("import audit event %s: %w", event.ID, err)
		}
	}
	return nil
}

func (s *Store) ensureManagedRuntimeTx(ctx context.Context, tx *sql.Tx) error {
	now := time.Now().UTC()
	labelsJSON, err := marshalJSON(map[string]string{"managed": "true"})
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO fugue_runtimes (id, tenant_id, name, type, status, endpoint, labels_json, node_key_id, agent_key_prefix, agent_key_hash, last_heartbeat_at, created_at, updated_at)
VALUES ($1, NULL, $2, $3, $4, $5, $6, NULL, '', '', NULL, $7, $8)
ON CONFLICT (id) DO UPDATE SET
	name = EXCLUDED.name,
	type = EXCLUDED.type,
	status = EXCLUDED.status,
	endpoint = EXCLUDED.endpoint,
	labels_json = EXCLUDED.labels_json,
	updated_at = EXCLUDED.updated_at
`, "runtime_managed_shared", "managed-shared", model.RuntimeTypeManagedShared, model.RuntimeStatusActive, "in-cluster", labelsJSON, now, now)
	if err != nil {
		return fmt.Errorf("ensure managed shared runtime: %w", err)
	}
	return nil
}

func (s *Store) ensureMachinesTx(ctx context.Context, tx *sql.Tx) error {
	state := model.State{}

	rows, err := tx.QueryContext(ctx, `
SELECT id, tenant_id, name, type, status, endpoint, labels_json, node_key_id, agent_key_prefix, agent_key_hash, last_heartbeat_at, created_at, updated_at
FROM fugue_runtimes
WHERE type IN ($1, $2)
ORDER BY updated_at ASC, created_at ASC
`, model.RuntimeTypeManagedOwned, model.RuntimeTypeExternalOwned)
	if err != nil {
		return fmt.Errorf("load runtimes for machine backfill: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		runtime, err := scanRuntime(rows)
		if err != nil {
			return err
		}
		state.Runtimes = append(state.Runtimes, runtime)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate runtimes for machine backfill: %w", err)
	}

	machineRows, err := tx.QueryContext(ctx, `
SELECT id, tenant_id, name, connection_mode, status, endpoint, labels_json, node_key_id, runtime_id, runtime_name, cluster_node_name, fingerprint_prefix, fingerprint_hash, last_seen_at, created_at, updated_at
FROM fugue_machines
ORDER BY created_at ASC
`)
	if err != nil {
		return fmt.Errorf("load existing machines: %w", err)
	}
	defer machineRows.Close()

	for machineRows.Next() {
		machine, err := scanMachine(machineRows)
		if err != nil {
			return err
		}
		state.Machines = append(state.Machines, machine)
	}
	if err := machineRows.Err(); err != nil {
		return fmt.Errorf("iterate existing machines: %w", err)
	}

	beforeCount := len(state.Machines)
	ensureMachineRecords(&state)
	if len(state.Machines) == beforeCount {
		return nil
	}

	for _, machine := range state.Machines[beforeCount:] {
		labelsJSON, err := marshalNullableJSON(machine.Labels)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_machines (id, tenant_id, name, connection_mode, status, endpoint, labels_json, node_key_id, runtime_id, runtime_name, cluster_node_name, fingerprint_prefix, fingerprint_hash, last_seen_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
ON CONFLICT (id) DO NOTHING
`, machine.ID, nullIfEmpty(machine.TenantID), machine.Name, machine.ConnectionMode, machine.Status, machine.Endpoint, labelsJSON, nullIfEmpty(machine.NodeKeyID), machine.RuntimeID, machine.RuntimeName, machine.ClusterNodeName, machine.FingerprintPrefix, machine.FingerprintHash, machine.LastSeenAt, machine.CreatedAt, machine.UpdatedAt); err != nil {
			if isUniqueViolation(err) {
				continue
			}
			return fmt.Errorf("backfill machine %s: %w", machine.ID, err)
		}
	}
	return nil
}

func nullIfEmpty(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return strings.TrimSpace(value)
}

func intPointerValue(value *int) any {
	if value == nil {
		return nil
	}
	return *value
}

func marshalJSON(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal json: %w", err)
	}
	return raw, nil
}

func marshalNullableJSON(value any) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	return marshalJSON(value)
}

func decodeJSONValue[T any](raw []byte) (T, error) {
	var out T
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return out, nil
	}
	if err := json.Unmarshal(trimmed, &out); err != nil {
		return out, fmt.Errorf("unmarshal json value: %w", err)
	}
	return out, nil
}

func decodeJSONPointer[T any](raw []byte) (*T, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, nil
	}
	var out T
	if err := json.Unmarshal(trimmed, &out); err != nil {
		return nil, fmt.Errorf("unmarshal json pointer: %w", err)
	}
	return &out, nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func mapDBErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, sql.ErrNoRows):
		return ErrNotFound
	case isUniqueViolation(err):
		return ErrConflict
	default:
		return err
	}
}

func (s *Store) notifyOperationTx(ctx context.Context, tx *sql.Tx, operationID string) error {
	if _, err := tx.ExecContext(ctx, `SELECT pg_notify($1, $2)`, PostgresOperationChannel, strings.TrimSpace(operationID)); err != nil {
		return fmt.Errorf("notify operation channel: %w", err)
	}
	return nil
}
