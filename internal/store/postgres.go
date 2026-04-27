package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"

	"github.com/jackc/pgx/v5/pgconn"
)

const (
	postgresBootstrapLockID  int64  = 315609238744281
	PostgresOperationChannel string = "fugue_operations"
	postgresPingTimeout             = 20 * time.Second
	// Bootstrap can serialize across concurrently starting replicas while it
	// waits on the advisory lock and runs startup repair queries.
	postgresBootstrapTimeout = 2 * time.Minute
	// When schema changes do need to run DDL, fail fast on blocked relation
	// locks so bootstrap can retry instead of hanging the whole API startup.
	postgresBootstrapLockTimeout = 5 * time.Second
)

const (
	postgresSchemaVersionMetaKey     = "schema_version"
	postgresSchemaVersionValue       = "relational-v1"
	postgresSchemaFingerprintMetaKey = "schema_fingerprint"
)

var postgresBootstrapRetryableCodes = map[string]struct{}{
	"40P01": {}, // deadlock_detected
	"40001": {}, // serialization_failure
	"55P03": {}, // lock_not_available
}

var postgresMetaSchemaStatement = `CREATE TABLE IF NOT EXISTS fugue_meta (
		key TEXT PRIMARY KEY,
		value TEXT NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	)`

var postgresSchemaStatements = []string{
	postgresMetaSchemaStatement,
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
	`ALTER TABLE fugue_projects ADD COLUMN IF NOT EXISTS delete_requested_at TIMESTAMPTZ NULL`,
	`CREATE UNIQUE INDEX IF NOT EXISTS idx_fugue_projects_tenant_slug ON fugue_projects (tenant_id, slug)`,
	`CREATE TABLE IF NOT EXISTS fugue_api_keys (
		id TEXT PRIMARY KEY,
		tenant_id TEXT NULL REFERENCES fugue_tenants(id) ON DELETE CASCADE,
		label TEXT NOT NULL,
		prefix TEXT NOT NULL,
		hash TEXT NOT NULL UNIQUE,
		status TEXT NOT NULL DEFAULT 'active',
		scopes_json JSONB NOT NULL,
		created_at TIMESTAMPTZ NOT NULL,
		last_used_at TIMESTAMPTZ NULL,
		disabled_at TIMESTAMPTZ NULL
	)`,
	`ALTER TABLE fugue_api_keys ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active'`,
	`ALTER TABLE fugue_api_keys ADD COLUMN IF NOT EXISTS disabled_at TIMESTAMPTZ NULL`,
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
		scope TEXT NOT NULL DEFAULT 'tenant-runtime',
		status TEXT NOT NULL,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		last_used_at TIMESTAMPTZ NULL,
		revoked_at TIMESTAMPTZ NULL
	)`,
	`ALTER TABLE fugue_node_keys ADD COLUMN IF NOT EXISTS scope TEXT NOT NULL DEFAULT 'tenant-runtime'`,
	`CREATE TABLE IF NOT EXISTS fugue_machines (
		id TEXT PRIMARY KEY,
		tenant_id TEXT NULL REFERENCES fugue_tenants(id) ON DELETE CASCADE,
		name TEXT NOT NULL,
		scope TEXT NOT NULL DEFAULT 'tenant-runtime',
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
		policy_json JSONB NULL,
		last_seen_at TIMESTAMPTZ NULL,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL
	)`,
	`ALTER TABLE fugue_machines ADD COLUMN IF NOT EXISTS scope TEXT NOT NULL DEFAULT 'tenant-runtime'`,
	`ALTER TABLE fugue_machines ADD COLUMN IF NOT EXISTS policy_json JSONB NULL`,
	`CREATE UNIQUE INDEX IF NOT EXISTS idx_fugue_machines_tenant_fingerprint_hash ON fugue_machines ((COALESCE(tenant_id, '')), fingerprint_hash) WHERE fingerprint_hash <> ''`,
	`CREATE UNIQUE INDEX IF NOT EXISTS idx_fugue_machines_runtime_id ON fugue_machines (runtime_id) WHERE runtime_id <> ''`,
	`CREATE INDEX IF NOT EXISTS idx_fugue_machines_node_key_id ON fugue_machines (node_key_id) WHERE node_key_id IS NOT NULL`,
	`CREATE TABLE IF NOT EXISTS fugue_runtimes (
		id TEXT PRIMARY KEY,
		tenant_id TEXT NULL REFERENCES fugue_tenants(id) ON DELETE CASCADE,
		name TEXT NOT NULL,
		machine_name TEXT NOT NULL DEFAULT '',
		type TEXT NOT NULL,
		access_mode TEXT NOT NULL DEFAULT 'private',
		public_offer_json JSONB NULL,
		pool_mode TEXT NOT NULL DEFAULT 'dedicated',
		connection_mode TEXT NOT NULL DEFAULT '',
		status TEXT NOT NULL,
		endpoint TEXT NOT NULL DEFAULT '',
		labels_json JSONB NULL,
		node_key_id TEXT NULL REFERENCES fugue_node_keys(id) ON DELETE SET NULL,
		cluster_node_name TEXT NOT NULL DEFAULT '',
		fingerprint_prefix TEXT NOT NULL DEFAULT '',
		fingerprint_hash TEXT NOT NULL DEFAULT '',
		agent_key_prefix TEXT NOT NULL DEFAULT '',
		agent_key_hash TEXT NOT NULL DEFAULT '',
		last_seen_at TIMESTAMPTZ NULL,
		last_heartbeat_at TIMESTAMPTZ NULL,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL
	)`,
	`ALTER TABLE fugue_runtimes ADD COLUMN IF NOT EXISTS machine_name TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE fugue_runtimes ADD COLUMN IF NOT EXISTS access_mode TEXT NOT NULL DEFAULT 'private'`,
	`ALTER TABLE fugue_runtimes ADD COLUMN IF NOT EXISTS public_offer_json JSONB NULL`,
	`ALTER TABLE fugue_runtimes ADD COLUMN IF NOT EXISTS pool_mode TEXT NOT NULL DEFAULT 'dedicated'`,
	`ALTER TABLE fugue_runtimes ADD COLUMN IF NOT EXISTS connection_mode TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE fugue_runtimes ADD COLUMN IF NOT EXISTS cluster_node_name TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE fugue_runtimes ADD COLUMN IF NOT EXISTS fingerprint_prefix TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE fugue_runtimes ADD COLUMN IF NOT EXISTS fingerprint_hash TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE fugue_runtimes ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ NULL`,
	`CREATE UNIQUE INDEX IF NOT EXISTS idx_fugue_runtimes_tenant_name_ci ON fugue_runtimes ((COALESCE(tenant_id, '')), lower(name))`,
	`CREATE UNIQUE INDEX IF NOT EXISTS idx_fugue_runtimes_agent_key_hash ON fugue_runtimes (agent_key_hash) WHERE agent_key_hash <> ''`,
	`CREATE UNIQUE INDEX IF NOT EXISTS idx_fugue_runtimes_tenant_fingerprint_hash ON fugue_runtimes ((COALESCE(tenant_id, '')), fingerprint_hash) WHERE fingerprint_hash <> ''`,
	`CREATE INDEX IF NOT EXISTS idx_fugue_runtimes_fingerprint_hash ON fugue_runtimes (fingerprint_hash) WHERE fingerprint_hash <> ''`,
	`CREATE TABLE IF NOT EXISTS fugue_runtime_access_grants (
		runtime_id TEXT NOT NULL REFERENCES fugue_runtimes(id) ON DELETE CASCADE,
		tenant_id TEXT NOT NULL REFERENCES fugue_tenants(id) ON DELETE CASCADE,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (runtime_id, tenant_id)
	)`,
	`CREATE INDEX IF NOT EXISTS idx_fugue_runtime_access_grants_tenant_id ON fugue_runtime_access_grants (tenant_id, created_at ASC)`,
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
	`CREATE TABLE IF NOT EXISTS fugue_app_domains (
		hostname TEXT PRIMARY KEY,
		tenant_id TEXT NOT NULL REFERENCES fugue_tenants(id) ON DELETE CASCADE,
		app_id TEXT NOT NULL REFERENCES fugue_apps(id) ON DELETE CASCADE,
		status TEXT NOT NULL,
		tls_status TEXT NOT NULL DEFAULT '',
		verification_txt_name TEXT NOT NULL DEFAULT '',
		verification_txt_value TEXT NOT NULL DEFAULT '',
		route_target TEXT NOT NULL DEFAULT '',
		last_message TEXT NOT NULL DEFAULT '',
		tls_last_message TEXT NOT NULL DEFAULT '',
		last_checked_at TIMESTAMPTZ NULL,
		verified_at TIMESTAMPTZ NULL,
		tls_last_checked_at TIMESTAMPTZ NULL,
		tls_ready_at TIMESTAMPTZ NULL,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL
	)`,
	`ALTER TABLE fugue_app_domains ADD COLUMN IF NOT EXISTS tls_status TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE fugue_app_domains ADD COLUMN IF NOT EXISTS tls_last_message TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE fugue_app_domains ADD COLUMN IF NOT EXISTS tls_last_checked_at TIMESTAMPTZ NULL`,
	`ALTER TABLE fugue_app_domains ADD COLUMN IF NOT EXISTS tls_ready_at TIMESTAMPTZ NULL`,
	`CREATE UNIQUE INDEX IF NOT EXISTS idx_fugue_app_domains_hostname_ci ON fugue_app_domains (lower(hostname))`,
	`CREATE INDEX IF NOT EXISTS idx_fugue_app_domains_app_id ON fugue_app_domains (app_id, created_at ASC)`,
	`CREATE TABLE IF NOT EXISTS fugue_backing_services (
		id TEXT PRIMARY KEY,
		tenant_id TEXT NOT NULL REFERENCES fugue_tenants(id) ON DELETE CASCADE,
		project_id TEXT NOT NULL REFERENCES fugue_projects(id) ON DELETE CASCADE,
		owner_app_id TEXT NULL REFERENCES fugue_apps(id) ON DELETE SET NULL,
		name TEXT NOT NULL,
		description TEXT NOT NULL DEFAULT '',
		type TEXT NOT NULL,
		provisioner TEXT NOT NULL,
		status TEXT NOT NULL,
		spec_json JSONB NOT NULL,
		current_runtime_started_at TIMESTAMPTZ NULL,
		current_runtime_ready_at TIMESTAMPTZ NULL,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL
	)`,
	`ALTER TABLE fugue_backing_services ADD COLUMN IF NOT EXISTS current_runtime_started_at TIMESTAMPTZ NULL`,
	`ALTER TABLE fugue_backing_services ADD COLUMN IF NOT EXISTS current_runtime_ready_at TIMESTAMPTZ NULL`,
	`CREATE UNIQUE INDEX IF NOT EXISTS idx_fugue_backing_services_tenant_project_name_ci ON fugue_backing_services (tenant_id, project_id, lower(name))`,
	`CREATE INDEX IF NOT EXISTS idx_fugue_backing_services_owner_app_id ON fugue_backing_services (owner_app_id) WHERE owner_app_id IS NOT NULL`,
	`CREATE TABLE IF NOT EXISTS fugue_service_bindings (
		id TEXT PRIMARY KEY,
		tenant_id TEXT NOT NULL REFERENCES fugue_tenants(id) ON DELETE CASCADE,
		app_id TEXT NOT NULL REFERENCES fugue_apps(id) ON DELETE CASCADE,
		service_id TEXT NOT NULL REFERENCES fugue_backing_services(id) ON DELETE CASCADE,
		alias TEXT NOT NULL DEFAULT '',
		env_json JSONB NULL,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL
	)`,
	`CREATE UNIQUE INDEX IF NOT EXISTS idx_fugue_service_bindings_app_service ON fugue_service_bindings (app_id, service_id)`,
	`CREATE INDEX IF NOT EXISTS idx_fugue_service_bindings_service_id ON fugue_service_bindings (service_id)`,
	`CREATE TABLE IF NOT EXISTS fugue_tenant_billing (
		tenant_id TEXT PRIMARY KEY REFERENCES fugue_tenants(id) ON DELETE CASCADE,
		managed_cap_json JSONB NOT NULL,
		managed_image_storage_gibibytes BIGINT NOT NULL DEFAULT 0,
		balance_microcents BIGINT NOT NULL DEFAULT 0,
		price_book_json JSONB NOT NULL,
		last_accrued_at TIMESTAMPTZ NOT NULL,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL
	)`,
	`ALTER TABLE fugue_tenant_billing ADD COLUMN IF NOT EXISTS managed_image_storage_gibibytes BIGINT NOT NULL DEFAULT 0`,
	`CREATE TABLE IF NOT EXISTS fugue_billing_events (
		id TEXT PRIMARY KEY,
		tenant_id TEXT NOT NULL REFERENCES fugue_tenants(id) ON DELETE CASCADE,
		type TEXT NOT NULL,
		amount_microcents BIGINT NOT NULL DEFAULT 0,
		balance_after_microcents BIGINT NOT NULL DEFAULT 0,
		metadata_json JSONB NULL,
		created_at TIMESTAMPTZ NOT NULL
	)`,
	`CREATE INDEX IF NOT EXISTS idx_fugue_billing_events_tenant_created_at ON fugue_billing_events (tenant_id, created_at DESC)`,
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
	`CREATE TABLE IF NOT EXISTS fugue_source_uploads (
		id TEXT PRIMARY KEY,
		tenant_id TEXT NOT NULL REFERENCES fugue_tenants(id) ON DELETE CASCADE,
		filename TEXT NOT NULL DEFAULT '',
		content_type TEXT NOT NULL DEFAULT '',
		sha256 TEXT NOT NULL,
		size_bytes BIGINT NOT NULL,
		download_token TEXT NOT NULL,
		archive_bytes BYTEA NOT NULL,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL
	)`,
	`CREATE INDEX IF NOT EXISTS idx_fugue_source_uploads_tenant_created_at ON fugue_source_uploads (tenant_id, created_at DESC)`,
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
	`CREATE INDEX IF NOT EXISTS idx_fugue_operations_app_created_at ON fugue_operations (app_id, created_at)`,
	`CREATE INDEX IF NOT EXISTS idx_fugue_operations_app_status ON fugue_operations (app_id, status)`,
	`CREATE INDEX IF NOT EXISTS idx_fugue_operations_tenant_created_at ON fugue_operations (tenant_id, created_at)`,
	`CREATE INDEX IF NOT EXISTS idx_fugue_operations_tenant_app_created_at ON fugue_operations (tenant_id, app_id, created_at)`,
	`CREATE INDEX IF NOT EXISTS idx_fugue_operations_tenant_app_status ON fugue_operations (tenant_id, app_id, status)`,
	`CREATE INDEX IF NOT EXISTS idx_fugue_operations_import_failed_app_updated_created ON fugue_operations (app_id, updated_at DESC, created_at DESC) WHERE type = 'import' AND status = 'failed'`,
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

	pingCtx, pingCancel := context.WithTimeout(context.Background(), postgresPingTimeout)
	defer pingCancel()

	if err := s.db.PingContext(pingCtx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}

	bootstrapCtx, bootstrapCancel := context.WithTimeout(context.Background(), postgresBootstrapTimeout)
	defer bootstrapCancel()

	if err := s.bootstrapDatabase(bootstrapCtx); err != nil {
		return err
	}

	s.dbReady = true
	return nil
}

func (s *Store) bootstrapDatabase(ctx context.Context) error {
	for attempt := 0; ; attempt++ {
		err := s.bootstrapDatabaseOnce(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil || !isRetryableBootstrapError(err) {
			return err
		}
		if sleepErr := sleepContext(ctx, bootstrapRetryDelay(attempt)); sleepErr != nil {
			return fmt.Errorf("retry postgres bootstrap after transient failure: %w", errors.Join(err, sleepErr))
		}
	}
}

func (s *Store) bootstrapDatabaseOnce(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin bootstrap transaction: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1)", postgresBootstrapLockID); err != nil {
		return fmt.Errorf("acquire postgres advisory lock: %w", err)
	}
	if _, err := s.applyPostgresSchemaTx(ctx, tx); err != nil {
		return err
	}

	schemaVersion, exists, err := s.getMetaTx(ctx, tx, postgresSchemaVersionMetaKey)
	if err != nil {
		return err
	}
	if !exists || schemaVersion != postgresSchemaVersionValue {
		if err := s.upsertMetaTx(ctx, tx, postgresSchemaVersionMetaKey, postgresSchemaVersionValue); err != nil {
			return err
		}
	}
	if err := s.ensureManagedRuntimeTx(ctx, tx); err != nil {
		return err
	}
	if err := s.ensureRuntimeMetadataTx(ctx, tx); err != nil {
		return err
	}
	if err := s.ensureFailedImportAppStatusTx(ctx, tx); err != nil {
		return err
	}
	if err := s.ensureTenantBillingRecordsTx(ctx, tx); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit bootstrap transaction: %w", err)
	}
	return nil
}

func (s *Store) applyPostgresSchemaTx(ctx context.Context, tx *sql.Tx) (bool, error) {
	if _, err := tx.ExecContext(ctx, postgresMetaSchemaStatement); err != nil {
		return false, fmt.Errorf("ensure postgres meta table: %w", err)
	}

	desiredFingerprint := postgresSchemaFingerprint()
	currentFingerprint, exists, err := s.getMetaTx(ctx, tx, postgresSchemaFingerprintMetaKey)
	if err != nil {
		return false, err
	}
	if exists && currentFingerprint == desiredFingerprint {
		return false, nil
	}
	if _, err := tx.ExecContext(ctx, `SELECT set_config('lock_timeout', $1, true)`, formatPostgresDuration(postgresBootstrapLockTimeout)); err != nil {
		return false, fmt.Errorf("set postgres bootstrap lock timeout: %w", err)
	}
	for _, stmt := range postgresSchemaStatements[1:] {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return false, fmt.Errorf("apply postgres schema: %w", err)
		}
	}
	if err := s.upsertMetaTx(ctx, tx, postgresSchemaFingerprintMetaKey, desiredFingerprint); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) ensureFailedImportAppStatusTx(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `
UPDATE fugue_apps AS a
SET status_json = jsonb_set(
		jsonb_set(
			jsonb_set(COALESCE(a.status_json, '{}'::jsonb), '{phase}', to_jsonb($1::text), true),
			'{last_operation_id}', to_jsonb(o.id::text), true
		),
		'{last_message}', to_jsonb(COALESCE(NULLIF(BTRIM(o.error_message), ''), $2)::text), true
	),
	updated_at = GREATEST(a.updated_at, o.updated_at)
FROM (
	SELECT DISTINCT ON (app_id) app_id, id, error_message, updated_at
	FROM fugue_operations
	WHERE type = $3 AND status = $4
	ORDER BY app_id, updated_at DESC, created_at DESC
) AS o
WHERE a.id = o.app_id
  AND COALESCE(a.status_json->>'phase', '') = $5
`, "failed", "operation failed", model.OperationTypeImport, model.OperationStatusFailed, "importing"); err != nil {
		return fmt.Errorf("repair stale importing apps from failed imports: %w", err)
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

func (s *Store) ensureManagedRuntimeTx(ctx context.Context, tx *sql.Tx) error {
	now := time.Now().UTC()
	labelsJSON, err := marshalJSON(map[string]string{"managed": "true"})
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO fugue_runtimes (id, tenant_id, name, machine_name, type, access_mode, public_offer_json, pool_mode, connection_mode, status, endpoint, labels_json, node_key_id, cluster_node_name, fingerprint_prefix, fingerprint_hash, agent_key_prefix, agent_key_hash, last_seen_at, last_heartbeat_at, created_at, updated_at)
VALUES ($1, NULL, $2, $2, $3, $4, NULL, $5, '', $6, $7, $8, NULL, '', '', '', '', '', NULL, NULL, $9, $10)
ON CONFLICT (id) DO UPDATE SET
	name = EXCLUDED.name,
	machine_name = EXCLUDED.machine_name,
	type = EXCLUDED.type,
	access_mode = EXCLUDED.access_mode,
	public_offer_json = EXCLUDED.public_offer_json,
	pool_mode = EXCLUDED.pool_mode,
	status = EXCLUDED.status,
	endpoint = EXCLUDED.endpoint,
	labels_json = EXCLUDED.labels_json,
	updated_at = EXCLUDED.updated_at
`, "runtime_managed_shared", "managed-shared", model.RuntimeTypeManagedShared, model.RuntimeAccessModePlatformShared, model.RuntimePoolModeDedicated, model.RuntimeStatusActive, "in-cluster", labelsJSON, now, now)
	if err != nil {
		return fmt.Errorf("ensure managed shared runtime: %w", err)
	}
	return nil
}

func (s *Store) ensureRuntimeMetadataTx(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `
UPDATE fugue_runtimes AS r
SET machine_name = CASE WHEN r.machine_name = '' THEN m.name ELSE r.machine_name END,
	connection_mode = CASE WHEN r.connection_mode = '' THEN m.connection_mode ELSE r.connection_mode END,
	endpoint = CASE WHEN r.endpoint = '' THEN m.endpoint ELSE r.endpoint END,
	labels_json = CASE WHEN r.labels_json IS NULL THEN m.labels_json ELSE r.labels_json END,
	node_key_id = COALESCE(r.node_key_id, m.node_key_id),
	cluster_node_name = CASE WHEN r.cluster_node_name = '' THEN m.cluster_node_name ELSE r.cluster_node_name END,
	fingerprint_prefix = CASE WHEN r.fingerprint_prefix = '' THEN m.fingerprint_prefix ELSE r.fingerprint_prefix END,
	fingerprint_hash = CASE WHEN r.fingerprint_hash = '' THEN m.fingerprint_hash ELSE r.fingerprint_hash END,
	last_seen_at = COALESCE(r.last_seen_at, m.last_seen_at),
	last_heartbeat_at = COALESCE(r.last_heartbeat_at, m.last_seen_at)
FROM fugue_machines AS m
WHERE m.runtime_id <> ''
  AND r.id = m.runtime_id
`); err != nil {
		return fmt.Errorf("backfill runtime metadata from legacy machines: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE fugue_runtimes
SET machine_name = CASE WHEN machine_name = '' THEN name ELSE machine_name END,
	connection_mode = CASE
		WHEN connection_mode = '' AND type = $1 THEN 'cluster'
		WHEN connection_mode = '' AND type = $2 THEN 'agent'
		ELSE connection_mode
	END,
	cluster_node_name = CASE
		WHEN cluster_node_name = '' AND type = $1 AND (node_key_id IS NOT NULL OR fingerprint_hash <> '' OR status = $3) THEN name
		ELSE cluster_node_name
	END,
	last_seen_at = COALESCE(last_seen_at, last_heartbeat_at)
`, model.RuntimeTypeManagedOwned, model.RuntimeTypeExternalOwned, model.RuntimeStatusActive); err != nil {
		return fmt.Errorf("normalize runtime metadata defaults: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE fugue_runtimes
SET access_mode = CASE
	WHEN type = $1 THEN $2
	WHEN access_mode = $2 THEN $2
	WHEN access_mode = $3 THEN $3
	WHEN access_mode = $4 THEN $4
	ELSE $3
END
`, model.RuntimeTypeManagedShared, model.RuntimeAccessModePlatformShared, model.RuntimeAccessModePrivate, model.RuntimeAccessModePublic); err != nil {
		return fmt.Errorf("normalize runtime access mode defaults: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE fugue_runtimes
SET pool_mode = CASE
	WHEN type = $1 AND pool_mode = $2 THEN $2
	ELSE $3
END
`, model.RuntimeTypeManagedOwned, model.RuntimePoolModeInternalShared, model.RuntimePoolModeDedicated); err != nil {
		return fmt.Errorf("normalize runtime pool mode defaults: %w", err)
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

func isRetryableBootstrapError(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	_, retryable := postgresBootstrapRetryableCodes[pgErr.Code]
	return retryable
}

func postgresSchemaFingerprint() string {
	hasher := sha256.New()
	for _, stmt := range postgresSchemaStatements {
		_, _ = hasher.Write([]byte(strings.TrimSpace(stmt)))
		_, _ = hasher.Write([]byte{0})
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

func bootstrapRetryDelay(attempt int) time.Duration {
	delay := 250 * time.Millisecond
	for range attempt {
		if delay >= 2*time.Second {
			return 2 * time.Second
		}
		delay *= 2
	}
	if delay > 2*time.Second {
		return 2 * time.Second
	}
	return delay
}

func formatPostgresDuration(d time.Duration) string {
	if d%time.Second == 0 {
		return fmt.Sprintf("%ds", int(d/time.Second))
	}
	return fmt.Sprintf("%dms", d/time.Millisecond)
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
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
