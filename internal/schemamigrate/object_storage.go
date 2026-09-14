package schemamigrate

import (
	"context"
	"fmt"
)

// No cascade from app/project deletion: object ownership and revocation records
// outlive compute. Tenant deletion is blocked until resources are dealt with.
const ObjectStorageSQL = `
CREATE TABLE IF NOT EXISTS fugue_object_storage_config (
 id TEXT PRIMARY KEY CHECK (id = 'platform'), body JSONB NOT NULL
);
CREATE TABLE IF NOT EXISTS fugue_object_stores (
 id TEXT PRIMARY KEY,
 tenant_id TEXT NOT NULL REFERENCES fugue_tenants(id),
 project_id TEXT NOT NULL,
 name TEXT NOT NULL,
 body JSONB NOT NULL,
 UNIQUE (tenant_id, project_id, name)
);
CREATE TABLE IF NOT EXISTS fugue_object_storage_credentials (
 id TEXT PRIMARY KEY,
 store_id TEXT NOT NULL REFERENCES fugue_object_stores(id),
 name TEXT NOT NULL,
 body JSONB NOT NULL,
 UNIQUE (store_id, name)
);
CREATE INDEX IF NOT EXISTS idx_fugue_object_stores_tenant ON fugue_object_stores(tenant_id, project_id);
`

// MigrateObjectStorage only adds independent tables; prior API releases continue
// serving when this lane is retried or a subsequent code deployment fails.
func MigrateObjectStorage(ctx context.Context, databaseURL string) error {
	databaseURL, err := normalizeDatabaseURL(databaseURL)
	if err != nil {
		return err
	}
	ctx, cancel := boundedContext(ctx)
	defer cancel()
	db, err := openDatabase(databaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SELECT set_config('lock_timeout', $1, true)`, platformStateLockLimit.String()); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(315609238744286)`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, ObjectStorageSQL); err != nil {
		return fmt.Errorf("create object storage schema: %w", err)
	}
	return tx.Commit()
}
