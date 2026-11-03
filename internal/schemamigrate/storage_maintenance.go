package schemamigrate

import "context"

const StorageMaintenanceSQL = `
CREATE TABLE IF NOT EXISTS fugue_storage_maintenance_jobs (
 id TEXT PRIMARY KEY, kind TEXT NOT NULL, scope_id TEXT NOT NULL,
 status TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL, body JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_storage_maintenance_recent ON fugue_storage_maintenance_jobs(updated_at DESC,id DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_storage_maintenance_active ON fugue_storage_maintenance_jobs(kind,scope_id)
 WHERE status IN ('queued','running','retrying');
`

func MigrateStorageMaintenance(ctx context.Context, databaseURL string) error {
	dsn, err := normalizeDatabaseURL(databaseURL)
	if err != nil {
		return err
	}
	db, err := openDatabase(dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	ctx, cancel := boundedContext(ctx)
	defer cancel()
	_, err = db.ExecContext(ctx, StorageMaintenanceSQL)
	return err
}
