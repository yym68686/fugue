package schemamigrate

import (
	"context"
	"fmt"
)

// Additive schema; old APIs keep working during the independent schema lane.
const SourceUploadSessionsSQL = `
CREATE TABLE IF NOT EXISTS fugue_source_upload_sessions (
 id TEXT PRIMARY KEY,
 tenant_id TEXT NOT NULL,
 request_id TEXT NOT NULL,
 document JSONB NOT NULL,
 UNIQUE (tenant_id, request_id)
);
CREATE TABLE IF NOT EXISTS fugue_source_upload_chunks (
 session_id TEXT NOT NULL REFERENCES fugue_source_upload_sessions(id) ON DELETE CASCADE,
 chunk_index INTEGER NOT NULL CHECK (chunk_index >= 0 AND chunk_index < 32),
 data BYTEA NOT NULL CHECK (octet_length(data) <= 4194304),
 PRIMARY KEY (session_id, chunk_index)
);`

func MigrateSourceUploadSessions(ctx context.Context, databaseURL string) error {
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
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(315609238744285)`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, SourceUploadSessionsSQL); err != nil {
		return fmt.Errorf("create source upload session schema: %w", err)
	}
	if _, err = tx.ExecContext(ctx, `ALTER TABLE IF EXISTS fugue_data_transfers ADD COLUMN IF NOT EXISTS prewarm_cache_json JSONB NULL`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS idx_fugue_data_prewarm_active ON fugue_data_transfers(workspace_id,snapshot_id,target) WHERE direction='prewarm' AND prewarm_cache_json IS NOT NULL AND status IN ('planned','running')`); err != nil {
		return err
	}
	return tx.Commit()
}
