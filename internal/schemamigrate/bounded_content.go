package schemamigrate

import (
	"context"
	"fmt"
)

// These are additive stores. Legacy content/checkpoints are never rewritten by
// migration, and readers are deployed before either new writer is enabled.
const BoundedContentSQL = `
CREATE TABLE IF NOT EXISTS fugue_platform_input_manifests (
 content_hash TEXT PRIMARY KEY CHECK (content_hash ~ '^sha256:[0-9a-f]{64}$'),
 format TEXT NOT NULL CHECK (format = 'runtime-snapshot-json-v1'),
 size_bytes BIGINT NOT NULL CHECK (size_bytes > 0 AND size_bytes <= 1073741824),
 chunk_size INTEGER NOT NULL CHECK (chunk_size > 0 AND chunk_size <= 262144),
 chunk_count INTEGER NOT NULL CHECK (chunk_count > 0 AND chunk_count <= 1048576),
 created_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS fugue_platform_input_chunks (
 content_hash TEXT NOT NULL REFERENCES fugue_platform_input_manifests(content_hash) DEFERRABLE INITIALLY DEFERRED,
 ordinal INTEGER NOT NULL CHECK (ordinal >= 0 AND ordinal < 1048576),
 chunk_hash TEXT NOT NULL CHECK (chunk_hash ~ '^sha256:[0-9a-f]{64}$'),
 data BYTEA NOT NULL CHECK (octet_length(data) > 0 AND octet_length(data) <= 262144),
 PRIMARY KEY (content_hash, ordinal)
);
CREATE TABLE IF NOT EXISTS fugue_observation_checkpoint_heads (
 checkpoint_key TEXT PRIMARY KEY,
 revision BIGINT NOT NULL CHECK (revision > 0),
 metadata BYTEA NOT NULL CHECK (octet_length(metadata) <= 262144),
 updated_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS fugue_observation_checkpoint_entries (
 checkpoint_key TEXT NOT NULL REFERENCES fugue_observation_checkpoint_heads(checkpoint_key) DEFERRABLE INITIALLY DEFERRED,
 bucket TEXT NOT NULL CHECK (octet_length(bucket) <= 160),
 entry_key TEXT NOT NULL CHECK (octet_length(entry_key) <= 4096),
 value BYTEA NOT NULL CHECK (octet_length(value) <= 65536),
 PRIMARY KEY (checkpoint_key, bucket, entry_key)
);`

func MigrateBoundedContent(ctx context.Context, databaseURL string) error {
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
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(315609238744289)`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, BoundedContentSQL); err != nil {
		return fmt.Errorf("create bounded content stores: %w", err)
	}
	// The query verifies all required columns/types even when CREATE IF NOT
	// EXISTS encounters a pre-existing table. A mismatched schema fails closed.
	for _, table := range []struct{ name, columns, casts string }{
		{"fugue_platform_input_manifests", "content_hash,format,size_bytes,chunk_size,chunk_count,created_at", "NULL::text,NULL::text,NULL::bigint,NULL::integer,NULL::integer,NULL::timestamptz"},
		{"fugue_platform_input_chunks", "content_hash,ordinal,chunk_hash,data", "NULL::text,NULL::integer,NULL::text,NULL::bytea"},
		{"fugue_observation_checkpoint_heads", "checkpoint_key,revision,metadata,updated_at", "NULL::text,NULL::bigint,NULL::bytea,NULL::timestamptz"},
		{"fugue_observation_checkpoint_entries", "checkpoint_key,bucket,entry_key,value", "NULL::text,NULL::text,NULL::text,NULL::bytea"},
	} {
		rows, err := tx.QueryContext(ctx, "SELECT "+table.columns+" FROM "+table.name+" WHERE false UNION ALL SELECT "+table.casts+" WHERE false")
		if err != nil {
			return fmt.Errorf("verify bounded content schema %s: %w", table.name, err)
		}
		if err = rows.Close(); err != nil {
			return err
		}
	}
	return tx.Commit()
}
