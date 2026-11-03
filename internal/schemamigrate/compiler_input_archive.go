package schemamigrate

import (
	"context"
	"fmt"
)

// CompilerInputArchiveSQL creates an inert, additive ledger for the external,
// byte-for-byte compiler-input archive. Applying it never uploads, deletes,
// reroutes, or changes a reader. Archive writers and reader routing require a
// later, separately gated rollout.
const CompilerInputArchiveSQL = `
ALTER TABLE fugue_platform_input_manifests ADD COLUMN IF NOT EXISTS storage_location TEXT NOT NULL DEFAULT 'legacy' CHECK (storage_location IN ('legacy','hot'));
CREATE TABLE IF NOT EXISTS fugue_platform_input_hot_chunks (
 content_hash TEXT NOT NULL REFERENCES fugue_platform_input_manifests(content_hash) DEFERRABLE INITIALLY DEFERRED,
 ordinal INTEGER NOT NULL CHECK (ordinal>=0 AND ordinal<1048576),
 chunk_hash TEXT NOT NULL CHECK (chunk_hash ~ '^sha256:[0-9a-f]{64}$'),
 data BYTEA NOT NULL CHECK (octet_length(data)>0 AND octet_length(data)<=262144),
 PRIMARY KEY(content_hash,ordinal)
);
CREATE TABLE IF NOT EXISTS fugue_platform_input_archives (
 content_hash TEXT PRIMARY KEY REFERENCES fugue_platform_input_manifests(content_hash) ON DELETE RESTRICT,
 archive_format TEXT NOT NULL CHECK (archive_format='compiler-input-archive-v1'),
 source_format TEXT NOT NULL CHECK (source_format IN ('runtime-snapshot-json-v1','runtime-snapshot-json-chunk-codec-v2','runtime-snapshot-shared-v3','runtime-snapshot-separated-v4')),
 raw_bytes BIGINT NOT NULL CHECK (raw_bytes>0 AND raw_bytes<=1073741824),
 segment_bytes INTEGER NOT NULL CHECK (segment_bytes>=65536 AND segment_bytes<=16777216),
 segment_count INTEGER NOT NULL CHECK (segment_count>0 AND segment_count<=16384),
 object_prefix TEXT NOT NULL CHECK (length(object_prefix) BETWEEN 1 AND 512),
 backend_hash TEXT NOT NULL CHECK (backend_hash ~ '^sha256:[0-9a-f]{64}$'),
 state TEXT NOT NULL CHECK (state IN ('planned','uploading','verified','routed','blocked')),
 source_epoch BIGINT NOT NULL CHECK (source_epoch>=0),
 verified_digest TEXT NOT NULL CHECK (verified_digest='' OR verified_digest ~ '^sha256:[0-9a-f]{64}$'),
 fencing_token BIGINT NOT NULL CHECK (fencing_token>=0),
 created_at TIMESTAMPTZ NOT NULL,
 verified_at TIMESTAMPTZ NULL,
 routed_at TIMESTAMPTZ NULL,
 updated_at TIMESTAMPTZ NOT NULL,
 CHECK ((state IN ('verified','routed') AND verified_digest=content_hash AND verified_at IS NOT NULL) OR (state IN ('planned','uploading','blocked') AND verified_digest='')),
 CHECK ((state='routed' AND routed_at IS NOT NULL) OR (state<>'routed' AND routed_at IS NULL))
);
CREATE TABLE IF NOT EXISTS fugue_platform_input_archive_segments (
 content_hash TEXT NOT NULL REFERENCES fugue_platform_input_archives(content_hash) ON DELETE CASCADE,
 ordinal INTEGER NOT NULL CHECK (ordinal>=0 AND ordinal<16384),
 object_key TEXT NOT NULL CHECK (length(object_key) BETWEEN 1 AND 1024),
 raw_bytes INTEGER NOT NULL CHECK (raw_bytes>0 AND raw_bytes<=16777216),
 stored_bytes INTEGER NOT NULL CHECK (stored_bytes>0 AND stored_bytes<=17825792),
 segment_hash TEXT NOT NULL CHECK (segment_hash ~ '^sha256:[0-9a-f]{64}$'),
 stored_hash TEXT NOT NULL CHECK (stored_hash ~ '^sha256:[0-9a-f]{64}$'),
 object_etag TEXT NOT NULL DEFAULT '',
 uploaded_at TIMESTAMPTZ NULL,
 verified_at TIMESTAMPTZ NULL,
 PRIMARY KEY(content_hash,ordinal),
 CHECK ((uploaded_at IS NULL)=(verified_at IS NULL)),
 UNIQUE(object_key)
);
CREATE INDEX IF NOT EXISTS fugue_platform_input_archive_segments_key_idx
 ON fugue_platform_input_archive_segments(object_key);
CREATE TABLE IF NOT EXISTS fugue_platform_input_archive_leases (
 content_hash TEXT PRIMARY KEY REFERENCES fugue_platform_input_archives(content_hash) ON DELETE CASCADE,
 owner TEXT NOT NULL CHECK (length(owner) BETWEEN 1 AND 256),
 source_epoch BIGINT NOT NULL CHECK (source_epoch>=0),
 fencing_token BIGINT NOT NULL CHECK (fencing_token>=0),
 expires_at TIMESTAMPTZ NOT NULL,
 updated_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS fugue_platform_input_archive_control (
 singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
 epoch BIGINT NOT NULL CHECK (epoch>=0),
 writes_fenced BOOLEAN NOT NULL DEFAULT FALSE,
 retirement_started_at TIMESTAMPTZ NULL,
 retirement_plan_sha256 TEXT NOT NULL DEFAULT '' CHECK (retirement_plan_sha256='' OR retirement_plan_sha256 ~ '^[0-9a-f]{64}$'),
 retirement_reader_current TEXT NOT NULL DEFAULT '' CHECK (retirement_reader_current='' OR retirement_reader_current ~ '^[0-9a-f]{40}$'),
 retirement_reader_rollback TEXT NOT NULL DEFAULT '' CHECK (retirement_reader_rollback='' OR retirement_reader_rollback ~ '^[0-9a-f]{40}$'),
 retirement_archive_policy_sha256 TEXT NOT NULL DEFAULT '' CHECK (retirement_archive_policy_sha256='' OR retirement_archive_policy_sha256 ~ '^[0-9a-f]{64}$'),
 updated_at TIMESTAMPTZ NOT NULL
);
ALTER TABLE fugue_platform_input_archive_control ADD COLUMN IF NOT EXISTS retirement_plan_sha256 TEXT NOT NULL DEFAULT '' CHECK (retirement_plan_sha256='' OR retirement_plan_sha256 ~ '^[0-9a-f]{64}$');
ALTER TABLE fugue_platform_input_archive_control ADD COLUMN IF NOT EXISTS retirement_reader_current TEXT NOT NULL DEFAULT '' CHECK (retirement_reader_current='' OR retirement_reader_current ~ '^[0-9a-f]{40}$');
ALTER TABLE fugue_platform_input_archive_control ADD COLUMN IF NOT EXISTS retirement_reader_rollback TEXT NOT NULL DEFAULT '' CHECK (retirement_reader_rollback='' OR retirement_reader_rollback ~ '^[0-9a-f]{40}$');
ALTER TABLE fugue_platform_input_archive_control ADD COLUMN IF NOT EXISTS retirement_archive_policy_sha256 TEXT NOT NULL DEFAULT '' CHECK (retirement_archive_policy_sha256='' OR retirement_archive_policy_sha256 ~ '^[0-9a-f]{64}$');
INSERT INTO fugue_platform_input_archive_control(singleton,epoch,writes_fenced,updated_at)
 VALUES(TRUE,0,FALSE,now()) ON CONFLICT(singleton) DO NOTHING;
`

func MigrateCompilerInputArchive(ctx context.Context, databaseURL string) error {
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
	if _, err = tx.ExecContext(ctx, `SELECT set_config('lock_timeout',$1,true)`, platformStateLockLimit.String()); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(315609238744289)`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, CompilerInputArchiveSQL); err != nil {
		return fmt.Errorf("create compiler input archive ledger: %w", err)
	}
	for _, table := range []struct{ name, columns, casts string }{
		{"fugue_platform_input_hot_chunks", "content_hash,ordinal,chunk_hash,data", "NULL::text,NULL::integer,NULL::text,NULL::bytea"},
		{"fugue_platform_input_manifests", "storage_location", "NULL::text"},
		{"fugue_platform_input_archives", "content_hash,archive_format,source_format,raw_bytes,segment_bytes,segment_count,object_prefix,backend_hash,state,source_epoch,verified_digest,fencing_token,created_at,verified_at,routed_at,updated_at", "NULL::text,NULL::text,NULL::text,NULL::bigint,NULL::integer,NULL::integer,NULL::text,NULL::text,NULL::text,NULL::bigint,NULL::text,NULL::bigint,NULL::timestamptz,NULL::timestamptz,NULL::timestamptz,NULL::timestamptz"},
		{"fugue_platform_input_archive_segments", "content_hash,ordinal,object_key,raw_bytes,stored_bytes,segment_hash,stored_hash,object_etag,uploaded_at,verified_at", "NULL::text,NULL::integer,NULL::text,NULL::integer,NULL::integer,NULL::text,NULL::text,NULL::text,NULL::timestamptz,NULL::timestamptz"},
		{"fugue_platform_input_archive_leases", "content_hash,owner,source_epoch,fencing_token,expires_at,updated_at", "NULL::text,NULL::text,NULL::bigint,NULL::bigint,NULL::timestamptz,NULL::timestamptz"},
		{"fugue_platform_input_archive_control", "singleton,epoch,writes_fenced,retirement_started_at,retirement_plan_sha256,retirement_reader_current,retirement_reader_rollback,retirement_archive_policy_sha256,updated_at", "NULL::boolean,NULL::bigint,NULL::boolean,NULL::timestamptz,NULL::text,NULL::text,NULL::text,NULL::text,NULL::timestamptz"},
	} {
		rows, queryErr := tx.QueryContext(ctx, "SELECT "+table.columns+" FROM "+table.name+" WHERE false UNION ALL SELECT "+table.casts+" WHERE false")
		if queryErr != nil {
			return fmt.Errorf("verify compiler input archive schema %s: %w", table.name, queryErr)
		}
		if queryErr = rows.Close(); queryErr != nil {
			return queryErr
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}
