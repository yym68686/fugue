package schemamigrate

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// Components contain canonical frame bytes or evidence bytes, never serving
// decisions. The original input digest remains the publication identity.
const CompilerInputSeparatedSQL = `
CREATE TABLE IF NOT EXISTS fugue_platform_input_components (
 content_hash TEXT PRIMARY KEY CHECK (content_hash ~ '^sha256:[0-9a-f]{64}$'),
 size_bytes BIGINT NOT NULL CHECK (size_bytes>0 AND size_bytes<=1073741824),
 chunk_count INTEGER NOT NULL CHECK (chunk_count>0 AND chunk_count<=4096),
 created_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS fugue_platform_input_component_recipes (
 content_hash TEXT NOT NULL REFERENCES fugue_platform_input_components(content_hash) DEFERRABLE INITIALLY DEFERRED,
 ordinal INTEGER NOT NULL CHECK (ordinal>=0 AND ordinal<4096),
 recipe_hash TEXT NOT NULL CHECK (recipe_hash ~ '^sha256:[0-9a-f]{64}$'),
 recipe BYTEA NOT NULL CHECK (octet_length(recipe)>=17 AND octet_length(recipe)<=49181),
 PRIMARY KEY(content_hash,ordinal)
);
CREATE TABLE IF NOT EXISTS fugue_platform_input_component_refs (
 content_hash TEXT NOT NULL REFERENCES fugue_platform_input_components(content_hash) DEFERRABLE INITIALLY DEFERRED,
 pack_id BIGINT NOT NULL REFERENCES fugue_platform_input_shared_packs(pack_id),
 PRIMARY KEY(content_hash,pack_id)
);
CREATE INDEX IF NOT EXISTS fugue_platform_input_component_refs_pack_idx ON fugue_platform_input_component_refs(pack_id);
CREATE TABLE IF NOT EXISTS fugue_platform_input_separated_roots (
 content_hash TEXT PRIMARY KEY REFERENCES fugue_platform_input_manifests(content_hash),
 frame_hash TEXT NOT NULL REFERENCES fugue_platform_input_components(content_hash),
 layout_hash BYTEA NOT NULL CHECK (octet_length(layout_hash)=32),
 layout BYTEA NOT NULL CHECK (octet_length(layout)>0 AND octet_length(layout)<=2097152)
);
CREATE TABLE IF NOT EXISTS fugue_platform_input_separated_refs (
 content_hash TEXT NOT NULL REFERENCES fugue_platform_input_separated_roots(content_hash),
 component_hash TEXT NOT NULL REFERENCES fugue_platform_input_components(content_hash),
 PRIMARY KEY(content_hash,component_hash)
);
CREATE INDEX IF NOT EXISTS fugue_platform_input_separated_refs_component_idx ON fugue_platform_input_separated_refs(component_hash);
CREATE TABLE IF NOT EXISTS fugue_platform_input_storage_events (
 event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 content_hash TEXT NOT NULL,
 old_format TEXT NOT NULL,
 new_format TEXT NOT NULL,
 old_payload_bytes BIGINT NOT NULL,
 new_pack_bytes BIGINT NOT NULL,
 created_at TIMESTAMPTZ NOT NULL
);
`

const separatedInputFormatConstraint = "CHECK (((format = 'runtime-snapshot-json-v1'::text) OR ((format = 'runtime-snapshot-json-chunk-codec-v2'::text) AND (chunk_size <= 262139)) OR ((format = 'runtime-snapshot-shared-v3'::text) AND (chunk_size = 262144)) OR ((format = 'runtime-snapshot-separated-v4'::text) AND (chunk_size = 262144))))"

func MigrateCompilerInputSeparated(ctx context.Context, databaseURL string) error {
	databaseURL, err := normalizeDatabaseURL(databaseURL)
	if err != nil {
		return err
	}
	db, err := openDatabase(databaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	ctx, cancel := boundedContext(ctx)
	defer cancel()
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
	var definition string
	if err = tx.QueryRowContext(ctx, `SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='fugue_platform_input_manifests'::regclass AND conname='fugue_platform_input_manifests_format_check'`).Scan(&definition); err != nil {
		return err
	}
	if strings.TrimSuffix(definition, " NOT VALID") != separatedInputFormatConstraint {
		if definition != sharedInputFormatConstraint {
			return fmt.Errorf("unexpected input format constraint")
		}
		if _, err = tx.ExecContext(ctx, `ALTER TABLE fugue_platform_input_manifests DROP CONSTRAINT fugue_platform_input_manifests_format_check, ADD CONSTRAINT fugue_platform_input_manifests_format_check CHECK (format='runtime-snapshot-json-v1' OR (format='runtime-snapshot-json-chunk-codec-v2' AND chunk_size<=262139) OR (format='runtime-snapshot-shared-v3' AND chunk_size=262144) OR (format='runtime-snapshot-separated-v4' AND chunk_size=262144)) NOT VALID`); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, CompilerInputSeparatedSQL); err != nil {
		return err
	}
	if err = VerifyCompilerInputSeparated(ctx, tx); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `ALTER TABLE fugue_platform_input_manifests VALIDATE CONSTRAINT fugue_platform_input_manifests_format_check`)
	return err
}

// Recheck barriers at maintenance time too: schema installation in the past is
// not proof that a later operator has not disabled a foreign-key trigger.
func VerifyCompilerInputSeparated(ctx context.Context, tx *sql.Tx) error {
	for table, constraints := range map[string]map[string]string{
		"fugue_platform_input_components":        {"pkey": "PRIMARY KEY (content_hash)"},
		"fugue_platform_input_component_recipes": {"pkey": "PRIMARY KEY (content_hash, ordinal)", "content_hash_fkey": "FOREIGN KEY (content_hash) REFERENCES fugue_platform_input_components(content_hash) DEFERRABLE INITIALLY DEFERRED"},
		"fugue_platform_input_component_refs":    {"pkey": "PRIMARY KEY (content_hash, pack_id)", "content_hash_fkey": "FOREIGN KEY (content_hash) REFERENCES fugue_platform_input_components(content_hash) DEFERRABLE INITIALLY DEFERRED", "pack_id_fkey": "FOREIGN KEY (pack_id) REFERENCES fugue_platform_input_shared_packs(pack_id)"},
		"fugue_platform_input_separated_roots":   {"pkey": "PRIMARY KEY (content_hash)", "content_hash_fkey": "FOREIGN KEY (content_hash) REFERENCES fugue_platform_input_manifests(content_hash)", "frame_hash_fkey": "FOREIGN KEY (frame_hash) REFERENCES fugue_platform_input_components(content_hash)"},
		"fugue_platform_input_separated_refs":    {"pkey": "PRIMARY KEY (content_hash, component_hash)", "content_hash_fkey": "FOREIGN KEY (content_hash) REFERENCES fugue_platform_input_separated_roots(content_hash)", "component_hash_fkey": "FOREIGN KEY (component_hash) REFERENCES fugue_platform_input_components(content_hash)"},
		"fugue_platform_input_shared_refs":       {"pkey": "PRIMARY KEY (content_hash, pack_id)", "pack_id_fkey": "FOREIGN KEY (pack_id) REFERENCES fugue_platform_input_shared_packs(pack_id)"},
		"fugue_platform_input_shared_objects":    {"pkey": "PRIMARY KEY (object_id)", "pack_id_fkey": "FOREIGN KEY (pack_id) REFERENCES fugue_platform_input_shared_packs(pack_id)"},
	} {
		for suffix, want := range constraints {
			var actual string
			err := tx.QueryRowContext(ctx, `SELECT pg_get_constraintdef(c.oid) FROM pg_constraint c WHERE c.conrelid=$1::regclass AND c.conname=$2 AND c.convalidated AND NOT EXISTS(SELECT 1 FROM pg_trigger t WHERE t.tgconstraint=c.oid AND t.tgenabled NOT IN('O','A'))`, table, table+"_"+suffix).Scan(&actual)
			if err != nil || actual != want {
				return fmt.Errorf("compiler input reference barrier missing or changed: %s/%s", table, suffix)
			}
		}
	}
	return nil
}
