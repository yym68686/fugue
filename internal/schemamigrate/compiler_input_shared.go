package schemamigrate

import (
	"context"
	"fmt"
	"strings"
)

// No existing input is rewritten. Foreign keys prevent deleting a
// pack while any committed input references it. Readers also verify the
// exact closure between recipe hashes and relational references.
const CompilerInputSharedSQL = `
CREATE TABLE IF NOT EXISTS fugue_platform_input_shared_packs (
 pack_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 pack_hash BYTEA NOT NULL UNIQUE CHECK (octet_length(pack_hash)=32),
 size_bytes INTEGER NOT NULL CHECK (size_bytes > 0 AND size_bytes <= 262139),
 stored_hash BYTEA NOT NULL CHECK (octet_length(stored_hash)=32),
 data BYTEA NOT NULL CHECK (octet_length(data) > 0 AND octet_length(data) <= 262144),
 created_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS fugue_platform_input_shared_objects (
 object_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 object_hash BYTEA NOT NULL UNIQUE CHECK (octet_length(object_hash)=32),
 size_bytes INTEGER NOT NULL CHECK (size_bytes > 0 AND size_bytes <= 32768),
 pack_id BIGINT NOT NULL REFERENCES fugue_platform_input_shared_packs(pack_id),
 pack_offset INTEGER NOT NULL CHECK (pack_offset>=0 AND pack_offset+size_bytes<=262139)
);
CREATE INDEX IF NOT EXISTS fugue_platform_input_shared_objects_pack_idx ON fugue_platform_input_shared_objects(pack_id);
CREATE TABLE IF NOT EXISTS fugue_platform_input_shared_recipes (
 content_hash TEXT NOT NULL REFERENCES fugue_platform_input_manifests(content_hash) DEFERRABLE INITIALLY DEFERRED,
 ordinal INTEGER NOT NULL CHECK (ordinal >= 0 AND ordinal < 4096),
 recipe_hash TEXT NOT NULL CHECK (recipe_hash ~ '^sha256:[0-9a-f]{64}$'),
 recipe BYTEA NOT NULL CHECK (octet_length(recipe) >= 17 AND octet_length(recipe) <= 49181),
 PRIMARY KEY (content_hash, ordinal)
);
CREATE TABLE IF NOT EXISTS fugue_platform_input_shared_refs (
 content_hash TEXT NOT NULL REFERENCES fugue_platform_input_manifests(content_hash) DEFERRABLE INITIALLY DEFERRED,
 pack_id BIGINT NOT NULL REFERENCES fugue_platform_input_shared_packs(pack_id),
 PRIMARY KEY (content_hash, pack_id)
);
CREATE INDEX IF NOT EXISTS fugue_platform_input_shared_refs_pack_idx ON fugue_platform_input_shared_refs(pack_id);
`

func MigrateCompilerInputShared(ctx context.Context, databaseURL string) error {
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
	var definition string
	if err = tx.QueryRowContext(ctx, `SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='fugue_platform_input_manifests'::regclass AND conname='fugue_platform_input_manifests_format_check' AND contype='c'`).Scan(&definition); err != nil {
		return err
	}
	const v2 = "CHECK (((format = 'runtime-snapshot-json-v1'::text) OR ((format = 'runtime-snapshot-json-chunk-codec-v2'::text) AND (chunk_size <= 262139))))"
	if strings.TrimSuffix(definition, " NOT VALID") != sharedInputFormatConstraint && strings.TrimSuffix(definition, " NOT VALID") != separatedInputFormatConstraint {
		if definition != v2 {
			return fmt.Errorf("unexpected compiler input format constraint")
		}
		if _, err = tx.ExecContext(ctx, `ALTER TABLE fugue_platform_input_manifests DROP CONSTRAINT fugue_platform_input_manifests_format_check, ADD CONSTRAINT fugue_platform_input_manifests_format_check CHECK (format='runtime-snapshot-json-v1' OR (format='runtime-snapshot-json-chunk-codec-v2' AND chunk_size<=262139) OR (format='runtime-snapshot-shared-v3' AND chunk_size=262144)) NOT VALID`); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, CompilerInputSharedSQL); err != nil {
		return fmt.Errorf("create shared compiler input stores: %w", err)
	}
	for _, table := range []struct{ name, columns, casts string }{
		{"fugue_platform_input_shared_packs", "pack_id,pack_hash,size_bytes,stored_hash,data,created_at", "NULL::bigint,NULL::bytea,NULL::integer,NULL::bytea,NULL::bytea,NULL::timestamptz"},
		{"fugue_platform_input_shared_objects", "object_id,object_hash,size_bytes,pack_id,pack_offset", "NULL::bigint,NULL::bytea,NULL::integer,NULL::bigint,NULL::integer"},
		{"fugue_platform_input_shared_recipes", "content_hash,ordinal,recipe_hash,recipe", "NULL::text,NULL::integer,NULL::text,NULL::bytea"},
		{"fugue_platform_input_shared_refs", "content_hash,pack_id", "NULL::text,NULL::bigint"},
	} {
		rows, err := tx.QueryContext(ctx, "SELECT "+table.columns+" FROM "+table.name+" WHERE false UNION ALL SELECT "+table.casts+" WHERE false")
		if err != nil {
			return fmt.Errorf("verify shared input schema %s: %w", table.name, err)
		}
		if err = rows.Close(); err != nil {
			return err
		}
	}
	// CREATE IF NOT EXISTS alone must not bless a pre-existing table with
	// absent/wrong uniqueness or reference barriers. Verify the exact required
	// constraints, not just the names of readable columns.
	for table, constraints := range map[string]map[string]string{
		"fugue_platform_input_shared_packs": {
			"pkey":              "PRIMARY KEY (pack_id)",
			"pack_hash_key":     "UNIQUE (pack_hash)",
			"pack_hash_check":   "CHECK ((octet_length(pack_hash) = 32))",
			"stored_hash_check": "CHECK ((octet_length(stored_hash) = 32))",
			"size_bytes_check":  "CHECK (((size_bytes > 0) AND (size_bytes <= 262139)))",
			"data_check":        "CHECK (((octet_length(data) > 0) AND (octet_length(data) <= 262144)))",
		},
		"fugue_platform_input_shared_objects": {
			"pkey":              "PRIMARY KEY (object_id)",
			"object_hash_key":   "UNIQUE (object_hash)",
			"object_hash_check": "CHECK ((octet_length(object_hash) = 32))",
			"size_bytes_check":  "CHECK (((size_bytes > 0) AND (size_bytes <= 32768)))",
			"check":             "CHECK (((pack_offset >= 0) AND ((pack_offset + size_bytes) <= 262139)))",
			"pack_id_fkey":      "FOREIGN KEY (pack_id) REFERENCES fugue_platform_input_shared_packs(pack_id)",
		},
		"fugue_platform_input_shared_recipes": {
			"pkey":              "PRIMARY KEY (content_hash, ordinal)",
			"content_hash_fkey": "FOREIGN KEY (content_hash) REFERENCES fugue_platform_input_manifests(content_hash) DEFERRABLE INITIALLY DEFERRED",
			"ordinal_check":     "CHECK (((ordinal >= 0) AND (ordinal < 4096)))",
			"recipe_check":      "CHECK (((octet_length(recipe) >= 17) AND (octet_length(recipe) <= 49181)))",
			"recipe_hash_check": "CHECK ((recipe_hash ~ '^sha256:[0-9a-f]{64}$'::text))",
		},
		"fugue_platform_input_shared_refs": {
			"pkey":              "PRIMARY KEY (content_hash, pack_id)",
			"content_hash_fkey": "FOREIGN KEY (content_hash) REFERENCES fugue_platform_input_manifests(content_hash) DEFERRABLE INITIALLY DEFERRED",
			"pack_id_fkey":      "FOREIGN KEY (pack_id) REFERENCES fugue_platform_input_shared_packs(pack_id)",
		},
	} {
		for suffix, want := range constraints {
			var actual string
			if err = tx.QueryRowContext(ctx, `SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid=$1::regclass AND conname=$2 AND convalidated`, table, table+"_"+suffix).Scan(&actual); err != nil {
				return fmt.Errorf("missing shared input constraint %s/%s: %w", table, suffix, err)
			}
			if actual != want {
				return fmt.Errorf("unexpected shared input constraint %s/%s", table, suffix)
			}
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `ALTER TABLE fugue_platform_input_manifests VALIDATE CONSTRAINT fugue_platform_input_manifests_format_check`)
	return err
}

const sharedInputFormatConstraint = "CHECK (((format = 'runtime-snapshot-json-v1'::text) OR ((format = 'runtime-snapshot-json-chunk-codec-v2'::text) AND (chunk_size <= 262139)) OR ((format = 'runtime-snapshot-shared-v3'::text) AND (chunk_size = 262144))))"
