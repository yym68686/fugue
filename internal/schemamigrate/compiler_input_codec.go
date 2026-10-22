package schemamigrate

import (
	"context"
	"fmt"
	"strings"
)

// Additive format admission only. No content is rewritten or removed, and a
// rollback migrator using CREATE IF NOT EXISTS preserves the expanded check.
func MigrateCompilerInputCodec(ctx context.Context, databaseURL string) error {
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
	err = tx.QueryRowContext(ctx, `SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='fugue_platform_input_manifests'::regclass AND conname='fugue_platform_input_manifests_format_check' AND contype='c'`).Scan(&definition)
	if err != nil {
		return fmt.Errorf("read compiler input format constraint: %w", err)
	}
	const legacy = "CHECK ((format = 'runtime-snapshot-json-v1'::text))"
	const expanded = "CHECK (((format = 'runtime-snapshot-json-v1'::text) OR ((format = 'runtime-snapshot-json-chunk-codec-v2'::text) AND (chunk_size <= 262139))))"
	if strings.TrimSuffix(definition, " NOT VALID") == expanded {
		if err = tx.Commit(); err != nil {
			return err
		}
		_, err = db.ExecContext(ctx, `ALTER TABLE fugue_platform_input_manifests VALIDATE CONSTRAINT fugue_platform_input_manifests_format_check`)
		return err
	}
	if definition != legacy {
		return fmt.Errorf("unexpected compiler input format constraint")
	}
	if _, err = tx.ExecContext(ctx, `ALTER TABLE fugue_platform_input_manifests DROP CONSTRAINT fugue_platform_input_manifests_format_check, ADD CONSTRAINT fugue_platform_input_manifests_format_check CHECK (format='runtime-snapshot-json-v1' OR (format='runtime-snapshot-json-chunk-codec-v2' AND chunk_size<=262139)) NOT VALID`); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	// Validate without holding ACCESS EXCLUSIVE for a table scan. Only manifest
	// metadata is scanned; input chunk bodies are untouched.
	_, err = db.ExecContext(ctx, `ALTER TABLE fugue_platform_input_manifests VALIDATE CONSTRAINT fugue_platform_input_manifests_format_check`)
	return err
}
