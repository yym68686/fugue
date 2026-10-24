package schemamigrate

import (
	"context"
	"fmt"
)

// Separate fixed-size slots preserve the serving table and its replay domain.
// A candidate replaces only its own consumer's previous shadow observation.
const PlatformShadowConsumersSQL = `CREATE TABLE IF NOT EXISTS fugue_platform_shadow_consumers (
 consumer_id TEXT NOT NULL,
 artifact_kind TEXT NOT NULL CHECK (artifact_kind = 'dns_answer_bundle'),
 scope_key TEXT NOT NULL CHECK (scope_key LIKE 'authority-cell:cell-%'),
 body JSONB NOT NULL CHECK (jsonb_typeof(body) = 'object' AND octet_length(body::text) <= 65536),
 PRIMARY KEY (consumer_id, artifact_kind, scope_key),
 CHECK (body->>'consumer_id' = consumer_id AND body->>'artifact_kind' = artifact_kind AND body->>'scope_key' = scope_key)
)`

// This additive migration does not lock or rewrite serving observations.
func MigratePlatformShadowConsumers(ctx context.Context, databaseURL string) error {
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
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('fugue/platform-shadow-consumers/v1', 0))`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, PlatformShadowConsumersSQL); err != nil {
		return fmt.Errorf("create isolated shadow observations: %w", err)
	}
	return tx.Commit()
}
