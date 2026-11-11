package schemamigrate

import (
	"context"
	"fmt"
)

// Separate fixed-size slots preserve the serving table and its replay domain.
// A candidate replaces only its own consumer's previous shadow observation.
const PlatformShadowConsumersSQL = `CREATE TABLE IF NOT EXISTS fugue_platform_shadow_consumers (
 consumer_id TEXT NOT NULL,
 artifact_kind TEXT NOT NULL CONSTRAINT fugue_shadow_kind_v2 CHECK (artifact_kind IN ('dns_answer_bundle','edge_route_bundle','caddy_route_config')),
 scope_key TEXT NOT NULL CONSTRAINT fugue_shadow_scope_v2 CHECK (
  (artifact_kind='dns_answer_bundle' AND scope_key LIKE 'authority-cell:cell-%') OR
  (artifact_kind IN ('edge_route_bundle','caddy_route_config') AND scope_key<>'' AND scope_key=lower(btrim(scope_key)))),
 body JSONB NOT NULL CHECK (jsonb_typeof(body) = 'object' AND octet_length(body::text) <= 65536),
 PRIMARY KEY (consumer_id, artifact_kind, scope_key),
 CHECK (body->>'consumer_id' = consumer_id AND body->>'artifact_kind' = artifact_kind AND body->>'scope_key' = scope_key)
)`

// Only the small isolated observation table is validated. No serving rows or
// compiler input bodies are rewritten; a busy lock or scan refuses migration.
const platformShadowConsumersUpgradeSQL = `DO $migration$
BEGIN
 IF (SELECT count(*) FROM pg_constraint WHERE conrelid='fugue_platform_shadow_consumers'::regclass
     AND conname IN ('fugue_shadow_kind_v2','fugue_shadow_scope_v2') AND convalidated)=2 THEN RETURN; END IF;
 ALTER TABLE fugue_platform_shadow_consumers
  DROP CONSTRAINT IF EXISTS fugue_platform_shadow_consumers_artifact_kind_check,
  DROP CONSTRAINT IF EXISTS fugue_platform_shadow_consumers_scope_key_check,
  DROP CONSTRAINT IF EXISTS fugue_shadow_kind_v2,
  DROP CONSTRAINT IF EXISTS fugue_shadow_scope_v2,
  ADD CONSTRAINT fugue_shadow_kind_v2 CHECK (artifact_kind IN ('dns_answer_bundle','edge_route_bundle','caddy_route_config')),
  ADD CONSTRAINT fugue_shadow_scope_v2 CHECK (
   (artifact_kind='dns_answer_bundle' AND scope_key LIKE 'authority-cell:cell-%') OR
   (artifact_kind IN ('edge_route_bundle','caddy_route_config') AND scope_key<>'' AND scope_key=lower(btrim(scope_key))));
END $migration$`

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
	for _, statement := range []string{`SET LOCAL lock_timeout='250ms'`, `SET LOCAL statement_timeout='5s'`} {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('fugue/platform-shadow-consumers/v1', 0))`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, PlatformShadowConsumersSQL); err != nil {
		return fmt.Errorf("create isolated shadow observations: %w", err)
	}
	if _, err = tx.ExecContext(ctx, platformShadowConsumersUpgradeSQL); err != nil {
		return fmt.Errorf("upgrade isolated shadow observations: %w", err)
	}
	return tx.Commit()
}
