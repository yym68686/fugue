package schemamigrate

import (
	"context"
	"database/sql"
	"fmt"
)

// This additive migration leaves old code and existing release rows valid.
// Once a controller binds a workload, ordinary release updates cannot replace
// or erase its identity, including updates from a rolled-back controller.
// The executable intent is frozen with the binding; routing and runtime facts
// can still change without destroying the original revision's provenance.
const appReleaseWorkloadSQL = `
ALTER TABLE fugue_app_releases ADD COLUMN IF NOT EXISTS revision_workload_json JSONB NULL;
CREATE OR REPLACE FUNCTION fugue_preserve_app_release_workload() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.revision_workload_json IS NOT NULL AND jsonb_typeof(NEW.revision_workload_json) <> 'object' THEN
  RAISE EXCEPTION 'revision workload must be an object' USING ERRCODE = '23514';
 END IF;
 IF TG_OP = 'UPDATE' AND OLD.revision_workload_json IS NOT NULL AND
    (NEW.revision_workload_json IS DISTINCT FROM OLD.revision_workload_json OR
     NEW.id IS DISTINCT FROM OLD.id OR NEW.app_id IS DISTINCT FROM OLD.app_id OR NEW.tenant_id IS DISTINCT FROM OLD.tenant_id) THEN
  RAISE EXCEPTION 'bound revision workload identity is immutable' USING ERRCODE = '23514';
 END IF;
 RETURN NEW;
END
$$;
CREATE OR REPLACE TRIGGER fugue_app_release_workload_immutable
BEFORE INSERT OR UPDATE ON fugue_app_releases
FOR EACH ROW EXECUTE FUNCTION fugue_preserve_app_release_workload();

-- A separate additive guard survives rollback to the earlier schema writer,
-- which replaces only the workload identity function above.
CREATE OR REPLACE FUNCTION fugue_preserve_bound_release_intent() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.revision_workload_json IS NOT NULL AND
    (NEW.source_ref IS DISTINCT FROM OLD.source_ref OR
     NEW.resolved_image_ref IS DISTINCT FROM OLD.resolved_image_ref OR
     NEW.runtime_id IS DISTINCT FROM OLD.runtime_id OR
     NEW.spec_snapshot_json IS DISTINCT FROM OLD.spec_snapshot_json) THEN
  RAISE EXCEPTION 'bound revision executable intent is immutable' USING ERRCODE = '40001';
 END IF;
 RETURN NEW;
END
$$;
CREATE OR REPLACE TRIGGER fugue_app_release_intent_immutable
BEFORE UPDATE ON fugue_app_releases
FOR EACH ROW EXECUTE FUNCTION fugue_preserve_bound_release_intent();
`

func MigrateAppReleaseWorkload(ctx context.Context, databaseURL string) error {
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
	return applyAppReleaseWorkload(ctx, db)
}

func applyAppReleaseWorkload(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT set_config('lock_timeout', $1, true)`, platformStateLockLimit.String()); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, int64(315609238744289)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, appReleaseWorkloadSQL); err != nil {
		return fmt.Errorf("migrate app release workload binding: %w", err)
	}
	var complete bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (
 SELECT 1 FROM pg_attribute WHERE attrelid=to_regclass('fugue_app_releases')
 AND attname='revision_workload_json' AND atttypid='jsonb'::regtype AND NOT attnotnull AND NOT attisdropped
) AND EXISTS (
 SELECT 1 FROM pg_trigger WHERE tgrelid=to_regclass('fugue_app_releases')
 AND tgname='fugue_app_release_workload_immutable' AND tgenabled='O' AND NOT tgisinternal
) AND EXISTS (
 SELECT 1 FROM pg_trigger WHERE tgrelid=to_regclass('fugue_app_releases')
 AND tgname='fugue_app_release_intent_immutable' AND tgenabled='O' AND NOT tgisinternal
)`).Scan(&complete); err != nil {
		return fmt.Errorf("verify app release workload binding schema: %w", err)
	}
	if !complete {
		return fmt.Errorf("app release workload binding schema has an unexpected shape")
	}
	return tx.Commit()
}
