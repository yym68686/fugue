package schemamigrate

import (
	"context"
	"fmt"
)

// Enforce the retirement fence even for older API/controller processes. A
// release row becomes a durable tombstone before Kubernetes cleanup begins.
// Both sides lock the referenced rows; competing policy/retirement writes
// either serialize or one transaction aborts, never both commit unsafely.
const appReleaseRetirementSQL = `
CREATE OR REPLACE FUNCTION fugue_guard_app_release_retirement() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE p record;
BEGIN
 IF TG_OP = 'UPDATE' AND (OLD.status = 'retired' OR OLD.role = 'retired') THEN
  IF NEW IS DISTINCT FROM OLD THEN
   RAISE EXCEPTION 'retired release is immutable' USING ERRCODE = '40001';
  END IF;
  RETURN NEW;
 END IF;
 IF NEW.status = 'retired' OR NEW.role = 'retired' THEN
  IF NEW.status <> 'retired' OR NEW.role <> 'retired' THEN
   RAISE EXCEPTION 'incomplete release retirement' USING ERRCODE = '40001';
  END IF;
  FOR p IN SELECT stable_release_id,candidate_release_id FROM fugue_app_traffic_policies
   WHERE app_id = NEW.app_id OR stable_release_id = NEW.id OR candidate_release_id = NEW.id FOR UPDATE LOOP
   IF p.stable_release_id = NEW.id OR p.candidate_release_id = NEW.id THEN
    RAISE EXCEPTION 'traffic still references retiring release' USING ERRCODE = '40001';
   END IF;
  END LOOP;
 END IF;
 RETURN NEW;
END
$$;
CREATE OR REPLACE FUNCTION fugue_guard_retired_traffic_references() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE r record;
BEGIN
 FOR r IN SELECT id,status,role FROM fugue_app_releases
  WHERE id IN (NEW.stable_release_id,NEW.candidate_release_id) ORDER BY id FOR SHARE LOOP
  IF r.status = 'retired' OR r.role = 'retired' THEN
   RAISE EXCEPTION 'traffic cannot reference retired release' USING ERRCODE = '40001';
  END IF;
 END LOOP;
 RETURN NEW;
END
$$;
CREATE OR REPLACE TRIGGER fugue_app_release_retirement_fence
BEFORE INSERT OR UPDATE ON fugue_app_releases
FOR EACH ROW EXECUTE FUNCTION fugue_guard_app_release_retirement();
CREATE OR REPLACE TRIGGER fugue_app_traffic_retirement_fence
BEFORE INSERT OR UPDATE ON fugue_app_traffic_policies
FOR EACH ROW EXECUTE FUNCTION fugue_guard_retired_traffic_references();
`

func MigrateAppReleaseRetirement(ctx context.Context, databaseURL string) error {
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
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, int64(315609238744290)); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, appReleaseRetirementSQL); err != nil {
		return fmt.Errorf("migrate release retirement fence: %w", err)
	}
	var count int
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM pg_trigger WHERE NOT tgisinternal AND tgenabled='O' AND
 ((tgrelid=to_regclass('fugue_app_releases') AND tgname='fugue_app_release_retirement_fence') OR
  (tgrelid=to_regclass('fugue_app_traffic_policies') AND tgname='fugue_app_traffic_retirement_fence'))`).Scan(&count); err != nil {
		return err
	}
	if count != 2 {
		return fmt.Errorf("release retirement fence triggers missing")
	}
	return tx.Commit()
}
