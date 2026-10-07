package schemamigrate

import (
	"context"
	"fmt"
)

const ImageRetirementSQL = `
CREATE TABLE IF NOT EXISTS fugue_image_retirements (
 image_id TEXT NOT NULL, digest TEXT NOT NULL, image_json JSONB NOT NULL,
 recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY(image_id,digest)
);
CREATE OR REPLACE FUNCTION fugue_retain_image_retirement() RETURNS trigger LANGUAGE plpgsql AS $body$
BEGIN
 IF NEW.lifecycle_state IN ('deleting','deleted') AND NEW.canonical_digest ~ '^sha256:[0-9a-f]{64}$' THEN
  INSERT INTO fugue_image_retirements(image_id,digest,image_json)
  VALUES(NEW.id,NEW.canonical_digest,to_jsonb(NEW)-'manifest_json')
  ON CONFLICT(image_id,digest) DO NOTHING;
 END IF;
 RETURN NEW;
END $body$;
DROP TRIGGER IF EXISTS fugue_retain_image_retirement ON fugue_images;
CREATE TRIGGER fugue_retain_image_retirement AFTER INSERT OR UPDATE OF lifecycle_state ON fugue_images
 FOR EACH ROW EXECUTE FUNCTION fugue_retain_image_retirement();
INSERT INTO fugue_image_retirements(image_id,digest,image_json)
 SELECT id,canonical_digest,to_jsonb(i)-'manifest_json' FROM fugue_images i
 WHERE lifecycle_state IN ('deleting','deleted') AND canonical_digest ~ '^sha256:[0-9a-f]{64}$'
 ON CONFLICT(image_id,digest) DO NOTHING;
`

func MigrateImageRetirement(ctx context.Context, databaseURL string) error {
	dsn, err := normalizeDatabaseURL(databaseURL)
	if err != nil {
		return err
	}
	db, err := openDatabase(dsn)
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
	if _, err = tx.ExecContext(ctx, `SET LOCAL lock_timeout='5s'`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(315609238744291)`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `ALTER TABLE fugue_localpv_inventories ADD COLUMN IF NOT EXISTS bound_pv_count_known BOOLEAN NOT NULL DEFAULT TRUE`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, ImageRetirementSQL+ImageOrphanSQL); err != nil {
		return fmt.Errorf("migrate image retirement: %w", err)
	}
	return tx.Commit()
}
