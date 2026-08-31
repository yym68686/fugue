package schemamigrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const (
	imageCacheManifestGraphLockID = int64(315609238744284)
	imageCacheManifestGraphSQL    = `ALTER TABLE fugue_image_cache_manifests ADD COLUMN IF NOT EXISTS referenced_manifests_json JSONB NULL`
)

var ErrImageCacheManifestGraphMigrationRequired = errors.New("image-cache manifest graph schema migration is required")

func MigrateImageCacheManifestGraph(ctx context.Context, databaseURL string) error {
	databaseURL, err := normalizeDatabaseURL(databaseURL)
	if err != nil {
		return err
	}
	migrateCtx, cancel := boundedContext(ctx)
	defer cancel()
	database, err := openDatabase(databaseURL)
	if err != nil {
		return err
	}
	defer database.Close()
	if err := waitForImageCacheManifestTable(migrateCtx, database, platformStatePoll); err != nil {
		return err
	}
	if err := applyImageCacheManifestGraph(migrateCtx, database); err != nil {
		return err
	}
	return verifyImageCacheManifestGraph(migrateCtx, database)
}

func waitForImageCacheManifestTable(ctx context.Context, database *sql.DB, pollInterval time.Duration) error {
	if database == nil || pollInterval <= 0 {
		return fmt.Errorf("image-cache manifest graph schema wait dependency is invalid")
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		var relationOID sql.NullInt64
		err := database.QueryRowContext(ctx, `SELECT to_regclass('fugue_image_cache_manifests')::oid`).Scan(&relationOID)
		if err == nil && relationOID.Valid && relationOID.Int64 > 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for image-cache manifest table: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func applyImageCacheManifestGraph(ctx context.Context, database *sql.DB) error {
	tx, err := database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin image-cache manifest graph migration: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT set_config('lock_timeout', $1, true)`, platformStateLockLimit.String()); err != nil {
		return fmt.Errorf("set image-cache manifest graph lock timeout: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, imageCacheManifestGraphLockID); err != nil {
		return fmt.Errorf("lock image-cache manifest graph migration: %w", err)
	}
	complete, err := inspectImageCacheManifestGraph(ctx, tx)
	if err != nil {
		return err
	}
	if !complete {
		if _, err := tx.ExecContext(ctx, imageCacheManifestGraphSQL); err != nil {
			return fmt.Errorf("apply image-cache manifest graph migration: %w", err)
		}
		complete, err = inspectImageCacheManifestGraph(ctx, tx)
		if err != nil {
			return err
		}
		if !complete {
			return ErrImageCacheManifestGraphMigrationRequired
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit image-cache manifest graph migration: %w", err)
	}
	return nil
}

func verifyImageCacheManifestGraph(ctx context.Context, database *sql.DB) error {
	complete, err := inspectImageCacheManifestGraph(ctx, database)
	if err != nil {
		return err
	}
	if !complete {
		return ErrImageCacheManifestGraphMigrationRequired
	}
	return nil
}

func inspectImageCacheManifestGraph(ctx context.Context, queryer platformStateQueryer) (bool, error) {
	rows, err := queryer.QueryContext(ctx, `
SELECT format_type(attribute.atttypid, attribute.atttypmod),
       CASE WHEN attribute.attnotnull THEN 'NO' ELSE 'YES' END
FROM pg_attribute AS attribute
WHERE attribute.attrelid = to_regclass('fugue_image_cache_manifests')::oid
  AND attribute.attnum > 0
  AND NOT attribute.attisdropped
  AND attribute.attname = 'referenced_manifests_json'`)
	if err != nil {
		return false, fmt.Errorf("query image-cache manifest graph schema: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return false, rows.Err()
	}
	var dataType, nullable string
	if err := rows.Scan(&dataType, &nullable); err != nil {
		return false, fmt.Errorf("scan image-cache manifest graph schema: %w", err)
	}
	if rows.Next() {
		return false, fmt.Errorf("image-cache manifest graph schema column is duplicated")
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	if dataType != "jsonb" || nullable != "YES" {
		return false, fmt.Errorf("image-cache manifest graph schema column has unexpected shape")
	}
	return true, nil
}
