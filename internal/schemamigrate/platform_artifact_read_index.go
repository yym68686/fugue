package schemamigrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const (
	platformArtifactReadIndexLockID = int64(315609238744285)
	platformArtifactReadIndexName   = "idx_fugue_platform_artifacts_updated_id"
	platformArtifactReadIndexSQL    = `CREATE INDEX CONCURRENTLY ` + platformArtifactReadIndexName + ` ON fugue_platform_artifacts (updated_at DESC, id ASC)`
)

var ErrPlatformArtifactReadIndexRequired = errors.New("platform-artifact read index migration is required")

// MigratePlatformArtifactReadIndex uses PostgreSQL's online index builder so
// the multi-gigabyte artifact table remains readable and writable throughout
// the independent schema release.
func MigratePlatformArtifactReadIndex(ctx context.Context, databaseURL string) error {
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
	if err := waitForPlatformArtifactTable(migrateCtx, database, platformStatePoll); err != nil {
		return err
	}
	connection, err := database.Conn(migrateCtx)
	if err != nil {
		return fmt.Errorf("acquire platform-artifact index migration connection: %w", err)
	}
	defer connection.Close()
	// Keep the builder below the database's memory budget. This dedicated
	// connection is closed with the migration and is never returned to clients.
	for _, setting := range []string{
		`SET maintenance_work_mem = '32MB'`,
		`SET max_parallel_maintenance_workers = 0`,
		`SET lock_timeout = '2s'`,
	} {
		if _, err := connection.ExecContext(migrateCtx, setting); err != nil {
			return fmt.Errorf("configure platform-artifact index migration: %w", err)
		}
	}
	if _, err := connection.ExecContext(migrateCtx, `SELECT pg_advisory_lock($1)`, platformArtifactReadIndexLockID); err != nil {
		return fmt.Errorf("lock platform-artifact index migration: %w", err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = connection.ExecContext(cleanupCtx, `SELECT pg_advisory_unlock($1)`, platformArtifactReadIndexLockID)
	}()

	valid, exists, err := inspectPlatformArtifactReadIndex(migrateCtx, connection)
	if err != nil {
		return err
	}
	if valid {
		return nil
	}
	if exists {
		if _, err := connection.ExecContext(migrateCtx, `DROP INDEX CONCURRENTLY IF EXISTS `+platformArtifactReadIndexName); err != nil {
			return fmt.Errorf("drop incomplete platform-artifact read index: %w", err)
		}
	}
	if _, err := connection.ExecContext(migrateCtx, platformArtifactReadIndexSQL); err != nil {
		return fmt.Errorf("create platform-artifact read index: %w", err)
	}
	valid, _, err = inspectPlatformArtifactReadIndex(migrateCtx, connection)
	if err != nil {
		return err
	}
	if !valid {
		return ErrPlatformArtifactReadIndexRequired
	}
	return nil
}

func waitForPlatformArtifactTable(ctx context.Context, database *sql.DB, pollInterval time.Duration) error {
	if database == nil || pollInterval <= 0 {
		return errors.New("platform-artifact index schema wait dependency is invalid")
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		var relationOID sql.NullInt64
		err := database.QueryRowContext(ctx, `SELECT to_regclass('fugue_platform_artifacts')::oid`).Scan(&relationOID)
		if err == nil && relationOID.Valid && relationOID.Int64 > 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for platform-artifact table: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

type platformArtifactIndexQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func inspectPlatformArtifactReadIndex(ctx context.Context, queryer platformArtifactIndexQueryer) (valid bool, exists bool, err error) {
	var definition sql.NullString
	var isValid sql.NullBool
	err = queryer.QueryRowContext(ctx, `
SELECT pg_get_indexdef(index_class.oid), index_state.indisvalid
FROM pg_class AS index_class
JOIN pg_index AS index_state ON index_state.indexrelid = index_class.oid
WHERE index_class.oid = to_regclass($1)`, platformArtifactReadIndexName).Scan(&definition, &isValid)
	if errors.Is(err, sql.ErrNoRows) {
		return false, false, nil
	}
	if err != nil {
		return false, false, fmt.Errorf("inspect platform-artifact read index: %w", err)
	}
	exact := `CREATE INDEX ` + platformArtifactReadIndexName + ` ON public.fugue_platform_artifacts USING btree (updated_at DESC, id)`
	if !definition.Valid || definition.String != exact {
		return false, true, fmt.Errorf("platform-artifact read index has an unexpected definition; refusing replacement")
	}
	return isValid.Valid && isValid.Bool, true, nil
}
