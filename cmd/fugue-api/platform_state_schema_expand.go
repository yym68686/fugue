package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	platformStateSchemaExpandLockID    = int64(315609238744282)
	platformStateSchemaExpandTimeout   = 30 * time.Second
	platformStateSchemaExpandPoll      = 250 * time.Millisecond
	platformStateSchemaExpandLockLimit = 5 * time.Second
)

const platformStateSchemaExpandSQL = `ALTER TABLE fugue_platform_consumer_instances
	ADD COLUMN IF NOT EXISTS observation_evidence_hash TEXT NOT NULL DEFAULT '',
	ADD COLUMN IF NOT EXISTS observation_window_started_at TIMESTAMPTZ NULL,
	ADD COLUMN IF NOT EXISTS observation_window_heartbeat_count BIGINT NOT NULL DEFAULT 0`

func platformStateSchemaExpandRequired(databaseURL string) bool {
	return strings.TrimSpace(databaseURL) != ""
}

func runPlatformStateSchemaExpand(ctx context.Context, databaseURL string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var err error
	databaseURL, err = normalizePlatformStateDatabaseURL(databaseURL)
	if err != nil {
		return err
	}
	expandCtx, cancel := context.WithTimeout(ctx, platformStateSchemaExpandTimeout)
	defer cancel()
	database, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return fmt.Errorf("open platform-state schema database: %w", err)
	}
	defer database.Close()
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)

	if err := waitForPlatformStateBootstrapTable(expandCtx, database, platformStateSchemaExpandPoll); err != nil {
		return err
	}
	if err := applyPlatformStateSchemaExpand(expandCtx, database); err != nil {
		return err
	}
	if err := verifyPlatformStateSchemaExpand(expandCtx, database); err != nil {
		return err
	}
	return nil
}

func normalizePlatformStateDatabaseURL(databaseURL string) (string, error) {
	databaseURL = strings.TrimSpace(databaseURL)
	if databaseURL == "" {
		return "", fmt.Errorf("platform-state schema expand requires a PostgreSQL database URL")
	}
	if _, err := pgx.ParseConfig(databaseURL); err != nil {
		return "", fmt.Errorf("platform-state schema expand requires a valid PostgreSQL database URL")
	}
	return databaseURL, nil
}

func waitForPlatformStateBootstrapTable(ctx context.Context, database *sql.DB, pollInterval time.Duration) error {
	if database == nil {
		return fmt.Errorf("platform-state schema database is nil")
	}
	if pollInterval <= 0 {
		return fmt.Errorf("platform-state schema poll interval must be positive")
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		var relationOID sql.NullInt64
		err := database.QueryRowContext(ctx, `SELECT to_regclass('fugue_platform_consumer_instances')::oid`).Scan(&relationOID)
		if err == nil && relationOID.Valid && relationOID.Int64 > 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for platform-state bootstrap table: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func applyPlatformStateSchemaExpand(ctx context.Context, database *sql.DB) error {
	tx, err := database.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin platform-state schema expand: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT set_config('lock_timeout', $1, true)`, platformStateSchemaExpandLockLimit.String()); err != nil {
		return fmt.Errorf("set platform-state schema lock timeout: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, platformStateSchemaExpandLockID); err != nil {
		return fmt.Errorf("lock platform-state schema expand: %w", err)
	}
	complete, err := inspectPlatformStateSchemaExpand(ctx, tx)
	if err != nil {
		return err
	}
	if complete {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit platform-state schema verification: %w", err)
		}
		return nil
	}
	if _, err := tx.ExecContext(ctx, platformStateSchemaExpandSQL); err != nil {
		return fmt.Errorf("apply platform-state schema expand: %w", err)
	}
	complete, err = inspectPlatformStateSchemaExpand(ctx, tx)
	if err != nil {
		return err
	}
	if !complete {
		return fmt.Errorf("platform-state schema expand remained incomplete after DDL")
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit platform-state schema expand: %w", err)
	}
	return nil
}

func verifyPlatformStateSchemaExpand(ctx context.Context, database *sql.DB) error {
	complete, err := inspectPlatformStateSchemaExpand(ctx, database)
	if err != nil {
		return err
	}
	if !complete {
		return fmt.Errorf("platform-state schema expand is incomplete")
	}
	return nil
}

type platformStateSchemaQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func inspectPlatformStateSchemaExpand(ctx context.Context, queryer platformStateSchemaQueryer) (bool, error) {
	rows, err := queryer.QueryContext(ctx, `
SELECT attribute.attname,
       format_type(attribute.atttypid, attribute.atttypmod),
       CASE WHEN attribute.attnotnull THEN 'NO' ELSE 'YES' END,
       COALESCE(pg_get_expr(default_value.adbin, default_value.adrelid), '')
FROM pg_attribute AS attribute
LEFT JOIN pg_attrdef AS default_value
  ON default_value.adrelid = attribute.attrelid
 AND default_value.adnum = attribute.attnum
WHERE attribute.attrelid = to_regclass('fugue_platform_consumer_instances')::oid
  AND attribute.attnum > 0
  AND NOT attribute.attisdropped
  AND attribute.attname IN (
    'observation_evidence_hash',
    'observation_window_started_at',
    'observation_window_heartbeat_count'
	  )`)
	if err != nil {
		return false, fmt.Errorf("query platform-state schema expand: %w", err)
	}
	defer rows.Close()
	type columnShape struct {
		dataType   string
		nullable   string
		defaultSQL string
	}
	columns := make(map[string]columnShape, 3)
	for rows.Next() {
		var name string
		var shape columnShape
		if err := rows.Scan(&name, &shape.dataType, &shape.nullable, &shape.defaultSQL); err != nil {
			return false, fmt.Errorf("scan platform-state schema expand: %w", err)
		}
		columns[name] = shape
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate platform-state schema expand: %w", err)
	}
	expected := map[string]columnShape{
		"observation_evidence_hash":          {dataType: "text", nullable: "NO", defaultSQL: "''::text"},
		"observation_window_started_at":      {dataType: "timestamp with time zone", nullable: "YES", defaultSQL: ""},
		"observation_window_heartbeat_count": {dataType: "bigint", nullable: "NO", defaultSQL: "0"},
	}
	for name, want := range expected {
		got, ok := columns[name]
		if !ok {
			continue
		}
		if got != want {
			return false, fmt.Errorf("platform-state schema column %s failed exact verification", name)
		}
	}
	return len(columns) == len(expected), nil
}
