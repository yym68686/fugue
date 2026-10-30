package schemamigrate

import (
	"context"
	"fmt"
)

// Independent additive tables; tombstones deliberately outlive app deletion.
// No serving rows or credentials are copied or changed by this migration.
const AppMailSQL = `
CREATE TABLE IF NOT EXISTS fugue_mail_platform_config (
 id TEXT PRIMARY KEY CHECK(id='platform'), body JSONB NOT NULL
);
CREATE TABLE IF NOT EXISTS fugue_app_mail_bindings (
 app_id TEXT PRIMARY KEY, generation BIGINT NOT NULL CHECK(generation>0), body JSONB NOT NULL
);`

func MigrateAppMail(ctx context.Context, databaseURL string) error {
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
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('fugue/app-mail-schema/v1', 0))`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, AppMailSQL); err != nil {
		return fmt.Errorf("create application mail schema: %w", err)
	}
	return tx.Commit()
}
