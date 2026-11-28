package providergate

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
)

//go:embed storage_audit.sql
var storageAuditPolicySchema string

// ApplyStorageAuditPolicySchema installs the provider-side policy fence and
// immutable receipt. The freeze and enrollment are separate operator actions.
func ApplyStorageAuditPolicySchema(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return errors.New("storage audit provider database is required")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SET LOCAL statement_timeout = '10s'; SET LOCAL lock_timeout = '1s'`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, storageAuditPolicySchema); err != nil {
		return fmt.Errorf("install storage audit policy authority: %w", err)
	}
	return tx.Commit()
}
