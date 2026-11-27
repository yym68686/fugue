package store

import (
	"context"
	"database/sql"
	"fmt"
	"fugue/internal/model"
)

func dataDeletionConflict() error {
	return fmt.Errorf("%w: data resource is referenced; detach apps, finish or cancel transfers, revoke grants and delete retained snapshots first", ErrConflict)
}

// Lock the workspace row before checking dependent records. Its foreign-key
// writers cannot create new transfer/grant/snapshot references during deletion.
func guardPGDataDeletion(ctx context.Context, tx *sql.Tx, workspaceID string, snapshot *model.DataSnapshot) error {
	var tenant, name, slug string
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(tenant_id,''),name,slug FROM fugue_data_workspaces WHERE id=$1 FOR UPDATE`, workspaceID).Scan(&tenant, &name, &slug); err != nil {
		return mapDBErr(err)
	}
	blockers, err := queryDataDeletionBlockers(ctx, tx, model.DataWorkspace{ID: workspaceID, TenantID: tenant, Name: name, Slug: slug}, snapshot)
	if err != nil {
		return err
	}
	if len(blockers) > 0 {
		return dataDeletionConflict()
	}
	return nil
}
