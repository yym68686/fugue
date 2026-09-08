package store

import (
	"context"
	"database/sql"
	"fmt"
	"fugue/internal/model"
	"strings"
)

func dataDeletionConflict() error {
	return fmt.Errorf("%w: data resource is referenced; detach apps, finish or cancel transfers, revoke grants and delete retained snapshots first", ErrConflict)
}
func dataSpecReferences(spec *model.AppSpec, tenant string, workspace model.DataWorkspace, snapshot *model.DataSnapshot) bool {
	if spec == nil || spec.Data == nil {
		return false
	}
	for _, ref := range spec.Data.Workspaces {
		matches := ref.WorkspaceID == workspace.ID || (tenant == workspace.TenantID && (ref.Workspace == workspace.Name || ref.Workspace == workspace.Slug || ref.Workspace == workspace.ID))
		if matches && (snapshot == nil || ref.Version == "" || strings.EqualFold(ref.Version, "latest") || ref.Version == snapshot.Version || ref.Version == snapshot.ID) {
			return true
		}
	}
	return false
}
func guardDataDeletion(state *model.State, workspace model.DataWorkspace, snapshot *model.DataSnapshot) error {
	if len(dataDeletionBlockers(state, workspace, snapshot)) > 0 {
		return dataDeletionConflict()
	}
	return nil
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
