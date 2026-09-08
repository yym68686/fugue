package store

import (
	"context"
	"database/sql"
	"fugue/internal/model"
	"sort"
	"strings"
)

func validateDataReferencesState(st *model.State, tenant string, spec *model.AppSpec) error {
	if spec == nil || spec.Data == nil {
		return nil
	}
	for _, ref := range spec.Data.Workspaces {
		matches := []model.DataWorkspace{}
		for _, ws := range st.DataWorkspaces {
			if (ref.WorkspaceID != "" && ws.ID == ref.WorkspaceID) || (ref.WorkspaceID == "" && ws.TenantID == tenant && (ws.ID == ref.Workspace || ws.Name == ref.Workspace || ws.Slug == model.Slugify(ref.Workspace))) {
				matches = append(matches, ws)
			}
		}
		if len(matches) != 1 {
			return ErrConflict
		}
		if ref.Version != "" && !strings.EqualFold(ref.Version, "latest") {
			found := false
			for _, snapshot := range st.DataSnapshots {
				if snapshot.WorkspaceID == matches[0].ID && snapshot.DeletedAt == nil && (snapshot.ID == ref.Version || snapshot.Version == ref.Version) {
					found = true
					break
				}
			}
			if !found {
				return ErrConflict
			}
		}
	}
	return nil
}

// JSON references have no FK. Hold parent key-share locks until the app or
// operation write commits, so deletion's FOR UPDATE and reference scan cannot
// miss a reference that was validated just before the deletion started.
func lockDataReferencesTx(ctx context.Context, tx *sql.Tx, tenant string, spec *model.AppSpec) error {
	if spec == nil || spec.Data == nil {
		return nil
	}
	refs := append([]model.AppDataWorkspaceMaterialization(nil), spec.Data.Workspaces...)
	sort.Slice(refs, func(i, j int) bool {
		return refs[i].WorkspaceID+refs[i].Workspace < refs[j].WorkspaceID+refs[j].Workspace
	})
	for _, ref := range refs {
		rows, err := tx.QueryContext(ctx, `SELECT id FROM fugue_data_workspaces WHERE ($1<>'' AND id=$1) OR ($1='' AND tenant_id=$2 AND (id=$3 OR name=$3 OR slug=$4)) ORDER BY id FOR KEY SHARE`, ref.WorkspaceID, tenant, ref.Workspace, model.Slugify(ref.Workspace))
		if err != nil {
			return err
		}
		ids := []string{}
		for rows.Next() {
			var id string
			if err = rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if len(ids) != 1 {
			return ErrConflict
		}
		if ref.Version != "" && !strings.EqualFold(ref.Version, "latest") {
			var exists bool
			if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM fugue_data_snapshots WHERE workspace_id=$1 AND deleted_at IS NULL AND (id=$2 OR version=$2))`, ids[0], ref.Version).Scan(&exists); err != nil {
				return err
			}
			if !exists {
				return ErrConflict
			}
		}
	}
	return nil
}
