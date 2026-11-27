package store

import (
	"context"
	"database/sql"
	"fugue/internal/model"
	"time"
)

type DataDeletionBlocker struct {
	Kind  string `json:"kind"`
	ID    string `json:"id"`
	State string `json:"state"`
}

func (s *Store) InspectDataDeletion(ws model.DataWorkspace, snapshot *model.DataSnapshot) ([]DataDeletionBlocker, error) {

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return queryDataDeletionBlockers(ctx, s.db, ws, snapshot)
}

type dataDeletionQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func queryDataDeletionBlockers(ctx context.Context, q dataDeletionQueryer, ws model.DataWorkspace, snapshot *model.DataSnapshot) ([]DataDeletionBlocker, error) {
	snapshotID, version := "", ""
	if snapshot != nil {
		snapshotID = snapshot.ID
		version = snapshot.Version
	}
	rows, err := q.QueryContext(ctx, `
SELECT 'transfer',id,status FROM fugue_data_transfers WHERE workspace_id=$1 AND (status IN ('planned','running') OR (direction='prewarm' AND COALESCE(prewarm_cache_json->>'state','')<>'removed'))
	UNION ALL SELECT 'retirement',id,status FROM fugue_storage_maintenance_jobs WHERE kind='workspace-retire' AND scope_id=$1 AND body->>'execute'='true'
	UNION ALL SELECT 'backup_policy',id,status FROM fugue_backup_policies WHERE enabled=true AND target_type='data-workspace' AND (target_json->>'workspace_id'=$1 OR (COALESCE(target_json->>'workspace_id','')='' AND COALESCE(target_tenant_id,'')=$4 AND target_json->>'name' IN ($5,$6)))
	UNION ALL SELECT 'backup_run',id,status FROM fugue_backup_runs WHERE status IN ('pending','running') AND target_type='data-workspace' AND (target_json->>'workspace_id'=$1 OR (COALESCE(target_json->>'workspace_id','')='' AND COALESCE(target_tenant_id,'')=$4 AND target_json->>'name' IN ($5,$6)))
	UNION ALL SELECT 'backup_artifact',id,status FROM fugue_backup_artifacts WHERE status='active' AND target_type='data-workspace' AND (target_json->>'workspace_id'=$1 OR (COALESCE(target_json->>'workspace_id','')='' AND COALESCE(target_tenant_id,'')=$4 AND target_json->>'name' IN ($5,$6)))
UNION ALL SELECT 'grant',id,status FROM fugue_data_grants WHERE workspace_id=$1 AND status='active' AND (expires_at IS NULL OR expires_at>now()) AND ($2='' OR snapshot_id='' OR snapshot_id=$2)
UNION ALL SELECT 'snapshot',id,'retained' FROM fugue_data_snapshots WHERE workspace_id=$1 AND deleted_at IS NULL AND $2=''
UNION ALL SELECT DISTINCT 'app',a.id,'referenced' FROM fugue_apps a,jsonb_array_elements(COALESCE(a.spec_json->'data'->'workspaces','[]'::jsonb)) ref
 WHERE (ref->>'workspace_id'=$1 OR (COALESCE(a.tenant_id,'')=$4 AND ref->>'workspace' IN ($1,$5,$6))) AND ($2='' OR COALESCE(ref->>'version','') IN ('','latest',$2,$3))
UNION ALL SELECT DISTINCT 'operation',op.id,op.status FROM fugue_operations op,jsonb_array_elements(COALESCE(op.desired_spec_json->'data'->'workspaces','[]'::jsonb)) ref
 WHERE op.status IN ('pending','running','waiting-agent') AND (ref->>'workspace_id'=$1 OR (COALESCE(op.tenant_id,'')=$4 AND ref->>'workspace' IN ($1,$5,$6))) AND ($2='' OR COALESCE(ref->>'version','') IN ('','latest',$2,$3))
ORDER BY 1,2`, ws.ID, snapshotID, version, ws.TenantID, ws.Name, ws.Slug)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []DataDeletionBlocker{}
	for rows.Next() {
		var b DataDeletionBlocker
		if err = rows.Scan(&b.Kind, &b.ID, &b.State); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}
