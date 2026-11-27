package store

import (
	"context"
	"time"

	"fugue/internal/model"
)

// DataReferenceLock is shared by GC and every operation which can introduce a
// blob reference. Inventory listing happens outside it; final mark-and-delete
// happens inside it. An unrelated sweeper lock would not fence writers.
const DataReferenceLock = "fugue-data-references-v1"

func dataReferenceMutation[T any](s *Store, fn func() (T, error)) (out T, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	acquired, err := s.WithAdvisoryLock(ctx, DataReferenceLock, func() error {
		var e error
		out, e = fn()
		return e
	})
	if err == nil && !acquired {
		err = ErrConflict
	}
	return out, err
}

func (s *Store) CreateDataSnapshot(v model.DataSnapshot) (model.DataSnapshot, error) {
	return dataReferenceMutation(s, func() (model.DataSnapshot, error) {
		if err := s.checkDataWorkspaceActive(v.WorkspaceID); err != nil {
			return model.DataSnapshot{}, err
		}
		return s.createDataSnapshot(v)
	})
}
func (s *Store) CreateDataTransfer(v model.DataTransfer) (model.DataTransfer, error) {
	return dataReferenceMutation(s, func() (model.DataTransfer, error) {
		if err := s.checkDataWorkspaceActive(v.WorkspaceID); err != nil {
			return model.DataTransfer{}, err
		}
		return s.createDataTransfer(v)
	})
}
func (s *Store) UpdateDataTransfer(v model.DataTransfer) (model.DataTransfer, error) {
	return dataReferenceMutation(s, func() (model.DataTransfer, error) {
		workspaceID, err := s.dataTransferWorkspaceID(v.ID)
		if err != nil {
			return model.DataTransfer{}, err
		}
		if err := s.checkDataWorkspaceActive(workspaceID); err != nil {
			if err != ErrDataWorkspaceRetiring {
				return model.DataTransfer{}, err
			}
			old, err := s.GetDataTransfer(v.ID)
			if err != nil {
				return model.DataTransfer{}, err
			}
			cleanupOnly := v.Direction == "prewarm" && v.Status == old.Status && (v.Status == "completed" || v.Status == "canceled" || v.Status == "failed")
			if (v.Status != "canceled" && v.Status != "failed" && !cleanupOnly) || v.WorkspaceID != old.WorkspaceID || v.Direction != old.Direction || v.Source != old.Source || v.Target != old.Target || v.SnapshotID != old.SnapshotID || digestManifest(v.Manifest) != digestManifest(old.Manifest) {
				return model.DataTransfer{}, ErrDataWorkspaceRetiring
			}
		}
		return s.updateDataTransfer(v)
	})
}
func (s *Store) UpdateDataWorkspace(id, tenant string, admin bool, v DataWorkspaceUpdate) (model.DataWorkspace, error) {
	return dataReferenceMutation(s, func() (model.DataWorkspace, error) {
		ws, err := s.GetDataWorkspace(id, tenant, admin)
		if err != nil {
			return ws, err
		}
		if err := s.checkDataWorkspaceActive(ws.ID); err != nil {
			return ws, err
		}
		return s.updateDataWorkspace(id, tenant, admin, v)
	})
}

type DataGCReference struct {
	BackendID   string
	SHA256      string
	ObjectKey   string
	WorkspaceID string
	Size        int64
}

// GC needs identities, never potentially enormous transfer plans.
func (s *Store) ListDataMigrationSourceIDs(ctx context.Context, workspaceID string) ([]string, error) {
	out := []string{}
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT source FROM fugue_data_transfers WHERE workspace_id=$1 AND direction='migrate' AND status='completed' AND source<>''`, workspaceID)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, mapDBErr(rows.Err())
}

// ListDataGCReferences projects references across ALL tenants and workspaces,
// without loading transfer plans. Authorization expiry is deliberately ignored:
// it is not a transfer lifecycle deadline. Completed migrations keep source
// copies of live snapshots until an explicit migration retirement is supported.
func (s *Store) ListDataGCReferences(ctx context.Context) ([]DataGCReference, error) {
	rows, err := s.db.QueryContext(ctx, `WITH refs AS (
		 SELECT w.storage_backend_id backend_id,w.id workspace_id, e FROM fugue_data_snapshots s
		 JOIN fugue_data_workspaces w ON w.id=s.workspace_id
		 CROSS JOIN LATERAL jsonb_array_elements(s.manifest_json->'entries') e WHERE s.deleted_at IS NULL OR s.deleted_at>now()-interval '7 days' OR EXISTS(SELECT 1 FROM fugue_storage_maintenance_jobs j WHERE j.kind='workspace-retire' AND j.scope_id=w.id AND j.body->>'execute'='true')
		 UNION ALL
		 SELECT b.backend_id,w.id workspace_id, e FROM fugue_data_transfers t JOIN fugue_data_workspaces w ON w.id=t.workspace_id
		 CROSS JOIN LATERAL unnest(ARRAY[w.storage_backend_id,CASE WHEN t.direction='migrate' THEN t.source ELSE w.storage_backend_id END,CASE WHEN t.direction='migrate' THEN t.target ELSE w.storage_backend_id END]) b(backend_id)
		 CROSS JOIN LATERAL jsonb_array_elements(t.manifest_json->'entries') e
		 WHERE t.status NOT IN ('completed','failed','canceled') OR t.updated_at>now()-interval '7 days' OR EXISTS(SELECT 1 FROM fugue_storage_maintenance_jobs j WHERE j.kind='workspace-retire' AND j.scope_id=w.id AND j.body->>'execute'='true')
		 UNION ALL
		 SELECT t.source,t.workspace_id, e FROM fugue_data_transfers t JOIN fugue_data_snapshots s ON s.workspace_id=t.workspace_id
		 CROSS JOIN LATERAL jsonb_array_elements(s.manifest_json->'entries') e
		 WHERE t.direction='migrate' AND t.status='completed' AND (s.deleted_at IS NULL OR s.deleted_at>now()-interval '7 days' OR EXISTS(SELECT 1 FROM fugue_storage_maintenance_jobs j WHERE j.kind='workspace-retire' AND j.scope_id=t.workspace_id AND j.body->>'execute'='true'))
		 ) SELECT DISTINCT backend_id, e->>'sha256', COALESCE(e->>'object_key',''),workspace_id,COALESCE((e->>'size')::bigint,0) FROM refs
		 WHERE e->>'kind'='file' AND COALESCE(e->>'sha256','')<>''`)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	out := []DataGCReference{}
	for rows.Next() {
		var v DataGCReference
		if err := rows.Scan(&v.BackendID, &v.SHA256, &v.ObjectKey, &v.WorkspaceID, &v.Size); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, mapDBErr(rows.Err())
}
