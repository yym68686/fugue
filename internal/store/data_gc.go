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
	return dataReferenceMutation(s, func() (model.DataSnapshot, error) { return s.createDataSnapshot(v) })
}
func (s *Store) CreateDataTransfer(v model.DataTransfer) (model.DataTransfer, error) {
	return dataReferenceMutation(s, func() (model.DataTransfer, error) { return s.createDataTransfer(v) })
}
func (s *Store) UpdateDataTransfer(v model.DataTransfer) (model.DataTransfer, error) {
	return dataReferenceMutation(s, func() (model.DataTransfer, error) { return s.updateDataTransfer(v) })
}
func (s *Store) UpdateDataWorkspace(id, tenant string, admin bool, v DataWorkspaceUpdate) (model.DataWorkspace, error) {
	return dataReferenceMutation(s, func() (model.DataWorkspace, error) { return s.updateDataWorkspace(id, tenant, admin, v) })
}

type DataGCReference struct {
	BackendID string
	SHA256    string
	ObjectKey string
}

// GC needs identities, never potentially enormous transfer plans.
func (s *Store) ListDataMigrationSourceIDs(ctx context.Context, workspaceID string) ([]string, error) {
	out := []string{}
	if s.usingDatabase() {
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
	err := s.withLockedState(false, func(st *model.State) error {
		seen := map[string]bool{}
		for _, t := range st.DataTransfers {
			if t.WorkspaceID == workspaceID && t.Direction == "migrate" && t.Status == "completed" && t.Source != "" && !seen[t.Source] {
				seen[t.Source] = true
				out = append(out, t.Source)
			}
		}
		return nil
	})
	return out, err
}

// ListDataGCReferences projects references across ALL tenants and workspaces,
// without loading transfer plans. Authorization expiry is deliberately ignored:
// it is not a transfer lifecycle deadline. Completed migrations keep source
// copies of live snapshots until an explicit migration retirement is supported.
func (s *Store) ListDataGCReferences(ctx context.Context) ([]DataGCReference, error) {
	if s.usingDatabase() {
		rows, err := s.db.QueryContext(ctx, `WITH refs AS (
		 SELECT w.storage_backend_id backend_id, e FROM fugue_data_snapshots s
		 JOIN fugue_data_workspaces w ON w.id=s.workspace_id
		 CROSS JOIN LATERAL jsonb_array_elements(s.manifest_json->'entries') e WHERE s.deleted_at IS NULL
		 UNION ALL
		 SELECT b.backend_id, e FROM fugue_data_transfers t JOIN fugue_data_workspaces w ON w.id=t.workspace_id
		 CROSS JOIN LATERAL unnest(ARRAY[w.storage_backend_id,CASE WHEN t.direction='migrate' THEN t.source ELSE w.storage_backend_id END,CASE WHEN t.direction='migrate' THEN t.target ELSE w.storage_backend_id END]) b(backend_id)
		 CROSS JOIN LATERAL jsonb_array_elements(t.manifest_json->'entries') e
		 WHERE t.status NOT IN ('completed','failed','canceled')
		 UNION ALL
		 SELECT t.source, e FROM fugue_data_transfers t JOIN fugue_data_snapshots s ON s.workspace_id=t.workspace_id
		 CROSS JOIN LATERAL jsonb_array_elements(s.manifest_json->'entries') e
		 WHERE t.direction='migrate' AND t.status='completed' AND s.deleted_at IS NULL
		 ) SELECT DISTINCT backend_id, e->>'sha256', COALESCE(e->>'object_key','') FROM refs
		 WHERE e->>'kind'='file' AND COALESCE(e->>'sha256','')<>''`)
		if err != nil {
			return nil, mapDBErr(err)
		}
		defer rows.Close()
		out := []DataGCReference{}
		for rows.Next() {
			var v DataGCReference
			if err := rows.Scan(&v.BackendID, &v.SHA256, &v.ObjectKey); err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, mapDBErr(rows.Err())
	}
	out := []DataGCReference{}
	err := s.withLockedState(false, func(state *model.State) error {
		workspaces := map[string]model.DataWorkspace{}
		for _, w := range state.DataWorkspaces {
			workspaces[w.ID] = w
		}
		add := func(backend string, manifest model.DataManifest) {
			for _, e := range manifest.Entries {
				if e.Kind == model.DataManifestEntryKindFile && e.SHA256 != "" {
					out = append(out, DataGCReference{backend, e.SHA256, e.ObjectKey})
				}
			}
		}
		for _, v := range state.DataSnapshots {
			if v.DeletedAt == nil {
				add(workspaces[v.WorkspaceID].StorageBackendID, v.Manifest)
			}
		}
		for _, t := range state.DataTransfers {
			if t.Status != model.DataTransferStatusCompleted && t.Status != model.DataTransferStatusFailed && t.Status != model.DataTransferStatusCanceled {
				ids := []string{workspaces[t.WorkspaceID].StorageBackendID}
				if t.Direction == model.DataTransferDirectionMigrate {
					ids = append(ids, t.Source, t.Target)
				}
				for _, backend := range ids {
					add(backend, t.Manifest)
				}
			}
			if t.Direction == model.DataTransferDirectionMigrate && t.Status == model.DataTransferStatusCompleted {
				for _, v := range state.DataSnapshots {
					if v.WorkspaceID == t.WorkspaceID && v.DeletedAt == nil {
						add(t.Source, v.Manifest)
					}
				}
			}
		}
		return nil
	})
	return out, err
}
