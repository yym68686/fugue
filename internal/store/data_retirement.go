package store

import (
	"context"
	"encoding/json"
	"errors"
	"fugue/internal/model"
	"slices"
	"time"
)

const DataRetirementGrace = 7 * 24 * time.Hour

var ErrDataWorkspaceRetiring = errors.New("data workspace is retiring; new references are prohibited")

// The normal progress-update path only needs the immutable workspace identity.
// Do not read large manifests or signed transfer plans just to check a freeze.
func (s *Store) dataTransferWorkspaceID(id string) (string, error) {
	if s.usingDatabase() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var workspaceID string
		err := s.db.QueryRowContext(ctx, `SELECT workspace_id FROM fugue_data_transfers WHERE id=$1`, id).Scan(&workspaceID)
		return workspaceID, mapDBErr(err)
	}
	t, err := s.GetDataTransfer(id)
	return t.WorkspaceID, err
}

func (s *Store) checkDataBackupTargetActive(target model.BackupTarget, tenant string) error {
	if !s.usingDatabase() {
		return s.withLockedState(false, func(st *model.State) error {
			return checkDataBackupTargetActiveState(st, target, tenant)
		})
	}
	id := target.WorkspaceID
	if id == "" {
		id = target.Name
	}
	if id == "" {
		return nil
	}
	// This is a lifecycle fence, not a new authorization boundary. Historical
	// imported policies may contain mismatched targets; the existing API and
	// execution guards must still reject those with their established errors.
	if err := s.checkDataWorkspaceActive(id); err != nil {
		return err
	}
	ws, err := s.GetDataWorkspace(id, tenant, tenant == "")
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return s.checkDataWorkspaceActive(ws.ID)
}

func checkDataBackupTargetActiveState(st *model.State, target model.BackupTarget, tenant string) error {
	id := target.WorkspaceID
	if id == "" {
		id = target.Name
	}
	if dataRetirementJobState(st, id) != nil {
		return ErrDataWorkspaceRetiring
	}
	if i := findDataWorkspaceByIDNameOrSlug(st, id, tenant, tenant == ""); i >= 0 && dataRetirementJobState(st, st.DataWorkspaces[i].ID) != nil {
		return ErrDataWorkspaceRetiring
	}
	return nil
}

func dataRetirementJobState(st *model.State, id string) *model.StorageMaintenanceJob {
	for i := range st.StorageMaintenanceJobs {
		j := &st.StorageMaintenanceJobs[i]
		if j.Kind == "workspace-retire" && j.ScopeID == id && j.Execute {
			return j
		}
	}
	return nil
}

func (s *Store) DataWorkspaceRetirement(id string) (model.StorageMaintenanceJob, error) {
	if s.usingDatabase() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var raw []byte
		var j model.StorageMaintenanceJob
		if err := s.db.QueryRowContext(ctx, `SELECT body FROM fugue_storage_maintenance_jobs WHERE kind='workspace-retire' AND scope_id=$1 AND body->>'execute'='true' ORDER BY updated_at DESC LIMIT 1`, id).Scan(&raw); err != nil {
			return j, mapDBErr(err)
		}
		err := json.Unmarshal(raw, &j)
		return j, err
	}
	var out model.StorageMaintenanceJob
	err := s.withLockedState(false, func(st *model.State) error {
		if j := dataRetirementJobState(st, id); j != nil {
			out = *j
			return nil
		}
		return ErrNotFound
	})
	return out, err
}

func (s *Store) checkDataWorkspaceActive(id string) error {
	_, err := s.DataWorkspaceRetirement(id)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return ErrDataWorkspaceRetiring
}

// Snapshots are intentionally retired by this operation. Everything else must
// either be absent or explicitly authorized for its normal lifecycle cleanup.
func DataRetirementBlockers(blockers []DataDeletionBlocker, transfers []model.DataTransfer, opt model.DataWorkspaceRetirementOptions) []DataDeletionBlocker {
	byID := map[string]model.DataTransfer{}
	for _, t := range transfers {
		byID[t.ID] = t
	}
	out := []DataDeletionBlocker{}
	for _, b := range blockers {
		switch b.Kind {
		case "snapshot", "retirement":
			continue
		case "grant":
			if opt.RevokeGrants {
				continue
			}
		case "transfer":
			t := byID[b.ID]
			cache := t.Direction == model.DataTransferDirectionPrewarm && (t.Cache == nil || t.Cache.State != "removed")
			if cache && opt.EvictCaches {
				continue
			}
			if !cache && t.Status == model.DataTransferStatusPlanned && opt.CancelPlannedTransfers {
				continue
			}
		}
		out = append(out, b)
	}
	return out
}

// Freeze and job creation are atomic with the dependency check. The reference
// fence covers snapshot/transfer writers; the parent row lock covers app/operation
// JSON references, which have no foreign key of their own.
func (s *Store) BeginDataWorkspaceRetirement(ws model.DataWorkspace, opt model.DataWorkspaceRetirementOptions, actorType, actorID string) (model.StorageMaintenanceJob, error) {
	return dataReferenceMutation(s, func() (model.StorageMaintenanceJob, error) {
		now := time.Now().UTC()
		j := model.StorageMaintenanceJob{ID: model.NewID("storage_job"), Kind: "workspace-retire", ScopeID: ws.ID, Execute: true, Status: "queued", CreatedAt: now, UpdatedAt: now,
			WorkspaceRetirement: &model.DataWorkspaceRetirement{TenantID: ws.TenantID, ProjectID: ws.ProjectID, Name: ws.Name, ActorID: actorID, ActorType: actorType, Options: opt, Phase: "evicting"}}
		if s.usingDatabase() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			tx, err := s.db.BeginTx(ctx, nil)
			if err != nil {
				return j, err
			}
			defer tx.Rollback()
			var id string
			if err = tx.QueryRowContext(ctx, `SELECT id FROM fugue_data_workspaces WHERE id=$1 FOR UPDATE`, ws.ID).Scan(&id); err != nil {
				return j, mapDBErr(err)
			}
			var raw []byte
			err = tx.QueryRowContext(ctx, `SELECT body FROM fugue_storage_maintenance_jobs WHERE kind='workspace-retire' AND scope_id=$1 AND body->>'execute'='true' ORDER BY updated_at DESC LIMIT 1`, ws.ID).Scan(&raw)
			if err == nil {
				err = json.Unmarshal(raw, &j)
				if err == nil && j.Status == "failed" {
					j.Status = "queued"
					j.Attempts = 0
					j.Error = ""
					j.RetryAt = nil
					j.UpdatedAt = now
					raw, err = json.Marshal(j)
					if err != nil {
						return j, err
					}
					if _, err = tx.ExecContext(ctx, `UPDATE fugue_storage_maintenance_jobs SET status=$2,updated_at=$3,body=$4 WHERE id=$1`, j.ID, j.Status, j.UpdatedAt, raw); err != nil {
						return j, err
					}
					err = tx.Commit()
				}
				return j, err
			}
			if !errors.Is(mapDBErr(err), ErrNotFound) {
				return j, err
			}
			blockers, err := queryDataDeletionBlockers(ctx, tx, ws, nil)
			if err != nil {
				return j, err
			}
			transfers, err := s.ListDataTransferSummaries("", ws.ID, true)
			if err != nil {
				return j, err
			}
			if len(DataRetirementBlockers(blockers, transfers, opt)) > 0 {
				return j, dataDeletionConflict()
			}
			if opt.CancelPlannedTransfers {
				if _, err = tx.ExecContext(ctx, `UPDATE fugue_data_transfers SET status='canceled',finished_at=$2,updated_at=$2 WHERE workspace_id=$1 AND status='planned' AND direction<>'prewarm'`, ws.ID, now); err != nil {
					return j, err
				}
			}
			if opt.EvictCaches {
				if _, err = tx.ExecContext(ctx, `UPDATE fugue_data_transfers SET status=CASE WHEN status IN ('planned','running') THEN 'canceled' ELSE status END, expires_at=$2,finished_at=COALESCE(finished_at,$2),updated_at=$2,prewarm_cache_json=CASE WHEN prewarm_cache_json IS NULL THEN NULL ELSE jsonb_set(jsonb_set(prewarm_cache_json,'{state}','"cleanup_pending"'),'{observed_at}',to_jsonb($2::timestamptz)) END WHERE workspace_id=$1 AND direction='prewarm' AND COALESCE(prewarm_cache_json->>'state','')<>'removed'`, ws.ID, now); err != nil {
					return j, err
				}
			}
			if opt.RevokeGrants {
				if _, err = tx.ExecContext(ctx, `UPDATE fugue_data_grants SET status='revoked',revoked_at=$2 WHERE workspace_id=$1 AND status='active'`, ws.ID, now); err != nil {
					return j, err
				}
			}
			raw, err = json.Marshal(j)
			if err != nil {
				return j, err
			}
			if _, err = tx.ExecContext(ctx, `INSERT INTO fugue_storage_maintenance_jobs(id,kind,scope_id,status,updated_at,body) VALUES($1,$2,$3,$4,$5,$6)`, j.ID, j.Kind, j.ScopeID, j.Status, j.UpdatedAt, raw); err != nil {
				return j, mapDBErr(err)
			}
			return j, tx.Commit()
		}
		err := s.withLockedState(true, func(st *model.State) error {
			if old := dataRetirementJobState(st, ws.ID); old != nil {
				if old.Status == "failed" {
					old.Status = "queued"
					old.Attempts = 0
					old.Error = ""
					old.RetryAt = nil
					old.UpdatedAt = now
				}
				j = *old
				return nil
			}
			i := findDataWorkspace(st, ws.ID)
			if i < 0 {
				return ErrNotFound
			}
			if len(DataRetirementBlockers(dataDeletionBlockers(st, ws, nil), st.DataTransfers, opt)) > 0 {
				return dataDeletionConflict()
			}
			for i := range st.DataTransfers {
				t := &st.DataTransfers[i]
				if t.WorkspaceID != ws.ID {
					continue
				}
				if t.Direction == "prewarm" && (t.Cache == nil || t.Cache.State != "removed") && opt.EvictCaches {
					t.ExpiresAt = &now
					if t.Status == "planned" || t.Status == "running" {
						t.Status = "canceled"
						t.FinishedAt = &now
					}
					if t.Cache != nil {
						t.Cache.State = "cleanup_pending"
						t.Cache.ObservedAt = now
					}
					t.UpdatedAt = now
				} else if t.Status == "planned" && opt.CancelPlannedTransfers {
					t.Status = "canceled"
					t.FinishedAt = &now
					t.UpdatedAt = now
				}
			}
			if opt.RevokeGrants {
				for i := range st.DataGrants {
					g := &st.DataGrants[i]
					if g.WorkspaceID == ws.ID && g.Status == "active" {
						g.Status = "revoked"
						g.RevokedAt = &now
					}
				}
			}
			st.StorageMaintenanceJobs = append(st.StorageMaintenanceJobs, j)
			return nil
		})
		return j, err
	})
}

// All snapshots are soft-deleted together, only after runtime cleanup has been
// observed. The task's grace clock is saved in the SAME transaction as release.
func (s *Store) ReleaseDataWorkspaceRetirement(j model.StorageMaintenanceJob) (model.StorageMaintenanceJob, error) {
	if j.WorkspaceRetirement == nil {
		return j, ErrInvalidInput
	}
	retCopy := *j.WorkspaceRetirement
	j.WorkspaceRetirement = &retCopy
	return dataReferenceMutation(s, func() (model.StorageMaintenanceJob, error) {
		current, err := s.GetStorageMaintenanceJob(j.ID)
		if err != nil {
			return j, err
		}
		if current.Kind != "workspace-retire" || current.ScopeID != j.ScopeID || current.WorkspaceRetirement == nil {
			return j, ErrInvalidInput
		}
		if current.WorkspaceRetirement.Phase != "evicting" {
			return current, nil
		}
		now := time.Now().UTC()
		transfers, err := s.ListDataTransferSummaries("", j.ScopeID, true)
		if err != nil {
			return j, err
		}
		after := now.Add(DataRetirementGrace)
		for _, t := range transfers {
			if t.ExpiresAt != nil && t.ExpiresAt.Add(DataRetirementGrace).After(after) {
				after = t.ExpiresAt.Add(DataRetirementGrace)
			}
		}
		j.WorkspaceRetirement.Phase = "grace"
		j.WorkspaceRetirement.ReleasedAt = &now
		j.WorkspaceRetirement.ReclaimAfter = &after
		j.UpdatedAt = now
		if s.usingDatabase() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			tx, err := s.db.BeginTx(ctx, nil)
			if err != nil {
				return j, err
			}
			defer tx.Rollback()
			var id string
			if err = tx.QueryRowContext(ctx, `SELECT id FROM fugue_data_workspaces WHERE id=$1 FOR UPDATE`, j.ScopeID).Scan(&id); err != nil {
				return j, mapDBErr(err)
			}
			ws, err := s.GetDataWorkspace(j.ScopeID, "", true)
			if err != nil {
				return j, err
			}
			blockers, err := queryDataDeletionBlockers(ctx, tx, ws, nil)
			if err != nil {
				return j, err
			}
			if len(DataRetirementBlockers(blockers, transfers, model.DataWorkspaceRetirementOptions{})) > 0 {
				return j, ErrConflict
			}
			if _, err = tx.ExecContext(ctx, `UPDATE fugue_data_snapshots SET deleted_at=$2 WHERE workspace_id=$1 AND deleted_at IS NULL`, j.ScopeID, now); err != nil {
				return j, err
			}
			if _, err = tx.ExecContext(ctx, `UPDATE fugue_data_workspaces SET used_bytes=0,updated_at=$2 WHERE id=$1`, j.ScopeID, now); err != nil {
				return j, err
			}
			raw, err := json.Marshal(j)
			if err != nil {
				return j, err
			}
			if _, err = tx.ExecContext(ctx, `UPDATE fugue_storage_maintenance_jobs SET body=$2,updated_at=$3 WHERE id=$1`, j.ID, raw, j.UpdatedAt); err != nil {
				return j, err
			}
			return j, tx.Commit()
		}
		err = s.withLockedState(true, func(st *model.State) error {
			i := findDataWorkspace(st, j.ScopeID)
			if i < 0 {
				return ErrNotFound
			}
			if len(DataRetirementBlockers(dataDeletionBlockers(st, st.DataWorkspaces[i], nil), st.DataTransfers, model.DataWorkspaceRetirementOptions{})) > 0 {
				return ErrConflict
			}
			for i := range st.DataSnapshots {
				v := &st.DataSnapshots[i]
				if v.WorkspaceID == j.ScopeID && v.DeletedAt == nil {
					v.DeletedAt = &now
				}
			}
			st.DataWorkspaces[i].UsedBytes = 0
			st.DataWorkspaces[i].UpdatedAt = now
			for i := range st.StorageMaintenanceJobs {
				if st.StorageMaintenanceJobs[i].ID == j.ID {
					st.StorageMaintenanceJobs[i] = j
					return nil
				}
			}
			return ErrNotFound
		})
		return j, err
	})
}

func (s *Store) FinalizeDataWorkspaceRetirement(j model.StorageMaintenanceJob) error {
	_, err := dataReferenceMutation(s, func() (bool, error) { err := s.FinalizeDataWorkspaceRetirementLocked(j); return err == nil, err })
	return err
}

// FinalizeDataWorkspaceRetirementLocked must be called while holding
// DataReferenceLock through the final shared-reference check AND this deletion.
func (s *Store) FinalizeDataWorkspaceRetirementLocked(j model.StorageMaintenanceJob) error {
	if j.WorkspaceRetirement == nil || j.WorkspaceRetirement.ReclaimAfter == nil || time.Now().Before(*j.WorkspaceRetirement.ReclaimAfter) {
		return ErrConflict
	}
	finalize := func() (bool, error) {
		if s.usingDatabase() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			tx, err := s.db.BeginTx(ctx, nil)
			if err != nil {
				return false, err
			}
			defer tx.Rollback()
			var id string
			err = tx.QueryRowContext(ctx, `SELECT id FROM fugue_data_workspaces WHERE id=$1 FOR UPDATE`, j.ScopeID).Scan(&id)
			if errors.Is(mapDBErr(err), ErrNotFound) {
				return true, nil
			}
			if err != nil {
				return false, err
			}
			ws, err := s.GetDataWorkspace(j.ScopeID, "", true)
			if err != nil {
				return false, err
			}
			b, err := queryDataDeletionBlockers(ctx, tx, ws, nil)
			if err != nil {
				return false, err
			}
			for _, v := range b {
				if v.Kind != "retirement" {
					return false, ErrConflict
				}
			}
			_, err = tx.ExecContext(ctx, `DELETE FROM fugue_data_workspaces WHERE id=$1`, j.ScopeID)
			if err != nil {
				return false, err
			}
			return true, tx.Commit()
		}
		err := s.withLockedState(true, func(st *model.State) error {
			i := findDataWorkspace(st, j.ScopeID)
			if i < 0 {
				return nil
			}
			for _, v := range dataDeletionBlockers(st, st.DataWorkspaces[i], nil) {
				if v.Kind != "retirement" {
					return ErrConflict
				}
			}
			st.DataWorkspaces = append(st.DataWorkspaces[:i], st.DataWorkspaces[i+1:]...)
			st.DataSnapshots = slices.DeleteFunc(st.DataSnapshots, func(v model.DataSnapshot) bool { return v.WorkspaceID == j.ScopeID })
			st.DataTransfers = slices.DeleteFunc(st.DataTransfers, func(v model.DataTransfer) bool { return v.WorkspaceID == j.ScopeID })
			st.DataGrants = slices.DeleteFunc(st.DataGrants, func(v model.DataGrant) bool { return v.WorkspaceID == j.ScopeID })
			st.DataWorkspaceAccessGrants = slices.DeleteFunc(st.DataWorkspaceAccessGrants, func(v model.DataWorkspaceAccessGrant) bool { return v.WorkspaceID == j.ScopeID })
			return nil
		})
		return err == nil, err
	}
	_, err := finalize()
	return err
}

// Project only content identities. Never load signed transfer plans into a
// retirement task or expose them in its progress responses.
func (s *Store) ListDataRetirementObjects(ctx context.Context, ws model.DataWorkspace) ([]DataGCReference, error) {
	if s.usingDatabase() {
		rows, err := s.db.QueryContext(ctx, `WITH entries AS (
		 SELECT $2::text backend_id,e FROM fugue_data_snapshots s CROSS JOIN LATERAL jsonb_array_elements(s.manifest_json->'entries') e WHERE s.workspace_id=$1
		 UNION ALL
		 SELECT b.backend_id,e FROM fugue_data_transfers t CROSS JOIN LATERAL unnest(ARRAY[$2::text,CASE WHEN t.direction='migrate' THEN t.source ELSE $2 END,CASE WHEN t.direction='migrate' THEN t.target ELSE $2 END]) b(backend_id) CROSS JOIN LATERAL jsonb_array_elements(t.manifest_json->'entries') e WHERE t.workspace_id=$1
		 UNION ALL
		 SELECT t.source,e FROM fugue_data_transfers t JOIN fugue_data_snapshots s ON s.workspace_id=t.workspace_id CROSS JOIN LATERAL jsonb_array_elements(s.manifest_json->'entries') e WHERE t.workspace_id=$1 AND t.direction='migrate' AND t.status='completed'
		) SELECT DISTINCT backend_id,e->>'sha256',COALESCE(e->>'object_key',''),COALESCE((e->>'size')::bigint,0) FROM entries WHERE e->>'kind'='file' AND COALESCE(e->>'sha256','')<>''`, ws.ID, ws.StorageBackendID)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := []DataGCReference{}
		for rows.Next() {
			v := DataGCReference{WorkspaceID: ws.ID}
			if err = rows.Scan(&v.BackendID, &v.SHA256, &v.ObjectKey, &v.Size); err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, rows.Err()
	}
	out := []DataGCReference{}
	err := s.withLockedState(false, func(st *model.State) error {
		add := func(backend string, m model.DataManifest) {
			for _, e := range m.Entries {
				if e.Kind == "file" && e.SHA256 != "" {
					out = append(out, DataGCReference{BackendID: backend, SHA256: e.SHA256, ObjectKey: e.ObjectKey, Size: e.Size, WorkspaceID: ws.ID})
				}
			}
		}
		for _, v := range st.DataSnapshots {
			if v.WorkspaceID == ws.ID {
				add(ws.StorageBackendID, v.Manifest)
			}
		}
		for _, t := range st.DataTransfers {
			if t.WorkspaceID != ws.ID {
				continue
			}
			add(ws.StorageBackendID, t.Manifest)
			if t.Direction == "migrate" {
				add(t.Source, t.Manifest)
				add(t.Target, t.Manifest)
				if t.Status == "completed" {
					for _, v := range st.DataSnapshots {
						if v.WorkspaceID == ws.ID {
							add(t.Source, v.Manifest)
						}
					}
				}
			}
		}
		return nil
	})
	return out, err
}
