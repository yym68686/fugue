package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fugue/internal/model"
	"sort"
	"time"
)

// Bounded, engine-specific projection; never loads payloads or large manifests.
func (s *Store) ListLonghornReconciliationArtifacts(limit int) ([]BackupUsageArtifact, error) {
	if limit < 1 || limit > 501 {
		return nil, ErrInvalidInput
	}
	if !s.usingDatabase() {
		items, err := s.ListBackupUsageArtifacts("", true)
		if err != nil {
			return nil, err
		}
		out := []BackupUsageArtifact{}
		for _, v := range items {
			if v.Artifact.Kind == model.BackupArtifactKindLonghornSnapshot {
				out = append(out, v)
			}
		}
		sort.Slice(out, func(i, j int) bool {
			if !out[i].Artifact.CreatedAt.Equal(out[j].Artifact.CreatedAt) {
				return out[i].Artifact.CreatedAt.After(out[j].Artifact.CreatedAt)
			}
			return out[i].Artifact.ID > out[j].Artifact.ID
		})
		if len(out) > limit {
			out = out[:limit]
		}
		return out, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `SELECT id, run_id, backend_id, tenant_id, status, protected, deleted_at, physical_deleted_at, physical_delete_attempted_at, physical_delete_error,
		jsonb_build_object('longhorn_backup_name',manifest_json->'metadata'->>'longhorn_backup_name','volume_name',manifest_json->'metadata'->>'volume_name',
		'backup_target_name',manifest_json->'metadata'->>'backup_target_name','longhorn_namespace',manifest_json->'metadata'->>'longhorn_namespace',
		'backup_target_url',manifest_json->'metadata'->>'backup_target_url','longhorn_backup_url',manifest_json->'metadata'->>'longhorn_backup_url',
		'longhorn_cleanup_version',manifest_json->'metadata'->>'longhorn_cleanup_version')
		FROM fugue_backup_artifacts WHERE kind = 'longhorn-snapshot' ORDER BY created_at DESC, id DESC LIMIT $1`, limit)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	items := []BackupUsageArtifact{}
	for rows.Next() {
		var v BackupUsageArtifact
		var raw []byte
		var runID, backendID, tenantID sql.NullString
		a := &v.Artifact
		if err = rows.Scan(&a.ID, &runID, &backendID, &tenantID, &a.Status, &a.Protected, &a.DeletedAt, &v.PhysicalDeletedAt, &v.PhysicalDeleteAttemptedAt, &v.PhysicalDeleteError, &raw); err != nil {
			return nil, err
		}
		a.RunID, a.BackendID, a.TenantID = runID.String, backendID.String, tenantID.String
		a.Kind = model.BackupArtifactKindLonghornSnapshot
		if err = json.Unmarshal(raw, &a.Manifest.Metadata); err != nil {
			return nil, err
		}
		items = append(items, v)
	}
	return items, mapDBErr(rows.Err())
}
