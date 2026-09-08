package store

import (
	"context"
	"fugue/internal/model"
	"sort"
	"time"
)

// Include terminal transfers until runtime resources have been reclaimed.
// Fresh successful caches need only their Job/credential cleanup, then expiry.
func (s *Store) ListDataPrewarmsForReconcile(limit int) ([]model.DataTransfer, error) {
	if limit < 1 || limit > 1000 {
		limit = 100
	}
	out := []model.DataTransfer{}
	now := time.Now().UTC()
	if !s.usingDatabase() {
		err := s.withLockedState(false, func(st *model.State) error {
			for _, t := range st.DataTransfers {
				if t.Direction != model.DataTransferDirectionPrewarm {
					continue
				}
				if t.Cache != nil && t.Cache.State == "removed" {
					continue
				}
				if t.Status == model.DataTransferStatusCompleted && t.Cache != nil && t.Cache.State == "ready" && t.ExpiresAt != nil && t.ExpiresAt.After(now) && t.Cache.WorkerCleaned {
					continue
				}
				out = append(out, t)
			}
			sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt.Before(out[j].UpdatedAt) })
			if len(out) > limit {
				out = out[:limit]
			}
			return nil
		})
		return out, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `SELECT id, COALESCE(tenant_id, ''), workspace_id, snapshot_id, version, message, direction, status, source, target, manifest_json, plan_blobs_json, part_size, expires_at, bytes_total, bytes_done, files_total, files_done, error_code, error_message, created_at, updated_at, started_at, finished_at, prewarm_cache_json FROM fugue_data_transfers
 WHERE direction='prewarm' AND COALESCE(prewarm_cache_json->>'state','')<>'removed'
 AND NOT (status='completed' AND COALESCE(prewarm_cache_json->>'worker_cleaned','false')='true' AND expires_at>now()) ORDER BY updated_at ASC LIMIT $1`, limit)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		t, err := scanDataTransfer(rows)
		if err != nil {
			return out, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}
