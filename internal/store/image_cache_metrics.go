package store

import (
	"context"
	"time"

	"fugue/internal/model"
)

// ListImageCachePrunePlanMetrics reads the same recent plan window as the
// diagnostic inventory, without transferring or decoding per-image evidence.
// The result is a metric projection, never an executable deletion plan.
func (s *Store) ListImageCachePrunePlanMetrics(parent context.Context, limit int) ([]model.ImageCachePrunePlan, error) {
	if limit <= 0 {
		return nil, ErrInvalidInput
	}

	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `SELECT id, node_id, cluster_node_name, runtime_id, mode, status, created_at,
 candidate_manifest_count, candidate_blob_count, candidate_blob_bytes, planned_delete_bytes, protected_manifest_count, protection_summary_json
 FROM fugue_image_cache_prune_plans ORDER BY created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	var plans []model.ImageCachePrunePlan
	for rows.Next() {
		var p model.ImageCachePrunePlan
		var protection []byte
		if err := rows.Scan(&p.ID, &p.NodeID, &p.ClusterNodeName, &p.RuntimeID, &p.Mode, &p.Status, &p.CreatedAt,
			&p.CandidateManifestCount, &p.CandidateBlobCount, &p.CandidateBlobBytes, &p.PlannedDeleteBytes, &p.ProtectedManifestCount, &protection); err != nil {
			return nil, mapDBErr(err)
		}
		p.ProtectionSummary, err = decodeJSONValue[map[string]int](protection)
		if err != nil {
			return nil, err
		}
		plans = append(plans, p)
	}
	return plans, mapDBErr(rows.Err())
}

// ListImageCachePruneTaskMetrics excludes unrelated maintenance history and
// task logs, and returns only the latest completed task per raw metric label set.
// Do not filter prune_reason before selecting the latest task: a newer manual
// prune must still suppress an older orphan-prune result. The caller retains
// its final reduction over trimmed labels; raw grouping here conservatively
// preserves distinct whitespace spellings without duplicating Go normalization
// rules in SQL.
func (s *Store) ListImageCachePruneTaskMetrics(parent context.Context) ([]model.NodeUpdateTask, error) {

	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `SELECT id, machine_id, cluster_node_name, runtime_id, created_at, updated_at, result_message,
  dry_run, allow_delete, prune_reason
 FROM (
  SELECT DISTINCT ON (machine_id, cluster_node_name, runtime_id, COALESCE(payload_json->>'dry_run', ''), COALESCE(payload_json->>'allow_delete', ''))
   id, machine_id, cluster_node_name, runtime_id, created_at, updated_at, result_message,
   payload_json->>'dry_run' AS dry_run, payload_json->>'allow_delete' AS allow_delete, payload_json->>'prune_reason' AS prune_reason
  FROM fugue_node_update_tasks WHERE status = $1 AND task_type = $2
  ORDER BY machine_id, cluster_node_name, runtime_id, COALESCE(payload_json->>'dry_run', ''), COALESCE(payload_json->>'allow_delete', ''), updated_at DESC, created_at DESC, id DESC
 ) latest ORDER BY created_at DESC, id DESC`, model.NodeUpdateTaskStatusCompleted, model.NodeUpdateTaskTypePruneImageCache)
	if err != nil {
		return nil, mapDBErr(err)
	}
	defer rows.Close()
	var tasks []model.NodeUpdateTask
	for rows.Next() {
		var t model.NodeUpdateTask
		var dryRun, allowDelete, pruneReason *string
		if err := rows.Scan(&t.ID, &t.MachineID, &t.ClusterNodeName, &t.RuntimeID, &t.CreatedAt, &t.UpdatedAt, &t.ResultMessage, &dryRun, &allowDelete, &pruneReason); err != nil {
			return nil, mapDBErr(err)
		}
		t.Type = model.NodeUpdateTaskTypePruneImageCache
		t.Status = model.NodeUpdateTaskStatusCompleted
		t.Payload = map[string]string{}
		for k, v := range map[string]*string{"dry_run": dryRun, "allow_delete": allowDelete, "prune_reason": pruneReason} {
			if v != nil {
				t.Payload[k] = *v
			}
		}
		tasks = append(tasks, t)
	}
	return tasks, mapDBErr(rows.Err())
}
