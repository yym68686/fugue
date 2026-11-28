package store

import (
	"context"
	"time"

	"fugue/internal/model"
)

// ListImageCachePrunePlanMetrics returns the image-cache metric projection.
func (s *Store) ListImageCachePrunePlanMetrics(parent context.Context, limit int) ([]model.ImageCachePrunePlan, error) {
	out, err := s.imageCache().ListImageCachePrunePlanMetrics(parent, limit)
	return out, mapDBErr(err)
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
