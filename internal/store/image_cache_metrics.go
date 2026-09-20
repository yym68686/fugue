package store

import (
	"context"
	"sort"
	"strings"
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
	if !s.usingDatabase() {
		plans, err := s.ListImageCachePrunePlans(model.ImageCachePrunePlanFilter{Limit: limit})
		if err != nil {
			return nil, err
		}
		for i := range plans {
			plans[i] = imageCachePrunePlanMetrics(plans[i])
		}
		return plans, nil
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

func imageCachePrunePlanMetrics(p model.ImageCachePrunePlan) model.ImageCachePrunePlan {
	protection := make(map[string]int, len(p.ProtectionSummary))
	for k, v := range p.ProtectionSummary {
		protection[k] = v
	}
	return model.ImageCachePrunePlan{ID: p.ID, NodeID: p.NodeID, ClusterNodeName: p.ClusterNodeName, RuntimeID: p.RuntimeID, Mode: p.Mode, Status: p.Status, CreatedAt: p.CreatedAt,
		CandidateManifestCount: p.CandidateManifestCount, CandidateBlobCount: p.CandidateBlobCount, CandidateBlobBytes: p.CandidateBlobBytes, PlannedDeleteBytes: p.PlannedDeleteBytes, ProtectedManifestCount: p.ProtectedManifestCount, ProtectionSummary: protection}
}

// ListImageCachePruneTaskMetrics excludes unrelated maintenance history and
// task logs. It intentionally keeps all completed prune tasks: the caller
// selects the latest per label set before checking its prune_reason.
func (s *Store) ListImageCachePruneTaskMetrics(parent context.Context) ([]model.NodeUpdateTask, error) {
	if !s.usingDatabase() {
		var tasks []model.NodeUpdateTask
		err := s.withLockedState(false, func(state *model.State) error {
			for _, t := range state.NodeUpdateTasks {
				if strings.TrimSpace(t.Status) == model.NodeUpdateTaskStatusCompleted && t.Type == model.NodeUpdateTaskTypePruneImageCache {
					tasks = append(tasks, imageCachePruneTaskMetrics(t))
				}
			}
			return nil
		})
		sort.Slice(tasks, func(i, j int) bool { return tasks[i].CreatedAt.After(tasks[j].CreatedAt) })
		return tasks, err
	}
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `SELECT id, machine_id, cluster_node_name, runtime_id, created_at, updated_at, result_message,
  payload_json->>'dry_run', payload_json->>'allow_delete', payload_json->>'prune_reason'
 FROM fugue_node_update_tasks WHERE status = $1 AND task_type = $2 ORDER BY created_at DESC`, model.NodeUpdateTaskStatusCompleted, model.NodeUpdateTaskTypePruneImageCache)
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

func imageCachePruneTaskMetrics(t model.NodeUpdateTask) model.NodeUpdateTask {
	payload := map[string]string{}
	for _, k := range []string{"dry_run", "allow_delete", "prune_reason"} {
		if v, ok := t.Payload[k]; ok {
			payload[k] = v
		}
	}
	return model.NodeUpdateTask{ID: t.ID, MachineID: t.MachineID, ClusterNodeName: t.ClusterNodeName, RuntimeID: t.RuntimeID, CreatedAt: t.CreatedAt, UpdatedAt: t.UpdatedAt, ResultMessage: t.ResultMessage, Type: t.Type, Status: t.Status, Payload: payload}
}
