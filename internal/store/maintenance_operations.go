package store

import (
	"context"
	"fmt"
	"time"

	"fugue/internal/model"
)

// ListImageRetentionOperations preserves every operation's lifecycle and image
// inputs, including active operations without a source. Inline files, secrets
// and rollback specs are deliberately excluded from maintenance snapshots.
// Do not limit this list: an older operation can still protect an image.
func (s *Store) ListImageRetentionOperations(tenantID string, platformAdmin bool) ([]model.Operation, error) {
	if !s.usingDatabase() {
		var result []model.Operation
		err := s.withLockedState(false, func(state *model.State) error {
			for _, op := range state.Operations {
				if !platformAdmin && op.TenantID != tenantID {
					continue
				}
				if op.DesiredSpec != nil {
					op.DesiredSpec = &model.AppSpec{Image: op.DesiredSpec.Image}
				}
				op.ConfigBaseSpec = nil
				result = append(result, op)
			}
			sortOperationsOldestFirst(result)
			return nil
		})
		return result, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// The transactionally maintained candidate projection excludes config_base_spec.
	// Left join keeps source-less active operations for retention's in-flight guard.
	query := `SELECT o.id, o.tenant_id, o.type, o.status, o.app_id,
	 CASE WHEN c.operation_id IS NOT NULL THEN c.image ELSE o.desired_spec_json->>'image' END,
	 CASE WHEN c.operation_id IS NOT NULL THEN c.source
	      WHEN jsonb_typeof(o.desired_source_json) = 'object' THEN o.desired_source_json - 'config_base_spec'
	      ELSE o.desired_source_json END,
	 o.created_at, o.updated_at, o.started_at, o.completed_at
 FROM fugue_operations o LEFT JOIN fugue_image_candidate_operations c ON c.operation_id = o.id`
	var args []any
	if !platformAdmin {
		query += ` WHERE o.tenant_id = $1`
		args = append(args, tenantID)
	}
	query += ` ORDER BY o.created_at ASC, o.id ASC`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list image retention operations: %w", err)
	}
	defer rows.Close()
	var result []model.Operation
	for rows.Next() {
		op, err := scanImageOperation(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, op)
	}
	return result, rows.Err()
}
