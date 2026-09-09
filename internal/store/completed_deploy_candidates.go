package store

import (
	"context"
	"fmt"
	"sort"
	"time"

	"fugue/internal/model"
)

const CompletedDeployCandidatePageSize = 32

type CompletedDeployCursor struct {
	CompletedAt time.Time
	ID          string
}

// ListCompletedDeployCandidates reads only post-creation completed deployments.
// Keyset pagination bounds each response without discarding older matching
// candidates. ConfigBaseSpec and unrelated operation metadata are not needed by
// the guard and are deliberately omitted from the projection.
func (s *Store) ListCompletedDeployCandidates(ctx context.Context, tenantID, appID string, after time.Time, cursor *CompletedDeployCursor) ([]model.Operation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if tenantID == "" || appID == "" || after.IsZero() {
		return nil, ErrInvalidInput
	}
	if !s.usingDatabase() {
		var ops []model.Operation
		err := s.withLockedState(false, func(state *model.State) error {
			for _, op := range state.Operations {
				if op.TenantID != tenantID || op.AppID != appID || op.Type != model.OperationTypeDeploy || op.Status != model.OperationStatusCompleted || op.CompletedAt == nil || !op.CompletedAt.After(after) {
					continue
				}
				if cursor != nil && (op.CompletedAt.After(cursor.CompletedAt) || (op.CompletedAt.Equal(cursor.CompletedAt) && op.ID >= cursor.ID)) {
					continue
				}
				ops = append(ops, op)
			}
			sort.Slice(ops, func(i, j int) bool {
				if ops[i].CompletedAt.Equal(*ops[j].CompletedAt) {
					return ops[i].ID > ops[j].ID
				}
				return ops[i].CompletedAt.After(*ops[j].CompletedAt)
			})
			if len(ops) > CompletedDeployCandidatePageSize {
				ops = ops[:CompletedDeployCandidatePageSize]
			}
			return ctx.Err()
		})
		return ops, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	query := `SELECT id, desired_spec_json, desired_source_json - 'config_base_spec', completed_at
FROM fugue_operations
WHERE tenant_id = $1 AND app_id = $2 AND type = 'deploy' AND status = 'completed'
  AND completed_at > $3`
	args := []any{tenantID, appID, after}
	if cursor != nil {
		query += ` AND (completed_at, id) < ($4, $5)`
		args = append(args, cursor.CompletedAt, cursor.ID)
	}
	query += fmt.Sprintf(" ORDER BY completed_at DESC, id DESC LIMIT %d", CompletedDeployCandidatePageSize)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query completed deploy candidates: %w", err)
	}
	defer rows.Close()
	ops := make([]model.Operation, 0, CompletedDeployCandidatePageSize)
	for rows.Next() {
		op := model.Operation{TenantID: tenantID, AppID: appID, Type: model.OperationTypeDeploy, Status: model.OperationStatusCompleted}
		var specRaw, sourceRaw []byte
		var completed time.Time
		if err := rows.Scan(&op.ID, &specRaw, &sourceRaw, &completed); err != nil {
			return nil, err
		}
		op.DesiredSpec, err = decodeJSONPointer[model.AppSpec](specRaw)
		if err != nil {
			return nil, err
		}
		model.ApplyAppSpecDefaults(op.DesiredSpec)
		op.DesiredSource, op.DesiredOriginSource, err = decodeOperationSourceState(sourceRaw)
		if err != nil {
			return nil, err
		}
		op.CompletedAt = &completed
		ops = append(ops, op)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read completed deploy candidates: %w", err)
	}
	return ops, nil
}
