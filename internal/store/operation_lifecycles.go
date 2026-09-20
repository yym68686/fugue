package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"fugue/internal/model"
)

// ListActiveOperationLifecycles reads the status and timestamps needed for
// recovery and stuck-operation checks. It must not hydrate deployment payloads
// or scan terminal history to determine which operations are in flight.
func (s *Store) ListActiveOperationLifecycles() ([]model.Operation, error) {
	return s.listOperationLifecycles(true)
}

// ListOperationLifecycles retains every operation identity, status and timestamp
// for distributed image retention. There is deliberately no history limit:
// even an old operation can protect an image or determine its retention order.
// These summaries cannot be used to execute an operation or restore its config.
func (s *Store) ListOperationLifecycles() ([]model.Operation, error) {
	return s.listOperationLifecycles(false)
}

func operationLifecycle(op model.Operation) model.Operation {
	return model.Operation{
		ID: op.ID, AppID: op.AppID, Type: op.Type, Status: op.Status,
		CreatedAt: op.CreatedAt, StartedAt: op.StartedAt, CompletedAt: op.CompletedAt,
	}
}

func (s *Store) listOperationLifecycles(activeOnly bool) ([]model.Operation, error) {
	if !s.usingDatabase() {
		var ops []model.Operation
		err := s.withLockedState(false, func(state *model.State) error {
			for _, op := range state.Operations {
				if activeOnly && !isActiveOperationStatus(op.Status) {
					continue
				}
				ops = append(ops, operationLifecycle(op))
			}
			sortActiveOperations(ops)
			return nil
		})
		return ops, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	query := `SELECT id, app_id, type, status, created_at, started_at, completed_at
FROM fugue_operations`
	var args []any
	if activeOnly {
		// Match the existing status index and the execution scheduler's exact
		// active-state predicate; do not apply a function to the indexed column.
		query += "\nWHERE status IN ($1, $2, $3)"
		args = []any{model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent}
	}
	query += "\nORDER BY created_at ASC, id ASC"
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list operation lifecycles: %w", err)
	}
	defer rows.Close()
	var ops []model.Operation
	for rows.Next() {
		var op model.Operation
		var started, completed sql.NullTime
		if err := rows.Scan(&op.ID, &op.AppID, &op.Type, &op.Status, &op.CreatedAt, &started, &completed); err != nil {
			return nil, fmt.Errorf("scan operation lifecycle: %w", err)
		}
		if started.Valid {
			op.StartedAt = &started.Time
		}
		if completed.Valid {
			op.CompletedAt = &completed.Time
		}
		ops = append(ops, op)
	}
	if err := rows.Err(); err != nil {
		// Never return a partial inventory: missing identities would weaken
		// active-operation protection and image-retention ordering.
		return nil, fmt.Errorf("iterate operation lifecycles after %d rows: %w", len(ops), err)
	}
	return ops, nil
}
