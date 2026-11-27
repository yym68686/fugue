package store

import (
	"context"
	"database/sql"
	"errors"

	"fugue/internal/model"
)

// Only an incident hold may be released through this method. Other lifecycle
// states must be preserved even when a web request is retried.
func (s *Store) SetTenantAccessHold(ctx context.Context, id string, held bool) error {
	status := "active"
	if held {
		status = model.TenantStatusAccessHeld
	}
	var result string
	err := s.db.QueryRowContext(ctx, `UPDATE fugue_tenants SET status=$2,
			updated_at=CASE WHEN status=$2 THEN updated_at ELSE NOW() END
			WHERE id=$1 AND status IN ('active',$3) RETURNING id`, id, status, model.TenantStatusAccessHeld).Scan(&result)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	return err
}
