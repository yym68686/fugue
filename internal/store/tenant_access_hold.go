package store

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"fugue/internal/model"
)

// Only an incident hold may be released through this method. Other lifecycle
// states must be preserved even when a web request is retried.
func (s *Store) SetTenantAccessHold(ctx context.Context, id string, held bool) error {
	status := "active"
	if held {
		status = model.TenantStatusAccessHeld
	}
	if s.usingDatabase() {
		var result string
		err := s.db.QueryRowContext(ctx, `UPDATE fugue_tenants SET status=$2,
			updated_at=CASE WHEN status=$2 THEN updated_at ELSE NOW() END
			WHERE id=$1 AND status IN ('active',$3) RETURNING id`, id, status, model.TenantStatusAccessHeld).Scan(&result)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	return s.withLockedState(true, func(state *model.State) error {
		i := findTenant(state, id)
		if i < 0 {
			return ErrNotFound
		}
		t := &state.Tenants[i]
		if t.Status != "active" && t.Status != model.TenantStatusAccessHeld {
			return ErrConflict
		}
		if t.Status != status {
			t.Status = status
			t.UpdatedAt = time.Now().UTC()
		}
		return nil
	})
}

func tenantAccessHeld(state *model.State, id string) bool {
	i := findTenant(state, id)
	return i >= 0 && state.Tenants[i].Status == model.TenantStatusAccessHeld
}
