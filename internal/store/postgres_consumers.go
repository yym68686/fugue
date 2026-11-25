package store

import (
	"context"
	"sort"

	"fugue/internal/model"
)

// Resolve network grants from binding identity, independent of secret-bearing app specs.
func (s *Store) PostgresConsumerIDs(ctx context.Context, tenantID, serviceID string) ([]string, error) {
	ids := []string{}
	if s.usingDatabase() {
		rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT b.app_id FROM fugue_service_bindings b
			JOIN fugue_apps a ON a.id=b.app_id AND a.tenant_id=b.tenant_id
			WHERE b.tenant_id=$1 AND b.service_id=$2 ORDER BY b.app_id`, tenantID, serviceID)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return nil, err
			}
			ids = append(ids, id)
		}
		return ids, rows.Err()
	}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, b := range state.ServiceBindings {
			if b.ServiceID == serviceID && b.TenantID == tenantID {
				ids = append(ids, b.AppID)
			}
		}
		sort.Strings(ids)
		return nil
	})
	return ids, err
}
