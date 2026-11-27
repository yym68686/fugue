package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

type ManagedPostgresAutoMemoryBackoff struct {
	Blocked   bool
	Downscale bool
}

// ManagedPostgresAutoMemoryBackoffs returns the resize history decisions for
// these services without decoding operation specifications or source payloads.
// Pending/running resizes of any age and by any actor still block both directions.
func (s *Store) ManagedPostgresAutoMemoryBackoffs(ctx context.Context, tenantID, appID string, serviceIDs []string, now time.Time) (map[string]ManagedPostgresAutoMemoryBackoff, error) {
	tenantID, appID = strings.TrimSpace(tenantID), strings.TrimSpace(appID)
	if tenantID == "" || appID == "" {
		return nil, ErrInvalidInput
	}
	out := make(map[string]ManagedPostgresAutoMemoryBackoff)
	if len(serviceIDs) == 0 {
		return out, nil
	}
	prefix := ManagedPostgresAutoMemoryRequestActorPrefix + "/"
	failedAfter, completedAfter := now.Add(-6*time.Hour), now.Add(-time.Hour)
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `
SELECT service_id,
       bool_or(status IN ($5, $6, $7)),
       bool_or(status = $8)
FROM fugue_operations
WHERE tenant_id = $1 AND app_id = $2 AND service_id = ANY($3::text[]) AND type = $4
  AND (status IN ($5, $6) OR
       (requested_by_type = $9 AND starts_with(requested_by_id, $10)
        AND ((status = $7 AND updated_at > $11) OR (status = $8 AND updated_at > $12))))
GROUP BY service_id`, tenantID, appID, serviceIDs, model.OperationTypeDatabaseResize,
		model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusFailed, model.OperationStatusCompleted,
		model.ActorTypeSystem, prefix, failedAfter, completedAfter)
	if err != nil {
		return nil, fmt.Errorf("read PostgreSQL memory resize backoff: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var backoff ManagedPostgresAutoMemoryBackoff
		if err := rows.Scan(&id, &backoff.Blocked, &backoff.Downscale); err != nil {
			return nil, err
		}
		out[id] = backoff
	}
	return out, rows.Err()
}
