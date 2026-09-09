package store

import (
	"context"
	"time"

	"fugue/internal/model"
)

// ListAppWorkloadIdentitiesWithTiming is for attributing pods to apps only.
// Replica counts and phase are retained to apply the existing tombstone filter;
// source, routes, executable configuration and service bindings are not read.
func (s *Store) ListAppWorkloadIdentitiesWithTiming(tenantID string, platformAdmin bool) ([]model.App, AppListReadTiming, error) {
	var timing AppListReadTiming
	if !s.usingDatabase() {
		apps, err := s.ListAppsMetadata(tenantID, platformAdmin)
		return apps, timing, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	query := `SELECT id, tenant_id, project_id, name, ''::text, NULL::jsonb, NULL::jsonb,
 jsonb_build_object('runtime_id',spec_json->'runtime_id','replicas',spec_json->'replicas'),
 jsonb_build_object('phase',status_json->'phase','current_runtime_id',status_json->'current_runtime_id','current_replicas',status_json->'current_replicas'),
 created_at, updated_at FROM fugue_apps WHERE ` + appVisiblePhasePredicate
	var args []any
	if !platformAdmin {
		query += ` AND tenant_id=$1`
		args = append(args, tenantID)
	}
	query += ` ORDER BY created_at ASC`
	apps, err := s.pgReadAppRows(ctx, query, args, false, &timing)
	return apps, timing, err
}
