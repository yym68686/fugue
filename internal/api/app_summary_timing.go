package api

import (
	"context"

	"fugue/internal/model"
	"fugue/internal/store"
)

func (s *Server) listAppSummariesWithTiming(ctx context.Context, tenantID string, platformAdmin, hydrateServices bool) ([]model.App, error) {
	apps, stages, err := s.store.ListAppSummariesWithTiming(tenantID, platformAdmin, hydrateServices)
	recordAppReadTiming(ctx, stages)
	return apps, err
}

func (s *Server) listAppWorkloadIdentitiesWithTiming(ctx context.Context, tenantID string, platformAdmin bool) ([]model.App, error) {
	apps, stages, err := s.store.ListAppWorkloadIdentitiesWithTiming(tenantID, platformAdmin)
	recordAppReadTiming(ctx, stages)
	return apps, err
}

func recordAppReadTiming(ctx context.Context, stages store.AppListReadTiming) {
	timing := serverTimingFromContext(ctx)
	timing.Add("apps_acquire", stages.Acquire)
	timing.Add("apps_query", stages.Query)
	timing.Add("apps_rows", stages.Rows)
	timing.Add("apps_decode", stages.Decode)
	timing.Add("apps_services", stages.Services)
}
