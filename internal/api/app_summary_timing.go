package api

import (
	"context"

	"fugue/internal/model"
)

func (s *Server) listAppSummariesWithTiming(ctx context.Context, tenantID string, platformAdmin, hydrateServices bool) ([]model.App, error) {
	apps, stages, err := s.store.ListAppSummariesWithTiming(tenantID, platformAdmin, hydrateServices)
	timing := serverTimingFromContext(ctx)
	timing.Add("apps_acquire", stages.Acquire)
	timing.Add("apps_query", stages.Query)
	timing.Add("apps_rows", stages.Rows)
	timing.Add("apps_decode", stages.Decode)
	timing.Add("apps_services", stages.Services)
	return apps, err
}
