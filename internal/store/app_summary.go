package store

import (
	"time"

	"fugue/internal/model"
)

// AppListReadTiming measures disjoint client-side read stages. Query includes
// connection-pool acquisition and the first result; Rows includes driver reads
// of the remaining results. Decode includes scan, normalization and filtering.
type AppListReadTiming struct {
	Query, Rows, Decode, Services time.Duration
}

// AppReadSummary excludes executable configuration payloads from read views.
// It preserves identities, resource settings, status, sources and bindings.
func AppReadSummary(app model.App) model.App {
	app.Spec.Env = nil
	app.Spec.GeneratedEnv = nil
	app.Spec.Files = nil
	app.Spec.Command = nil
	app.Spec.Args = nil
	return app
}

func (s *Store) ListAppSummaries(tenantID string, platformAdmin bool, hydrateServices bool) ([]model.App, error) {
	if s.usingDatabase() {
		return s.pgListAppsView(tenantID, platformAdmin, hydrateServices, true)
	}
	apps, err := s.listAppsView(tenantID, platformAdmin, hydrateServices)
	if err != nil {
		return nil, err
	}
	for i := range apps {
		apps[i] = AppReadSummary(apps[i])
	}
	return apps, nil
}

func (s *Store) ListAppSummariesWithTiming(tenantID string, platformAdmin, hydrateServices bool) ([]model.App, AppListReadTiming, error) {
	var timing AppListReadTiming
	if s.usingDatabase() {
		apps, err := s.pgListAppsViewWithTiming(tenantID, platformAdmin, hydrateServices, true, &timing)
		return apps, timing, err
	}
	apps, err := s.ListAppSummaries(tenantID, platformAdmin, hydrateServices)
	return apps, timing, err
}
