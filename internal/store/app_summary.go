package store

import (
	"time"

	"fugue/internal/model"
)

// AppListReadTiming measures disjoint client-side read stages. Acquire includes
// establishing a connection when necessary; Query excludes that acquisition.
type AppListReadTiming struct {
	Acquire                       time.Duration
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
	return s.pgListAppsView(tenantID, platformAdmin, hydrateServices, true)
}

func (s *Store) ListAppSummariesWithTiming(tenantID string, platformAdmin, hydrateServices bool) ([]model.App, AppListReadTiming, error) {
	var timing AppListReadTiming
	apps, err := s.pgListAppsViewWithTiming(tenantID, platformAdmin, hydrateServices, true, &timing)
	return apps, timing, err
}
