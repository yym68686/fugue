package store

import "fugue/internal/model"

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
