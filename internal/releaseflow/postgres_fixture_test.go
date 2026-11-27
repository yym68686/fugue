package releaseflow

import (
	"testing"

	"fugue/internal/model"
	"fugue/internal/store"
	"fugue/internal/testpostgres"
)

func newStoreTestDatabase(t testing.TB, fixture string) *store.Store {
	t.Helper()
	s := store.New(testpostgres.Database(t, fixture))
	t.Cleanup(func() { _ = s.Close() })
	s.ConfigureDataBlobDir(t.TempDir())
	if err := s.Init(); err != nil {
		t.Fatal("init isolated PostgreSQL store: ", err)
	}
	return s
}

func seedReleaseflowApp(t testing.TB, s *store.Store, app model.App) model.App {
	t.Helper()
	tenant, err := s.CreateTenant("release-flow")
	if err != nil {
		t.Fatal(err)
	}
	project, err := s.CreateProject(tenant.ID, "release-flow", "", "")
	if err != nil {
		t.Fatal(err)
	}
	spec := app.Spec
	spec.RuntimeID = model.DefaultManagedRuntimeID
	if spec.Replicas == 0 {
		spec.Replicas = 1
	}
	saved, err := s.CreateApp(tenant.ID, project.ID, app.Name, "", spec)
	if err != nil {
		t.Fatal(err)
	}
	app.ID, app.TenantID, app.ProjectID = saved.ID, saved.TenantID, saved.ProjectID
	return app
}
