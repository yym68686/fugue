package api

import (
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestMoveAppProjectMovesOwnedBackingServiceAtomically(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Project Move API Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	sourceProject, err := s.CreateProject(tenant.ID, "default", "")
	if err != nil {
		t.Fatalf("create source project: %v", err)
	}
	if _, err := s.UpdateTenantBilling(tenant.ID, model.BillingResourceSpec{
		CPUMilliCores:    1000,
		MemoryMebibytes:  2048,
		StorageGibibytes: 2,
	}); err != nil {
		t.Fatalf("raise billing cap: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, sourceProject.ID, "dataocean", "", model.AppSpec{
		Image:     "ghcr.io/example/dataocean:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Postgres: &model.AppPostgresSpec{
			Database: "dataocean",
			User:     "dataocean",
			Password: "secret",
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	app, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app: %v", err)
	}
	ownedServiceID := app.BackingServices[0].ID

	_, apiKey, err := s.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.write", "project.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/move-project", apiKey, map[string]any{
		"target_project_name":    "dataocean",
		"create_project":         true,
		"include_owned_services": true,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "\"plan\"") {
		t.Fatalf("expected plan response, got %s", recorder.Body.String())
	}

	movedApp, err := s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get moved app: %v", err)
	}
	if movedApp.ProjectID == sourceProject.ID {
		t.Fatalf("expected app to move out of source project, got %s", movedApp.ProjectID)
	}
	if len(movedApp.BackingServices) != 1 || movedApp.BackingServices[0].ID != ownedServiceID {
		t.Fatalf("expected owned service to follow app, got %+v", movedApp.BackingServices)
	}
	if movedApp.BackingServices[0].ProjectID != movedApp.ProjectID {
		t.Fatalf("expected owned service project %s, got %s", movedApp.ProjectID, movedApp.BackingServices[0].ProjectID)
	}
	if len(movedApp.Bindings) != 1 || movedApp.Bindings[0].ServiceID != ownedServiceID {
		t.Fatalf("expected binding to keep service attachment, got %+v", movedApp.Bindings)
	}
	if _, err := s.GetProject(movedApp.ProjectID); err != nil {
		t.Fatalf("expected target project to exist: %v", err)
	}
}
