package store

import (
	"testing"
	"time"

	"fugue/internal/model"
)

func TestHydrateAppBackingServicesWithIndexPreservesBindingsAndDedupesServices(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	app := model.App{ID: "app_one"}
	otherApp := model.App{ID: "app_two"}
	firstService := model.BackingService{
		ID:        "svc_one",
		TenantID:  "tenant_one",
		Name:      "first",
		Status:    model.BackingServiceStatusActive,
		CreatedAt: now.Add(time.Second),
	}
	secondService := model.BackingService{
		ID:        "svc_two",
		TenantID:  "tenant_one",
		Name:      "second",
		Status:    model.BackingServiceStatusActive,
		CreatedAt: now,
	}
	deletedService := model.BackingService{
		ID:        "svc_deleted",
		TenantID:  "tenant_one",
		Name:      "deleted",
		Status:    model.BackingServiceStatusDeleted,
		CreatedAt: now.Add(2 * time.Second),
	}

	state := &model.State{
		BackingServices: []model.BackingService{firstService, secondService, deletedService},
		ServiceBindings: []model.ServiceBinding{
			{ID: "binding_late", AppID: app.ID, ServiceID: firstService.ID, CreatedAt: now.Add(2 * time.Second)},
			{ID: "binding_early", AppID: app.ID, ServiceID: secondService.ID, CreatedAt: now},
			{ID: "binding_duplicate", AppID: app.ID, ServiceID: firstService.ID, CreatedAt: now.Add(3 * time.Second)},
			{ID: "binding_deleted", AppID: app.ID, ServiceID: deletedService.ID, CreatedAt: now.Add(4 * time.Second)},
			{ID: "binding_other_app", AppID: otherApp.ID, ServiceID: firstService.ID, CreatedAt: now},
		},
	}

	index := newAppBackingServiceIndex(state)
	hydrateAppBackingServicesWithIndex(index, &app)
	hydrateAppBackingServicesWithIndex(index, &otherApp)

	if len(app.Bindings) != 3 {
		t.Fatalf("expected duplicate live bindings to be preserved and deleted service skipped, got %+v", app.Bindings)
	}
	if app.Bindings[0].ID != "binding_early" || app.Bindings[1].ID != "binding_late" || app.Bindings[2].ID != "binding_duplicate" {
		t.Fatalf("expected bindings sorted by creation time, got %+v", app.Bindings)
	}
	if len(app.BackingServices) != 2 {
		t.Fatalf("expected duplicate service to be deduped, got %+v", app.BackingServices)
	}
	if app.BackingServices[0].ID != secondService.ID || app.BackingServices[1].ID != firstService.ID {
		t.Fatalf("expected services sorted by creation time, got %+v", app.BackingServices)
	}
	if len(otherApp.Bindings) != 1 || otherApp.Bindings[0].ID != "binding_other_app" {
		t.Fatalf("expected independent hydration for other app, got %+v", otherApp.Bindings)
	}
}
