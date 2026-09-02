package store

import (
	"errors"
	"testing"

	"fugue/internal/model"
)

func TestCancelOperationOnlyCancelsPendingOperations(t *testing.T) {
	s := New(t.TempDir() + "/store.json")
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Cancel Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	runtimeObj, _, err := s.CreateRuntime(tenant.ID, "runtime", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{Image: "example/demo:latest", Replicas: 1, RuntimeID: runtimeObj.ID})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	op, err := s.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeMigrate, TargetRuntimeID: runtimeObj.ID})
	if err != nil {
		t.Fatalf("create operation: %v", err)
	}
	canceled, err := s.CancelOperation(op.ID, "operator canceled before execution")
	if err != nil {
		t.Fatalf("cancel pending operation: %v", err)
	}
	if canceled.Status != model.OperationStatusCanceled || canceled.CompletedAt == nil {
		t.Fatalf("unexpected canceled operation: %+v", canceled)
	}
	if _, err := s.CancelOperation(op.ID, "retry"); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected terminal cancel retry conflict, got %v", err)
	}

	running, err := s.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeMigrate, TargetRuntimeID: runtimeObj.ID})
	if err != nil {
		t.Fatalf("create second operation: %v", err)
	}
	if _, _, err := s.TryClaimPendingOperation(running.ID); err != nil {
		t.Fatalf("claim second operation: %v", err)
	}
	if _, err := s.CancelOperation(running.ID, "too late"); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected running cancel conflict, got %v", err)
	}
}
