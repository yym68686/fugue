package store

import (
	"errors"
	"testing"

	"fugue/internal/model"
	"fugue/internal/operationcancel"
)

func cancelOperationForTest(s *Store, op model.Operation, reason string) (model.Operation, error) {
	return s.OperationCancellation(model.Principal{TenantID: op.TenantID, Scopes: map[string]struct{}{"app.write": {}}}).Cancel(op.ID, reason)
}

func TestCancelOperationOnlyCancelsPendingOperationsOrRunningDeploys(t *testing.T) {
	s := newStoreTestDatabase(t, "")
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
	for _, actor := range []model.Principal{
		{TenantID: tenant.ID},
		{TenantID: "other-tenant", Scopes: map[string]struct{}{"app.write": {}}},
		{TenantID: tenant.ID, ProjectID: "other-project", Scopes: map[string]struct{}{"project.write": {}}},
	} {
		if _, err := s.OperationCancellation(actor).Cancel(op.ID, "not authorized"); !errors.Is(err, operationcancel.ErrNotVisible) {
			t.Fatalf("expected scope rejection, got %v", err)
		}
	}
	if wrote, err := s.OperationQueue().CancelSupersededDeploy(op.ID, "not a deploy"); err != nil || wrote {
		t.Fatalf("queue cancellation touched a migration: %v %v", wrote, err)
	}
	current, err := s.GetOperation(op.ID)
	if err != nil || current.Status != model.OperationStatusPending {
		t.Fatalf("unauthorized call changed the operation: %v %+v", err, current)
	}
	owner := model.Principal{TenantID: tenant.ID, ProjectID: project.ID, Scopes: map[string]struct{}{"project.write": {}}}
	canceled, err := s.OperationCancellation(owner).Cancel(op.ID, "operator canceled before execution")
	if err != nil {
		t.Fatalf("cancel pending operation: %v", err)
	}
	if canceled.Status != model.OperationStatusCanceled || canceled.CompletedAt == nil {
		t.Fatalf("unexpected canceled operation: %+v", canceled)
	}
	if _, err := cancelOperationForTest(s, op, "retry"); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected terminal cancel retry conflict, got %v", err)
	}

	running, err := s.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeDeploy, TargetRuntimeID: runtimeObj.ID, DesiredSpec: &app.Spec})
	if err != nil {
		t.Fatalf("create second operation: %v", err)
	}
	if _, _, err := s.TryClaimPendingOperation(running.ID); err != nil {
		t.Fatalf("claim second operation: %v", err)
	}
	if canceledRunning, err := cancelOperationForTest(s, running, "deploy superseded"); err != nil || canceledRunning.Status != model.OperationStatusCanceled {
		t.Fatalf("expected running deploy cancellation, got op=%+v err=%v", canceledRunning, err)
	}
}
