package store

import (
	"errors"
	"fugue/internal/model"
	"strings"
	"testing"
)

func TestReconcileSpecPreconditionAndActiveOperationAreAtomic(t *testing.T) {
	s := New(t.TempDir() + "/store.json")
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, _ := s.CreateTenant("Reconcile Tenant")
	project, _ := s.CreateProject(tenant.ID, "apps", "")
	runtime, _, _ := s.CreateRuntime(tenant.ID, "runtime", model.RuntimeTypeManagedOwned, "", nil)
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{Image: "example/demo:latest", Replicas: 1, RuntimeID: runtime.ID})
	if err != nil {
		t.Fatal(err)
	}
	spec := app.Spec
	spec.RestartToken = "restart-test"
	op := model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &spec}
	if _, err := s.CreateOperationForAppSpec(op, strings.Repeat("0", 64)); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale plan accepted: %v", err)
	}
	created, err := s.CreateOperationForAppSpec(op, model.AppSpecSHA256(app.Spec))
	if err != nil {
		t.Fatal(err)
	}
	if created.ID == "" {
		t.Fatal("missing operation")
	}
	current, err := s.GetApp(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateOperationForAppSpec(op, model.AppSpecSHA256(current.Spec)); !errors.Is(err, ErrConflict) {
		t.Fatalf("active operation was not protected: %v", err)
	}
}
