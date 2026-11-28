package store

import (
	"testing"

	"fugue/internal/model"
)

func TestQueueTerminalFailureCannotOverwriteClaimOrCancellation(t *testing.T) {
	s := newStoreTestDatabase(t, "")
	tenant, err := s.CreateTenant("fixture")
	if err != nil {
		t.Fatal(err)
	}
	project, err := s.CreateProject(tenant.ID, "fixture", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "target", "", model.AppSpec{Image: "registry.example/workload:stable", Replicas: 1, RuntimeID: model.DefaultManagedRuntimeID})
	if err != nil {
		t.Fatal(err)
	}
	op, err := s.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &app.Spec})
	if err != nil {
		t.Fatal(err)
	}
	op, err = s.GetOperation(op.ID)
	if err != nil {
		t.Fatal(err)
	}
	queue := s.OperationQueue()
	if _, ok, err := queue.TryClaimPendingOperation(op.ID); err != nil || !ok {
		t.Fatalf("claim=%v err=%v", ok, err)
	}
	expectStatus := func(want string) {
		t.Helper()
		if _, wrote, err := queue.FailBeforeClaim(op, "observed missing dependency"); err != nil || wrote {
			t.Fatalf("stale snapshot wrote: %v %v", wrote, err)
		}
		current, err := s.GetOperation(op.ID)
		if err != nil || current.Status != want {
			t.Fatalf("status=%q, want=%q err=%v", current.Status, want, err)
		}
	}
	expectStatus(model.OperationStatusRunning)
	if _, err := cancelOperationForTest(s, op, "user cancelled"); err != nil {
		t.Fatal(err)
	}
	expectStatus(model.OperationStatusCanceled)

	next, err := s.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &app.Spec})
	if err != nil {
		t.Fatal(err)
	}
	next, err = s.GetOperation(next.ID)
	if err != nil {
		t.Fatal(err)
	}
	if failed, wrote, err := queue.FailBeforeClaim(next, "missing dependency"); err != nil || !wrote || failed.Status != model.OperationStatusFailed {
		t.Fatalf("pending snapshot was not failed: %s %v %v", failed.Status, wrote, err)
	}
}
