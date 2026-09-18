package controller

import (
	"fugue/internal/model"
	"fugue/internal/store"
	"path/filepath"
	"testing"
)

func TestPendingDeleteCancelsStuckDeployBeforeClaim(t *testing.T) {
	for _, running := range []bool{false, true} {
		s := store.New(filepath.Join(t.TempDir(), "state.json"))
		if err := s.Init(); err != nil {
			t.Fatal(err)
		}
		tenant, err := s.CreateTenant("example")
		if err != nil {
			t.Fatal(err)
		}
		project, err := s.CreateProject(tenant.ID, "example", "")
		if err != nil {
			t.Fatal(err)
		}
		app, err := s.CreateApp(tenant.ID, project.ID, "example", "", model.AppSpec{RuntimeID: "runtime_managed_shared", Image: "example/image:latest", Replicas: 1, Ports: []int{8080}})
		if err != nil {
			t.Fatal(err)
		}
		deploy, err := s.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &app.Spec})
		if err != nil {
			t.Fatal(err)
		}
		if running {
			if _, ok, err := s.TryClaimPendingOperation(deploy.ID); err != nil || !ok {
				t.Fatalf("claim %v %v", ok, err)
			}
		}
		deletion, err := s.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeDelete})
		if err != nil {
			t.Fatal(err)
		}
		active, err := s.ListActiveOperations()
		if err != nil {
			t.Fatal(err)
		}
		svc := &Service{Store: s}
		if changed, err := svc.cancelDeploysSupersededByDeletion(active); err != nil || !changed {
			t.Fatalf("cancel changed=%v err=%v", changed, err)
		}
		op, err := s.GetOperation(deploy.ID)
		if err != nil || op.Status != model.OperationStatusCanceled {
			t.Fatalf("deploy still active: %+v %v", op, err)
		}
		if _, ok, err := s.TryClaimPendingOperation(deletion.ID); err != nil || !ok {
			t.Fatalf("delete remains blocked: %v %v", ok, err)
		}
	}
}
