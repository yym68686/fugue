package controller

import (
	"context"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
	"io"
	"log"
	"path/filepath"
	"testing"
)

func TestQueuedBuildExecutionPreservesScaleAfterEnqueue(t *testing.T) {
	st := store.New(filepath.Join(t.TempDir(), "state.json"))
	if e := st.Init(); e != nil {
		t.Fatal(e)
	}
	tenant, _ := st.CreateTenant("queue merge")
	project, _ := st.CreateProject(tenant.ID, "demo", "")
	app, e := st.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{Image: "example/app:old", RuntimeID: model.DefaultManagedRuntimeID, Ports: []int{8080}, Replicas: 1})
	if e != nil {
		t.Fatal(e)
	}
	parent, e := st.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeImport, DesiredSpec: &app.Spec, DesiredSource: &model.AppSource{Type: model.AppSourceTypeDockerImage, ImageRef: "example/app:new"}})
	if e != nil {
		t.Fatal(e)
	}
	built := app.Spec
	built.Image = "example/app:new"
	deploy, e := st.CreateDeployOperationAfterImport(parent.ID, model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &built, DesiredSource: parent.DesiredSource})
	if e != nil {
		t.Fatal(e)
	}
	n := 2
	scale, e := st.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeScale, DesiredReplicas: &n})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = st.CompleteManagedOperation(scale.ID, "", "scaled"); e != nil {
		t.Fatal(e)
	}
	op, ok, e := st.TryClaimPendingOperation(deploy.ID)
	if e != nil || !ok {
		t.Fatal("claim", e)
	}
	svc := &Service{Store: st, Renderer: runtime.Renderer{BaseDir: t.TempDir()}, Logger: log.New(io.Discard, "", 0)}
	if e = svc.executeManagedOperation(context.Background(), op); e != nil {
		t.Fatal(e)
	}
	result, e := st.GetApp(app.ID)
	if e != nil {
		t.Fatal(e)
	}
	if result.Spec.Replicas != 2 || result.Spec.Image != built.Image {
		t.Fatalf("activation lost latest intent: replicas=%d image=%s", result.Spec.Replicas, result.Spec.Image)
	}
}
