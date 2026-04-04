package controller

import (
	"context"
	"io"
	"log"
	"path/filepath"
	"reflect"
	"testing"

	"fugue/internal/appimages"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func TestExecuteManagedDeleteOperationDeletesOnlyExclusiveManagedImages(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Delete Images Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "gallery", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	const (
		pushBase = "registry.push.example"
		pullBase = "registry.pull.example"
	)

	oldSource := model.AppSource{
		Type:             model.AppSourceTypeGitHubPublic,
		RepoURL:          "https://github.com/example/demo",
		ResolvedImageRef: pushBase + "/fugue-apps/example-demo:git-old",
	}
	oldSpec := model.AppSpec{
		Image:     pullBase + "/fugue-apps/example-demo:git-old",
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}
	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", oldSpec, oldSource, model.AppRoute{
		Hostname:    "demo.apps.example.com",
		BaseDomain:  "apps.example.com",
		PublicURL:   "https://demo.apps.example.com",
		ServicePort: 80,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}

	oldDeployOp, err := stateStore.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeDeploy,
		AppID:         app.ID,
		DesiredSpec:   &oldSpec,
		DesiredSource: &oldSource,
	})
	if err != nil {
		t.Fatalf("create old deploy operation: %v", err)
	}
	if _, err := stateStore.CompleteManagedOperationWithResult(oldDeployOp.ID, "/tmp/old.yaml", "old deployed", &oldSpec, &oldSource); err != nil {
		t.Fatalf("complete old deploy operation: %v", err)
	}

	currentSource := oldSource
	currentSource.ResolvedImageRef = pushBase + "/fugue-apps/example-demo:git-current"
	currentSpec := oldSpec
	currentSpec.Image = pullBase + "/fugue-apps/example-demo:git-current"
	currentDeployOp, err := stateStore.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeDeploy,
		AppID:         app.ID,
		DesiredSpec:   &currentSpec,
		DesiredSource: &currentSource,
	})
	if err != nil {
		t.Fatalf("create current deploy operation: %v", err)
	}
	if _, err := stateStore.CompleteManagedOperationWithResult(currentDeployOp.ID, "/tmp/current.yaml", "current deployed", &currentSpec, &currentSource); err != nil {
		t.Fatalf("complete current deploy operation: %v", err)
	}

	sharedAppSpec := currentSpec
	sharedAppSource := currentSource
	if _, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "prod", "", sharedAppSpec, sharedAppSource, model.AppRoute{
		Hostname:    "prod.apps.example.com",
		BaseDomain:  "apps.example.com",
		PublicURL:   "https://prod.apps.example.com",
		ServicePort: 80,
	}); err != nil {
		t.Fatalf("create shared app: %v", err)
	}

	app, err = stateStore.GetApp(app.ID)
	if err != nil {
		t.Fatalf("reload app before delete: %v", err)
	}

	deleteOp, err := stateStore.CreateOperation(model.Operation{
		TenantID: tenant.ID,
		Type:     model.OperationTypeDelete,
		AppID:    app.ID,
	})
	if err != nil {
		t.Fatalf("create delete operation: %v", err)
	}
	claimed, found, err := stateStore.ClaimNextPendingOperation()
	if err != nil {
		t.Fatalf("claim delete operation: %v", err)
	}
	if !found {
		t.Fatal("expected delete operation to be claimable")
	}
	if claimed.ID != deleteOp.ID {
		t.Fatalf("expected claimed delete operation %s, got %s", deleteOp.ID, claimed.ID)
	}

	var deletedRefs []string
	svc := &Service{
		Store:            stateStore,
		Renderer:         runtime.Renderer{BaseDir: t.TempDir()},
		Logger:           log.New(io.Discard, "", 0),
		registryPushBase: pushBase,
		registryPullBase: pullBase,
		deleteManagedImage: func(_ context.Context, imageRef string) (appimages.DeleteResult, error) {
			deletedRefs = append(deletedRefs, imageRef)
			return appimages.DeleteResult{ImageRef: imageRef, Deleted: true}, nil
		},
	}

	if err := svc.executeManagedOperation(context.Background(), claimed); err != nil {
		t.Fatalf("execute managed delete operation: %v", err)
	}

	wantDeletedRefs := []string{
		pushBase + "/fugue-apps/example-demo:git-old",
	}
	if !reflect.DeepEqual(deletedRefs, wantDeletedRefs) {
		t.Fatalf("expected deleted refs %v, got %v", wantDeletedRefs, deletedRefs)
	}
}
