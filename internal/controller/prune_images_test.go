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

func TestExecuteManagedDeployOperationPrunesManagedImageHistoryBeyondLimit(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Prune Images Tenant")
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
		Image:            pullBase + "/fugue-apps/example-demo:git-old",
		ImageMirrorLimit: 1,
		Ports:            []int{80},
		Replicas:         1,
		RuntimeID:        "runtime_managed_shared",
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
	claimed, found, err := stateStore.ClaimNextPendingOperation()
	if err != nil {
		t.Fatalf("claim current deploy operation: %v", err)
	}
	if !found {
		t.Fatal("expected current deploy operation to be claimable")
	}
	if claimed.ID != currentDeployOp.ID {
		t.Fatalf("expected claimed deploy operation %s, got %s", currentDeployOp.ID, claimed.ID)
	}

	existingRefs := map[string]bool{
		pushBase + "/fugue-apps/example-demo:git-old":     true,
		pushBase + "/fugue-apps/example-demo:git-current": true,
	}
	var deletedRefs []string
	svc := &Service{
		Store:            stateStore,
		Renderer:         runtime.Renderer{BaseDir: t.TempDir()},
		Logger:           log.New(io.Discard, "", 0),
		registryPushBase: pushBase,
		registryPullBase: pullBase,
		inspectManagedImage: func(_ context.Context, imageRef string) (bool, map[string]int64, error) {
			return existingRefs[imageRef], nil, nil
		},
		deleteManagedImage: func(_ context.Context, imageRef string) (appimages.DeleteResult, error) {
			deletedRefs = append(deletedRefs, imageRef)
			return appimages.DeleteResult{ImageRef: imageRef, Deleted: true}, nil
		},
	}

	if err := svc.executeManagedOperation(context.Background(), claimed); err != nil {
		t.Fatalf("execute managed deploy operation: %v", err)
	}

	wantDeletedRefs := []string{
		pushBase + "/fugue-apps/example-demo:git-old",
	}
	if !reflect.DeepEqual(deletedRefs, wantDeletedRefs) {
		t.Fatalf("expected deleted refs %v, got %v", wantDeletedRefs, deletedRefs)
	}
}
