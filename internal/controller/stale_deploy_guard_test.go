package controller

import (
	"context"
	"io"
	"log"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func TestExecuteManagedDeployOperationSkipsStaleSnapshotAfterNewerDeploy(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Stale Deploy")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
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
		RepoBranch:       "main",
		BuildStrategy:    model.AppBuildStrategyDockerfile,
		CommitSHA:        "oldcommit",
		ResolvedImageRef: pushBase + "/fugue-apps/demo:git-old",
	}
	oldSpec := model.AppSpec{
		Image:     pullBase + "/fugue-apps/demo:git-old",
		Ports:     []int{8080},
		Replicas:  2,
		RuntimeID: "runtime_managed_shared",
	}
	app, err := stateStore.CreateImportedAppWithoutRoute(tenant.ID, project.ID, "demo", "", oldSpec, oldSource)
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	newSource := oldSource
	newSource.CommitSHA = "newcommit"
	newSource.ResolvedImageRef = pushBase + "/fugue-apps/demo:git-new"
	newSpec := oldSpec
	newSpec.Image = pullBase + "/fugue-apps/demo:git-new"
	newDeploy, err := stateStore.CreateOperation(model.Operation{
		TenantID:            tenant.ID,
		Type:                model.OperationTypeDeploy,
		RequestedByType:     model.ActorTypeBootstrap,
		RequestedByID:       model.OperationRequestedByGitHubSyncController,
		AppID:               app.ID,
		DesiredSpec:         &newSpec,
		DesiredSource:       &newSource,
		DesiredOriginSource: &newSource,
	})
	if err != nil {
		t.Fatalf("create newer deploy operation: %v", err)
	}
	if _, found, err := stateStore.TryClaimPendingOperation(newDeploy.ID); err != nil {
		t.Fatalf("claim newer deploy operation: %v", err)
	} else if !found {
		t.Fatal("expected newer deploy operation to be claimable")
	}

	staleSource := oldSource
	staleSpec := oldSpec
	staleDeploy, err := stateStore.CreateOperation(model.Operation{
		TenantID:            tenant.ID,
		Type:                model.OperationTypeDeploy,
		RequestedByType:     model.ActorTypeBootstrap,
		RequestedByID:       "bootstrap-admin",
		AppID:               app.ID,
		DesiredSpec:         &staleSpec,
		DesiredSource:       &staleSource,
		DesiredOriginSource: &staleSource,
	})
	if err != nil {
		t.Fatalf("create stale deploy operation: %v", err)
	}
	if _, err := stateStore.CompleteManagedOperation(newDeploy.ID, "/tmp/new.yaml", "new deployed"); err != nil {
		t.Fatalf("complete newer deploy operation: %v", err)
	}

	claimedStale, found, err := stateStore.TryClaimPendingOperation(staleDeploy.ID)
	if err != nil {
		t.Fatalf("claim stale deploy operation: %v", err)
	}
	if !found {
		t.Fatal("expected stale deploy operation to be claimable")
	}

	svc := &Service{
		Store:            stateStore,
		Renderer:         runtime.Renderer{BaseDir: t.TempDir()},
		Logger:           log.New(io.Discard, "", 0),
		registryPushBase: pushBase,
		registryPullBase: pullBase,
		inspectManagedImage: func(_ context.Context, imageRef string) (bool, map[string]int64, error) {
			t.Fatalf("stale deploy should not inspect image %q", imageRef)
			return false, nil, nil
		},
	}
	if err := svc.executeManagedOperation(context.Background(), claimedStale); err != nil {
		t.Fatalf("execute stale deploy operation: %v", err)
	}

	gotApp, err := stateStore.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app: %v", err)
	}
	if gotApp.Spec.Image != newSpec.Image {
		t.Fatalf("expected app image to remain %q, got %q", newSpec.Image, gotApp.Spec.Image)
	}
	if build := model.AppBuildSource(gotApp); build == nil || build.CommitSHA != newSource.CommitSHA {
		t.Fatalf("expected build source commit %q, got %+v", newSource.CommitSHA, build)
	}
	if origin := model.AppOriginSource(gotApp); origin == nil || origin.CommitSHA != newSource.CommitSHA {
		t.Fatalf("expected origin source commit %q, got %+v", newSource.CommitSHA, origin)
	}

	gotStale, err := stateStore.GetOperation(staleDeploy.ID)
	if err != nil {
		t.Fatalf("get stale operation: %v", err)
	}
	if gotStale.Status != model.OperationStatusCompleted {
		t.Fatalf("expected stale operation completed, got %q", gotStale.Status)
	}
	if !strings.Contains(gotStale.ResultMessage, "skipped") {
		t.Fatalf("expected stale operation skip message, got %q", gotStale.ResultMessage)
	}
}
