package controller

import (
	"context"
	"io"
	"log"
	"os"
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

func TestExecuteManagedDeployOperationSkipsNoopDesiredStateWithoutRender(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Noop Deploy")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	source := model.AppSource{
		Type:             model.AppSourceTypeGitHubPublic,
		RepoURL:          "https://github.com/example/demo",
		RepoBranch:       "main",
		BuildStrategy:    model.AppBuildStrategyDockerfile,
		CommitSHA:        "currentcommit",
		ResolvedImageRef: "registry.push.example/fugue-apps/demo:git-current",
	}
	spec := model.AppSpec{
		Image:     "registry.pull.example/fugue-apps/demo:git-current",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}
	app, err := stateStore.CreateImportedAppWithoutRoute(tenant.ID, project.ID, "demo", "", spec, source)
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:            tenant.ID,
		Type:                model.OperationTypeDeploy,
		RequestedByType:     model.ActorTypeBootstrap,
		RequestedByID:       "test-noop",
		AppID:               app.ID,
		DesiredSpec:         &spec,
		DesiredSource:       &source,
		DesiredOriginSource: &source,
	})
	if err != nil {
		t.Fatalf("create no-op deploy operation: %v", err)
	}
	claimed, found, err := stateStore.TryClaimPendingOperation(op.ID)
	if err != nil {
		t.Fatalf("claim no-op deploy operation: %v", err)
	}
	if !found {
		t.Fatal("expected no-op deploy operation to be claimable")
	}

	renderFile := filepath.Join(t.TempDir(), "render-base-file")
	if err := os.WriteFile(renderFile, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("create render sentinel file: %v", err)
	}
	svc := &Service{
		Store:    stateStore,
		Renderer: runtime.Renderer{BaseDir: renderFile},
		Logger:   log.New(io.Discard, "", 0),
		inspectManagedImage: func(_ context.Context, imageRef string) (bool, map[string]int64, error) {
			t.Fatalf("no-op deploy should not inspect image %q", imageRef)
			return false, nil, nil
		},
	}
	if err := svc.executeManagedOperation(context.Background(), claimed); err != nil {
		t.Fatalf("execute no-op deploy operation: %v", err)
	}
	completed, err := stateStore.GetOperation(op.ID)
	if err != nil {
		t.Fatalf("get completed operation: %v", err)
	}
	if completed.Status != model.OperationStatusCompleted {
		t.Fatalf("expected no-op operation completed, got %q", completed.Status)
	}
	if !strings.Contains(completed.ResultMessage, "desired state is already current") {
		t.Fatalf("expected no-op skip message, got %q", completed.ResultMessage)
	}
	gotApp, err := stateStore.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app: %v", err)
	}
	if gotApp.Spec.Image != spec.Image || gotApp.Spec.Replicas != spec.Replicas {
		t.Fatalf("expected app spec to stay current, got %+v", gotApp.Spec)
	}
}

func TestDeployOperationDesiredStateDoesNotMatchRestartOnlyChange(t *testing.T) {
	t.Parallel()

	current := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:        "ghcr.io/example/demo:latest",
			Ports:        []int{8080},
			Replicas:     1,
			RuntimeID:    "runtime_demo",
			RestartToken: "restart-old",
		},
	}
	desired := current.Spec
	desired.RestartToken = "restart-new"
	op := model.Operation{Type: model.OperationTypeDeploy, DesiredSpec: &desired}
	if deployOperationDesiredStateMatchesApp(op, current) {
		t.Fatal("restart-only deploy must not be treated as no-op")
	}
}

func TestRightSizingDowntimeFailSafeRefusesOnlySystemRightSizing(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:       "app_demo",
		TenantID: "tenant_demo",
		Name:     "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:latest",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
			PersistentStorage: &model.AppPersistentStorageSpec{
				Mode: model.AppPersistentStorageModeMovableRWO,
				Mounts: []model.AppPersistentStorageMount{
					{Kind: model.AppPersistentStorageMountKindFile, Path: "/home/api.yaml"},
				},
			},
		},
	}
	svc := &Service{
		Renderer: runtime.Renderer{BaseDir: t.TempDir()},
		Logger:   log.New(io.Discard, "", 0),
	}
	rightSizingOp := model.Operation{
		ID:              "op_right_sizing",
		TenantID:        app.TenantID,
		Type:            model.OperationTypeDeploy,
		RequestedByType: model.ActorTypeSystem,
		RequestedByID:   model.OperationRequestedByRightSizingDownscale,
		AppID:           app.ID,
		DesiredSpec:     &app.Spec,
	}
	if err := svc.refuseRightSizingDowntimeIfNeeded(context.Background(), rightSizingOp, app, runtime.SchedulingConstraints{}, nil); err == nil {
		t.Fatal("expected right-sizing fail-safe to refuse downtime-required deployment")
	}
	manualOp := rightSizingOp
	manualOp.ID = "op_manual"
	manualOp.RequestedByType = model.ActorTypeAPIKey
	manualOp.RequestedByID = "user-key"
	if err := svc.refuseRightSizingDowntimeIfNeeded(context.Background(), manualOp, app, runtime.SchedulingConstraints{}, nil); err != nil {
		t.Fatalf("manual downtime-required deploy must not be refused by right-sizing fail-safe: %v", err)
	}
}
