package controller

import (
	"bytes"
	"context"
	"io"
	"log"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/store"
)

func TestHandleClaimedOperationFailsDeployWhenManagedImageIsMissingFromRegistry(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "registry.pull.example/fugue-apps/demo:git-old",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:             model.AppSourceTypeGitHubPublic,
		RepoURL:          "https://github.com/example/demo",
		RepoBranch:       "main",
		BuildStrategy:    model.AppBuildStrategyDockerfile,
		CommitSHA:        "oldcommit",
		ResolvedImageRef: "registry.push.example/fugue-apps/demo:git-old",
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	desiredSpec := app.Spec
	desiredSpec.Image = "registry.pull.example/fugue-apps/demo:git-newcommit"
	desiredSource := *app.Source
	desiredSource.CommitSHA = "newcommit"
	desiredSource.ResolvedImageRef = "registry.push.example/fugue-apps/demo:git-newcommit"
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeDeploy,
		AppID:         app.ID,
		DesiredSpec:   &desiredSpec,
		DesiredSource: &desiredSource,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}
	claimed, found, err := stateStore.ClaimNextPendingOperation()
	if err != nil {
		t.Fatalf("claim deploy operation: %v", err)
	}
	if !found {
		t.Fatal("expected deploy operation to be claimable")
	}

	svc := &Service{
		Store:            stateStore,
		Logger:           log.New(io.Discard, "", 0),
		registryPushBase: "registry.push.example",
		registryPullBase: "registry.pull.example",
		inspectManagedImage: func(_ context.Context, imageRef string) (bool, map[string]int64, error) {
			if imageRef != desiredSource.ResolvedImageRef {
				t.Fatalf("unexpected managed image ref %q", imageRef)
			}
			return false, nil, nil
		},
	}

	if err := svc.handleClaimedOperation(context.Background(), claimed); err != nil {
		t.Fatalf("handle claimed operation: %v", err)
	}

	failedOp, err := stateStore.GetOperation(op.ID)
	if err != nil {
		t.Fatalf("get failed deploy operation: %v", err)
	}
	if failedOp.Status != model.OperationStatusFailed {
		t.Fatalf("expected failed deploy status, got %q", failedOp.Status)
	}
	if !strings.Contains(failedOp.ErrorMessage, "queued image rebuild") {
		t.Fatalf("expected queued rebuild message, got %q", failedOp.ErrorMessage)
	}
	ops, err := stateStore.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("list app operations: %v", err)
	}
	foundRebuild := false
	for _, candidate := range ops {
		if candidate.Type == model.OperationTypeImport && candidate.RequestedByID == model.OperationRequestedByImageRebuild {
			foundRebuild = true
			break
		}
	}
	if !foundRebuild {
		t.Fatal("expected missing image to queue an import rebuild")
	}
}

func TestHandleClaimedOperationFailsDeployWhenRuntimeImageIsMissingFromRegistry(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "registry.pull.example/fugue-apps/demo:git-old",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:             model.AppSourceTypeGitHubPublic,
		RepoURL:          "https://github.com/example/demo",
		RepoBranch:       "main",
		BuildStrategy:    model.AppBuildStrategyDockerfile,
		CommitSHA:        "oldcommit",
		ResolvedImageRef: "registry.push.example/fugue-apps/demo:git-old",
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	desiredSpec := app.Spec
	desiredSpec.Image = "ghcr.io/example/runtime:missing"
	desiredSource := *app.Source
	desiredSource.CommitSHA = "newcommit"
	desiredSource.ResolvedImageRef = "registry.push.example/fugue-apps/demo:git-newcommit"
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeDeploy,
		AppID:         app.ID,
		DesiredSpec:   &desiredSpec,
		DesiredSource: &desiredSource,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}
	claimed, found, err := stateStore.ClaimNextPendingOperation()
	if err != nil {
		t.Fatalf("claim deploy operation: %v", err)
	}
	if !found {
		t.Fatal("expected deploy operation to be claimable")
	}

	svc := &Service{
		Store:            stateStore,
		Logger:           log.New(io.Discard, "", 0),
		registryPushBase: "registry.push.example",
		registryPullBase: "registry.pull.example",
		inspectManagedImage: func(_ context.Context, imageRef string) (bool, map[string]int64, error) {
			switch imageRef {
			case desiredSource.ResolvedImageRef:
				return true, nil, nil
			case desiredSpec.Image:
				return false, nil, nil
			default:
				t.Fatalf("unexpected image ref %q", imageRef)
				return false, nil, nil
			}
		},
	}

	if err := svc.handleClaimedOperation(context.Background(), claimed); err != nil {
		t.Fatalf("handle claimed operation: %v", err)
	}

	failedOp, err := stateStore.GetOperation(op.ID)
	if err != nil {
		t.Fatalf("get failed deploy operation: %v", err)
	}
	if failedOp.Status != model.OperationStatusFailed {
		t.Fatalf("expected failed deploy status, got %q", failedOp.Status)
	}
	if !strings.Contains(failedOp.ErrorMessage, "runtime image") {
		t.Fatalf("expected runtime image error, got %q", failedOp.ErrorMessage)
	}
}

func TestHandleClaimedOperationFailsMigrateWhenManagedImageIsMissingFromRegistry(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := stateStore.CreateTenant("Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "web", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	targetRuntime, _, err := stateStore.CreateRuntime(tenant.ID, "target", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create target runtime: %v", err)
	}

	app, err := stateStore.CreateImportedApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "registry.pull.example/fugue-apps/demo:git-old",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:             model.AppSourceTypeGitHubPublic,
		RepoURL:          "https://github.com/example/demo",
		RepoBranch:       "main",
		BuildStrategy:    model.AppBuildStrategyDockerfile,
		CommitSHA:        "oldcommit",
		ResolvedImageRef: "registry.push.example/fugue-apps/demo:git-old",
	}, model.AppRoute{})
	if err != nil {
		t.Fatalf("create imported app: %v", err)
	}

	desiredSpec := app.Spec
	desiredSpec.RuntimeID = targetRuntime.ID
	desiredSource := *app.Source
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:        tenant.ID,
		Type:            model.OperationTypeMigrate,
		AppID:           app.ID,
		TargetRuntimeID: targetRuntime.ID,
		DesiredSpec:     &desiredSpec,
		DesiredSource:   &desiredSource,
	})
	if err != nil {
		t.Fatalf("create migrate operation: %v", err)
	}
	claimed, found, err := stateStore.ClaimNextPendingOperation()
	if err != nil {
		t.Fatalf("claim migrate operation: %v", err)
	}
	if !found {
		t.Fatal("expected migrate operation to be claimable")
	}

	svc := &Service{
		Store:            stateStore,
		Logger:           log.New(io.Discard, "", 0),
		registryPushBase: "registry.push.example",
		registryPullBase: "registry.pull.example",
		inspectManagedImage: func(_ context.Context, imageRef string) (bool, map[string]int64, error) {
			if imageRef != desiredSource.ResolvedImageRef {
				t.Fatalf("unexpected managed image ref %q", imageRef)
			}
			return false, nil, nil
		},
	}

	if err := svc.handleClaimedOperation(context.Background(), claimed); err != nil {
		t.Fatalf("handle claimed operation: %v", err)
	}

	failedOp, err := stateStore.GetOperation(op.ID)
	if err != nil {
		t.Fatalf("get failed migrate operation: %v", err)
	}
	if failedOp.Status != model.OperationStatusFailed {
		t.Fatalf("expected failed migrate status, got %q", failedOp.Status)
	}
	if !strings.Contains(failedOp.ErrorMessage, "queued image rebuild") {
		t.Fatalf("expected queued rebuild message, got %q", failedOp.ErrorMessage)
	}
	ops, err := stateStore.ListOperationsByApp(tenant.ID, false, app.ID)
	if err != nil {
		t.Fatalf("list app operations: %v", err)
	}
	foundRebuild := false
	for _, candidate := range ops {
		if candidate.Type == model.OperationTypeImport && candidate.RequestedByID == model.OperationRequestedByImageRebuild {
			foundRebuild = true
			break
		}
	}
	if !foundRebuild {
		t.Fatal("expected missing image to queue an import rebuild")
	}
	gotApp, err := stateStore.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app: %v", err)
	}
	if gotApp.Spec.RuntimeID != app.Spec.RuntimeID {
		t.Fatalf("expected app runtime to remain %q, got %q", app.Spec.RuntimeID, gotApp.Spec.RuntimeID)
	}
}

func TestEnsureManagedDeployImageReadyInspectsPullBaseRuntimeViaPushBase(t *testing.T) {
	t.Parallel()

	managedRef := "registry.push.example/fugue-apps/demo:git-newcommit"
	runtimePullRef := "registry.pull.example/fugue-apps/runtime@sha256:abc123"
	runtimePushRef := "registry.push.example/fugue-apps/runtime@sha256:abc123"
	app := model.App{
		Spec: model.AppSpec{
			Image:    runtimePullRef,
			Replicas: 1,
		},
		Source: &model.AppSource{
			ResolvedImageRef: managedRef,
		},
	}

	inspected := make([]string, 0, 2)
	svc := &Service{
		registryPushBase: "registry.push.example",
		registryPullBase: "registry.pull.example",
		inspectManagedImage: func(_ context.Context, imageRef string) (bool, map[string]int64, error) {
			inspected = append(inspected, imageRef)
			switch imageRef {
			case managedRef, runtimePushRef:
				return true, nil, nil
			case runtimePullRef:
				t.Fatalf("controller should not inspect node-only registry pull ref %q", imageRef)
			default:
				t.Fatalf("unexpected image ref %q", imageRef)
			}
			return false, nil, nil
		},
	}

	if err := svc.ensureManagedDeployImageReady(context.Background(), app); err != nil {
		t.Fatalf("ensure deploy image ready: %v", err)
	}
	if len(inspected) != 2 || inspected[0] != managedRef || inspected[1] != runtimePushRef {
		t.Fatalf("expected inspect refs [%q %q], got %v", managedRef, runtimePushRef, inspected)
	}
}

func TestEnsureManagedDeployImageReadyDoesNotInspectPullBaseAliasAfterPushRefMiss(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	managedRef := "registry.push.example/fugue-apps/demo:git-newcommit"
	runtimePullRef := "registry.pull.example/fugue-apps/runtime@sha256:abc123"
	runtimePushRef := "registry.push.example/fugue-apps/runtime@sha256:abc123"
	app := model.App{
		ID:       "app_1",
		TenantID: "tenant_1",
		Spec: model.AppSpec{
			Image:     runtimePullRef,
			Replicas:  1,
			RuntimeID: "runtime_1",
		},
		Source: &model.AppSource{
			ResolvedImageRef: managedRef,
		},
	}
	if _, err := stateStore.UpsertImageLocation(model.ImageLocation{
		TenantID:  app.TenantID,
		AppID:     app.ID,
		ImageRef:  runtimePullRef,
		RuntimeID: app.Spec.RuntimeID,
		Status:    model.ImageLocationStatusPresent,
	}); err != nil {
		t.Fatalf("record image location: %v", err)
	}

	inspected := make([]string, 0, 2)
	svc := &Service{
		Store:                         stateStore,
		registryPushBase:              "registry.push.example",
		registryPullBase:              "registry.pull.example",
		importImageInspectMaxAttempts: 1,
		inspectManagedImage: func(_ context.Context, imageRef string) (bool, map[string]int64, error) {
			inspected = append(inspected, imageRef)
			switch imageRef {
			case managedRef:
				return true, nil, nil
			case runtimePushRef:
				return false, nil, nil
			case runtimePullRef:
				t.Fatalf("controller should not inspect node-only registry pull ref %q", imageRef)
			default:
				t.Fatalf("unexpected image ref %q", imageRef)
			}
			return false, nil, nil
		},
	}

	if err := svc.ensureManagedDeployImageReady(context.Background(), app); err != nil {
		t.Fatalf("ensure deploy image ready: %v", err)
	}
	if len(inspected) != 2 || inspected[0] != managedRef || inspected[1] != runtimePushRef {
		t.Fatalf("expected inspect refs [%q %q], got %v", managedRef, runtimePushRef, inspected)
	}
}

func TestScheduleImageHydrationSkipsLegacyUpdaterWithoutWarning(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Image Hydration Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, nodeSecret, err := stateStore.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	updater, _, err := stateStore.EnrollNodeUpdater(
		nodeSecret,
		"worker-1",
		"https://worker-1.example.com",
		nil,
		"worker-1",
		"machine-1",
		"v1",
		"join-v1",
		nil,
	)
	if err != nil {
		t.Fatalf("enroll node updater: %v", err)
	}

	var logs bytes.Buffer
	svc := &Service{
		Store:  stateStore,
		Logger: log.New(&logs, "", 0),
	}
	app := model.App{ID: "app_1", TenantID: tenant.ID}
	target := deployImageTarget{RuntimeID: updater.RuntimeID, ClusterNodeName: updater.ClusterNodeName}

	svc.scheduleImageHydration(context.Background(), app, target, "registry.example/app@sha256:abc")

	tasks, err := stateStore.ListNodeUpdateTasks(tenant.ID, false, updater.ID, "")
	if err != nil {
		t.Fatalf("list node update tasks: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("expected no hydrate task for legacy updater, got %+v", tasks)
	}
	if logs.Len() != 0 {
		t.Fatalf("expected no warning log for unsupported optional hydrate task, got %q", logs.String())
	}
}

func TestScheduleImageHydrationCreatesTaskForCapableUpdater(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Image Hydration Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, nodeSecret, err := stateStore.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	updater, _, err := stateStore.EnrollNodeUpdater(
		nodeSecret,
		"worker-1",
		"https://worker-1.example.com",
		nil,
		"worker-1",
		"machine-1",
		"v2",
		"join-v2",
		[]string{"heartbeat", "tasks", model.NodeUpdateTaskTypePrepullAppImages},
	)
	if err != nil {
		t.Fatalf("enroll node updater: %v", err)
	}

	svc := &Service{
		Store:  stateStore,
		Logger: log.New(io.Discard, "", 0),
	}
	app := model.App{ID: "app_1", TenantID: tenant.ID}
	imageRef := "registry.example/app@sha256:abc"

	svc.scheduleImageHydration(context.Background(), app, deployImageTarget{RuntimeID: updater.RuntimeID, ClusterNodeName: updater.ClusterNodeName}, imageRef)

	tasks, err := stateStore.ListNodeUpdateTasks(tenant.ID, false, updater.ID, "")
	if err != nil {
		t.Fatalf("list node update tasks: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected one hydrate task, got %+v", tasks)
	}
	if tasks[0].Type != model.NodeUpdateTaskTypePrepullAppImages || tasks[0].Payload["images"] != imageRef {
		t.Fatalf("unexpected hydrate task: %+v", tasks[0])
	}
}
