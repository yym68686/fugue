package controller

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
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

func TestDeployImageRefAvailableUsesLocationEvidenceWhenNodeLocalBuilderRegistryEnabled(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	app := model.App{
		ID:       "app_1",
		TenantID: "tenant_1",
		Spec: model.AppSpec{
			Image:    "registry.pull.example/fugue-apps/demo:git-abc123",
			Replicas: 1,
		},
	}
	managedRef := "registry.push.example/fugue-apps/demo:git-abc123"
	if _, err := stateStore.UpsertImageLocation(model.ImageLocation{
		TenantID: app.TenantID,
		AppID:    app.ID,
		ImageRef: managedRef,
		Status:   model.ImageLocationStatusPresent,
	}); err != nil {
		t.Fatalf("record image location: %v", err)
	}

	svc := &Service{
		Store:                         stateStore,
		registryPushBase:              "registry.push.example",
		registryPullBase:              "registry.pull.example",
		builderRegistryPushBase:       "127.0.0.1:5000",
		importImageInspectMaxAttempts: 1,
		inspectManagedImage: func(_ context.Context, imageRef string) (bool, map[string]int64, error) {
			if imageRef != managedRef {
				t.Fatalf("unexpected image ref %q", imageRef)
			}
			return false, nil, errors.New("central registry unavailable")
		},
	}

	exists, err := svc.deployImageRefAvailable(context.Background(), app, deployImageTarget{}, managedRef)
	if err != nil {
		t.Fatalf("deploy image ref available: %v", err)
	}
	if !exists {
		t.Fatal("expected location evidence to make deploy image available")
	}
}

func TestStrictDistributedDeploySchedulesTargetReplicaInsteadOfRegistryFallback(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Distributed Deploy Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, nodeSecret, err := stateStore.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	if _, _, err := stateStore.EnrollNodeUpdater(nodeSecret, "worker-source", "https://worker-source.example.com", nil, "worker-source", "machine-source", "v2", "join-v2", []string{"heartbeat", "tasks", model.NodeUpdateTaskTypeReplicateAppImage}); err != nil {
		t.Fatalf("enroll source updater: %v", err)
	}
	targetUpdater, _, err := stateStore.EnrollNodeUpdater(nodeSecret, "worker-target", "https://worker-target.example.com", nil, "worker-target", "machine-target", "v2", "join-v2", []string{"heartbeat", "tasks", model.NodeUpdateTaskTypeReplicateAppImage})
	if err != nil {
		t.Fatalf("enroll target updater: %v", err)
	}
	image, err := stateStore.UpsertImage(model.Image{
		TenantID:        tenant.ID,
		AppID:           "app_1",
		ImageRef:        "registry.fugue.internal:5000/fugue-apps/demo:git-abc",
		CanonicalDigest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		LifecycleState:  model.ImageLifecycleAvailable,
	})
	if err != nil {
		t.Fatalf("upsert image: %v", err)
	}
	now := time.Now().UTC()
	if _, err := stateStore.UpsertImageReplica(model.ImageReplica{
		ImageID:         image.ID,
		TenantID:        tenant.ID,
		AppID:           image.AppID,
		NodeID:          "machine-source",
		ClusterNodeName: "worker-source",
		CacheEndpoint:   "http://worker-source.example.com:5000",
		Status:          model.ImageReplicaStatusPresent,
		LastVerifiedAt:  &now,
	}); err != nil {
		t.Fatalf("upsert source replica: %v", err)
	}
	app := model.App{
		ID:       image.AppID,
		TenantID: tenant.ID,
		Spec: model.AppSpec{
			Image:     image.ImageRef,
			Replicas:  1,
			RuntimeID: targetUpdater.RuntimeID,
		},
		Source: &model.AppSource{ResolvedImageRef: image.ImageRef},
	}
	svc := &Service{
		Store:            stateStore,
		Config:           config.ControllerConfig{ImageStoreMode: "distributed", ImageStoreMinReplicas: 2, ImageStoreTargetReplicas: 2},
		registryPushBase: "registry.fugue.internal:5000",
		registryPullBase: "registry.fugue.internal:5000",
		inspectManagedImage: func(context.Context, string) (bool, map[string]int64, error) {
			t.Fatal("strict distributed deploy must not inspect the central registry")
			return false, nil, nil
		},
	}

	available, err := svc.deployImageRefAvailable(context.Background(), app, deployImageTarget{
		RuntimeID:       targetUpdater.RuntimeID,
		ClusterNodeName: targetUpdater.ClusterNodeName,
	}, image.ImageRef)
	if !errors.Is(err, errDeployImageReplicationPending) {
		t.Fatalf("expected pending deploy image replication, got available=%v err=%v", available, err)
	}
	tasks, err := stateStore.ListImageReplicationTasks(model.ImageReplicationTaskFilter{ImageID: image.ID, PlatformAdmin: true, Status: model.ImageReplicationTaskStatusPending})
	if err != nil {
		t.Fatalf("list replication tasks: %v", err)
	}
	if len(tasks) != 1 || tasks[0].Priority != model.ImageReplicationPriorityDeployBlocking || tasks[0].TargetClusterNodeName != targetUpdater.ClusterNodeName {
		t.Fatalf("expected deploy-blocking target replication task, got %+v", tasks)
	}
	nodeTasks, err := stateStore.ListNodeUpdateTasks(tenant.ID, false, targetUpdater.ID, model.NodeUpdateTaskStatusPending)
	if err != nil {
		t.Fatalf("list node update tasks: %v", err)
	}
	if len(nodeTasks) != 1 || nodeTasks[0].Type != model.NodeUpdateTaskTypeReplicateAppImage {
		t.Fatalf("expected replicate-app-image node task, got %+v", nodeTasks)
	}
}

func TestStrictDistributedDeployUsesTargetLocationWhenReplicaLeaseExpired(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Distributed Location Deploy Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, nodeSecret, err := stateStore.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	if _, _, err := stateStore.EnrollNodeUpdater(nodeSecret, "worker-source", "https://worker-source.example.com", nil, "worker-source", "machine-source", "v2", "join-v2", []string{"heartbeat", "tasks", model.NodeUpdateTaskTypeReplicateAppImage}); err != nil {
		t.Fatalf("enroll source updater: %v", err)
	}
	targetUpdater, _, err := stateStore.EnrollNodeUpdater(nodeSecret, "worker-target", "https://worker-target.example.com", nil, "worker-target", "machine-target", "v6", "join-v1", []string{"heartbeat", "tasks", model.NodeUpdateTaskTypePrepullAppImages})
	if err != nil {
		t.Fatalf("enroll target updater: %v", err)
	}
	image, err := stateStore.UpsertImage(model.Image{
		TenantID:        tenant.ID,
		AppID:           "app_1",
		ImageRef:        "registry.fugue.internal:5000/fugue-apps/demo:git-abc",
		CanonicalDigest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		LifecycleState:  model.ImageLifecycleAvailable,
	})
	if err != nil {
		t.Fatalf("upsert image: %v", err)
	}
	now := time.Now().UTC()
	expired := now.Add(-time.Hour)
	if _, err := stateStore.UpsertImageReplica(model.ImageReplica{
		ImageID:         image.ID,
		TenantID:        tenant.ID,
		AppID:           image.AppID,
		NodeID:          "machine-source",
		ClusterNodeName: "worker-source",
		CacheEndpoint:   "http://worker-source.example.com:5000",
		Status:          model.ImageReplicaStatusPresent,
		LastVerifiedAt:  &now,
	}); err != nil {
		t.Fatalf("upsert source replica: %v", err)
	}
	if _, err := stateStore.UpsertImageReplica(model.ImageReplica{
		ImageID:         image.ID,
		TenantID:        tenant.ID,
		AppID:           image.AppID,
		NodeID:          targetUpdater.MachineID,
		RuntimeID:       targetUpdater.RuntimeID,
		ClusterNodeName: targetUpdater.ClusterNodeName,
		CacheEndpoint:   "http://worker-target.example.com:5000",
		Status:          model.ImageReplicaStatusPresent,
		LastVerifiedAt:  &expired,
		LeaseExpiresAt:  &expired,
	}); err != nil {
		t.Fatalf("upsert expired target replica: %v", err)
	}
	if _, err := stateStore.UpsertImageLocation(model.ImageLocation{
		TenantID:        tenant.ID,
		AppID:           image.AppID,
		ImageRef:        image.ImageRef,
		Digest:          image.CanonicalDigest,
		NodeID:          targetUpdater.MachineID,
		RuntimeID:       targetUpdater.RuntimeID,
		ClusterNodeName: targetUpdater.ClusterNodeName,
		CacheEndpoint:   "http://worker-target.example.com:5000",
		Status:          model.ImageLocationStatusPresent,
		LastSeenAt:      &now,
	}); err != nil {
		t.Fatalf("upsert target image location: %v", err)
	}
	app := model.App{
		ID:       image.AppID,
		TenantID: tenant.ID,
		Spec: model.AppSpec{
			Image:     image.ImageRef,
			Replicas:  1,
			RuntimeID: targetUpdater.RuntimeID,
		},
		Source: &model.AppSource{ResolvedImageRef: image.ImageRef},
	}
	svc := &Service{
		Store:            stateStore,
		Config:           config.ControllerConfig{ImageStoreMode: "distributed", ImageStoreMinReplicas: 2, ImageStoreTargetReplicas: 2},
		registryPushBase: "registry.fugue.internal:5000",
		registryPullBase: "registry.fugue.internal:5000",
		inspectManagedImage: func(context.Context, string) (bool, map[string]int64, error) {
			t.Fatal("strict distributed deploy must not inspect the central registry")
			return false, nil, nil
		},
	}

	available, err := svc.deployImageRefAvailable(context.Background(), app, deployImageTarget{
		RuntimeID:       targetUpdater.RuntimeID,
		ClusterNodeName: targetUpdater.ClusterNodeName,
	}, image.ImageRef)
	if err != nil {
		t.Fatalf("deploy image ref available: %v", err)
	}
	if !available {
		t.Fatal("expected target image location evidence to make deploy image available")
	}
	tasks, err := stateStore.ListImageReplicationTasks(model.ImageReplicationTaskFilter{ImageID: image.ID, PlatformAdmin: true, Status: model.ImageReplicationTaskStatusPending})
	if err != nil {
		t.Fatalf("list replication tasks: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("expected no target replication task when location is present, got %+v", tasks)
	}
	nodeTasks, err := stateStore.ListNodeUpdateTasks(tenant.ID, false, targetUpdater.ID, "")
	if err != nil {
		t.Fatalf("list node update tasks: %v", err)
	}
	if len(nodeTasks) != 0 {
		t.Fatalf("expected no node update task when location is present, got %+v", nodeTasks)
	}
}

func TestStrictDistributedDeployPinnedManagedSharedTargetUsesClusterNode(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Pinned Shared Deploy Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, nodeSecret, err := stateStore.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	if _, _, err := stateStore.EnrollNodeUpdater(nodeSecret, "worker-source", "https://worker-source.example.com", nil, "worker-source", "machine-source", "v2", "join-v2", []string{"heartbeat", "tasks", model.NodeUpdateTaskTypeReplicateAppImage}); err != nil {
		t.Fatalf("enroll source updater: %v", err)
	}
	targetUpdater, _, err := stateStore.EnrollNodeUpdater(nodeSecret, "worker-target", "https://worker-target.example.com", nil, "worker-target", "machine-target", "v2", "join-v2", []string{"heartbeat", "tasks", model.NodeUpdateTaskTypeReplicateAppImage})
	if err != nil {
		t.Fatalf("enroll target updater: %v", err)
	}
	image, err := stateStore.UpsertImage(model.Image{
		TenantID:        tenant.ID,
		AppID:           "app_1",
		ImageRef:        "registry.fugue.internal:5000/fugue-apps/demo:git-abc",
		CanonicalDigest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		LifecycleState:  model.ImageLifecycleAvailable,
	})
	if err != nil {
		t.Fatalf("upsert image: %v", err)
	}
	now := time.Now().UTC()
	if _, err := stateStore.UpsertImageReplica(model.ImageReplica{
		ImageID:         image.ID,
		TenantID:        tenant.ID,
		AppID:           image.AppID,
		NodeID:          "machine-source",
		ClusterNodeName: "worker-source",
		CacheEndpoint:   "http://worker-source.example.com:5000",
		Status:          model.ImageReplicaStatusPresent,
		LastVerifiedAt:  &now,
	}); err != nil {
		t.Fatalf("upsert source replica: %v", err)
	}
	app := model.App{
		ID:       image.AppID,
		TenantID: tenant.ID,
		Spec: model.AppSpec{
			Image:     image.ImageRef,
			Replicas:  1,
			RuntimeID: "runtime_managed_shared",
		},
		Source: &model.AppSource{ResolvedImageRef: image.ImageRef},
	}
	svc := &Service{
		Store:            stateStore,
		Config:           config.ControllerConfig{ImageStoreMode: "distributed", ImageStoreMinReplicas: 2, ImageStoreTargetReplicas: 2},
		registryPushBase: "registry.fugue.internal:5000",
		registryPullBase: "registry.fugue.internal:5000",
		inspectManagedImage: func(context.Context, string) (bool, map[string]int64, error) {
			t.Fatal("strict distributed deploy must not inspect the central registry")
			return false, nil, nil
		},
	}

	target := svc.deployImageTarget(app, runtimepkg.SchedulingConstraints{
		NodeSelector: map[string]string{kubeHostnameLabelKey: targetUpdater.ClusterNodeName},
	})
	if target.RuntimeID != "" || target.ClusterNodeName != targetUpdater.ClusterNodeName {
		t.Fatalf("expected pinned deploy target to use only cluster node, got %+v", target)
	}
	available, err := svc.deployImageRefAvailable(context.Background(), app, target, image.ImageRef)
	if !errors.Is(err, errDeployImageReplicationPending) {
		t.Fatalf("expected pending deploy image replication, got available=%v err=%v", available, err)
	}
	tasks, err := stateStore.ListImageReplicationTasks(model.ImageReplicationTaskFilter{ImageID: image.ID, PlatformAdmin: true, Status: model.ImageReplicationTaskStatusPending})
	if err != nil {
		t.Fatalf("list replication tasks: %v", err)
	}
	if len(tasks) != 1 || tasks[0].Priority != model.ImageReplicationPriorityDeployBlocking || tasks[0].TargetClusterNodeName != targetUpdater.ClusterNodeName {
		t.Fatalf("expected deploy-blocking target replication task, got %+v", tasks)
	}
}

func TestStrictDistributedDeployWithoutImageIndexDoesNotInspectRegistry(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Registryless Missing Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	imageRef := "registry.fugue.internal:5000/fugue-apps/missing:git-abc"
	app := model.App{
		ID:       "app_1",
		TenantID: tenant.ID,
		Spec: model.AppSpec{
			Image:    imageRef,
			Replicas: 1,
		},
		Source: &model.AppSource{ResolvedImageRef: imageRef},
	}
	svc := &Service{
		Store:            stateStore,
		Config:           config.ControllerConfig{ImageStoreMode: "distributed"},
		registryPushBase: "registry.fugue.internal:5000",
		registryPullBase: "registry.fugue.internal:5000",
		inspectManagedImage: func(context.Context, string) (bool, map[string]int64, error) {
			t.Fatal("strict distributed deploy must not inspect the central registry when image index is missing")
			return false, nil, nil
		},
	}

	available, err := svc.deployImageRefAvailable(context.Background(), app, deployImageTarget{}, imageRef)
	if err != nil {
		t.Fatalf("deploy image ref available: %v", err)
	}
	if available {
		t.Fatal("expected missing registryless image without index or location evidence to be unavailable")
	}
}

func TestStrictDistributedDeployUsesLegacyLocationEvidenceWithoutRegistryFallback(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Registryless Location Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	imageRef := "registry.fugue.internal:5000/fugue-apps/legacy:git-abc"
	app := model.App{
		ID:       "app_1",
		TenantID: tenant.ID,
		Spec: model.AppSpec{
			Image:     imageRef,
			Replicas:  1,
			RuntimeID: "runtime_1",
		},
		Source: &model.AppSource{ResolvedImageRef: imageRef},
	}
	now := time.Now().UTC()
	if _, err := stateStore.UpsertImageLocation(model.ImageLocation{
		TenantID:        tenant.ID,
		AppID:           app.ID,
		ImageRef:        imageRef,
		RuntimeID:       "runtime_1",
		ClusterNodeName: "worker-1",
		CacheEndpoint:   "http://worker-1.example.com:5000",
		Status:          model.ImageLocationStatusPresent,
		LastSeenAt:      &now,
	}); err != nil {
		t.Fatalf("upsert legacy image location: %v", err)
	}
	svc := &Service{
		Store:            stateStore,
		Config:           config.ControllerConfig{ImageStoreMode: "distributed"},
		registryPushBase: "registry.fugue.internal:5000",
		registryPullBase: "registry.fugue.internal:5000",
		inspectManagedImage: func(context.Context, string) (bool, map[string]int64, error) {
			t.Fatal("strict distributed deploy must not inspect the central registry when node-local location evidence exists")
			return false, nil, nil
		},
	}

	available, err := svc.deployImageRefAvailable(context.Background(), app, deployImageTarget{
		RuntimeID:       "runtime_1",
		ClusterNodeName: "worker-1",
	}, imageRef)
	if err != nil {
		t.Fatalf("deploy image ref available: %v", err)
	}
	if !available {
		t.Fatal("expected node-local location evidence to make strict distributed deploy available")
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

func TestScheduleImageHydrationNormalizesLegacyManagedRegistryRef(t *testing.T) {
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
		Store:            stateStore,
		Logger:           log.New(io.Discard, "", 0),
		registryPushBase: "fugue-fugue-registry.fugue-system.svc.cluster.local:5000",
		registryPullBase: "registry.fugue.internal:5000",
	}
	app := model.App{ID: "app_1", TenantID: tenant.ID}
	legacyRef := "fugue-fugue-registry.fugue-system.svc.cluster.local:5000/fugue-apps/demo:git-abc"
	wantRef := "registry.fugue.internal:5000/fugue-apps/demo:git-abc"

	svc.scheduleImageHydration(context.Background(), app, deployImageTarget{RuntimeID: updater.RuntimeID, ClusterNodeName: updater.ClusterNodeName}, legacyRef)

	tasks, err := stateStore.ListNodeUpdateTasks(tenant.ID, false, updater.ID, "")
	if err != nil {
		t.Fatalf("list node update tasks: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected one hydrate task, got %+v", tasks)
	}
	if tasks[0].Payload["images"] != wantRef || tasks[0].Payload["image_ref"] != wantRef {
		t.Fatalf("expected normalized hydrate image %q, got %+v", wantRef, tasks[0].Payload)
	}
}
