package controller

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestScheduleDistributedImagePruneUsesDiskLimitDeletePayloadWhenEnabled(t *testing.T) {
	t.Parallel()

	payload := scheduleDistributedImagePruneTestPayload(t, true)
	if payload["dry_run"] != "false" {
		t.Fatalf("dry_run = %q, want false", payload["dry_run"])
	}
	if payload["allow_delete"] != "true" {
		t.Fatalf("allow_delete = %q, want true", payload["allow_delete"])
	}
	if payload["image_ref"] != "registry.fugue.internal:5000/fugue-apps/demo:git-abc" {
		t.Fatalf("image_ref = %q", payload["image_ref"])
	}
	if payload["digest"] != "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("digest = %q", payload["digest"])
	}
	if payload["max_delete_bytes"] != "10Gi" {
		t.Fatalf("max_delete_bytes = %q, want 10Gi", payload["max_delete_bytes"])
	}
}

func TestScheduleDistributedImagePruneKeepsDryRunWhenDisabled(t *testing.T) {
	t.Parallel()

	payload := scheduleDistributedImagePruneTestPayload(t, false)
	if payload["dry_run"] != "true" {
		t.Fatalf("dry_run = %q, want true", payload["dry_run"])
	}
	if payload["allow_delete"] != "false" {
		t.Fatalf("allow_delete = %q, want false", payload["allow_delete"])
	}
}

func TestApplyDistributedImageRetentionPlanSchedulesSingleReplicaAfterDeletingTransition(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Retention Transition Tenant")
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
		"machine-1",
		"fingerprint-worker-1",
		"v1",
		"join-v1",
		[]string{"heartbeat", "tasks", model.NodeUpdateTaskTypePruneImageCache},
	)
	if err != nil {
		t.Fatalf("enroll updater: %v", err)
	}
	image, err := stateStore.UpsertImage(model.Image{
		TenantID:        tenant.ID,
		AppID:           "app_1",
		ImageRef:        "registry.fugue.internal:5000/fugue-apps/demo:old",
		CanonicalDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		LifecycleState:  model.ImageLifecycleAvailable,
	})
	if err != nil {
		t.Fatalf("upsert image: %v", err)
	}
	now := time.Now().UTC()
	if _, err := stateStore.UpsertImageReplica(model.ImageReplica{
		ImageID:         image.ID,
		TenantID:        image.TenantID,
		AppID:           image.AppID,
		Digest:          image.CanonicalDigest,
		NodeID:          updater.MachineID,
		RuntimeID:       updater.RuntimeID,
		ClusterNodeName: updater.ClusterNodeName,
		CacheEndpoint:   "http://worker-1:5000",
		Status:          model.ImageReplicaStatusPresent,
		LastVerifiedAt:  &now,
	}); err != nil {
		t.Fatalf("upsert image replica: %v", err)
	}

	svc := &Service{
		Store: stateStore,
		Config: config.ControllerConfig{
			ImageStoreMode:                "distributed",
			ImageStoreMinReplicas:         1,
			ImageStoreTargetReplicas:      1,
			ImageStorePruneEnabled:        true,
			ImageStorePruneMaxDeleteBytes: "10Gi",
		},
	}
	plan := model.DistributedImageRetentionPlan{DropImageIDs: []string{image.ID}}
	if err := svc.applyDistributedImageRetentionPlan(context.Background(), model.App{ID: image.AppID}, []model.Image{image}, plan, now); err != nil {
		t.Fatalf("apply retention plan: %v", err)
	}

	tasks, err := stateStore.ListNodeUpdateTasks(image.TenantID, false, updater.ID, model.NodeUpdateTaskStatusPending)
	if err != nil {
		t.Fatalf("list prune tasks: %v", err)
	}
	if len(tasks) != 1 || tasks[0].Payload["prune_reason"] != "unpinned-excess-replica" || tasks[0].Payload["image_id"] != image.ID {
		t.Fatalf("expected one targeted retention prune task, got %+v", tasks)
	}
	images, err := stateStore.ListImages(model.ImageFilter{TenantID: image.TenantID, AppID: image.AppID})
	if err != nil {
		t.Fatalf("list images: %v", err)
	}
	if len(images) != 1 || images[0].LifecycleState != model.ImageLifecycleDeleting {
		t.Fatalf("expected deleting image after retention, got %+v", images)
	}
	replicas, err := stateStore.ListImageReplicas(model.ImageReplicaFilter{ImageID: image.ID, PlatformAdmin: true})
	if err != nil {
		t.Fatalf("list image replicas: %v", err)
	}
	if len(replicas) != 1 || replicas[0].Status != model.ImageReplicaStatusStale {
		t.Fatalf("expected stale replica after retention, got %+v", replicas)
	}
}

func scheduleDistributedImagePruneTestPayload(t *testing.T, pruneEnabled bool) map[string]string {
	t.Helper()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Distributed Image Cleanup Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, nodeSecret, err := stateStore.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	image, err := stateStore.UpsertImage(model.Image{
		TenantID:        tenant.ID,
		AppID:           "app_1",
		ImageRef:        "registry.fugue.internal:5000/fugue-apps/demo:git-abc",
		CanonicalDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		LifecycleState:  model.ImageLifecycleAvailable,
	})
	if err != nil {
		t.Fatalf("upsert image: %v", err)
	}
	now := time.Now().UTC()
	for idx, name := range []string{"worker-1", "worker-2", "worker-3"} {
		updater, _, err := stateStore.EnrollNodeUpdater(
			nodeSecret,
			name,
			"https://"+name+".example.com",
			nil,
			name,
			"fingerprint-"+name,
			"v1",
			"join-v1",
			[]string{"heartbeat", "tasks", model.NodeUpdateTaskTypePruneImageCache},
		)
		if err != nil {
			t.Fatalf("enroll updater %s: %v", name, err)
		}
		if _, err := stateStore.UpsertImageReplica(model.ImageReplica{
			ImageID:         image.ID,
			TenantID:        image.TenantID,
			AppID:           image.AppID,
			Digest:          image.CanonicalDigest,
			NodeID:          updater.MachineID,
			RuntimeID:       updater.RuntimeID,
			ClusterNodeName: updater.ClusterNodeName,
			CacheEndpoint:   "http://" + name + ":5000",
			Status:          model.ImageReplicaStatusPresent,
			LastVerifiedAt:  &now,
			SizeBytes:       int64(idx + 1),
		}); err != nil {
			t.Fatalf("upsert replica %s: %v", name, err)
		}
	}
	svc := &Service{
		Store: stateStore,
		Config: config.ControllerConfig{
			ImageStoreMode:                "distributed",
			ImageStoreMinReplicas:         2,
			ImageStoreTargetReplicas:      2,
			ImageStorePruneEnabled:        pruneEnabled,
			ImageStorePruneMaxDeleteBytes: "10Gi",
		},
	}
	if err := svc.scheduleDistributedImagePrune(context.Background(), image); err != nil {
		t.Fatalf("schedule prune: %v", err)
	}
	tasks, err := stateStore.ListNodeUpdateTasks(image.TenantID, false, "", model.NodeUpdateTaskStatusPending)
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("tasks = %d, want 1: %+v", len(tasks), tasks)
	}
	if tasks[0].Type != model.NodeUpdateTaskTypePruneImageCache {
		t.Fatalf("task type = %q", tasks[0].Type)
	}
	return tasks[0].Payload
}

func TestCleanupDeletedAppDistributedImagesRetiresUnreferencedImage(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	image, err := stateStore.UpsertImage(model.Image{
		ID:                       "img_deleted_app",
		TenantID:                 "tenant_deleted_app",
		AppID:                    "app_deleted",
		ImageRef:                 "registry.example/fugue-apps/deleted:old",
		CanonicalDigest:          "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
		LifecycleState:           model.ImageLifecycleAvailable,
		RequiredReplicaCount:     1,
		MinAvailableReplicaCount: 1,
	})
	if err != nil {
		t.Fatalf("upsert image: %v", err)
	}
	if _, err := stateStore.UpsertImagePin(model.ImagePin{
		ImageID: image.ID, TenantID: image.TenantID, AppID: image.AppID,
		Reason: model.ImagePinReasonCurrentDeploy, MinReplicas: 1,
	}); err != nil {
		t.Fatalf("upsert image pin: %v", err)
	}
	now := time.Now().UTC()
	lease := now.Add(time.Hour)
	if _, err := stateStore.UpsertImageReplica(model.ImageReplica{
		ImageID: image.ID, TenantID: image.TenantID, AppID: image.AppID,
		Digest: image.CanonicalDigest, ClusterNodeName: "worker-1",
		Status: model.ImageReplicaStatusPresent, LastVerifiedAt: &now, LeaseExpiresAt: &lease,
	}); err != nil {
		t.Fatalf("upsert image replica: %v", err)
	}

	svc := &Service{Store: stateStore, Config: config.ControllerConfig{ImageStoreMode: "distributed"}}
	if err := svc.cleanupDeletedAppDistributedImages(context.Background(), model.App{ID: image.AppID, TenantID: image.TenantID}); err != nil {
		t.Fatalf("cleanup deleted app images: %v", err)
	}
	got, err := stateStore.GetImage(image.ID, image.TenantID, false)
	if err != nil {
		t.Fatalf("get retired image: %v", err)
	}
	if got.LifecycleState != model.ImageLifecycleDeleting {
		t.Fatalf("deleted app image lifecycle = %q, want deleting", got.LifecycleState)
	}
	pins, err := stateStore.ListImagePins(model.ImagePinFilter{ImageID: image.ID, PlatformAdmin: true})
	if err != nil {
		t.Fatalf("list image pins: %v", err)
	}
	if len(pins) != 0 {
		t.Fatalf("deleted app pins should be removed: %+v", pins)
	}
	replicas, err := stateStore.ListImageReplicas(model.ImageReplicaFilter{ImageID: image.ID, PlatformAdmin: true})
	if err != nil {
		t.Fatalf("list image replicas: %v", err)
	}
	if len(replicas) != 1 || replicas[0].Status != model.ImageReplicaStatusStale || replicas[0].LastError != "retention_excess" {
		t.Fatalf("retired image replica should be stale: %+v", replicas)
	}
}

func TestCleanupDeletedAppDistributedImagesPreservesSharedPinnedImage(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	image, err := stateStore.UpsertImage(model.Image{
		ID:              "img_shared_app",
		TenantID:        "tenant_shared_app",
		AppID:           "app_deleted",
		ImageRef:        "registry.example/fugue-apps/shared:current",
		CanonicalDigest: "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		LifecycleState:  model.ImageLifecycleAvailable,
	})
	if err != nil {
		t.Fatalf("upsert image: %v", err)
	}
	for _, appID := range []string{"app_deleted", "app_active"} {
		if _, err := stateStore.UpsertImagePin(model.ImagePin{
			ImageID: image.ID, TenantID: image.TenantID, AppID: appID,
			Reason: model.ImagePinReasonCurrentDeploy, MinReplicas: 1,
		}); err != nil {
			t.Fatalf("upsert image pin for %s: %v", appID, err)
		}
	}

	svc := &Service{Store: stateStore, Config: config.ControllerConfig{ImageStoreMode: "distributed"}}
	if err := svc.cleanupDeletedAppDistributedImages(context.Background(), model.App{ID: "app_deleted", TenantID: image.TenantID}); err != nil {
		t.Fatalf("cleanup deleted app images: %v", err)
	}
	got, err := stateStore.GetImage(image.ID, image.TenantID, false)
	if err != nil {
		t.Fatalf("get shared image: %v", err)
	}
	if got.LifecycleState != model.ImageLifecycleAvailable {
		t.Fatalf("shared pinned image lifecycle = %q, want available", got.LifecycleState)
	}
	pins, err := stateStore.ListImagePins(model.ImagePinFilter{ImageID: image.ID, PlatformAdmin: true})
	if err != nil {
		t.Fatalf("list shared image pins: %v", err)
	}
	if len(pins) != 1 || pins[0].AppID != "app_active" {
		t.Fatalf("active app pin must remain: %+v", pins)
	}
}

func TestDistributedImageRetentionSweepReplaysDeletedAppCleanup(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Deleted App Replay Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "deleted-app-replay", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "old-app", "", model.AppSpec{
		Image: "registry.example/fugue-apps/old-app:current", Replicas: 1, RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	image, err := stateStore.UpsertImage(model.Image{
		ID: "img_deleted_replay", TenantID: tenant.ID, AppID: app.ID,
		ImageRef: app.Spec.Image, CanonicalDigest: "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
		LifecycleState: model.ImageLifecycleLost, RequiredReplicaCount: 1, MinAvailableReplicaCount: 1,
	})
	if err != nil {
		t.Fatalf("upsert lost image: %v", err)
	}
	if _, err := stateStore.UpsertImagePin(model.ImagePin{
		ImageID: image.ID, TenantID: tenant.ID, AppID: app.ID, Reason: model.ImagePinReasonCurrentDeploy, MinReplicas: 1,
	}); err != nil {
		t.Fatalf("upsert current pin: %v", err)
	}
	deleteOp, err := stateStore.CreateOperation(model.Operation{TenantID: tenant.ID, Type: model.OperationTypeDelete, AppID: app.ID})
	if err != nil {
		t.Fatalf("create delete operation: %v", err)
	}
	if _, found, err := stateStore.ClaimNextPendingOperation(); err != nil || !found {
		t.Fatalf("claim delete operation: found=%v err=%v", found, err)
	}
	if _, err := stateStore.CompleteManagedOperation(deleteOp.ID, "/tmp/deleted-app.yaml", "deleted"); err != nil {
		t.Fatalf("complete delete operation: %v", err)
	}

	svc := &Service{Store: stateStore, Config: config.ControllerConfig{ImageStoreMode: "distributed"}}
	if err := svc.sweepDistributedImageRetention(context.Background()); err != nil {
		t.Fatalf("sweep distributed image retention: %v", err)
	}
	got, err := stateStore.GetImage(image.ID, tenant.ID, false)
	if err != nil {
		t.Fatalf("get replayed image: %v", err)
	}
	if got.LifecycleState != model.ImageLifecycleDeleting {
		t.Fatalf("replayed deleted-app image lifecycle = %q, want deleting", got.LifecycleState)
	}
	pins, err := stateStore.ListImagePins(model.ImagePinFilter{ImageID: image.ID, PlatformAdmin: true})
	if err != nil {
		t.Fatalf("list replayed pins: %v", err)
	}
	if len(pins) != 0 {
		t.Fatalf("deleted-app replay must remove obsolete pins: %+v", pins)
	}
	deletedApps, err := stateStore.ListDeletedAppsMetadata("", true)
	if err != nil {
		t.Fatalf("list deleted app metadata: %v", err)
	}
	if len(deletedApps) != 1 || deletedApps[0].ID != app.ID {
		t.Fatalf("deleted app metadata = %+v, want only %s", deletedApps, app.ID)
	}
}
