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
