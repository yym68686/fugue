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
