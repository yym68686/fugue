package controller

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestScheduleImageCacheInventoryReportsRequiresCapability(t *testing.T) {
	t.Parallel()

	stateStore, nodeSecret := newImageCacheControllerTestStore(t)
	for _, item := range []struct {
		node         string
		capabilities []string
	}{
		{"worker-1", []string{"heartbeat", "tasks", model.NodeUpdateTaskTypeReportImageCache}},
		{"worker-2", []string{"heartbeat", "tasks"}},
	} {
		if _, _, err := stateStore.EnrollNodeUpdater(nodeSecret, item.node, "https://"+item.node+".example.com", nil, item.node, "fingerprint-"+item.node, "v10", "join-v10", item.capabilities); err != nil {
			t.Fatalf("enroll updater %s: %v", item.node, err)
		}
	}
	svc := &Service{
		Store:  stateStore,
		Config: config.ControllerConfig{ImageCacheInventoryEnabled: true},
	}
	if err := svc.scheduleImageCacheInventoryReports(context.Background()); err != nil {
		t.Fatalf("schedule inventory: %v", err)
	}
	if err := svc.scheduleImageCacheInventoryReports(context.Background()); err != nil {
		t.Fatalf("reschedule inventory: %v", err)
	}
	tasks, err := stateStore.ListNodeUpdateTasks("", true, "", model.NodeUpdateTaskStatusPending)
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(tasks) != 1 || tasks[0].Type != model.NodeUpdateTaskTypeReportImageCache || tasks[0].ClusterNodeName != "worker-1" {
		t.Fatalf("unexpected tasks: %+v", tasks)
	}
}

func TestScheduleOrphanImageCachePruneObservePersistsPlanOnly(t *testing.T) {
	t.Parallel()

	stateStore, nodeSecret := newImageCacheControllerTestStore(t)
	if _, _, err := stateStore.EnrollNodeUpdater(nodeSecret, "worker-1", "https://worker-1.example.com", nil, "machine-1", "fingerprint-worker-1", "v10", "join-v10", []string{"heartbeat", "tasks", model.NodeUpdateTaskTypePruneImageCache}); err != nil {
		t.Fatalf("enroll updater: %v", err)
	}
	upsertControllerImageCacheManifest(t, stateStore)
	svc := &Service{
		Store: stateStore,
		Config: config.ControllerConfig{
			ImageStoreOrphanPruneMode:                  model.ImageCachePruneModeObserve,
			ImageStoreOrphanPruneGracePeriod:           time.Hour,
			ImageStoreOrphanPruneMaxDeleteBytesPerNode: "1Gi",
			ImageStoreOrphanPruneMinReplicaCount:       1,
			ImageCacheInventoryTTL:                     2 * time.Hour,
		},
	}
	if err := svc.scheduleOrphanImageCachePrune(context.Background()); err != nil {
		t.Fatalf("schedule orphan prune: %v", err)
	}
	plans, err := stateStore.ListImageCachePrunePlans(model.ImageCachePrunePlanFilter{Mode: model.ImageCachePruneModeObserve})
	if err != nil {
		t.Fatalf("list plans: %v", err)
	}
	if len(plans) != 1 || plans[0].CandidateManifestCount != 1 {
		t.Fatalf("unexpected plans: %+v", plans)
	}
	tasks, err := stateStore.ListNodeUpdateTasks("", true, "", model.NodeUpdateTaskStatusPending)
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("observe mode created tasks: %+v", tasks)
	}
}

func TestScheduleOrphanImageCachePruneDryRunCreatesNonDeletingTask(t *testing.T) {
	t.Parallel()

	stateStore, nodeSecret := newImageCacheControllerTestStore(t)
	if _, _, err := stateStore.EnrollNodeUpdater(nodeSecret, "worker-1", "https://worker-1.example.com", nil, "machine-1", "fingerprint-worker-1", "v10", "join-v10", []string{"heartbeat", "tasks", model.NodeUpdateTaskTypePruneImageCache}); err != nil {
		t.Fatalf("enroll updater: %v", err)
	}
	upsertControllerImageCacheManifest(t, stateStore)
	svc := &Service{
		Store: stateStore,
		Config: config.ControllerConfig{
			ImageStoreOrphanPruneMode:                  model.ImageCachePruneModeDryRun,
			ImageStoreOrphanPruneGracePeriod:           time.Hour,
			ImageStoreOrphanPruneMaxTargetsPerNode:     10,
			ImageStoreOrphanPruneMaxDeleteBytesPerNode: "1Gi",
			ImageStoreOrphanPruneMinReplicaCount:       1,
			ImageCacheInventoryTTL:                     2 * time.Hour,
		},
	}
	if err := svc.scheduleOrphanImageCachePrune(context.Background()); err != nil {
		t.Fatalf("schedule orphan prune: %v", err)
	}
	tasks, err := stateStore.ListNodeUpdateTasks("", true, "", model.NodeUpdateTaskStatusPending)
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(tasks) != 1 || tasks[0].Type != model.NodeUpdateTaskTypePruneImageCache {
		t.Fatalf("unexpected tasks: %+v", tasks)
	}
	if tasks[0].Payload["dry_run"] != "true" || tasks[0].Payload["allow_delete"] != "false" || tasks[0].Payload["targets_json"] == "" {
		t.Fatalf("unexpected prune payload: %+v", tasks[0].Payload)
	}
}

func TestScheduleOrphanImageCachePruneDeleteCreatesLimitedDeletingTask(t *testing.T) {
	t.Parallel()

	stateStore, nodeSecret := newImageCacheControllerTestStore(t)
	if _, _, err := stateStore.EnrollNodeUpdater(nodeSecret, "worker-1", "https://worker-1.example.com", nil, "machine-1", "fingerprint-worker-1", "v10", "join-v10", []string{"heartbeat", "tasks", model.NodeUpdateTaskTypePruneImageCache}); err != nil {
		t.Fatalf("enroll updater: %v", err)
	}
	upsertControllerImageCacheManifest(t, stateStore)
	svc := &Service{
		Store: stateStore,
		Config: config.ControllerConfig{
			ImageStoreOrphanPruneMode:                  model.ImageCachePruneModeDelete,
			ImageStoreOrphanPruneGracePeriod:           time.Hour,
			ImageStoreOrphanPruneMaxTargetsPerNode:     10,
			ImageStoreOrphanPruneMaxDeleteBytesPerNode: "104857600",
			ImageStoreOrphanPruneMinReplicaCount:       1,
			ImageCacheInventoryTTL:                     2 * time.Hour,
		},
	}
	if err := svc.scheduleOrphanImageCachePrune(context.Background()); err != nil {
		t.Fatalf("schedule orphan prune: %v", err)
	}
	tasks, err := stateStore.ListNodeUpdateTasks("", true, "", model.NodeUpdateTaskStatusPending)
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(tasks) != 1 || tasks[0].Type != model.NodeUpdateTaskTypePruneImageCache {
		t.Fatalf("unexpected tasks: %+v", tasks)
	}
	if tasks[0].Payload["dry_run"] != "false" ||
		tasks[0].Payload["allow_delete"] != "true" ||
		tasks[0].Payload["max_delete_bytes"] != "104857600" ||
		tasks[0].Payload["prune_reason"] != "image-cache-orphan" {
		t.Fatalf("unexpected prune payload: %+v", tasks[0].Payload)
	}
	var targets []map[string]string
	if err := json.Unmarshal([]byte(tasks[0].Payload["targets_json"]), &targets); err != nil {
		t.Fatalf("decode targets: %v", err)
	}
	if len(targets) != 1 || targets[0]["repo"] != "fugue-apps/demo" {
		t.Fatalf("unexpected targets: %+v", targets)
	}
}

func TestScheduleOrphanImageCachePruneDeleteHaltsUnsafeCandidateReasons(t *testing.T) {
	t.Parallel()

	stateStore, nodeSecret := newImageCacheControllerTestStore(t)
	if _, _, err := stateStore.EnrollNodeUpdater(nodeSecret, "worker-1", "https://worker-1.example.com", nil, "machine-1", "fingerprint-worker-1", "v10", "join-v10", []string{"heartbeat", "tasks", model.NodeUpdateTaskTypePruneImageCache}); err != nil {
		t.Fatalf("enroll updater: %v", err)
	}
	digest := "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	image, err := stateStore.UpsertImage(model.Image{
		TenantID:        "tenant_1",
		AppID:           "app_1",
		ImageRef:        "registry.fugue.internal:5000/fugue-apps/demo:old",
		CanonicalDigest: digest,
		LifecycleState:  model.ImageLifecycleAvailable,
	})
	if err != nil {
		t.Fatalf("upsert image: %v", err)
	}
	if _, err := stateStore.UpsertImageReplica(model.ImageReplica{
		ImageID:         image.ID,
		TenantID:        image.TenantID,
		AppID:           image.AppID,
		Digest:          digest,
		NodeID:          "machine-1",
		RuntimeID:       "runtime-1",
		ClusterNodeName: "worker-1",
		Status:          model.ImageReplicaStatusStale,
	}); err != nil {
		t.Fatalf("upsert stale replica: %v", err)
	}
	now := time.Now().UTC()
	for _, node := range []string{"worker-2", "worker-3"} {
		if _, err := stateStore.UpsertImageReplica(model.ImageReplica{
			ImageID:         image.ID,
			TenantID:        image.TenantID,
			AppID:           image.AppID,
			Digest:          digest,
			ClusterNodeName: node,
			Status:          model.ImageReplicaStatusPresent,
			LastVerifiedAt:  &now,
		}); err != nil {
			t.Fatalf("upsert healthy replica: %v", err)
		}
	}
	upsertControllerImageCacheManifestWithDigest(t, stateStore, digest)
	svc := &Service{
		Store: stateStore,
		Config: config.ControllerConfig{
			ImageStoreOrphanPruneMode:                  model.ImageCachePruneModeDelete,
			ImageStoreOrphanPruneGracePeriod:           time.Hour,
			ImageStoreOrphanPruneMaxTargetsPerNode:     10,
			ImageStoreOrphanPruneMaxDeleteBytesPerNode: "104857600",
			ImageStoreOrphanPruneMinReplicaCount:       1,
			ImageCacheInventoryTTL:                     2 * time.Hour,
		},
	}
	if err := svc.scheduleOrphanImageCachePrune(context.Background()); err != nil {
		t.Fatalf("schedule orphan prune: %v", err)
	}
	plans, err := stateStore.ListImageCachePrunePlans(model.ImageCachePrunePlanFilter{Mode: model.ImageCachePruneModeDelete})
	if err != nil {
		t.Fatalf("list plans: %v", err)
	}
	if len(plans) != 1 || plans[0].CandidateSummary["stale_replica"] != 1 {
		t.Fatalf("expected persisted unsafe plan, got %+v", plans)
	}
	tasks, err := stateStore.ListNodeUpdateTasks("", true, "", model.NodeUpdateTaskStatusPending)
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("unsafe candidate reason created prune tasks: %+v", tasks)
	}
}

func TestScheduleOrphanImageCachePruneSkipsWhenPruneAlreadyActive(t *testing.T) {
	t.Parallel()

	stateStore, nodeSecret := newImageCacheControllerTestStore(t)
	updater, _, err := stateStore.EnrollNodeUpdater(nodeSecret, "worker-1", "https://worker-1.example.com", nil, "machine-1", "fingerprint-worker-1", "v10", "join-v10", []string{"heartbeat", "tasks", model.NodeUpdateTaskTypePruneImageCache})
	if err != nil {
		t.Fatalf("enroll updater: %v", err)
	}
	upsertControllerImageCacheManifest(t, stateStore)
	if _, err := stateStore.CreateNodeUpdateTask(controllerImageCachePrunePrincipal(), updater.ID, "", "", model.NodeUpdateTaskTypePruneImageCache, map[string]string{
		"prune_reason": "manual-canary",
	}); err != nil {
		t.Fatalf("create active prune task: %v", err)
	}
	svc := &Service{
		Store: stateStore,
		Config: config.ControllerConfig{
			ImageStoreOrphanPruneMode:                  model.ImageCachePruneModeDelete,
			ImageStoreOrphanPruneGracePeriod:           time.Hour,
			ImageStoreOrphanPruneMaxTargetsPerNode:     10,
			ImageStoreOrphanPruneMaxDeleteBytesPerNode: "104857600",
			ImageStoreOrphanPruneMinReplicaCount:       1,
			ImageCacheInventoryTTL:                     2 * time.Hour,
		},
	}
	if err := svc.scheduleOrphanImageCachePrune(context.Background()); err != nil {
		t.Fatalf("schedule orphan prune: %v", err)
	}
	tasks, err := stateStore.ListNodeUpdateTasks("", true, "", model.NodeUpdateTaskStatusPending)
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(tasks) != 1 || tasks[0].Payload["prune_reason"] != "manual-canary" {
		t.Fatalf("expected only existing prune task, got %+v", tasks)
	}
}

func TestScheduleOrphanImageCachePruneHaltsAfterControllerFailure(t *testing.T) {
	t.Parallel()

	stateStore, nodeSecret := newImageCacheControllerTestStore(t)
	updater, _, err := stateStore.EnrollNodeUpdater(nodeSecret, "worker-1", "https://worker-1.example.com", nil, "machine-1", "fingerprint-worker-1", "v10", "join-v10", []string{"heartbeat", "tasks", model.NodeUpdateTaskTypePruneImageCache})
	if err != nil {
		t.Fatalf("enroll updater: %v", err)
	}
	upsertControllerImageCacheManifest(t, stateStore)
	task, err := stateStore.CreateNodeUpdateTask(controllerImageCachePrunePrincipal(), updater.ID, "", "", model.NodeUpdateTaskTypePruneImageCache, map[string]string{
		"prune_reason": "image-cache-orphan",
	})
	if err != nil {
		t.Fatalf("create prune task: %v", err)
	}
	if _, err := stateStore.ClaimNodeUpdateTask(task.ID, updater.ID); err != nil {
		t.Fatalf("claim prune task: %v", err)
	}
	if _, err := stateStore.CompleteNodeUpdateTask(task.ID, updater.ID, model.NodeUpdateTaskStatusFailed, "failed", "post inventory failed"); err != nil {
		t.Fatalf("fail prune task: %v", err)
	}
	svc := &Service{
		Store: stateStore,
		Config: config.ControllerConfig{
			ImageStoreOrphanPruneMode:                  model.ImageCachePruneModeDelete,
			ImageStoreOrphanPruneGracePeriod:           time.Hour,
			ImageStoreOrphanPruneMaxTargetsPerNode:     10,
			ImageStoreOrphanPruneMaxDeleteBytesPerNode: "104857600",
			ImageStoreOrphanPruneMinReplicaCount:       1,
			ImageCacheInventoryTTL:                     2 * time.Hour,
		},
	}
	if err := svc.scheduleOrphanImageCachePrune(context.Background()); err != nil {
		t.Fatalf("schedule orphan prune: %v", err)
	}
	pending, err := stateStore.ListNodeUpdateTasks("", true, "", model.NodeUpdateTaskStatusPending)
	if err != nil {
		t.Fatalf("list pending tasks: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("failed controller prune did not halt automation: %+v", pending)
	}
}

func TestControllerImageCachePrunePlanClassifiesStaleReplica(t *testing.T) {
	t.Parallel()

	stateStore, _ := newImageCacheControllerTestStore(t)
	digest := "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	image, err := stateStore.UpsertImage(model.Image{
		TenantID:        "tenant_1",
		AppID:           "app_1",
		ImageRef:        "registry.fugue.internal:5000/fugue-apps/demo:old",
		CanonicalDigest: digest,
		LifecycleState:  model.ImageLifecycleAvailable,
	})
	if err != nil {
		t.Fatalf("upsert image: %v", err)
	}
	if _, err := stateStore.UpsertImageReplica(model.ImageReplica{
		ImageID:         image.ID,
		TenantID:        image.TenantID,
		AppID:           image.AppID,
		Digest:          digest,
		NodeID:          "machine-1",
		RuntimeID:       "runtime-1",
		ClusterNodeName: "worker-1",
		Status:          model.ImageReplicaStatusStale,
	}); err != nil {
		t.Fatalf("upsert stale replica: %v", err)
	}
	now := time.Now().UTC()
	for _, node := range []string{"worker-2", "worker-3"} {
		if _, err := stateStore.UpsertImageReplica(model.ImageReplica{
			ImageID:         image.ID,
			TenantID:        image.TenantID,
			AppID:           image.AppID,
			Digest:          digest,
			ClusterNodeName: node,
			Status:          model.ImageReplicaStatusPresent,
			LastVerifiedAt:  &now,
		}); err != nil {
			t.Fatalf("upsert healthy replica: %v", err)
		}
	}
	upsertControllerImageCacheManifestWithDigest(t, stateStore, digest)
	svc := &Service{
		Store: stateStore,
		Config: config.ControllerConfig{
			ImageStoreOrphanPruneGracePeriod:           time.Hour,
			ImageStoreOrphanPruneMaxDeleteBytesPerNode: "1Gi",
			ImageStoreOrphanPruneMinReplicaCount:       1,
			ImageCacheInventoryTTL:                     2 * time.Hour,
		},
	}
	protected, err := svc.controllerImageCacheProtectedSet(context.Background())
	if err != nil {
		t.Fatalf("protected set: %v", err)
	}
	node := model.ImageCacheNodeInventory{NodeID: "machine-1", RuntimeID: "runtime-1", ClusterNodeName: "worker-1"}
	plan, err := svc.computeControllerImageCachePrunePlan(context.Background(), node, protected, model.ImageCachePruneModeObserve)
	if err != nil {
		t.Fatalf("compute plan: %v", err)
	}
	if len(plan.Candidates) != 1 || plan.Candidates[0].Reason != "stale_replica" {
		t.Fatalf("expected stale replica candidate, got %+v", plan)
	}
}

func TestControllerImageCachePrunePlanClassifiesDeletedGeneration(t *testing.T) {
	t.Parallel()

	stateStore, _ := newImageCacheControllerTestStore(t)
	digest := "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	if _, err := stateStore.UpsertImage(model.Image{
		TenantID:        "tenant_1",
		AppID:           "app_1",
		ImageRef:        "registry.fugue.internal:5000/fugue-apps/demo:old",
		CanonicalDigest: digest,
		LifecycleState:  model.ImageLifecycleDeleted,
	}); err != nil {
		t.Fatalf("upsert deleted image: %v", err)
	}
	upsertControllerImageCacheManifestWithDigest(t, stateStore, digest)
	svc := &Service{
		Store: stateStore,
		Config: config.ControllerConfig{
			ImageStoreOrphanPruneGracePeriod:           time.Hour,
			ImageStoreOrphanPruneMaxDeleteBytesPerNode: "1Gi",
			ImageStoreOrphanPruneMinReplicaCount:       1,
			ImageCacheInventoryTTL:                     2 * time.Hour,
		},
	}
	protected, err := svc.controllerImageCacheProtectedSet(context.Background())
	if err != nil {
		t.Fatalf("protected set: %v", err)
	}
	node := model.ImageCacheNodeInventory{NodeID: "machine-1", RuntimeID: "runtime-1", ClusterNodeName: "worker-1"}
	plan, err := svc.computeControllerImageCachePrunePlan(context.Background(), node, protected, model.ImageCachePruneModeObserve)
	if err != nil {
		t.Fatalf("compute plan: %v", err)
	}
	if len(plan.Candidates) != 1 || plan.Candidates[0].Reason != "deleted_image_generation" {
		t.Fatalf("expected deleted generation candidate, got %+v", plan)
	}
}

func newImageCacheControllerTestStore(t *testing.T) (*store.Store, string) {
	t.Helper()
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Image Cache Controller Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, nodeSecret, err := stateStore.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	return stateStore, nodeSecret
}

func upsertControllerImageCacheManifest(t *testing.T, stateStore *store.Store) {
	t.Helper()
	upsertControllerImageCacheManifestWithDigest(t, stateStore, "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
}

func upsertControllerImageCacheManifestWithDigest(t *testing.T, stateStore *store.Store, digest string) {
	t.Helper()
	created := time.Now().UTC().Add(-48 * time.Hour)
	if _, err := stateStore.UpsertImageCacheInventory(model.ImageCacheNodeInventory{
		NodeID:          "machine-1",
		ClusterNodeName: "worker-1",
		RuntimeID:       "runtime-1",
		CacheEndpoint:   "http://worker-1:5000",
		ManifestCount:   1,
		ObservedAt:      time.Now().UTC(),
		Status:          "reported",
	}, []model.ImageCacheManifest{{
		Repo:              "fugue-apps/demo",
		Target:            "old",
		Digest:            digest,
		ManifestSizeBytes: 100,
		TotalBlobBytes:    500,
		ReferencedBlobs:   []string{"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
		CreatedAtObserved: &created,
		LastSeenAt:        time.Now().UTC(),
		Present:           true,
	}}); err != nil {
		t.Fatalf("upsert image cache inventory: %v", err)
	}
}
