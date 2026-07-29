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

func TestControllerImageCachePruneProtectsActiveImageDigestBlob(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Retention Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "Retention Project", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "active", "", model.AppSpec{
		Image:     "registry.pull.example/fugue-apps/demo:current",
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	image, err := stateStore.UpsertImage(model.Image{
		TenantID:        tenant.ID,
		AppID:           app.ID,
		ImageRef:        "registry.push.example/fugue-apps/demo:current",
		CanonicalDigest: digest,
		LifecycleState:  model.ImageLifecycleAvailable,
	})
	if err != nil {
		t.Fatalf("create active image: %v", err)
	}
	if _, err := stateStore.UpsertImageReplica(model.ImageReplica{
		ImageID:         image.ID,
		TenantID:        tenant.ID,
		AppID:           app.ID,
		Digest:          digest,
		NodeID:          "node-1",
		RuntimeID:       "runtime_managed_shared",
		ClusterNodeName: "worker-1",
		Status:          model.ImageReplicaStatusPresent,
		LastVerifiedAt:  timePointer(time.Now().UTC()),
	}); err != nil {
		t.Fatalf("create active replica: %v", err)
	}
	now := time.Now().UTC()
	cacheNode, err := stateStore.UpsertImageCacheInventory(model.ImageCacheNodeInventory{
		NodeID:          "node-1",
		RuntimeID:       "runtime_managed_shared",
		ClusterNodeName: "worker-1",
		ObservedAt:      now,
		UnreferencedBlobs: []model.ImageCachePruneBlobCandidate{{
			Digest:    digest,
			SizeBytes: 1024,
		}},
	}, nil)
	if err != nil {
		t.Fatalf("record unreferenced active blob: %v", err)
	}
	svc := &Service{
		Store: stateStore,
		Config: config.ControllerConfig{
			ImageCacheInventoryTTL:           2 * time.Hour,
			ImageStoreOrphanPruneGracePeriod: time.Nanosecond,
		},
		registryPushBase: "registry.push.example",
		registryPullBase: "registry.pull.example",
	}
	protected, err := svc.controllerImageCacheProtectedSet(context.Background())
	if err != nil {
		t.Fatalf("compute protected image set: %v", err)
	}
	plan, err := svc.computeControllerImageCachePrunePlan(context.Background(), cacheNode, protected, model.ImageCachePruneModeDelete)
	if err != nil {
		t.Fatalf("compute prune plan: %v", err)
	}
	if plan.CandidateBlobCount != 0 || plan.ProtectedBlobCount != 1 {
		t.Fatalf("active image digest must be protected from blob prune, got %+v", plan)
	}
}
