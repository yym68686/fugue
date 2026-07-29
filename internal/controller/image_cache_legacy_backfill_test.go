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

func TestReconcileLegacyDistributedImageMetadataBackfillsCurrentPhysicalImage(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Legacy Distributed Image Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "Legacy Distributed Image Project", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	managedRef := "registry.push.example/fugue-apps/demo@" + digest
	runtimeRef := "registry.pull.example/fugue-apps/demo@" + digest
	app, err := stateStore.CreateImportedAppWithoutRoute(tenant.ID, project.ID, "legacy-app", "", model.AppSpec{
		Image:     runtimeRef,
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:             model.AppSourceTypeDockerImage,
		ImageRef:         "ghcr.io/example/demo@" + digest,
		ResolvedImageRef: managedRef,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	now := time.Now().UTC()
	if _, err := stateStore.UpsertImageCacheInventory(model.ImageCacheNodeInventory{
		NodeID:          "machine-1",
		RuntimeID:       "runtime_managed_shared",
		ClusterNodeName: "worker-1",
		CacheEndpoint:   "http://worker-1:5000",
		ObservedAt:      now,
		Status:          "reported",
	}, []model.ImageCacheManifest{{
		Repo:              "fugue-apps/demo",
		Target:            digest,
		Digest:            digest,
		MediaType:         "application/vnd.oci.image.manifest.v1+json",
		ManifestSizeBytes: 100,
		TotalBlobBytes:    500,
		LastSeenAt:        now,
		Present:           true,
	}}); err != nil {
		t.Fatalf("upsert cache inventory: %v", err)
	}

	svc := &Service{
		Store: stateStore,
		Config: config.ControllerConfig{
			ImageStoreMode:           "distributed",
			ImageStoreMinReplicas:    1,
			ImageStoreTargetReplicas: 2,
			ImageCacheInventoryTTL:   2 * time.Hour,
		},
		registryPushBase: "registry.push.example",
		registryPullBase: "registry.pull.example",
		now:              func() time.Time { return now },
	}
	if err := svc.reconcileLegacyDistributedImageMetadata(context.Background()); err != nil {
		t.Fatalf("backfill legacy image metadata: %v", err)
	}
	if err := svc.reconcileLegacyDistributedImageMetadata(context.Background()); err != nil {
		t.Fatalf("repeat legacy image metadata backfill: %v", err)
	}

	images, err := stateStore.ListImages(model.ImageFilter{TenantID: tenant.ID, AppID: app.ID})
	if err != nil {
		t.Fatalf("list images: %v", err)
	}
	if len(images) != 1 || images[0].CanonicalDigest != digest || images[0].LifecycleState != model.ImageLifecycleAvailable {
		t.Fatalf("unexpected backfilled images: %+v", images)
	}
	aliases, err := stateStore.ListImageAliases(model.ImageAliasFilter{ImageID: images[0].ID, TenantID: tenant.ID})
	if err != nil {
		t.Fatalf("list aliases: %v", err)
	}
	if !legacyBackfillHasAlias(aliases, managedRef) || !legacyBackfillHasAlias(aliases, runtimeRef) {
		t.Fatalf("expected managed and runtime aliases, got %+v", aliases)
	}
	pins, err := stateStore.ListImagePins(model.ImagePinFilter{ImageID: images[0].ID, TenantID: tenant.ID, AppID: app.ID})
	if err != nil {
		t.Fatalf("list pins: %v", err)
	}
	if len(pins) != 1 || pins[0].Reason != model.ImagePinReasonCurrentDeploy {
		t.Fatalf("unexpected current image pins: %+v", pins)
	}
	replicas, err := stateStore.ListImageReplicas(model.ImageReplicaFilter{ImageID: images[0].ID, TenantID: tenant.ID})
	if err != nil {
		t.Fatalf("list replicas: %v", err)
	}
	if len(replicas) != 1 || replicas[0].ClusterNodeName != "worker-1" || replicas[0].Status != model.ImageReplicaStatusPresent {
		t.Fatalf("unexpected backfilled replicas: %+v", replicas)
	}
}

func legacyBackfillHasAlias(aliases []model.ImageAlias, want string) bool {
	for _, alias := range aliases {
		if alias.AliasRef == want {
			return true
		}
	}
	return false
}
