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
	firstPassImages, err := stateStore.ListImages(model.ImageFilter{TenantID: tenant.ID, AppID: app.ID})
	if err != nil {
		t.Fatalf("list first-pass images: %v", err)
	}
	if len(firstPassImages) != 1 {
		t.Fatalf("unexpected first-pass images: %+v", firstPassImages)
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
	if !images[0].UpdatedAt.Equal(firstPassImages[0].UpdatedAt) {
		t.Fatalf("complete metadata should not be rewritten: first=%s second=%s", firstPassImages[0].UpdatedAt, images[0].UpdatedAt)
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
	locations, err := stateStore.ListImageLocations(model.ImageLocationFilter{
		TenantID: tenant.ID,
		ImageRef: images[0].ImageRef,
		Digest:   digest,
		Status:   model.ImageLocationStatusPresent,
	})
	if err != nil {
		t.Fatalf("list image locations: %v", err)
	}
	if len(locations) != 1 || locations[0].ClusterNodeName != "worker-1" || locations[0].LastSeenAt == nil || !locations[0].LastSeenAt.Equal(now) {
		t.Fatalf("unexpected backfilled image locations: %+v", locations)
	}
}

func TestReconcileLegacyDistributedImageMetadataRepairsLostImageFromFreshPhysicalInventory(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Lost Distributed Image Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "Lost Distributed Image Project", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	digest := "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	managedRef := "registry.push.example/fugue-apps/repair:git-current"
	runtimeRef := "registry.pull.example/fugue-apps/repair:git-current"
	app, err := stateStore.CreateImportedAppWithoutRoute(tenant.ID, project.ID, "repair-app", "", model.AppSpec{
		Image:     runtimeRef,
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}, model.AppSource{
		Type:             model.AppSourceTypeDockerImage,
		ImageRef:         "ghcr.io/example/repair:git-current",
		ResolvedImageRef: managedRef,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	image, err := stateStore.UpsertImage(model.Image{
		TenantID:                 tenant.ID,
		AppID:                    app.ID,
		ImageRef:                 "registry.push.example/fugue-apps/repair:historical-alias",
		CanonicalDigest:          digest,
		SourceOperationID:        "op_original",
		LifecycleState:           model.ImageLifecycleLost,
		RequiredReplicaCount:     2,
		MinAvailableReplicaCount: 1,
	})
	if err != nil {
		t.Fatalf("create lost image: %v", err)
	}
	if _, err := stateStore.UpsertImageAlias(model.ImageAlias{
		ImageID:  image.ID,
		TenantID: tenant.ID,
		AliasRef: managedRef,
		Digest:   digest,
	}); err != nil {
		t.Fatalf("create current image alias: %v", err)
	}
	staleAt := time.Now().UTC().Add(-3 * time.Hour)
	expiredAt := staleAt.Add(time.Hour)
	staleReplica, err := stateStore.UpsertImageReplica(model.ImageReplica{
		ImageID:         image.ID,
		TenantID:        tenant.ID,
		AppID:           app.ID,
		Digest:          digest,
		NodeID:          "machine-1",
		RuntimeID:       "runtime_managed_shared",
		ClusterNodeName: "worker-1",
		CacheEndpoint:   "http://worker-1:5000",
		Status:          model.ImageReplicaStatusStale,
		LastVerifiedAt:  &staleAt,
		LeaseExpiresAt:  &expiredAt,
		LastError:       "verification expired",
	})
	if err != nil {
		t.Fatalf("create stale replica: %v", err)
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
		ImageRef:       managedRef,
		Repo:           "fugue-apps/repair",
		Target:         "git-current",
		Digest:         digest,
		MediaType:      "application/vnd.oci.image.manifest.v1+json",
		TotalBlobBytes: 700,
		LastSeenAt:     now,
		Present:        true,
	}}); err != nil {
		t.Fatalf("upsert fresh cache inventory: %v", err)
	}

	svc := &Service{
		Store: stateStore,
		Config: config.ControllerConfig{
			ImageStoreMode:           "distributed",
			ImageStoreMinReplicas:    1,
			ImageStoreTargetReplicas: 1,
			ImageCacheInventoryTTL:   2 * time.Hour,
		},
		registryPushBase: "registry.push.example",
		registryPullBase: "registry.pull.example",
		now:              func() time.Time { return now },
	}
	if err := svc.reconcileLegacyDistributedImageMetadata(context.Background()); err != nil {
		t.Fatalf("repair distributed image metadata: %v", err)
	}

	images, err := stateStore.ListImages(model.ImageFilter{TenantID: tenant.ID, AppID: app.ID})
	if err != nil {
		t.Fatalf("list images: %v", err)
	}
	if len(images) != 1 || images[0].ID != image.ID {
		t.Fatalf("repair must update the existing image without duplicating it: %+v", images)
	}
	if images[0].LifecycleState != model.ImageLifecycleAvailable || images[0].SourceOperationID != "op_original" {
		t.Fatalf("repair must restore availability while preserving provenance: %+v", images[0])
	}

	replicas, err := stateStore.ListImageReplicas(model.ImageReplicaFilter{ImageID: image.ID, TenantID: tenant.ID})
	if err != nil {
		t.Fatalf("list replicas: %v", err)
	}
	if len(replicas) != 1 || replicas[0].ID != staleReplica.ID || replicas[0].Status != model.ImageReplicaStatusPresent {
		t.Fatalf("repair must refresh the existing physical replica: %+v", replicas)
	}
	if replicas[0].LastVerifiedAt == nil || !replicas[0].LastVerifiedAt.Equal(now) ||
		replicas[0].LeaseExpiresAt == nil || !replicas[0].LeaseExpiresAt.Equal(now.Add(2*time.Hour)) ||
		replicas[0].LastError != "" {
		t.Fatalf("unexpected repaired replica evidence: %+v", replicas[0])
	}

	aliases, err := stateStore.ListImageAliases(model.ImageAliasFilter{ImageID: image.ID, TenantID: tenant.ID})
	if err != nil {
		t.Fatalf("list aliases: %v", err)
	}
	if !legacyBackfillHasAlias(aliases, managedRef) || !legacyBackfillHasAlias(aliases, runtimeRef) {
		t.Fatalf("repair must restore current managed and runtime aliases: %+v", aliases)
	}
	pins, err := stateStore.ListImagePins(model.ImagePinFilter{
		ImageID:  image.ID,
		TenantID: tenant.ID,
		AppID:    app.ID,
		Reason:   model.ImagePinReasonCurrentDeploy,
	})
	if err != nil {
		t.Fatalf("list current deploy pins: %v", err)
	}
	if len(pins) != 1 || pins[0].MinReplicas != 1 {
		t.Fatalf("repair must restore the current-deploy pin: %+v", pins)
	}
	locations, err := stateStore.ListImageLocations(model.ImageLocationFilter{
		TenantID: tenant.ID,
		ImageRef: images[0].ImageRef,
		Digest:   digest,
		Status:   model.ImageLocationStatusPresent,
	})
	if err != nil {
		t.Fatalf("list repaired image locations: %v", err)
	}
	if len(locations) != 1 || locations[0].ClusterNodeName != "worker-1" ||
		locations[0].LastSeenAt == nil || !locations[0].LastSeenAt.Equal(now) {
		t.Fatalf("repair must materialize fresh digest-bound location evidence: %+v", locations)
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
