package controller

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/store"
)

const integrityTestDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestReconcileDistributedImageIntegrityRepairsLegacyEmptyDigestRows(t *testing.T) {
	t.Parallel()

	storePath := filepath.Join(t.TempDir(), "store.json")
	stateStore := store.New(storePath)
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	image, err := stateStore.UpsertImage(model.Image{
		TenantID:        "tenant",
		AppID:           "app",
		ImageRef:        "registry.cache.example:5000/fugue-apps/demo:current",
		CanonicalDigest: integrityTestDigest,
		LifecycleState:  model.ImageLifecycleAvailable,
	})
	if err != nil {
		t.Fatalf("create importing image: %v", err)
	}
	replica, err := stateStore.UpsertImageReplica(model.ImageReplica{
		ImageID:         image.ID,
		TenantID:        image.TenantID,
		AppID:           image.AppID,
		NodeID:          "node-1",
		RuntimeID:       "runtime-1",
		ClusterNodeName: "worker-1",
		Status:          model.ImageReplicaStatusMissing,
	})
	if err != nil {
		t.Fatalf("create legacy replica fixture: %v", err)
	}
	location, err := stateStore.UpsertImageLocation(model.ImageLocation{
		TenantID:        image.TenantID,
		AppID:           image.AppID,
		ImageRef:        image.ImageRef,
		NodeID:          "node-1",
		RuntimeID:       "runtime-1",
		ClusterNodeName: "worker-1",
		CacheEndpoint:   "http://worker-1:5000",
		Status:          model.ImageLocationStatusPulling,
	})
	if err != nil {
		t.Fatalf("create legacy location fixture: %v", err)
	}
	now := time.Now().UTC()
	if _, err := stateStore.UpsertImageCacheInventory(model.ImageCacheNodeInventory{
		NodeID:           "node-1",
		RuntimeID:        "runtime-1",
		ClusterNodeName:  "worker-1",
		CacheEndpoint:    "http://worker-1:5000",
		ObservedAt:       now,
		SnapshotComplete: true,
	}, []model.ImageCacheManifest{{
		NodeID:          "node-1",
		RuntimeID:       "runtime-1",
		ClusterNodeName: "worker-1",
		ImageRef:        image.ImageRef,
		Repo:            "fugue-apps/demo",
		Target:          "current",
		Digest:          integrityTestDigest,
		ReferencedBlobs: []string{"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
		TotalBlobBytes:  100,
		LastSeenAt:      now,
		Present:         true,
	}}); err != nil {
		t.Fatalf("record complete cache evidence: %v", err)
	}

	// Simulate rows written by the pre-canonical-digest controller.  Production
	// repair must be able to read these rows even though new writes are guarded.
	raw, err := os.ReadFile(storePath)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	var state model.State
	if err := json.Unmarshal(raw, &state); err != nil {
		t.Fatalf("decode state: %v", err)
	}
	for index := range state.Images {
		if state.Images[index].ID == image.ID {
			state.Images[index].CanonicalDigest = ""
			state.Images[index].LifecycleState = model.ImageLifecycleAvailable
			state.Images[index].UpdatedAt = now
		}
	}
	for index := range state.ImageReplicas {
		if state.ImageReplicas[index].ID == replica.ID {
			state.ImageReplicas[index].Digest = ""
			state.ImageReplicas[index].Status = model.ImageReplicaStatusPresent
			state.ImageReplicas[index].UpdatedAt = now
		}
	}
	for index := range state.ImageLocations {
		if state.ImageLocations[index].ID == location.ID {
			state.ImageLocations[index].Digest = ""
			state.ImageLocations[index].Status = model.ImageLocationStatusPresent
			state.ImageLocations[index].LastSeenAt = &now
			state.ImageLocations[index].UpdatedAt = now
		}
	}
	encoded, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		t.Fatalf("encode legacy state: %v", err)
	}
	if err := os.WriteFile(storePath, encoded, 0o600); err != nil {
		t.Fatalf("write legacy state: %v", err)
	}

	svc := &Service{
		Store: stateStore,
		Config: config.ControllerConfig{
			ImageStoreMode:         "distributed",
			ImageCacheInventoryTTL: 2 * time.Hour,
		},
		now: func() time.Time { return now },
	}
	if err := svc.reconcileDistributedImageIntegrity(context.Background()); err != nil {
		t.Fatalf("reconcile image integrity: %v", err)
	}
	images, err := stateStore.ListImages(model.ImageFilter{AppID: image.AppID, PlatformAdmin: true})
	if err != nil {
		t.Fatalf("list repaired images: %v", err)
	}
	if len(images) != 1 || images[0].CanonicalDigest != integrityTestDigest || images[0].LifecycleState != model.ImageLifecycleAvailable {
		t.Fatalf("unexpected repaired image: %+v", images)
	}
	replicas, err := stateStore.ListImageReplicas(model.ImageReplicaFilter{ImageID: image.ID, PlatformAdmin: true})
	if err != nil {
		t.Fatalf("list repaired replicas: %v", err)
	}
	if len(replicas) != 1 || replicas[0].Status != model.ImageReplicaStatusPresent || replicas[0].Digest != integrityTestDigest {
		t.Fatalf("unexpected repaired replica: %+v", replicas)
	}
	if replicas[0].LastVerifiedAt == nil || !replicas[0].LastVerifiedAt.Equal(now) ||
		replicas[0].LeaseExpiresAt == nil || !replicas[0].LeaseExpiresAt.Equal(now.Add(2*time.Hour)) {
		t.Fatalf("repaired replica must carry bounded verification evidence: %+v", replicas[0])
	}
	locations, err := stateStore.ListImageLocations(model.ImageLocationFilter{AppID: image.AppID, PlatformAdmin: true})
	if err != nil {
		t.Fatalf("list repaired locations: %v", err)
	}
	missingLocations, err := stateStore.ListImageLocations(model.ImageLocationFilter{AppID: image.AppID, Status: model.ImageLocationStatusMissing, PlatformAdmin: true})
	if err != nil {
		t.Fatalf("list demoted locations: %v", err)
	}
	canonical := 0
	for _, candidate := range locations {
		if candidate.Digest == integrityTestDigest {
			canonical++
		}
	}
	if canonical != 1 || len(missingLocations) == 0 {
		t.Fatalf("expected canonical replacement and demoted legacy location, present=%+v missing=%+v", locations, missingLocations)
	}
}

func TestReconcileDistributedImageIntegrityDemotesLegacyDuplicateWhenCanonicalIdentityExists(t *testing.T) {
	t.Parallel()

	storePath := filepath.Join(t.TempDir(), "store.json")
	stateStore := store.New(storePath)
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	const legacySeedDigest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	imageRef := "registry.cache.example:5000/fugue-apps/demo:current"
	locationImageRef := "registry.cache.example:5000/fugue-apps/demo@" + integrityTestDigest
	legacy, err := stateStore.UpsertImage(model.Image{
		TenantID:        "tenant",
		AppID:           "app",
		ImageRef:        imageRef,
		CanonicalDigest: legacySeedDigest,
		LifecycleState:  model.ImageLifecycleAvailable,
	})
	if err != nil {
		t.Fatalf("create legacy image seed: %v", err)
	}
	legacyReplica, err := stateStore.UpsertImageReplica(model.ImageReplica{
		ImageID:         legacy.ID,
		TenantID:        legacy.TenantID,
		AppID:           legacy.AppID,
		Digest:          legacySeedDigest,
		NodeID:          "node-legacy",
		RuntimeID:       "runtime-legacy",
		ClusterNodeName: "worker-legacy",
		Status:          model.ImageReplicaStatusPresent,
	})
	if err != nil {
		t.Fatalf("create legacy replica seed: %v", err)
	}
	legacyAlias, err := stateStore.UpsertImageAlias(model.ImageAlias{
		ImageID:  legacy.ID,
		TenantID: legacy.TenantID,
		AliasRef: imageRef,
	})
	if err != nil {
		t.Fatalf("create legacy alias seed: %v", err)
	}
	canonical, err := stateStore.UpsertImage(model.Image{
		TenantID:        legacy.TenantID,
		AppID:           legacy.AppID,
		ImageRef:        imageRef,
		CanonicalDigest: integrityTestDigest,
		LifecycleState:  model.ImageLifecycleAvailable,
	})
	if err != nil {
		t.Fatalf("create existing canonical image: %v", err)
	}
	canonicalTagAlias, err := stateStore.UpsertImageAlias(model.ImageAlias{
		ImageID:  canonical.ID,
		TenantID: canonical.TenantID,
		AliasRef: imageRef,
		Digest:   integrityTestDigest,
	})
	if err != nil {
		t.Fatalf("create canonical tag alias seed: %v", err)
	}
	canonicalDigestAlias, err := stateStore.UpsertImageAlias(model.ImageAlias{
		ImageID:  canonical.ID,
		TenantID: canonical.TenantID,
		AliasRef: "registry.cache.example:5000/fugue-apps/demo@" + integrityTestDigest,
		Digest:   integrityTestDigest,
	})
	if err != nil {
		t.Fatalf("create canonical digest alias seed: %v", err)
	}
	now := time.Now().UTC()
	staleLocationSeenAt := now.Add(-3 * time.Hour)
	expiresAt := now.Add(time.Hour)
	legacyPin, err := stateStore.UpsertImagePin(model.ImagePin{
		ImageID:     legacy.ID,
		TenantID:    legacy.TenantID,
		AppID:       legacy.AppID,
		OperationID: "op-legacy",
		Reason:      model.ImagePinReasonCurrentDeploy,
		MinReplicas: 1,
		ExpiresAt:   &expiresAt,
	})
	if err != nil {
		t.Fatalf("create legacy pin seed: %v", err)
	}
	canonicalPin, err := stateStore.UpsertImagePin(model.ImagePin{
		ImageID:     canonical.ID,
		TenantID:    canonical.TenantID,
		AppID:       canonical.AppID,
		OperationID: "op-canonical",
		Reason:      model.ImagePinReasonCurrentDeploy,
		MinReplicas: 1,
		ExpiresAt:   &expiresAt,
	})
	if err != nil {
		t.Fatalf("create canonical pin seed: %v", err)
	}
	legacyLocation, err := stateStore.UpsertImageLocation(model.ImageLocation{
		TenantID:        legacy.TenantID,
		AppID:           legacy.AppID,
		ImageRef:        locationImageRef,
		Digest:          legacySeedDigest,
		NodeID:          "node-canonical",
		RuntimeID:       "runtime-canonical",
		ClusterNodeName: "worker-canonical",
		CacheEndpoint:   "http://worker-canonical:5000",
		Status:          model.ImageLocationStatusPresent,
		LastSeenAt:      &now,
	})
	if err != nil {
		t.Fatalf("create legacy location seed: %v", err)
	}
	canonicalLocation, err := stateStore.UpsertImageLocation(model.ImageLocation{
		TenantID:        canonical.TenantID,
		AppID:           "canonical-location-app",
		ImageRef:        locationImageRef,
		Digest:          integrityTestDigest,
		NodeID:          "node-canonical",
		RuntimeID:       "runtime-canonical",
		ClusterNodeName: "worker-canonical",
		CacheEndpoint:   "http://worker-canonical:5000",
		Status:          model.ImageLocationStatusPresent,
		LastSeenAt:      &now,
	})
	if err != nil {
		t.Fatalf("create canonical location seed: %v", err)
	}
	if legacyLocation.ID == canonicalLocation.ID {
		t.Fatalf("legacy and canonical location fixtures unexpectedly share ID %s", legacyLocation.ID)
	}
	if _, err := stateStore.UpsertImageCacheInventory(model.ImageCacheNodeInventory{
		NodeID:           "node-canonical",
		RuntimeID:        "runtime-canonical",
		ClusterNodeName:  "worker-canonical",
		ObservedAt:       now,
		SnapshotComplete: true,
	}, []model.ImageCacheManifest{{
		NodeID:          "node-canonical",
		RuntimeID:       "runtime-canonical",
		ClusterNodeName: "worker-canonical",
		ImageRef:        locationImageRef,
		Repo:            "fugue-apps/demo",
		Target:          "current",
		Digest:          integrityTestDigest,
		ReferencedBlobs: []string{"sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"},
		TotalBlobBytes:  100,
		LastSeenAt:      now,
		Present:         true,
	}}); err != nil {
		t.Fatalf("record canonical cache evidence: %v", err)
	}

	// Recreate the pre-enforcement duplicate: the legacy row has the same
	// tenant/ref as the canonical row but no digest, and its replica claimed
	// Present without content-addressed identity.
	raw, err := os.ReadFile(storePath)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	var state model.State
	if err := json.Unmarshal(raw, &state); err != nil {
		t.Fatalf("decode state: %v", err)
	}
	for index := range state.Images {
		if state.Images[index].ID == legacy.ID {
			state.Images[index].CanonicalDigest = ""
			state.Images[index].LifecycleState = model.ImageLifecycleAvailable
			state.Images[index].UpdatedAt = now
		}
	}
	for index := range state.ImageReplicas {
		if state.ImageReplicas[index].ID == legacyReplica.ID {
			state.ImageReplicas[index].Digest = ""
			state.ImageReplicas[index].Status = model.ImageReplicaStatusPresent
			state.ImageReplicas[index].LastVerifiedAt = nil
			state.ImageReplicas[index].LeaseExpiresAt = nil
		}
	}
	for index := range state.ImageLocations {
		if state.ImageLocations[index].ID == legacyLocation.ID {
			state.ImageLocations[index].Digest = ""
			state.ImageLocations[index].Status = model.ImageLocationStatusPresent
			state.ImageLocations[index].LastSeenAt = &staleLocationSeenAt
			state.ImageLocations[index].UpdatedAt = staleLocationSeenAt
		}
	}
	encoded, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		t.Fatalf("encode legacy duplicate state: %v", err)
	}
	if err := os.WriteFile(storePath, encoded, 0o600); err != nil {
		t.Fatalf("write legacy duplicate state: %v", err)
	}
	seededLocations, err := stateStore.ListImageLocations(model.ImageLocationFilter{
		ImageRef:      locationImageRef,
		Status:        model.ImageLocationStatusPresent,
		PlatformAdmin: true,
	})
	if err != nil {
		t.Fatalf("list seeded duplicate locations: %v", err)
	}
	if len(seededLocations) != 2 {
		t.Fatalf("duplicate location fixture count = %d, want 2: %+v", len(seededLocations), seededLocations)
	}

	svc := &Service{
		Store: stateStore,
		Config: config.ControllerConfig{
			ImageStoreMode:         "distributed",
			ImageCacheInventoryTTL: 2 * time.Hour,
		},
		now: func() time.Time { return now },
	}
	if err := svc.reconcileDistributedImageIntegrity(context.Background()); err != nil {
		t.Fatalf("reconcile duplicate image identity: %v", err)
	}
	refreshedCanonical, err := stateStore.GetImage(canonical.ID, "", true)
	if err != nil {
		t.Fatalf("get canonical image: %v", err)
	}
	if refreshedCanonical.CanonicalDigest != integrityTestDigest || refreshedCanonical.LifecycleState != model.ImageLifecycleAvailable {
		t.Fatalf("canonical image must remain available: %+v", refreshedCanonical)
	}
	refreshedLegacy, err := stateStore.GetImage(legacy.ID, "", true)
	if err != nil {
		t.Fatalf("get demoted legacy image: %v", err)
	}
	if refreshedLegacy.CanonicalDigest != "" || refreshedLegacy.LifecycleState != model.ImageLifecycleLost {
		t.Fatalf("legacy duplicate must be demoted without overwriting canonical identity: %+v", refreshedLegacy)
	}
	replicas, err := stateStore.ListImageReplicas(model.ImageReplicaFilter{ImageID: legacy.ID, PlatformAdmin: true})
	if err != nil {
		t.Fatalf("list legacy replicas: %v", err)
	}
	if len(replicas) != 1 || replicas[0].Status != model.ImageReplicaStatusMissing || replicas[0].Digest != "" {
		t.Fatalf("legacy duplicate replica must be demoted: %+v", replicas)
	}
	legacyAliases, err := stateStore.ListImageAliases(model.ImageAliasFilter{ImageID: legacy.ID, PlatformAdmin: true})
	if err != nil {
		t.Fatalf("list legacy aliases: %v", err)
	}
	if len(legacyAliases) != 1 || legacyAliases[0].ID != legacyAlias.ID || legacyAliases[0].AliasRef != imageRef {
		t.Fatalf("legacy alias changed during identity demotion: %+v", legacyAliases)
	}
	canonicalAliases, err := stateStore.ListImageAliases(model.ImageAliasFilter{ImageID: canonical.ID, PlatformAdmin: true})
	if err != nil {
		t.Fatalf("list canonical aliases: %v", err)
	}
	canonicalAliasIDs := map[string]bool{}
	for _, alias := range canonicalAliases {
		if alias.Digest != integrityTestDigest {
			t.Fatalf("canonical alias digest changed during identity demotion: %+v", alias)
		}
		canonicalAliasIDs[alias.ID] = true
	}
	if len(canonicalAliases) != 2 || !canonicalAliasIDs[canonicalTagAlias.ID] || !canonicalAliasIDs[canonicalDigestAlias.ID] {
		t.Fatalf("canonical aliases changed during identity demotion: %+v", canonicalAliases)
	}
	legacyPins, err := stateStore.ListImagePins(model.ImagePinFilter{ImageID: legacy.ID, PlatformAdmin: true})
	if err != nil {
		t.Fatalf("list legacy pins: %v", err)
	}
	if len(legacyPins) != 1 || legacyPins[0].ID != legacyPin.ID || legacyPins[0].Reason != model.ImagePinReasonCurrentDeploy {
		t.Fatalf("legacy pin changed during identity demotion: %+v", legacyPins)
	}
	canonicalPins, err := stateStore.ListImagePins(model.ImagePinFilter{ImageID: canonical.ID, PlatformAdmin: true})
	if err != nil {
		t.Fatalf("list canonical pins: %v", err)
	}
	if len(canonicalPins) != 1 || canonicalPins[0].ID != canonicalPin.ID || canonicalPins[0].Reason != model.ImagePinReasonCurrentDeploy {
		t.Fatalf("canonical pin changed during identity demotion: %+v", canonicalPins)
	}
	presentLocations, err := stateStore.ListImageLocations(model.ImageLocationFilter{
		ImageRef:      locationImageRef,
		Status:        model.ImageLocationStatusPresent,
		PlatformAdmin: true,
	})
	if err != nil {
		t.Fatalf("list present duplicate image locations: %v", err)
	}
	if len(presentLocations) != 1 || presentLocations[0].ID != canonicalLocation.ID ||
		presentLocations[0].AppID != canonicalLocation.AppID || presentLocations[0].Digest != integrityTestDigest ||
		presentLocations[0].Status != model.ImageLocationStatusPresent ||
		presentLocations[0].LastError != canonicalLocation.LastError ||
		!presentLocations[0].UpdatedAt.Equal(canonicalLocation.UpdatedAt) {
		t.Fatalf("canonical location changed during identity demotion: %+v", presentLocations)
	}
	missingLocations, err := stateStore.ListImageLocations(model.ImageLocationFilter{
		AppID:         legacy.AppID,
		Status:        model.ImageLocationStatusMissing,
		PlatformAdmin: true,
	})
	if err != nil {
		t.Fatalf("list missing duplicate image locations: %v", err)
	}
	if len(missingLocations) != 1 || missingLocations[0].ID != legacyLocation.ID ||
		missingLocations[0].Digest != "" || missingLocations[0].Status != model.ImageLocationStatusMissing ||
		missingLocations[0].LastError != "image location evidence is stale and has no canonical digest" ||
		missingLocations[0].LastSeenAt == nil || !missingLocations[0].LastSeenAt.Equal(now) {
		t.Fatalf("legacy tag-only location was not demoted: %+v", missingLocations)
	}
}

func TestDemoteLegacyImageLocationDefersConcurrentRefreshWithoutMaintenanceError(t *testing.T) {
	t.Parallel()

	storePath := filepath.Join(t.TempDir(), "store.json")
	stateStore := store.New(storePath)
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	location, err := stateStore.UpsertImageLocation(model.ImageLocation{
		TenantID:        "tenant",
		AppID:           "app",
		ImageRef:        "registry.cache.example:5000/fugue-apps/demo:current",
		NodeID:          "node",
		RuntimeID:       "runtime",
		ClusterNodeName: "worker",
		CacheEndpoint:   "http://worker:5000",
		Status:          model.ImageLocationStatusPulling,
	})
	if err != nil {
		t.Fatalf("create legacy location fixture: %v", err)
	}
	refreshedAt := location.UpdatedAt.Add(time.Second)
	raw, err := os.ReadFile(storePath)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	var state model.State
	if err := json.Unmarshal(raw, &state); err != nil {
		t.Fatalf("decode state: %v", err)
	}
	for index := range state.ImageLocations {
		if state.ImageLocations[index].ID == location.ID {
			state.ImageLocations[index].Status = model.ImageLocationStatusPresent
			state.ImageLocations[index].LastSeenAt = &refreshedAt
			state.ImageLocations[index].UpdatedAt = refreshedAt
		}
	}
	encoded, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		t.Fatalf("encode refreshed state: %v", err)
	}
	if err := os.WriteFile(storePath, encoded, 0o600); err != nil {
		t.Fatalf("write refreshed state: %v", err)
	}

	svc := &Service{Store: stateStore}
	if err := svc.demoteLegacyImageLocationIfUnchanged(
		location.ID,
		location.UpdatedAt,
		refreshedAt.Add(time.Second),
		"stale maintenance classification",
	); err != nil {
		t.Fatalf("concurrent refresh should defer without maintenance error: %v", err)
	}
	present, err := stateStore.ListImageLocations(model.ImageLocationFilter{
		NodeID:        location.NodeID,
		Status:        model.ImageLocationStatusPresent,
		PlatformAdmin: true,
	})
	if err != nil {
		t.Fatalf("list concurrently refreshed location: %v", err)
	}
	if len(present) != 1 || present[0].ID != location.ID || present[0].Digest != "" ||
		!present[0].UpdatedAt.Equal(refreshedAt) {
		t.Fatalf("concurrently refreshed location was changed: %+v", present)
	}
}

func TestDistributedImageIdentityExistsRequiresAvailableCanonicalPeer(t *testing.T) {
	t.Parallel()

	const digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	legacy := model.Image{
		ID:             "legacy",
		TenantID:       "tenant",
		ImageRef:       "registry.cache.example:5000/fugue-apps/demo:current",
		LifecycleState: model.ImageLifecycleAvailable,
	}
	for _, lifecycle := range []string{
		model.ImageLifecycleAvailable,
		model.ImageLifecycleLost,
		model.ImageLifecycleDeleting,
		model.ImageLifecycleDeleted,
	} {
		lifecycle := lifecycle
		t.Run(lifecycle, func(t *testing.T) {
			peer := model.Image{
				ID:              "canonical",
				TenantID:        legacy.TenantID,
				ImageRef:        legacy.ImageRef,
				CanonicalDigest: digest,
				LifecycleState:  lifecycle,
			}
			got := distributedImageIdentityExists([]model.Image{legacy, peer}, legacy, digest)
			want := lifecycle == model.ImageLifecycleAvailable
			if got != want {
				t.Fatalf("identity exists for lifecycle %q = %t, want %t", lifecycle, got, want)
			}
		})
	}
}

func TestReconcileDistributedImageIntegrityRefusesToDemoteLegacyForNonServingCanonicalPeer(t *testing.T) {
	t.Parallel()
	for _, lifecycle := range []string{
		model.ImageLifecycleLost,
		model.ImageLifecycleDeleting,
		model.ImageLifecycleDeleted,
	} {
		lifecycle := lifecycle
		t.Run(lifecycle, func(t *testing.T) {
			assertReconcileRefusesLegacyDemotionForNonServingPeer(t, lifecycle)
		})
	}
}

func assertReconcileRefusesLegacyDemotionForNonServingPeer(t *testing.T, lifecycle string) {
	t.Helper()
	storePath := filepath.Join(t.TempDir(), "store.json")
	stateStore := store.New(storePath)
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	const legacySeedDigest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	imageRef := "registry.cache.example:5000/fugue-apps/demo:current"
	legacy, err := stateStore.UpsertImage(model.Image{
		TenantID:        "tenant",
		AppID:           "app",
		ImageRef:        imageRef,
		CanonicalDigest: legacySeedDigest,
		LifecycleState:  model.ImageLifecycleAvailable,
	})
	if err != nil {
		t.Fatalf("create legacy seed: %v", err)
	}
	canonical, err := stateStore.UpsertImage(model.Image{
		TenantID:        legacy.TenantID,
		AppID:           legacy.AppID,
		ImageRef:        imageRef,
		CanonicalDigest: integrityTestDigest,
		LifecycleState:  lifecycle,
	})
	if err != nil {
		t.Fatalf("create lost canonical peer: %v", err)
	}
	now := time.Now().UTC()
	if _, err := stateStore.UpsertImageCacheInventory(model.ImageCacheNodeInventory{
		NodeID:           "node-canonical",
		RuntimeID:        "runtime-canonical",
		ClusterNodeName:  "worker-canonical",
		ObservedAt:       now,
		SnapshotComplete: true,
	}, []model.ImageCacheManifest{{
		NodeID:          "node-canonical",
		RuntimeID:       "runtime-canonical",
		ClusterNodeName: "worker-canonical",
		ImageRef:        imageRef,
		Repo:            "fugue-apps/demo",
		Target:          "current",
		Digest:          integrityTestDigest,
		ReferencedBlobs: []string{"sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"},
		TotalBlobBytes:  100,
		LastSeenAt:      now,
		Present:         true,
	}}); err != nil {
		t.Fatalf("record canonical cache evidence: %v", err)
	}

	raw, err := os.ReadFile(storePath)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	var state model.State
	if err := json.Unmarshal(raw, &state); err != nil {
		t.Fatalf("decode state: %v", err)
	}
	for index := range state.Images {
		if state.Images[index].ID == legacy.ID {
			state.Images[index].CanonicalDigest = ""
			state.Images[index].LifecycleState = model.ImageLifecycleAvailable
			state.Images[index].UpdatedAt = now
		}
	}
	encoded, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		t.Fatalf("encode duplicate state: %v", err)
	}
	if err := os.WriteFile(storePath, encoded, 0o600); err != nil {
		t.Fatalf("write duplicate state: %v", err)
	}

	svc := &Service{
		Store: stateStore,
		Config: config.ControllerConfig{
			ImageStoreMode:         "distributed",
			ImageCacheInventoryTTL: 2 * time.Hour,
		},
		now: func() time.Time { return now },
	}
	if err := svc.reconcileDistributedImageIntegrity(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "backfill canonical digest") {
		t.Fatalf("expected lost canonical peer to block legacy demotion, got %v", err)
	}
	refreshedLegacy, err := stateStore.GetImage(legacy.ID, "", true)
	if err != nil {
		t.Fatalf("get legacy image: %v", err)
	}
	if refreshedLegacy.CanonicalDigest != "" || refreshedLegacy.LifecycleState != model.ImageLifecycleAvailable {
		t.Fatalf("legacy image changed without an available canonical peer: %+v", refreshedLegacy)
	}
	refreshedCanonical, err := stateStore.GetImage(canonical.ID, "", true)
	if err != nil {
		t.Fatalf("get canonical image: %v", err)
	}
	if refreshedCanonical.CanonicalDigest != integrityTestDigest || refreshedCanonical.LifecycleState != lifecycle {
		t.Fatalf("non-serving canonical peer changed unexpectedly: %+v", refreshedCanonical)
	}
}

func TestReconcileDistributedImageIntegrityDemotesAmbiguousEvidence(t *testing.T) {
	t.Parallel()

	storePath := filepath.Join(t.TempDir(), "store.json")
	stateStore := store.New(storePath)
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	image, err := stateStore.UpsertImage(model.Image{
		TenantID:        "tenant",
		AppID:           "app",
		ImageRef:        "registry.cache.example:5000/fugue-apps/demo:current",
		CanonicalDigest: integrityTestDigest,
		LifecycleState:  model.ImageLifecycleAvailable,
	})
	if err != nil {
		t.Fatalf("create importing image: %v", err)
	}
	now := time.Now().UTC()
	for _, digest := range []string{
		integrityTestDigest,
		"sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
	} {
		if _, err := stateStore.UpsertImageCacheInventory(model.ImageCacheNodeInventory{
			NodeID:          digest,
			ClusterNodeName: digest,
			ObservedAt:      now,
		}, []model.ImageCacheManifest{{
			ImageRef:        image.ImageRef,
			Repo:            "fugue-apps/demo",
			Target:          "current",
			Digest:          digest,
			ReferencedBlobs: []string{"sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"},
			TotalBlobBytes:  10,
			LastSeenAt:      now,
			Present:         true,
		}}); err != nil {
			t.Fatalf("record ambiguous evidence: %v", err)
		}
	}
	raw, err := os.ReadFile(storePath)
	if err != nil {
		t.Fatalf("read ambiguous state: %v", err)
	}
	var legacyState model.State
	if err := json.Unmarshal(raw, &legacyState); err != nil {
		t.Fatalf("decode ambiguous state: %v", err)
	}
	for index := range legacyState.Images {
		if legacyState.Images[index].ID == image.ID {
			legacyState.Images[index].CanonicalDigest = ""
			legacyState.Images[index].LifecycleState = model.ImageLifecycleAvailable
			legacyState.Images[index].UpdatedAt = now
		}
	}
	encoded, err := json.MarshalIndent(legacyState, "", "  ")
	if err != nil {
		t.Fatalf("encode ambiguous state: %v", err)
	}
	if err := os.WriteFile(storePath, encoded, 0o600); err != nil {
		t.Fatalf("write ambiguous state: %v", err)
	}
	svc := &Service{
		Store: stateStore,
		Config: config.ControllerConfig{
			ImageStoreMode:         "distributed",
			ImageCacheInventoryTTL: 2 * time.Hour,
		},
		now: func() time.Time { return now },
	}
	if err := svc.reconcileDistributedImageIntegrity(context.Background()); err != nil {
		t.Fatalf("reconcile ambiguous image: %v", err)
	}
	refreshed, err := stateStore.GetImage(image.ID, "", true)
	if err != nil {
		t.Fatalf("get demoted image: %v", err)
	}
	if refreshed.LifecycleState != model.ImageLifecycleLost || refreshed.CanonicalDigest != "" {
		t.Fatalf("ambiguous image must be demoted without guessed digest: %+v", refreshed)
	}
}

func TestReconcileDistributedImageIntegrityDoesNotPromoteReplicaFromAnotherNodeEvidence(t *testing.T) {
	t.Parallel()

	storePath := filepath.Join(t.TempDir(), "store.json")
	stateStore := store.New(storePath)
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	image, err := stateStore.UpsertImage(model.Image{
		TenantID:        "tenant",
		AppID:           "app",
		ImageRef:        "registry.cache.example:5000/fugue-apps/demo:current",
		CanonicalDigest: integrityTestDigest,
		LifecycleState:  model.ImageLifecycleAvailable,
	})
	if err != nil {
		t.Fatalf("create image: %v", err)
	}
	replica, err := stateStore.UpsertImageReplica(model.ImageReplica{
		ImageID:         image.ID,
		TenantID:        image.TenantID,
		AppID:           image.AppID,
		NodeID:          "node-a",
		RuntimeID:       "runtime-a",
		ClusterNodeName: "worker-a",
		Status:          model.ImageReplicaStatusMissing,
	})
	if err != nil {
		t.Fatalf("create legacy replica fixture: %v", err)
	}

	now := time.Now().UTC()
	if _, err := stateStore.UpsertImageCacheInventory(model.ImageCacheNodeInventory{
		NodeID:           "node-b",
		RuntimeID:        "runtime-b",
		ClusterNodeName:  "worker-b",
		ObservedAt:       now,
		SnapshotComplete: true,
	}, []model.ImageCacheManifest{{
		NodeID:          "node-b",
		RuntimeID:       "runtime-b",
		ClusterNodeName: "worker-b",
		ImageRef:        image.ImageRef,
		Repo:            "fugue-apps/demo",
		Target:          "current",
		Digest:          integrityTestDigest,
		ReferencedBlobs: []string{"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
		TotalBlobBytes:  100,
		LastSeenAt:      now,
		Present:         true,
	}}); err != nil {
		t.Fatalf("record other-node cache evidence: %v", err)
	}

	// Simulate a legacy row that claimed Present before replica digests were
	// mandatory. The only fresh physical proof is deliberately on node B.
	raw, err := os.ReadFile(storePath)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	var legacyState model.State
	if err := json.Unmarshal(raw, &legacyState); err != nil {
		t.Fatalf("decode state: %v", err)
	}
	for index := range legacyState.ImageReplicas {
		if legacyState.ImageReplicas[index].ID == replica.ID {
			legacyState.ImageReplicas[index].Status = model.ImageReplicaStatusPresent
			legacyState.ImageReplicas[index].Digest = ""
			legacyState.ImageReplicas[index].LastVerifiedAt = nil
			legacyState.ImageReplicas[index].LeaseExpiresAt = nil
		}
	}
	encoded, err := json.MarshalIndent(legacyState, "", "  ")
	if err != nil {
		t.Fatalf("encode legacy state: %v", err)
	}
	if err := os.WriteFile(storePath, encoded, 0o600); err != nil {
		t.Fatalf("write legacy state: %v", err)
	}

	svc := &Service{
		Store: stateStore,
		Config: config.ControllerConfig{
			ImageStoreMode:         "distributed",
			ImageCacheInventoryTTL: 2 * time.Hour,
		},
		now: func() time.Time { return now },
	}
	if err := svc.reconcileDistributedImageIntegrity(context.Background()); err != nil {
		t.Fatalf("reconcile image integrity: %v", err)
	}
	replicas, err := stateStore.ListImageReplicas(model.ImageReplicaFilter{ImageID: image.ID, PlatformAdmin: true})
	if err != nil {
		t.Fatalf("list replicas: %v", err)
	}
	if len(replicas) != 1 || replicas[0].Status != model.ImageReplicaStatusMissing || replicas[0].Digest != "" {
		t.Fatalf("other-node evidence must not promote the legacy replica: %+v", replicas)
	}
}

func TestImageIntegrityManifestForReplicaRequiresExactFreshCompleteProof(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	cutoff := now.Add(-2 * time.Hour)
	image := model.Image{
		ImageRef:        "registry.cache.example:5000/fugue-apps/demo:current",
		CanonicalDigest: integrityTestDigest,
	}
	replica := model.ImageReplica{
		NodeID:          "node-a",
		RuntimeID:       "runtime-a",
		ClusterNodeName: "worker-a",
	}
	valid := model.ImageCacheManifest{
		NodeID:          replica.NodeID,
		RuntimeID:       replica.RuntimeID,
		ClusterNodeName: replica.ClusterNodeName,
		ImageRef:        image.ImageRef,
		Repo:            "fugue-apps/demo",
		Target:          "current",
		Digest:          integrityTestDigest,
		ReferencedBlobs: []string{"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
		TotalBlobBytes:  100,
		LastSeenAt:      now,
		Present:         true,
	}

	tests := []struct {
		name   string
		mutate func(*model.ImageCacheManifest)
		want   bool
	}{
		{name: "exact proof", want: true},
		{name: "other node", mutate: func(manifest *model.ImageCacheManifest) { manifest.NodeID = "node-b" }},
		{name: "missing target identity", mutate: func(manifest *model.ImageCacheManifest) { manifest.NodeID = "" }},
		{name: "wrong image reference", mutate: func(manifest *model.ImageCacheManifest) {
			manifest.ImageRef = "registry.cache.example:5000/fugue-apps/other:current"
			manifest.Repo = "fugue-apps/other"
			manifest.Target = "current"
		}},
		{name: "wrong digest", mutate: func(manifest *model.ImageCacheManifest) {
			manifest.Digest = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
		}},
		{name: "incomplete blob graph", mutate: func(manifest *model.ImageCacheManifest) { manifest.ReferencedBlobs = nil }},
		{name: "stale inventory", mutate: func(manifest *model.ImageCacheManifest) { manifest.LastSeenAt = cutoff.Add(-time.Second) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manifest := valid
			if tt.mutate != nil {
				tt.mutate(&manifest)
			}
			_, got := imageIntegrityManifestForReplica(image, replica, []model.ImageCacheManifest{manifest}, cutoff)
			if got != tt.want {
				t.Fatalf("proof=%v, want %v: %+v", got, tt.want, manifest)
			}
		})
	}
}

func TestIntegrityManifestRepositoryMatchesTaggedDigestReference(t *testing.T) {
	t.Parallel()

	imageRef := "registry.cache.example:5000/fugue-apps/demo:build-a@" + integrityTestDigest
	manifest := model.ImageCacheManifest{Repo: "fugue-apps/demo"}
	if !integrityManifestMatchesImageRepository(imageRef, manifest) {
		t.Fatalf("expected tagged digest reference %q to match manifest repository %q", imageRef, manifest.Repo)
	}
}
