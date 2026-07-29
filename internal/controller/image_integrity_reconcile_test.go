package controller

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
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
