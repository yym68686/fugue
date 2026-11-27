package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
)

const integrityTestDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

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
	const fixture = "image-integrity"
	stateStore := newStoreTestDatabase(t, fixture)
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("image-integrity")
	if err != nil {
		t.Fatal(err)
	}
	imageRef := "registry.cache.example:5000/fugue-apps/demo:current"
	const seedDigest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	legacy, err := stateStore.UpsertImage(model.Image{
		TenantID:        tenant.ID,
		AppID:           "app",
		ImageRef:        imageRef,
		CanonicalDigest: seedDigest,
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
	// Migration-era rows predate the digest requirement for available images.
	if _, err := openStoreTestSQL(t, fixture).Exec(`UPDATE fugue_images SET canonical_digest = '', updated_at = $2 WHERE id = $1`, legacy.ID, now); err != nil {
		t.Fatal(err)
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
