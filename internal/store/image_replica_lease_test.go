package store

import (
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestMarkStaleImageReplicasHonorsLeaseBeforeVerifyFallback(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	image, err := s.UpsertImage(model.Image{
		TenantID:        "tenant_lease",
		AppID:           "app_lease",
		ImageRef:        "registry.example/fugue-apps/lease:current",
		CanonicalDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		LifecycleState:  model.ImageLifecycleAvailable,
	})
	if err != nil {
		t.Fatalf("upsert image: %v", err)
	}
	now := time.Now().UTC()
	verifiedAt := now.Add(-time.Hour)
	leaseExpiresAt := now.Add(time.Hour)
	if _, err := s.UpsertImageReplica(model.ImageReplica{
		ImageID:         image.ID,
		TenantID:        image.TenantID,
		AppID:           image.AppID,
		Digest:          image.CanonicalDigest,
		ClusterNodeName: "worker-lease",
		Status:          model.ImageReplicaStatusPresent,
		LastVerifiedAt:  &verifiedAt,
		LeaseExpiresAt:  &leaseExpiresAt,
	}); err != nil {
		t.Fatalf("upsert leased replica: %v", err)
	}

	stale, err := s.MarkStaleImageReplicas(now, now.Add(-10*time.Minute))
	if err != nil {
		t.Fatalf("mark stale before lease expiry: %v", err)
	}
	if stale != 0 {
		t.Fatalf("valid lease must prevent staleness, got %d", stale)
	}
	stale, err = s.MarkStaleImageReplicas(now.Add(2*time.Hour), now.Add(2*time.Hour-10*time.Minute))
	if err != nil {
		t.Fatalf("mark stale after lease expiry: %v", err)
	}
	if stale != 1 {
		t.Fatalf("expired lease should become stale, got %d", stale)
	}
}
