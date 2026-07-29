package store

import (
	"errors"
	"path/filepath"
	"testing"

	"fugue/internal/model"
)

const testCanonicalImageDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestImageStoreDoesNotAdvertiseAvailableWithoutCanonicalDigest(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	if _, err := s.UpsertImage(model.Image{
		TenantID:        "tenant",
		ImageRef:        "registry.example/app:tag",
		CanonicalDigest: "",
		LifecycleState:  model.ImageLifecycleAvailable,
	}); !errors.Is(err, ErrCanonicalDigestRequired) {
		t.Fatalf("expected canonical digest validation error, got %v", err)
	}
	image, err := s.UpsertImage(model.Image{
		TenantID: "tenant",
		ImageRef: "registry.example/app:tag",
	})
	if err != nil {
		t.Fatalf("implicit importing image should be accepted: %v", err)
	}
	if image.LifecycleState != model.ImageLifecycleImporting {
		t.Fatalf("expected digest-less image to remain importing, got %+v", image)
	}
}

func TestImageStorePresentReplicaRequiresCanonicalDigest(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	image, err := s.UpsertImage(model.Image{
		TenantID:        "tenant",
		ImageRef:        "registry.example/app:tag",
		CanonicalDigest: testCanonicalImageDigest,
		LifecycleState:  model.ImageLifecycleAvailable,
	})
	if err != nil {
		t.Fatalf("create image: %v", err)
	}
	if _, err := s.UpsertImageReplica(model.ImageReplica{
		ImageID:  image.ID,
		TenantID: image.TenantID,
		Status:   model.ImageReplicaStatusPresent,
		Digest:   "sha256:not-a-real-canonical-digest",
	}); !errors.Is(err, ErrCanonicalDigestRequired) {
		t.Fatalf("expected invalid replica digest to be rejected, got %v", err)
	}
	// A report that omits the digest is enriched from the verified image
	// identity; the resulting Present record is still content-addressed.
	replica, err := s.UpsertImageReplica(model.ImageReplica{
		ImageID:  image.ID,
		TenantID: image.TenantID,
		Status:   model.ImageReplicaStatusPresent,
	})
	if err != nil {
		t.Fatalf("enrich replica digest: %v", err)
	}
	if replica.Digest != testCanonicalImageDigest {
		t.Fatalf("expected canonical replica digest, got %+v", replica)
	}
}
