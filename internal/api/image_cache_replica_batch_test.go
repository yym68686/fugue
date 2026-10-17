package api

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

func replicaProtectionFixture(tb testing.TB, count int) (*Server, []model.Image, []model.ImageReplica) {
	tb.Helper()
	now := time.Now().UTC()
	future, expired := now.Add(time.Hour), now.Add(-time.Hour)
	var images []model.Image
	var replicas []model.ImageReplica
	for i := 0; i < count; i++ {
		digest := fmt.Sprintf("sha256:%064x", i+1)
		image := model.Image{ID: fmt.Sprintf("image-%d", i), TenantID: fmt.Sprintf("tenant-%d", i%2), ImageRef: fmt.Sprintf("registry.example/image-%d@%s", i, digest), CanonicalDigest: digest, LifecycleState: model.ImageLifecycleAvailable}
		if i%3 == 1 {
			image.LifecycleState = model.ImageLifecycleDeleted
		}
		if i%3 == 2 {
			image.LifecycleState = model.ImageLifecycleImporting
		}
		images = append(images, image)
		for j, status := range []string{model.ImageReplicaStatusPresent, model.ImageReplicaStatusPresent, model.ImageReplicaStatusStale, model.ImageReplicaStatusFailed, model.ImageReplicaStatusMissing, model.ImageReplicaStatusPresent} {
			replica := model.ImageReplica{ID: fmt.Sprintf("replica-%d-%d", i, j), ImageID: image.ID, TenantID: image.TenantID, Digest: digest, NodeID: fmt.Sprintf("node-%d", j), ClusterNodeName: fmt.Sprintf("worker-%d", j), RuntimeID: fmt.Sprintf("runtime-%d", j), Status: status, UpdatedAt: now.Add(-time.Duration(j) * time.Minute), LeaseExpiresAt: &future}
			if j == 5 {
				replica.LeaseExpiresAt = &expired
			}
			replicas = append(replicas, replica)
		}
	}
	// A replica belonging to an image outside the input must not affect protection.
	replicas = append(replicas, model.ImageReplica{ID: "unrelated", ImageID: "unrelated-image", Status: model.ImageReplicaStatusPresent, Digest: "sha256:" + strings.Repeat("f", 64), LeaseExpiresAt: &future, UpdatedAt: now})
	path := filepath.Join(tb.TempDir(), "state.json")
	raw, err := json.Marshal(model.State{Images: images, ImageReplicas: replicas})
	if err != nil {
		tb.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		tb.Fatal(err)
	}
	return &Server{store: store.New(path)}, images, replicas
}

func newReplicaProtectionSet() imageCacheProtectedSet {
	return imageCacheProtectedSet{
		minReplicaRefs: map[string]struct{}{}, imageIDsByRef: map[string][]string{}, replicaIDsByRef: map[string][]string{},
		minReplicaKeeperRefs: map[string][]imageCacheReplicaCandidate{}, replicaCandidateRefs: map[string][]imageCacheReplicaCandidate{},
	}
}

func TestImageCacheReplicaProtectionKeepsEligibilityAndNodeBindings(t *testing.T) {
	server, images, _ := replicaProtectionFixture(t, 3)
	protected := newReplicaProtectionSet()
	if err := server.populateImageCacheReplicaCandidateRefs(&protected, images); err != nil {
		t.Fatal(err)
	}
	if err := server.populateImageCacheMinimumReplicaRefs(&protected, images); err != nil {
		t.Fatal(err)
	}
	for i, image := range images {
		keys := exactImageReferenceKeys(image.ImageRef, image.CanonicalDigest)
		for _, key := range keys {
			got := protected.replicaCandidateRefs[key]
			want := []imageCacheReplicaCandidate{}
			for j := 2; j <= 4; j++ {
				want = append(want, imageCacheReplicaCandidate{NodeID: fmt.Sprintf("node-%d", j), RuntimeID: fmt.Sprintf("runtime-%d", j), ClusterNodeName: fmt.Sprintf("worker-%d", j), Reason: "stale_replica", ReplicaID: fmt.Sprintf("replica-%d-%d", i, j)})
			}
			if image.LifecycleState != model.ImageLifecycleDeleted {
				want = append(want, imageCacheReplicaCandidate{NodeID: "node-1", RuntimeID: "runtime-1", ClusterNodeName: "worker-1", Reason: "excess_replica", ReplicaID: fmt.Sprintf("replica-%d-1", i)})
				keeper := protected.minReplicaKeeperRefs[key]
				if len(keeper) != 1 || keeper[0].ReplicaID != fmt.Sprintf("replica-%d-0", i) || keeper[0].NodeID != "node-0" {
					t.Errorf("keeper changed: %+v", keeper)
				}
			} else if len(protected.minReplicaKeeperRefs[key]) != 0 {
				t.Error("deleted image gained minimum keeper")
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("candidate identity/order changed for %s: got %+v want %+v", key, got, want)
			}
		}
	}
	// The minimum-replica pass must fetch current state again, not reuse the
	// earlier candidate pass. A disappearing replica must protect the image.
	if _, err := server.store.MarkStaleImageReplicas(time.Now().Add(2*time.Hour), time.Now()); err != nil {
		t.Fatal(err)
	}
	fresh := newReplicaProtectionSet()
	if err := server.populateImageCacheMinimumReplicaRefs(&fresh, images); err != nil {
		t.Fatal(err)
	}
	if !keySetContainsAny(fresh.minReplicaRefs, exactImageReferenceKeys(images[0].ImageRef, images[0].CanonicalDigest)...) {
		t.Fatal("minimum-replica check reused stale replicas")
	}
}

func TestImageCacheReplicaProtectionPropagatesReadErrors(t *testing.T) {
	server := &Server{store: store.New(t.TempDir())}
	images := []model.Image{{ID: "image", LifecycleState: model.ImageLifecycleAvailable}}
	for _, run := range []func(*imageCacheProtectedSet, []model.Image) error{server.populateImageCacheReplicaCandidateRefs, server.populateImageCacheMinimumReplicaRefs} {
		protected := newReplicaProtectionSet()
		if err := run(&protected, images); err == nil {
			t.Fatal("read failure became an empty, unprotected replica set")
		}
		if err := run(&protected, nil); err != nil {
			t.Fatal("empty image list unexpectedly reads storage", err)
		}
	}
}

func BenchmarkImageCacheReplicaProtection(b *testing.B) {
	for _, count := range []int{32, 128} {
		b.Run(fmt.Sprintf("images-%d", count), func(b *testing.B) {
			server, images, _ := replicaProtectionFixture(b, count)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				protected := newReplicaProtectionSet()
				if err := server.populateImageCacheReplicaCandidateRefs(&protected, images); err != nil {
					b.Fatal(err)
				}
				if err := server.populateImageCacheMinimumReplicaRefs(&protected, images); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
