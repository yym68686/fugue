package store

import (
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestImageCacheInventoryUpsertAndStaleFilters(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	oldSeen := time.Now().UTC().Add(-3 * time.Hour)
	newSeen := time.Now().UTC()
	node := model.ImageCacheNodeInventory{
		NodeID:          "machine-1",
		ClusterNodeName: "worker-1",
		RuntimeID:       "runtime-1",
		CacheEndpoint:   "http://worker-1:5000",
		CacheBytes:      123,
		ManifestCount:   2,
		ObservedAt:      newSeen,
		Status:          "reported",
	}
	if _, err := s.UpsertImageCacheInventory(node, []model.ImageCacheManifest{
		{Repo: "fugue-apps/demo", Target: "old", Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", LastSeenAt: oldSeen, Present: true},
		{Repo: "fugue-apps/demo", Target: "new", Digest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", LastSeenAt: newSeen, TotalBlobBytes: 10, Present: true},
	}); err != nil {
		t.Fatalf("upsert inventory: %v", err)
	}
	if _, err := s.UpsertImageCacheInventory(node, []model.ImageCacheManifest{
		{Repo: "fugue-apps/demo", Target: "new", Digest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", LastSeenAt: newSeen.Add(time.Minute), TotalBlobBytes: 20, Present: true},
	}); err != nil {
		t.Fatalf("upsert inventory replacement: %v", err)
	}

	nodes, err := s.ListImageCacheNodeInventories(model.ImageCacheNodeInventoryFilter{ClusterNodeName: "worker-1", StaleAfter: time.Now().UTC().Add(-time.Hour)})
	if err != nil {
		t.Fatalf("list nodes: %v", err)
	}
	if len(nodes) != 1 || nodes[0].ManifestCount != 2 {
		t.Fatalf("unexpected nodes: %+v", nodes)
	}
	manifests, err := s.ListImageCacheManifests(model.ImageCacheManifestFilter{ClusterNodeName: "worker-1", SeenAfter: time.Now().UTC().Add(-time.Hour), PresentOnly: true})
	if err != nil {
		t.Fatalf("list manifests: %v", err)
	}
	if len(manifests) != 1 || manifests[0].Target != "new" || manifests[0].TotalBlobBytes != 20 {
		t.Fatalf("unexpected fresh manifests: %+v", manifests)
	}
}

func TestImageCacheInventoryCompleteSnapshotMarksMissingManifestsAbsent(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	firstSeen := time.Now().UTC().Add(-time.Hour)
	secondSeen := time.Now().UTC()
	node := model.ImageCacheNodeInventory{
		NodeID:           "machine-1",
		ClusterNodeName:  "worker-1",
		RuntimeID:        "runtime-1",
		ManifestCount:    3,
		ObservedAt:       firstSeen,
		Status:           "reported",
		SnapshotComplete: true,
	}
	if _, err := s.UpsertImageCacheInventory(node, []model.ImageCacheManifest{
		{Repo: "fugue-apps/demo", Target: "keep-a", Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Present: true},
		{Repo: "fugue-apps/demo", Target: "keep-b", Digest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Present: true},
		{Repo: "fugue-apps/demo", Target: "gone", Digest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", Present: true},
	}); err != nil {
		t.Fatalf("upsert first inventory: %v", err)
	}

	chunkNode := node
	chunkNode.ManifestCount = 2
	chunkNode.ObservedAt = secondSeen
	chunkNode.SnapshotComplete = false
	if _, err := s.UpsertImageCacheInventory(chunkNode, []model.ImageCacheManifest{
		{Repo: "fugue-apps/demo", Target: "keep-a", Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Present: true},
	}); err != nil {
		t.Fatalf("upsert first chunk: %v", err)
	}
	manifests, err := s.ListImageCacheManifests(model.ImageCacheManifestFilter{ClusterNodeName: "worker-1", PresentOnly: true})
	if err != nil {
		t.Fatalf("list first chunk manifests: %v", err)
	}
	if len(manifests) != 3 {
		t.Fatalf("first chunk should not mark old manifests absent, got %+v", manifests)
	}

	finalChunkNode := chunkNode
	finalChunkNode.SnapshotComplete = true
	if _, err := s.UpsertImageCacheInventory(finalChunkNode, []model.ImageCacheManifest{
		{Repo: "fugue-apps/demo", Target: "keep-b", Digest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Present: true},
	}); err != nil {
		t.Fatalf("upsert final chunk: %v", err)
	}
	manifests, err = s.ListImageCacheManifests(model.ImageCacheManifestFilter{ClusterNodeName: "worker-1", PresentOnly: true})
	if err != nil {
		t.Fatalf("list final manifests: %v", err)
	}
	if len(manifests) != 2 {
		t.Fatalf("expected only current snapshot manifests present, got %+v", manifests)
	}
	all, err := s.ListImageCacheManifests(model.ImageCacheManifestFilter{ClusterNodeName: "worker-1"})
	if err != nil {
		t.Fatalf("list all manifests: %v", err)
	}
	for _, manifest := range all {
		if manifest.Target == "gone" && manifest.Present {
			t.Fatalf("missing manifest still marked present: %+v", manifest)
		}
	}
}

func TestLocalPVInventoryUpsertReplacesNodeSnapshot(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	first, err := s.UpsertLocalPVInventory(model.LocalPVInventory{
		NodeID:             "machine-1",
		ClusterNodeName:    "worker-1",
		VGName:             "fugue-vg",
		ImagePath:          "/var/lib/fugue/lvm-localpv/fugue-vg.img",
		ImageSizeBytes:     10,
		LVCount:            1,
		ActiveLVCount:      1,
		BoundPVCount:       1,
		SafeToDecommission: false,
		UnsafeReasons:      []string{"active_lvs_present"},
		ObservedAt:         time.Now().UTC().Add(-time.Minute),
	})
	if err != nil {
		t.Fatalf("upsert first inventory: %v", err)
	}
	second, err := s.UpsertLocalPVInventory(model.LocalPVInventory{
		NodeID:             "machine-1",
		ClusterNodeName:    "worker-1",
		VGName:             "fugue-vg",
		ImagePath:          "/var/lib/fugue/lvm-localpv/fugue-vg.img",
		ImageSizeBytes:     10,
		LoopDevice:         "/dev/loop9",
		LoopBackingFile:    "/var/lib/fugue/lvm-localpv/fugue-vg.img",
		LVCount:            0,
		ActiveLVCount:      0,
		BoundPVCount:       0,
		SafeToDecommission: true,
		ObservedAt:         time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("upsert second inventory: %v", err)
	}
	if first.ID != second.ID {
		t.Fatalf("expected inventory row replacement, got first=%s second=%s", first.ID, second.ID)
	}
	inventories, err := s.ListLocalPVInventories(model.LocalPVInventoryFilter{ClusterNodeName: "worker-1"})
	if err != nil {
		t.Fatalf("list inventories: %v", err)
	}
	if len(inventories) != 1 || !inventories[0].SafeToDecommission || inventories[0].LVCount != 0 {
		t.Fatalf("unexpected inventories: %+v", inventories)
	}
}
