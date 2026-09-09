package store

import (
	"fmt"
	"testing"
	"time"

	"fugue/internal/imagecacheevidence"
	"fugue/internal/model"
)

func TestImageCacheInventoryBatchPostgresPreservesSnapshotSemantics(t *testing.T) {
	s := billingBatchPGStore(t)
	id := model.NewID("cache_batch_test")
	t.Cleanup(func() {
		s.db.Exec(`DELETE FROM fugue_image_cache_manifests WHERE node_id=$1`, id)
		s.db.Exec(`DELETE FROM fugue_image_cache_nodes WHERE node_id=$1`, id)
	})
	now := time.Now().UTC().Truncate(time.Microsecond)
	node := model.ImageCacheNodeInventory{NodeID: id, ClusterNodeName: id, ObservedAt: now}
	manifests := make([]model.ImageCacheManifest, 1201)
	for i := range manifests {
		manifests[i] = model.ImageCacheManifest{
			ID: fmt.Sprintf("%s_%d", id, i), Repo: "apps/sample", Target: fmt.Sprintf("v%d", i), Digest: fmt.Sprintf("sha256:%064x", i+1),
			Present: true, ManifestSizeBytes: 10, TotalBlobBytes: 20, LastSeenAt: now,
		}
	}
	started := time.Now()
	if _, err := s.UpsertImageCacheInventory(node, manifests); err != nil {
		t.Fatal(err)
	}
	t.Logf("1201 manifest snapshot: %s", time.Since(started))
	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM fugue_image_cache_manifests WHERE node_id=$1 AND present`, id).Scan(&count); err != nil || count != len(manifests) {
		t.Fatalf("wrong first snapshot coverage %d %v", count, err)
	}
	first := manifests[0]
	last := first
	last.ID = model.NewID("duplicate_manifest")
	last.TotalBlobBytes = 50
	node.ObservedAt = now.Add(time.Second)
	first.LastSeenAt = node.ObservedAt
	last.LastSeenAt = node.ObservedAt
	if _, err := s.UpsertImageCacheInventory(node, []model.ImageCacheManifest{first, last}); err != nil {
		t.Fatal(err)
	}
	var storedID string
	var bytes int64
	if err := s.db.QueryRow(`SELECT id,total_blob_bytes FROM fugue_image_cache_manifests WHERE node_id=$1 AND target='v0'`, id).Scan(&storedID, &bytes); err != nil || storedID != first.ID || bytes != 50 {
		t.Fatalf("duplicate upsert changed semantics %s %d %v", storedID, bytes, err)
	}
	if err := s.db.QueryRow(`SELECT count(*) FROM fugue_image_cache_manifests WHERE node_id=$1 AND present`, id).Scan(&count); err != nil || count != len(manifests) {
		t.Fatal("partial chunk hid earlier manifests")
	}
	node.SnapshotComplete = true
	if _, err := s.UpsertImageCacheInventory(node, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT count(*) FROM fugue_image_cache_manifests WHERE node_id=$1 AND present`, id).Scan(&count); err != nil || count != 1 {
		t.Fatalf("complete snapshot did not mark omitted manifests absent: %d %v", count, err)
	}
	incomplete := first
	incomplete.GraphStatus = imagecacheevidence.GraphStatusIncomplete
	incomplete.GraphFailureReason = imagecacheevidence.ReasonMissingBlob
	incomplete.ManifestSizeBytes = 999
	incomplete.TotalBlobBytes = 999
	if _, err := s.UpsertImageCacheInventory(node, []model.ImageCacheManifest{incomplete}); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT total_blob_bytes FROM fugue_image_cache_manifests WHERE node_id=$1 AND target='v0'`, id).Scan(&bytes); err != nil || bytes != 0 {
		t.Fatal("incomplete graph became countable")
	}
	// A conflicting primary key in a later batch must roll back all earlier
	// rows and the node observation written by this request.
	node.ObservedAt = now.Add(2 * time.Second)
	failed := make([]model.ImageCacheManifest, 501)
	for i := range failed {
		failed[i] = manifests[i]
		failed[i].ID = model.NewID("batch_rollback")
		failed[i].Target = fmt.Sprintf("failed%d", i)
	}
	failed[500].ID = first.ID
	if _, err := s.UpsertImageCacheInventory(node, failed); err == nil {
		t.Fatal("expected primary key conflict")
	}
	if err := s.db.QueryRow(`SELECT count(*) FROM fugue_image_cache_manifests WHERE node_id=$1 AND target LIKE 'failed%'`, id).Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed multi-batch update partially committed: %d %v", count, err)
	}
}
