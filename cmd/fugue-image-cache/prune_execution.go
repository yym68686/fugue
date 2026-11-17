package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

// Writers never wait for an inventory scan or an entire prune. Only a final
// unlink/manifest delete holds mu. A writer starting between observation and
// deletion invalidates the observation; reclamation yields to that writer.
type imageCacheContentActivity struct {
	mu         sync.Mutex
	generation uint64
	writers    int
}

func (c *imageCache) beginContentMutation() func() {
	a := &c.contentActivity
	a.mu.Lock()
	a.generation++
	a.writers++
	a.mu.Unlock()
	return func() {
		a.mu.Lock()
		a.generation++
		a.writers--
		a.mu.Unlock()
	}
}

func (c *imageCache) contentSnapshot() (uint64, bool) {
	a := &c.contentActivity
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.generation, a.writers == 0
}

func (c *imageCache) withStableContent(generation uint64, action func() error) (bool, error) {
	a := &c.contentActivity
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.writers != 0 || a.generation != generation {
		return false, nil
	}
	return true, action()
}

func (c *imageCache) executeImageCachePrunePlan(plan *imageCachePrunePlan) (err error) {
	if plan == nil {
		return nil
	}
	plan.ExecutionStatus = "completed"
	defer func() {
		if err != nil {
			plan.ExecutionStatus = "failed"
		}
		c.metrics.prunePlannedBytes.Add(uint64(maxInt64(plan.PlannedDeleteBytes)))
		c.metrics.pruneDeletedBytes.Add(uint64(maxInt64(plan.DeletedBytes)))
		c.metrics.pruneSkippedBytes.Add(uint64(maxInt64(plan.SkippedBlobBytes)))
		if plan.ExecutionStatus == "deferred" {
			c.metrics.pruneDeferred.Add(1)
		}
		if err != nil {
			c.metrics.pruneErrors.Add(1)
		}
		receipt := map[string]any{
			"execution_status": plan.ExecutionStatus, "planned_delete_bytes": plan.PlannedDeleteBytes,
			"deleted_bytes": plan.DeletedBytes, "deleted_manifest_bytes": plan.DeletedManifestBytes,
			"deleted_blob_bytes": plan.DeletedBlobBytes, "skipped_blob_bytes": plan.SkippedBlobBytes,
			"skipped_reason": plan.SkippedReason, "remaining_snapshot_valid": plan.RemainingSnapshotValid,
			"remaining_unreferenced_bytes": plan.RemainingUnreferencedBytes,
			"skip_details":                 plan.SkipDetails,
		}
		if err != nil {
			receipt["error"] = err.Error()
		}
		raw, _ := json.Marshal(receipt)
		log.Printf("image-cache prune execution receipt %s", raw)
	}()
	deferBlobs := func(reason string, start int) {
		plan.SkippedReason = reason
		plan.ExecutionStatus = "deferred"
		if plan.Deleted {
			plan.ExecutionStatus = "partial"
		}
		for _, blob := range plan.deleteBlobs[start:] {
			plan.skipBlob(blob, reason)
		}
	}
	if !plan.contentSnapshotValid {
		deferBlobs("active_content_mutation", 0)
		return nil
	}
	generation := plan.contentGeneration
	if ok, _ := c.withStableContent(generation, func() error { return nil }); !ok {
		deferBlobs("content_changed", 0)
		return nil
	}

	// Re-read pins and the graph before any destructive work. A pin protects
	// all aliases of the same digest; retained parents protect their children.
	records, err := c.managementManifestRecords()
	if err != nil {
		return err
	}
	pins, err := c.readPins()
	if err != nil {
		return err
	}
	pinned := pinnedImageCacheDigests(records, pins)
	deletes := make([]imageCacheManifestRecord, 0, len(plan.deleteManifests))
	current := make(map[string]imageCacheManifestRecord, len(records))
	for _, record := range records {
		current[imageCacheManifestRecordKey(record)] = record
	}
	for _, record := range plan.deleteManifests {
		now, exists := current[imageCacheManifestRecordKey(record)]
		if !exists {
			continue
		}
		if now.Digest != record.Digest || !now.ModifiedAt.Equal(record.ModifiedAt) {
			deferBlobs("manifest_changed", 0)
			return nil
		}
		if _, protected := pinned[imageCacheManifestReferenceKey(now.Repo, now.Digest)]; protected {
			continue
		}
		deletes = append(deletes, now)
	}
	deletes, _ = protectImageCacheManifestDeletes(records, deletes)
	for _, record := range deletes {
		ok, deleteErr := c.withStableContent(generation, func() error { return c.deleteLocalManifest(record.Repo, record.Target) })
		if deleteErr != nil {
			return deleteErr
		}
		if !ok {
			deferBlobs("content_changed", 0)
			return nil
		}
		plan.Deleted = true
		plan.DeletedBytes += record.SizeBytes
		plan.DeletedManifestBytes += record.SizeBytes
		plan.DeletedManifests = append(plan.DeletedManifests, manifestEntry(record, nil))
	}

	// All selected aliases have now been processed. Never retain a stale
	// positive reference observed halfway through deleting those aliases.
	records, err = c.managementManifestRecords()
	if err != nil {
		return err
	}
	references := map[string]struct{}{}
	for _, record := range records {
		for _, digest := range record.ReferencedBlobs {
			references[digest] = struct{}{}
		}
	}
	checked := map[string]struct{}{}
	for _, record := range append(append([]imageCacheManifestRecord(nil), records...), deletes...) {
		key := imageCacheManifestReferenceKey(record.Repo, record.Digest)
		if _, seen := checked[key]; seen {
			continue
		}
		checked[key] = struct{}{}
		blobs, checkErr := c.localManifestReferencedBlobDigests(record.Repo, record.Digest)
		if checkErr != nil {
			plan.SkipDetails = append(plan.SkipDetails, fmt.Sprintf("repo=%s digest=%s: %v", record.Repo, record.Digest, checkErr))
			deferBlobs("reference_check_failed", 0)
			return nil
		}
		for _, digest := range blobs {
			references[digest] = struct{}{}
		}
	}
	for index, blob := range plan.deleteBlobs {
		if _, referenced := references[blob.Digest]; referenced {
			plan.skipBlob(blob, "still_referenced")
			continue
		}
		removed := false
		ok, deleteErr := c.withStableContent(generation, func() error {
			// Blob content may have been replaced since planning. Never unlink
			// a path whose observed identity/size/mtime has changed.
			info, statErr := os.Lstat(blob.Path)
			if errors.Is(statErr, os.ErrNotExist) {
				return nil
			}
			if statErr != nil {
				return statErr
			}
			if !info.Mode().IsRegular() || (blob.info != nil && !os.SameFile(blob.info, info)) || info.Size() != blob.SizeBytes || !info.ModTime().Equal(blob.ModifiedAt) {
				plan.skipBlob(blob, "blob_changed")
				return nil
			}
			if removeErr := os.Remove(blob.Path); removeErr != nil {
				return fmt.Errorf("delete blob %s: %w", blob.Digest, removeErr)
			}
			removed = true
			return nil
		})
		if deleteErr != nil {
			return deleteErr
		}
		if !ok {
			deferBlobs("content_changed", index)
			return nil
		}
		if removed {
			plan.Deleted = true
			plan.DeletedBytes += blob.SizeBytes
			plan.DeletedBlobBytes += blob.SizeBytes
			plan.DeletedBlobs = append(plan.DeletedBlobs, blobEntry(blob))
		}
	}
	blobs, err := c.imageCacheBlobRecords()
	if err != nil {
		return err
	}
	for _, blob := range unreferencedImageCacheBlobs(records, blobs, nil) {
		if _, live := references[blob.Digest]; live {
			continue
		}
		plan.RemainingUnreferencedCount++
		plan.RemainingUnreferencedBytes += blob.SizeBytes
	}
	currentGeneration, inactive := c.contentSnapshot()
	plan.RemainingSnapshotValid = inactive && generation == currentGeneration
	if !plan.RemainingSnapshotValid {
		plan.RemainingUnreferencedCount, plan.RemainingUnreferencedBytes = 0, 0
	}
	if plan.SkippedBlobBytes > 0 || plan.RemainingUnreferencedBytes > 0 || len(deletes) < len(plan.deleteManifests) {
		plan.ExecutionStatus = "partial"
	}
	return nil
}

func (p *imageCachePrunePlan) skipBlob(blob imageCacheBlobRecord, reason string) {
	entry := blobEntry(blob)
	entry.Reason = reason
	p.SkippedBlobs = append(p.SkippedBlobs, entry)
	p.SkippedBlobBytes += blob.SizeBytes
}

func pinnedImageCacheDigests(records []imageCacheManifestRecord, pins cachePinsFile) map[string]struct{} {
	result := map[string]struct{}{}
	for _, pin := range pins.Pins {
		if pin.ExpiresAt != nil && pin.ExpiresAt.Before(time.Now()) {
			continue
		}
		if key := imageCacheManifestReferenceKey(pin.Repo, pin.Digest); key != "" {
			result[key] = struct{}{}
		}
		for _, record := range records {
			if record.Repo == pin.Repo && record.Target == pin.Target {
				result[imageCacheManifestReferenceKey(record.Repo, record.Digest)] = struct{}{}
			}
		}
	}
	return result
}
