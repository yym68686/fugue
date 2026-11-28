package imagecacheinventory

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"fugue/internal/imagecacheevidence"
	"fugue/internal/imagecacheusage"
	"fugue/internal/model"
)

type imageCacheUploadEntry struct {
	Path       string `json:"path"`
	SizeBytes  int64  `json:"size_bytes"`
	ModifiedAt string `json:"modified_at"`
	Reason     string `json:"reason"`
}
type imageCacheUploadObservation struct {
	Candidates      []imageCacheUploadEntry `json:"candidates"`
	Skipped         []imageCacheUploadEntry `json:"skipped"`
	Deleted         []imageCacheUploadEntry `json:"deleted"`
	CandidateBytes  int64                   `json:"candidate_bytes"`
	DeletedBytes    int64                   `json:"deleted_bytes"`
	TotalBytes      int64                   `json:"total_bytes"`
	StateMismatches int                     `json:"state_mismatches"`
	Error           string                  `json:"error"`
}
type Report struct {
	UploadTemp        *imageCacheUploadObservation        `json:"upload_temp"`
	Node              model.ImageCacheNodeInventory       `json:"node"`
	Manifests         []imageCacheInventoryManifestReport `json:"manifests"`
	UnreferencedBlobs []imageCacheInventoryBlobReport     `json:"unreferenced_blobs"`
	ObservedAt        *time.Time                          `json:"observed_at"`
	TotalCount        int                                 `json:"manifest_total_count"`
	ChunkIndex        int                                 `json:"chunk_index"`
	ChunkCount        int                                 `json:"chunk_count"`
	Endpoint          string                              `json:"endpoint"`
	ClusterNode       string                              `json:"cluster_node"`
	Pins              []any                               `json:"pins"`
	Disk              imageCacheInventoryDiskReport       `json:"disk"`
}

type imageCacheInventoryDiskReport struct {
	Enabled              bool    `json:"enabled"`
	TotalBytes           int64   `json:"total_bytes"`
	UsedBytes            int64   `json:"used_bytes"`
	FreeBytes            int64   `json:"free_bytes"`
	UsedPercent          float64 `json:"used_percent"`
	CacheBytes           int64   `json:"cache_bytes"`
	HighWatermarkPercent float64 `json:"high_watermark_percent"`
	LowWatermarkPercent  float64 `json:"low_watermark_percent"`
	MinFreeBytes         int64   `json:"min_free_bytes"`
	MaxDeleteBytesPerRun int64   `json:"max_delete_bytes_per_run"`
	OverHighWatermark    bool    `json:"over_high_watermark"`
	BelowMinFree         bool    `json:"below_min_free"`
	NeededDeleteBytes    int64   `json:"needed_delete_bytes"`
}

type imageCacheInventoryManifestReport struct {
	model.ImageCacheManifest
	ContentType             string   `json:"content_type"`
	SizeBytes               int64    `json:"size_bytes"`
	ReferencedBlobBytes     int64    `json:"referenced_blob_bytes"`
	UniqueBlobBytesObserved int64    `json:"unique_blob_bytes_observed"`
	ReferencedManifests     []string `json:"referenced_manifests"`
	ModifiedAt              string   `json:"modified_at"`
}

type imageCacheInventoryBlobReport struct {
	Digest     string `json:"digest"`
	SizeBytes  int64  `json:"size_bytes"`
	ModifiedAt string `json:"modified_at"`
}

// Prepare binds a report to its already authenticated updater and normalizes node evidence.
func (req Report) Prepare(updater model.NodeUpdater, now time.Time) (model.ImageCacheNodeInventory, []model.ImageCacheManifest, error) {
	observedAt := now.UTC()
	if req.ObservedAt != nil && !req.ObservedAt.IsZero() {
		observedAt = req.ObservedAt.UTC()
	}
	node := req.Node
	node.NodeID = firstNonEmptyString(node.NodeID, updater.MachineID)
	node.ClusterNodeName = firstNonEmptyString(node.ClusterNodeName, req.ClusterNode, updater.ClusterNodeName)
	node.RuntimeID = firstNonEmptyString(node.RuntimeID, updater.RuntimeID)
	for _, pair := range [][2]string{{node.NodeID, updater.MachineID}, {node.ClusterNodeName, updater.ClusterNodeName}, {node.RuntimeID, updater.RuntimeID}} {
		if pair[1] != "" && pair[0] != "" && pair[0] != pair[1] {
			return model.ImageCacheNodeInventory{}, nil, errors.New("inventory identity does not match authenticated updater")
		}
	}
	node.ReportedByNodeUpdaterID = updater.ID
	node.CacheEndpoint = strings.TrimRight(firstNonEmptyString(node.CacheEndpoint, req.Endpoint), "/")
	node.FilesystemTotalBytes = firstNonZeroInt64(node.FilesystemTotalBytes, req.Disk.TotalBytes)
	node.FilesystemFreeBytes = firstNonZeroInt64(node.FilesystemFreeBytes, req.Disk.FreeBytes)
	if node.FilesystemUsedPercent == 0 {
		node.FilesystemUsedPercent = req.Disk.UsedPercent
	}
	node.FilesystemUsedPercent = imagecacheusage.ConservativeUsedPercent(
		node.FilesystemUsedPercent,
		node.FilesystemTotalBytes,
		node.FilesystemFreeBytes,
	)
	node.CacheBytes = firstNonZeroInt64(node.CacheBytes, req.Disk.CacheBytes)
	node.PinCount = firstNonZeroInt(node.PinCount, len(req.Pins))
	if len(req.UnreferencedBlobs) > 0 {
		node.UnreferencedBlobs = make([]model.ImageCachePruneBlobCandidate, 0, len(req.UnreferencedBlobs))
		node.UnreferencedBlobCount = 0
		node.UnreferencedBlobBytes = 0
		for _, blob := range req.UnreferencedBlobs {
			digest := strings.TrimSpace(blob.Digest)
			if digest == "" {
				continue
			}
			item := model.ImageCachePruneBlobCandidate{
				NodeName:           firstNonEmptyString(node.ClusterNodeName, node.NodeID, node.RuntimeID),
				Digest:             digest,
				SizeBytes:          blob.SizeBytes,
				Reason:             "unreferenced_blob",
				PlannedDeleteBytes: blob.SizeBytes,
				LastSeenAt:         strings.TrimSpace(blob.ModifiedAt),
			}
			node.UnreferencedBlobs = append(node.UnreferencedBlobs, item)
			node.UnreferencedBlobBytes += blob.SizeBytes
		}
		node.UnreferencedBlobCount = len(node.UnreferencedBlobs)
	}
	node.ObservedAt = observedAt
	node.ManifestCount = firstNonZeroInt(node.ManifestCount, req.TotalCount, len(req.Manifests))
	node.SnapshotComplete = imageCacheInventorySnapshotComplete(req, len(req.Manifests))
	manifests := make([]model.ImageCacheManifest, 0, len(req.Manifests))
	pinned := map[string]struct{}{}
	for _, pin := range req.Pins {
		raw, _ := json.Marshal(pin)
		var p struct {
			Repo   string `json:"repo"`
			Target string `json:"target"`
		}
		_ = json.Unmarshal(raw, &p)
		if p.Repo != "" && p.Target != "" {
			pinned[strings.Trim(p.Repo, "/")+"\x00"+strings.TrimSpace(p.Target)] = struct{}{}
		}
	}
	for _, reported := range req.Manifests {
		manifest := reported.ImageCacheManifest
		manifest.NodeID = node.NodeID
		manifest.ClusterNodeName = node.ClusterNodeName
		manifest.RuntimeID = node.RuntimeID
		manifest.LastSeenAt = observedAt
		if manifest.MediaType == "" {
			manifest.MediaType = strings.TrimSpace(reported.ContentType)
		}
		manifest.GraphStatus = imagecacheevidence.NormalizeGraphStatus(manifest.GraphStatus)
		if imagecacheevidence.GraphIsIncomplete(manifest.GraphStatus) {
			manifest.GraphFailureReason = imagecacheevidence.NormalizeGraphFailureReason(manifest.GraphFailureReason)
			manifest.ManifestSizeBytes = 0
			manifest.TotalBlobBytes = 0
			manifest.ReferencedBlobs = nil
			manifest.ReferencedManifests = nil
		} else {
			manifest.GraphFailureReason = ""
			manifest.ReferencedManifests = append([]string(nil), reported.ReferencedManifests...)
			manifest.ManifestSizeBytes = firstNonZeroInt64(manifest.ManifestSizeBytes, reported.SizeBytes)
			manifest.TotalBlobBytes = firstNonZeroInt64(manifest.TotalBlobBytes, reported.ReferencedBlobBytes, reported.UniqueBlobBytesObserved)
		}
		if manifest.CreatedAtObserved == nil && strings.TrimSpace(reported.ModifiedAt) != "" {
			if parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(reported.ModifiedAt)); err == nil {
				manifest.CreatedAtObserved = &parsed
			}
		}
		if manifest.ImageRef == "" && node.CacheEndpoint != "" && manifest.Repo != "" && manifest.Target != "" {
			manifest.ImageRef = strings.TrimRight(node.CacheEndpoint, "/") + "/" + strings.Trim(manifest.Repo, "/") + ":" + strings.TrimSpace(manifest.Target)
		}
		if _, ok := pinned[strings.Trim(manifest.Repo, "/")+"\x00"+strings.TrimSpace(manifest.Target)]; ok {
			manifest.PinnedLocally = true
		}
		manifest.Present = true
		manifests = append(manifests, manifest)
	}
	return node, manifests, nil
}

func imageCacheInventorySnapshotComplete(req Report, reportedManifestCount int) bool {
	if req.ChunkCount > 1 {
		return req.ChunkIndex >= req.ChunkCount-1
	}
	if req.ChunkCount == 1 {
		return true
	}
	if req.TotalCount > 0 {
		return req.TotalCount == reportedManifestCount
	}
	return true
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func firstNonZeroInt64(values ...int64) int64 {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func firstNonZeroInt(values ...int) int {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}
