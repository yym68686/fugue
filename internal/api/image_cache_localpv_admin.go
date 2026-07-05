package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/imagecachekeys"
	"fugue/internal/model"
	"fugue/internal/store"
)

const (
	defaultImageCacheInventoryTTL       = 2 * time.Hour
	defaultImageCacheOrphanGracePeriod  = 24 * time.Hour
	defaultImageCachePruneMaxDeleteByte = int64(1 << 30)
	defaultImageCachePruneMaxTargets    = 50
)

type imageCachePrunePlanOptions struct {
	skipNodeUpdateTaskID string
}

func (s *Server) handleNodeUpdaterReportImageCacheInventory(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if principal.ActorType != model.ActorTypeNodeUpdater {
		httpx.WriteError(w, http.StatusForbidden, "node updater credentials required")
		return
	}
	updater, err := s.nodeUpdaterByPrincipal(principal)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	node, manifests, err := decodeImageCacheInventoryReport(r, updater)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	node.ReportedByNodeUpdaterID = updater.ID
	node.NodeID = firstNonEmptyImageAPIString(node.NodeID, updater.MachineID)
	node.ClusterNodeName = firstNonEmptyImageAPIString(node.ClusterNodeName, updater.ClusterNodeName)
	node.RuntimeID = firstNonEmptyImageAPIString(node.RuntimeID, updater.RuntimeID)
	if node.ManifestCount == 0 {
		node.ManifestCount = len(manifests)
	}
	node, err = s.store.UpsertImageCacheInventory(node, manifests)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"node": node})
}

func (s *Server) handleNodeUpdaterReportLocalPVInventory(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if principal.ActorType != model.ActorTypeNodeUpdater {
		httpx.WriteError(w, http.StatusForbidden, "node updater credentials required")
		return
	}
	updater, err := s.nodeUpdaterByPrincipal(principal)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	var req struct {
		Inventory model.LocalPVInventory `json:"inventory"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	inventory := req.Inventory
	inventory.ReportedByNodeUpdaterID = updater.ID
	inventory.NodeID = firstNonEmptyImageAPIString(inventory.NodeID, updater.MachineID)
	inventory.ClusterNodeName = firstNonEmptyImageAPIString(inventory.ClusterNodeName, updater.ClusterNodeName)
	inventory.RuntimeID = firstNonEmptyImageAPIString(inventory.RuntimeID, updater.RuntimeID)
	if inventory.ObservedAt.IsZero() {
		inventory.ObservedAt = time.Now().UTC()
	}
	inventory.SafeToDecommission, inventory.UnsafeReasons = evaluateLocalPVDecommissionSafety(inventory)
	inventory, err = s.store.UpsertLocalPVInventory(inventory)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "localpv_inventory_reported", "localpv_inventory", firstNonEmptyImageAPIString(inventory.ClusterNodeName, inventory.NodeID), "", map[string]string{
		"node_id":              inventory.NodeID,
		"cluster_node_name":    inventory.ClusterNodeName,
		"runtime_id":           inventory.RuntimeID,
		"vg_name":              inventory.VGName,
		"lv_count":             fmt.Sprintf("%d", inventory.LVCount),
		"active_lv_count":      fmt.Sprintf("%d", inventory.ActiveLVCount),
		"bound_pv_count":       fmt.Sprintf("%d", inventory.BoundPVCount),
		"safe_to_decommission": fmt.Sprintf("%t", inventory.SafeToDecommission),
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"inventory": inventory})
}

func (s *Server) handleAdminListImageCacheInventory(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	filter := model.ImageCacheNodeInventoryFilter{
		NodeID:          r.URL.Query().Get("node_id"),
		ClusterNodeName: r.URL.Query().Get("cluster_node_name"),
		RuntimeID:       r.URL.Query().Get("runtime_id"),
	}
	nodes, err := s.store.ListImageCacheNodeInventories(filter)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	manifestFilter := model.ImageCacheManifestFilter{
		NodeID:          filter.NodeID,
		ClusterNodeName: filter.ClusterNodeName,
		RuntimeID:       filter.RuntimeID,
		PresentOnly:     true,
	}
	if len(nodes) == 1 && manifestFilter.NodeID == "" && manifestFilter.ClusterNodeName == "" {
		manifestFilter.NodeID = nodes[0].NodeID
		manifestFilter.ClusterNodeName = nodes[0].ClusterNodeName
	}
	manifests, err := s.store.ListImageCacheManifests(manifestFilter)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if nodes == nil {
		nodes = []model.ImageCacheNodeInventory{}
	}
	if manifests == nil {
		manifests = []model.ImageCacheManifest{}
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"nodes":     nodes,
		"manifests": manifests,
	})
}

func (s *Server) handleAdminGetImageCachePrunePlan(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	mode := strings.TrimSpace(r.URL.Query().Get("mode"))
	plan, err := s.computeImageCachePrunePlan(r, model.ImageCachePrunePlanFilter{
		NodeID:          r.URL.Query().Get("node_id"),
		ClusterNodeName: r.URL.Query().Get("cluster_node_name"),
		RuntimeID:       r.URL.Query().Get("runtime_id"),
		Mode:            mode,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if parseImageCacheBoolQuery(r.URL.Query().Get("persist")) {
		plan, err = s.store.UpsertImageCachePrunePlan(plan)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"plan": plan})
}

func (s *Server) handleAdminCreateImageCachePrunePlanTask(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	var req struct {
		NodeID          string `json:"node_id"`
		ClusterNodeName string `json:"cluster_node_name"`
		RuntimeID       string `json:"runtime_id"`
		Mode            string `json:"mode"`
		AllowDelete     bool   `json:"allow_delete"`
		MaxDeleteBytes  int64  `json:"max_delete_bytes"`
		DryRun          *bool  `json:"dry_run"`
	}
	if r.Body != nil && r.ContentLength != 0 {
		if err := httpx.DecodeJSON(r, &req); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	mode := normalizeImageCachePruneAPIMode(req.Mode)
	if mode == "" {
		mode = model.ImageCachePruneModeDryRun
	}
	if mode == model.ImageCachePruneModeDelete && !req.AllowDelete {
		httpx.WriteError(w, http.StatusBadRequest, "delete mode requires allow_delete=true")
		return
	}
	plan, err := s.computeImageCachePrunePlan(r, model.ImageCachePrunePlanFilter{
		NodeID:          req.NodeID,
		ClusterNodeName: req.ClusterNodeName,
		RuntimeID:       req.RuntimeID,
		Mode:            mode,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if req.MaxDeleteBytes > 0 {
		plan.MaxDeleteBytes = req.MaxDeleteBytes
	}
	plan.Mode = mode
	plan.Status = model.ImageCachePrunePlanStatusPlanned
	plan, err = s.store.UpsertImageCachePrunePlan(plan)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if (plan.CandidateManifestCount == 0 && plan.CandidateBlobCount == 0) || mode == model.ImageCachePruneModeObserve {
		httpx.WriteJSON(w, http.StatusCreated, map[string]any{"plan": plan})
		return
	}
	updater, err := s.findNodeUpdaterForImageCachePlan(plan)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	targets := pruneTaskTargets(plan.Candidates, defaultImageCachePruneMaxTargets)
	targetsRaw, err := json.Marshal(targets)
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	dryRun := mode != model.ImageCachePruneModeDelete
	if req.DryRun != nil {
		dryRun = *req.DryRun
	}
	allowDelete := req.AllowDelete && !dryRun && mode == model.ImageCachePruneModeDelete
	task, err := s.store.CreateNodeUpdateTask(principal, updater.ID, updater.ClusterNodeName, updater.RuntimeID, model.NodeUpdateTaskTypePruneImageCache, map[string]string{
		"prune_plan_id":              plan.ID,
		"dry_run":                    fmt.Sprintf("%t", dryRun),
		"allow_delete":               fmt.Sprintf("%t", allowDelete),
		"targets_json":               string(targetsRaw),
		"max_delete_bytes":           fmt.Sprintf("%d", plan.MaxDeleteBytes),
		"include_unreferenced_blobs": fmt.Sprintf("%t", plan.CandidateBlobCount > 0),
		"candidate_blob_bytes":       fmt.Sprintf("%d", plan.CandidateBlobBytes),
		"candidate_manifest_count":   fmt.Sprintf("%d", plan.CandidateManifestCount),
		"candidate_blob_count":       fmt.Sprintf("%d", plan.CandidateBlobCount),
		"prune_reason":               "image-cache-orphan",
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	plan.Status = model.ImageCachePrunePlanStatusScheduled
	plan, _ = s.store.UpsertImageCachePrunePlan(plan)
	s.appendAudit(principal, "image_cache_prune_plan.schedule", "image_cache_prune_plan", plan.ID, "", map[string]string{
		"node_id":           plan.NodeID,
		"cluster_node_name": plan.ClusterNodeName,
		"mode":              plan.Mode,
		"task_id":           task.ID,
	})
	httpx.WriteJSON(w, http.StatusCreated, map[string]any{"plan": plan, "task": task})
}

func (s *Server) handleAdminListLocalPVInventory(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	inventories, err := s.store.ListLocalPVInventories(model.LocalPVInventoryFilter{
		NodeID:          r.URL.Query().Get("node_id"),
		ClusterNodeName: r.URL.Query().Get("cluster_node_name"),
		RuntimeID:       r.URL.Query().Get("runtime_id"),
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if inventories == nil {
		inventories = []model.LocalPVInventory{}
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"inventories": inventories})
}

type imageCacheInventoryReport struct {
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
	ContentType             string `json:"content_type"`
	SizeBytes               int64  `json:"size_bytes"`
	ReferencedBlobBytes     int64  `json:"referenced_blob_bytes"`
	UniqueBlobBytesObserved int64  `json:"unique_blob_bytes_observed"`
	ModifiedAt              string `json:"modified_at"`
}

type imageCacheInventoryBlobReport struct {
	Digest     string `json:"digest"`
	SizeBytes  int64  `json:"size_bytes"`
	ModifiedAt string `json:"modified_at"`
}

func decodeImageCacheInventoryReport(r *http.Request, updater model.NodeUpdater) (model.ImageCacheNodeInventory, []model.ImageCacheManifest, error) {
	var req imageCacheInventoryReport
	if err := httpx.DecodeJSON(r, &req); err != nil {
		return model.ImageCacheNodeInventory{}, nil, err
	}
	observedAt := time.Now().UTC()
	if req.ObservedAt != nil && !req.ObservedAt.IsZero() {
		observedAt = req.ObservedAt.UTC()
	}
	node := req.Node
	node.NodeID = firstNonEmptyImageAPIString(node.NodeID, updater.MachineID)
	node.ClusterNodeName = firstNonEmptyImageAPIString(node.ClusterNodeName, req.ClusterNode, updater.ClusterNodeName)
	node.RuntimeID = firstNonEmptyImageAPIString(node.RuntimeID, updater.RuntimeID)
	node.CacheEndpoint = strings.TrimRight(firstNonEmptyImageAPIString(node.CacheEndpoint, req.Endpoint), "/")
	node.FilesystemTotalBytes = firstNonZeroInt64(node.FilesystemTotalBytes, req.Disk.TotalBytes)
	node.FilesystemFreeBytes = firstNonZeroInt64(node.FilesystemFreeBytes, req.Disk.FreeBytes)
	if node.FilesystemUsedPercent == 0 {
		node.FilesystemUsedPercent = req.Disk.UsedPercent
	}
	node.CacheBytes = firstNonZeroInt64(node.CacheBytes, req.Disk.CacheBytes)
	node.PinCount = firstNonZeroInt(node.PinCount, len(req.Pins))
	if len(req.UnreferencedBlobs) > 0 {
		node.UnreferencedBlobs = make([]model.ImageCachePruneBlobCandidate, 0, len(req.UnreferencedBlobs))
		for _, blob := range req.UnreferencedBlobs {
			digest := strings.TrimSpace(blob.Digest)
			if digest == "" {
				continue
			}
			item := model.ImageCachePruneBlobCandidate{
				NodeName:           firstNonEmptyImageAPIString(node.ClusterNodeName, node.NodeID, node.RuntimeID),
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
		manifest.ManifestSizeBytes = firstNonZeroInt64(manifest.ManifestSizeBytes, reported.SizeBytes)
		manifest.TotalBlobBytes = firstNonZeroInt64(manifest.TotalBlobBytes, reported.ReferencedBlobBytes, reported.UniqueBlobBytesObserved)
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

func imageCacheInventorySnapshotComplete(req imageCacheInventoryReport, reportedManifestCount int) bool {
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

func (s *Server) computeImageCachePrunePlan(r *http.Request, filter model.ImageCachePrunePlanFilter) (model.ImageCachePrunePlan, error) {
	return s.computeImageCachePrunePlanWithOptions(r, filter, imageCachePrunePlanOptions{})
}

func (s *Server) computeImageCachePrunePlanWithOptions(r *http.Request, filter model.ImageCachePrunePlanFilter, options imageCachePrunePlanOptions) (model.ImageCachePrunePlan, error) {
	mode := normalizeImageCachePruneAPIMode(filter.Mode)
	if mode == "" {
		mode = model.ImageCachePruneModeObserve
	}
	nodes, err := s.store.ListImageCacheNodeInventories(model.ImageCacheNodeInventoryFilter{
		NodeID:          filter.NodeID,
		ClusterNodeName: filter.ClusterNodeName,
		RuntimeID:       filter.RuntimeID,
		StaleAfter:      time.Now().UTC().Add(-defaultImageCacheInventoryTTL),
	})
	if err != nil {
		return model.ImageCachePrunePlan{}, err
	}
	manifestFilter := model.ImageCacheManifestFilter{
		NodeID:          filter.NodeID,
		ClusterNodeName: filter.ClusterNodeName,
		RuntimeID:       filter.RuntimeID,
		SeenAfter:       time.Now().UTC().Add(-defaultImageCacheInventoryTTL),
		PresentOnly:     true,
	}
	if len(nodes) == 1 && manifestFilter.NodeID == "" && manifestFilter.ClusterNodeName == "" {
		manifestFilter.NodeID = nodes[0].NodeID
		manifestFilter.ClusterNodeName = nodes[0].ClusterNodeName
	}
	manifests, err := s.store.ListImageCacheManifests(manifestFilter)
	if err != nil {
		return model.ImageCachePrunePlan{}, err
	}
	protected := imageCacheProtectedSet{}
	if err := s.populateImageCacheProtectedSetWithOptions(r, &protected, imageCacheProtectedSetOptions{
		skipNodeUpdateTaskID: options.skipNodeUpdateTaskID,
	}); err != nil {
		return model.ImageCachePrunePlan{}, err
	}
	now := time.Now().UTC()
	plan := model.ImageCachePrunePlan{
		ID:                model.NewID("imgcacheprune"),
		Mode:              mode,
		MaxDeleteBytes:    defaultImageCachePruneMaxDeleteByte,
		MinManifestAge:    defaultImageCacheOrphanGracePeriod.String(),
		ProtectionSummary: map[string]int{},
		CandidateSummary:  map[string]int{},
		CreatedAt:         now,
		Status:            model.ImageCachePrunePlanStatusPlanned,
	}
	if len(nodes) == 1 {
		plan.NodeID = nodes[0].NodeID
		plan.ClusterNodeName = nodes[0].ClusterNodeName
		plan.RuntimeID = nodes[0].RuntimeID
		plan.NodePressure = imageCacheNodePressure(nodes[0])
		for _, blob := range nodes[0].UnreferencedBlobs {
			if strings.TrimSpace(blob.Digest) == "" {
				continue
			}
			if blob.Reason == "" {
				blob.Reason = "unreferenced_blob"
			}
			if blob.PlannedDeleteBytes == 0 {
				blob.PlannedDeleteBytes = firstNonZeroInt64(blob.SizeBytes, 0)
			}
			if strings.TrimSpace(blob.NodeName) == "" {
				blob.NodeName = firstNonEmptyImageAPIString(nodes[0].ClusterNodeName, nodes[0].NodeID, nodes[0].RuntimeID)
			}
			plan.UnreferencedBlobs = append(plan.UnreferencedBlobs, blob)
			plan.CandidateBlobCount++
			blobBytes := firstNonZeroInt64(blob.PlannedDeleteBytes, blob.SizeBytes)
			plan.CandidateBlobBytes += blobBytes
			plan.PlannedDeleteBytes += blobBytes
			plan.CandidateSummary[blob.Reason]++
		}
	} else {
		plan.NodeID = strings.TrimSpace(filter.NodeID)
		plan.ClusterNodeName = strings.TrimSpace(filter.ClusterNodeName)
		plan.RuntimeID = strings.TrimSpace(filter.RuntimeID)
	}
	for _, manifest := range manifests {
		candidate := imageCachePruneCandidateForManifest(manifest, protected, now)
		if candidate.Protected {
			plan.ProtectedManifestCount++
			plan.ProtectionSummary[candidate.SkipReason]++
			plan.ProtectedManifests = append(plan.ProtectedManifests, candidate)
			plan.SkippedManifests = append(plan.SkippedManifests, candidate)
			continue
		}
		plan.CandidateManifestCount++
		plan.CandidateSummary[candidate.Reason]++
		plan.PlannedDeleteBytes += candidate.PlannedDeleteBytes
		plan.Candidates = append(plan.Candidates, candidate)
	}
	sort.SliceStable(plan.Candidates, func(i, j int) bool {
		if plan.Candidates[i].Reason != plan.Candidates[j].Reason {
			return plan.Candidates[i].Reason < plan.Candidates[j].Reason
		}
		return plan.Candidates[i].PlannedDeleteBytes > plan.Candidates[j].PlannedDeleteBytes
	})
	if plan.PlannedDeleteBytes > plan.MaxDeleteBytes {
		plan.BudgetExhausted = true
		plan.PlannedDeleteBytes = plan.MaxDeleteBytes
	}
	sort.SliceStable(plan.ProtectedManifests, func(i, j int) bool {
		return plan.ProtectedManifests[i].PlannedDeleteBytes > plan.ProtectedManifests[j].PlannedDeleteBytes
	})
	return plan, nil
}

type imageCacheProtectedSet struct {
	availableRefs        map[string]struct{}
	lostRefs             map[string]struct{}
	deletedRefs          map[string]struct{}
	pinnedRefs           map[string]struct{}
	liveRefs             map[string]struct{}
	taskRefs             map[string]struct{}
	minReplicaRefs       map[string]struct{}
	imageIDsByRef        map[string][]string
	pinIDsByRef          map[string][]string
	taskIDsByRef         map[string][]string
	workloadRefsByRef    map[string][]string
	replicaIDsByRef      map[string][]string
	minReplicaKeeperRefs map[string][]imageCacheReplicaCandidate
	replicaCandidateRefs map[string][]imageCacheReplicaCandidate
}

type imageCacheReplicaCandidate struct {
	NodeID          string
	RuntimeID       string
	ClusterNodeName string
	Reason          string
	ReplicaID       string
}

type imageCacheProtectedSetOptions struct {
	skipNodeUpdateTaskID string
}

func (s *Server) populateImageCacheProtectedSet(r *http.Request, protected *imageCacheProtectedSet) error {
	return s.populateImageCacheProtectedSetWithOptions(r, protected, imageCacheProtectedSetOptions{})
}

func (s *Server) populateImageCacheProtectedSetWithOptions(r *http.Request, protected *imageCacheProtectedSet, options imageCacheProtectedSetOptions) error {
	protected.availableRefs = map[string]struct{}{}
	protected.lostRefs = map[string]struct{}{}
	protected.deletedRefs = map[string]struct{}{}
	protected.pinnedRefs = map[string]struct{}{}
	protected.liveRefs = map[string]struct{}{}
	protected.taskRefs = map[string]struct{}{}
	protected.minReplicaRefs = map[string]struct{}{}
	protected.imageIDsByRef = map[string][]string{}
	protected.pinIDsByRef = map[string][]string{}
	protected.taskIDsByRef = map[string][]string{}
	protected.workloadRefsByRef = map[string][]string{}
	protected.replicaIDsByRef = map[string][]string{}
	protected.minReplicaKeeperRefs = map[string][]imageCacheReplicaCandidate{}
	protected.replicaCandidateRefs = map[string][]imageCacheReplicaCandidate{}

	images, err := s.store.ListImages(model.ImageFilter{PlatformAdmin: true})
	if err != nil {
		return err
	}
	imageByID := map[string]model.Image{}
	for _, image := range images {
		imageByID[image.ID] = image
		keys := exactImageReferenceKeys(image.ImageRef, image.CanonicalDigest)
		addImageCacheDetail(protected.imageIDsByRef, keys, image.ID)
		switch strings.TrimSpace(image.LifecycleState) {
		case model.ImageLifecycleAvailable:
			// Available images are protected by node-aware minimum replica
			// keepers below, not by a repo-wide available-image key.
		case model.ImageLifecycleLost:
			addKeys(protected.lostRefs, keys...)
		case model.ImageLifecycleDeleting, model.ImageLifecycleDeleted:
			addKeys(protected.deletedRefs, keys...)
		}
	}
	aliases, err := s.store.ListImageAliases(model.ImageAliasFilter{PlatformAdmin: true})
	if err != nil {
		return err
	}
	for _, alias := range aliases {
		image := imageByID[alias.ImageID]
		keys := exactImageReferenceKeys(alias.AliasRef, firstNonEmptyImageAPIString(alias.Digest, image.CanonicalDigest))
		addImageCacheDetail(protected.imageIDsByRef, keys, image.ID)
		switch strings.TrimSpace(image.LifecycleState) {
		case model.ImageLifecycleAvailable:
			// Available aliases are protected by node-aware minimum replica
			// keepers below, not by a repo-wide available-image key.
		case model.ImageLifecycleLost:
			addKeys(protected.lostRefs, keys...)
		case model.ImageLifecycleDeleting, model.ImageLifecycleDeleted:
			addKeys(protected.deletedRefs, keys...)
		}
	}
	pins, err := s.store.ListImagePins(model.ImagePinFilter{PlatformAdmin: true})
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, pin := range pins {
		if pin.ExpiresAt != nil && pin.ExpiresAt.Before(now) {
			continue
		}
		image := imageByID[pin.ImageID]
		keys := exactImageReferenceKeys(image.ImageRef, image.CanonicalDigest)
		addKeys(protected.pinnedRefs, keys...)
		addImageCacheDetail(protected.pinIDsByRef, keys, pin.ID)
	}
	apps, err := s.store.ListAppsMetadata("", true)
	if err == nil {
		for ref := range s.liveManagedImageRefSet(r.Context(), apps) {
			keys := exactImageReferenceKeys(ref, "")
			addKeys(protected.liveRefs, keys...)
			addImageCacheDetail(protected.workloadRefsByRef, keys, ref)
		}
	}
	for _, status := range []string{model.NodeUpdateTaskStatusPending, model.NodeUpdateTaskStatusRunning} {
		tasks, err := s.store.ListNodeUpdateTasks("", true, "", status)
		if err != nil {
			return err
		}
		for _, task := range tasks {
			if strings.TrimSpace(options.skipNodeUpdateTaskID) != "" && task.ID == strings.TrimSpace(options.skipNodeUpdateTaskID) {
				continue
			}
			if imageCacheNodeUpdateTaskObsolete(task, imageByID) {
				continue
			}
			keys := exactImageReferenceKeys(task.Payload["image_ref"], task.Payload["digest"])
			addKeys(protected.taskRefs, keys...)
			addImageCacheDetail(protected.taskIDsByRef, keys, task.ID)
			keys = exactImageReferenceKeys(task.Payload["images"], "")
			addKeys(protected.taskRefs, keys...)
			addImageCacheDetail(protected.taskIDsByRef, keys, task.ID)
			if raw := strings.TrimSpace(task.Payload["targets_json"]); raw != "" {
				var targets []struct {
					Repo   string `json:"repo"`
					Target string `json:"target"`
					Digest string `json:"digest"`
				}
				if json.Unmarshal([]byte(raw), &targets) == nil {
					for _, target := range targets {
						keys := exactManifestReferenceKeys(target.Repo, target.Target, target.Digest, "")
						addKeys(protected.taskRefs, keys...)
						addImageCacheDetail(protected.taskIDsByRef, keys, task.ID)
					}
				}
			}
		}
	}
	for _, status := range []string{model.ImageReplicationTaskStatusPending, model.ImageReplicationTaskStatusRunning} {
		tasks, err := s.store.ListImageReplicationTasks(model.ImageReplicationTaskFilter{Status: status, PlatformAdmin: true})
		if err != nil {
			return err
		}
		for _, task := range tasks {
			image := imageByID[task.ImageID]
			if strings.TrimSpace(image.ID) == "" || imageCacheReplicationTaskObsolete(task, image) {
				continue
			}
			keys := exactImageReferenceKeys(image.ImageRef, image.CanonicalDigest)
			addKeys(protected.taskRefs, keys...)
			addImageCacheDetail(protected.taskIDsByRef, keys, task.ID)
		}
	}
	if err := s.populateImageCacheReplicaCandidateRefs(protected, images); err != nil {
		return err
	}
	if err := s.populateImageCacheMinimumReplicaRefs(protected, images); err != nil {
		return err
	}
	return nil
}

func (s *Server) populateImageCacheMinimumReplicaRefs(protected *imageCacheProtectedSet, images []model.Image) error {
	if protected == nil {
		return nil
	}
	now := time.Now().UTC()
	for _, image := range images {
		switch strings.TrimSpace(image.LifecycleState) {
		case "", model.ImageLifecycleAvailable, model.ImageLifecycleImporting:
		default:
			continue
		}
		replicas, err := s.store.ListImageReplicas(model.ImageReplicaFilter{
			ImageID:       image.ID,
			Status:        model.ImageReplicaStatusPresent,
			PlatformAdmin: true,
		})
		if err != nil {
			return err
		}
		healthy := healthyImageReplicasAPI(replicas, now)
		keys := exactImageReferenceKeys(image.ImageRef, image.CanonicalDigest)
		if len(healthy) == 0 {
			addKeys(protected.minReplicaRefs, keys...)
			addImageCacheDetail(protected.imageIDsByRef, keys, image.ID)
			continue
		}
		sort.SliceStable(healthy, func(i, j int) bool {
			left := healthy[i].UpdatedAt
			if healthy[i].LastVerifiedAt != nil {
				left = *healthy[i].LastVerifiedAt
			}
			right := healthy[j].UpdatedAt
			if healthy[j].LastVerifiedAt != nil {
				right = *healthy[j].LastVerifiedAt
			}
			if !left.Equal(right) {
				return left.After(right)
			}
			return healthy[i].ID < healthy[j].ID
		})
		for idx, replica := range healthy {
			if idx < 1 {
				addImageCacheReplicaCandidate(protected.minReplicaKeeperRefs, keys, imageCacheReplicaCandidate{
					NodeID:          strings.TrimSpace(replica.NodeID),
					RuntimeID:       strings.TrimSpace(replica.RuntimeID),
					ClusterNodeName: strings.TrimSpace(replica.ClusterNodeName),
					Reason:          "minimum_replica_count",
					ReplicaID:       strings.TrimSpace(replica.ID),
				})
				addImageCacheDetail(protected.replicaIDsByRef, keys, replica.ID)
				addImageCacheDetail(protected.imageIDsByRef, keys, image.ID)
				continue
			}
			addImageCacheReplicaCandidate(protected.replicaCandidateRefs, keys, imageCacheReplicaCandidate{
				NodeID:          strings.TrimSpace(replica.NodeID),
				RuntimeID:       strings.TrimSpace(replica.RuntimeID),
				ClusterNodeName: strings.TrimSpace(replica.ClusterNodeName),
				Reason:          "excess_replica",
				ReplicaID:       strings.TrimSpace(replica.ID),
			})
		}
	}
	return nil
}

func (s *Server) populateImageCacheReplicaCandidateRefs(protected *imageCacheProtectedSet, images []model.Image) error {
	if protected == nil {
		return nil
	}
	for _, image := range images {
		replicas, err := s.store.ListImageReplicas(model.ImageReplicaFilter{
			ImageID:       image.ID,
			TenantID:      image.TenantID,
			PlatformAdmin: true,
		})
		if err != nil {
			return err
		}
		keys := exactImageReferenceKeys(image.ImageRef, image.CanonicalDigest)
		for _, replica := range replicas {
			reason := imageCacheReplicaCandidateReason(replica.Status)
			if reason == "" {
				continue
			}
			addImageCacheReplicaCandidate(protected.replicaCandidateRefs, keys, imageCacheReplicaCandidate{
				NodeID:          strings.TrimSpace(replica.NodeID),
				RuntimeID:       strings.TrimSpace(replica.RuntimeID),
				ClusterNodeName: strings.TrimSpace(replica.ClusterNodeName),
				Reason:          reason,
				ReplicaID:       strings.TrimSpace(replica.ID),
			})
		}
	}
	return nil
}

func imageCachePruneCandidateForManifest(manifest model.ImageCacheManifest, protected imageCacheProtectedSet, now time.Time) model.ImageCachePruneCandidate {
	keys := exactManifestReferenceKeys(manifest.Repo, manifest.Target, manifest.Digest, manifest.ImageRef)
	out := model.ImageCachePruneCandidate{
		ImageRef:            manifest.ImageRef,
		NodeName:            firstNonEmptyImageAPIString(manifest.ClusterNodeName, manifest.NodeID, manifest.RuntimeID),
		Repo:                manifest.Repo,
		Target:              manifest.Target,
		Digest:              manifest.Digest,
		ReferencedBlobs:     append([]string(nil), manifest.ReferencedBlobs...),
		PlannedDeleteBytes:  firstNonZeroInt64(manifest.TotalBlobBytes, manifest.ManifestSizeBytes),
		ReferencedBlobCount: len(manifest.ReferencedBlobs),
		ReferencedBlobBytes: manifest.TotalBlobBytes,
		LastSeenAt:          manifest.LastSeenAt.UTC().Format(time.RFC3339),
	}
	if manifest.CreatedAtObserved != nil {
		out.CreatedAtObserved = manifest.CreatedAtObserved.UTC().Format(time.RFC3339)
	}
	if manifest.PinnedLocally {
		out.Protected = true
		out.SkipReason = "local_pin"
		out.SkipDetails = []string{"manifest is pinned locally on the node"}
		return out
	}
	if keySetContainsAny(protected.liveRefs, keys...) {
		out.Protected = true
		out.SkipReason = "current_workload"
		out.MatchedWorkloadRefs = imageCacheDetailsForKeys(protected.workloadRefsByRef, keys)
		out.SkipDetails = []string{"exact image generation is used by a live workload"}
		return out
	}
	if keySetContainsAny(protected.pinnedRefs, keys...) {
		out.Protected = true
		out.SkipReason = "active_pin"
		out.MatchedPinIDs = imageCacheDetailsForKeys(protected.pinIDsByRef, keys)
		out.MatchedImageIDs = imageCacheDetailsForKeys(protected.imageIDsByRef, keys)
		out.SkipDetails = []string{"exact image generation has an active image pin"}
		return out
	}
	if keySetContainsAny(protected.taskRefs, keys...) {
		out.Protected = true
		out.SkipReason = "active_task"
		out.MatchedTaskIDs = imageCacheDetailsForKeys(protected.taskIDsByRef, keys)
		out.SkipDetails = []string{"exact image generation is referenced by an active node/image replication task"}
		return out
	}
	if ids := imageCacheReplicaIDsForManifest(protected.minReplicaKeeperRefs, manifest, keys); len(ids) > 0 {
		out.Protected = true
		out.SkipReason = "minimum_replica_count"
		out.MatchedReplicaIDs = ids
		out.MatchedImageIDs = imageCacheDetailsForKeys(protected.imageIDsByRef, keys)
		out.SkipDetails = []string{"node-aware minimum replica keeper protects this local copy"}
		return out
	}
	ageBase := manifest.LastSeenAt
	if manifest.CreatedAtObserved != nil {
		ageBase = *manifest.CreatedAtObserved
	}
	if !ageBase.IsZero() && now.Sub(ageBase) < defaultImageCacheOrphanGracePeriod {
		out.Protected = true
		out.SkipReason = "recent_manifest"
		out.SkipDetails = []string{"manifest is newer than the configured minimum age"}
		return out
	}
	if keySetContainsAny(protected.lostRefs, keys...) {
		out.Reason = "lost_image"
		out.MatchedImageIDs = imageCacheDetailsForKeys(protected.imageIDsByRef, keys)
		return out
	}
	if keySetContainsAny(protected.deletedRefs, keys...) {
		out.Reason = "deleted_image_generation"
		out.MatchedImageIDs = imageCacheDetailsForKeys(protected.imageIDsByRef, keys)
		return out
	}
	if reason, ok := imageCacheReplicaCandidateForManifest(protected.replicaCandidateRefs, manifest, keys); ok {
		out.Reason = reason
		out.MatchedReplicaIDs = imageCacheReplicaIDsForManifest(protected.replicaCandidateRefs, manifest, keys)
		out.MatchedImageIDs = imageCacheDetailsForKeys(protected.imageIDsByRef, keys)
		return out
	}
	if keySetContainsAny(protected.minReplicaRefs, keys...) {
		out.Protected = true
		out.SkipReason = "minimum_replica_count"
		out.MatchedReplicaIDs = imageCacheDetailsForKeys(protected.replicaIDsByRef, keys)
		out.MatchedImageIDs = imageCacheDetailsForKeys(protected.imageIDsByRef, keys)
		out.SkipDetails = []string{"no healthy replica exists; keep one exact image generation until repair or retention marks it removable"}
		return out
	}
	out.Reason = "missing_control_plane_image"
	return out
}

func imageCacheReplicaCandidateReason(status string) string {
	switch strings.TrimSpace(status) {
	case model.ImageReplicaStatusStale, model.ImageReplicaStatusFailed, model.ImageReplicaStatusMissing:
		return "stale_replica"
	default:
		return ""
	}
}

func addImageCacheReplicaCandidate(set map[string][]imageCacheReplicaCandidate, keys []string, candidate imageCacheReplicaCandidate) {
	if set == nil || candidate.Reason == "" {
		return
	}
	for _, key := range keys {
		key = strings.ToLower(strings.TrimSpace(key))
		if key == "" {
			continue
		}
		set[key] = append(set[key], candidate)
	}
}

func imageCacheReplicaCandidateForManifest(set map[string][]imageCacheReplicaCandidate, manifest model.ImageCacheManifest, keys []string) (string, bool) {
	for _, key := range keys {
		for _, candidate := range set[strings.ToLower(strings.TrimSpace(key))] {
			if imageCacheReplicaCandidateMatchesManifest(candidate, manifest) {
				return candidate.Reason, true
			}
		}
	}
	return "", false
}

func imageCacheReplicaIDsForManifest(set map[string][]imageCacheReplicaCandidate, manifest model.ImageCacheManifest, keys []string) []string {
	out := []string{}
	for _, key := range keys {
		for _, candidate := range set[strings.ToLower(strings.TrimSpace(key))] {
			if candidate.ReplicaID == "" || !imageCacheReplicaCandidateMatchesManifest(candidate, manifest) {
				continue
			}
			out = append(out, candidate.ReplicaID)
		}
	}
	return uniqueNonEmptyStrings(out)
}

func imageCacheReplicaCandidateMatchesManifest(candidate imageCacheReplicaCandidate, manifest model.ImageCacheManifest) bool {
	if candidate.ClusterNodeName != "" && manifest.ClusterNodeName != "" && candidate.ClusterNodeName == manifest.ClusterNodeName {
		return true
	}
	if candidate.NodeID != "" && manifest.NodeID != "" && candidate.NodeID == manifest.NodeID {
		return true
	}
	if candidate.RuntimeID != "" && manifest.RuntimeID != "" && candidate.RuntimeID == manifest.RuntimeID {
		return true
	}
	return false
}

func (s *Server) findNodeUpdaterForImageCachePlan(plan model.ImageCachePrunePlan) (model.NodeUpdater, error) {
	updaters, err := s.store.ListNodeUpdaters("", true)
	if err != nil {
		return model.NodeUpdater{}, err
	}
	for _, updater := range updaters {
		if !strings.EqualFold(strings.TrimSpace(updater.Status), model.NodeUpdaterStatusActive) {
			continue
		}
		if plan.NodeID != "" && strings.TrimSpace(updater.MachineID) != plan.NodeID {
			continue
		}
		if plan.ClusterNodeName != "" && strings.TrimSpace(updater.ClusterNodeName) != plan.ClusterNodeName {
			continue
		}
		if plan.RuntimeID != "" && strings.TrimSpace(updater.RuntimeID) != plan.RuntimeID {
			continue
		}
		supported, err := s.store.NodeUpdaterTargetSupportsTask(updater.ID, updater.ClusterNodeName, updater.RuntimeID, model.NodeUpdateTaskTypePruneImageCache)
		if err != nil {
			return model.NodeUpdater{}, err
		}
		if supported {
			return updater, nil
		}
	}
	return model.NodeUpdater{}, store.ErrNotFound
}

func pruneTaskTargets(candidates []model.ImageCachePruneCandidate, limit int) []map[string]string {
	if limit <= 0 || limit > len(candidates) {
		limit = len(candidates)
	}
	out := make([]map[string]string, 0, limit)
	for _, candidate := range candidates {
		if candidate.Protected {
			continue
		}
		out = append(out, map[string]string{
			"repo":   candidate.Repo,
			"target": candidate.Target,
			"digest": candidate.Digest,
		})
		if len(out) >= limit {
			break
		}
	}
	return out
}

func evaluateLocalPVDecommissionSafety(in model.LocalPVInventory) (bool, []string) {
	reasons := append([]string(nil), in.UnsafeReasons...)
	if in.LVCount != 0 {
		reasons = append(reasons, "active_lvs_present")
	}
	if in.ActiveLVCount != 0 {
		reasons = append(reasons, "active_lvs_present")
	}
	if in.BoundPVCount != 0 {
		reasons = append(reasons, "bound_pvs_present")
	}
	if strings.TrimSpace(in.ImagePath) == "" {
		reasons = append(reasons, "image_path_missing")
	}
	if strings.TrimSpace(in.LoopDevice) == "" {
		reasons = append(reasons, "loop_device_missing")
	}
	if strings.TrimSpace(in.LoopBackingFile) != "" && strings.TrimSpace(in.ImagePath) != "" && strings.TrimSpace(in.LoopBackingFile) != strings.TrimSpace(in.ImagePath) {
		reasons = append(reasons, "loop_backing_file_mismatch")
	}
	reasons = uniqueNonEmptyStrings(reasons)
	return len(reasons) == 0, reasons
}

func normalizeImageCachePruneAPIMode(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", model.ImageCachePruneModeObserve:
		return model.ImageCachePruneModeObserve
	case "dryrun", "dry_run", model.ImageCachePruneModeDryRun:
		return model.ImageCachePruneModeDryRun
	case model.ImageCachePruneModeDelete:
		return model.ImageCachePruneModeDelete
	default:
		return ""
	}
}

func imageReferenceKeys(ref, digest string) []string {
	return imagecachekeys.ImageReferenceKeys(ref, digest)
}

func exactImageReferenceKeys(ref, digest string) []string {
	return imagecachekeys.ExactImageReferenceKeys(ref, digest)
}

func manifestReferenceKeys(repo, target, digest, imageRef string) []string {
	return imagecachekeys.ManifestReferenceKeys(repo, target, digest, imageRef)
}

func exactManifestReferenceKeys(repo, target, digest, imageRef string) []string {
	return imagecachekeys.ExactManifestReferenceKeys(repo, target, digest, imageRef)
}

func addKeys(set map[string]struct{}, values ...string) {
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			continue
		}
		set[value] = struct{}{}
	}
}

func addImageCacheDetail(set map[string][]string, keys []string, value string) {
	value = strings.TrimSpace(value)
	if set == nil || value == "" {
		return
	}
	for _, key := range keys {
		key = strings.ToLower(strings.TrimSpace(key))
		if key == "" {
			continue
		}
		set[key] = append(set[key], value)
	}
}

func imageCacheDetailsForKeys(set map[string][]string, keys []string) []string {
	out := []string{}
	for _, key := range keys {
		key = strings.ToLower(strings.TrimSpace(key))
		if key == "" {
			continue
		}
		out = append(out, set[key]...)
	}
	return uniqueNonEmptyStrings(out)
}

func healthyImageReplicasAPI(replicas []model.ImageReplica, now time.Time) []model.ImageReplica {
	out := make([]model.ImageReplica, 0, len(replicas))
	for _, replica := range replicas {
		if replica.Status != model.ImageReplicaStatusPresent {
			continue
		}
		if replica.LeaseExpiresAt != nil && replica.LeaseExpiresAt.Before(now) {
			continue
		}
		out = append(out, replica)
	}
	return out
}

func imageCacheReplicationTaskObsolete(task model.ImageReplicationTask, image model.Image) bool {
	switch strings.TrimSpace(image.LifecycleState) {
	case model.ImageLifecycleDeleting, model.ImageLifecycleDeleted, model.ImageLifecycleLost:
		return strings.TrimSpace(task.Priority) != model.ImageReplicationPriorityDeployBlocking
	default:
		return false
	}
}

func imageCacheNodeUpdateTaskObsolete(task model.NodeUpdateTask, imageByID map[string]model.Image) bool {
	if task.Type != model.NodeUpdateTaskTypeReplicateAppImage {
		return false
	}
	image := imageByID[strings.TrimSpace(task.Payload["image_id"])]
	if strings.TrimSpace(image.ID) == "" {
		return strings.TrimSpace(task.Payload["image_id"]) != ""
	}
	return imageCacheReplicationTaskObsolete(model.ImageReplicationTask{Priority: task.Payload["priority"]}, image)
}

func imageCacheNodePressure(node model.ImageCacheNodeInventory) bool {
	if node.FilesystemUsedPercent >= 85 {
		return true
	}
	return strings.Contains(strings.ToLower(strings.TrimSpace(node.Status)), "pressure")
}

func keySetContainsAny(set map[string]struct{}, values ...string) bool {
	for _, value := range values {
		if _, ok := set[strings.ToLower(strings.TrimSpace(value))]; ok {
			return true
		}
	}
	return false
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

func parseImageCacheBoolQuery(raw string) bool {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func uniqueNonEmptyStrings(values []string) []string {
	seen := map[string]struct{}{}
	out := []string{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
