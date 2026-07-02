package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

const (
	defaultImageCacheInventoryTTL       = 2 * time.Hour
	defaultImageCacheOrphanGracePeriod  = 24 * time.Hour
	defaultImageCachePruneMaxDeleteByte = int64(1 << 30)
	defaultImageCachePruneMaxTargets    = 50
)

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
	if plan.CandidateManifestCount == 0 || mode == model.ImageCachePruneModeObserve {
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
		"prune_plan_id":    plan.ID,
		"dry_run":          fmt.Sprintf("%t", dryRun),
		"allow_delete":     fmt.Sprintf("%t", allowDelete),
		"targets_json":     string(targetsRaw),
		"max_delete_bytes": fmt.Sprintf("%d", plan.MaxDeleteBytes),
		"prune_reason":     "image-cache-orphan",
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
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"inventories": inventories})
}

type imageCacheInventoryReport struct {
	Node        model.ImageCacheNodeInventory       `json:"node"`
	Manifests   []imageCacheInventoryManifestReport `json:"manifests"`
	ObservedAt  *time.Time                          `json:"observed_at"`
	TotalCount  int                                 `json:"manifest_total_count"`
	ChunkIndex  int                                 `json:"chunk_index"`
	ChunkCount  int                                 `json:"chunk_count"`
	Endpoint    string                              `json:"endpoint"`
	ClusterNode string                              `json:"cluster_node"`
	Pins        []any                               `json:"pins"`
	Disk        imageCacheInventoryDiskReport       `json:"disk"`
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
	node.ObservedAt = observedAt
	node.ManifestCount = firstNonZeroInt(node.ManifestCount, req.TotalCount, len(req.Manifests))
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

func (s *Server) computeImageCachePrunePlan(r *http.Request, filter model.ImageCachePrunePlanFilter) (model.ImageCachePrunePlan, error) {
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
	if err := s.populateImageCacheProtectedSet(r, &protected); err != nil {
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
		plan.PlannedDeleteBytes = plan.MaxDeleteBytes
	}
	return plan, nil
}

type imageCacheProtectedSet struct {
	availableRefs        map[string]struct{}
	lostRefs             map[string]struct{}
	deletedRefs          map[string]struct{}
	pinnedRefs           map[string]struct{}
	liveRefs             map[string]struct{}
	taskRefs             map[string]struct{}
	replicaCandidateRefs map[string][]imageCacheReplicaCandidate
}

type imageCacheReplicaCandidate struct {
	NodeID          string
	RuntimeID       string
	ClusterNodeName string
	Reason          string
}

func (s *Server) populateImageCacheProtectedSet(r *http.Request, protected *imageCacheProtectedSet) error {
	protected.availableRefs = map[string]struct{}{}
	protected.lostRefs = map[string]struct{}{}
	protected.deletedRefs = map[string]struct{}{}
	protected.pinnedRefs = map[string]struct{}{}
	protected.liveRefs = map[string]struct{}{}
	protected.taskRefs = map[string]struct{}{}
	protected.replicaCandidateRefs = map[string][]imageCacheReplicaCandidate{}

	images, err := s.store.ListImages(model.ImageFilter{PlatformAdmin: true})
	if err != nil {
		return err
	}
	imageByID := map[string]model.Image{}
	for _, image := range images {
		imageByID[image.ID] = image
		keys := imageReferenceKeys(image.ImageRef, image.CanonicalDigest)
		switch strings.TrimSpace(image.LifecycleState) {
		case model.ImageLifecycleAvailable:
			addKeys(protected.availableRefs, keys...)
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
		keys := imageReferenceKeys(alias.AliasRef, firstNonEmptyImageAPIString(alias.Digest, image.CanonicalDigest))
		switch strings.TrimSpace(image.LifecycleState) {
		case model.ImageLifecycleAvailable:
			addKeys(protected.availableRefs, keys...)
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
		addKeys(protected.pinnedRefs, imageReferenceKeys(image.ImageRef, image.CanonicalDigest)...)
	}
	apps, err := s.store.ListAppsMetadata("", true)
	if err == nil {
		for ref := range s.liveManagedImageRefSet(r.Context(), apps) {
			addKeys(protected.liveRefs, imageReferenceKeys(ref, "")...)
		}
	}
	for _, status := range []string{model.NodeUpdateTaskStatusPending, model.NodeUpdateTaskStatusRunning} {
		tasks, err := s.store.ListNodeUpdateTasks("", true, "", status)
		if err != nil {
			return err
		}
		for _, task := range tasks {
			addKeys(protected.taskRefs, imageReferenceKeys(task.Payload["image_ref"], task.Payload["digest"])...)
			addKeys(protected.taskRefs, imageReferenceKeys(task.Payload["images"], "")...)
			if raw := strings.TrimSpace(task.Payload["targets_json"]); raw != "" {
				var targets []struct {
					Repo   string `json:"repo"`
					Target string `json:"target"`
					Digest string `json:"digest"`
				}
				if json.Unmarshal([]byte(raw), &targets) == nil {
					for _, target := range targets {
						addKeys(protected.taskRefs, manifestReferenceKeys(target.Repo, target.Target, target.Digest, "")...)
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
			addKeys(protected.taskRefs, imageReferenceKeys(image.ImageRef, image.CanonicalDigest)...)
		}
	}
	if err := s.populateImageCacheReplicaCandidateRefs(protected, images); err != nil {
		return err
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
		keys := imageReferenceKeys(image.ImageRef, image.CanonicalDigest)
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
			})
		}
	}
	return nil
}

func imageCachePruneCandidateForManifest(manifest model.ImageCacheManifest, protected imageCacheProtectedSet, now time.Time) model.ImageCachePruneCandidate {
	keys := manifestReferenceKeys(manifest.Repo, manifest.Target, manifest.Digest, manifest.ImageRef)
	out := model.ImageCachePruneCandidate{
		ImageRef:           manifest.ImageRef,
		Repo:               manifest.Repo,
		Target:             manifest.Target,
		Digest:             manifest.Digest,
		ReferencedBlobs:    append([]string(nil), manifest.ReferencedBlobs...),
		PlannedDeleteBytes: firstNonZeroInt64(manifest.TotalBlobBytes, manifest.ManifestSizeBytes),
		LastSeenAt:         manifest.LastSeenAt.UTC().Format(time.RFC3339),
	}
	if manifest.CreatedAtObserved != nil {
		out.CreatedAtObserved = manifest.CreatedAtObserved.UTC().Format(time.RFC3339)
	}
	if manifest.PinnedLocally {
		out.Protected = true
		out.SkipReason = "local_pin"
		return out
	}
	if keySetContainsAny(protected.liveRefs, keys...) {
		out.Protected = true
		out.SkipReason = "current_workload"
		return out
	}
	if keySetContainsAny(protected.pinnedRefs, keys...) {
		out.Protected = true
		out.SkipReason = "active_pin"
		return out
	}
	if keySetContainsAny(protected.taskRefs, keys...) {
		out.Protected = true
		out.SkipReason = "active_task"
		return out
	}
	ageBase := manifest.LastSeenAt
	if manifest.CreatedAtObserved != nil {
		ageBase = *manifest.CreatedAtObserved
	}
	if !ageBase.IsZero() && now.Sub(ageBase) < defaultImageCacheOrphanGracePeriod {
		out.Protected = true
		out.SkipReason = "recent_manifest"
		return out
	}
	if keySetContainsAny(protected.availableRefs, keys...) {
		if _, ok := imageCacheReplicaCandidateForManifest(protected.replicaCandidateRefs, manifest, keys); !ok {
			out.Protected = true
			out.SkipReason = "available_image"
			return out
		}
	}
	if keySetContainsAny(protected.lostRefs, keys...) {
		out.Reason = "lost_image"
		return out
	}
	if keySetContainsAny(protected.deletedRefs, keys...) {
		out.Reason = "deleted_image_generation"
		return out
	}
	if reason, ok := imageCacheReplicaCandidateForManifest(protected.replicaCandidateRefs, manifest, keys); ok {
		out.Reason = reason
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
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		set[key] = append(set[key], candidate)
	}
}

func imageCacheReplicaCandidateForManifest(set map[string][]imageCacheReplicaCandidate, manifest model.ImageCacheManifest, keys []string) (string, bool) {
	for _, key := range keys {
		for _, candidate := range set[strings.TrimSpace(key)] {
			if imageCacheReplicaCandidateMatchesManifest(candidate, manifest) {
				return candidate.Reason, true
			}
		}
	}
	return "", false
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
	refs := strings.Fields(strings.NewReplacer(",", " ").Replace(strings.TrimSpace(ref)))
	out := []string{}
	for _, value := range refs {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		out = append(out, value)
		withoutRegistry := value
		if idx := strings.Index(withoutRegistry, "/"); idx >= 0 && strings.Contains(withoutRegistry[:idx], ".") || strings.Contains(strings.SplitN(withoutRegistry, "/", 2)[0], ":") {
			if slash := strings.Index(withoutRegistry, "/"); slash >= 0 {
				withoutRegistry = withoutRegistry[slash+1:]
			}
		}
		out = append(out, withoutRegistry)
		if strings.Contains(withoutRegistry, "@") {
			parts := strings.SplitN(withoutRegistry, "@", 2)
			out = append(out, parts[0], parts[1])
		} else if idx := strings.LastIndex(withoutRegistry, ":"); idx > 0 {
			out = append(out, withoutRegistry[:idx], withoutRegistry[idx+1:])
		}
	}
	if strings.TrimSpace(digest) != "" {
		out = append(out, strings.TrimSpace(digest))
	}
	return uniqueNonEmptyStrings(out)
}

func manifestReferenceKeys(repo, target, digest, imageRef string) []string {
	keys := imageReferenceKeys(imageRef, digest)
	repo = strings.Trim(strings.TrimSpace(repo), "/")
	target = strings.TrimSpace(target)
	if repo != "" {
		keys = append(keys, repo)
		if target != "" {
			keys = append(keys, repo+":"+target, repo+"@"+target)
		}
	}
	if target != "" {
		keys = append(keys, target)
	}
	if digest != "" {
		keys = append(keys, digest)
		if repo != "" {
			keys = append(keys, repo+"@"+digest)
		}
	}
	return uniqueNonEmptyStrings(keys)
}

func addKeys(set map[string]struct{}, values ...string) {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		set[value] = struct{}{}
	}
}

func keySetContainsAny(set map[string]struct{}, values ...string) bool {
	for _, value := range values {
		if _, ok := set[strings.TrimSpace(value)]; ok {
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
