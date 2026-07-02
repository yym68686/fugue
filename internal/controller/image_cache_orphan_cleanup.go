package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

func (s *Service) runImageCacheStorageMaintenance(ctx context.Context) error {
	if err := s.scheduleImageCacheInventoryReports(ctx); err != nil {
		return err
	}
	return s.scheduleOrphanImageCachePrune(ctx)
}

func (s *Service) scheduleImageCacheInventoryReports(ctx context.Context) error {
	if s == nil || s.Store == nil || !s.Config.ImageCacheInventoryEnabled {
		return nil
	}
	updaters, err := s.Store.ListNodeUpdaters("", true)
	if err != nil {
		return err
	}
	principal := model.Principal{
		ActorType: model.ActorTypeSystem,
		ActorID:   "fugue-controller/image-cache-inventory",
		Scopes:    map[string]struct{}{"platform.admin": {}},
	}
	for _, updater := range updaters {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !strings.EqualFold(strings.TrimSpace(updater.Status), model.NodeUpdaterStatusActive) {
			continue
		}
		supported, err := s.Store.NodeUpdaterTargetSupportsTask(updater.ID, updater.ClusterNodeName, updater.RuntimeID, model.NodeUpdateTaskTypeReportImageCache)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrInvalidInput) {
				continue
			}
			return err
		}
		if !supported {
			continue
		}
		if _, err := s.Store.CreateNodeUpdateTask(principal, updater.ID, updater.ClusterNodeName, updater.RuntimeID, model.NodeUpdateTaskTypeReportImageCache, map[string]string{
			"reason": "scheduled-image-cache-inventory",
		}); err != nil && !errors.Is(err, store.ErrInvalidInput) {
			return err
		}
	}
	return nil
}

func (s *Service) scheduleOrphanImageCachePrune(ctx context.Context) error {
	if s == nil || s.Store == nil {
		return nil
	}
	mode := normalizeControllerImageCachePruneMode(s.Config.ImageStoreOrphanPruneMode)
	if mode == "" {
		return nil
	}
	ttl := s.Config.ImageCacheInventoryTTL
	if ttl <= 0 {
		ttl = 2 * time.Hour
	}
	nodes, err := s.Store.ListImageCacheNodeInventories(model.ImageCacheNodeInventoryFilter{
		StaleAfter: time.Now().UTC().Add(-ttl),
	})
	if err != nil {
		return err
	}
	if len(nodes) == 0 {
		return nil
	}
	protected, err := s.controllerImageCacheProtectedSet(ctx)
	if err != nil {
		return err
	}
	updaters, err := s.Store.ListNodeUpdaters("", true)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if err := ctx.Err(); err != nil {
			return err
		}
		plan, err := s.computeControllerImageCachePrunePlan(ctx, node, protected, mode)
		if err != nil {
			return err
		}
		plan, err = s.Store.UpsertImageCachePrunePlan(plan)
		if err != nil {
			return err
		}
		if mode == model.ImageCachePruneModeObserve || plan.CandidateManifestCount == 0 {
			continue
		}
		updater, ok := controllerImageCacheUpdaterForNode(updaters, node)
		if !ok {
			continue
		}
		supported, err := s.Store.NodeUpdaterTargetSupportsTask(updater.ID, updater.ClusterNodeName, updater.RuntimeID, model.NodeUpdateTaskTypePruneImageCache)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrInvalidInput) {
				continue
			}
			return err
		}
		if !supported {
			continue
		}
		targets := controllerImageCachePruneTargets(plan.Candidates, s.Config.ImageStoreOrphanPruneMaxTargetsPerNode)
		if len(targets) == 0 {
			continue
		}
		targetsRaw, err := json.Marshal(targets)
		if err != nil {
			return err
		}
		dryRun := mode != model.ImageCachePruneModeDelete
		allowDelete := mode == model.ImageCachePruneModeDelete
		if _, err := s.Store.CreateNodeUpdateTask(controllerImageCachePrunePrincipal(), updater.ID, updater.ClusterNodeName, updater.RuntimeID, model.NodeUpdateTaskTypePruneImageCache, map[string]string{
			"prune_plan_id":    plan.ID,
			"dry_run":          fmt.Sprintf("%t", dryRun),
			"allow_delete":     fmt.Sprintf("%t", allowDelete),
			"targets_json":     string(targetsRaw),
			"max_delete_bytes": s.controllerImageCacheMaxDeleteBytesString(),
			"min_manifest_age": s.controllerImageCacheGracePeriod().String(),
			"prune_reason":     "image-cache-orphan",
		}); err != nil && !errors.Is(err, store.ErrInvalidInput) {
			return err
		}
	}
	return nil
}

func controllerImageCacheUpdaterForNode(updaters []model.NodeUpdater, node model.ImageCacheNodeInventory) (model.NodeUpdater, bool) {
	for _, updater := range updaters {
		if !strings.EqualFold(strings.TrimSpace(updater.Status), model.NodeUpdaterStatusActive) {
			continue
		}
		if node.ClusterNodeName != "" {
			if strings.TrimSpace(updater.ClusterNodeName) != "" && strings.TrimSpace(updater.ClusterNodeName) != strings.TrimSpace(node.ClusterNodeName) {
				continue
			}
			return updater, true
		}
		if node.RuntimeID != "" && strings.TrimSpace(updater.RuntimeID) != "" && strings.TrimSpace(updater.RuntimeID) != strings.TrimSpace(node.RuntimeID) {
			continue
		}
		if node.NodeID != "" && strings.TrimSpace(updater.MachineID) != "" && strings.TrimSpace(updater.MachineID) != strings.TrimSpace(node.NodeID) {
			continue
		}
		return updater, true
	}
	return model.NodeUpdater{}, false
}

func (s *Service) computeControllerImageCachePrunePlan(ctx context.Context, node model.ImageCacheNodeInventory, protected controllerImageCacheProtectedSet, mode string) (model.ImageCachePrunePlan, error) {
	manifests, err := s.Store.ListImageCacheManifests(model.ImageCacheManifestFilter{
		NodeID:          node.NodeID,
		ClusterNodeName: node.ClusterNodeName,
		RuntimeID:       node.RuntimeID,
		SeenAfter:       s.controllerImageCacheInventorySeenAfter(),
		PresentOnly:     true,
	})
	if err != nil {
		return model.ImageCachePrunePlan{}, err
	}
	now := time.Now().UTC()
	plan := model.ImageCachePrunePlan{
		ID:                model.NewID("imgcacheprune"),
		NodeID:            node.NodeID,
		ClusterNodeName:   node.ClusterNodeName,
		RuntimeID:         node.RuntimeID,
		Mode:              mode,
		MaxDeleteBytes:    s.controllerImageCacheMaxDeleteBytes(),
		MinManifestAge:    s.controllerImageCacheGracePeriod().String(),
		ProtectionSummary: map[string]int{},
		CandidateSummary:  map[string]int{},
		CreatedAt:         now,
		Status:            model.ImageCachePrunePlanStatusPlanned,
	}
	for _, manifest := range manifests {
		candidate := s.controllerImageCacheCandidate(manifest, protected, now)
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
	if plan.MaxDeleteBytes > 0 && plan.PlannedDeleteBytes > plan.MaxDeleteBytes {
		plan.PlannedDeleteBytes = plan.MaxDeleteBytes
	}
	_ = ctx
	return plan, nil
}

type controllerImageCacheProtectedSet struct {
	availableRefs        map[string]struct{}
	lostRefs             map[string]struct{}
	deletedRefs          map[string]struct{}
	pinnedRefs           map[string]struct{}
	liveRefs             map[string]struct{}
	taskRefs             map[string]struct{}
	minReplicaRefs       map[string]struct{}
	replicaCandidateRefs map[string][]controllerImageCacheReplicaCandidate
}

type controllerImageCacheReplicaCandidate struct {
	NodeID          string
	RuntimeID       string
	ClusterNodeName string
	Reason          string
}

func (s *Service) controllerImageCacheProtectedSet(ctx context.Context) (controllerImageCacheProtectedSet, error) {
	protected := controllerImageCacheProtectedSet{
		availableRefs:        map[string]struct{}{},
		lostRefs:             map[string]struct{}{},
		deletedRefs:          map[string]struct{}{},
		pinnedRefs:           map[string]struct{}{},
		liveRefs:             map[string]struct{}{},
		taskRefs:             map[string]struct{}{},
		minReplicaRefs:       map[string]struct{}{},
		replicaCandidateRefs: map[string][]controllerImageCacheReplicaCandidate{},
	}
	images, err := s.Store.ListImages(model.ImageFilter{PlatformAdmin: true})
	if err != nil {
		return protected, err
	}
	imageByID := map[string]model.Image{}
	for _, image := range images {
		imageByID[image.ID] = image
		keys := controllerImageReferenceKeys(image.ImageRef, image.CanonicalDigest)
		switch strings.TrimSpace(image.LifecycleState) {
		case model.ImageLifecycleAvailable:
			addControllerImageKeys(protected.availableRefs, keys...)
		case model.ImageLifecycleLost:
			addControllerImageKeys(protected.lostRefs, keys...)
		case model.ImageLifecycleDeleting, model.ImageLifecycleDeleted:
			addControllerImageKeys(protected.deletedRefs, keys...)
		}
	}
	aliases, err := s.Store.ListImageAliases(model.ImageAliasFilter{PlatformAdmin: true})
	if err != nil {
		return protected, err
	}
	for _, alias := range aliases {
		image := imageByID[alias.ImageID]
		keys := controllerImageReferenceKeys(alias.AliasRef, firstNonEmptyImageCacheControllerString(alias.Digest, image.CanonicalDigest))
		switch strings.TrimSpace(image.LifecycleState) {
		case model.ImageLifecycleAvailable:
			addControllerImageKeys(protected.availableRefs, keys...)
		case model.ImageLifecycleLost:
			addControllerImageKeys(protected.lostRefs, keys...)
		case model.ImageLifecycleDeleting, model.ImageLifecycleDeleted:
			addControllerImageKeys(protected.deletedRefs, keys...)
		}
	}
	pins, err := s.Store.ListImagePins(model.ImagePinFilter{PlatformAdmin: true})
	if err != nil {
		return protected, err
	}
	now := time.Now().UTC()
	for _, pin := range pins {
		if pin.ExpiresAt != nil && pin.ExpiresAt.Before(now) {
			continue
		}
		image := imageByID[pin.ImageID]
		addControllerImageKeys(protected.pinnedRefs, controllerImageReferenceKeys(image.ImageRef, image.CanonicalDigest)...)
	}
	apps, err := s.Store.ListAppsMetadata("", true)
	if err == nil {
		for ref := range s.liveManagedImageRefSet(ctx, apps) {
			addControllerImageKeys(protected.liveRefs, controllerImageReferenceKeys(ref, "")...)
		}
	}
	if err := s.populateControllerImageTaskRefs(&protected, imageByID); err != nil {
		return protected, err
	}
	if err := s.populateControllerImageMinimumReplicaRefs(&protected, images); err != nil {
		return protected, err
	}
	if err := s.populateControllerImageReplicaCandidateRefs(&protected, images); err != nil {
		return protected, err
	}
	return protected, nil
}

func (s *Service) populateControllerImageTaskRefs(protected *controllerImageCacheProtectedSet, imageByID map[string]model.Image) error {
	for _, status := range []string{model.NodeUpdateTaskStatusPending, model.NodeUpdateTaskStatusRunning} {
		tasks, err := s.Store.ListNodeUpdateTasks("", true, "", status)
		if err != nil {
			return err
		}
		for _, task := range tasks {
			addControllerImageKeys(protected.taskRefs, controllerImageReferenceKeys(task.Payload["image_ref"], task.Payload["digest"])...)
			addControllerImageKeys(protected.taskRefs, controllerImageReferenceKeys(task.Payload["images"], "")...)
			if raw := strings.TrimSpace(task.Payload["targets_json"]); raw != "" {
				var targets []struct {
					Repo   string `json:"repo"`
					Target string `json:"target"`
					Digest string `json:"digest"`
				}
				if json.Unmarshal([]byte(raw), &targets) == nil {
					for _, target := range targets {
						addControllerImageKeys(protected.taskRefs, controllerManifestReferenceKeys(target.Repo, target.Target, target.Digest, "")...)
					}
				}
			}
		}
	}
	for _, status := range []string{model.ImageReplicationTaskStatusPending, model.ImageReplicationTaskStatusRunning} {
		tasks, err := s.Store.ListImageReplicationTasks(model.ImageReplicationTaskFilter{Status: status, PlatformAdmin: true})
		if err != nil {
			return err
		}
		for _, task := range tasks {
			image := imageByID[task.ImageID]
			addControllerImageKeys(protected.taskRefs, controllerImageReferenceKeys(image.ImageRef, image.CanonicalDigest)...)
		}
	}
	return nil
}

func (s *Service) populateControllerImageMinimumReplicaRefs(protected *controllerImageCacheProtectedSet, images []model.Image) error {
	minReplicas := s.controllerImageCacheMinReplicaCount()
	if minReplicas <= 0 {
		return nil
	}
	now := time.Now().UTC()
	for _, image := range images {
		switch strings.TrimSpace(image.LifecycleState) {
		case "", model.ImageLifecycleAvailable, model.ImageLifecycleImporting:
		default:
			continue
		}
		replicas, err := s.Store.ListImageReplicas(model.ImageReplicaFilter{
			ImageID:       image.ID,
			Status:        model.ImageReplicaStatusPresent,
			PlatformAdmin: true,
		})
		if err != nil {
			return err
		}
		if len(healthyImageReplicas(replicas, now)) <= minReplicas {
			addControllerImageKeys(protected.minReplicaRefs, controllerImageReferenceKeys(image.ImageRef, image.CanonicalDigest)...)
		}
	}
	return nil
}

func (s *Service) populateControllerImageReplicaCandidateRefs(protected *controllerImageCacheProtectedSet, images []model.Image) error {
	if protected == nil {
		return nil
	}
	for _, image := range images {
		replicas, err := s.Store.ListImageReplicas(model.ImageReplicaFilter{
			ImageID:       image.ID,
			TenantID:      image.TenantID,
			PlatformAdmin: true,
		})
		if err != nil {
			return err
		}
		keys := controllerImageReferenceKeys(image.ImageRef, image.CanonicalDigest)
		for _, replica := range replicas {
			reason := controllerImageCacheReplicaCandidateReason(replica.Status)
			if reason == "" {
				continue
			}
			addControllerImageCacheReplicaCandidate(protected.replicaCandidateRefs, keys, controllerImageCacheReplicaCandidate{
				NodeID:          strings.TrimSpace(replica.NodeID),
				RuntimeID:       strings.TrimSpace(replica.RuntimeID),
				ClusterNodeName: strings.TrimSpace(replica.ClusterNodeName),
				Reason:          reason,
			})
		}
	}
	return nil
}

func (s *Service) controllerImageCacheCandidate(manifest model.ImageCacheManifest, protected controllerImageCacheProtectedSet, now time.Time) model.ImageCachePruneCandidate {
	keys := controllerManifestReferenceKeys(manifest.Repo, manifest.Target, manifest.Digest, manifest.ImageRef)
	out := model.ImageCachePruneCandidate{
		ImageRef:           manifest.ImageRef,
		Repo:               manifest.Repo,
		Target:             manifest.Target,
		Digest:             manifest.Digest,
		ReferencedBlobs:    append([]string(nil), manifest.ReferencedBlobs...),
		PlannedDeleteBytes: firstNonZeroControllerInt64(manifest.TotalBlobBytes, manifest.ManifestSizeBytes),
		LastSeenAt:         manifest.LastSeenAt.UTC().Format(time.RFC3339),
	}
	if manifest.CreatedAtObserved != nil {
		out.CreatedAtObserved = manifest.CreatedAtObserved.UTC().Format(time.RFC3339)
	}
	for _, rule := range []struct {
		refs   map[string]struct{}
		reason string
	}{
		{protected.liveRefs, "current_workload"},
		{protected.pinnedRefs, "active_pin"},
		{protected.taskRefs, "active_task"},
		{protected.minReplicaRefs, "minimum_replica_count"},
	} {
		if controllerKeySetContainsAny(rule.refs, keys...) {
			out.Protected = true
			out.SkipReason = rule.reason
			return out
		}
	}
	if manifest.PinnedLocally {
		out.Protected = true
		out.SkipReason = "local_pin"
		return out
	}
	ageBase := manifest.LastSeenAt
	if manifest.CreatedAtObserved != nil {
		ageBase = *manifest.CreatedAtObserved
	}
	if !ageBase.IsZero() && now.Sub(ageBase) < s.controllerImageCacheGracePeriod() {
		out.Protected = true
		out.SkipReason = "recent_manifest"
		return out
	}
	if controllerKeySetContainsAny(protected.availableRefs, keys...) {
		if _, ok := controllerImageCacheReplicaCandidateForManifest(protected.replicaCandidateRefs, manifest, keys); !ok {
			out.Protected = true
			out.SkipReason = "available_image"
			return out
		}
	}
	if controllerKeySetContainsAny(protected.lostRefs, keys...) {
		out.Reason = "lost_image"
		return out
	}
	if controllerKeySetContainsAny(protected.deletedRefs, keys...) {
		out.Reason = "deleted_image_generation"
		return out
	}
	if reason, ok := controllerImageCacheReplicaCandidateForManifest(protected.replicaCandidateRefs, manifest, keys); ok {
		out.Reason = reason
		return out
	}
	out.Reason = "missing_control_plane_image"
	return out
}

func controllerImageCacheReplicaCandidateReason(status string) string {
	switch strings.TrimSpace(status) {
	case model.ImageReplicaStatusStale, model.ImageReplicaStatusFailed, model.ImageReplicaStatusMissing:
		return "stale_replica"
	default:
		return ""
	}
}

func addControllerImageCacheReplicaCandidate(set map[string][]controllerImageCacheReplicaCandidate, keys []string, candidate controllerImageCacheReplicaCandidate) {
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

func controllerImageCacheReplicaCandidateForManifest(set map[string][]controllerImageCacheReplicaCandidate, manifest model.ImageCacheManifest, keys []string) (string, bool) {
	for _, key := range keys {
		for _, candidate := range set[strings.ToLower(strings.TrimSpace(key))] {
			if controllerImageCacheReplicaCandidateMatchesManifest(candidate, manifest) {
				return candidate.Reason, true
			}
		}
	}
	return "", false
}

func controllerImageCacheReplicaCandidateMatchesManifest(candidate controllerImageCacheReplicaCandidate, manifest model.ImageCacheManifest) bool {
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

func controllerImageCachePruneTargets(candidates []model.ImageCachePruneCandidate, limit int) []map[string]string {
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

func normalizeControllerImageCachePruneMode(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", "off", "disabled", "none":
		return ""
	case model.ImageCachePruneModeObserve:
		return model.ImageCachePruneModeObserve
	case model.ImageCachePruneModeDryRun, "dryrun":
		return model.ImageCachePruneModeDryRun
	case model.ImageCachePruneModeDelete:
		return model.ImageCachePruneModeDelete
	default:
		return model.ImageCachePruneModeObserve
	}
}

func (s *Service) controllerImageCacheInventorySeenAfter() time.Time {
	ttl := s.Config.ImageCacheInventoryTTL
	if ttl <= 0 {
		ttl = 2 * time.Hour
	}
	return time.Now().UTC().Add(-ttl)
}

func (s *Service) controllerImageCacheGracePeriod() time.Duration {
	if s == nil || s.Config.ImageStoreOrphanPruneGracePeriod <= 0 {
		return 24 * time.Hour
	}
	return s.Config.ImageStoreOrphanPruneGracePeriod
}

func (s *Service) controllerImageCacheMaxDeleteBytesString() string {
	value := strings.TrimSpace(s.Config.ImageStoreOrphanPruneMaxDeleteBytesPerNode)
	if value == "" {
		return "1073741824"
	}
	return value
}

func (s *Service) controllerImageCacheMaxDeleteBytes() int64 {
	return parseControllerImageCacheByteSize(s.controllerImageCacheMaxDeleteBytesString())
}

func (s *Service) controllerImageCacheMinReplicaCount() int {
	if s == nil || s.Config.ImageStoreOrphanPruneMinReplicaCount <= 0 {
		return 1
	}
	return s.Config.ImageStoreOrphanPruneMinReplicaCount
}

func controllerImageCachePrunePrincipal() model.Principal {
	return model.Principal{
		ActorType: model.ActorTypeSystem,
		ActorID:   "fugue-controller/image-cache-orphan-prune",
		Scopes:    map[string]struct{}{"platform.admin": {}},
	}
}

func parseControllerImageCacheByteSize(raw string) int64 {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 1 << 30
	}
	multiplier := int64(1)
	lower := strings.ToLower(raw)
	for suffix, value := range map[string]int64{
		"kib": 1 << 10,
		"ki":  1 << 10,
		"mib": 1 << 20,
		"mi":  1 << 20,
		"gib": 1 << 30,
		"gi":  1 << 30,
		"kb":  1000,
		"mb":  1000 * 1000,
		"gb":  1000 * 1000 * 1000,
	} {
		if strings.HasSuffix(lower, suffix) {
			multiplier = value
			raw = strings.TrimSpace(raw[:len(raw)-len(suffix)])
			break
		}
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || value <= 0 {
		return 1 << 30
	}
	return int64(value * float64(multiplier))
}

func controllerImageReferenceKeys(imageRef, digest string) []string {
	imageRef = strings.TrimSpace(imageRef)
	keys := []string{}
	if imageRef != "" {
		keys = append(keys, strings.ToLower(imageRef))
		ref := strings.TrimPrefix(imageRef, "http://")
		ref = strings.TrimPrefix(ref, "https://")
		withoutRegistry := ref
		firstSegment := strings.SplitN(withoutRegistry, "/", 2)[0]
		if strings.Contains(firstSegment, ".") || strings.Contains(firstSegment, ":") {
			if slash := strings.Index(withoutRegistry, "/"); slash >= 0 {
				withoutRegistry = withoutRegistry[slash+1:]
			}
		}
		if repo, target, ok := splitControllerImageRepoTarget(withoutRegistry); ok {
			keys = append(keys, controllerManifestReferenceKeys(repo, target, digest, "")...)
		}
	}
	if digest = normalizeControllerImageCacheDigest(digest); digest != "" {
		keys = append(keys, digest)
	}
	return dedupeControllerStrings(keys)
}

func controllerManifestReferenceKeys(repo, target, digest, imageRef string) []string {
	repo = strings.Trim(strings.TrimSpace(repo), "/")
	target = strings.TrimSpace(target)
	digest = normalizeControllerImageCacheDigest(digest)
	keys := []string{}
	if repo != "" && target != "" {
		keys = append(keys, strings.ToLower(repo+":"+target))
	}
	if repo != "" && digest != "" {
		keys = append(keys, strings.ToLower(repo+"@"+digest))
	}
	if digest != "" {
		keys = append(keys, digest)
	}
	if imageRef != "" {
		keys = append(keys, controllerImageReferenceKeys(imageRef, digest)...)
	}
	return dedupeControllerStrings(keys)
}

func splitControllerImageRepoTarget(ref string) (string, string, bool) {
	ref = strings.Trim(strings.TrimSpace(ref), "/")
	if ref == "" {
		return "", "", false
	}
	if strings.Contains(ref, "@") {
		parts := strings.SplitN(ref, "@", 2)
		return strings.Trim(parts[0], "/"), strings.TrimSpace(parts[1]), true
	}
	if idx := strings.LastIndex(ref, ":"); idx > 0 && idx+1 < len(ref) {
		return strings.Trim(ref[:idx], "/"), strings.TrimSpace(ref[idx+1:]), true
	}
	return ref, "latest", true
}

func normalizeControllerImageCacheDigest(digest string) string {
	digest = strings.ToLower(strings.TrimSpace(digest))
	if strings.HasPrefix(digest, "@") {
		digest = strings.TrimPrefix(digest, "@")
	}
	if strings.HasPrefix(digest, "sha256:") {
		return digest
	}
	return ""
}

func addControllerImageKeys(set map[string]struct{}, keys ...string) {
	for _, key := range keys {
		key = strings.ToLower(strings.TrimSpace(key))
		if key != "" {
			set[key] = struct{}{}
		}
	}
}

func controllerKeySetContainsAny(set map[string]struct{}, keys ...string) bool {
	for _, key := range keys {
		if _, ok := set[strings.ToLower(strings.TrimSpace(key))]; ok {
			return true
		}
	}
	return false
}

func dedupeControllerStrings(values []string) []string {
	seen := map[string]struct{}{}
	out := []string{}
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func firstNonEmptyImageCacheControllerString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func firstNonZeroControllerInt64(values ...int64) int64 {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}
