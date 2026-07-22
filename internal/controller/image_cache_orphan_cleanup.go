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

	"fugue/internal/imagecachekeys"
	"fugue/internal/model"
	"fugue/internal/store"
)

const (
	defaultImageCachePruneGlobalConcurrency = 1
	defaultImageCachePruneNodeCooldown      = 30 * time.Minute
	defaultImageCachePressurePruneBudget    = int64(25 << 30)
)

type nodeImageCachePrunePlan struct {
	node model.ImageCacheNodeInventory
	plan model.ImageCachePrunePlan
}

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
	if mode == model.ImageCachePruneModeDelete {
		failedTask, halted, err := s.controllerImageCacheAutomaticPruneFailedTask()
		if err != nil {
			return err
		}
		if halted {
			if s.Logger != nil {
				s.Logger.Printf("halt image-cache orphan auto prune: previous controller prune task failed task=%s node=%s error=%s", failedTask.ID, failedTask.ClusterNodeName, failedTask.ErrorMessage)
			}
			return nil
		}
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
	plans := make([]nodeImageCachePrunePlan, 0, len(nodes))
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
		plans = append(plans, nodeImageCachePrunePlan{node: node, plan: plan})
	}
	if mode == model.ImageCachePruneModeObserve {
		return nil
	}
	if mode == model.ImageCachePruneModeDelete {
		for _, item := range plans {
			if reason := controllerImageCacheAutomaticDeleteUnsafeReason(item.plan); reason != "" {
				if s.Logger != nil {
					s.Logger.Printf("halt image-cache orphan auto prune: unsafe candidate reason=%s node=%s plan=%s", reason, item.plan.ClusterNodeName, item.plan.ID)
				}
				return nil
			}
		}
	}
	sortControllerImageCachePrunePlans(plans)
	updaters, err := s.Store.ListNodeUpdaters("", true)
	if err != nil {
		return err
	}
	activeGlobal, err := s.controllerImageCacheRunningPruneTaskCount()
	if err != nil {
		return err
	}
	if activeGlobal >= defaultImageCachePruneGlobalConcurrency {
		if s.Logger != nil {
			s.Logger.Printf("skip image-cache orphan prune scheduling: global prune running limit reached active=%d limit=%d", activeGlobal, defaultImageCachePruneGlobalConcurrency)
		}
		return nil
	}
	for _, item := range plans {
		if err := ctx.Err(); err != nil {
			return err
		}
		node := item.node
		plan := item.plan
		if plan.CandidateManifestCount == 0 && plan.CandidateBlobCount == 0 {
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
		active, err := s.controllerImageCacheHasActivePruneTask(updater.ID)
		if err != nil {
			return err
		}
		if active {
			if s.Logger != nil {
				s.Logger.Printf("skip image-cache orphan prune for node=%s updater=%s: prune task already pending or running", plan.ClusterNodeName, updater.ID)
			}
			continue
		}
		coolingDown, cooldownUntil, err := s.controllerImageCacheNodePruneCoolingDown(updater.ID)
		if err != nil {
			return err
		}
		if coolingDown {
			if s.Logger != nil {
				s.Logger.Printf("skip image-cache orphan prune for node=%s updater=%s: cooldown until %s", plan.ClusterNodeName, updater.ID, cooldownUntil.Format(time.RFC3339))
			}
			continue
		}
		targets := controllerImageCachePruneTargets(plan.Candidates, s.Config.ImageStoreOrphanPruneMaxTargetsPerNode)
		if len(targets) == 0 && plan.CandidateBlobCount == 0 {
			continue
		}
		targetsRaw, err := json.Marshal(targets)
		if err != nil {
			return err
		}
		dryRun := mode != model.ImageCachePruneModeDelete
		allowDelete := mode == model.ImageCachePruneModeDelete
		if _, err := s.Store.CreateNodeUpdateTask(controllerImageCachePrunePrincipal(), updater.ID, updater.ClusterNodeName, updater.RuntimeID, model.NodeUpdateTaskTypePruneImageCache, map[string]string{
			"prune_plan_id":              plan.ID,
			"dry_run":                    fmt.Sprintf("%t", dryRun),
			"allow_delete":               fmt.Sprintf("%t", allowDelete),
			"targets_json":               string(targetsRaw),
			"max_delete_bytes":           fmt.Sprintf("%d", plan.MaxDeleteBytes),
			"min_manifest_age":           s.controllerImageCacheGracePeriod().String(),
			"include_unreferenced_blobs": fmt.Sprintf("%t", plan.CandidateBlobCount > 0),
			"candidate_blob_count":       fmt.Sprintf("%d", plan.CandidateBlobCount),
			"candidate_blob_bytes":       fmt.Sprintf("%d", plan.CandidateBlobBytes),
			"prune_reason":               "image-cache-orphan",
		}); err != nil {
			if !errors.Is(err, store.ErrInvalidInput) {
				return err
			}
		} else {
			activeGlobal++
			if activeGlobal >= defaultImageCachePruneGlobalConcurrency {
				break
			}
		}
	}
	return nil
}

func (s *Service) controllerImageCacheAutomaticPruneFailedTask() (model.NodeUpdateTask, bool, error) {
	if s == nil || s.Store == nil {
		return model.NodeUpdateTask{}, false, nil
	}
	tasks, err := s.Store.ListNodeUpdateTasks("", true, "", model.NodeUpdateTaskStatusFailed)
	if err != nil {
		return model.NodeUpdateTask{}, false, err
	}
	principal := controllerImageCachePrunePrincipal()
	for _, task := range tasks {
		if task.Type != model.NodeUpdateTaskTypePruneImageCache {
			continue
		}
		if strings.TrimSpace(task.RequestedByID) != principal.ActorID {
			continue
		}
		if strings.TrimSpace(task.Payload["prune_reason"]) != "image-cache-orphan" {
			continue
		}
		return task, true, nil
	}
	return model.NodeUpdateTask{}, false, nil
}

func (s *Service) controllerImageCacheHasActivePruneTask(updaterID string) (bool, error) {
	if s == nil || s.Store == nil {
		return false, nil
	}
	updaterID = strings.TrimSpace(updaterID)
	if updaterID == "" {
		return false, nil
	}
	for _, status := range []string{model.NodeUpdateTaskStatusPending, model.NodeUpdateTaskStatusRunning} {
		tasks, err := s.Store.ListNodeUpdateTasks("", true, updaterID, status)
		if err != nil {
			return false, err
		}
		for _, task := range tasks {
			if task.Type == model.NodeUpdateTaskTypePruneImageCache {
				return true, nil
			}
		}
	}
	return false, nil
}

func (s *Service) controllerImageCacheRunningPruneTaskCount() (int, error) {
	if s == nil || s.Store == nil {
		return 0, nil
	}
	count := 0
	tasks, err := s.Store.ListNodeUpdateTasks("", true, "", model.NodeUpdateTaskStatusRunning)
	if err != nil {
		return 0, err
	}
	for _, task := range tasks {
		if controllerImageCacheControllerPruneTask(task) {
			count++
		}
	}
	return count, nil
}

func sortControllerImageCachePrunePlans(plans []nodeImageCachePrunePlan) {
	sort.SliceStable(plans, func(i, j int) bool {
		left := plans[i].plan
		right := plans[j].plan
		if left.NodePressure != right.NodePressure {
			return left.NodePressure
		}
		leftBytes := controllerImageCachePruneCandidateBytes(left)
		rightBytes := controllerImageCachePruneCandidateBytes(right)
		if leftBytes != rightBytes {
			return leftBytes > rightBytes
		}
		if left.CandidateBlobBytes != right.CandidateBlobBytes {
			return left.CandidateBlobBytes > right.CandidateBlobBytes
		}
		leftCandidates := left.CandidateManifestCount + left.CandidateBlobCount
		rightCandidates := right.CandidateManifestCount + right.CandidateBlobCount
		if leftCandidates != rightCandidates {
			return leftCandidates > rightCandidates
		}
		leftName := firstNonEmptyImageCacheControllerString(left.ClusterNodeName, left.NodeID, left.RuntimeID)
		rightName := firstNonEmptyImageCacheControllerString(right.ClusterNodeName, right.NodeID, right.RuntimeID)
		return leftName < rightName
	})
}

func controllerImageCachePruneCandidateBytes(plan model.ImageCachePrunePlan) int64 {
	total := plan.CandidateBlobBytes
	for _, candidate := range plan.Candidates {
		total += candidate.PlannedDeleteBytes
	}
	return total
}

func (s *Service) controllerImageCacheNodePruneCoolingDown(updaterID string) (bool, time.Time, error) {
	if s == nil || s.Store == nil {
		return false, time.Time{}, nil
	}
	updaterID = strings.TrimSpace(updaterID)
	if updaterID == "" {
		return false, time.Time{}, nil
	}
	now := time.Now().UTC()
	for _, status := range []string{model.NodeUpdateTaskStatusCompleted, model.NodeUpdateTaskStatusFailed} {
		tasks, err := s.Store.ListNodeUpdateTasks("", true, updaterID, status)
		if err != nil {
			return false, time.Time{}, err
		}
		for _, task := range tasks {
			if !controllerImageCacheControllerPruneTask(task) {
				continue
			}
			finished := task.UpdatedAt
			if task.CompletedAt != nil {
				finished = *task.CompletedAt
			}
			if finished.IsZero() {
				finished = task.CreatedAt
			}
			cooldownUntil := finished.UTC().Add(defaultImageCachePruneNodeCooldown)
			if cooldownUntil.After(now) {
				return true, cooldownUntil, nil
			}
			break
		}
	}
	return false, time.Time{}, nil
}

func controllerImageCacheControllerPruneTask(task model.NodeUpdateTask) bool {
	if task.Type != model.NodeUpdateTaskTypePruneImageCache {
		return false
	}
	if strings.TrimSpace(task.RequestedByID) != controllerImageCachePrunePrincipal().ActorID {
		return false
	}
	return strings.TrimSpace(task.Payload["prune_reason"]) == "image-cache-orphan"
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
		MaxDeleteBytes:    s.controllerImageCacheMaxDeleteBytesForNode(node),
		MinManifestAge:    s.controllerImageCacheGracePeriod().String(),
		ProtectionSummary: map[string]int{},
		CandidateSummary:  map[string]int{},
		CreatedAt:         now,
		Status:            model.ImageCachePrunePlanStatusPlanned,
		NodePressure:      controllerImageCacheNodePressure(node),
	}
	for _, blob := range node.UnreferencedBlobs {
		if strings.TrimSpace(blob.Digest) == "" {
			continue
		}
		if blob.Reason == "" {
			blob.Reason = "unreferenced_blob"
		}
		if blob.PlannedDeleteBytes == 0 {
			blob.PlannedDeleteBytes = firstNonZeroControllerInt64(blob.SizeBytes)
		}
		if strings.TrimSpace(blob.NodeName) == "" {
			blob.NodeName = firstNonEmptyImageCacheControllerString(node.ClusterNodeName, node.NodeID, node.RuntimeID)
		}
		plan.UnreferencedBlobs = append(plan.UnreferencedBlobs, blob)
		plan.CandidateBlobCount++
		blobBytes := firstNonZeroControllerInt64(blob.PlannedDeleteBytes, blob.SizeBytes)
		plan.CandidateBlobBytes += blobBytes
		plan.PlannedDeleteBytes += blobBytes
		plan.CandidateSummary[blob.Reason]++
	}
	classified := make([]model.ImageCachePruneCandidate, 0, len(manifests))
	for _, manifest := range manifests {
		classified = append(classified, s.controllerImageCacheCandidate(manifest, protected, now))
	}
	classified = protectControllerImageCacheSharedDigestAliases(classified)
	for _, candidate := range classified {
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
	if plan.MaxDeleteBytes > 0 && plan.PlannedDeleteBytes > plan.MaxDeleteBytes {
		plan.BudgetExhausted = true
		plan.PlannedDeleteBytes = plan.MaxDeleteBytes
	}
	sort.SliceStable(plan.ProtectedManifests, func(i, j int) bool {
		return plan.ProtectedManifests[i].PlannedDeleteBytes > plan.ProtectedManifests[j].PlannedDeleteBytes
	})
	_ = ctx
	return plan, nil
}

func protectControllerImageCacheSharedDigestAliases(candidates []model.ImageCachePruneCandidate) []model.ImageCachePruneCandidate {
	protectedByDigest := map[string][]model.ImageCachePruneCandidate{}
	for _, candidate := range candidates {
		if !candidate.Protected {
			continue
		}
		key := controllerImageCacheDigestGroupKey(candidate.Repo, candidate.Digest)
		if key == "" {
			continue
		}
		protectedByDigest[key] = append(protectedByDigest[key], candidate)
	}
	for key := range protectedByDigest {
		sort.SliceStable(protectedByDigest[key], func(i, j int) bool {
			left := protectedByDigest[key][i]
			right := protectedByDigest[key][j]
			if left.Target != right.Target {
				return left.Target < right.Target
			}
			return left.SkipReason < right.SkipReason
		})
	}
	for idx := range candidates {
		candidate := &candidates[idx]
		if candidate.Protected {
			continue
		}
		protectors := protectedByDigest[controllerImageCacheDigestGroupKey(candidate.Repo, candidate.Digest)]
		if len(protectors) == 0 {
			continue
		}
		protector := protectors[0]
		candidate.Protected = true
		candidate.Reason = ""
		candidate.SkipReason = "shared_digest_protected_alias"
		candidate.SkipDetails = []string{fmt.Sprintf(
			"same repository digest is protected by target %q (%s)",
			protector.Target,
			protector.SkipReason,
		)}
		candidate.MatchedImageIDs = dedupeControllerStrings(append(candidate.MatchedImageIDs, protector.MatchedImageIDs...))
		candidate.MatchedPinIDs = dedupeControllerStrings(append(candidate.MatchedPinIDs, protector.MatchedPinIDs...))
		candidate.MatchedTaskIDs = dedupeControllerStrings(append(candidate.MatchedTaskIDs, protector.MatchedTaskIDs...))
		candidate.MatchedWorkloadRefs = dedupeControllerStrings(append(candidate.MatchedWorkloadRefs, protector.MatchedWorkloadRefs...))
		candidate.MatchedReplicaIDs = dedupeControllerStrings(append(candidate.MatchedReplicaIDs, protector.MatchedReplicaIDs...))
	}
	return candidates
}

func controllerImageCacheDigestGroupKey(repo, digest string) string {
	repo = strings.ToLower(strings.Trim(strings.TrimSpace(repo), "/"))
	digest = normalizeControllerImageCacheDigest(digest)
	if repo == "" || digest == "" {
		return ""
	}
	return repo + "\x00" + digest
}

type controllerImageCacheProtectedSet struct {
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
	minReplicaKeeperRefs map[string][]controllerImageCacheReplicaCandidate
	replicaCandidateRefs map[string][]controllerImageCacheReplicaCandidate
}

type controllerImageCacheReplicaCandidate struct {
	NodeID          string
	RuntimeID       string
	ClusterNodeName string
	Reason          string
	ReplicaID       string
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
		imageIDsByRef:        map[string][]string{},
		pinIDsByRef:          map[string][]string{},
		taskIDsByRef:         map[string][]string{},
		workloadRefsByRef:    map[string][]string{},
		replicaIDsByRef:      map[string][]string{},
		minReplicaKeeperRefs: map[string][]controllerImageCacheReplicaCandidate{},
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
		addControllerImageDetails(protected.imageIDsByRef, keys, image.ID)
		switch strings.TrimSpace(image.LifecycleState) {
		case model.ImageLifecycleAvailable:
			// Available images are protected by node-aware minimum replica
			// keepers below, not by a repo-wide available-image key.
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
		addControllerImageDetails(protected.imageIDsByRef, keys, image.ID)
		switch strings.TrimSpace(image.LifecycleState) {
		case model.ImageLifecycleAvailable:
			// Available aliases are protected by node-aware minimum replica
			// keepers below, not by a repo-wide available-image key.
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
		keys := controllerImageReferenceKeys(image.ImageRef, image.CanonicalDigest)
		addControllerImageKeys(protected.pinnedRefs, keys...)
		addControllerImageDetails(protected.pinIDsByRef, keys, pin.ID)
	}
	apps, err := s.Store.ListAppsMetadata("", true)
	if err == nil {
		for ref := range s.liveManagedImageRefSet(ctx, apps) {
			keys := controllerImageReferenceKeys(ref, "")
			addControllerImageKeys(protected.liveRefs, keys...)
			addControllerImageDetails(protected.workloadRefsByRef, keys, ref)
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
			if controllerNodeUpdateTaskObsolete(task, imageByID) {
				continue
			}
			keys := controllerImageReferenceKeys(task.Payload["image_ref"], task.Payload["digest"])
			addControllerImageKeys(protected.taskRefs, keys...)
			addControllerImageDetails(protected.taskIDsByRef, keys, task.ID)
			keys = controllerImageReferenceKeys(task.Payload["images"], "")
			addControllerImageKeys(protected.taskRefs, keys...)
			addControllerImageDetails(protected.taskIDsByRef, keys, task.ID)
			if raw := strings.TrimSpace(task.Payload["targets_json"]); raw != "" {
				var targets []struct {
					Repo   string `json:"repo"`
					Target string `json:"target"`
					Digest string `json:"digest"`
				}
				if json.Unmarshal([]byte(raw), &targets) == nil {
					for _, target := range targets {
						keys := controllerManifestReferenceKeys(target.Repo, target.Target, target.Digest, "")
						addControllerImageKeys(protected.taskRefs, keys...)
						addControllerImageDetails(protected.taskIDsByRef, keys, task.ID)
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
			if strings.TrimSpace(image.ID) == "" || controllerImageReplicationTaskObsolete(task, image) {
				continue
			}
			keys := controllerImageReferenceKeys(image.ImageRef, image.CanonicalDigest)
			addControllerImageKeys(protected.taskRefs, keys...)
			addControllerImageDetails(protected.taskIDsByRef, keys, task.ID)
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
		healthy := healthyImageReplicas(replicas, now)
		keys := controllerImageReferenceKeys(image.ImageRef, image.CanonicalDigest)
		if len(healthy) == 0 {
			addControllerImageKeys(protected.minReplicaRefs, keys...)
			addControllerImageDetails(protected.imageIDsByRef, keys, image.ID)
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
			if idx < minReplicas {
				addControllerImageCacheReplicaCandidate(protected.minReplicaKeeperRefs, keys, controllerImageCacheReplicaCandidate{
					NodeID:          strings.TrimSpace(replica.NodeID),
					RuntimeID:       strings.TrimSpace(replica.RuntimeID),
					ClusterNodeName: strings.TrimSpace(replica.ClusterNodeName),
					Reason:          "minimum_replica_count",
					ReplicaID:       strings.TrimSpace(replica.ID),
				})
				addControllerImageDetails(protected.replicaIDsByRef, keys, replica.ID)
				addControllerImageDetails(protected.imageIDsByRef, keys, image.ID)
				continue
			}
			addControllerImageCacheReplicaCandidate(protected.replicaCandidateRefs, keys, controllerImageCacheReplicaCandidate{
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
				ReplicaID:       strings.TrimSpace(replica.ID),
			})
		}
	}
	return nil
}

func (s *Service) controllerImageCacheCandidate(manifest model.ImageCacheManifest, protected controllerImageCacheProtectedSet, now time.Time) model.ImageCachePruneCandidate {
	keys := controllerManifestReferenceKeys(manifest.Repo, manifest.Target, manifest.Digest, manifest.ImageRef)
	out := model.ImageCachePruneCandidate{
		ImageRef:            manifest.ImageRef,
		NodeName:            firstNonEmptyImageCacheControllerString(manifest.ClusterNodeName, manifest.NodeID, manifest.RuntimeID),
		Repo:                manifest.Repo,
		Target:              manifest.Target,
		Digest:              manifest.Digest,
		ReferencedBlobs:     append([]string(nil), manifest.ReferencedBlobs...),
		PlannedDeleteBytes:  firstNonZeroControllerInt64(manifest.TotalBlobBytes, manifest.ManifestSizeBytes),
		ReferencedBlobCount: len(manifest.ReferencedBlobs),
		ReferencedBlobBytes: manifest.TotalBlobBytes,
		LastSeenAt:          manifest.LastSeenAt.UTC().Format(time.RFC3339),
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
	} {
		if controllerKeySetContainsAny(rule.refs, keys...) {
			out.Protected = true
			out.SkipReason = rule.reason
			switch rule.reason {
			case "current_workload":
				out.MatchedWorkloadRefs = controllerImageDetailsForKeys(protected.workloadRefsByRef, keys)
				out.SkipDetails = []string{"exact image generation is used by a live workload"}
			case "active_pin":
				out.MatchedPinIDs = controllerImageDetailsForKeys(protected.pinIDsByRef, keys)
				out.MatchedImageIDs = controllerImageDetailsForKeys(protected.imageIDsByRef, keys)
				out.SkipDetails = []string{"exact image generation has an active image pin"}
			case "active_task":
				out.MatchedTaskIDs = controllerImageDetailsForKeys(protected.taskIDsByRef, keys)
				out.SkipDetails = []string{"exact image generation is referenced by an active node/image replication task"}
			case "minimum_replica_count":
				out.MatchedReplicaIDs = controllerImageDetailsForKeys(protected.replicaIDsByRef, keys)
				out.MatchedImageIDs = controllerImageDetailsForKeys(protected.imageIDsByRef, keys)
				out.SkipDetails = []string{"node-aware minimum replica keeper protects this local copy"}
			}
			return out
		}
	}
	if manifest.PinnedLocally {
		out.Protected = true
		out.SkipReason = "local_pin"
		out.SkipDetails = []string{"manifest is pinned locally on the node"}
		return out
	}
	if ids := controllerImageCacheReplicaIDsForManifest(protected.minReplicaKeeperRefs, manifest, keys); len(ids) > 0 {
		out.Protected = true
		out.SkipReason = "minimum_replica_count"
		out.MatchedReplicaIDs = ids
		out.MatchedImageIDs = controllerImageDetailsForKeys(protected.imageIDsByRef, keys)
		out.SkipDetails = []string{"node-aware minimum replica keeper protects this local copy"}
		return out
	}
	ageBase := manifest.LastSeenAt
	if manifest.CreatedAtObserved != nil {
		ageBase = *manifest.CreatedAtObserved
	}
	if !ageBase.IsZero() && now.Sub(ageBase) < s.controllerImageCacheGracePeriod() {
		out.Protected = true
		out.SkipReason = "recent_manifest"
		out.SkipDetails = []string{"manifest is newer than the configured minimum age"}
		return out
	}
	if controllerKeySetContainsAny(protected.lostRefs, keys...) {
		out.Reason = "lost_image"
		out.MatchedImageIDs = controllerImageDetailsForKeys(protected.imageIDsByRef, keys)
		return out
	}
	if controllerKeySetContainsAny(protected.deletedRefs, keys...) {
		out.Reason = "deleted_image_generation"
		out.MatchedImageIDs = controllerImageDetailsForKeys(protected.imageIDsByRef, keys)
		return out
	}
	if reason, ok := controllerImageCacheReplicaCandidateForManifest(protected.replicaCandidateRefs, manifest, keys); ok {
		out.Reason = reason
		out.MatchedReplicaIDs = controllerImageCacheReplicaIDsForManifest(protected.replicaCandidateRefs, manifest, keys)
		out.MatchedImageIDs = controllerImageDetailsForKeys(protected.imageIDsByRef, keys)
		return out
	}
	if controllerKeySetContainsAny(protected.minReplicaRefs, keys...) {
		out.Protected = true
		out.SkipReason = "minimum_replica_count"
		out.MatchedReplicaIDs = controllerImageDetailsForKeys(protected.replicaIDsByRef, keys)
		out.MatchedImageIDs = controllerImageDetailsForKeys(protected.imageIDsByRef, keys)
		out.SkipDetails = []string{"no healthy replica exists; keep one exact image generation until repair or retention marks it removable"}
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

func controllerImageCacheReplicaIDsForManifest(set map[string][]controllerImageCacheReplicaCandidate, manifest model.ImageCacheManifest, keys []string) []string {
	out := []string{}
	for _, key := range keys {
		for _, candidate := range set[strings.ToLower(strings.TrimSpace(key))] {
			if candidate.ReplicaID == "" || !controllerImageCacheReplicaCandidateMatchesManifest(candidate, manifest) {
				continue
			}
			out = append(out, candidate.ReplicaID)
		}
	}
	return dedupeControllerStrings(out)
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

func controllerImageCacheAutomaticDeleteUnsafeReason(plan model.ImageCachePrunePlan) string {
	for _, candidate := range plan.Candidates {
		if candidate.Protected {
			return "protected_candidate"
		}
		reason := strings.TrimSpace(candidate.Reason)
		switch reason {
		case "missing_control_plane_image", "lost_image", "deleted_image_generation", "stale_replica", "excess_replica":
			continue
		default:
			if reason == "" {
				return "empty_candidate_reason"
			}
			return reason
		}
	}
	return ""
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
		return "10Gi"
	}
	return value
}

func (s *Service) controllerImageCacheMaxDeleteBytes() int64 {
	return parseControllerImageCacheByteSize(s.controllerImageCacheMaxDeleteBytesString())
}

func (s *Service) controllerImageCacheMaxDeleteBytesForNode(node model.ImageCacheNodeInventory) int64 {
	value := s.controllerImageCacheMaxDeleteBytes()
	if controllerImageCacheNodePressure(node) && value > 0 && value < defaultImageCachePressurePruneBudget {
		return defaultImageCachePressurePruneBudget
	}
	return value
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
		return 10 << 30
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
		return 10 << 30
	}
	return int64(value * float64(multiplier))
}

func controllerImageReferenceKeys(imageRef, digest string) []string {
	return imagecachekeys.ExactImageReferenceKeys(imageRef, digest)
}

func controllerManifestReferenceKeys(repo, target, digest, imageRef string) []string {
	return imagecachekeys.ExactManifestReferenceKeys(repo, target, digest, imageRef)
}

func normalizeControllerImageCacheDigest(digest string) string {
	return imagecachekeys.NormalizeDigest(digest)
}

func addControllerImageKeys(set map[string]struct{}, keys ...string) {
	for _, key := range keys {
		key = strings.ToLower(strings.TrimSpace(key))
		if key != "" {
			set[key] = struct{}{}
		}
	}
}

func addControllerImageDetails(set map[string][]string, keys []string, value string) {
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

func controllerImageDetailsForKeys(set map[string][]string, keys []string) []string {
	out := []string{}
	for _, key := range keys {
		key = strings.ToLower(strings.TrimSpace(key))
		if key == "" {
			continue
		}
		out = append(out, set[key]...)
	}
	return dedupeControllerStrings(out)
}

func controllerKeySetContainsAny(set map[string]struct{}, keys ...string) bool {
	for _, key := range keys {
		if _, ok := set[strings.ToLower(strings.TrimSpace(key))]; ok {
			return true
		}
	}
	return false
}

func controllerImageCacheNodePressure(node model.ImageCacheNodeInventory) bool {
	if node.FilesystemUsedPercent >= 85 {
		return true
	}
	return strings.Contains(strings.ToLower(strings.TrimSpace(node.Status)), "pressure")
}

func controllerNodeUpdateTaskObsolete(task model.NodeUpdateTask, imageByID map[string]model.Image) bool {
	if task.Type != model.NodeUpdateTaskTypeReplicateAppImage {
		return false
	}
	image := imageByID[strings.TrimSpace(task.Payload["image_id"])]
	if strings.TrimSpace(image.ID) == "" {
		return strings.TrimSpace(task.Payload["image_id"]) != ""
	}
	return controllerImageReplicationTaskObsolete(model.ImageReplicationTask{Priority: task.Payload["priority"]}, image)
}

func controllerImageReplicationTaskObsolete(task model.ImageReplicationTask, image model.Image) bool {
	switch strings.TrimSpace(image.LifecycleState) {
	case model.ImageLifecycleDeleting, model.ImageLifecycleDeleted, model.ImageLifecycleLost:
		return strings.TrimSpace(task.Priority) != model.ImageReplicationPriorityDeployBlocking
	default:
		return false
	}
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
