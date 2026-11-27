package store

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"

	"fugue/internal/imagecacheevidence"
	"fugue/internal/imagecachepolicy"
	"fugue/internal/model"
)

func (s *Store) UpsertImageCacheInventory(node model.ImageCacheNodeInventory, manifests []model.ImageCacheManifest) (model.ImageCacheNodeInventory, error) {
	node = normalizeImageCacheNodeInventory(node)
	if node.ManifestCount == 0 {
		node.ManifestCount = len(manifests)
	}
	if node.NodeID == "" && node.ClusterNodeName == "" {
		return model.ImageCacheNodeInventory{}, ErrInvalidInput
	}
	return s.pgUpsertImageCacheInventory(node, manifests)
}

func (s *Store) ListImageCacheNodeInventories(filter model.ImageCacheNodeInventoryFilter) ([]model.ImageCacheNodeInventory, error) {
	filter = normalizeImageCacheNodeInventoryFilter(filter)
	return s.pgListImageCacheNodeInventories(filter)
}

func (s *Store) ListImageCacheManifests(filter model.ImageCacheManifestFilter) ([]model.ImageCacheManifest, error) {
	filter = normalizeImageCacheManifestFilter(filter)
	return s.pgListImageCacheManifests(filter)
}

func (s *Store) UpsertImageCachePrunePlan(plan model.ImageCachePrunePlan) (model.ImageCachePrunePlan, error) {
	plan = normalizeImageCachePrunePlan(plan)
	if plan.NodeID == "" && plan.ClusterNodeName == "" {
		return model.ImageCachePrunePlan{}, ErrInvalidInput
	}
	if plan.Mode == "" || plan.Status == "" {
		return model.ImageCachePrunePlan{}, ErrInvalidInput
	}
	return s.pgUpsertImageCachePrunePlan(plan)
}

// UpsertImageCachePrunePlanSummary persists the bounded diagnostic projection
// of an automatically generated plan. The full plan hash is retained so a
// scheduled task can still bind its exact target payload to the explanation;
// the detailed per-image rows are not copied into long-lived control-plane
// history. Explicit admin previews continue to use UpsertImageCachePrunePlan.
func (s *Store) UpsertImageCachePrunePlanSummary(plan model.ImageCachePrunePlan) (model.ImageCachePrunePlan, error) {
	plan = normalizeImageCachePrunePlan(plan)
	if plan.CreatedAt.IsZero() {
		plan.CreatedAt = time.Now().UTC()
	}
	if plan.NodeID == "" && plan.ClusterNodeName == "" {
		return model.ImageCachePrunePlan{}, ErrInvalidInput
	}
	if plan.Mode == "" || plan.Status == "" {
		return model.ImageCachePrunePlan{}, ErrInvalidInput
	}
	// Automatic scheduler passes describe the same bounded observation until
	// the underlying inventory/policy hash changes. Give that projection a
	// stable identity so repeated blocked/cooldown passes update one receipt
	// instead of appending another row on every controller tick.
	plan.ID = imageCachePruneSummaryID(plan)
	plan.Candidates = nil
	plan.ProtectedManifests = nil
	plan.SkippedManifests = nil
	plan.UnreferencedBlobs = nil
	return s.pgUpsertImageCachePrunePlan(plan)
}

func imageCachePruneSummaryID(plan model.ImageCachePrunePlan) string {
	identity := plan.NodeID + "\x00" + plan.ClusterNodeName + "\x00" + plan.RuntimeID + "\x00" + plan.Mode + "\x00" + plan.PlanHash
	sum := sha256.Sum256([]byte(identity))
	return "imgcacheprune-summary-" + hex.EncodeToString(sum[:16])
}

func (s *Store) ListImageCachePrunePlans(filter model.ImageCachePrunePlanFilter) ([]model.ImageCachePrunePlan, error) {
	filter = normalizeImageCachePrunePlanFilter(filter)
	return s.pgListImageCachePrunePlans(filter)
}

func (s *Store) UpsertLocalPVInventory(inventory model.LocalPVInventory) (model.LocalPVInventory, error) {
	inventory = normalizeLocalPVInventory(inventory)
	if inventory.NodeID == "" && inventory.ClusterNodeName == "" {
		return model.LocalPVInventory{}, ErrInvalidInput
	}
	return s.pgUpsertLocalPVInventory(inventory)
}

func (s *Store) ListLocalPVInventories(filter model.LocalPVInventoryFilter) ([]model.LocalPVInventory, error) {
	filter = normalizeLocalPVInventoryFilter(filter)
	return s.pgListLocalPVInventories(filter)
}

func normalizeImageCacheNodeInventory(in model.ImageCacheNodeInventory) model.ImageCacheNodeInventory {
	in.ID = strings.TrimSpace(in.ID)
	in.NodeID = strings.TrimSpace(in.NodeID)
	in.ClusterNodeName = strings.TrimSpace(in.ClusterNodeName)
	in.RuntimeID = strings.TrimSpace(in.RuntimeID)
	in.CacheEndpoint = strings.TrimRight(strings.TrimSpace(in.CacheEndpoint), "/")
	in.StorePath = strings.TrimSpace(in.StorePath)
	in.Status = strings.TrimSpace(in.Status)
	in.LastError = strings.TrimSpace(in.LastError)
	in.ReportedByNodeUpdaterID = strings.TrimSpace(in.ReportedByNodeUpdaterID)
	if in.ObservedAt.IsZero() {
		in.ObservedAt = time.Now().UTC()
	}
	if in.Status == "" {
		in.Status = "ok"
	}
	return in
}

func normalizeImageCacheNodeInventoryFilter(filter model.ImageCacheNodeInventoryFilter) model.ImageCacheNodeInventoryFilter {
	filter.NodeID = strings.TrimSpace(filter.NodeID)
	filter.ClusterNodeName = strings.TrimSpace(filter.ClusterNodeName)
	filter.RuntimeID = strings.TrimSpace(filter.RuntimeID)
	return filter
}

func normalizeImageCacheManifest(in model.ImageCacheManifest) model.ImageCacheManifest {
	in.ID = stripImageNUL(in.ID)
	in.NodeID = stripImageNUL(in.NodeID)
	in.ClusterNodeName = stripImageNUL(in.ClusterNodeName)
	in.RuntimeID = stripImageNUL(in.RuntimeID)
	in.ImageRef = stripImageNUL(in.ImageRef)
	in.Repo = stripImageNUL(in.Repo)
	in.Target = stripImageNUL(in.Target)
	in.Digest = stripImageNUL(in.Digest)
	in.MediaType = stripImageNUL(in.MediaType)
	in.ID = strings.TrimSpace(in.ID)
	in.NodeID = strings.TrimSpace(in.NodeID)
	in.ClusterNodeName = strings.TrimSpace(in.ClusterNodeName)
	in.RuntimeID = strings.TrimSpace(in.RuntimeID)
	in.ImageRef = strings.TrimSpace(in.ImageRef)
	in.Repo = strings.Trim(strings.TrimSpace(in.Repo), "/")
	in.Target = strings.TrimSpace(in.Target)
	in.Digest = strings.TrimSpace(in.Digest)
	in.MediaType = strings.TrimSpace(in.MediaType)
	in.GraphStatus = imagecacheevidence.NormalizeGraphStatus(in.GraphStatus)
	if imagecacheevidence.GraphIsIncomplete(in.GraphStatus) {
		in.GraphFailureReason = imagecacheevidence.NormalizeGraphFailureReason(in.GraphFailureReason)
		in.ManifestSizeBytes = 0
		in.TotalBlobBytes = 0
		in.ReferencedBlobs = nil
		in.ReferencedManifests = nil
	} else {
		in.GraphFailureReason = ""
		in.ReferencedBlobs = normalizeStringList(in.ReferencedBlobs)
		in.ReferencedManifests = normalizeStringList(in.ReferencedManifests)
	}
	return in
}

func normalizeImageCacheManifestFilter(filter model.ImageCacheManifestFilter) model.ImageCacheManifestFilter {
	filter.NodeID = strings.TrimSpace(filter.NodeID)
	filter.ClusterNodeName = strings.TrimSpace(filter.ClusterNodeName)
	filter.RuntimeID = strings.TrimSpace(filter.RuntimeID)
	filter.Repo = strings.Trim(strings.TrimSpace(filter.Repo), "/")
	filter.Target = strings.TrimSpace(filter.Target)
	filter.Digest = strings.TrimSpace(filter.Digest)
	return filter
}

func normalizeImageCachePrunePlan(in model.ImageCachePrunePlan) model.ImageCachePrunePlan {
	in.ID = strings.TrimSpace(in.ID)
	in.NodeID = strings.TrimSpace(in.NodeID)
	in.ClusterNodeName = strings.TrimSpace(in.ClusterNodeName)
	in.RuntimeID = strings.TrimSpace(in.RuntimeID)
	in.Mode = normalizeImageCachePruneMode(in.Mode)
	in.Status = normalizeImageCachePrunePlanStatus(in.Status)
	in.MinManifestAge = strings.TrimSpace(in.MinManifestAge)
	in.Error = strings.TrimSpace(in.Error)
	if in.Status == "" {
		in.Status = model.ImageCachePrunePlanStatusPlanned
	}
	imagecachepolicy.Seal(&in)
	return in
}

func normalizeImageCachePrunePlanFilter(filter model.ImageCachePrunePlanFilter) model.ImageCachePrunePlanFilter {
	filter.NodeID = strings.TrimSpace(filter.NodeID)
	filter.ClusterNodeName = strings.TrimSpace(filter.ClusterNodeName)
	filter.RuntimeID = strings.TrimSpace(filter.RuntimeID)
	filter.Mode = normalizeImageCachePruneMode(filter.Mode)
	filter.Status = normalizeImageCachePrunePlanStatus(filter.Status)
	return filter
}

func normalizeImageCachePruneMode(raw string) string {
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

func normalizeImageCachePrunePlanStatus(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", model.ImageCachePrunePlanStatusPlanned:
		return model.ImageCachePrunePlanStatusPlanned
	case model.ImageCachePrunePlanStatusScheduled:
		return model.ImageCachePrunePlanStatusScheduled
	case model.ImageCachePrunePlanStatusCompleted:
		return model.ImageCachePrunePlanStatusCompleted
	case model.ImageCachePrunePlanStatusFailed:
		return model.ImageCachePrunePlanStatusFailed
	default:
		return ""
	}
}

func normalizeLocalPVInventory(in model.LocalPVInventory) model.LocalPVInventory {
	in.ID = strings.TrimSpace(in.ID)
	in.NodeID = strings.TrimSpace(in.NodeID)
	in.ClusterNodeName = strings.TrimSpace(in.ClusterNodeName)
	in.RuntimeID = strings.TrimSpace(in.RuntimeID)
	in.NodeRoles = normalizeStringList(in.NodeRoles)
	in.VGName = strings.TrimSpace(in.VGName)
	in.ImagePath = strings.TrimSpace(in.ImagePath)
	in.LoopDevice = strings.TrimSpace(in.LoopDevice)
	in.LoopBackingFile = strings.TrimSpace(in.LoopBackingFile)
	in.LVNames = normalizeStringList(in.LVNames)
	in.BoundPVCRefs = normalizeStringList(in.BoundPVCRefs)
	in.UnsafeReasons = normalizeStringList(in.UnsafeReasons)
	for _, reason := range in.UnsafeReasons {
		if reason == "kubectl_pv_unavailable" || reason == "bound_pv_count_unknown" {
			in.BoundPVCount = -1
			break
		}
	}
	if in.BoundPVCount < 0 {
		in.BoundPVCountKnown = false
	} else if !in.BoundPVCountKnown {
		// Legacy callers did not have the explicit known bit. A non-negative
		// count from those callers is a real observation; -1 is the unknown
		// sentinel emitted when kubectl was unavailable.
		in.BoundPVCountKnown = true
	}
	in.ReportedByNodeUpdaterID = strings.TrimSpace(in.ReportedByNodeUpdaterID)
	if in.ObservedAt.IsZero() {
		in.ObservedAt = time.Now().UTC()
	}
	return in
}

func normalizeLocalPVInventoryFilter(filter model.LocalPVInventoryFilter) model.LocalPVInventoryFilter {
	filter.NodeID = strings.TrimSpace(filter.NodeID)
	filter.ClusterNodeName = strings.TrimSpace(filter.ClusterNodeName)
	filter.RuntimeID = strings.TrimSpace(filter.RuntimeID)
	return filter
}

func firstNonEmptyImageCacheString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
