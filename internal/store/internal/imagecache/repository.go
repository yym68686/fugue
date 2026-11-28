package imagecache

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"time"

	"fugue/internal/imagecacheevidence"
	"fugue/internal/imagecachepolicy"
	"fugue/internal/model"
)

func (s Repository) UpsertImageCacheInventory(node model.ImageCacheNodeInventory, manifests []model.ImageCacheManifest) (model.ImageCacheNodeInventory, error) {
	node = normalizeImageCacheNodeInventory(node)
	if node.ManifestCount == 0 {
		node.ManifestCount = len(manifests)
	}
	if node.NodeID == "" && node.ClusterNodeName == "" {
		return model.ImageCacheNodeInventory{}, s.invalidInput
	}
	return s.pgUpsertImageCacheInventory(node, manifests)
}

func (s Repository) ListImageCacheNodeInventories(filter model.ImageCacheNodeInventoryFilter) ([]model.ImageCacheNodeInventory, error) {
	filter = normalizeImageCacheNodeInventoryFilter(filter)
	return s.pgListImageCacheNodeInventories(filter)
}

func (s Repository) ListImageCacheManifests(filter model.ImageCacheManifestFilter) ([]model.ImageCacheManifest, error) {
	filter = normalizeImageCacheManifestFilter(filter)
	return s.pgListImageCacheManifests(filter)
}

func (s Repository) UpsertImageCachePrunePlan(plan model.ImageCachePrunePlan) (model.ImageCachePrunePlan, error) {
	plan = normalizeImageCachePrunePlan(plan)
	if plan.NodeID == "" && plan.ClusterNodeName == "" {
		return model.ImageCachePrunePlan{}, s.invalidInput
	}
	if plan.Mode == "" || plan.Status == "" {
		return model.ImageCachePrunePlan{}, s.invalidInput
	}
	return s.pgUpsertImageCachePrunePlan(plan)
}

// UpsertImageCachePrunePlanSummary persists the bounded diagnostic projection
// of an automatically generated plan. The full plan hash is retained so a
// scheduled task can still bind its exact target payload to the explanation;
// the detailed per-image rows are not copied into long-lived control-plane
// history. Explicit admin previews continue to use UpsertImageCachePrunePlan.
func (s Repository) UpsertImageCachePrunePlanSummary(plan model.ImageCachePrunePlan) (model.ImageCachePrunePlan, error) {
	plan = normalizeImageCachePrunePlan(plan)
	if plan.CreatedAt.IsZero() {
		plan.CreatedAt = time.Now().UTC()
	}
	if plan.NodeID == "" && plan.ClusterNodeName == "" {
		return model.ImageCachePrunePlan{}, s.invalidInput
	}
	if plan.Mode == "" || plan.Status == "" {
		return model.ImageCachePrunePlan{}, s.invalidInput
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

func (s Repository) ListImageCachePrunePlans(filter model.ImageCachePrunePlanFilter) ([]model.ImageCachePrunePlan, error) {
	filter = normalizeImageCachePrunePlanFilter(filter)
	return s.pgListImageCachePrunePlans(filter)
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

func stripImageNUL(value string) string {
	return strings.ReplaceAll(value, "\x00", "")
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

func firstNonEmptyImageCacheString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func normalizeStringList(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
