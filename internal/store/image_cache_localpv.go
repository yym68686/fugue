package store

import (
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) UpsertImageCacheInventory(node model.ImageCacheNodeInventory, manifests []model.ImageCacheManifest) (model.ImageCacheNodeInventory, error) {
	node = normalizeImageCacheNodeInventory(node)
	if node.NodeID == "" && node.ClusterNodeName == "" {
		return model.ImageCacheNodeInventory{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgUpsertImageCacheInventory(node, manifests)
	}
	var out model.ImageCacheNodeInventory
	err := s.withLockedState(true, func(state *model.State) error {
		now := time.Now().UTC()
		index := findImageCacheNodeInventory(state.ImageCacheNodes, node)
		if index >= 0 {
			current := state.ImageCacheNodes[index]
			node.ID = current.ID
			node.CreatedAt = current.CreatedAt
			node.UpdatedAt = now
			state.ImageCacheNodes[index] = node
		} else {
			node.ID = firstNonEmptyImageCacheString(node.ID, model.NewID("imgcache"))
			node.CreatedAt = now
			node.UpdatedAt = now
			state.ImageCacheNodes = append(state.ImageCacheNodes, node)
		}
		out = node
		for _, manifest := range manifests {
			manifest = normalizeImageCacheManifest(manifest)
			manifest.NodeID = firstNonEmptyImageCacheString(manifest.NodeID, node.NodeID)
			manifest.ClusterNodeName = firstNonEmptyImageCacheString(manifest.ClusterNodeName, node.ClusterNodeName)
			manifest.RuntimeID = firstNonEmptyImageCacheString(manifest.RuntimeID, node.RuntimeID)
			if manifest.Repo == "" || manifest.Target == "" {
				continue
			}
			if manifest.LastSeenAt.IsZero() {
				manifest.LastSeenAt = node.ObservedAt
			}
			if manifest.LastSeenAt.IsZero() {
				manifest.LastSeenAt = now
			}
			index := findImageCacheManifest(state.ImageCacheManifests, manifest)
			if index >= 0 {
				current := state.ImageCacheManifests[index]
				manifest.ID = current.ID
				manifest.CreatedAt = current.CreatedAt
				manifest.UpdatedAt = now
				state.ImageCacheManifests[index] = manifest
			} else {
				manifest.ID = firstNonEmptyImageCacheString(manifest.ID, model.NewID("imgcacheman"))
				manifest.CreatedAt = now
				manifest.UpdatedAt = now
				state.ImageCacheManifests = append(state.ImageCacheManifests, manifest)
			}
		}
		if node.SnapshotComplete {
			for idx, manifest := range state.ImageCacheManifests {
				if !imageCacheManifestBelongsToNode(manifest, node) || !manifest.Present {
					continue
				}
				if manifest.LastSeenAt.Before(node.ObservedAt) {
					manifest.Present = false
					manifest.UpdatedAt = now
					state.ImageCacheManifests[idx] = manifest
				}
			}
		}
		return nil
	})
	return out, err
}

func (s *Store) ListImageCacheNodeInventories(filter model.ImageCacheNodeInventoryFilter) ([]model.ImageCacheNodeInventory, error) {
	filter = normalizeImageCacheNodeInventoryFilter(filter)
	if s.usingDatabase() {
		return s.pgListImageCacheNodeInventories(filter)
	}
	out := []model.ImageCacheNodeInventory{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, node := range state.ImageCacheNodes {
			if imageCacheNodeInventoryMatchesFilter(node, filter) {
				out = append(out, node)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ObservedAt.After(out[j].ObservedAt)
	})
	return out, nil
}

func (s *Store) ListImageCacheManifests(filter model.ImageCacheManifestFilter) ([]model.ImageCacheManifest, error) {
	filter = normalizeImageCacheManifestFilter(filter)
	if s.usingDatabase() {
		return s.pgListImageCacheManifests(filter)
	}
	out := []model.ImageCacheManifest{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, manifest := range state.ImageCacheManifests {
			if imageCacheManifestMatchesFilter(manifest, filter) {
				out = append(out, manifest)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].LastSeenAt.After(out[j].LastSeenAt)
	})
	return out, nil
}

func (s *Store) UpsertImageCachePrunePlan(plan model.ImageCachePrunePlan) (model.ImageCachePrunePlan, error) {
	plan = normalizeImageCachePrunePlan(plan)
	if plan.NodeID == "" && plan.ClusterNodeName == "" {
		return model.ImageCachePrunePlan{}, ErrInvalidInput
	}
	if plan.Mode == "" || plan.Status == "" {
		return model.ImageCachePrunePlan{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgUpsertImageCachePrunePlan(plan)
	}
	var out model.ImageCachePrunePlan
	err := s.withLockedState(true, func(state *model.State) error {
		now := time.Now().UTC()
		index := findImageCachePrunePlan(state.ImageCachePrunePlans, plan.ID)
		if index >= 0 {
			current := state.ImageCachePrunePlans[index]
			plan.ID = current.ID
			plan.CreatedAt = current.CreatedAt
			state.ImageCachePrunePlans[index] = plan
			out = plan
			return nil
		}
		plan.ID = firstNonEmptyImageCacheString(plan.ID, model.NewID("imgcacheprune"))
		if plan.CreatedAt.IsZero() {
			plan.CreatedAt = now
		}
		state.ImageCachePrunePlans = append(state.ImageCachePrunePlans, plan)
		out = plan
		return nil
	})
	return out, err
}

func (s *Store) ListImageCachePrunePlans(filter model.ImageCachePrunePlanFilter) ([]model.ImageCachePrunePlan, error) {
	filter = normalizeImageCachePrunePlanFilter(filter)
	if s.usingDatabase() {
		return s.pgListImageCachePrunePlans(filter)
	}
	out := []model.ImageCachePrunePlan{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, plan := range state.ImageCachePrunePlans {
			if imageCachePrunePlanMatchesFilter(plan, filter) {
				out = append(out, plan)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})
	if filter.Limit > 0 && len(out) > filter.Limit {
		out = out[:filter.Limit]
	}
	return out, nil
}

func (s *Store) UpsertLocalPVInventory(inventory model.LocalPVInventory) (model.LocalPVInventory, error) {
	inventory = normalizeLocalPVInventory(inventory)
	if inventory.NodeID == "" && inventory.ClusterNodeName == "" {
		return model.LocalPVInventory{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgUpsertLocalPVInventory(inventory)
	}
	var out model.LocalPVInventory
	err := s.withLockedState(true, func(state *model.State) error {
		now := time.Now().UTC()
		index := findLocalPVInventory(state.LocalPVInventories, inventory)
		if index >= 0 {
			current := state.LocalPVInventories[index]
			inventory.ID = current.ID
			inventory.CreatedAt = current.CreatedAt
			inventory.UpdatedAt = now
			state.LocalPVInventories[index] = inventory
		} else {
			inventory.ID = firstNonEmptyImageCacheString(inventory.ID, model.NewID("localpv"))
			inventory.CreatedAt = now
			inventory.UpdatedAt = now
			state.LocalPVInventories = append(state.LocalPVInventories, inventory)
		}
		out = inventory
		return nil
	})
	return out, err
}

func (s *Store) ListLocalPVInventories(filter model.LocalPVInventoryFilter) ([]model.LocalPVInventory, error) {
	filter = normalizeLocalPVInventoryFilter(filter)
	if s.usingDatabase() {
		return s.pgListLocalPVInventories(filter)
	}
	out := []model.LocalPVInventory{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, inventory := range state.LocalPVInventories {
			if localPVInventoryMatchesFilter(inventory, filter) {
				out = append(out, inventory)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ObservedAt.After(out[j].ObservedAt)
	})
	return out, nil
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
	in.ID = strings.TrimSpace(in.ID)
	in.NodeID = strings.TrimSpace(in.NodeID)
	in.ClusterNodeName = strings.TrimSpace(in.ClusterNodeName)
	in.RuntimeID = strings.TrimSpace(in.RuntimeID)
	in.ImageRef = strings.TrimSpace(in.ImageRef)
	in.Repo = strings.Trim(strings.TrimSpace(in.Repo), "/")
	in.Target = strings.TrimSpace(in.Target)
	in.Digest = strings.TrimSpace(in.Digest)
	in.MediaType = strings.TrimSpace(in.MediaType)
	in.ReferencedBlobs = normalizeStringList(in.ReferencedBlobs)
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

func imageCacheNodeInventoryMatchesFilter(in model.ImageCacheNodeInventory, filter model.ImageCacheNodeInventoryFilter) bool {
	if filter.NodeID != "" && strings.TrimSpace(in.NodeID) != filter.NodeID {
		return false
	}
	if filter.ClusterNodeName != "" && strings.TrimSpace(in.ClusterNodeName) != filter.ClusterNodeName {
		return false
	}
	if filter.RuntimeID != "" && strings.TrimSpace(in.RuntimeID) != filter.RuntimeID {
		return false
	}
	if !filter.StaleAfter.IsZero() && in.ObservedAt.Before(filter.StaleAfter) {
		return false
	}
	return true
}

func imageCacheManifestMatchesFilter(in model.ImageCacheManifest, filter model.ImageCacheManifestFilter) bool {
	if filter.NodeID != "" && strings.TrimSpace(in.NodeID) != filter.NodeID {
		return false
	}
	if filter.ClusterNodeName != "" && strings.TrimSpace(in.ClusterNodeName) != filter.ClusterNodeName {
		return false
	}
	if filter.RuntimeID != "" && strings.TrimSpace(in.RuntimeID) != filter.RuntimeID {
		return false
	}
	if filter.Repo != "" && strings.Trim(strings.TrimSpace(in.Repo), "/") != filter.Repo {
		return false
	}
	if filter.Target != "" && strings.TrimSpace(in.Target) != filter.Target {
		return false
	}
	if filter.Digest != "" && strings.TrimSpace(in.Digest) != filter.Digest {
		return false
	}
	if filter.PresentOnly && !in.Present {
		return false
	}
	if !filter.SeenAfter.IsZero() && in.LastSeenAt.Before(filter.SeenAfter) {
		return false
	}
	return true
}

func imageCacheManifestBelongsToNode(manifest model.ImageCacheManifest, node model.ImageCacheNodeInventory) bool {
	nodeID := strings.TrimSpace(node.NodeID)
	clusterNodeName := strings.TrimSpace(node.ClusterNodeName)
	if nodeID != "" && strings.TrimSpace(manifest.NodeID) != nodeID {
		return false
	}
	if clusterNodeName != "" && strings.TrimSpace(manifest.ClusterNodeName) != clusterNodeName {
		return false
	}
	return nodeID != "" || clusterNodeName != ""
}

func imageCachePrunePlanMatchesFilter(in model.ImageCachePrunePlan, filter model.ImageCachePrunePlanFilter) bool {
	if filter.NodeID != "" && strings.TrimSpace(in.NodeID) != filter.NodeID {
		return false
	}
	if filter.ClusterNodeName != "" && strings.TrimSpace(in.ClusterNodeName) != filter.ClusterNodeName {
		return false
	}
	if filter.RuntimeID != "" && strings.TrimSpace(in.RuntimeID) != filter.RuntimeID {
		return false
	}
	if filter.Mode != "" && strings.TrimSpace(in.Mode) != filter.Mode {
		return false
	}
	if filter.Status != "" && strings.TrimSpace(in.Status) != filter.Status {
		return false
	}
	return true
}

func localPVInventoryMatchesFilter(in model.LocalPVInventory, filter model.LocalPVInventoryFilter) bool {
	if filter.NodeID != "" && strings.TrimSpace(in.NodeID) != filter.NodeID {
		return false
	}
	if filter.ClusterNodeName != "" && strings.TrimSpace(in.ClusterNodeName) != filter.ClusterNodeName {
		return false
	}
	if filter.RuntimeID != "" && strings.TrimSpace(in.RuntimeID) != filter.RuntimeID {
		return false
	}
	if !filter.StaleAfter.IsZero() && in.ObservedAt.Before(filter.StaleAfter) {
		return false
	}
	return true
}

func findImageCacheNodeInventory(nodes []model.ImageCacheNodeInventory, needle model.ImageCacheNodeInventory) int {
	for idx, node := range nodes {
		if strings.TrimSpace(needle.NodeID) != "" && strings.TrimSpace(node.NodeID) == strings.TrimSpace(needle.NodeID) {
			return idx
		}
		if strings.TrimSpace(needle.NodeID) == "" && strings.TrimSpace(needle.ClusterNodeName) != "" && strings.TrimSpace(node.ClusterNodeName) == strings.TrimSpace(needle.ClusterNodeName) {
			return idx
		}
	}
	return -1
}

func findImageCacheManifest(manifests []model.ImageCacheManifest, needle model.ImageCacheManifest) int {
	for idx, manifest := range manifests {
		if strings.TrimSpace(manifest.NodeID) != strings.TrimSpace(needle.NodeID) {
			continue
		}
		if strings.TrimSpace(manifest.ClusterNodeName) != strings.TrimSpace(needle.ClusterNodeName) {
			continue
		}
		if strings.Trim(strings.TrimSpace(manifest.Repo), "/") != strings.Trim(strings.TrimSpace(needle.Repo), "/") {
			continue
		}
		if strings.TrimSpace(manifest.Target) != strings.TrimSpace(needle.Target) {
			continue
		}
		if strings.TrimSpace(manifest.Digest) != strings.TrimSpace(needle.Digest) {
			continue
		}
		return idx
	}
	return -1
}

func findImageCachePrunePlan(plans []model.ImageCachePrunePlan, id string) int {
	id = strings.TrimSpace(id)
	if id == "" {
		return -1
	}
	for idx, plan := range plans {
		if strings.TrimSpace(plan.ID) == id {
			return idx
		}
	}
	return -1
}

func findLocalPVInventory(inventories []model.LocalPVInventory, needle model.LocalPVInventory) int {
	for idx, inventory := range inventories {
		if strings.TrimSpace(needle.NodeID) != "" && strings.TrimSpace(inventory.NodeID) == strings.TrimSpace(needle.NodeID) {
			return idx
		}
		if strings.TrimSpace(needle.NodeID) == "" && strings.TrimSpace(needle.ClusterNodeName) != "" && strings.TrimSpace(inventory.ClusterNodeName) == strings.TrimSpace(needle.ClusterNodeName) {
			return idx
		}
	}
	return -1
}

func firstNonEmptyImageCacheString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
