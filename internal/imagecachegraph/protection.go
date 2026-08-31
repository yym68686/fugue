package imagecachegraph

import (
	"fmt"
	"sort"
	"strings"

	"fugue/internal/imagecachekeys"
	"fugue/internal/model"
)

func AutomaticDeleteReasonSafe(reason string) bool {
	switch strings.TrimSpace(reason) {
	case "deleted_image_generation", "stale_replica", "excess_replica":
		return true
	default:
		return false
	}
}

// OrderCandidatesParentsFirst keeps manifest graph roots ahead of their
// children so a bounded prune task cannot repeatedly select a still-referenced
// child while omitting the parent that authorizes its deletion.
func OrderCandidatesParentsFirst(candidates []model.ImageCachePruneCandidate) []model.ImageCachePruneCandidate {
	ordered := append([]model.ImageCachePruneCandidate(nil), candidates...)
	if len(ordered) < 2 {
		return ordered
	}
	childrenByDigest := make(map[string][]int, len(ordered))
	for index, candidate := range ordered {
		key := manifestDigestKey(candidate.Repo, candidate.Digest)
		if key != "" {
			childrenByDigest[key] = append(childrenByDigest[key], index)
		}
	}
	children := make([][]int, len(ordered))
	indegree := make([]int, len(ordered))
	for parentIndex, parent := range ordered {
		seen := map[int]struct{}{}
		for _, childDigest := range parent.ReferencedManifests {
			for _, childIndex := range childrenByDigest[manifestDigestKey(parent.Repo, childDigest)] {
				if childIndex == parentIndex {
					continue
				}
				if _, exists := seen[childIndex]; exists {
					continue
				}
				seen[childIndex] = struct{}{}
				children[parentIndex] = append(children[parentIndex], childIndex)
				indegree[childIndex]++
			}
		}
	}
	ready := make([]int, 0, len(ordered))
	for index, count := range indegree {
		if count == 0 {
			ready = append(ready, index)
		}
	}
	less := func(left, right int) bool {
		if ordered[left].Reason != ordered[right].Reason {
			return ordered[left].Reason < ordered[right].Reason
		}
		if ordered[left].PlannedDeleteBytes != ordered[right].PlannedDeleteBytes {
			return ordered[left].PlannedDeleteBytes > ordered[right].PlannedDeleteBytes
		}
		if ordered[left].Repo != ordered[right].Repo {
			return ordered[left].Repo < ordered[right].Repo
		}
		if ordered[left].Target != ordered[right].Target {
			return ordered[left].Target < ordered[right].Target
		}
		return ordered[left].Digest < ordered[right].Digest
	}
	result := make([]model.ImageCachePruneCandidate, 0, len(ordered))
	emitted := make([]bool, len(ordered))
	for len(ready) > 0 {
		sort.Slice(ready, func(i, j int) bool { return less(ready[i], ready[j]) })
		index := ready[0]
		ready = ready[1:]
		if emitted[index] {
			continue
		}
		emitted[index] = true
		result = append(result, ordered[index])
		for _, childIndex := range children[index] {
			indegree[childIndex]--
			if indegree[childIndex] == 0 {
				ready = append(ready, childIndex)
			}
		}
	}
	remaining := make([]int, 0)
	for index := range ordered {
		if !emitted[index] {
			remaining = append(remaining, index)
		}
	}
	sort.Slice(remaining, func(i, j int) bool { return less(remaining[i], remaining[j]) })
	for _, index := range remaining {
		result = append(result, ordered[index])
	}
	return result
}

// SelectCandidatesWithGraphClosure applies a soft item limit without splitting
// a manifest graph. The first graph may exceed the limit because deleting only
// its root would discard the control-plane evidence needed to authorize its
// children on a later inventory cycle.
func SelectCandidatesWithGraphClosure(candidates []model.ImageCachePruneCandidate, limit int) []model.ImageCachePruneCandidate {
	ordered := OrderCandidatesParentsFirst(candidates)
	if limit <= 0 || limit > len(ordered) {
		limit = len(ordered)
	}
	childrenByDigest := make(map[string][]int, len(ordered))
	for index, candidate := range ordered {
		if candidate.Protected {
			continue
		}
		key := manifestDigestKey(candidate.Repo, candidate.Digest)
		if key != "" {
			childrenByDigest[key] = append(childrenByDigest[key], index)
		}
	}
	selected := make([]bool, len(ordered))
	result := make([]model.ImageCachePruneCandidate, 0, limit)
	for rootIndex, root := range ordered {
		if root.Protected || selected[rootIndex] {
			continue
		}
		closure := candidateGraphClosure(ordered, childrenByDigest, selected, rootIndex)
		if len(result) > 0 && len(result)+len(closure) > limit {
			break
		}
		for _, index := range closure {
			selected[index] = true
			result = append(result, ordered[index])
		}
		if len(result) >= limit {
			break
		}
	}
	return result
}

func candidateGraphClosure(candidates []model.ImageCachePruneCandidate, childrenByDigest map[string][]int, selected []bool, rootIndex int) []int {
	seen := map[int]struct{}{}
	var visit func(int)
	closure := make([]int, 0)
	visit = func(index int) {
		if index < 0 || index >= len(candidates) || candidates[index].Protected || selected[index] {
			return
		}
		if _, exists := seen[index]; exists {
			return
		}
		seen[index] = struct{}{}
		closure = append(closure, index)
		for _, digest := range candidates[index].ReferencedManifests {
			for _, childIndex := range childrenByDigest[manifestDigestKey(candidates[index].Repo, digest)] {
				visit(childIndex)
			}
		}
	}
	visit(rootIndex)
	return closure
}

// ProtectManifestGraph propagates a parent's control-plane disposition to its
// OCI/Docker child manifests. A child can be deleted automatically only when
// every surviving parent is part of the same authorized deletion graph.
func ProtectManifestGraph(manifests []model.ImageCacheManifest, candidates []model.ImageCachePruneCandidate) []model.ImageCachePruneCandidate {
	if len(manifests) == 0 || len(manifests) != len(candidates) {
		return candidates
	}
	childrenByDigest := make(map[string][]int, len(manifests))
	for index, manifest := range manifests {
		key := manifestDigestKey(manifest.Repo, manifest.Digest)
		if key != "" {
			childrenByDigest[key] = append(childrenByDigest[key], index)
		}
	}
	changed := true
	for changed {
		changed = false
		for parentIndex, parent := range manifests {
			if len(parent.ReferencedManifests) == 0 {
				continue
			}
			parentCandidate := candidates[parentIndex]
			for _, childDigest := range parent.ReferencedManifests {
				for _, childIndex := range childrenByDigest[manifestDigestKey(parent.Repo, childDigest)] {
					if applyParentDisposition(&candidates[childIndex], parentCandidate) {
						changed = true
					}
				}
			}
		}
	}
	return candidates
}

func applyParentDisposition(child *model.ImageCachePruneCandidate, parent model.ImageCachePruneCandidate) bool {
	if child == nil || child.Protected {
		return false
	}
	detail := fmt.Sprintf("referenced by parent target %q digest %q", parent.Target, parent.Digest)
	if parent.Protected {
		child.Protected = true
		child.Reason = ""
		child.SkipReason = "referenced_by_protected_manifest"
		child.SkipDetails = appendUnique(child.SkipDetails, detail)
		return true
	}
	if !AutomaticDeleteReasonSafe(parent.Reason) {
		child.Protected = true
		child.Reason = ""
		child.SkipReason = "referenced_by_quarantined_manifest"
		child.SkipDetails = appendUnique(child.SkipDetails, detail)
		return true
	}
	if strings.TrimSpace(child.Reason) != "missing_control_plane_image" {
		return false
	}
	child.Reason = parent.Reason
	child.SkipDetails = appendUnique(child.SkipDetails, "delete with authorized parent manifest graph")
	child.MatchedImageIDs = appendUnique(child.MatchedImageIDs, parent.MatchedImageIDs...)
	child.MatchedPinIDs = appendUnique(child.MatchedPinIDs, parent.MatchedPinIDs...)
	child.MatchedTaskIDs = appendUnique(child.MatchedTaskIDs, parent.MatchedTaskIDs...)
	child.MatchedWorkloadRefs = appendUnique(child.MatchedWorkloadRefs, parent.MatchedWorkloadRefs...)
	child.MatchedReplicaIDs = appendUnique(child.MatchedReplicaIDs, parent.MatchedReplicaIDs...)
	return true
}

func manifestDigestKey(repo, digest string) string {
	repo = strings.ToLower(strings.Trim(strings.TrimSpace(repo), "/"))
	digest = imagecachekeys.NormalizeDigest(digest)
	if repo == "" || digest == "" {
		return ""
	}
	return repo + "\x00" + digest
}

func appendUnique(values []string, additions ...string) []string {
	seen := make(map[string]struct{}, len(values)+len(additions))
	out := make([]string, 0, len(values)+len(additions))
	for _, value := range append(append([]string(nil), values...), additions...) {
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
	return out
}
