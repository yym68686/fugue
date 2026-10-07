package imagecachepolicy

import (
	"fmt"
	"fugue/internal/imagecachegraph"
	"fugue/internal/imagecachekeys"
	"fugue/internal/model"
	"sort"
	"strings"
)

func ProtectSharedDigestAliases(candidates []model.ImageCachePruneCandidate) []model.ImageCachePruneCandidate {
	protectedByDigest := map[string][]model.ImageCachePruneCandidate{}
	for _, candidate := range candidates {
		if !candidate.Protected {
			continue
		}
		key := imageCacheDigestGroupKey(candidate.Repo, candidate.Digest)
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
		protectors := protectedByDigest[imageCacheDigestGroupKey(candidate.Repo, candidate.Digest)]
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
		candidate.MatchedImageIDs = dedupe(append(candidate.MatchedImageIDs, protector.MatchedImageIDs...))
		candidate.MatchedPinIDs = dedupe(append(candidate.MatchedPinIDs, protector.MatchedPinIDs...))
		candidate.MatchedTaskIDs = dedupe(append(candidate.MatchedTaskIDs, protector.MatchedTaskIDs...))
		candidate.MatchedWorkloadRefs = dedupe(append(candidate.MatchedWorkloadRefs, protector.MatchedWorkloadRefs...))
		candidate.MatchedReplicaIDs = dedupe(append(candidate.MatchedReplicaIDs, protector.MatchedReplicaIDs...))
	}
	return candidates
}

func imageCacheDigestGroupKey(repo, digest string) string {
	repo = strings.ToLower(strings.Trim(strings.TrimSpace(repo), "/"))
	digest = imagecachekeys.NormalizeDigest(digest)
	if repo == "" || digest == "" {
		return ""
	}
	return repo + "\x00" + digest
}

func dedupe(in []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, s := range in {
		if s != "" && !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}
func Finalize(manifests []model.ImageCacheManifest, candidates []model.ImageCachePruneCandidate, mode string) []model.ImageCachePruneCandidate {
	for i := range candidates {
		c := &candidates[i]
		if mode == model.ImageCachePruneModeDelete && !c.Protected && !imagecachegraph.AutomaticDeleteReasonSafe(c.Reason) {
			c.Protected = true
			c.SkipReason = "unsafe candidate reason " + c.Reason
		}
	}
	candidates = ProtectSharedDigestAliases(candidates)
	candidates = imagecachegraph.ProtectManifestGraph(manifests, candidates)
	for i := range candidates {
		Describe(&candidates[i])
	}
	return candidates
}
