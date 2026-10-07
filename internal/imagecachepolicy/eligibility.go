package imagecachepolicy

import (
	"fugue/internal/imagecachekeys"
	"fugue/internal/model"
	"strings"
	"time"
)

const Version = "image-cache-retirement/v2"
const DefaultGracePeriod = 24 * time.Hour

// Facts are observations, not authority supplied by the caller of a delete API.
// Adapters build them from authenticated control-plane snapshots; one evaluator
// serves preview, scheduling and execution-time revalidation.
type Facts struct {
	ActiveDigest, Migration, Live, Pin, Task, Lost, Deleted, MinimumReplica             bool
	ImageIDs, WorkloadRefs, PinIDs, TaskIDs, KeeperIDs, ReplicaIDs, CandidateReplicaIDs []string
	ReplicaReason                                                                       string
}

func Evaluate(manifest model.ImageCacheManifest, facts Facts, now time.Time, gracePeriod time.Duration) (out model.ImageCachePruneCandidate) {
	defer func() {
		out.PolicyVersion = Version
		if !manifest.CreatedAt.IsZero() {
			out.FirstSeenAt = manifest.CreatedAt.UTC().Format(time.RFC3339)
		}
		Describe(&out)
	}()
	if gracePeriod <= 0 {
		gracePeriod = DefaultGracePeriod
	}
	out = model.ImageCachePruneCandidate{
		MatchedImageIDs:     facts.ImageIDs,
		ImageRef:            manifest.ImageRef,
		NodeName:            firstNonEmpty(manifest.ClusterNodeName, manifest.NodeID, manifest.RuntimeID),
		Repo:                manifest.Repo,
		Target:              manifest.Target,
		Digest:              manifest.Digest,
		ReferencedBlobs:     append([]string(nil), manifest.ReferencedBlobs...),
		ReferencedManifests: append([]string(nil), manifest.ReferencedManifests...),
		PlannedDeleteBytes:  firstNonZero(manifest.TotalBlobBytes, manifest.ManifestSizeBytes),
		ReferencedBlobCount: len(manifest.ReferencedBlobs),
		ReferencedBlobBytes: manifest.TotalBlobBytes,
		LastSeenAt:          manifest.LastSeenAt.UTC().Format(time.RFC3339),
	}
	if manifest.CreatedAtObserved != nil {
		out.CreatedAtObserved = manifest.CreatedAtObserved.UTC().Format(time.RFC3339)
	}
	if digest := imagecachekeys.NormalizeDigest(manifest.Digest); digest != "" {
		if facts.ActiveDigest {
			out.Protected = true
			out.SkipReason = "active_image_digest"
			out.SkipDetails = []string{"canonical digest is used by an active workload or pin"}
			return out
		}
	}
	for _, rule := range []struct {
		active         bool
		reason, detail string
	}{
		{facts.Migration, "migration_cutover_pending", "migration cutover evidence is not verified; old image artifacts remain protected"},
		{facts.Live, "current_workload", "exact image generation is used by a live workload"},
		{facts.Pin, "active_pin", "exact image generation has an active image pin"},
		{facts.Task, "active_task", "exact image generation is referenced by an active node/image replication task"},
	} {
		if rule.active {
			out.Protected = true
			out.SkipReason = rule.reason
			out.SkipDetails = []string{rule.detail}
			out.MatchedImageIDs = facts.ImageIDs
			out.MatchedWorkloadRefs = facts.WorkloadRefs
			out.MatchedPinIDs = facts.PinIDs
			out.MatchedTaskIDs = facts.TaskIDs
			return out
		}
	}

	if manifest.PinnedLocally {
		out.Protected = true
		out.SkipReason = "local_pin"
		out.SkipDetails = []string{"manifest is pinned locally on the node"}
		return out
	}
	if ids := facts.KeeperIDs; len(ids) > 0 {
		out.Protected = true
		out.SkipReason = "minimum_replica_count"
		out.MatchedReplicaIDs = ids
		out.MatchedImageIDs = facts.ImageIDs
		out.SkipDetails = []string{"node-aware minimum replica keeper protects this local copy"}
		return out
	}
	ageBase := manifest.LastSeenAt
	if manifest.CreatedAtObserved != nil {
		ageBase = *manifest.CreatedAtObserved
	}
	if !ageBase.IsZero() && now.Sub(ageBase) < gracePeriod {
		out.Protected = true
		out.SkipReason = "recent_manifest"
		out.SkipDetails = []string{"manifest is newer than the configured minimum age"}
		return out
	}
	if facts.Lost {
		out.Reason = "lost_image"
		out.MatchedImageIDs = facts.ImageIDs
		return out
	}
	if facts.Deleted {
		out.Reason = "deleted_image_generation"
		out.MatchedImageIDs = facts.ImageIDs
		return out
	}
	if reason := facts.ReplicaReason; reason != "" {
		out.Reason = reason
		out.MatchedReplicaIDs = facts.CandidateReplicaIDs
		out.MatchedImageIDs = facts.ImageIDs
		return out
	}
	if facts.MinimumReplica {
		out.Protected = true
		out.SkipReason = "minimum_replica_count"
		out.MatchedReplicaIDs = facts.ReplicaIDs
		out.MatchedImageIDs = facts.ImageIDs
		out.SkipDetails = []string{"no healthy replica exists; keep one exact image generation until repair or retention marks it removable"}
		return out
	}
	out.Reason = "missing_control_plane_image"
	return out
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}
func firstNonZero(values ...int64) int64 {
	for _, v := range values {
		if v != 0 {
			return v
		}
	}
	return 0
}

// Describe exposes lifecycle status without treating a diagnostic candidate as
// deletion authorization. The durable plan stores this classification.
func Describe(out *model.ImageCachePruneCandidate) {
	out.PolicyVersion = Version
	if out.Reason == "missing_control_plane_image" {
		out.LifecycleState = "quarantined"
		return
	}
	if out.Protected {
		out.LifecycleState = "protected"
		return
	}
	switch out.Reason {
	case "orphan_retirement":
		out.LifecycleState = "delete_eligible"
	case "deleted_image_generation", "stale_replica", "excess_replica":
		out.LifecycleState = "delete_eligible"
		out.RetirementEvidence = append(append([]string(nil), out.MatchedImageIDs...), out.MatchedReplicaIDs...)
	default:
		out.LifecycleState = "quarantined"
		out.SkipDetails = []string{"retirement requires a durable generation or replica decision; missing metadata and elapsed time do not authorize deletion"}
	}
}
