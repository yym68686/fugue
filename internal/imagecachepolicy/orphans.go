package imagecachepolicy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

func DefaultOrphanPolicy() model.ImageOrphanPolicy {
	return model.ImageOrphanPolicy{SweepIntervalSeconds: 1800, MaxTargetsPerNode: 50, NodeCooldownSeconds: 1800, Mode: "observe", RepositoryPrefixes: []string{}, QuarantineSeconds: 86400, MinimumObservations: 2, InventoryMaxAgeSeconds: 7200}
}
func NormalizeOrphanPolicy(p model.ImageOrphanPolicy) model.ImageOrphanPolicy {
	if p.SweepIntervalSeconds == 0 {
		p.SweepIntervalSeconds = 1800
	}
	if p.MaxTargetsPerNode == 0 {
		p.MaxTargetsPerNode = 50
	}
	if p.NodeCooldownSeconds == 0 {
		p.NodeCooldownSeconds = 1800
	}
	return p
}
func ValidateOrphanPolicy(p model.ImageOrphanPolicy) error {
	p = NormalizeOrphanPolicy(p)
	if p.SweepIntervalSeconds < 60 || p.SweepIntervalSeconds > 3600 || p.MaxTargetsPerNode < 1 || p.MaxTargetsPerNode > 500 || p.NodeCooldownSeconds < 60 || p.NodeCooldownSeconds > 3600 {
		return fmt.Errorf("invalid sweep interval, target budget or cooldown")
	}
	if p.Mode != "observe" && p.Mode != "retire" {
		return fmt.Errorf("invalid orphan policy mode")
	}
	if len(p.RepositoryPrefixes) == 0 || p.QuarantineSeconds < 600 || p.MinimumObservations < 2 || p.InventoryMaxAgeSeconds < 60 || p.InventoryMaxAgeSeconds > 7200 {
		return fmt.Errorf("explicit repository scope, quarantine >=600s, >=2 observations and inventory age 60..7200s required")
	}
	for _, v := range p.RepositoryPrefixes {
		if v == "" || strings.Trim(v, "/") != v || strings.ContainsAny(v, " *@:\\") {
			return fmt.Errorf("invalid repository prefix")
		}
	}
	for _, v := range p.ExcludedNodes {
		if strings.TrimSpace(v) == "" {
			return fmt.Errorf("invalid excluded node")
		}
	}
	return nil
}
func OrphanNode(m model.ImageCacheManifest) string { return firstNonEmpty(m.ClusterNodeName, m.NodeID) }
func OrphanKey(m model.ImageCacheManifest) string {
	raw, _ := json.Marshal([]string{OrphanNode(m), m.Repo, m.Target, m.Digest})
	sum := sha256.Sum256(raw)
	return "orphan_" + hex.EncodeToString(sum[:])
}
func OrphanGraphHash(m model.ImageCacheManifest) string {
	b := append([]string{}, m.ReferencedBlobs...)
	sort.Strings(b)
	c := append([]string{}, m.ReferencedManifests...)
	sort.Strings(c)
	raw, _ := json.Marshal([]any{m.Digest, m.GraphStatus, m.GraphFailureReason, b, c})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
func OrphanScope(p model.ImageOrphanPolicy, m model.ImageCacheManifest) bool {
	for _, n := range p.ExcludedNodes {
		if n == OrphanNode(m) {
			return false
		}
	}
	for _, prefix := range p.RepositoryPrefixes {
		if m.Repo == prefix || strings.HasPrefix(m.Repo, prefix+"/") {
			return true
		}
	}
	return false
}

// Coverage never drops an offline participant implicitly. Exclusions are durable
// administrator intent and never grant permission to delete on excluded nodes.
func OrphanCoverage(p model.ImageOrphanPolicy, nodes []model.ImageCacheNodeInventory, updaters []model.NodeUpdater, now time.Time) (bool, string) {
	excluded := map[string]bool{}
	for _, n := range p.ExcludedNodes {
		excluded[n] = true
	}
	byUpdater := map[string]model.ImageCacheNodeInventory{}
	for _, n := range nodes {
		byUpdater[n.ReportedByNodeUpdaterID] = n
	}
	count := 0
	covered := map[string]bool{}
	for _, u := range updaters {
		if u.Status != model.NodeUpdaterStatusActive || excluded[u.ClusterNodeName] {
			continue
		}
		capable := false
		for _, c := range u.Capabilities {
			if c == model.NodeUpdateTaskTypeReportImageCache {
				capable = true
			}
		}
		n, found := byUpdater[u.ID]
		if !found && !capable {
			continue
		}
		count++
		covered[n.ReportedByNodeUpdaterID] = true
		if !found || !n.SnapshotComplete || n.LastError != "" || n.ObservedAt.Before(now.Add(-time.Duration(p.InventoryMaxAgeSeconds)*time.Second)) || n.ObservedAt.After(now.Add(time.Minute)) || n.ClusterNodeName != u.ClusterNodeName || (u.MachineID != "" && n.NodeID != u.MachineID) {
			return false, "incomplete_inventory:" + u.ClusterNodeName
		}
		if u.LastHeartbeatAt == nil || u.LastHeartbeatAt.Before(now.Add(-time.Duration(p.InventoryMaxAgeSeconds)*time.Second)) {
			return false, "stale_updater:" + u.ClusterNodeName
		}
	}
	for _, n := range nodes {
		if !excluded[n.ClusterNodeName] && !covered[n.ReportedByNodeUpdaterID] {
			return false, "unauthenticated_inventory:" + n.ClusterNodeName
		}
	}
	if count == 0 {
		return false, "no_authenticated_inventories"
	}
	return true, "complete"
}

// ObserveOrphan advances only on a new inventory; repeated controller runs do
// not count as independent observations. A changed graph/policy or protection
// starts a new quarantine. There is no inferred application ownership.
func ObserveOrphan(p model.ImageOrphanPolicy, old model.ImageOrphanDecision, m model.ImageCacheManifest, c model.ImageCachePruneCandidate, coverage bool, now time.Time) model.ImageOrphanDecision {
	d := model.ImageOrphanDecision{ID: OrphanKey(m), Node: OrphanNode(m), Repo: m.Repo, Target: m.Target, Digest: m.Digest, Ownership: "unknown", State: "quarantined", PolicyGeneration: p.Generation, GraphHash: OrphanGraphHash(m), FirstObservedAt: m.LastSeenAt, LastObservedAt: m.LastSeenAt, Reason: "awaiting_independent_observations"}
	if !coverage || m.LastSeenAt.Before(p.UpdatedAt) || !OrphanScope(p, m) || c.Protected || c.Reason != "missing_control_plane_image" || len(c.MatchedImageIDs) > 0 || m.GraphStatus != "complete" || m.GraphFailureReason != "" || !m.Present {
		d.State = "protected"
		d.Reason = firstNonEmpty(c.SkipReason, "coverage_or_identity_protected")
		return d
	}
	d.Observations = 1
	if old.PolicyGeneration == p.Generation && old.GraphHash == d.GraphHash && (old.State == "quarantined" || old.State == "retirement_authorized") {
		d.FirstObservedAt = old.FirstObservedAt
		d.Observations = old.Observations
		if m.LastSeenAt.After(old.LastObservedAt) {
			d.Observations++
		}
	}
	if p.Mode == "retire" && d.Observations >= p.MinimumObservations && !d.LastObservedAt.Before(d.FirstObservedAt.Add(time.Duration(p.QuarantineSeconds)*time.Second)) && !now.Before(p.UpdatedAt.Add(time.Duration(p.QuarantineSeconds)*time.Second)) {
		d.State = "retirement_authorized"
		d.Reason = "explicit_orphan_policy"
	}
	return d
}
func ApplyOrphanDecision(c model.ImageCachePruneCandidate, m model.ImageCacheManifest, p model.ImageOrphanPolicy, d model.ImageOrphanDecision, coverage bool, now time.Time) model.ImageCachePruneCandidate {
	if c.Protected || c.Reason != "missing_control_plane_image" || len(c.MatchedImageIDs) > 0 {
		return c
	}
	if !coverage || p.Mode != "retire" || !OrphanScope(p, m) || d.ID != OrphanKey(m) || d.State != "retirement_authorized" || d.PolicyGeneration != p.Generation || d.GraphHash != OrphanGraphHash(m) || m.GraphStatus != "complete" || !m.Present || d.LastObservedAt.Before(now.Add(-time.Duration(p.InventoryMaxAgeSeconds)*time.Second)) {
		return c
	}
	c.Reason = "orphan_retirement"
	c.RetirementEvidence = []string{d.ID, fmt.Sprintf("orphan-policy:%d", p.Generation)}
	Describe(&c)
	return c
}
