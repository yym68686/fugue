package edgequality

import "fugue/internal/model"

// A shared DNS alias may restore a historical order only when every service
// independently replays and approves it. One service cannot lend its history
// or freshness to another owner or path.
func applyServiceHistory(snapshot Snapshot, result *Result) bool {
	if snapshot.Policy.Version != PairedContinuityPolicyVersion || len(snapshot.ServiceReceipts) < 2 {
		return false
	}
	var history *model.PhysicalQualityHistoryUse
	for _, child := range snapshot.ServiceReceipts {
		h := child.Result.History
		if h == nil || len(h.ReusedOrder) == 0 || child.Result.ProposedEdgeID != h.ReusedOrder[0] {
			return false
		}
		if history == nil {
			history = model.ClonePhysicalQualityHistoryUse(h)
			continue
		}
		if history.ReusedOrder[0] != h.ReusedOrder[0] || history.SourceArtifactID != h.SourceArtifactID || history.InputDigest != h.InputDigest {
			return false
		}
		if h.EvidenceKind == "published_order" {
			history.EvidenceKind = "published_order"
		}
		common := []string{}
		for _, id := range history.ReusedOrder {
			for _, other := range h.ReusedOrder {
				if id == other {
					common = append(common, id)
					break
				}
			}
		}
		history.ReusedOrder = common
		if h.ObservedAt.Before(history.ObservedAt) {
			history.ObservedAt, history.AgeSeconds = h.ObservedAt, h.AgeSeconds
		}
		if h.PolicyVersion != history.PolicyVersion {
			history.PolicyVersion = "mixed_original_service_policies"
		}
	}
	if history == nil || len(history.ReusedOrder) == 0 {
		return false
	}
	for _, candidate := range result.Candidates {
		if candidate.EdgeID == history.ReusedOrder[0] && len(candidate.HardGates) > 0 {
			return false
		}
	}
	history.Reason = "shared_service_historical_order_unanimously_replayed"
	result.History, result.QualityState, result.QualityObservedAt = history, "retained", &history.ObservedAt
	result.ProposedEdgeID, result.Hypothesis = history.ReusedOrder[0], "hold"
	if result.ProposedEdgeID != snapshot.CurrentEdgeID {
		result.Hypothesis = "switch"
	}
	result.Blockers = append(result.Blockers, history.Reason)
	return true
}
