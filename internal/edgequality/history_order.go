package edgequality

import (
	"errors"
	"reflect"
	"sort"
	"time"

	"fugue/internal/model"
)

// PublishedOrderEvidence is deliberately not a replacement measurement receipt.
// It can retain the actual predecessor, never restore a different quality winner.
type PublishedOrderEvidence struct {
	SourceArtifactID string                      `json:"source_artifact_id"`
	InputDigest      string                      `json:"input_digest"`
	EvidenceDigest   string                      `json:"evidence_digest"`
	Selection        *model.DNSPhysicalSelection `json:"selection"`
}

func validatePublishedOrder(snapshot Snapshot) error {
	h := snapshot.HistoricalOrder
	if h == nil {
		return nil
	}
	previous := snapshot.PreviousSelection
	if !IsContinuityNetworkPolicy(snapshot.Policy.Version) || h.SourceArtifactID == "" || len(h.InputDigest) != 71 || len(h.EvidenceDigest) != 71 ||
		model.ValidateDNSPhysicalSelection(h.Selection) != nil || previous == nil || !reflect.DeepEqual(previous, h.Selection) ||
		previous.BusinessAllocation != nil || previous.Scope != snapshot.Scope || previous.PrimaryEdgeID != snapshot.CurrentEdgeID ||
		previous.CapturedAt.After(snapshot.CapturedAt) || originalQualityTime(previous) == nil {
		return errors.New("published historical order differs from actual predecessor")
	}
	return nil
}

func applyPublishedOrderContinuity(snapshot Snapshot, result *Result) {
	h := snapshot.HistoricalOrder
	if h == nil || result.History != nil || result.QualityState == "measured" || len(snapshot.Blockers) != 0 {
		return
	}
	eligible := map[string]bool{}
	for _, candidate := range snapshot.Candidates {
		eligible[candidate.EdgeID] = candidate.ServingBlockedReason == "" && len(candidate.HardGates) == 0 && proofFresh(candidate, snapshot.Policy, snapshot.CapturedAt)
	}
	for _, candidate := range result.Candidates {
		eligible[candidate.EdgeID] = eligible[candidate.EdgeID] && len(candidate.HardGates) == 0
	}
	if !eligible[snapshot.CurrentEdgeID] {
		return
	}
	order, seen := []string{}, map[string]bool{}
	for _, id := range h.Selection.OrderedEdgeIDs {
		if eligible[id] {
			order, seen[id] = append(order, id), true
		}
	}
	recovered := []string{}
	for id, allowed := range eligible {
		if allowed && !seen[id] {
			recovered = append(recovered, id)
		}
	}
	sort.Strings(recovered)
	order = append(order, recovered...)
	at := originalQualityTime(h.Selection)
	reason := "verified_published_order_retained_no_fresh_comparable_evidence"
	result.History = &model.PhysicalQualityHistoryUse{EvidenceKind: "published_order", SourceArtifactID: h.SourceArtifactID, InputDigest: h.InputDigest,
		EvidenceDigest: h.EvidenceDigest, PolicyVersion: "published-physical-order-v1", ObservedAt: *at,
		AgeSeconds: int64(snapshot.CapturedAt.Sub(*at) / time.Second), Reason: reason,
		OriginalOrder: append([]string(nil), h.Selection.OrderedEdgeIDs...), ReusedOrder: order}
	result.QualityState, result.QualityObservedAt, result.ProposedEdgeID, result.Hypothesis = "retained", at, snapshot.CurrentEdgeID, "hold"
	result.Blockers = append(result.Blockers, reason)
}
