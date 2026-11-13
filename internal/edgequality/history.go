package edgequality

import (
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"time"

	"fugue/internal/model"
)

// HistoricalEvidence is a single original, positively verified compiler input.
// It is deliberately separate from current observations: old protocols retain
// their original semantics, timestamps, missing metrics and comparison rules.
type HistoricalEvidence struct {
	SourceArtifactID string                      `json:"source_artifact_id"`
	InputDigest      string                      `json:"input_digest"`
	Selection        *model.DNSPhysicalSelection `json:"selection"`
	Receipt          Receipt                     `json:"-"`
}

type historicalEvidenceWire struct {
	SourceArtifactID string                      `json:"source_artifact_id"`
	InputDigest      string                      `json:"input_digest"`
	Selection        *model.DNSPhysicalSelection `json:"selection"`
	Receipt          []byte                      `json:"receipt"`
}

func (h HistoricalEvidence) MarshalJSON() ([]byte, error) {
	raw, err := json.Marshal(h.Receipt)
	if err != nil {
		return nil, err
	}
	return json.Marshal(historicalEvidenceWire{h.SourceArtifactID, h.InputDigest, h.Selection, raw})
}

func (h *HistoricalEvidence) UnmarshalJSON(raw []byte) error {
	var wire historicalEvidenceWire
	if err := json.Unmarshal(raw, &wire); err != nil {
		return err
	}
	if len(wire.Receipt) > 8<<20 {
		return errors.New("historical receipt exceeds bound")
	}
	var receipt Receipt
	if err := json.Unmarshal(wire.Receipt, &receipt); err != nil {
		return err
	}
	*h = HistoricalEvidence{wire.SourceArtifactID, wire.InputDigest, wire.Selection, receipt}
	return nil
}

func ValidateHistoricalEvidence(snapshot Snapshot) error {
	if err := validatePublishedOrder(snapshot); err != nil {
		return err
	}
	h := snapshot.HistoricalEvidence
	if h == nil {
		return nil
	}
	old := h.Receipt.Snapshot
	if snapshot.Policy.Version != PairedContinuityPolicyVersion || h.SourceArtifactID == "" || len(h.InputDigest) != 71 ||
		old.HistoricalEvidence != nil || old.HistoricalOrder != nil || len(old.ServiceReceipts) != 0 || old.Policy.Version == "network-only-experiment-v1" ||
		old.Hostname != snapshot.Hostname || DNSHostname(old) != DNSHostname(snapshot) || old.PathPrefix != snapshot.PathPrefix ||
		old.Scope != snapshot.Scope || old.TrafficClass != snapshot.TrafficClass || old.CapturedAt.After(snapshot.CapturedAt) ||
		model.ValidateDNSPhysicalSelection(h.Selection) != nil || h.Selection.BusinessAllocation != nil || h.Selection.History != nil ||
		h.Selection.EvidenceDigest != h.Receipt.Digest || h.Selection.QualityState != "measured" && h.Selection.QualityState != "retained" {
		return errors.New("historical quality context or original provenance invalid")
	}
	// Recompile, rather than trusting a cached winner or changing the old clock.
	selection, err := CompileSelection(h.Receipt, DNSBinding{ReceiptID: h.Selection.DNSReceiptID,
		LoadedDigest: h.Selection.LoadedDigest, PolicyDigest: h.Selection.PolicyDigest, Hostname: DNSHostname(old), Scope: old.Scope,
		CurrentEdgeID: old.CurrentEdgeID, ObservedAt: old.CapturedAt, ReplayMatched: true, WriteSucceeded: true}, old.CapturedAt)
	if err != nil || !reflect.DeepEqual(selection, h.Selection) {
		return errors.New("historical quality order differs from original replay")
	}
	return nil
}

func applyHistoricalContinuity(snapshot Snapshot, result *Result) {
	h := snapshot.HistoricalEvidence
	if h == nil || result.QualityState == "measured" || len(snapshot.Blockers) != 0 {
		return
	}
	eligible := map[string]bool{}
	for _, candidate := range snapshot.Candidates {
		eligible[candidate.EdgeID] = candidate.ServingBlockedReason == "" && len(candidate.HardGates) == 0 && proofFresh(candidate, snapshot.Policy, snapshot.CapturedAt)
	}
	// Fresh negative capacity evidence still excludes a historical winner.
	for _, candidate := range result.Candidates {
		eligible[candidate.EdgeID] = eligible[candidate.EdgeID] && len(candidate.HardGates) == 0
	}
	compatible := map[string]bool{}
	for _, old := range h.Receipt.Snapshot.Candidates {
		for _, current := range snapshot.Candidates {
			if old.EdgeID == current.EdgeID && old.EdgeGroupID == current.EdgeGroupID && old.RouteGeneration == current.RouteGeneration {
				compatible[old.EdgeID] = true
			}
		}
	}
	order := []string{}
	seen := map[string]bool{}
	for _, id := range h.Selection.OrderedEdgeIDs {
		if eligible[id] && compatible[id] {
			order = append(order, id)
			seen[id] = true
		}
	}
	if len(order) == 0 {
		return
	}
	// A recovered candidate can rejoin without being claimed as a measured
	// winner. Only candidates actually compared in history inherit its order.
	recovered := []string{}
	for id, allowed := range eligible {
		if allowed && !seen[id] {
			recovered = append(recovered, id)
		}
	}
	sort.Strings(recovered)
	order = append(order, recovered...)
	at := originalQualityTime(h.Selection)
	if at == nil {
		return
	}
	reason := "historical_order_retained_no_fresh_comparable_evidence"
	if order[0] != snapshot.CurrentEdgeID {
		reason = "historical_order_restored_after_current_admission_checks"
	}
	result.History = &model.PhysicalQualityHistoryUse{SourceArtifactID: h.SourceArtifactID, InputDigest: h.InputDigest,
		EvidenceDigest: h.Receipt.Digest, PolicyVersion: h.Receipt.Snapshot.Policy.Version, ObservedAt: *at,
		AgeSeconds: int64(snapshot.CapturedAt.Sub(*at) / time.Second), Reason: reason,
		OriginalOrder: append([]string(nil), h.Selection.OrderedEdgeIDs...), ReusedOrder: order}
	result.QualityState, result.QualityObservedAt = "retained", at
	result.ProposedEdgeID, result.Hypothesis = order[0], "hold"
	if order[0] != snapshot.CurrentEdgeID {
		result.Hypothesis = "switch"
	}
	result.Blockers = append(result.Blockers, reason)
}
