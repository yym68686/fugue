package api

import (
	"context"
	"encoding/json"

	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/store"
)

// Read history from the independently verified LKG, not the active shadow or
// a volatile freshness cache. Missing history never deletes live observations.
func (s *Server) bindHistoricalQualityEvidence(ctx context.Context, snapshot *edgequality.Snapshot, nodeID string) {
	if snapshot.Policy.Version != model.PhysicalPairedContinuityPolicyVersion {
		return
	}
	lkg, err := s.store.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, snapshot.Scope)
	if err != nil || lkg == nil {
		return
	}
	parent, err := s.store.GetPlatformArtifact(lkg.ArtifactID)
	if err != nil {
		return
	}
	source, err := s.observeQualityInputSource(parent)
	if err != nil || s.checkQualityInputSource(ctx, source) != nil {
		return
	}
	digest, err := platformconfig.ArtifactRuntimeInputDigest(parent)
	if err != nil {
		return
	}
	input, err := s.store.GetPlatformCompilerInputDNSSelections(digest, func(summary store.CompilerDNSSelectionSummary) bool {
		return summary.NodeID == nodeID && summary.Hostname == edgequality.DNSHostname(*snapshot) && summary.HasEvidence && summary.HasSelection
	})
	if err != nil || input.IntentGeneration != parent.Metadata["intent_generation"] || input.PolicyGeneration != parent.Metadata["policy_generation"] {
		return
	}
	for _, fact := range input.DNSSelections {
		if fact.NodeID != nodeID || fact.Hostname != edgequality.DNSHostname(*snapshot) || fact.Type != "A" || fact.PhysicalSelection == nil || len(fact.PhysicalEvidence) > 8<<20 {
			continue
		}
		var receipt edgequality.Receipt
		if json.Unmarshal(fact.PhysicalEvidence, &receipt) != nil {
			continue
		}
		compiled, err := compileBoundPhysicalQualitySelection(receipt, receipt.Snapshot.CapturedAt)
		if err != nil {
			continue
		}
		declared, _ := platformconfig.Digest(fact.PhysicalSelection)
		derived, _ := platformconfig.Digest(compiled)
		if declared != derived {
			continue
		}
		if len(receipt.Snapshot.ServiceReceipts) > 0 {
			matched := false
			for _, child := range receipt.Snapshot.ServiceReceipts {
				if edgequality.ServiceEvidenceKey(child.Snapshot) != edgequality.ServiceEvidenceKey(*snapshot) {
					continue
				}
				compiled, err = compileBoundPhysicalQualitySelection(child, child.Snapshot.CapturedAt)
				if err == nil {
					receipt, matched = child, true
				}
				break
			}
			if !matched {
				continue
			}
		}
		history := &edgequality.HistoricalEvidence{SourceArtifactID: parent.ID, InputDigest: digest, Selection: compiled, Receipt: receipt}
		if receipt.Snapshot.HistoricalEvidence != nil && receipt.Result.History != nil {
			// Keep one immutable original, not an ever-growing receipt chain.
			history = receipt.Snapshot.HistoricalEvidence
		}
		trial := *snapshot
		trial.HistoricalEvidence = history
		if edgequality.ValidateHistoricalEvidence(trial) != nil || !s.qualityInputSourceUnchanged(ctx, source) || s.checkQualityInputSource(ctx, source) != nil {
			continue
		}
		snapshot.HistoricalEvidence = history
		return
	}
}
