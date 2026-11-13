package api

import (
	"context"
	"encoding/json"
	"time"

	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/store"
)

// Prepare only the bounded byte-position/hash index, not a cached quality
// decision. Run this before starting the fresh capture deadline and clock:
// completely verifying a cold retained input can exceed a live observation's
// short budget. The later bind must reread its evidence and authority.
func (s *Server) prepareHistoricalQualityInput(parentCtx context.Context, scope string) error {
	if err := parentCtx.Err(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()
	lkg, err := s.store.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, scope)
	if err != nil || lkg == nil {
		return parentCtx.Err()
	}
	parent, err := s.store.GetPlatformArtifact(lkg.ArtifactID)
	if err != nil {
		return parentCtx.Err()
	}
	source, err := s.observeQualityInputSource(parent)
	if err != nil || s.checkQualityInputSource(ctx, source) != nil {
		return parentCtx.Err()
	}
	digest, err := platformconfig.ArtifactRuntimeInputDigest(parent)
	if err != nil {
		return parentCtx.Err()
	}
	_, err = s.store.GetPlatformCompilerInputDNSSelectionsContext(ctx, digest, func(store.CompilerDNSSelectionSummary) bool { return false })
	if err != nil && s.log != nil {
		s.log.Printf("historical quality preparation unavailable artifact=%s input=%s: %v", parent.ID, digest, err)
	}
	// Optional history failure cannot suppress independent live evidence. Only
	// cancellation of the enclosing request/capture cancels both phases.
	return parentCtx.Err()
}

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
	input, err := s.store.GetPlatformCompilerInputDNSSelectionsContext(ctx, digest, func(summary store.CompilerDNSSelectionSummary) bool {
		return summary.NodeID == nodeID && summary.Hostname == edgequality.DNSHostname(*snapshot) && summary.HasEvidence && summary.HasSelection
	})
	if err != nil {
		if s.log != nil {
			s.log.Printf("historical quality input unavailable artifact=%s input=%s: %v", parent.ID, digest, err)
		}
		return
	}
	if input.IntentGeneration != parent.Metadata["intent_generation"] || input.PolicyGeneration != parent.Metadata["policy_generation"] {
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
