package api

import (
	"context"
	"encoding/json"
	"reflect"
	"time"

	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/store"
)

type historicalQualityPreparationKey struct{}

// Request-local read admission, not a serving lease or persisted authority
// cache. Binding still rereads the current signed source and LKG. Publishing
// independently executes its normal complete dependency admission.
type historicalQualityPreparation struct {
	source   qualityInputSource
	original bool
	expires  time.Time
}

func preparedHistoricalQualityAdmission(ctx context.Context, source qualityInputSource, original bool) (bool, error) {
	p, prepared := ctx.Value(historicalQualityPreparationKey{}).(*historicalQualityPreparation)
	if !prepared {
		return false, nil
	}
	if err := ctx.Err(); err != nil {
		return true, err
	}
	if p == nil || !time.Now().Before(p.expires) || original && !p.original || !reflect.DeepEqual(p.source, source) {
		return true, store.ErrConflict
	}
	return true, nil
}

// Complete expensive dependency admission before the fresh observation clock.
// Carry it only through this bounded request/batch, never in Server state.
func (s *Server) prepareHistoricalQualityContext(parentCtx context.Context, scope string) (context.Context, error) {
	if err := parentCtx.Err(); err != nil {
		return parentCtx, err
	}
	ctx, cancel := context.WithTimeout(parentCtx, 90*time.Second)
	defer cancel()
	lkg, err := s.store.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, scope)
	if err != nil || lkg == nil {
		return parentCtx, parentCtx.Err()
	}
	parent, err := s.store.GetPlatformArtifact(lkg.ArtifactID)
	if err != nil {
		return parentCtx, parentCtx.Err()
	}
	source, err := s.observeQualityInputSource(parent)
	if err != nil {
		return parentCtx, parentCtx.Err()
	}
	if err := s.checkQualityInputSource(ctx, source); err != nil {
		if s.log != nil {
			s.log.Printf("historical quality runtime admission unavailable artifact=%s: %v", parent.ID, err)
		}
		return parentCtx, parentCtx.Err()
	}
	p := &historicalQualityPreparation{source: source}
	prepared := func() (context.Context, error) {
		// The original clocks and source are never renewed. This lifetime only
		// bounds sharing of a completed read check within the current batch.
		p.expires = time.Now().Add(30 * time.Second)
		return context.WithValue(parentCtx, historicalQualityPreparationKey{}, p), parentCtx.Err()
	}
	if err := s.checkOriginalQualityInputSource(ctx, source); err != nil {
		if s.log != nil {
			s.log.Printf("historical quality original admission unavailable artifact=%s: %v", parent.ID, err)
		}
		return prepared()
	}
	p.original = true
	digest, err := originalQualityInputDigest(parent)
	if err != nil {
		return prepared()
	}
	_, err = s.store.GetPlatformCompilerInputDNSSelectionsContext(ctx, digest, func(store.CompilerDNSSelectionSummary) bool { return false })
	if err != nil && s.log != nil {
		s.log.Printf("historical quality preparation unavailable artifact=%s input=%s: %v", parent.ID, digest, err)
	}
	// Optional history failure cannot suppress independent live evidence. Only
	// cancellation of the enclosing request/capture cancels both phases.
	return prepared()
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
	// A fallback publication can legitimately retain only the signed order.
	// Preserve that independent provenance even if raw probe history is absent.
	s.bindPublishedQualityOrder(ctx, snapshot, nodeID, source)
	if s.checkOriginalQualityInputSource(ctx, source) != nil {
		return
	}
	digest, err := originalQualityInputDigest(parent)
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

// Opted-in configuration inputs deliberately omit raw probe transcripts.
// Historical replay must read the original audit input, with its own admission.
func originalQualityInputDigest(parent model.PlatformArtifact) (string, error) {
	if _, err := platformconfig.ArtifactRuntimeInputDigest(parent); err != nil {
		return "", err
	}
	return parent.Metadata["input_snapshot_digest"], nil
}

func (s *Server) checkOriginalQualityInputSource(ctx context.Context, source qualityInputSource) error {
	if prepared, err := preparedHistoricalQualityAdmission(ctx, source, true); prepared {
		return err
	}
	return s.store.CheckPlatformInputRead(ctx, []store.PlatformInputReadSelection{{
		ArtifactID: source.parent.ID, ContentHash: source.parent.ContentHash, ReleaseID: source.release.ID,
		LKGKind: source.lkg.ArtifactKind, LKGScope: source.lkg.ScopeKey, LKG: &source.lkg, OriginalInput: true,
	}})
}
