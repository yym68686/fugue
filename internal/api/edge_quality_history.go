package api

import (
	"context"
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
	source     qualityInputSource
	original   bool
	expires    time.Time
	input      store.CompilerInputDNSSelections
	inputErr   error
	runtimeErr error
	facts      map[string][]historicalQualityFact
}

type historicalQualityFact struct {
	fact                    platformconfig.DNSSelectionObservation
	artifactID, inputDigest string
	priorSource             *qualityInputSource
}

const historicalQualityBatchEvidenceBytes = 64 << 20

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
	return true, p.runtimeErr
}

// Complete expensive dependency admission before the fresh observation clock.
// Carry it only through this bounded request/batch, never in Server state.
func (s *Server) prepareHistoricalQualityContext(parentCtx context.Context, scope string, queryKeys ...string) (context.Context, error) {
	started := time.Now()
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
	p := &historicalQualityPreparation{source: source, inputErr: store.ErrConflict}
	prepared := func() (context.Context, error) {
		// The original clocks and source are never renewed. This lifetime only
		// bounds sharing of a completed read check within the current batch.
		p.expires = time.Now().Add(2 * time.Minute)
		if s.log != nil {
			s.log.Printf("historical quality prepared artifact=%s input_facts=%d history_queries=%d elapsed=%s runtime_admitted=%t original_admitted=%t input_ready=%t", parent.ID,
				len(p.input.DNSSelections), len(p.facts), time.Since(started), p.runtimeErr == nil, p.original, p.inputErr == nil)
		}
		return context.WithValue(parentCtx, historicalQualityPreparationKey{}, p), parentCtx.Err()
	}
	if err := s.checkQualityInputSource(ctx, source); err != nil {
		if s.log != nil {
			s.log.Printf("historical quality runtime admission unavailable artifact=%s: %v", parent.ID, err)
		}
		p.runtimeErr = err
		return prepared()
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
	remaining := historicalQualityBatchEvidenceBytes
	requested := map[string]bool{}
	for _, key := range queryKeys {
		requested[key] = true
	}
	p.input, p.inputErr = s.store.GetPlatformCompilerInputDNSSelectionsContext(ctx, digest, boundedHistoricalQualitySelector(requested, &remaining))
	err = p.inputErr
	if err != nil && s.log != nil {
		s.log.Printf("historical quality preparation unavailable artifact=%s input=%s: %v", parent.ID, digest, err)
	}
	p.facts = map[string][]historicalQualityFact{}
	if err == nil && p.input.IntentGeneration == parent.Metadata["intent_generation"] && p.input.PolicyGeneration == parent.Metadata["policy_generation"] {
		for _, fact := range p.input.DNSSelections {
			key := fact.NodeID + "\x00" + fact.Hostname
			p.facts[key] = append(p.facts[key], historicalQualityFact{fact: fact, artifactID: parent.ID, inputDigest: digest})
		}
	} else if err == nil {
		p.inputErr = store.ErrConflict
	}
	if p.inputErr == nil {
		s.preparePriorQualityHistory(ctx, p, requested, &remaining)
	}
	// Optional history failure cannot suppress independent live evidence. Only
	// cancellation of the enclosing request/capture cancels both phases.
	return prepared()
}

// Selective storage reads can fall back to a full verified read after an index
// representation change. Re-evaluating the same summary must not spend the
// body budget twice and accidentally suppress an already selected query.
func boundedHistoricalQualitySelector(requested map[string]bool, remaining *int) func(store.CompilerDNSSelectionSummary) bool {
	selected := map[string]int{}
	return func(summary store.CompilerDNSSelectionSummary) bool {
		key := summary.NodeID + "\x00" + summary.Hostname
		if len(requested) > 0 && !requested[key] || summary.Type != "A" || !(summary.HasEvidence && summary.HasSelection || summary.HasRetainedEvidence) {
			return false
		}
		if size, found := selected[key]; found {
			return size == summary.EvidenceBytes
		}
		if summary.EvidenceBytes < 0 || summary.EvidenceBytes > *remaining {
			return false
		}
		*remaining -= summary.EvidenceBytes
		selected[key] = summary.EvidenceBytes
		return true
	}
}

// Read history from the independently verified LKG, not the active shadow or
// a volatile freshness cache. Missing history never deletes live observations.
func (s *Server) bindHistoricalQualityEvidence(ctx context.Context, snapshot *edgequality.Snapshot, nodeID string) {
	if !model.IsContinuityPhysicalNetworkPolicy(snapshot.Policy.Version) {
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
	facts, err := s.historicalQualityFacts(ctx, source, nodeID, edgequality.DNSHostname(*snapshot))
	if err != nil {
		if s.log != nil {
			s.log.Printf("historical quality input unavailable artifact=%s input=%s: %v", parent.ID, digest, err)
		}
		return
	}
	var best *edgequality.HistoricalEvidence
	for _, historical := range facts {
		if !s.historicalQualityFactUnchanged(ctx, historical) {
			continue
		}
		fact := historical.fact
		if fact.NodeID != nodeID || fact.Hostname != edgequality.DNSHostname(*snapshot) || fact.Type != "A" {
			continue
		}
		raw := retainedPhysicalHistory(fact, nil)
		receipt, _, ok := replayablePhysicalHistory(raw, fact.Hostname)
		if !ok {
			continue
		}
		compiled, err := compileBoundPhysicalQualitySelection(receipt, receipt.Snapshot.CapturedAt)
		if err != nil {
			continue
		}
		if reflect.DeepEqual(raw, fact.PhysicalEvidence) {
			declared, _ := platformconfig.Digest(fact.PhysicalSelection)
			derived, _ := platformconfig.Digest(compiled)
			if declared != derived {
				continue
			}
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
		history := &edgequality.HistoricalEvidence{SourceArtifactID: historical.artifactID, InputDigest: historical.inputDigest, Selection: compiled, Receipt: receipt}
		if receipt.Snapshot.HistoricalEvidence != nil && receipt.Result.History != nil {
			// Keep one immutable original, not an ever-growing receipt chain.
			history = receipt.Snapshot.HistoricalEvidence
		}
		trial := *snapshot
		trial.HistoricalEvidence = history
		if edgequality.ValidateHistoricalEvidence(trial) != nil {
			continue
		}
		if best == nil || historicalQualityTime(history).After(historicalQualityTime(best)) {
			best = history
		}
	}
	if best != nil && s.qualityInputSourceUnchanged(ctx, source) && s.checkQualityInputSource(ctx, source) == nil {
		snapshot.HistoricalEvidence = best
	}
}

func historicalQualityTime(h *edgequality.HistoricalEvidence) time.Time {
	if h.Selection.QualityObservedAt != nil {
		return *h.Selection.QualityObservedAt
	}
	return h.Selection.CapturedAt
}

func (s *Server) historicalQualityFacts(ctx context.Context, source qualityInputSource, nodeID, hostname string) ([]historicalQualityFact, error) {
	if p, prepared := ctx.Value(historicalQualityPreparationKey{}).(*historicalQualityPreparation); prepared {
		if _, err := preparedHistoricalQualityAdmission(ctx, source, true); err != nil {
			return nil, err
		}
		return p.facts[nodeID+"\x00"+hostname], p.inputErr
	}
	digest, err := originalQualityInputDigest(source.parent)
	if err != nil {
		return nil, err
	}
	input, err := s.store.GetPlatformCompilerInputDNSSelectionsContext(ctx, digest, func(summary store.CompilerDNSSelectionSummary) bool {
		return summary.NodeID == nodeID && summary.Hostname == hostname && (summary.HasEvidence && summary.HasSelection || summary.HasRetainedEvidence)
	})
	if err != nil {
		return nil, err
	}
	if input.IntentGeneration != source.parent.Metadata["intent_generation"] || input.PolicyGeneration != source.parent.Metadata["policy_generation"] {
		return nil, store.ErrConflict
	}
	facts := make([]historicalQualityFact, 0, len(input.DNSSelections))
	for _, fact := range input.DNSSelections {
		facts = append(facts, historicalQualityFact{fact: fact, artifactID: source.parent.ID, inputDigest: digest})
	}
	return facts, nil
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
