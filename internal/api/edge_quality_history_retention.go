package api

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"fugue/internal/edgequality"
	"fugue/internal/platformconfig"
)

// Carry audit history even when the subsequent fresh capture fails and the
// publisher uses availability-only order. This applies to both universal and
// explicitly configured V7 routes, without another per-query input read.
func (s *Server) carryPreparedHistoricalQuality(ctx context.Context, projection *platformIntentProjectionResponse, covered map[string]bool) error {
	p, ok := ctx.Value(historicalQualityPreparationKey{}).(*historicalQualityPreparation)
	if !ok || p == nil || p.inputErr != nil || !s.qualityInputSourceUnchanged(ctx, p.source) || s.checkOriginalQualityInputSource(ctx, p.source) != nil {
		return nil // Missing optional history never prevents independent availability.
	}
	facts := append([]platformconfig.DNSSelectionObservation(nil), projection.RuntimeSnapshot.DNSSelections...)
	for index, fact := range facts {
		key := fact.NodeID + "\x00" + fact.Hostname
		if !covered[key] || fact.Type != "A" {
			continue
		}
		for _, prior := range p.facts[key] {
			if s.historicalQualityFactUnchanged(ctx, prior) {
				fact.RetainedPhysicalEvidence = retainedPhysicalHistory(fact, retainedPhysicalHistory(prior.fact, nil))
			}
		}
		fact.SourceDigest, fact.SourceGeneration = "", ""
		digest, err := platformconfig.Digest(fact)
		if err != nil {
			return err
		}
		fact.SourceDigest, fact.SourceGeneration = digest, "dns-observation_"+strings.TrimPrefix(digest, "sha256:")
		facts[index] = fact
	}
	if s.qualityInputSourceUnchanged(ctx, p.source) {
		projection.RuntimeSnapshot.DNSSelections = facts
	}
	return ctx.Err()
}

// Comparison history is audit data, not the current serving selection. Never
// replace it with a learning receipt or a receipt that only remembers an order.
// Replaying at the original clock preserves the original evaluator semantics.
func replayablePhysicalHistory(raw json.RawMessage, hostname string) (edgequality.Receipt, time.Time, bool) {
	var receipt edgequality.Receipt
	if len(raw) == 0 || len(raw) > 8<<20 || json.Unmarshal(raw, &receipt) != nil || edgequality.DNSHostname(receipt.Snapshot) != hostname ||
		receipt.Snapshot.Policy.Version == "network-only-experiment-v1" {
		return receipt, time.Time{}, false
	}
	selection, err := compileBoundPhysicalQualitySelection(receipt, receipt.Snapshot.CapturedAt)
	if err != nil || selection.BusinessAllocation != nil {
		return receipt, time.Time{}, false
	}
	if selection.QualityState == "measured" {
		at := selection.CapturedAt
		if selection.QualityObservedAt != nil {
			at = *selection.QualityObservedAt
		}
		return receipt, at, true
	}
	if h := receipt.Snapshot.HistoricalEvidence; h != nil && receipt.Result.History != nil && edgequality.ValidateHistoricalEvidence(receipt.Snapshot) == nil {
		return receipt, receipt.Result.History.ObservedAt, true
	}
	// Consensus roots carry independent, exact service/path histories. Keep the
	// complete root so a later binding cannot silently omit another owner.
	var oldest time.Time
	for _, child := range receipt.Snapshot.ServiceReceipts {
		raw, err := json.Marshal(child)
		if err != nil {
			return receipt, time.Time{}, false
		}
		_, at, ok := replayablePhysicalHistory(raw, hostname)
		if !ok {
			return receipt, time.Time{}, false
		}
		if oldest.IsZero() || at.Before(oldest) {
			oldest = at
		}
	}
	return receipt, oldest, !oldest.IsZero()
}

func retainedPhysicalHistory(fact platformconfig.DNSSelectionObservation, next json.RawMessage) json.RawMessage {
	if fact.Type != "A" {
		return nil
	}
	var retained json.RawMessage
	var newest time.Time
	for index, raw := range []json.RawMessage{fact.RetainedPhysicalEvidence, fact.PhysicalEvidence, next} {
		receipt, at, ok := replayablePhysicalHistory(raw, fact.Hostname)
		if ok && index == 1 {
			compiled, err := compileBoundPhysicalQualitySelection(receipt, receipt.Snapshot.CapturedAt)
			declared, _ := platformconfig.Digest(fact.PhysicalSelection)
			derived, _ := platformconfig.Digest(compiled)
			ok = err == nil && fact.PhysicalSelection != nil && declared == derived
		}
		if ok && (retained == nil || at.After(newest)) {
			retained, newest = raw, at
		}
	}
	return append(json.RawMessage(nil), retained...)
}
