package api

import (
	"encoding/json"
	"testing"
	"time"

	"fugue/internal/edgequality"
	"fugue/internal/platformconfig"
)

func TestLastValidQualityLoadsOriginalComparisonAfterLongCollectorGap(t *testing.T) {
	receipt := historicalBoundReceiptFixture(t)
	selection, err := compileBoundPhysicalQualitySelection(receipt, receipt.Snapshot.CapturedAt)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(receipt)
	fact := platformconfig.DNSSelectionObservation{NodeID: "dns-a", Hostname: receipt.Snapshot.Hostname, Type: "A", RetainedPhysicalEvidence: raw}
	input := platformconfig.RuntimeSnapshot{IntentGeneration: "intent-a", PolicyGeneration: "policy-a", DNSSelections: []platformconfig.DNSSelectionObservation{fact}}
	s, _, _ := qualityRestoreInputFixture(t, input)
	ctx, err := s.prepareHistoricalQualityContext(t.Context(), "global")
	if err != nil {
		t.Fatal(err)
	}
	current := receipt.Snapshot
	current.Policy, current.CapturedAt = edgequality.DefaultLastValidPolicy(), current.CapturedAt.Add(365*24*time.Hour)
	s.bindHistoricalQualityEvidence(ctx, &current, "dns-a")
	if current.HistoricalEvidence == nil || current.HistoricalEvidence.Receipt.Digest != receipt.Digest || !current.HistoricalEvidence.Selection.CapturedAt.Equal(selection.CapturedAt) {
		t.Fatal("last valid comparison expired or its clock was renewed")
	}
	projection := platformIntentProjectionResponse{RuntimeSnapshot: platformconfig.RuntimeSnapshot{DNSSelections: []platformconfig.DNSSelectionObservation{{NodeID: fact.NodeID, Hostname: fact.Hostname, Type: fact.Type}}}}
	if err := s.carryPreparedHistoricalQuality(ctx, &projection, map[string]bool{fact.NodeID + "\x00" + fact.Hostname: true}); err != nil || len(projection.RuntimeSnapshot.DNSSelections[0].RetainedPhysicalEvidence) == 0 {
		t.Fatal("availability fallback lost last valid comparison", err)
	}
}
