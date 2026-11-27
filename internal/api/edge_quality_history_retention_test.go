package api

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"

	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/store"
)

func TestComparisonHistorySurvivesAvailabilityPublicationAndRestart(t *testing.T) {
	receipt := historicalBoundReceiptFixture(t)
	raw, _ := json.Marshal(receipt)
	selection, err := compileBoundPhysicalQualitySelection(receipt, receipt.Snapshot.CapturedAt)
	if err != nil {
		t.Fatal(err)
	}
	projection, nodes, strategy, now := directQueryFixture()
	strategy.ECSEnabled, strategy.ExplorationPercent = false, 0
	strategy.OrderedProjection = &platformconfig.DNSOrderedProjection{DefaultOrder: model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-a", "edge-b"}}}
	availability := model.DNSAvailabilityPolicy{Version: model.DNSAvailabilityVersion, OrderedEdgeIDs: []string{"edge-a", "edge-b"}, RecoverySuccesses: 3, RecoveryMinSeconds: 60}
	strategy.AvailabilityRoutes = []platformconfig.DNSAvailabilityRoute{{Hostname: receipt.Snapshot.Hostname, Policy: availability}}
	if err := projectDirectDNSQueries(&projection, strategy, nodes, now); err != nil {
		t.Fatal(err)
	}
	for i := range projection.RuntimeSnapshot.DNSSelections {
		fact := &projection.RuntimeSnapshot.DNSSelections[i]
		if fact.Type == "A" {
			fact.PhysicalEvidence, fact.PhysicalSelection = raw, selection
		}
	}
	for iteration := 0; iteration < 2; iteration++ {
		if err := applyDNSAvailabilityBaseline(&projection, receipt.Snapshot.Hostname, &availability); err != nil {
			t.Fatal(err)
		}
		encoded, err := platformconfig.EncodeRuntimeSnapshot(projection.RuntimeSnapshot)
		if err != nil {
			t.Fatal(err)
		}
		projection.RuntimeSnapshot, err = platformconfig.DecodeRuntimeSnapshotJSON(encoded)
		if err != nil {
			t.Fatal(err)
		}
		for _, fact := range projection.RuntimeSnapshot.DNSSelections {
			if fact.Type != "A" {
				continue
			}
			if fact.PhysicalSelection != nil || len(fact.PhysicalEvidence) != 0 || len(fact.RetainedPhysicalEvidence) == 0 {
				t.Fatal("fallback conflated serving selection and comparison history")
			}
			input := platformconfig.RuntimeSnapshot{IntentGeneration: "intent-a", PolicyGeneration: "policy-a", DNSSelections: []platformconfig.DNSSelectionObservation{fact}}
			s, _, _ := qualityRestoreInputFixture(t, input)
			ctx, err := s.prepareHistoricalQualityContext(t.Context(), "global")
			if err != nil {
				t.Fatal(err)
			}
			fresh := fact
			fresh.RetainedPhysicalEvidence = nil
			nextProjection := platformIntentProjectionResponse{RuntimeSnapshot: platformconfig.RuntimeSnapshot{DNSSelections: []platformconfig.DNSSelectionObservation{fresh}}}
			if err := s.carryPreparedHistoricalQuality(ctx, &nextProjection, map[string]bool{fact.NodeID + "\x00" + fact.Hostname: true}); err != nil ||
				len(nextProjection.RuntimeSnapshot.DNSSelections[0].RetainedPhysicalEvidence) == 0 {
				t.Fatal("new explicit/universal capture lost history before availability fallback", err)
			}
			current := receipt.Snapshot
			current.Policy, current.CapturedAt = edgequality.DefaultPairedContinuityPolicy(), time.Now().UTC()
			s.bindHistoricalQualityEvidence(ctx, &current, fact.NodeID)
			if current.HistoricalEvidence == nil || current.HistoricalEvidence.Receipt.Digest != receipt.Digest ||
				!current.HistoricalEvidence.Selection.CapturedAt.Equal(selection.CapturedAt) {
				t.Fatal("fallback/restart lost or renewed original comparison")
			}
		}
	}
}

func TestRetainedComparisonNeverPromotesLearningOrReplacesNewerEvidence(t *testing.T) {
	measured := historicalBoundReceiptFixture(t)
	good, _ := json.Marshal(measured)
	learning, _ := businessPublicationFixture(t)
	bad, _ := json.Marshal(learning)
	fact := platformconfig.DNSSelectionObservation{Type: "A", Hostname: measured.Snapshot.Hostname, RetainedPhysicalEvidence: good}
	for index, raw := range []json.RawMessage{nil, bad, json.RawMessage(`{"snapshot":{}}`), json.RawMessage(`{`)} {
		got := retainedPhysicalHistory(fact, raw)
		if !bytes.Equal(got, good) {
			t.Fatalf("unknown/learning evidence erased a verified comparison case=%d before=%d after=%d", index, len(good), len(got))
		}
	}
	fact.RetainedPhysicalEvidence, fact.PhysicalEvidence = nil, bad
	if len(retainedPhysicalHistory(fact, nil)) != 0 {
		t.Fatal("learning was upgraded to a historical comparison")
	}
	fact.Hostname, fact.PhysicalEvidence = "other.example.test", good
	if len(retainedPhysicalHistory(fact, nil)) != 0 {
		t.Fatal("history crossed a DNS query boundary")
	}
	fact.Hostname, fact.PhysicalEvidence = measured.Snapshot.Hostname, good
	fact.PhysicalSelection, _ = compileBoundPhysicalQualitySelection(measured, measured.Snapshot.CapturedAt)
	fact.PhysicalSelection.PrimaryEdgeID = "tampered"
	if len(retainedPhysicalHistory(fact, nil)) != 0 {
		t.Fatal("audit retention laundered a mismatched declared selection")
	}
	newerSnapshot := measured.Snapshot
	newerSnapshot.CapturedAt = newerSnapshot.CapturedAt.Add(time.Second)
	newer, err := edgequality.Capture(newerSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	newerRaw, _ := json.Marshal(newer)
	fact.PhysicalEvidence, fact.PhysicalSelection = nil, nil
	fact.RetainedPhysicalEvidence = good
	if got := retainedPhysicalHistory(fact, newerRaw); !bytes.Equal(got, newerRaw) {
		t.Fatal("new comparison did not replace older evidence")
	}
	fact.RetainedPhysicalEvidence = newerRaw
	if got := retainedPhysicalHistory(fact, good); !bytes.Equal(got, newerRaw) {
		t.Fatal("older receipt replaced a more recent comparison")
	}
}

func TestPreparedHistoricalFactsAreSharedWithoutStorageReads(t *testing.T) {
	source := qualityInputSource{parent: model.PlatformArtifact{ID: "signed-parent"}}
	fact := historicalQualityFact{fact: platformconfig.DNSSelectionObservation{NodeID: "dns-a", Hostname: "app.example.test"}, artifactID: "signed-parent"}
	p := &historicalQualityPreparation{source: source, original: true, expires: time.Now().Add(time.Minute),
		facts: map[string][]historicalQualityFact{"dns-a\x00app.example.test": {fact}}}
	ctx := context.WithValue(t.Context(), historicalQualityPreparationKey{}, p)
	// No store: every concurrent candidate must consume the batch's immutable
	// read, rather than hashing/decoding the entire compiler input again.
	var s Server
	var workers sync.WaitGroup
	for i := 0; i < 32; i++ {
		workers.Go(func() {
			facts, err := s.historicalQualityFacts(ctx, source, "dns-a", "app.example.test")
			if err != nil || !reflect.DeepEqual(facts, []historicalQualityFact{fact}) {
				t.Error("prepared read not shared", err)
			}
		})
	}
	workers.Wait()
	p.inputErr = store.ErrConflict
	if _, err := s.historicalQualityFacts(ctx, source, "dns-a", "app.example.test"); err == nil {
		t.Fatal("failed preparation was silently accepted or retried in live window")
	}
}

func BenchmarkPreparedHistoricalFactLookup(b *testing.B) {
	source := qualityInputSource{parent: model.PlatformArtifact{ID: "signed-parent"}}
	p := &historicalQualityPreparation{source: source, original: true, expires: time.Now().Add(time.Hour), facts: map[string][]historicalQualityFact{
		"dns-a\x00app.example.test": {{artifactID: "signed-parent"}},
	}}
	ctx := context.WithValue(b.Context(), historicalQualityPreparationKey{}, p)
	var s Server
	b.ReportAllocs()
	for b.Loop() {
		if _, err := s.historicalQualityFacts(ctx, source, "dns-a", "app.example.test"); err != nil {
			b.Fatal(err)
		}
	}
}

func TestHistoricalSelectionBudgetIsBoundedAndIdempotent(t *testing.T) {
	remaining := 100
	needed := boundedHistoricalQualitySelector(map[string]bool{"dns-a\x00app.example.test": true}, &remaining)
	summary := store.CompilerDNSSelectionSummary{NodeID: "dns-a", Hostname: "app.example.test", Type: "A", HasRetainedEvidence: true, EvidenceBytes: 90}
	for i := 0; i < 3; i++ {
		if !needed(summary) || remaining != 10 {
			t.Fatal("storage fallback spent the same body budget twice")
		}
	}
	for _, mutate := range []func(*store.CompilerDNSSelectionSummary){
		func(s *store.CompilerDNSSelectionSummary) { s.Hostname = "other.example.test" },
		func(s *store.CompilerDNSSelectionSummary) { s.Type = "AAAA" },
		func(s *store.CompilerDNSSelectionSummary) { s.EvidenceBytes++ },
	} {
		other := summary
		mutate(&other)
		if needed(other) || remaining != 10 {
			t.Fatal("bounded selection widened query scope or byte budget")
		}
	}
	remaining = 80
	needed = boundedHistoricalQualitySelector(nil, &remaining)
	if needed(summary) || remaining != 80 {
		t.Fatal("oversized optional history consumed live capture budget")
	}
}
