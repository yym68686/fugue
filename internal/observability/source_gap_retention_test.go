package observability

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestGapHistoryConcurrentProjection(t *testing.T) {
	p := NewPipeline(Config{}, nil)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 100 {
				p.observeLogSource(logSourceObservation{Identity: "source", ObservedAt: time.Now().UTC(),
					Outcome: "rotation_gap", GapEvents: 1, GapReason: "rotation_boundary_missing"})
				if _, err := p.DiagnosticSources(t.Context()); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	value, err := p.DiagnosticSources(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	history := value.(map[string]any)["gap_history"].(map[string]any)
	rows := history["receipts"].([]logGapObservation)
	if len(rows) != logGapReceiptLimit || history["total"] != uint64(400) || history["evicted"] != uint64(400-logGapReceiptLimit) {
		t.Fatal("concurrent gaps lost or unbounded", history)
	}
	for i, row := range rows {
		if row.Sequence != uint64(400-logGapReceiptLimit+i+1) || row.Events != 1 {
			t.Fatal("gap sequence or delta changed", row)
		}
	}
}

func TestGapHistorySurvivesDetailTruncationAndSourceEviction(t *testing.T) {
	p := NewPipeline(Config{}, nil)
	now := time.Now().UTC()
	p.observeLogSource(logSourceObservation{Identity: "gap-source", Node: "node-a", ObservedAt: now, Outcome: "queue_rejected",
		Target:    logTargetObservation{PodPhase: "Failed", ContainerState: "terminated", StatusUnknown: true},
		GapEvents: 1, GapReason: "rotation_boundary_missing", GapLowerBound: now.Add(-time.Minute), GapUpperBound: now})
	for i := range 2100 {
		p.observeLogSource(logSourceObservation{Identity: fmt.Sprintf("pending-%04d", i), ObservedAt: now.Add(time.Second), PendingAt: now, Outcome: "cycle_budget"})
	}
	value, err := p.DiagnosticSources(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	snapshot := value.(map[string]any)
	if snapshot["truncated"] != true || snapshot["evicted_sources"] == uint64(0) {
		t.Fatal("fixture did not exercise both detail truncation and source eviction")
	}
	history, ok := snapshot["gap_history"].(map[string]any)
	if !ok {
		t.Fatal("new gap evidence disappeared with the source detail")
	}
	rows := history["receipts"].([]logGapObservation)
	if len(rows) != 1 || rows[0].Identity != "gap-source" || rows[0].Events != 1 || rows[0].Reason != "rotation_boundary_missing" || !rows[0].ObservedAt.Equal(now) || !rows[0].Target.StatusUnknown || rows[0].Target.ContainerState != "terminated" || history["total"] != uint64(1) {
		t.Fatal("gap evidence was lost or misattributed", history)
	}
	for range logGapReceiptLimit + 1 {
		p.observeLogSource(logSourceObservation{Identity: "new-gap", ObservedAt: now, Outcome: "rotation_gap", GapEvents: 1, GapReason: "rotation_boundary_missing"})
	}
	if rows[0].Identity != "gap-source" {
		t.Fatal("returned gap history shares eviction backing array")
	}
	value, _ = p.DiagnosticSources(t.Context())
	history = value.(map[string]any)["gap_history"].(map[string]any)
	if len(history["receipts"].([]logGapObservation)) != logGapReceiptLimit || history["total"] != uint64(logGapReceiptLimit+2) || history["evicted"] != uint64(2) {
		t.Fatal("gap retention is unbounded or unaccounted", history)
	}
}

func TestGapHistoryRecordsNewEventsNotRetainedCountsOrWidening(t *testing.T) {
	p := NewPipeline(Config{}, nil)
	now := time.Now().UTC()
	p.observeLogSource(logSourceObservation{Identity: "source", ObservedAt: now, Outcome: "rotation_gap", GapEvents: 2,
		GapReason: "rotation_boundary_missing", GapLowerBound: now.Add(-time.Minute), GapUpperBound: now})
	p.observeLogSource(logSourceObservation{Identity: "source", ObservedAt: now.Add(time.Second), Outcome: "queue_rejected",
		GapReason: "rotation_boundary_missing", GapLowerBound: now.Add(-time.Minute), GapUpperBound: now.Add(time.Second)})
	p.observeLogSource(logSourceObservation{Identity: "source", ObservedAt: now.Add(2 * time.Second), Outcome: "node_backoff"})
	p.observeLogSource(logSourceObservation{Identity: "source", ObservedAt: now.Add(3 * time.Second), Outcome: "drained"})
	value, _ := p.DiagnosticSources(t.Context())
	history := value.(map[string]any)["gap_history"].(map[string]any)
	rows := history["receipts"].([]logGapObservation)
	if len(rows) != 1 || rows[0].Events != 2 || !rows[0].ObservedAt.Equal(now) || !rows[0].UpperBound.Equal(now) || history["total"] != uint64(1) {
		t.Fatal("cumulative counts, recovery or widening fabricated a new gap event", history)
	}
	if p.sourceObservations["source"].GapEvents != 2 {
		t.Fatal("diagnostics changed existing gap accounting")
	}
}
