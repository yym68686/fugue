package edgequality

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestLastValidPolicyRetainsOldOrderWithoutHistoryAndWithoutRenewingTime(t *testing.T) {
	snapshot, binding := publishedOrderFixture(t)
	snapshot.Policy = DefaultLastValidPolicy()
	snapshot.HistoricalOrder = nil // Even the exact previous selection has no age cutoff in V8.
	original := *snapshot.PreviousSelection.QualityObservedAt
	snapshot.CapturedAt = original.Add(365 * 24 * time.Hour)
	for i := range snapshot.Candidates {
		snapshot.Candidates[i].ProofObservedAt = timePointer(snapshot.CapturedAt)
	}
	binding.ObservedAt = snapshot.CapturedAt
	for i := 0; i < 2; i++ {
		receipt, err := Capture(snapshot)
		if err != nil || receipt.Result.QualityState != "retained" || !receipt.Result.QualityObservedAt.Equal(original) {
			t.Fatal("age alone discarded or renewed last order", receipt.Result, err)
		}
		selection, err := CompileSelection(receipt, binding, snapshot.CapturedAt)
		if err != nil || !reflect.DeepEqual(selection.OrderedEdgeIDs, []string{"edge-b", "edge-a"}) {
			t.Fatal("last valid order was not publishable", selection, err)
		}
		raw, _ := json.Marshal(receipt)
		var restarted Receipt
		if json.Unmarshal(raw, &restarted) != nil {
			t.Fatal("restart decode failed")
		}
		if _, err := Replay(restarted); err != nil {
			t.Fatal("restart replay changed", err)
		}
		snapshot.PreviousSelection = selection
	}
	snapshot.Candidates[1].ServingBlockedReason = "dns_admission_suspended"
	receipt, err := Capture(snapshot)
	if err == nil {
		if selection, err := CompileSelection(receipt, binding, snapshot.CapturedAt); err == nil && selection.PrimaryEdgeID == "edge-b" {
			t.Fatal("old quality bypassed current suspension")
		}
	}
}

func TestLastValidComparisonRestoresWinnerAfterRecoveryWithoutFreshMeasurements(t *testing.T) {
	snapshot, binding := historicalFixture(t)
	snapshot.Policy = DefaultLastValidPolicy()
	for _, blocked := range []bool{true, false} {
		snapshot.Candidates[1].ServingBlockedReason = ""
		if blocked {
			snapshot.Candidates[1].ServingBlockedReason = "dns_admission_suspended"
		}
		receipt, err := Capture(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		selection, err := CompileSelection(receipt, binding, snapshot.CapturedAt)
		want := "edge-b"
		if blocked {
			want = "edge-a"
		}
		if err != nil || selection.PrimaryEdgeID != want || selection.History == nil || selection.QualityState != "retained" {
			t.Fatal("historical winner failed to rejoin or bypassed admission", selection, err)
		}
	}
	// Missing/learning measurements do not beat complete original evidence.
	if snapshot.HistoricalEvidence == nil {
		t.Fatal("historical comparison erased")
	}
	for _, wrong := range []string{"scope", "route", "path"} {
		copy := snapshot
		copy.Candidates = append([]Candidate(nil), snapshot.Candidates...)
		switch wrong {
		case "scope":
			copy.Scope = "other"
		case "route":
			copy.Candidates[1].RouteGeneration = "different"
		case "path":
			copy.PathPrefix = "/other/"
		}
		r, err := Capture(copy)
		if err == nil && r.Result.ProposedEdgeID == "edge-b" && r.Result.History != nil {
			t.Fatal("history crossed a current context boundary", wrong)
		}
	}
}

func TestLastValidNewCompleteComparisonReplacesOldWinnerIncludingFailures(t *testing.T) {
	history, _ := historicalFixture(t)
	fresh := pairedAlgorithmFixture()
	fresh.Policy = DefaultLastValidPolicy()
	fresh.CurrentEdgeID = "edge-b"
	fresh.HistoricalEvidence = history.HistoricalEvidence
	shift := history.CapturedAt.Sub(fresh.CapturedAt)
	fresh.CapturedAt = history.CapturedAt
	fresh.Candidates = history.Candidates
	for i := range fresh.Observations {
		o := &fresh.Observations[i]
		o.ObservedAt = o.ObservedAt.Add(shift)
		if o.EdgeID == "edge-a" {
			o.EdgeID = "edge-b"
		} else {
			o.EdgeID = "edge-a"
		}
	}
	r := evaluateNetwork(fresh)
	if r.QualityState != "measured" || r.ProposedEdgeID != "edge-a" || r.History != nil || r.QualityObservedAt == nil || !r.QualityObservedAt.After(history.HistoricalEvidence.Selection.CapturedAt) {
		t.Fatal("old winner hid newer complete evidence or connection failures", r)
	}
	p := DefaultLastValidPolicy()
	p.RetainOrderSeconds = 86400
	if model.ValidatePhysicalEdgeQualityPolicy(p) == nil {
		t.Fatal("V8 accepted an age-based expiry")
	}
}
