package compositecoordinator

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"fugue/internal/releasecontract"
)

func TestCompositeRuntimeLaneGenesisAndReservationAreMonotonic(t *testing.T) {
	now := time.Date(2026, 7, 23, 22, 0, 0, 0, time.UTC)
	settled := noopAuthorizationRecord(t, "composite-lane-settled")
	settled = advanceCompositeRecordToCommitted(t, settled)
	lane, err := NewRuntimeLaneFromHistory([]Record{settled}, now)
	if err != nil {
		t.Fatal(err)
	}
	if lane.Generation != settled.Plan.Generation || lane.FencingEpoch != settled.Plan.FencingEpoch ||
		lane.Version != 0 || lane.ActiveRecordID != "" || lane.LastSettledRecordID != settled.ID {
		t.Fatalf("unexpected genesis lane: %#v", lane)
	}
	plan := noopAuthorizationRecord(t, "composite-lane-next").Plan
	plan.Generation = "8"
	plan.FencingEpoch = "12"
	plan.Digest = ""
	plan = mustRebuildCompositePlan(t, plan)
	record, err := NewRecord("composite-lane-next", plan, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	next, err := ReserveRuntimeLane(lane, record, 0, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if next.Generation != "8" || next.FencingEpoch != "12" || next.Version != 1 ||
		next.ActiveRecordID != "composite-lane-next" || next.LastSettledRecordID != settled.ID {
		t.Fatalf("unexpected reserved lane: %#v", next)
	}
	if _, err := ReserveRuntimeLane(next, record, next.Version, now.Add(2*time.Second)); !errors.Is(err, ErrRuntimeLaneConflict) {
		t.Fatalf("second active record did not fail closed: %v", err)
	}
	mutated := next
	mutated.ActivePlanDigest = coordinatorDigest("f")
	mutated.Digest = DigestRuntimeLane(mutated)
	if VerifyRuntimeLane(mutated) != nil {
		t.Fatal("well-shaped mutation should reach history binding check")
	}
	if err := VerifyRuntimeLaneHistory(mutated, []Record{settled, record}); !errors.Is(err, ErrRuntimeLaneConflict) {
		t.Fatalf("mutated active plan binding error=%v", err)
	}
}

func TestCompositeRuntimeLaneRejectsStaleOrSkippedCAS(t *testing.T) {
	now := time.Date(2026, 7, 23, 22, 0, 0, 0, time.UTC)
	lane, err := NewRuntimeLaneFromHistory(nil, now)
	if err != nil {
		t.Fatal(err)
	}
	plan := noopAuthorizationRecord(t, "composite-lane-plan").Plan
	for _, test := range []struct {
		name            string
		generation      string
		fencing         string
		expectedVersion int64
	}{
		{name: "stale version", generation: "1", fencing: "1", expectedVersion: 1},
		{name: "skipped generation", generation: "2", fencing: "1", expectedVersion: 0},
		{name: "skipped fence", generation: "1", fencing: "2", expectedVersion: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := plan
			candidate.Generation = test.generation
			candidate.FencingEpoch = test.fencing
			candidate.Digest = ""
			candidate = mustRebuildCompositePlan(t, candidate)
			record, err := NewRecord("composite-lane-candidate", candidate, now.Add(time.Second))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ReserveRuntimeLane(lane, record, test.expectedVersion, now.Add(time.Second)); !errors.Is(err, ErrRuntimeLaneConflict) {
				t.Fatalf("unsafe reservation error=%v", err)
			}
		})
	}
}

func TestCompositeRuntimeLaneGenesisFailsClosedOnNonterminalHistory(t *testing.T) {
	record := noopAuthorizationRecord(t, "composite-lane-active")
	if _, err := NewRuntimeLaneFromHistory([]Record{record}, record.UpdatedAt.Add(time.Second)); !errors.Is(err, ErrRuntimeLaneConflict) {
		t.Fatalf("nonterminal history error=%v", err)
	}
}

func TestCompositeRuntimeLaneHistoryAllowsActiveTerminalPendingSettlement(t *testing.T) {
	now := time.Date(2026, 7, 23, 22, 0, 0, 0, time.UTC)
	lane, err := NewRuntimeLaneFromHistory(nil, now)
	if err != nil {
		t.Fatal(err)
	}
	plan := noopAuthorizationRecord(t, "composite-lane-terminal").Plan
	plan.Generation = "1"
	plan.FencingEpoch = "1"
	plan.Digest = ""
	plan = mustRebuildCompositePlan(t, plan)
	record, err := NewRecord("composite-lane-terminal", plan, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	lane, err = ReserveRuntimeLane(lane, record, 0, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	record = advanceCompositeRecordToCommitted(t, record)
	if err := VerifyRuntimeLaneHistory(lane, []Record{record}); err != nil {
		t.Fatalf("terminal active record must remain bound until settlement: %v", err)
	}
}

func TestCompositeRuntimeLaneRejectsUnknownDuplicateAndTrailingDurableJSON(t *testing.T) {
	lane, err := NewRuntimeLaneFromHistory(nil, time.Date(2026, 7, 23, 22, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(lane)
	if err != nil {
		t.Fatal(err)
	}
	unknown := bytes.Replace(encoded, []byte(`"kind":`), []byte(`"unknown":false,"kind":`), 1)
	duplicate := bytes.Replace(encoded, []byte(`"kind":`), []byte(`"key":"composite-runtime","kind":`), 1)
	for name, candidate := range map[string][]byte{
		"unknown":   unknown,
		"duplicate": duplicate,
		"trailing":  append(append([]byte(nil), encoded...), []byte(` {}`)...),
	} {
		t.Run(name, func(t *testing.T) {
			var decoded RuntimeLane
			if err := json.Unmarshal(candidate, &decoded); err == nil {
				t.Fatal("unsafe durable JSON decoded")
			}
		})
	}
}

func advanceCompositeRecordToCommitted(t *testing.T, record Record) Record {
	t.Helper()
	now := record.UpdatedAt
	transitions := []Transition{
		{Kind: TransitionBeginApply},
		{Kind: TransitionBeginObservation, EvidenceDigest: coordinatorDigest("1")},
		{Kind: TransitionCompleteObservation, EvidenceDigest: coordinatorDigest("2")},
		{Kind: TransitionBeginObservation, EvidenceDigest: coordinatorDigest("3")},
		{Kind: TransitionCompleteObservation, EvidenceDigest: coordinatorDigest("4")},
	}
	for _, transition := range transitions {
		now = now.Add(time.Second)
		var err error
		record, err = ApplyTransition(record, transition, now)
		if err != nil {
			t.Fatal(err)
		}
	}
	return record
}

func mustRebuildCompositePlan(t *testing.T, plan releasecontract.CompositeReleasePlan) releasecontract.CompositeReleasePlan {
	t.Helper()
	rebuilt, err := releasecontract.NewCompositeReleasePlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	return rebuilt
}
