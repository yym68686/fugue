package observability

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLogBoundaryObservationCountsOccurrencesAndBoundsSamples(t *testing.T) {
	required, seen := map[[32]byte]int{}, map[[32]byte]int{}
	var missingDigests, seenDigests []string
	for i := range 1000 {
		hash := sha256.Sum256([]byte(fmt.Sprintf("private-record-%d", i)))
		required[hash] = 3
		seen[hash] = 1
		missingDigests = append(missingDigests, hex.EncodeToString(hash[:]))
		seenDigests = append(seenDigests, hex.EncodeToString(hash[:]))
	}
	other := sha256.Sum256([]byte("private-observed-record"))
	seen[other] = 2
	seenDigests = append(seenDigests, hex.EncodeToString(other[:]))
	sort.Strings(missingDigests)
	sort.Strings(seenDigests)
	beforeRequired, beforeSeen := fmt.Sprint(required), fmt.Sprint(seen)
	for range 10 {
		fact := observeLogBoundary("cursor", "newer_record", required, seen)
		if fact.Witness != "cursor" || fact.Phase != "newer_record" || fact.RequiredDistinct != 1000 || fact.RequiredOccurrences != 3000 || fact.SeenDistinct != 1001 || fact.SeenOccurrences != 1002 || fact.MissingDistinct != 1000 || fact.MissingOccurrences != 2000 || fact.MissingSamplesOmitted != 996 || fact.SeenSamplesOmitted != 997 {
			t.Fatalf("lost exact boundary comparison: %+v", fact)
		}
		if len(fact.Missing) != logBoundarySampleLimit || len(fact.Seen) != logBoundarySampleLimit {
			t.Fatal("unbounded samples")
		}
		for i := range logBoundarySampleLimit {
			if fact.Missing[i].Digest != missingDigests[i] || fact.Missing[i].Required != 3 || fact.Missing[i].Seen != 1 || fact.Seen[i].Digest != seenDigests[i] {
				t.Fatal("samples depend on map iteration or lose original counts")
			}
		}
		raw, err := json.Marshal(fact)
		if err != nil || len(raw) > 1536 || strings.Contains(string(raw), "private-") {
			t.Fatal("unbounded or payload-bearing boundary evidence", len(raw), err)
		}
	}
	if fmt.Sprint(required) != beforeRequired || fmt.Sprint(seen) != beforeSeen {
		t.Fatal("observation changed collection boundary")
	}
}

func TestLogBoundaryObservationPreservesSatisfiedAndEmptyComparisons(t *testing.T) {
	hash := sha256.Sum256([]byte("private-record"))
	fact := observeLogBoundary("checkpoint", "eof", map[[32]byte]int{hash: 2}, map[[32]byte]int{hash: 3})
	if fact.MissingDistinct != 0 || fact.MissingOccurrences != 0 || fact.RequiredOccurrences != 2 || fact.SeenOccurrences != 3 || len(fact.Missing) != 0 {
		t.Fatal("satisfied boundary reported missing", fact)
	}
	if empty := observeLogBoundary("cursor", "eof", nil, nil); empty.RequiredDistinct != 0 || empty.MissingDistinct != 0 || len(empty.Seen) != 0 {
		t.Fatal("empty witness fabricated evidence", empty)
	}
}

func TestLogBoundaryReceiptCapturesActualMissingWitnessWithoutBehaviorChange(t *testing.T) {
	for _, trial := range []struct{ name, retained, current, witness, phase string }{
		{"newer", "B stdout F retained\n", "C stdout F after\n", "cursor", "newer_record"},
		{"empty", "", "", "cursor", "eof"},
		{"checkpoint", "B stdout F retained\n", "C stdout F after\n", "checkpoint", "newer_record"},
	} {
		t.Run(trial.name, func(t *testing.T) {
			c, target, rotated, _ := rotationFixture(t, 10, trial.retained, trial.current)
			rotationPoll(t, c, target)
			key := logTargetKey(target)
			before := c.cursors[key]
			if trial.phase == "eof" || trial.witness == "checkpoint" {
				cur := before
				cur.RecoveringRotation = true
				if trial.witness == "checkpoint" {
					cur.RecoveryBoundary = cur.Boundary
					cur.Boundary = map[[32]byte]int{}
				}
				c.cursors[key] = cur
			}
			rotated.Store(true)
			rotationPoll(t, c, target)
			value, err := c.pipeline.DiagnosticSources(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			snapshot := value.(map[string]any)
			fact := snapshot["sources"].([]logSourceObservation)[0].BoundaryCheck
			hash := sha256.Sum256([]byte("before"))
			if fact == nil || fact.Witness != trial.witness || fact.Phase != trial.phase || fact.RequiredDistinct != 1 || fact.RequiredOccurrences != 1 || fact.SeenDistinct != 0 || fact.MissingOccurrences != 1 || len(fact.Missing) != 1 || fact.Missing[0].Digest != hex.EncodeToString(hash[:]) {
				t.Fatalf("missing exact original boundary evidence: %+v", fact)
			}
			history := snapshot["gap_history"].(map[string]any)["receipts"].([]logGapObservation)
			if len(history) != 1 || history[0].Events != 1 || !history[0].LowerBound.Equal(before.Time) || !reflect.DeepEqual(history[0].BoundaryCheck, fact) || c.pipeline.kubernetesLogCursorGaps.Load() != 1 {
				t.Fatal("changed gap accounting or lost independent receipt")
			}
			want := []string{"before", "retained", "after"}
			if trial.phase == "eof" {
				want = []string{"before"}
			}
			if got := rotationMessages(t, c.pipeline); !reflect.DeepEqual(got, want) {
				t.Fatal("diagnostics changed admitted record multiplicity", got)
			}
			fact.Missing[0].Required = 999
			if history[0].BoundaryCheck.Missing[0].Required != 1 {
				t.Fatal("source projection aliases history")
			}
			history[0].BoundaryCheck.Missing[0].Required = 999
			c.pipeline.observeLogSource(logSourceObservation{Identity: key, Outcome: "drained"})
			value, _ = c.pipeline.DiagnosticSources(t.Context())
			history = value.(map[string]any)["gap_history"].(map[string]any)["receipts"].([]logGapObservation)
			if history[0].BoundaryCheck.Missing[0].Required != 1 {
				t.Fatal("recovery or returned projection altered retained witness")
			}
		})
	}
}

func TestLogBoundaryObservationConcurrentProjectionAndResponseBudget(t *testing.T) {
	required, seen := map[[32]byte]int{}, map[[32]byte]int{}
	for i := range 16 {
		hash := sha256.Sum256([]byte(fmt.Sprint(i)))
		required[hash], seen[hash] = 3, 1
	}
	fact := observeLogBoundary("checkpoint", "newer_record", required, seen)
	p := NewPipeline(Config{}, nil)
	now := time.Now().UTC()
	for i := range 512 {
		p.observeLogSource(logSourceObservation{Identity: fmt.Sprintf("source-%04d", i), Node: "node-a", ObservedAt: now,
			CursorBefore: now, CursorAfter: now, Outcome: "rotation_gap", GapEvents: 1,
			GapReason: "rotation_boundary_missing", GapLowerBound: now, GapUpperBound: now, BoundaryCheck: fact})
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 25 {
				value, err := p.DiagnosticSources(t.Context())
				if err != nil {
					t.Error(err)
					return
				}
				history := value.(map[string]any)["gap_history"].(map[string]any)["receipts"].([]logGapObservation)
				history[0].BoundaryCheck.Missing[0].Required = 999
			}
		})
	}
	wg.Wait()
	value, _ := p.DiagnosticSources(t.Context())
	raw, err := json.Marshal(value)
	if err != nil || len(raw) > 800<<10 {
		t.Fatal("bounded witness made the source snapshot unavailable", len(raw), err)
	}
	var decoded struct {
		Sources []map[string]any `json:"sources"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	for _, row := range decoded.Sources {
		if _, exists := row["boundary_check"]; exists {
			t.Fatal("witness multiplied across source projection instead of independent gap ring")
		}
	}
	history := value.(map[string]any)["gap_history"].(map[string]any)["receipts"].([]logGapObservation)
	if len(history) != logGapReceiptLimit || history[0].BoundaryCheck.Missing[0].Required != 3 {
		t.Fatal("concurrent returned samples changed retained evidence")
	}
}
