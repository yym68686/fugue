package weightedselector

import "testing"

func TestSelectIsDeterministicAndSkipsInactiveCandidates(t *testing.T) {
	t.Parallel()

	candidates := []Candidate{
		{ID: "inactive", Weight: 90},
		{ID: " stable ", Weight: 90, Active: true},
		{ID: "candidate", Weight: 10, Active: true},
	}
	first, ok := Select(candidates, "request-123")
	if !ok {
		t.Fatal("expected weighted selection")
	}
	second, ok := Select(candidates, "request-123")
	if !ok || first != second {
		t.Fatalf("selection must be deterministic: first=%+v second=%+v ok=%t", first, second, ok)
	}
	if first.SelectedID == "inactive" || first.TotalWeight != 100 {
		t.Fatalf("unexpected weighted selection: %+v", first)
	}
	if first.SelectedID == "stable" && candidates[first.Index].ID != " stable " {
		t.Fatalf("selection index no longer references the original candidate: %+v", first)
	}
}

func TestSelectRejectsCandidatesWithoutPositiveActiveWeight(t *testing.T) {
	t.Parallel()

	for _, candidates := range [][]Candidate{
		nil,
		{{ID: "inactive", Weight: 100}},
		{{ID: "zero", Active: true}},
		{{ID: "negative", Weight: -1, Active: true}},
	} {
		if selection, ok := Select(candidates, "request-123"); ok {
			t.Fatalf("unexpected selection %+v for candidates %+v", selection, candidates)
		}
	}
}

func TestBucketHandlesNonPositiveTotals(t *testing.T) {
	t.Parallel()

	if got := Bucket("request-123", 0); got != 0 {
		t.Fatalf("zero total bucket = %d, want 0", got)
	}
	if got := Bucket("request-123", -1); got != 0 {
		t.Fatalf("negative total bucket = %d, want 0", got)
	}
}
