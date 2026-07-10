package releaseflow

import (
	"fmt"
	"testing"

	"fugue/internal/weightedselector"
)

func TestWeightedSelectorExtractionPreservesEveryDecision(t *testing.T) {
	t.Parallel()

	candidateSets := [][]WeightedCandidate{
		nil,
		{{ID: "inactive", Weight: 100}},
		{{ID: "stable", Weight: 100, Active: true}},
		{{ID: "stable", Weight: 90, Active: true}, {ID: "candidate", Weight: 10, Active: true}},
		{{ID: "skip", Weight: 50}, {ID: "one", Weight: 1, Active: true}, {ID: "two", Weight: 2, Active: true}},
		{{ID: "zero", Active: true}, {ID: "negative", Weight: -2, Active: true}},
	}
	for setIndex, candidates := range candidateSets {
		extracted := make([]weightedselector.Candidate, len(candidates))
		for index, candidate := range candidates {
			extracted[index] = weightedselector.Candidate(candidate)
		}
		for keyIndex := 0; keyIndex < 10_000; keyIndex++ {
			key := fmt.Sprintf("set-%d-key-%d", setIndex, keyIndex)
			before, beforeOK := SelectWeighted(candidates, key)
			after, afterOK := weightedselector.Select(extracted, key)
			if beforeOK != afterOK ||
				before.Index != after.Index ||
				before.Bucket != after.Bucket ||
				before.TotalWeight != after.TotalWeight ||
				before.SelectedID != after.SelectedID {
				t.Fatalf("selection changed for set=%d key=%q: before=%+v/%t after=%+v/%t", setIndex, key, before, beforeOK, after, afterOK)
			}
		}
	}

	for total := -2; total <= 1_000; total++ {
		for keyIndex := 0; keyIndex < 100; keyIndex++ {
			key := fmt.Sprintf("bucket-%d", keyIndex)
			if before, after := WeightedBucket(key, total), weightedselector.Bucket(key, total); before != after {
				t.Fatalf("bucket changed for key=%q total=%d: before=%d after=%d", key, total, before, after)
			}
		}
	}
}
