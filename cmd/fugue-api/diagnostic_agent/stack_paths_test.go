package main

import (
	"fmt"
	"strings"
	"testing"
)

func TestStackPathsKeepCallerOrderAndCountEachRecursiveFrameOncePerSample(t *testing.T) {
	stack := " 10 leaf (/app/worker)\n 20 wrapper (/app/worker)\n 30 wrapper (/app/worker)\n 40 caller (/app/worker)\n\n"
	raw := []byte(stack + stack + " 10 leaf (/app/worker)\n 50 other (/app/worker)\n")
	_, cumulative, samples, _, _ := sampledFunctions(raw)
	counts := map[string]int{}
	for _, row := range cumulative {
		counts[row.Function] = row.Samples
		if row.Samples > samples {
			t.Fatal("recursive frame inflated samples")
		}
	}
	if samples != 3 || counts["wrapper (/app/worker)"] != 2 {
		t.Fatal("sample counts changed", counts)
	}
	result := summarizeStackPaths(raw)
	if result.ObservedSamples != 3 || result.OmittedSamples != 0 || len(result.Paths) != 2 || result.Paths[0].Samples != 2 {
		t.Fatalf("lost paths: %+v", result)
	}
	path := result.Paths[0].Frames
	if len(path) != 4 || path[1] != path[2] || result.Frames[path[3]] != "caller (/app/worker)" {
		t.Fatal("caller order/recursion lost")
	}
}

func TestStackPathsDeclareOmittedSamplesAtBudget(t *testing.T) {
	var raw strings.Builder
	for i := 0; i < 520; i++ {
		fmt.Fprintf(&raw, " 10 leaf-%d (/app/worker)\n\n", i)
	}
	result := summarizeStackPaths([]byte(raw.String()))
	if !result.Truncated || result.ObservedSamples != 520 || result.OmittedSamples != 8 || len(result.Paths) != 512 {
		t.Fatalf("missing sample accounting: %+v", result)
	}
}
