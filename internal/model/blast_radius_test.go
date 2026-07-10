package model

import "testing"

func TestEvaluateBlastRadiusEnforcesMaximumRemovalCaps(t *testing.T) {
	before := map[string]int{"api.example": 4, "admin.example": 2}
	after := map[string]int{"api.example": 1, "admin.example": 1}
	eval := EvaluateBlastRadius(before, after, "hostname", BlastRadiusPolicy{
		PreserveMinEligibleEdgesPerHost: 1,
		MaxRemovedEdgesPerHost:          1,
	})
	if eval.Pass {
		t.Fatalf("expected per-host removal cap to fail: %+v", eval)
	}
	if _, ok := eval.Violations["api.example"]; !ok {
		t.Fatalf("expected api.example removal violation: %+v", eval)
	}

	eval = EvaluateBlastRadius(
		map[string]int{"group-a": 1, "group-b": 1, "group-c": 1},
		map[string]int{"group-c": 1},
		"edge-group",
		BlastRadiusPolicy{
			PreserveMinHealthyEdgeGroups: 1,
			MaxRemovedEdgeGroups:         1,
		},
	)
	if eval.Pass || eval.Violations["_aggregate"] == "" {
		t.Fatalf("expected aggregate edge-group removal cap to fail: %+v", eval)
	}
}

func TestEvaluateBlastRadiusAllowsChangeWithinCaps(t *testing.T) {
	eval := EvaluateBlastRadius(
		map[string]int{"api.example": 3},
		map[string]int{"api.example": 2},
		"hostname",
		BlastRadiusPolicy{
			PreserveMinEligibleEdgesPerHost: 1,
			MaxRemovedEdgesPerHost:          1,
		},
	)
	if !eval.Pass {
		t.Fatalf("expected bounded removal to pass: %+v", eval)
	}
}
