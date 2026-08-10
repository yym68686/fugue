package model

import "testing"

func TestDefaultEdgeSelectionPolicyIsStickyAndValidated(t *testing.T) {
	policy := DefaultEdgeSelectionPolicy()
	if policy == nil {
		t.Fatal("default policy is nil")
	}
	if err := policy.Validate(); err != nil {
		t.Fatalf("default policy must validate: %v", err)
	}
	if policy.Mode != EdgeSelectionModeStickyLatency || !policy.Sticky || policy.StandbyCount != 1 {
		t.Fatalf("unexpected sticky policy: %+v", policy)
	}
}

func TestEdgeSelectionPolicyRejectsUnsafeValues(t *testing.T) {
	policy := *DefaultEdgeSelectionPolicy()
	policy.FailureThreshold = 0
	if err := policy.Validate(); err == nil {
		t.Fatal("zero failure threshold must be rejected")
	}
	policy = *DefaultEdgeSelectionPolicy()
	policy.Mode = "country_pinned"
	if err := policy.Validate(); err == nil {
		t.Fatal("unknown selection mode must be rejected")
	}
}
