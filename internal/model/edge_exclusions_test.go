package model

import (
	"testing"
	"time"
)

func TestEdgeRoutePolicyExclusionLifecycleNeverAutoClears(t *testing.T) {
	now := time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC)
	expires := now.Add(25 * time.Hour)
	policy := EdgeRoutePolicy{
		ExcludedEdgeIDs: []string{"edge-de-1"}, ExclusionOwnerDigest: "sha256:owner",
		ExclusionGeneration: 1, ExclusionFence: "fence", ExclusionExpiresAt: &expires,
	}
	cases := []struct {
		at   time.Time
		want string
	}{
		{now, EdgeExclusionLifecycleActive},
		{now.Add(2 * time.Hour), EdgeExclusionLifecycleExpiring24H},
		{now.Add(24*time.Hour + time.Second), EdgeExclusionLifecycleExpiring1H},
		{now.Add(25 * time.Hour), EdgeExclusionLifecycleExpiredHold},
		{now.Add(30 * 24 * time.Hour), EdgeExclusionLifecycleExpiredHold},
	}
	for _, tc := range cases {
		if got := EdgeRoutePolicyExclusionLifecycleAt(policy, tc.at); got != tc.want {
			t.Fatalf("lifecycle at %s = %q, want %q", tc.at, got, tc.want)
		}
		if !EdgeRoutePolicyHasExclusions(policy) {
			t.Fatal("time passage must not clear exclusion material")
		}
	}
	policy.ExclusionOwnerDigest = ""
	if got := EdgeRoutePolicyExclusionLifecycleAt(policy, now); got != EdgeExclusionLifecycleLegacyHold {
		t.Fatalf("legacy lifecycle = %q", got)
	}
}
