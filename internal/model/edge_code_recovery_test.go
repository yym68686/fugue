package model

import (
	"strings"
	"testing"
)

func recoveryTarget(slot string, generation uint64, code string) EdgeCodeRecoveryTarget {
	return EdgeCodeRecoveryTarget{Slot: slot, FrontGeneration: generation, BundleGeneration: "bundle-" + code + ".p10.r1", WorkerSourceSHA: strings.Repeat(code, 40), WorkerImageDigest: "sha256:" + strings.Repeat(code, 64)}
}

func TestEdgeCodeRecoveryResumesThroughOnlyRetainedInactiveSlots(t *testing.T) {
	record := "sha256:" + strings.Repeat("e", 64)
	current, previous, observed := recoveryTarget("a", 13, "c"), recoveryTarget("b", 12, "b"), recoveryTarget("a", 11, "a")
	plan, err := PlanEdgeCodeRecovery(record, observed, current, &previous)
	if err != nil || len(plan.Stages) != 2 || plan.Stages[0] != previous || plan.Stages[1] != current || plan.ExecutionAuthorized {
		t.Fatal("same-slot recovery must preserve the serving process", plan, err)
	}
	active := observed
	for _, stage := range plan.Stages {
		if stage.Slot == active.Slot || stage.FrontGeneration <= active.FrontGeneration {
			t.Fatal("plan overwrites active slot or reuses fence")
		}
		active = stage
	}
	// After a crash following the first activation, re-read actual local state;
	// never replay the bridge or blindly restart the now-active previous slot.
	resumed, err := PlanEdgeCodeRecovery(record, previous, current, &previous)
	if err != nil || len(resumed.Stages) != 1 || resumed.Stages[0] != current {
		t.Fatal("bridge restart not resumable", resumed, err)
	}
	done, err := PlanEdgeCodeRecovery(record, current, current, &previous)
	if err != nil || len(done.Stages) != 0 || done.ExecutionAuthorized {
		t.Fatal("current member was scheduled again", done, err)
	}
}

func TestEdgeCodeRecoveryRejectsMissingHistoryAndConflictingAuthority(t *testing.T) {
	for _, scenario := range []string{"missing_bridge", "wrong_bridge_slot", "stale_bridge", "future_bridge", "future_member", "same_fence_different_code", "invalid_record", "invalid_image", "invalid_source", "invalid_bundle", "zero_fence", "overflow_fence"} {
		t.Run(scenario, func(t *testing.T) {
			current, previous, observed := recoveryTarget("a", 13, "c"), recoveryTarget("b", 12, "b"), recoveryTarget("a", 11, "a")
			bridge := &previous
			record := "sha256:" + strings.Repeat("e", 64)
			switch scenario {
			case "missing_bridge":
				bridge = nil
			case "wrong_bridge_slot":
				previous.Slot = "a"
			case "stale_bridge":
				previous.FrontGeneration = 11
			case "future_bridge":
				previous.FrontGeneration = 14
			case "future_member":
				observed.FrontGeneration = 14
			case "same_fence_different_code":
				observed.FrontGeneration = 13
			case "invalid_record":
				record = "unverified"
			case "invalid_image":
				current.WorkerImageDigest = "mutable:tag"
			case "invalid_source":
				current.WorkerSourceSHA = "latest"
			case "invalid_bundle":
				observed.BundleGeneration = ""
			case "zero_fence":
				observed.FrontGeneration = 0
			case "overflow_fence":
				current.FrontGeneration = ^uint64(0)
			}
			plan, err := PlanEdgeCodeRecovery(record, observed, current, bridge)
			if err == nil || len(plan.Stages) != 0 || plan.ExecutionAuthorized {
				t.Fatal("unsupported recovery invented a mutable target", plan, err)
			}
		})
	}
}

func TestOppositeSlotRecoveryDoesNotRequireAnInventedIntermediateGeneration(t *testing.T) {
	observed, current := recoveryTarget("a", 3, "a"), recoveryTarget("b", 10, "b")
	plan, err := PlanEdgeCodeRecovery("sha256:"+strings.Repeat("c", 64), observed, current, nil)
	if err != nil || len(plan.Stages) != 1 || plan.Stages[0] != current {
		t.Fatal(plan, err)
	}
}
