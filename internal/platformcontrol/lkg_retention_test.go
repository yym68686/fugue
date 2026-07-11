package platformcontrol

import (
	"fmt"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestPlanPlatformLKGHistoryGCProtectsVerifiedPinnedAndMinimumHistory(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 11, 4, 0, 0, 0, time.UTC)
	history := testLKGHistory(now, 5)
	current := history[4]
	plan := PlanPlatformLKGHistoryGC(PlatformLKGGCRequest{
		History:                   history,
		Current:                   &current,
		PinnedRollbackGenerations: []string{"generation-1"},
		EvaluatedAt:               now,
		DeleteBefore:              now.Add(-24 * time.Hour),
		RetainGenerations:         3,
	})
	if !plan.SafeToApply || len(plan.Blockers) != 0 || plan.KeepCount != 4 || plan.DeleteCount != 1 {
		t.Fatalf("unexpected protected LKG GC plan: %+v", plan)
	}
	reasons := lkgGCDecisionReasons(plan.Decisions)
	if reasons["generation-5"] != "current_verified_lkg" ||
		reasons["generation-1"] != "pinned_rollback_generation" ||
		reasons["generation-4"] != "minimum_verified_history" ||
		reasons["generation-3"] != "minimum_verified_history" ||
		reasons["generation-2"] != "retention_expired" {
		t.Fatalf("unexpected LKG GC decisions: %+v", plan.Decisions)
	}
}

func TestPlanPlatformLKGHistoryGCClampsUnsafeRetentionAndKeepsRecentEvents(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 11, 4, 0, 0, 0, time.UTC)
	history := testLKGHistory(now, 6)
	current := history[5]
	plan := PlanPlatformLKGHistoryGC(PlatformLKGGCRequest{
		History:           history,
		Current:           &current,
		EvaluatedAt:       now,
		DeleteBefore:      now.Add(-108 * time.Hour),
		RetainGenerations: 1,
	})
	if !plan.SafeToApply || plan.KeepCount != 4 || plan.DeleteCount != 2 {
		t.Fatalf("minimum retention and recent events must be protected: %+v", plan)
	}
	reasons := lkgGCDecisionReasons(plan.Decisions)
	if reasons["generation-3"] != "retention_window" ||
		reasons["generation-2"] != "retention_expired" ||
		reasons["generation-1"] != "retention_expired" {
		t.Fatalf("unexpected retention-window decisions: %+v", plan.Decisions)
	}
}

func TestPlanPlatformLKGHistoryGCRollbackReverificationOrdersByVerificationEvent(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 11, 4, 0, 0, 0, time.UTC)
	history := testLKGHistory(now, 4)
	rollbackEvent := history[0]
	rollbackEvent.ID = "lkg-generation-1-reverified"
	rollbackEvent.UpdatedAt = now.Add(time.Minute)
	history = append(history, rollbackEvent)
	current := rollbackEvent
	plan := PlanPlatformLKGHistoryGC(PlatformLKGGCRequest{
		History:      history,
		Current:      &current,
		EvaluatedAt:  now,
		DeleteBefore: now.Add(-24 * time.Hour),
	})
	if !plan.SafeToApply || plan.DeleteCount != 1 {
		t.Fatalf("rollback re-verification must retain the latest three distinct generations: %+v", plan)
	}
	for _, decision := range plan.Decisions {
		if decision.Generation == "generation-1" && (decision.Delete || decision.Reason != "current_verified_lkg") {
			t.Fatalf("all events for the current re-verified generation must be protected: %+v", plan.Decisions)
		}
	}
	if reasons := lkgGCDecisionReasons(plan.Decisions); reasons["generation-2"] != "retention_expired" {
		t.Fatalf("generation ordering must use verification event time: %+v", plan.Decisions)
	}
}

func TestPlanPlatformLKGHistoryGCFailsClosedOnMissingReferencesOrMalformedEvidence(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 11, 4, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		mutate func(*PlatformLKGGCRequest)
	}{
		{
			name: "current generation missing",
			mutate: func(req *PlatformLKGGCRequest) {
				current := req.History[3]
				current.Generation = "missing-current"
				req.Current = &current
			},
		},
		{
			name: "stale current pointer",
			mutate: func(req *PlatformLKGGCRequest) {
				current := req.History[2]
				req.Current = &current
			},
		},
		{
			name: "current snapshot identity mismatch",
			mutate: func(req *PlatformLKGGCRequest) {
				current := req.History[3]
				current.Generation = req.History[2].Generation
				req.Current = &current
			},
		},
		{
			name: "pinned generation missing",
			mutate: func(req *PlatformLKGGCRequest) {
				req.PinnedRollbackGenerations = []string{"missing-pinned"}
			},
		},
		{
			name: "snapshot timestamp missing",
			mutate: func(req *PlatformLKGGCRequest) {
				req.History[0].CreatedAt = time.Time{}
				req.History[0].UpdatedAt = time.Time{}
			},
		},
		{
			name: "mixed artifact scope",
			mutate: func(req *PlatformLKGGCRequest) {
				req.History[0].ScopeKey = "other-scope"
			},
		},
		{
			name: "delete cutoff missing",
			mutate: func(req *PlatformLKGGCRequest) {
				req.DeleteBefore = time.Time{}
			},
		},
		{
			name: "evaluation time missing",
			mutate: func(req *PlatformLKGGCRequest) {
				req.EvaluatedAt = time.Time{}
			},
		},
		{
			name: "delete cutoff in future",
			mutate: func(req *PlatformLKGGCRequest) {
				req.DeleteBefore = req.EvaluatedAt.Add(time.Second)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			history := testLKGHistory(now, 4)
			current := history[3]
			req := PlatformLKGGCRequest{
				History:      history,
				Current:      &current,
				EvaluatedAt:  now,
				DeleteBefore: now.Add(-24 * time.Hour),
			}
			test.mutate(&req)
			plan := PlanPlatformLKGHistoryGC(req)
			if plan.SafeToApply || len(plan.Blockers) == 0 || plan.DeleteCount != 0 || plan.KeepCount != len(history) {
				t.Fatalf("unsafe LKG GC input must fail closed: %+v", plan)
			}
			for _, decision := range plan.Decisions {
				if decision.Delete {
					t.Fatalf("blocked LKG GC plan must not contain delete decisions: %+v", plan)
				}
			}
		})
	}
}

func testLKGHistory(now time.Time, count int) []model.PlatformLKGSnapshot {
	history := make([]model.PlatformLKGSnapshot, 0, count)
	for index := 1; index <= count; index++ {
		verifiedAt := now.Add(-time.Duration(count-index+1) * 24 * time.Hour)
		history = append(history, model.PlatformLKGSnapshot{
			ID:                 fmt.Sprintf("lkg-generation-%d", index),
			ArtifactID:         fmt.Sprintf("artifact-generation-%d", index),
			ArtifactKind:       model.PlatformArtifactKindEdgeRouteBundle,
			ScopeKey:           "global",
			Generation:         fmt.Sprintf("generation-%d", index),
			GenerationSequence: int64(index),
			CreatedAt:          verifiedAt,
			UpdatedAt:          verifiedAt,
		})
	}
	return history
}

func lkgGCDecisionReasons(decisions []PlatformLKGGCDecision) map[string]string {
	out := make(map[string]string, len(decisions))
	for _, decision := range decisions {
		out[decision.Generation] = decision.Reason
	}
	return out
}
