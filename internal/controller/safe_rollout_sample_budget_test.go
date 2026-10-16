package controller

import (
	"context"
	"testing"

	"fugue/internal/model"
)

func TestCanarySmallSampleRemainsInconclusiveUnlessAbsoluteGateFails(t *testing.T) {
	for _, hardFailure := range []bool{false, true} {
		policy := model.AppReleaseGatePolicy{MinCandidateRequests: 20, Max5xxRate: .01}
		candidate := map[string]any{"request_count": 3, "status_2xx_rate": 2.0 / 3, "has_status_class_counts": true}
		if hardFailure {
			candidate["error_5xx_rate"] = 1.0 / 3
		}
		svc := &Service{releaseGateMetricsQuerier: releaseMetricsByIDQuerier{metrics: map[string]map[string]any{
			"candidate": candidate,
			"stable":    {"request_count": 200, "status_2xx_rate": .85, "has_status_class_counts": true},
		}}}
		state := &safeRolloutState{Candidate: model.AppRelease{ID: "candidate"}, StableRelease: model.AppRelease{ID: "stable"}}
		gate := svc.evaluateSafeRolloutCandidateCanaryGate(context.Background(), state, policy)
		if gate.Status != model.AppReleaseGateStatusFail || safeRolloutCanaryGateOnlySampleDeficit(gate, policy) == hardFailure {
			t.Fatalf("hardFailure=%v: wrong canary classification: %+v", hardFailure, gate)
		}
	}
}

func TestMatchedSampleDeficitNeverBypassesFailuresOrFinalPromotion(t *testing.T) {
	policy := model.AppReleaseGatePolicy{MinCandidateRequests: 20}
	for _, scenario := range []string{"deficit", "absolute_error", "relative_error", "stable_missing", "query_failure", "no_verified_cohorts", "enough_samples"} {
		t.Run(scenario, func(t *testing.T) {
			cohorts := map[string]any{"mode": "matched_latency_cohorts", "matched_candidate_requests": 0.0}
			comparison := map[string]any{"latency_cohorts_evaluated": true, "latency_comparison": cohorts}
			gate := model.AppReleaseGateResult{Status: model.AppReleaseGateStatusFail,
				Metrics:  map[string]any{"request_count": 35.0, "comparison": comparison},
				Failures: []string{"matched latency cohort request count 0 is below minimum 20"}}
			switch scenario {
			case "absolute_error":
				gate.Failures = append(gate.Failures, "5xx rate 0.1000 exceeds 0.0100")
			case "relative_error":
				gate.Failures = append(gate.Failures, "candidate p95 ttfb is worse than stable in cohort POST /events")
			case "stable_missing":
				gate.Failures = append(gate.Failures, "stable baseline request count 0 is below minimum 1 for comparative release gate")
			case "query_failure":
				gate.Failures = append(gate.Failures, "comparative latency cohorts unavailable")
			case "no_verified_cohorts":
				comparison["latency_cohorts_evaluated"] = false
			case "enough_samples":
				cohorts["matched_candidate_requests"] = 20.0
			}
			inconclusive := safeRolloutCanaryGateOnlySampleDeficit(gate, policy)
			if inconclusive != (scenario == "deficit") {
				t.Fatalf("incorrect sample classification: %+v", gate)
			}
			if inconclusive && (!safeRolloutHasLaterCanaryStep([]int{1, 5, 25, 50, 100}, 2) || safeRolloutHasLaterCanaryStep([]int{1, 5, 25, 50, 100}, 3)) {
				t.Fatal("sample deficit must allow 25->50, never 50->100")
			}
		})
	}
}
