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
