package releaseflow

import (
	"fugue/internal/model"
	"testing"
)

func TestLatencyComparisonWindowPolicyIsExplicitAndBounded(t *testing.T) {
	if err := model.ValidateAppZeroDowntimePolicy(&model.AppZeroDowntimePolicy{Enabled: true, Mode: model.AppZeroDowntimeModeSafe, Strategy: model.AppZeroDowntimeStrategyStableCandidate, Gate: &model.AppReleaseGatePolicy{LatencyComparisonWindowSeconds: 300}}); err == nil {
		t.Fatal("comparison shorter than inherited ordinary window was accepted")
	}
	for _, seconds := range []int{-1, 0, 300, 600, 1800, 3600, 3601} {
		policy := &model.AppZeroDowntimePolicy{Enabled: true, Mode: model.AppZeroDowntimeModeSafe, Strategy: model.AppZeroDowntimeStrategyStableCandidate, Gate: &model.AppReleaseGatePolicy{WindowSeconds: 600, LatencyComparisonWindowSeconds: seconds}}
		err := model.ValidateAppZeroDowntimePolicy(policy)
		valid := seconds == 0 || seconds >= 600 && seconds <= 3600
		if (err == nil) != valid {
			t.Fatalf("window %d err=%v", seconds, err)
		}
		if valid {
			normalized := (ReleaseGateEvaluator{}).NormalizePolicy(policy.Gate)
			if normalized.WindowSeconds != 600 || normalized.LatencyComparisonWindowSeconds != seconds || normalized.MinCandidateRequests != DefaultAppReleaseGateMinRequests {
				t.Fatalf("policy changed unrelated gates: %+v", normalized)
			}
		}
	}
}
