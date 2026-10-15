package releaseflow

import (
	"testing"

	"fugue/internal/model"
)

func TestComparisonDefersBelowConfiguredSampleFloor(t *testing.T) {
	policy := model.AppReleaseGatePolicy{MinCandidateRequests: 20, Max5xxRate: .01, MaxP95TTFBMilliseconds: 2000}
	candidate := map[string]any{"request_count": 3, "status_2xx_rate": 2.0 / 3, "has_status_class_counts": true}
	stable := map[string]any{"request_count": 200, "status_2xx_rate": .85, "has_status_class_counts": true}
	comparison := BuildReleaseGateComparisonMetrics(candidate, stable)
	if got := ReleaseGateComparisonFailures(comparison, policy); len(got) != 0 {
		t.Fatalf("undersampled ratios classified as regressions: %v", got)
	}
	if got := ReleaseGateMetricFailures(candidate, policy); len(got) != 1 || !releaseflowStringsContain(got, "request count 3 is below minimum 20") {
		t.Fatalf("sample deficit was not preserved: %v", got)
	}
	candidate["request_count"] = 20
	if got := ReleaseGateComparisonFailures(BuildReleaseGateComparisonMetrics(candidate, stable), policy); !releaseflowStringsContain(got, "candidate 2xx rate") {
		t.Fatalf("regression at sample floor was not rejected: %v", got)
	}
}

func TestSampleDeficitDoesNotSuppressAbsoluteFailures(t *testing.T) {
	policy := model.AppReleaseGatePolicy{MinCandidateRequests: 20, Max5xxRate: .01, MaxEdgeUpstreamErrorRate: .005, MaxP95TTFBMilliseconds: 2000}
	metrics := map[string]any{"request_count": 3, "error_5xx_rate": 1.0 / 3, "edge_upstream_error_rate": 1.0 / 3, "p95_ttfb_ms": 3000}
	got := ReleaseGateMetricFailures(metrics, policy)
	for _, want := range []string{"below minimum", "5xx rate", "upstream error rate", "p95 ttfb"} {
		if !releaseflowStringsContain(got, want) {
			t.Fatalf("absolute failure %q was not preserved: %v", want, got)
		}
	}
}
