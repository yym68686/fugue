package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/releaseflow"
)

type matchedLatencyGateQuerier struct {
	rawPreferredReleaseMetricsQuerier
	cohorts []releaseflow.ReleaseGateLatencyCohort
	err     error
	calls   int
}

func (q *matchedLatencyGateQuerier) QueryReleaseGateLatencyCohorts(context.Context, string, string, string, time.Duration) ([]releaseflow.ReleaseGateLatencyCohort, error) {
	q.calls++
	return q.cohorts, q.err
}

func TestSafeRolloutMatchedLatencyRetainsAbsoluteErrorsAndEvidenceFailures(t *testing.T) {
	for _, scenario := range []string{"mixed_traffic", "cohort_regression", "absolute_latency", "absolute_error", "query_failure", "candidate_sample_deficit", "matched_sample_deficit"} {
		t.Run(scenario, func(t *testing.T) {
			policy := model.AppReleaseGatePolicy{MinCandidateRequests: 20, WindowSeconds: 600, Max5xxRate: 0.01, MaxP95TTFBMilliseconds: 60000, MaxP99DurationMilliseconds: 30000}
			q := &matchedLatencyGateQuerier{rawPreferredReleaseMetricsQuerier: rawPreferredReleaseMetricsQuerier{raw: map[string]map[string]any{
				"new": {"request_count": 110., "p95_ttfb_ms": 33000., "p99_duration_ms": 8000., "error_5xx_rate": 0.},
				"old": {"request_count": 140., "p95_ttfb_ms": 11000., "p99_duration_ms": 250., "error_5xx_rate": 0.},
			}}}
			q.cohorts = []releaseflow.ReleaseGateLatencyCohort{
				{ReleaseID: "new", Hostname: "app.example.test", Path: "/events", Method: "POST", Streaming: true, Requests: 65, P95TTFB: 36000},
				{ReleaseID: "old", Hostname: "app.example.test", Path: "/events", Method: "POST", Streaming: true, Requests: 55, P95TTFB: 35000},
			}
			wantFailure := ""
			switch scenario {
			case "cohort_regression":
				q.cohorts[0].P95TTFB = 62000
				wantFailure = "cohort"
			case "absolute_latency":
				q.raw["new"]["p95_ttfb_ms"] = 61000.
				wantFailure = "exceeds"
			case "absolute_error":
				q.raw["new"]["error_5xx_rate"] = 0.05
				wantFailure = "5xx"
			case "query_failure":
				q.err = errors.New("probe backend unavailable")
				wantFailure = "cohorts unavailable"
			case "candidate_sample_deficit":
				q.raw["new"]["request_count"] = 5.
				q.cohorts[0].Requests = 5
				wantFailure = "below minimum"
			case "matched_sample_deficit":
				q.cohorts[1].Requests = 5
				wantFailure = "matched latency cohort request count"
			}
			s := &Service{releaseGateMetricsQuerier: q}
			state := &safeRolloutState{CandidateApp: model.App{ID: "app"}, Candidate: model.AppRelease{ID: "new", Role: model.AppReleaseRoleCandidate}, StableRelease: model.AppRelease{ID: "old", Role: model.AppReleaseRoleStable}}
			gate := s.evaluateSafeRolloutCandidateCanaryGate(context.Background(), state, policy)
			if q.calls != 1 || q.rawCalls != 2 {
				t.Fatalf("wrong evidence queries: %+v", q)
			}
			if wantFailure == "" {
				if gate.Status != model.AppReleaseGateStatusPass {
					t.Fatalf("traffic-mix false regression: %+v", gate)
				}
			} else if gate.Status != model.AppReleaseGateStatusFail || !strings.Contains(strings.Join(gate.Failures, ";"), wantFailure) {
				t.Fatalf("missing failure %s: %+v", wantFailure, gate)
			}
			if scenario == "candidate_sample_deficit" && !safeRolloutCanaryGateOnlySampleDeficit(gate, policy) {
				t.Fatal("low-weight sample deficit cannot use existing bounded next step", gate)
			}
			if scenario == "matched_sample_deficit" && safeRolloutCanaryGateOnlySampleDeficit(gate, policy) {
				t.Fatal("missing stable cohort incorrectly bypassed", gate)
			}
		})
	}
}
