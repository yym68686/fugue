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

type latencyWindowTestQuerier struct {
	rawPreferredReleaseMetricsQuerier
	windows         []time.Duration
	recent, history []releaseflow.ReleaseGateLatencyCohort
	historyError    bool
}

func (q *latencyWindowTestQuerier) QueryReleaseGateLatencyCohorts(_ context.Context, app, candidate, stable string, window time.Duration) ([]releaseflow.ReleaseGateLatencyCohort, error) {
	if app != "app" || candidate != "new" || stable != "old" {
		return nil, errors.New("wrong release identities")
	}
	q.windows = append(q.windows, window)
	if window == 10*time.Minute {
		return q.recent, nil
	}
	if q.historyError {
		return nil, errors.New("history unavailable")
	}
	return q.history, nil
}

func TestSafeRolloutLatencyHistoryPreservesRecentGuards(t *testing.T) {
	for _, scenario := range []string{"sticky_history", "default_unchanged", "absolute_error", "absolute_latency", "recent_regression", "history_regression", "history_deficit", "history_query_error", "fresh_candidate_deficit", "invalid_window"} {
		t.Run(scenario, func(t *testing.T) {
			policy := model.AppReleaseGatePolicy{WindowSeconds: 600, LatencyComparisonWindowSeconds: 1800, MinCandidateRequests: 20, Max5xxRate: .01, MaxP95TTFBMilliseconds: 60000, MaxP99DurationMilliseconds: 30000}
			q := &latencyWindowTestQuerier{rawPreferredReleaseMetricsQuerier: rawPreferredReleaseMetricsQuerier{raw: map[string]map[string]any{
				"new": {"request_count": 42., "p95_ttfb_ms": 14000., "p99_duration_ms": 400., "error_5xx_rate": 0.},
				"old": {"request_count": 6., "p95_ttfb_ms": 160., "p99_duration_ms": 160., "error_5xx_rate": 0.},
			}}}
			q.recent = []releaseflow.ReleaseGateLatencyCohort{{ReleaseID: "new", Hostname: "app.example.test", Path: "/events", Method: "POST", Streaming: true, Requests: 36, P95TTFB: 14000}, {ReleaseID: "old", Hostname: "app.example.test", Path: "/ready", Method: "GET", Requests: 6, P95TTFB: 160}}
			q.history = []releaseflow.ReleaseGateLatencyCohort{{ReleaseID: "new", Hostname: "app.example.test", Path: "/events", Method: "POST", Streaming: true, Requests: 63, P95TTFB: 15000}, {ReleaseID: "old", Hostname: "app.example.test", Path: "/events", Method: "POST", Streaming: true, Requests: 50, P95TTFB: 18000}}
			wantFailure := ""
			switch scenario {
			case "default_unchanged":
				policy.LatencyComparisonWindowSeconds = 0
				wantFailure = "below minimum"
			case "absolute_error":
				q.raw["new"]["error_5xx_rate"] = .1
				wantFailure = "5xx"
			case "absolute_latency":
				q.raw["new"]["p95_ttfb_ms"] = 65000.
				wantFailure = "exceeds"
			case "recent_regression":
				q.recent = append([]releaseflow.ReleaseGateLatencyCohort(nil), q.history...)
				q.recent[0].P95TTFB = 40000
				wantFailure = "worse than stable"
			case "history_regression":
				q.history[0].P95TTFB = 40000
				wantFailure = "worse than stable"
			case "history_deficit":
				q.history[1].Requests = 19
				wantFailure = "below minimum"
			case "history_query_error":
				q.historyError = true
				wantFailure = "unavailable"
			case "fresh_candidate_deficit":
				q.raw["new"]["request_count"] = 5.
				wantFailure = "below minimum"
			case "invalid_window":
				policy.LatencyComparisonWindowSeconds = 7200
				wantFailure = "unavailable"
			}
			s := &Service{releaseGateMetricsQuerier: q}
			state := &safeRolloutState{CandidateApp: model.App{ID: "app"}, Candidate: model.AppRelease{ID: "new", Role: model.AppReleaseRoleCandidate}, StableRelease: model.AppRelease{ID: "old", Role: model.AppReleaseRoleStable}}
			gate := s.evaluateSafeRolloutCandidateCanaryGate(context.Background(), state, policy)
			if wantFailure == "" && gate.Status != model.AppReleaseGateStatusPass || wantFailure != "" && (gate.Status != model.AppReleaseGateStatusFail || !strings.Contains(strings.Join(gate.Failures, ";"), wantFailure)) {
				t.Fatalf("wrong gate: %+v", gate)
			}
			if q.rawCalls != 2 {
				t.Fatalf("recent absolute/error queries were removed: %d", q.rawCalls)
			}
			if (scenario == "recent_regression" || scenario == "default_unchanged") && len(q.windows) != 1 {
				t.Fatal("history masked recent evidence or changed default", q.windows)
			}
			if scenario == "sticky_history" && (len(q.windows) != 2 || q.windows[0] != 10*time.Minute || q.windows[1] != 30*time.Minute) {
				t.Fatal("unbounded or wrong comparison windows", q.windows)
			}
		})
	}
}
