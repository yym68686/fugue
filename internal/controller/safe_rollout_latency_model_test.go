package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/releaseflow"
)

type modelLatencyTestQuerier struct {
	latencyWindowTestQuerier
	modelCalls int
}

func (q *modelLatencyTestQuerier) QueryReleaseGateModelLatencyCohorts(ctx context.Context, app, candidate, stable string, window time.Duration) ([]releaseflow.ReleaseGateLatencyCohort, error) {
	q.modelCalls++
	return q.latencyWindowTestQuerier.QueryReleaseGateLatencyCohorts(ctx, app, candidate, stable, window)
}

func TestSafeRolloutModelLatencyKeepsRecentAndAbsoluteGuards(t *testing.T) {
	for _, scenario := range []string{"different_models_use_history", "unknown_model", "recent_same_model_regression", "absolute_error", "absolute_latency", "missing_model_querier"} {
		t.Run(scenario, func(t *testing.T) {
			policy := model.AppReleaseGatePolicy{WindowSeconds: 600, LatencyComparisonWindowSeconds: 1800, LatencyCohortModel: true, MinCandidateRequests: 20, Max5xxRate: .01, MaxP95TTFBMilliseconds: 60000}
			q := &modelLatencyTestQuerier{latencyWindowTestQuerier: latencyWindowTestQuerier{rawPreferredReleaseMetricsQuerier: rawPreferredReleaseMetricsQuerier{raw: map[string]map[string]any{
				"new": {"request_count": 40., "p95_ttfb_ms": 40000., "error_5xx_rate": 0.},
				"old": {"request_count": 30., "p95_ttfb_ms": 5000., "error_5xx_rate": 0.},
			}}}}
			q.recent = []releaseflow.ReleaseGateLatencyCohort{
				{ReleaseID: "new", Hostname: "app.example.test", Path: "/events", Method: "POST", Streaming: true, ModelScoped: true, Model: "reasoning", Requests: 40, P95TTFB: 40000},
				{ReleaseID: "old", Hostname: "app.example.test", Path: "/events", Method: "POST", Streaming: true, ModelScoped: true, Model: "fast", Requests: 30, P95TTFB: 5000},
			}
			q.history = append([]releaseflow.ReleaseGateLatencyCohort(nil), q.recent...)
			q.history[1].Model, q.history[1].P95TTFB = "reasoning", 41000
			var querier releaseflow.ReleaseGateMetricsQuerier = q
			wantFailure := ""
			switch scenario {
			case "unknown_model":
				q.history[0].Model = ""
				wantFailure = "below minimum"
			case "recent_same_model_regression":
				q.recent[1].Model = "reasoning"
				wantFailure = "worse than stable"
			case "absolute_error":
				q.raw["new"]["error_5xx_rate"] = .1
				wantFailure = "5xx"
			case "absolute_latency":
				q.raw["new"]["p95_ttfb_ms"] = 65000.
				wantFailure = "exceeds"
			case "missing_model_querier":
				querier = &q.latencyWindowTestQuerier
				wantFailure = "unavailable"
			}
			s := &Service{releaseGateMetricsQuerier: querier}
			state := &safeRolloutState{CandidateApp: model.App{ID: "app"}, Candidate: model.AppRelease{ID: "new"}, StableRelease: model.AppRelease{ID: "old"}}
			gate := s.evaluateSafeRolloutCandidateCanaryGate(context.Background(), state, policy)
			if wantFailure == "" && gate.Status != model.AppReleaseGateStatusPass || wantFailure != "" && (gate.Status != model.AppReleaseGateStatusFail || !strings.Contains(strings.Join(gate.Failures, ";"), wantFailure)) {
				t.Fatal("model evidence weakened a gate", gate)
			}
			if q.rawCalls != 2 || scenario == "recent_same_model_regression" && q.modelCalls != 1 || scenario == "different_models_use_history" && q.modelCalls != 2 {
				t.Fatal("wrong metrics source or history fallback", q.rawCalls, q.modelCalls)
			}
		})
	}
}
