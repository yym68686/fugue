package controller

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/releaseflow"
)

type matchedLatencyGateQuerier struct {
	rawPreferredReleaseMetricsQuerier
	cohorts  []releaseflow.ReleaseGateLatencyCohort
	sequence [][]releaseflow.ReleaseGateLatencyCohort
	err      error
	calls    int
}

func (q *matchedLatencyGateQuerier) QueryReleaseGateLatencyCohorts(context.Context, string, string, string, time.Duration) ([]releaseflow.ReleaseGateLatencyCohort, error) {
	q.calls++
	if len(q.sequence) > 0 {
		return q.sequence[min(q.calls-1, len(q.sequence)-1)], q.err
	}
	return q.cohorts, q.err
}

func TestMatchedSampleDeficitOnlyAdvancesToAnotherCanary(t *testing.T) {
	for _, enoughAtFinal := range []bool{false, true} {
		t.Run(map[bool]string{false: "final_deficit_aborts", true: "more_samples_allow_promotion"}[enoughAtFinal], func(t *testing.T) {
			stateStore, previous, candidate, op := newSafeRolloutTestState(t)
			candidate.Spec.Continuity.ZeroDowntime.Canary = &model.AppRolloutCanarySpec{Enabled: true, InitialWeight: 25, MaxWeight: 100, StepWeights: []int{25, 50, 100}}
			candidate.Spec.Continuity.ZeroDowntime.Gate = &model.AppReleaseGatePolicy{MinCandidateRequests: 20, MaxP95TTFBMilliseconds: 60000, MaxP99DurationMilliseconds: 30000}
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) }))
			defer upstream.Close()
			svc := newSafeRolloutIntegrationService(stateStore, upstream.URL, safeRolloutDrainMetrics{Ready: true, ActiveConnections: 0, MaxActiveConnections: 1, FinalCount: 1, Summary: map[string]any{"active_connections": 0}})
			svc.safeRolloutSleep = func(context.Context, time.Duration) error { return nil }
			state, err := svc.prepareSafeZeroDowntimeRollout(context.Background(), op, previous, candidate)
			if err != nil {
				t.Fatal(err)
			}
			q := &matchedLatencyGateQuerier{rawPreferredReleaseMetricsQuerier: rawPreferredReleaseMetricsQuerier{raw: map[string]map[string]any{
				state.Candidate.ID:     {"request_count": 35., "p95_ttfb_ms": 10000., "p99_duration_ms": 500.},
				state.StableRelease.ID: {"request_count": 100., "p95_ttfb_ms": 10000., "p99_duration_ms": 500.},
			}}}
			first := []releaseflow.ReleaseGateLatencyCohort{{ReleaseID: state.Candidate.ID, Hostname: "app.example.test", Path: "/events", Method: "POST", Streaming: true, Requests: 15, P95TTFB: 10000}, {ReleaseID: state.StableRelease.ID, Hostname: "app.example.test", Path: "/events", Method: "POST", Streaming: true, Requests: 40, P95TTFB: 10000}}
			second := append([]releaseflow.ReleaseGateLatencyCohort(nil), first...)
			if enoughAtFinal {
				second[0].Requests = 30
			}
			q.sequence = [][]releaseflow.ReleaseGateLatencyCohort{first, second}
			svc.releaseGateMetricsQuerier = q
			err = svc.completeSafeZeroDowntimeRollout(context.Background(), op, state)
			if enoughAtFinal && err != nil || !enoughAtFinal && (err == nil || !strings.Contains(err.Error(), "50%")) {
				t.Fatal("incorrect final gate result", err)
			}
			expectedCalls := 2
			if enoughAtFinal {
				expectedCalls++ // final pre-promotion comparison also remains mandatory
			}
			if q.calls != expectedCalls {
				t.Fatal("did not evaluate the bounded steps and final promotion", q.calls)
			}
			traffic, err := stateStore.GetAppTrafficPolicy(candidate.TenantID, true, candidate.ID)
			if err != nil {
				t.Fatal(err)
			}
			expected := state.StableRelease.ID
			if enoughAtFinal {
				expected = state.Candidate.ID
			}
			if traffic.StableReleaseID != expected || traffic.StableWeight != 100 || traffic.CandidateWeight != 0 {
				t.Fatal("incorrect serving release after final sample gate", traffic)
			}
			steps, err := stateStore.ListReleaseSteps(candidate.TenantID, true, "attempt_safe")
			if err != nil || !releaseStepsContainPhase(steps, "canary_gate", model.ReleaseStepStatusSkipped) {
				t.Fatal("missing inconclusive early step", err, steps)
			}
		})
	}
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
			if scenario == "matched_sample_deficit" && !safeRolloutCanaryGateOnlySampleDeficit(gate, policy) {
				t.Fatal("matched sample deficit cannot use an existing later canary step", gate)
			}
		})
	}
}
