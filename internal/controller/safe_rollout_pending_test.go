package controller

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"fugue/internal/model"
)

type deadlineEdgeObserver struct{}

func (deadlineEdgeObserver) WaitForSafeRolloutEdgeRouteBundle(context.Context, model.App, model.AppRelease, int, time.Time) (safeRolloutEdgeBundleObservation, error) {
	return safeRolloutEdgeBundleObservation{Ready: false, Summary: map[string]any{"waiting_nodes": []string{"edge-a:app_traffic_proof_mismatch"}}}, context.DeadlineExceeded
}

func TestSafeRolloutChildDeadlineRemainsPendingAndResumesSameRelease(t *testing.T) {
	st, previous, candidate, op := newSafeRolloutTestState(t)
	op, claimed, err := st.TryClaimPendingOperation(op.ID)
	if err != nil || !claimed {
		t.Fatal("claim", err)
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer upstream.Close()
	s := newSafeRolloutIntegrationService(st, upstream.URL, safeRolloutDrainMetrics{})
	s.safeRolloutEdgeBundleObserver = deadlineEdgeObserver{}
	state, err := s.prepareSafeZeroDowntimeRollout(context.Background(), op, previous, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.completeSafeZeroDowntimeRollout(context.Background(), op, state); !errors.Is(err, errSafeRolloutTrafficPending) || state.ServingVerified || state.StableAlignmentAllowed {
		t.Fatal("child timeout claimed success", err, state)
	}
	if err = s.deferSafeRolloutTrafficConfirmation(context.Background(), op); err != nil {
		t.Fatal(err)
	}
	pending, _ := st.GetOperation(op.ID)
	if !model.OperationWaitingForTrafficConfirmation(pending) || pending.CompletedAt != nil || pendingOperationRetryReady(pending, pending.UpdatedAt) || !pendingOperationRetryReady(pending, pending.UpdatedAt.Add(30*time.Second)) {
		t.Fatal("pending state/backoff lost", pending)
	}
	attempt, _, _ := st.FindReleaseAttemptForOperation(op.ID)
	if attempt.Status != model.ReleaseAttemptStatusRollingOut || attempt.FinishedAt != nil || attempt.Confidence == model.OperationEvidenceConfidenceConfirmed {
		t.Fatal("pending attempt became terminal", attempt)
	}
	steps, _ := st.ListReleaseSteps(candidate.TenantID, true, attempt.ID)
	for _, step := range steps {
		if step.Type == model.ReleaseStepTypeMarkDeployedDigest || step.Type == model.ReleaseStepTypeFinalize {
			t.Fatal("unconfirmed release marked deployed", step)
		}
	}
	policy, _ := st.GetAppTrafficPolicy(candidate.TenantID, true, candidate.ID)
	if policy.StableReleaseID != state.Candidate.ID || policy.StableWeight != 100 {
		t.Fatal("timeout reset promoted traffic", policy)
	}
	op, claimed, err = st.TryClaimPendingOperation(op.ID)
	if err != nil || !claimed {
		t.Fatal("reclaim", err)
	}
	restarted := newSafeRolloutIntegrationService(st, upstream.URL, safeRolloutDrainMetrics{})
	resumed, err := restarted.prepareSafeZeroDowntimeRollout(context.Background(), op, previous, candidate)
	if err != nil || resumed == nil || !resumed.AlreadyPromoted || resumed.Candidate.ID != state.Candidate.ID {
		t.Fatal("retry did not preserve identity and phase", resumed, err)
	}
	if err = restarted.completeSafeZeroDowntimeRollout(context.Background(), op, resumed); err != nil || !resumed.ServingVerified || !resumed.StableAlignmentAllowed {
		t.Fatal("positive proof did not recover", err)
	}
	releases, _ := st.ListAppReleases(model.AppReleaseFilter{AppID: candidate.ID, PlatformAdmin: true, IncludeRetired: true})
	if len(releases) != 2 {
		t.Fatal("retry created a different candidate", len(releases))
	}
}

func TestSafeRolloutPendingDoesNotOverrideCancellation(t *testing.T) {
	st, _, _, op := newSafeRolloutTestState(t)
	op, _, _ = st.TryClaimPendingOperation(op.ID)
	if _, err := st.CancelOperation(op.ID, "operator canceled"); err != nil {
		t.Fatal(err)
	}
	s := &Service{Store: st}
	if err := s.deferSafeRolloutTrafficConfirmation(context.Background(), op); err != nil {
		t.Fatal(err)
	}
	after, _ := st.GetOperation(op.ID)
	if after.Status != model.OperationStatusCanceled {
		t.Fatal("concurrent cancellation overwritten", after)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(safeRolloutTrafficPending(ctx), context.Canceled) {
		t.Fatal("parent cancellation swallowed")
	}
}

func TestSafeRolloutCanaryChildDeadlineResumesBeforeHealthGate(t *testing.T) {
	for _, unhealthy := range []bool{false, true} {
		name := "healthy"
		if unhealthy {
			name = "unhealthy"
		}
		t.Run(name, func(t *testing.T) {
			st, previous, candidate, op := newSafeRolloutTestState(t)
			candidate.Spec.Continuity.ZeroDowntime.Canary = &model.AppRolloutCanarySpec{Enabled: true, InitialWeight: 50, MaxWeight: 100, StepWeights: []int{50, 100}}
			candidate.Spec.Continuity.ZeroDowntime.Gate = &model.AppReleaseGatePolicy{WindowSeconds: 60, MinCandidateRequests: 10, Max5xxRate: 0.1}
			op, claimed, err := st.TryClaimPendingOperation(op.ID)
			if err != nil || !claimed {
				t.Fatal("claim", err)
			}
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
			defer upstream.Close()
			newService := func() *Service {
				s := newSafeRolloutIntegrationService(st, upstream.URL, safeRolloutDrainMetrics{})
				s.safeRolloutSleep = func(context.Context, time.Duration) error { return nil }
				s.releaseGateMetricsQuerier = releaseMetricsByIDQuerier{metrics: map[string]map[string]any{
					model.AppReleaseRoleCandidate: {"request_count": 20, "error_5xx_rate": 0.0, "edge_upstream_error_rate": 0.0},
					model.AppReleaseRoleStable:    {"request_count": 200, "error_5xx_rate": 0.0, "edge_upstream_error_rate": 0.0},
				}}
				return s
			}
			s := newService()
			s.safeRolloutEdgeBundleObserver = deadlineEdgeObserver{}
			state, err := s.prepareSafeZeroDowntimeRollout(context.Background(), op, previous, candidate)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.completeSafeZeroDowntimeRollout(context.Background(), op, state); !errors.Is(err, errSafeRolloutTrafficPending) {
				t.Fatal("canary timeout must remain pending", err)
			}
			if err := s.deferSafeRolloutTrafficConfirmation(context.Background(), op); err != nil {
				t.Fatal(err)
			}
			op, claimed, err = st.TryClaimPendingOperation(op.ID)
			if err != nil || !claimed {
				t.Fatal("reclaim", err)
			}
			restarted := newService()
			resumed, err := restarted.prepareSafeZeroDowntimeRollout(context.Background(), op, previous, candidate)
			if err != nil || resumed == nil || resumed.ResumeWeight != 50 || resumed.Candidate.ID != state.Candidate.ID {
				t.Fatal("same canary phase not resumed", resumed, err)
			}
			if unhealthy {
				// The active preflight remains healthy. Only the post-confirmation
				// canary observation fails, and must still roll traffic back.
				restarted.releaseGateMetricsQuerier = &sequencedReleaseGateMetricsQuerier{metrics: []map[string]any{
					{"request_count": 20, "error_5xx_rate": 0.0},
					{"request_count": 20, "error_5xx_rate": 0.5},
					{"request_count": 200, "error_5xx_rate": 0.0},
				}}
			}
			err = restarted.completeSafeZeroDowntimeRollout(context.Background(), op, resumed)
			policy, _ := st.GetAppTrafficPolicy(candidate.TenantID, true, candidate.ID)
			if unhealthy {
				if err == nil || errors.Is(err, errSafeRolloutTrafficPending) || resumed.ServingVerified || policy.StableReleaseID != state.StableRelease.ID || policy.CandidateWeight != 0 {
					t.Fatal("real canary failure did not roll back", err, policy)
				}
				steps, _ := st.ListReleaseSteps(candidate.TenantID, true, "attempt_safe")
				if !releaseStepsContainPhase(steps, "canary_gate", model.ReleaseStepStatusFailed) {
					t.Fatal("failure was not the post-confirmation canary gate", steps)
				}
			} else if err != nil || !resumed.ServingVerified || policy.StableReleaseID != state.Candidate.ID || policy.StableWeight != 100 {
				t.Fatal("confirmed canary did not progress", err, policy)
			}
			releases, _ := st.ListAppReleases(model.AppReleaseFilter{AppID: candidate.ID, PlatformAdmin: true, IncludeRetired: true})
			if len(releases) != 2 {
				t.Fatal("canary retry created a new release", len(releases))
			}
		})
	}
}
