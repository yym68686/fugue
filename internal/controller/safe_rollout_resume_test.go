package controller

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
)

type resumeEdgeObserver struct {
	weights []int
	cancel  context.CancelFunc
}

func (o *resumeEdgeObserver) WaitForSafeRolloutEdgeRouteBundle(ctx context.Context, _ model.App, _ model.AppRelease, weight int, _ time.Time) (safeRolloutEdgeBundleObservation, error) {
	o.weights = append(o.weights, weight)
	if o.cancel != nil {
		o.cancel()
		return safeRolloutEdgeBundleObservation{}, ctx.Err()
	}
	return safeRolloutEdgeBundleObservation{Ready: true}, nil
}

func TestSafeRolloutResumeKeepsCandidateAndPublishedProgress(t *testing.T) {
	for _, phase := range []string{"created", "receipt_missing", "canary", "promotion_pending", "promoted", "canonical"} {
		t.Run(phase, func(t *testing.T) {
			st, previous, candidate, op := newSafeRolloutTestState(t)
			candidate.Spec.Continuity.ZeroDowntime.Canary = &model.AppRolloutCanarySpec{Enabled: true, InitialWeight: 10, MaxWeight: 100, StepWeights: []int{10, 50, 100}}
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
			defer upstream.Close()
			s := newSafeRolloutIntegrationService(st, upstream.URL, safeRolloutDrainMetrics{})
			state, err := s.prepareSafeZeroDowntimeRollout(context.Background(), op, previous, candidate)
			if err != nil {
				t.Fatal(err)
			}
			if phase == "receipt_missing" { // Simulate crash before the best-effort creation receipt.
				// A fresh operation has no receipt, but its deterministic release is durable.
				other := op
				other.ID += "-crash"
				r, err := s.createSafeRolloutCandidate(other, candidate, state.StableRelease)
				if err != nil {
					t.Fatal(err)
				}
				op = other
				state.Candidate = r
			}
			if phase == "canary" || phase == "promotion_pending" || phase == "promoted" || phase == "canonical" {
				r := state.Candidate
				r.Status = model.AppReleaseStatusReady
				r.UpstreamURL = upstream.URL
				r, err = st.UpdateAppRelease(r)
				if err != nil {
					t.Fatal(err)
				}
				state.Candidate = r
				principal := model.Principal{TenantID: candidate.TenantID, ActorType: model.ActorTypeSystem}
				if _, err = s.appReleaseService().PromoteRelease(context.Background(), principal, candidate, r, 50); err != nil {
					t.Fatal(err)
				}
				if phase == "promotion_pending" {
					r.Role = model.AppReleaseRoleStable
					r.Status = model.AppReleaseStatusServing
					r.PromotedAt = &r.CreatedAt
					_, err = st.UpdateAppRelease(r)
					if err != nil {
						t.Fatal(err)
					}
				}
				if phase == "promoted" || phase == "canonical" {
					if _, err = s.appReleaseService().PromoteRelease(context.Background(), principal, candidate, r, 100); err != nil {
						t.Fatal(err)
					}
				}
				if phase == "canonical" {
					r, _ = st.GetAppRelease(candidate.TenantID, true, r.ID)
					r = s.safeRolloutApplyCanonicalStableFields(context.Background(), candidate, r, "aligned")
					_, err = st.UpdateAppRelease(r)
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			policy, _ := st.GetAppTrafficPolicy(candidate.TenantID, true, candidate.ID)
			releases, _ := st.ListAppReleases(model.AppReleaseFilter{AppID: candidate.ID, PlatformAdmin: true})
			// New Service instance models a new controller process with no state.
			restarted := newSafeRolloutIntegrationService(st, upstream.URL, safeRolloutDrainMetrics{})
			restarted.releaseGateMetricsQuerier = releaseMetricsByIDQuerier{metrics: map[string]map[string]any{
				model.AppReleaseRoleStable: {"request_count": 100, "http_5xx_rate": 0.0}, model.AppReleaseRoleCandidate: {"request_count": 100, "http_5xx_rate": 0.0},
			}}
			observer := &resumeEdgeObserver{}
			restarted.safeRolloutEdgeBundleObserver = observer
			resumed, err := restarted.prepareSafeZeroDowntimeRollout(context.Background(), op, previous, candidate)
			if err != nil {
				t.Fatal(err)
			}
			if resumed == nil || !resumed.Resumed || resumed.Candidate.ID != state.Candidate.ID || resumed.StableRelease.ID != state.StableRelease.ID {
				t.Fatalf("identity lost: %+v", resumed)
			}
			after, _ := st.GetAppTrafficPolicy(candidate.TenantID, true, candidate.ID)
			afterReleases, _ := st.ListAppReleases(model.AppReleaseFilter{AppID: candidate.ID, PlatformAdmin: true})
			if !reflect.DeepEqual(after, policy) || !reflect.DeepEqual(afterReleases, releases) {
				t.Fatal("resume preparation mutated traffic or releases")
			}
			if phase == "created" || phase == "receipt_missing" {
				return
			}
			if err := restarted.completeSafeZeroDowntimeRollout(context.Background(), op, resumed); err != nil {
				t.Fatal(err)
			}
			for _, w := range observer.weights {
				if w > 0 && w < 50 {
					t.Fatal("canary weight reset", observer.weights)
				}
			}
			if (phase == "promoted" || phase == "canonical" || phase == "promotion_pending") && !reflect.DeepEqual(observer.weights, []int{0}) {
				t.Fatal("promoted release reentered canary", observer.weights)
			}
			final, _ := st.GetAppTrafficPolicy(candidate.TenantID, true, candidate.ID)
			if final.StableReleaseID != state.Candidate.ID || final.StableWeight != 100 || !resumed.StableAlignmentAllowed {
				t.Fatal("resume did not reach alignment", final)
			}
			if phase == "canonical" {
				r, _ := st.GetAppRelease(candidate.TenantID, true, state.Candidate.ID)
				if r.DeploymentName != resumed.Candidate.DeploymentName {
					t.Fatal("canonical target rewritten")
				}
			}
		})
	}
}

func TestSafeRolloutResumeRejectsChangedIntentWithoutMutation(t *testing.T) {
	for _, change := range []string{"env", "restart", "image", "owner", "failed", "policy", "duplicate"} {
		t.Run(change, func(t *testing.T) {
			st, previous, candidate, op := newSafeRolloutTestState(t)
			s := &Service{Store: st}
			state, err := s.prepareSafeZeroDowntimeRollout(context.Background(), op, previous, candidate)
			if err != nil {
				t.Fatal(err)
			}
			switch change {
			case "env":
				candidate.Spec.Env = map[string]string{"KEY": "changed"}
			case "restart":
				candidate.Spec.RestartToken = "new"
			case "image":
				candidate.Spec.Image = "registry.example/other:v3"
			case "owner":
				r := state.Candidate
				r.TenantID = "other"
				_, err = st.UpdateAppRelease(r)
			case "failed":
				r := state.Candidate
				r.Status = model.AppReleaseStatusFailed
				_, err = st.UpdateAppRelease(r)
			case "policy":
				p, _ := st.GetAppTrafficPolicy(candidate.TenantID, true, candidate.ID)
				p.Mode = model.AppTrafficModePaused
				_, err = st.UpsertAppTrafficPolicy(p)
			case "duplicate":
				_, err = st.RecordReleaseStep(model.ReleaseStep{TenantID: candidate.TenantID, ReleaseAttemptID: "attempt_safe", OperationID: op.ID, Type: model.ReleaseStepTypeHealthCheck, Status: model.ReleaseStepStatusCompleted, Payload: map[string]any{"phase": "candidate_create", "candidate_release_id": "other", "stable_release_id": state.StableRelease.ID}})
			}
			if err != nil {
				t.Fatal(err)
			}
			before, _ := st.ListAppReleases(model.AppReleaseFilter{PlatformAdmin: true, IncludeRetired: true})
			policy, _ := st.GetAppTrafficPolicy(candidate.TenantID, true, candidate.ID)
			if _, err := s.prepareSafeZeroDowntimeRollout(context.Background(), op, previous, candidate); err == nil {
				t.Fatal("changed recovery accepted")
			}
			after, _ := st.ListAppReleases(model.AppReleaseFilter{PlatformAdmin: true, IncludeRetired: true})
			next, _ := st.GetAppTrafficPolicy(candidate.TenantID, true, candidate.ID)
			if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(policy, next) {
				t.Fatal("rejected recovery changed state")
			}
		})
	}
}

func TestSafeRolloutControllerCancellationPreservesCanary(t *testing.T) {
	st, previous, candidate, op := newSafeRolloutTestState(t)
	candidate.Spec.Continuity.ZeroDowntime.Canary = &model.AppRolloutCanarySpec{Enabled: true, InitialWeight: 50, MaxWeight: 100, StepWeights: []int{50, 100}}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer upstream.Close()
	s := newSafeRolloutIntegrationService(st, upstream.URL, safeRolloutDrainMetrics{})
	state, err := s.prepareSafeZeroDowntimeRollout(context.Background(), op, previous, candidate)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.safeRolloutEdgeBundleObserver = &resumeEdgeObserver{cancel: cancel}
	if err := s.completeSafeZeroDowntimeRollout(ctx, op, state); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation lost", err)
	}
	p, _ := st.GetAppTrafficPolicy(candidate.TenantID, true, candidate.ID)
	r, _ := st.GetAppRelease(candidate.TenantID, true, state.Candidate.ID)
	if p.CandidateReleaseID != r.ID || p.CandidateWeight != 50 || r.Status == model.AppReleaseStatusFailed {
		t.Fatal("shutdown aborted canary", p, r)
	}
	if err := s.abortSafeZeroDowntimeRollout(ctx, op, state, "stopping"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	after, _ := st.GetAppTrafficPolicy(candidate.TenantID, true, candidate.ID)
	if !reflect.DeepEqual(p, after) {
		t.Fatal("cancelled abort mutated traffic")
	}
}

func TestSafeRolloutOperationReleaseIDIsLabelSafeAndScoped(t *testing.T) {
	op := model.Operation{ID: "operation", TenantID: "tenant", AppID: "app"}
	id := safeRolloutOperationReleaseID(op)
	if len(id) > 63 || id != safeRolloutOperationReleaseID(op) {
		t.Fatal("invalid deterministic release id", id)
	}
	for _, changed := range []model.Operation{{ID: "other", TenantID: op.TenantID, AppID: op.AppID}, {ID: op.ID, TenantID: "other", AppID: op.AppID}, {ID: op.ID, TenantID: op.TenantID, AppID: "other"}} {
		if safeRolloutOperationReleaseID(changed) == id {
			t.Fatal("release identity is not scoped")
		}
	}
}

func TestSafeRolloutRequeuedOperationKeepsOriginalCandidate(t *testing.T) {
	st, previous, candidate, op := newSafeRolloutTestState(t)
	op, claimed, err := st.TryClaimPendingOperation(op.ID)
	if err != nil || !claimed {
		t.Fatal("claim", err)
	}
	s := &Service{Store: st}
	first, err := s.prepareSafeZeroDowntimeRollout(context.Background(), op, previous, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = st.RequeueManagedOperation(op.ID, "controller stopped"); err != nil {
		t.Fatal(err)
	}
	op, claimed, err = st.TryClaimPendingOperation(op.ID)
	if err != nil || !claimed {
		t.Fatal("reclaim", err)
	}
	restarted := &Service{Store: st}
	second, err := restarted.prepareSafeZeroDowntimeRollout(context.Background(), op, previous, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if first.Candidate.ID != second.Candidate.ID || !second.Resumed {
		t.Fatal("requeued operation created another revision")
	}
	releases, err := st.ListAppReleases(model.AppReleaseFilter{AppID: candidate.ID, PlatformAdmin: true, IncludeRetired: true})
	if err != nil || len(releases) != 2 {
		t.Fatalf("duplicate release: count=%d err=%v", len(releases), err)
	}
}
