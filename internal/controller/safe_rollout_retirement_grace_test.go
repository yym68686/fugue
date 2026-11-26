package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestResumedCanonicalHandoffRetiresOnlyExpiredIndependentRevision(t *testing.T) {
	for _, scenario := range []string{"unrelated", "source", "rollback", "previous_target", "invalid_marker", "retention", "unconfirmed_traffic"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			s, app, old, fixture := newDrainWorkloadFixture(t)
			s.safeRolloutDrainMetricsQuerier = kubeSafeRolloutDrainObserver{service: s}
			expired := time.Now().Add(-time.Minute)
			old.RetentionUntil = &expired
			if scenario == "retention" {
				future := time.Now().Add(time.Minute)
				old.RetentionUntil = &future
			}
			old, err := s.Store.UpdateAppRelease(old)
			if err != nil {
				t.Fatal(err)
			}
			policy, err := s.Store.GetAppTrafficPolicy(app.TenantID, true, app.ID)
			if err != nil {
				t.Fatal(err)
			}
			current, err := s.Store.GetAppRelease(app.TenantID, true, policy.StableReleaseID)
			if err != nil {
				t.Fatal(err)
			}
			op, err := s.Store.CreateOperation(model.Operation{AppID: app.ID, TenantID: app.TenantID, Type: model.OperationTypeDeploy,
				DesiredSpec: &app.Spec, ExecutionMode: model.ExecutionModeManaged})
			if err != nil {
				t.Fatal(err)
			}
			op, claimed, err := s.Store.TryClaimPendingOperation(op.ID)
			if err != nil || !claimed {
				t.Fatal("claim", err)
			}
			current.RevisionWorkload = &model.AppReleaseWorkload{OperationID: op.ID}
			rollback := model.AppRelease{ID: "rollback", CreatedAt: old.CreatedAt.Add(time.Minute)}
			marker := safeCanonicalHandoff{ReleaseID: current.ID, SourceReleaseID: "source",
				DeploymentUID: "canonical-uid", ReleaseKey: "source-key", Image: "example/source:v1"}
			switch scenario {
			case "source":
				marker.SourceReleaseID = old.ID
			case "rollback":
				rollback.ID = old.ID
			case "previous_target":
				marker.PreviousTargetReleaseID = old.ID
			case "invalid_marker":
				marker.DeploymentUID = "other-uid"
			case "unconfirmed_traffic":
				s.safeRolloutEdgeBundleObserver = staticSafeRolloutEdgeObserver{}
			}
			payload, err := json.Marshal(marker)
			if err != nil {
				t.Fatal(err)
			}
			fixture.extra = func(w http.ResponseWriter, r *http.Request) bool {
				if r.Method != http.MethodGet || !strings.HasSuffix(r.URL.Path, "/deployments/"+runtime.RuntimeAppResourceName(app)) {
					return false
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{
					"uid": "canonical-uid",
					"labels": map[string]string{runtime.FugueLabelAppID: app.ID, runtime.FugueLabelTenantID: app.TenantID,
						runtime.FugueLabelManagedBy: runtime.FugueLabelManagedByValue},
					"annotations": map[string]string{safeCanonicalHandoffAnnotation: string(payload)},
				}})
				return true
			}
			ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
			defer cancel()
			s.retireHistoricalRevisionsDuringHandoff(ctx, op, &safeRolloutState{
				Enabled: true, Resumed: true, AlreadyPromoted: true, ServingVerified: true,
				CandidateApp: app, Candidate: current, StableRelease: rollback,
			})
			got, err := s.Store.GetAppRelease(app.TenantID, true, old.ID)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "unrelated" {
				if got.Status != model.AppReleaseStatusRetired || fixture.deleteCalls != 2 || fixture.proxyCalls < 4 {
					t.Fatalf("historical retirement stalled: status=%s deletes=%d observations=%d", got.Status, fixture.deleteCalls, fixture.proxyCalls)
				}
			} else if got.Status != model.AppReleaseStatusDraining || fixture.deleteCalls != 0 {
				t.Fatalf("retirement ignored %s: status=%s deletes=%d", scenario, got.Status, fixture.deleteCalls)
			}
			after, err := s.Store.GetAppTrafficPolicy(app.TenantID, true, app.ID)
			if err != nil || after != policy {
				t.Fatal("retirement changed serving intent", err)
			}
		})
	}
}

func TestStableTrafficRetriesDoNotStarveRetirement(t *testing.T) {
	s, app, previous, fixture := newDrainWorkloadFixture(t)
	app.Spec.Continuity.ZeroDowntime.RetireGraceSeconds = 1
	s.safeRolloutDrainMetricsQuerier = kubeSafeRolloutDrainObserver{service: s}
	policy, err := s.Store.GetAppTrafficPolicy(app.TenantID, true, app.ID)
	if err != nil {
		t.Fatal(err)
	}
	principal := model.Principal{TenantID: app.TenantID, ActorType: model.ActorTypeSystem, ActorID: "safe-rollout-controller"}
	service := s.appReleaseService()
	// Pre-traffic failures keep reaffirming stable 100% faster than grace can
	// elapse. Use the real abort/restore and Store paths, then the complete
	// Kubernetes/edge/drain/UID retirement path, not a synthetic clock predicate.
	for deadline := policy.UpdatedAt.Add(1100 * time.Millisecond); time.Now().Before(deadline); {
		if _, err := service.AbortRelease(context.Background(), principal, app, model.AppRelease{}, false, "candidate failed before traffic"); err != nil {
			t.Fatal(err)
		}
		if _, err := service.RestoreStableTraffic(context.Background(), principal, app); err != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.retryDrainingAppReleaseRetirement(ctx, app); err != nil {
		t.Fatal(err)
	}
	retired, err := s.Store.GetAppRelease(app.TenantID, true, previous.ID)
	if err != nil || retired.Status != model.AppReleaseStatusRetired || fixture.deleteCalls != 2 || fixture.proxyCalls < 4 {
		t.Fatalf("no-op retries starved verified retirement: status=%s deletes=%d observations=%d err=%v", retired.Status, fixture.deleteCalls, fixture.proxyCalls, err)
	}
	current, err := s.Store.GetAppTrafficPolicy(app.TenantID, true, app.ID)
	if err != nil || current != policy {
		t.Fatal("no-op retries or retirement changed serving intent/version", err)
	}
}

func TestCanonicalStableBaselineRetriesPreserveTrafficVersion(t *testing.T) {
	st, app, _, op := newSafeRolloutTestState(t)
	s := &Service{Store: st}
	principal := model.Principal{TenantID: app.TenantID, ActorType: model.ActorTypeSystem, ActorID: "safe-rollout-controller"}
	if _, err := s.ensureSafeRolloutCanonicalStableBaseline(context.Background(), op, app, principal); err != nil {
		t.Fatal(err)
	}
	policy, err := st.GetAppTrafficPolicy(app.TenantID, true, app.ID)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := s.ensureSafeRolloutCanonicalStableBaseline(context.Background(), op, app, principal); err != nil {
			t.Fatal(err)
		}
		current, err := st.GetAppTrafficPolicy(app.TenantID, true, app.ID)
		if err != nil || current != policy {
			t.Fatal("baseline retry restarted retirement grace", err)
		}
	}
}
