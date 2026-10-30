package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/releaseflow"
	"fugue/internal/runtime"
)

type unhealthyRetirementMetrics struct{ calls int }

func (q *unhealthyRetirementMetrics) QueryReleaseGateMetrics(context.Context, string, string, string, time.Duration) (map[string]any, error) {
	q.calls++
	return map[string]any{"request_count": 100.0, "error_5xx_rate": 0.5}, nil
}

var _ releaseflow.ReleaseGateMetricsQuerier = (*unhealthyRetirementMetrics)(nil)

func TestIndependentCandidateAbortDoesNotRewriteRetainedCanonicalWorkload(t *testing.T) {
	st, previous, candidate, op := newSafeRolloutTestState(t)
	s := &Service{Store: st}
	s.Config.KubectlApply = true
	s.newKubeClient = func(string) (*kubeClient, error) {
		t.Fatal("independent candidate abort attempted to rewrite canonical workload")
		return nil, nil
	}
	state, err := s.prepareSafeZeroDowntimeRollout(context.Background(), op, previous, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := st.TryClaimPendingOperation(op.ID); err != nil || !ok {
		t.Fatal("claim", err)
	}
	state.Candidate.DeploymentName, state.Candidate.ServiceName = "independent", "independent"
	state.Candidate, err = st.UpdateAppRelease(state.Candidate)
	if err != nil {
		t.Fatal(err)
	}
	state.Candidate, err = st.BindAppReleaseWorkload(context.Background(), state.Candidate, model.AppReleaseWorkload{
		OperationID: op.ID, Namespace: runtime.NamespaceForTenant(candidate.TenantID), DeploymentName: "independent", DeploymentUID: "deployment-uid", DeploymentGeneration: 1,
		ServiceName: "independent", ServiceUID: "service-uid", ReleaseKey: "release-key", RuntimeID: candidate.Spec.RuntimeID, ImageRef: candidate.Spec.Image,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.abortSafeZeroDowntimeRollout(context.Background(), op, state, "candidate could not be scheduled"); err != nil {
		t.Fatal(err)
	}
	failed, err := st.GetAppRelease(candidate.TenantID, true, state.Candidate.ID)
	if err != nil || failed.Status != model.AppReleaseStatusFailed {
		t.Fatal("abort did not persist failed release", err)
	}
	policy, err := st.GetAppTrafficPolicy(candidate.TenantID, true, candidate.ID)
	if err != nil || policy.StableReleaseID != state.StableRelease.ID || policy.CandidateReleaseID != "" {
		t.Fatal("abort lost stable traffic", err)
	}
}

func TestWithdrawnRetirementDoesNotDependOnServingBusinessHealth(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "previous", true: "failed"}[failed], func(t *testing.T) {
			s, app, old, f := newDrainWorkloadFixtureWithOutcome(t, failed)
			health := &unhealthyRetirementMetrics{}
			s.releaseGateMetricsQuerier = health
			s.safeRolloutDrainMetricsQuerier = kubeSafeRolloutDrainObserver{service: s}
			if err := s.retryDrainingAppReleaseRetirement(context.Background(), app); err != nil {
				t.Fatal(err)
			}
			got, err := s.Store.GetAppRelease(app.TenantID, true, old.ID)
			if err != nil || got.Status != model.AppReleaseStatusRetired || f.deleteCalls != 2 || f.proxyCalls < 4 || health.calls != 0 {
				t.Fatalf("withdrawn release retained by business health: status=%s deletes=%d drain=%d gate=%d err=%v", got.Status, f.deleteCalls, f.proxyCalls, health.calls, err)
			}
		})
	}
}

func TestFailedUnscheduledCandidateCancelsThenRetiresAfterZeroProof(t *testing.T) {
	for _, scenario := range []string{"idle", "scheduled_race", "patch_conflict", "pod_remains", "replicaset_remains", "policy_changed", "retention", "unconfirmed_traffic", "wrong_outcome"} {
		t.Run(scenario, func(t *testing.T) {
			s, app, old, f := newDrainWorkloadFixtureWithOutcome(t, scenario != "wrong_outcome")
			if scenario == "wrong_outcome" {
				old.Role, old.Status = model.AppReleaseRoleCandidate, model.AppReleaseStatusFailed
				old, _ = s.Store.UpdateAppRelease(old)
			}
			s.safeRolloutDrainMetricsQuerier = kubeSafeRolloutDrainObserver{service: s}
			for _, pod := range f.pods {
				pod["status"] = map[string]any{"phase": "Pending", "conditions": []map[string]any{{"type": "PodScheduled", "status": "False", "reason": "Unschedulable"}}}
			}
			f.deployment["status"] = map[string]any{"observedGeneration": 1, "replicas": 2}
			if scenario == "retention" {
				future := time.Now().Add(time.Hour)
				old.RetentionUntil = &future
				old, _ = s.Store.UpdateAppRelease(old)
			}
			if scenario == "unconfirmed_traffic" {
				s.safeRolloutEdgeBundleObserver = staticSafeRolloutEdgeObserver{}
			}
			patches, captures := 0, 0
			f.extra = func(w http.ResponseWriter, r *http.Request) bool {
				if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/deployments/"+old.DeploymentName) {
					captures++
					if scenario == "scheduled_race" && captures == 4 {
						objectMapField(f.pods[0], "spec")["nodeName"] = "worker-a"
					}
					if scenario == "policy_changed" && captures == 4 {
						policy, _ := s.Store.GetAppTrafficPolicy(app.TenantID, true, app.ID)
						policy.Mode, policy.CandidateReleaseID, policy.StableWeight, policy.CandidateWeight = model.AppTrafficModeCanary, old.ID, 99, 1
						_, _ = s.Store.UpsertAppTrafficPolicy(policy)
					}
				}
				if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/replicasets/revision") {
					_ = json.NewEncoder(w).Encode(f.replicas[0])
					return true
				}
				if r.Method != http.MethodPatch || !strings.HasSuffix(r.URL.Path, "/deployments/"+old.DeploymentName) {
					return false
				}
				patches++
				var patch map[string]any
				if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
					t.Fatal(err)
				}
				if objectStringField(objectMapField(patch, "metadata"), "uid") != "deployment-uid" || objectStringField(objectMapField(patch, "metadata"), "resourceVersion") != "1" || nestedObjectValue(patch, "spec", "replicas") != float64(0) {
					t.Error("cancellation lacks identity/version/zero replicas", patch)
				}
				if scenario == "patch_conflict" {
					http.Error(w, "conflict", http.StatusConflict)
					return true
				}
				objectMapField(f.deployment, "spec")["replicas"] = 0
				objectMapField(f.deployment, "metadata")["resourceVersion"] = "2"
				objectMapField(f.deployment, "metadata")["generation"] = 2
				f.deployment["status"] = map[string]any{"observedGeneration": 2, "replicas": 0}
				f.replicas[0]["spec"] = map[string]any{"replicas": 0}
				f.replicas[0]["status"] = map[string]any{"observedGeneration": 1, "replicas": 0}
				if scenario == "replicaset_remains" {
					f.replicas[0]["spec"] = map[string]any{"replicas": 1}
				}
				if scenario != "pod_remains" {
					f.pods = nil
				}
				_, _ = w.Write([]byte(`{}`))
				return true
			}
			for i := 0; i < 2; i++ {
				if err := s.retryDrainingAppReleaseRetirement(context.Background(), app); err != nil {
					t.Fatal(err)
				}
			}
			got, err := s.Store.GetAppRelease(app.TenantID, true, old.ID)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "idle" {
				if patches != 1 || got.Status != model.AppReleaseStatusRetired || f.deleteCalls != 2 || f.proxyCalls != 0 {
					t.Fatalf("never-started candidate not retired: patches=%d status=%s deletes=%d proxies=%d", patches, got.Status, f.deleteCalls, f.proxyCalls)
				}
			} else if got.Status == model.AppReleaseStatusRetired || f.deleteCalls != 0 {
				t.Fatalf("retired without zero proof: %s deletes=%d", got.Status, f.deleteCalls)
			}
		})
	}
}
