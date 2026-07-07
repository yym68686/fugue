package releaseflow

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

func TestAppReleaseServicePromoteReplacesStable(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "state.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	app := releaseflowTestApp()
	service := AppReleaseService{
		Store: stateStore,
		ServiceURLForApp: func(context.Context, model.App) string {
			return "http://stable.internal:8080"
		},
	}
	principal := model.Principal{TenantID: app.TenantID, ActorType: model.ActorTypeSystem, ActorID: "test"}
	stable, err := service.EnsureStableRelease(context.Background(), app)
	if err != nil {
		t.Fatalf("ensure stable: %v", err)
	}
	if _, err := service.EnsureStableTrafficPolicy(context.Background(), principal, app); err != nil {
		t.Fatalf("ensure traffic policy: %v", err)
	}
	candidate, err := service.CreateRelease(context.Background(), app, CreateReleaseRequest{
		Role:        model.AppReleaseRoleCandidate,
		UpstreamURL: "http://candidate.internal:8080",
		Status:      model.AppReleaseStatusReady,
	})
	if err != nil {
		t.Fatalf("create candidate: %v", err)
	}

	policy, err := service.PromoteRelease(context.Background(), principal, app, candidate, 100)
	if err != nil {
		t.Fatalf("promote candidate: %v", err)
	}
	if policy.Mode != model.AppTrafficModeSingle || policy.StableReleaseID != candidate.ID || policy.CandidateReleaseID != "" || policy.StableWeight != 100 || policy.CandidateWeight != 0 {
		t.Fatalf("unexpected promoted policy: %+v", policy)
	}
	updatedCandidate, err := stateStore.GetAppRelease(app.TenantID, true, candidate.ID)
	if err != nil {
		t.Fatalf("get candidate: %v", err)
	}
	if updatedCandidate.Role != model.AppReleaseRoleStable || updatedCandidate.Status != model.AppReleaseStatusServing || updatedCandidate.PromotedAt == nil {
		t.Fatalf("candidate not promoted to serving stable: %+v", updatedCandidate)
	}
	updatedStable, err := stateStore.GetAppRelease(app.TenantID, true, stable.ID)
	if err != nil {
		t.Fatalf("get old stable: %v", err)
	}
	if updatedStable.Role != model.AppReleaseRolePrevious {
		t.Fatalf("expected old stable previous, got %+v", updatedStable)
	}
}

func TestAppReleaseServiceAbortRestoresStableTraffic(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "state.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	app := releaseflowTestApp()
	service := AppReleaseService{
		Store: stateStore,
		ServiceURLForApp: func(context.Context, model.App) string {
			return "http://stable.internal:8080"
		},
	}
	principal := model.Principal{TenantID: app.TenantID, ActorType: model.ActorTypeSystem, ActorID: "test"}
	if _, err := service.EnsureStableTrafficPolicy(context.Background(), principal, app); err != nil {
		t.Fatalf("ensure traffic policy: %v", err)
	}
	candidate, err := service.CreateRelease(context.Background(), app, CreateReleaseRequest{
		Role:        model.AppReleaseRoleCandidate,
		UpstreamURL: "http://candidate.internal:8080",
		Status:      model.AppReleaseStatusReady,
	})
	if err != nil {
		t.Fatalf("create candidate: %v", err)
	}
	if _, err := service.PromoteRelease(context.Background(), principal, app, candidate, 5); err != nil {
		t.Fatalf("start candidate canary: %v", err)
	}

	policy, err := service.AbortRelease(context.Background(), principal, app, candidate, true, "probe failed")
	if err != nil {
		t.Fatalf("abort candidate: %v", err)
	}
	if policy.Mode != model.AppTrafficModeSingle || policy.StableWeight != 100 || policy.CandidateWeight != 0 {
		t.Fatalf("expected stable-only traffic after abort, got %+v", policy)
	}
	updatedCandidate, err := stateStore.GetAppRelease(app.TenantID, true, candidate.ID)
	if err != nil {
		t.Fatalf("get candidate: %v", err)
	}
	if updatedCandidate.Status != model.AppReleaseStatusFailed || !strings.Contains(updatedCandidate.StatusReason, "probe failed") {
		t.Fatalf("expected failed candidate with reason, got %+v", updatedCandidate)
	}
}

func TestTrafficPlannerFailsClosedToStableWhenCandidateUnavailable(t *testing.T) {
	t.Parallel()

	binding := model.EdgeRouteBinding{
		AppID:       "app_demo",
		TenantID:    "tenant_demo",
		Hostname:    "demo.example.test",
		Status:      model.EdgeRouteStatusActive,
		UpstreamURL: "http://stable.internal:8080",
	}
	stable := model.AppRelease{ID: "rel_stable", AppID: binding.AppID, Role: model.AppReleaseRoleStable, UpstreamURL: "http://stable.internal:8080", Status: model.AppReleaseStatusServing}
	candidate := model.AppRelease{ID: "rel_candidate", AppID: binding.AppID, Role: model.AppReleaseRoleCandidate, Status: model.AppReleaseStatusCreating}
	policy := model.AppTrafficPolicy{AppID: binding.AppID, Mode: model.AppTrafficModeCanary, StableReleaseID: stable.ID, CandidateReleaseID: candidate.ID, StableWeight: 95, CandidateWeight: 5}

	planned := PlanAppReleaseTraffic(binding, map[string]model.AppTrafficPolicy{binding.AppID: policy}, AppReleaseByID([]model.AppRelease{stable, candidate}))
	if len(planned.Binding.Upstreams) != 2 {
		t.Fatalf("expected stable and candidate upstreams, got %+v", planned.Binding.Upstreams)
	}
	if planned.Binding.Upstreams[0].Weight != 100 || planned.Binding.Upstreams[0].Status != model.EdgeRouteStatusActive {
		t.Fatalf("expected stable fail-closed to 100 active, got %+v", planned.Binding.Upstreams[0])
	}
	if planned.Binding.Upstreams[1].Weight != 0 || planned.Binding.Upstreams[1].Status != model.EdgeRouteStatusUnavailable {
		t.Fatalf("expected candidate unavailable with zero weight, got %+v", planned.Binding.Upstreams[1])
	}
	if !strings.Contains(planned.StatusReason, "not ready") {
		t.Fatalf("expected not ready reason, got %q", planned.StatusReason)
	}
}

func TestReleaseGateEvaluatorFailsOnMetricsAndProbe(t *testing.T) {
	t.Parallel()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "broken", http.StatusServiceUnavailable)
	}))
	t.Cleanup(upstream.Close)

	evaluator := ReleaseGateEvaluator{MetricsQuerier: staticMetricsQuerier{metrics: map[string]any{
		"request_count":            10,
		"error_5xx_rate":           0.2,
		"edge_upstream_error_rate": 0.1,
		"p95_ttfb_ms":              2500,
		"p99_duration_ms":          35000,
	}}}
	app := releaseflowTestApp()
	release := model.AppRelease{ID: "rel_candidate", AppID: app.ID, Role: model.AppReleaseRoleCandidate, UpstreamURL: upstream.URL, Status: model.AppReleaseStatusReady}
	gate := evaluator.Evaluate(context.Background(), app, release, model.AppReleaseGatePolicy{
		WindowSeconds:              60,
		MinCandidateRequests:       20,
		Max5xxRate:                 0.01,
		MaxEdgeUpstreamErrorRate:   0.005,
		MaxP95TTFBMilliseconds:     2000,
		MaxP99DurationMilliseconds: 30000,
		Probes: []model.AppReleaseProbe{{
			Name:           "health",
			Method:         http.MethodGet,
			Path:           "/health",
			ExpectedStatus: http.StatusOK,
		}},
	})
	if gate.Status != model.AppReleaseGateStatusFail {
		t.Fatalf("expected gate fail, got %+v", gate)
	}
	for _, want := range []string{"below minimum", "5xx rate", "edge upstream error rate", "p95 ttfb", "p99 duration", "probe health failed"} {
		if !releaseflowStringsContain(gate.Failures, want) {
			t.Fatalf("expected gate failures to contain %q, got %+v", want, gate.Failures)
		}
	}
}

func TestWeightedSelectorDeterministicAndSkipsInactive(t *testing.T) {
	t.Parallel()

	candidates := []WeightedCandidate{
		{ID: "inactive", Weight: 90, Active: false},
		{ID: "stable", Weight: 90, Active: true},
		{ID: "candidate", Weight: 10, Active: true},
	}
	first, ok := SelectWeighted(candidates, "request-123")
	if !ok {
		t.Fatal("expected weighted selection")
	}
	second, ok := SelectWeighted(candidates, "request-123")
	if !ok {
		t.Fatal("expected second weighted selection")
	}
	if first != second {
		t.Fatalf("expected deterministic selection, got %+v and %+v", first, second)
	}
	if first.SelectedID == "inactive" || first.TotalWeight != 100 {
		t.Fatalf("unexpected weighted selection: %+v", first)
	}
}

func TestReleaseEvidenceViewBuilderIncludesReleaseSections(t *testing.T) {
	t.Parallel()

	attempt := model.ReleaseAttempt{ID: "attempt_123", TenantID: "tenant_demo", AppID: "app_demo"}
	release := model.AppRelease{ID: "rel_stable", TenantID: "tenant_demo", AppID: "app_demo", Role: model.AppReleaseRoleStable}
	policy := model.AppTrafficPolicy{ID: "policy_123", TenantID: "tenant_demo", AppID: "app_demo", StableReleaseID: release.ID, StableWeight: 100}
	gate := model.AppReleaseGateResult{Status: model.AppReleaseGateStatusPass, ReleaseID: release.ID}
	bundle := (ReleaseEvidenceViewBuilder{Now: func() time.Time { return time.Unix(1, 0).UTC() }}).ReleaseBundle(ReleaseEvidenceView{
		Metadata:        map[string]any{"release_attempt_id": attempt.ID},
		ReleaseAttempt:  &attempt,
		AppReleases:     []model.AppRelease{release},
		TrafficPolicies: []model.AppTrafficPolicy{policy},
		GateResults:     []model.AppReleaseGateResult{gate},
		ReleaseTimeline: []model.ReleaseTimelineEntry{{ID: "step_1", ReleaseAttemptID: attempt.ID, Type: model.ReleaseStepTypeHealthCheck}},
		Evidence:        []model.OperationEvidence{{ID: "evidence_1", TenantID: "tenant_demo", OperationID: "op_123"}},
	})
	if bundle.Metadata["redacted"] != true || len(bundle.RedactionReport) == 0 {
		t.Fatalf("expected redacted bundle metadata, got %+v report=%+v", bundle.Metadata, bundle.RedactionReport)
	}
	if len(bundle.AppReleases) != 1 || bundle.AppReleases[0].ID != release.ID {
		t.Fatalf("expected release records section, got %+v", bundle.AppReleases)
	}
	if len(bundle.TrafficPolicies) != 1 || bundle.TrafficPolicies[0].ID != policy.ID {
		t.Fatalf("expected traffic policy section, got %+v", bundle.TrafficPolicies)
	}
	if len(bundle.GateResults) != 1 || bundle.GateResults[0].ReleaseID != release.ID {
		t.Fatalf("expected gate result section, got %+v", bundle.GateResults)
	}
}

func TestRolloutReadinessResultError(t *testing.T) {
	t.Parallel()

	ready := RolloutReadinessResult{Ready: true, Message: "ok"}
	if err := ready.Error(); err != nil {
		t.Fatalf("expected ready result to have no error: %v", err)
	}
	failed := RolloutReadinessResult{Ready: false, Phase: "pod_failure", Message: "image pull failed"}
	if err := failed.Error(); err == nil || !strings.Contains(err.Error(), "image pull failed") {
		t.Fatalf("expected failed result error, got %v", err)
	}
}

type staticMetricsQuerier struct {
	metrics map[string]any
	err     error
}

func (q staticMetricsQuerier) QueryReleaseGateMetrics(context.Context, string, string, string, time.Duration) (map[string]any, error) {
	if q.err != nil {
		return nil, q.err
	}
	return q.metrics, nil
}

func releaseflowTestApp() model.App {
	return model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "demo",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/demo:v1",
			Ports:     []int{8080},
			Replicas:  1,
			RuntimeID: "runtime_demo",
		},
		Status: model.AppStatus{CurrentReplicas: 1},
	}
}

func releaseflowStringsContain(values []string, needle string) bool {
	for _, value := range values {
		if strings.Contains(value, needle) {
			return true
		}
	}
	return false
}

func ExampleWeightedBucket() {
	fmt.Println(WeightedBucket("request-123", 100) >= 0)
	// Output: true
}
