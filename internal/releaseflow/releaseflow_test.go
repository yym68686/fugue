package releaseflow

import (
	"context"
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
	app.Spec.Continuity = &model.AppContinuityPolicy{ZeroDowntime: &model.AppZeroDowntimePolicy{
		Enabled:               true,
		Mode:                  model.AppZeroDowntimeModeSafe,
		Strategy:              model.AppZeroDowntimeStrategyStableCandidate,
		RollbackWindowSeconds: 300,
	}}
	now := time.Unix(1000, 0).UTC()
	service := AppReleaseService{
		Store: stateStore,
		Now:   func() time.Time { return now },
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
	if updatedStable.Role != model.AppReleaseRolePrevious || updatedStable.Status != model.AppReleaseStatusDraining {
		t.Fatalf("expected old stable previous, got %+v", updatedStable)
	}
	if updatedStable.RetentionUntil == nil || !updatedStable.RetentionUntil.Equal(now.Add(300*time.Second)) {
		t.Fatalf("expected old stable retention window, got %+v", updatedStable.RetentionUntil)
	}
	if updatedCandidate.RollbackTargetID != stable.ID || updatedCandidate.ReleaseMessage == "" {
		t.Fatalf("expected promoted candidate to remember rollback target, got %+v", updatedCandidate)
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
	if policy.Mode != model.AppTrafficModeSingle || policy.CandidateReleaseID != "" || policy.StableWeight != 100 || policy.CandidateWeight != 0 {
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

func TestReleaseGateComparisonFailsWhenCandidateStatusRatioRegresses(t *testing.T) {
	t.Parallel()

	comparison := BuildReleaseGateComparisonMetrics(
		map[string]any{
			"request_count":            20,
			"error_5xx_rate":           0.02,
			"edge_upstream_error_rate": 0.0,
			"status_2xx_rate":          0.94,
			"has_status_class_counts":  true,
			"p95_ttfb_ms":              120.0,
			"p99_duration_ms":          200.0,
		},
		map[string]any{
			"request_count":            200,
			"error_5xx_rate":           0.0,
			"edge_upstream_error_rate": 0.0,
			"status_2xx_rate":          1.0,
			"has_status_class_counts":  true,
			"p95_ttfb_ms":              100.0,
			"p99_duration_ms":          150.0,
		},
	)
	failures := ReleaseGateComparisonFailures(comparison, model.AppReleaseGatePolicy{Max5xxRate: 0.10})
	for _, want := range []string{"candidate 5xx rate", "candidate 2xx rate"} {
		if !releaseflowStringsContain(failures, want) {
			t.Fatalf("expected comparison failure containing %q, got %+v", want, failures)
		}
	}
	if evidence := ReleaseGateComparisonEvidence(comparison); len(evidence) != 1 || !strings.Contains(evidence[0], "stable_requests=200") {
		t.Fatalf("expected comparison evidence, got %+v", evidence)
	}
}

func TestReleaseGateComparisonPassesWhenCandidateMatchesStable(t *testing.T) {
	t.Parallel()

	comparison := BuildReleaseGateComparisonMetrics(
		map[string]any{
			"request_count":            20,
			"error_5xx_rate":           0.001,
			"edge_upstream_error_rate": 0.0,
			"status_2xx_rate":          0.999,
			"has_status_class_counts":  true,
			"p95_ttfb_ms":              120.0,
			"p99_duration_ms":          200.0,
		},
		map[string]any{
			"request_count":            200,
			"error_5xx_rate":           0.0,
			"edge_upstream_error_rate": 0.0,
			"status_2xx_rate":          1.0,
			"has_status_class_counts":  true,
			"p95_ttfb_ms":              100.0,
			"p99_duration_ms":          150.0,
		},
	)
	if failures := ReleaseGateComparisonFailures(comparison, model.AppReleaseGatePolicy{Max5xxRate: 0.10}); len(failures) != 0 {
		t.Fatalf("expected comparison gate pass, got %+v", failures)
	}
}

func TestClickHouseReleaseGateMetricsQuerierUsesRollupReleaseID(t *testing.T) {
	t.Parallel()

	var query string
	clickHouse := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query = r.URL.Query().Get("query")
		_, _ = w.Write([]byte(`{"request_count":4,"error_5xx_count":1,"edge_upstream_error_count":0,"p95_ttfb_ms":120,"p99_duration_ms":350}` + "\n"))
	}))
	t.Cleanup(clickHouse.Close)

	querier := ClickHouseReleaseGateMetricsQuerier{
		DSN: clickHouse.URL + "?database=fugue_observability",
		Now: func() time.Time {
			return time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
		},
	}
	metrics, err := querier.QueryReleaseGateMetrics(context.Background(), "app_demo", "rel_candidate", model.AppReleaseRoleCandidate, time.Minute)
	if err != nil {
		t.Fatalf("query release gate metrics: %v", err)
	}
	if FloatMetric(metrics, "request_count") != 4 || FloatMetric(metrics, "error_5xx_rate") != 0.25 {
		t.Fatalf("unexpected metrics: %+v", metrics)
	}
	if metrics["metrics_source"] != "release_gate_rollups_1m" {
		t.Fatalf("expected rollup metrics source, got %+v", metrics)
	}
	for _, want := range []string{
		"FROM release_gate_rollups_1m",
		"app_id = 'app_demo'",
		"release_id = 'rel_candidate'",
		"toDateTime64('2026-07-07 11:59:00.000', 3, 'UTC')",
	} {
		if !strings.Contains(query, want) {
			t.Fatalf("expected query to contain %q, got %q", want, query)
		}
	}
	if strings.Contains(query, "release_role") {
		t.Fatalf("expected release id to take precedence over role, got %q", query)
	}
}

func TestClickHouseReleaseGateMetricsQuerierFallsBackToRawFacts(t *testing.T) {
	t.Parallel()

	var queries []string
	clickHouse := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		queries = append(queries, query)
		if strings.Contains(query, "FROM release_gate_rollups_1m") {
			_, _ = w.Write([]byte(`{"request_count":0,"error_5xx_count":0,"edge_upstream_error_count":0,"p95_ttfb_ms":0,"p99_duration_ms":0}` + "\n"))
			return
		}
		_, _ = w.Write([]byte(`{"request_count":4,"error_5xx_count":1,"edge_upstream_error_count":0,"p95_ttfb_ms":120,"p99_duration_ms":350}` + "\n"))
	}))
	t.Cleanup(clickHouse.Close)

	querier := ClickHouseReleaseGateMetricsQuerier{
		DSN: clickHouse.URL + "?database=fugue_observability",
		Now: func() time.Time {
			return time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
		},
	}
	metrics, err := querier.QueryReleaseGateMetrics(context.Background(), "app_demo", "rel_candidate", model.AppReleaseRoleCandidate, time.Minute)
	if err != nil {
		t.Fatalf("query release gate metrics: %v", err)
	}
	if FloatMetric(metrics, "request_count") != 4 || FloatMetric(metrics, "error_5xx_rate") != 0.25 {
		t.Fatalf("unexpected metrics: %+v", metrics)
	}
	if metrics["metrics_source"] != "request_facts" {
		t.Fatalf("expected raw metrics source, got %+v", metrics)
	}
	if len(queries) != 2 || !strings.Contains(queries[0], "FROM release_gate_rollups_1m") || !strings.Contains(queries[1], "FROM request_facts") {
		t.Fatalf("expected rollup query followed by raw fallback, got %+v", queries)
	}
	for _, want := range []string{
		"app_id = 'app_demo'",
		"JSONExtractString(summary_json, 'release_id') = 'rel_candidate'",
		"toDateTime64('2026-07-07 11:59:00.000', 3, 'UTC')",
	} {
		if !strings.Contains(queries[1], want) {
			t.Fatalf("expected raw query to contain %q, got %q", want, queries[1])
		}
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
