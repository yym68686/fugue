package controller

import (
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

func TestSafeZeroDowntimeRolloutProbeFailureAutoAborts(t *testing.T) {
	t.Parallel()

	stateStore, previous, candidate, op := newSafeRolloutTestState(t)
	candidate.Spec.Continuity.ZeroDowntime.Gate = &model.AppReleaseGatePolicy{
		Probes: []model.AppReleaseProbe{{Name: "health", Method: http.MethodGet, Path: "/health", ExpectedStatus: http.StatusOK}},
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
	}))
	t.Cleanup(upstream.Close)
	svc := &Service{
		Store:            stateStore,
		Logger:           log.New(io.Discard, "", 0),
		serviceURLForApp: func(context.Context, model.App) string { return upstream.URL },
	}
	state, err := svc.prepareSafeZeroDowntimeRollout(context.Background(), op, previous, candidate)
	if err != nil {
		t.Fatalf("prepare safe rollout: %v", err)
	}

	err = svc.completeSafeZeroDowntimeRollout(context.Background(), op, state)
	if err == nil || !strings.Contains(err.Error(), "gate failed") {
		t.Fatalf("expected gate failure, got %v", err)
	}
	updatedCandidate, err := stateStore.GetAppRelease(candidate.TenantID, true, state.Candidate.ID)
	if err != nil {
		t.Fatalf("get candidate release: %v", err)
	}
	if updatedCandidate.Status != model.AppReleaseStatusFailed {
		t.Fatalf("expected failed candidate release, got %+v", updatedCandidate)
	}
	policy, err := stateStore.GetAppTrafficPolicy(candidate.TenantID, true, candidate.ID)
	if err != nil {
		t.Fatalf("get traffic policy: %v", err)
	}
	if policy.Mode != model.AppTrafficModeSingle || policy.StableWeight != 100 || policy.CandidateWeight != 0 {
		t.Fatalf("expected stable-only traffic after abort, got %+v", policy)
	}
	evidence, err := stateStore.ListOperationEvidence(model.OperationEvidenceFilter{TenantID: candidate.TenantID, PlatformAdmin: true, OperationID: op.ID})
	if err != nil {
		t.Fatalf("list evidence: %v", err)
	}
	if !safeRolloutEvidenceHasType(evidence, model.OperationEvidenceTypeAppReleaseGateFailure) {
		t.Fatalf("expected app release gate failure evidence, got %+v", evidence)
	}
	steps, err := stateStore.ListReleaseSteps(candidate.TenantID, true, "attempt_safe")
	if err != nil {
		t.Fatalf("list release steps: %v", err)
	}
	if !releaseStepsContainPhase(steps, "gate_check", model.ReleaseStepStatusFailed) || !releaseStepsContainPhase(steps, "abort", model.ReleaseStepStatusCompleted) {
		t.Fatalf("expected failed gate and abort release steps, got %+v", steps)
	}
	events, err := stateStore.ListAuditEvents(candidate.TenantID, true, 100)
	if err != nil {
		t.Fatalf("list audit events: %v", err)
	}
	if !auditEventsContainAction(events, "app.release.gate.fail") || !auditEventsContainAction(events, "app.release.abort.auto") {
		t.Fatalf("expected gate fail and auto abort audit events, got %+v", events)
	}
}

func TestSafeZeroDowntimeRolloutCanaryMetricsFailureAutoAborts(t *testing.T) {
	t.Parallel()

	stateStore, previous, candidate, op := newSafeRolloutTestState(t)
	candidate.Spec.Continuity.ZeroDowntime.Canary = &model.AppRolloutCanarySpec{
		Enabled:               true,
		InitialWeight:         5,
		MaxWeight:             100,
		StepWeights:           []int{5, 10, 100},
		MinObservationSeconds: 0,
	}
	candidate.Spec.Continuity.ZeroDowntime.Gate = &model.AppReleaseGatePolicy{
		WindowSeconds:        60,
		MinCandidateRequests: 1,
		Max5xxRate:           0.01,
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(upstream.Close)
	svc := &Service{
		Store:            stateStore,
		Logger:           log.New(io.Discard, "", 0),
		serviceURLForApp: func(context.Context, model.App) string { return upstream.URL },
		safeRolloutSleep: func(context.Context, time.Duration) error { return nil },
		releaseGateMetricsQuerier: &sequencedReleaseGateMetricsQuerier{metrics: []map[string]any{
			{"request_count": 1, "error_5xx_rate": 0, "edge_upstream_error_rate": 0, "p95_ttfb_ms": 10, "p99_duration_ms": 20},
			{"request_count": 10, "error_5xx_rate": 0.50, "edge_upstream_error_rate": 0, "p95_ttfb_ms": 10, "p99_duration_ms": 20},
		}},
	}
	state, err := svc.prepareSafeZeroDowntimeRollout(context.Background(), op, previous, candidate)
	if err != nil {
		t.Fatalf("prepare safe rollout: %v", err)
	}

	err = svc.completeSafeZeroDowntimeRollout(context.Background(), op, state)
	if err == nil || !strings.Contains(err.Error(), "canary gate failed") {
		t.Fatalf("expected canary gate failure, got %v", err)
	}
	policy, err := stateStore.GetAppTrafficPolicy(candidate.TenantID, true, candidate.ID)
	if err != nil {
		t.Fatalf("get traffic policy: %v", err)
	}
	if policy.Mode != model.AppTrafficModeSingle || policy.StableWeight != 100 || policy.CandidateWeight != 0 {
		t.Fatalf("expected stable-only traffic after canary abort, got %+v", policy)
	}
	steps, err := stateStore.ListReleaseSteps(candidate.TenantID, true, "attempt_safe")
	if err != nil {
		t.Fatalf("list release steps: %v", err)
	}
	if !releaseStepsContainPhase(steps, "canary_shift", model.ReleaseStepStatusCompleted) || !releaseStepsContainPhase(steps, "canary_gate", model.ReleaseStepStatusFailed) {
		t.Fatalf("expected canary shift and failed canary gate steps, got %+v", steps)
	}
	updatedCandidate, err := stateStore.GetAppRelease(candidate.TenantID, true, state.Candidate.ID)
	if err != nil {
		t.Fatalf("get candidate release: %v", err)
	}
	if updatedCandidate.Status != model.AppReleaseStatusFailed || !strings.Contains(updatedCandidate.StatusReason, "canary gate failed") {
		t.Fatalf("expected failed candidate release with canary reason, got %+v", updatedCandidate)
	}
}

func TestSafeZeroDowntimeRolloutCanaryComparisonFailureAutoAborts(t *testing.T) {
	t.Parallel()

	stateStore, previous, candidate, op := newSafeRolloutTestState(t)
	candidate.Spec.Continuity.ZeroDowntime.Canary = &model.AppRolloutCanarySpec{
		Enabled:               true,
		InitialWeight:         5,
		MaxWeight:             100,
		StepWeights:           []int{5, 100},
		MinObservationSeconds: 0,
	}
	candidate.Spec.Continuity.ZeroDowntime.Gate = &model.AppReleaseGatePolicy{
		WindowSeconds:        60,
		MinCandidateRequests: 10,
		Max5xxRate:           0.10,
	}
	svc := &Service{
		Store:            stateStore,
		Logger:           log.New(io.Discard, "", 0),
		serviceURLForApp: func(context.Context, model.App) string { return "http://candidate.test" },
		safeRolloutSleep: func(context.Context, time.Duration) error { return nil },
	}
	state, err := svc.prepareSafeZeroDowntimeRollout(context.Background(), op, previous, candidate)
	if err != nil {
		t.Fatalf("prepare safe rollout: %v", err)
	}
	svc.releaseGateMetricsQuerier = releaseMetricsByIDQuerier{metrics: map[string]map[string]any{
		state.Candidate.ID: {
			"request_count":            20,
			"error_5xx_rate":           0.02,
			"edge_upstream_error_rate": 0.0,
			"status_2xx_rate":          0.94,
			"has_status_class_counts":  true,
			"p95_ttfb_ms":              100,
			"p99_duration_ms":          200,
		},
		state.StableRelease.ID: {
			"request_count":            200,
			"error_5xx_rate":           0.0,
			"edge_upstream_error_rate": 0.0,
			"status_2xx_rate":          1.0,
			"has_status_class_counts":  true,
			"p95_ttfb_ms":              100,
			"p99_duration_ms":          200,
		},
	}}

	err = svc.completeSafeZeroDowntimeRollout(context.Background(), op, state)
	if err == nil || !strings.Contains(err.Error(), "canary gate failed") || !strings.Contains(err.Error(), "candidate 5xx rate") {
		t.Fatalf("expected comparative canary gate failure, got %v", err)
	}
	policy, err := stateStore.GetAppTrafficPolicy(candidate.TenantID, true, candidate.ID)
	if err != nil {
		t.Fatalf("get traffic policy: %v", err)
	}
	if policy.Mode != model.AppTrafficModeSingle || policy.StableWeight != 100 || policy.CandidateWeight != 0 {
		t.Fatalf("expected stable-only traffic after comparative canary abort, got %+v", policy)
	}
	steps, err := stateStore.ListReleaseSteps(candidate.TenantID, true, "attempt_safe")
	if err != nil {
		t.Fatalf("list release steps: %v", err)
	}
	if !releaseStepsContainPhase(steps, "canary_gate", model.ReleaseStepStatusFailed) || !releaseStepsContainPhase(steps, "abort", model.ReleaseStepStatusCompleted) {
		t.Fatalf("expected failed comparative canary gate and abort, got %+v", steps)
	}
}

func TestSafeZeroDowntimeRolloutCanaryGateUsesRawRequestFactsBeforeRollups(t *testing.T) {
	t.Parallel()

	stateStore, previous, candidate, op := newSafeRolloutTestState(t)
	candidate.Spec.Continuity.ZeroDowntime.Canary = &model.AppRolloutCanarySpec{
		Enabled:               true,
		InitialWeight:         5,
		MaxWeight:             100,
		StepWeights:           []int{5, 100},
		MinObservationSeconds: 0,
	}
	candidate.Spec.Continuity.ZeroDowntime.Gate = &model.AppReleaseGatePolicy{
		WindowSeconds:        60,
		MinCandidateRequests: 10,
		Max5xxRate:           0.10,
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(upstream.Close)
	svc := &Service{
		Store:                         stateStore,
		Logger:                        log.New(io.Discard, "", 0),
		serviceURLForApp:              func(context.Context, model.App) string { return upstream.URL },
		safeRolloutSleep:              func(context.Context, time.Duration) error { return nil },
		safeRolloutEdgeBundleObserver: staticSafeRolloutEdgeObserver{observation: safeRolloutEdgeBundleObservation{Ready: true, RequiredNodes: 1, ReadyNodes: 1, Summary: map[string]any{"ready_nodes": 1}}},
		safeRolloutDrainMetricsQuerier: staticSafeRolloutDrainQuerier{metrics: safeRolloutDrainMetrics{
			Ready:                true,
			ActiveConnections:    0,
			MaxActiveConnections: 1,
			FinalCount:           1,
			Summary:              map[string]any{"active_connections": 0},
		}},
	}
	state, err := svc.prepareSafeZeroDowntimeRollout(context.Background(), op, previous, candidate)
	if err != nil {
		t.Fatalf("prepare safe rollout: %v", err)
	}
	querier := &rawPreferredReleaseMetricsQuerier{
		rollup: map[string]any{
			"request_count":            0,
			"error_5xx_rate":           0.0,
			"edge_upstream_error_rate": 0.0,
			"p95_ttfb_ms":              0,
			"p99_duration_ms":          0,
		},
		raw: map[string]map[string]any{
			state.Candidate.ID: {
				"request_count":            20,
				"error_5xx_rate":           0.0,
				"edge_upstream_error_rate": 0.0,
				"status_2xx_rate":          1.0,
				"has_status_class_counts":  true,
				"p95_ttfb_ms":              100,
				"p99_duration_ms":          200,
			},
			state.StableRelease.ID: {
				"request_count":            200,
				"error_5xx_rate":           0.0,
				"edge_upstream_error_rate": 0.0,
				"status_2xx_rate":          1.0,
				"has_status_class_counts":  true,
				"p95_ttfb_ms":              90,
				"p99_duration_ms":          180,
			},
		},
	}
	svc.releaseGateMetricsQuerier = querier

	if err := svc.completeSafeZeroDowntimeRollout(context.Background(), op, state); err != nil {
		t.Fatalf("expected canary rollout to use raw request facts and pass, got %v", err)
	}
	if querier.rawCalls == 0 {
		t.Fatal("expected canary gate to query raw request facts")
	}
	policy, err := stateStore.GetAppTrafficPolicy(candidate.TenantID, true, candidate.ID)
	if err != nil {
		t.Fatalf("get traffic policy: %v", err)
	}
	if policy.StableReleaseID != state.Candidate.ID || policy.CandidateReleaseID != "" || policy.StableWeight != 100 || policy.CandidateWeight != 0 {
		t.Fatalf("expected candidate promoted after raw request comparison, got %+v", policy)
	}
}

func TestSafeZeroDowntimeRolloutCanarySampleDeficitContinuesToNextWeight(t *testing.T) {
	t.Parallel()

	stateStore, previous, candidate, op := newSafeRolloutTestState(t)
	candidate.Spec.Continuity.ZeroDowntime.Canary = &model.AppRolloutCanarySpec{
		Enabled:               true,
		InitialWeight:         1,
		MaxWeight:             100,
		StepWeights:           []int{1, 5, 100},
		MinObservationSeconds: 0,
	}
	candidate.Spec.Continuity.ZeroDowntime.Gate = &model.AppReleaseGatePolicy{
		WindowSeconds:        60,
		MinCandidateRequests: 1,
		Max5xxRate:           0.10,
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(upstream.Close)
	svc := &Service{
		Store:                         stateStore,
		Logger:                        log.New(io.Discard, "", 0),
		serviceURLForApp:              func(context.Context, model.App) string { return upstream.URL },
		safeRolloutSleep:              func(context.Context, time.Duration) error { return nil },
		safeRolloutEdgeBundleObserver: staticSafeRolloutEdgeObserver{observation: safeRolloutEdgeBundleObservation{Ready: true, RequiredNodes: 1, ReadyNodes: 1, Summary: map[string]any{"ready_nodes": 1}}},
		safeRolloutDrainMetricsQuerier: staticSafeRolloutDrainQuerier{metrics: safeRolloutDrainMetrics{
			Ready:                true,
			ActiveConnections:    0,
			MaxActiveConnections: 1,
			FinalCount:           1,
			Summary:              map[string]any{"active_connections": 0},
		}},
		releaseGateMetricsQuerier: &sequencedReleaseGateMetricsQuerier{metrics: []map[string]any{
			{"request_count": 0, "error_5xx_rate": 0, "edge_upstream_error_rate": 0, "p95_ttfb_ms": 0, "p99_duration_ms": 0},
			{"request_count": 0, "error_5xx_rate": 0, "edge_upstream_error_rate": 0, "p95_ttfb_ms": 0, "p99_duration_ms": 0, "has_status_class_counts": true},
			{"request_count": 50, "error_5xx_rate": 0, "edge_upstream_error_rate": 0, "status_2xx_rate": 1.0, "p95_ttfb_ms": 100, "p99_duration_ms": 200, "has_status_class_counts": true},
			{"request_count": 10, "error_5xx_rate": 0, "edge_upstream_error_rate": 0, "status_2xx_rate": 1.0, "p95_ttfb_ms": 100, "p99_duration_ms": 200, "has_status_class_counts": true},
			{"request_count": 50, "error_5xx_rate": 0, "edge_upstream_error_rate": 0, "status_2xx_rate": 1.0, "p95_ttfb_ms": 100, "p99_duration_ms": 200, "has_status_class_counts": true},
			{"request_count": 10, "error_5xx_rate": 0, "edge_upstream_error_rate": 0, "status_2xx_rate": 1.0, "p95_ttfb_ms": 100, "p99_duration_ms": 200, "has_status_class_counts": true},
		}},
	}
	state, err := svc.prepareSafeZeroDowntimeRollout(context.Background(), op, previous, candidate)
	if err != nil {
		t.Fatalf("prepare safe rollout: %v", err)
	}

	if err := svc.completeSafeZeroDowntimeRollout(context.Background(), op, state); err != nil {
		t.Fatalf("expected rollout to continue after low-weight sample deficit, got %v", err)
	}
	steps, err := stateStore.ListReleaseSteps(candidate.TenantID, true, "attempt_safe")
	if err != nil {
		t.Fatalf("list release steps: %v", err)
	}
	if !releaseStepsContainPhase(steps, "canary_gate", model.ReleaseStepStatusSkipped) || !releaseStepsContainPhase(steps, "canary_gate", model.ReleaseStepStatusCompleted) {
		t.Fatalf("expected inconclusive low-weight canary gate followed by completed canary gate, got %+v", steps)
	}
	policy, err := stateStore.GetAppTrafficPolicy(candidate.TenantID, true, candidate.ID)
	if err != nil {
		t.Fatalf("get traffic policy: %v", err)
	}
	if policy.StableReleaseID != state.Candidate.ID || policy.CandidateReleaseID != "" || policy.StableWeight != 100 || policy.CandidateWeight != 0 {
		t.Fatalf("expected candidate promoted after later canary sample, got %+v", policy)
	}
}

func TestSafeZeroDowntimeRolloutCanaryWaitsForEdgeBundleBeforeObservation(t *testing.T) {
	t.Parallel()

	stateStore, previous, candidate, op := newSafeRolloutTestState(t)
	candidate.Spec.Continuity.ZeroDowntime.Canary = &model.AppRolloutCanarySpec{
		Enabled:               true,
		InitialWeight:         50,
		MaxWeight:             100,
		StepWeights:           []int{50, 100},
		MinObservationSeconds: 0,
	}
	svc := &Service{
		Store:                         stateStore,
		Logger:                        log.New(io.Discard, "", 0),
		serviceURLForApp:              func(context.Context, model.App) string { return "http://candidate.test" },
		safeRolloutSleep:              func(context.Context, time.Duration) error { return nil },
		safeRolloutEdgeBundleObserver: staticSafeRolloutEdgeObserver{observation: safeRolloutEdgeBundleObservation{Ready: false, RequiredNodes: 2, ReadyNodes: 1, WaitingNodes: []string{"edge-1:heartbeat_before_promotion"}, Summary: map[string]any{"ready_nodes": 1}}},
		releaseGateMetricsQuerier: releaseMetricsByIDQuerier{metrics: map[string]map[string]any{
			model.AppReleaseRoleCandidate: {"request_count": 10, "error_5xx_rate": 0, "edge_upstream_error_rate": 0, "p95_ttfb_ms": 100, "p99_duration_ms": 200},
		}},
	}
	state, err := svc.prepareSafeZeroDowntimeRollout(context.Background(), op, previous, candidate)
	if err != nil {
		t.Fatalf("prepare safe rollout: %v", err)
	}
	err = svc.completeSafeZeroDowntimeRollout(context.Background(), op, state)
	if err == nil || !strings.Contains(err.Error(), "canary edge route bundle wait failed") {
		t.Fatalf("expected canary edge bundle wait failure, got %v", err)
	}
	policy, err := stateStore.GetAppTrafficPolicy(candidate.TenantID, true, candidate.ID)
	if err != nil {
		t.Fatalf("get traffic policy: %v", err)
	}
	if policy.Mode != model.AppTrafficModeSingle || policy.CandidateReleaseID != "" || policy.StableWeight != 100 || policy.CandidateWeight != 0 {
		t.Fatalf("expected stable-only traffic after canary edge bundle wait failure, got %+v", policy)
	}
	steps, err := stateStore.ListReleaseSteps(candidate.TenantID, true, "attempt_safe")
	if err != nil {
		t.Fatalf("list release steps: %v", err)
	}
	if !releaseStepsContainPhase(steps, "canary_edge_bundle_wait", model.ReleaseStepStatusFailed) ||
		!releaseStepsContainPhase(steps, "abort", model.ReleaseStepStatusCompleted) {
		t.Fatalf("expected failed canary edge bundle wait and abort steps, got %+v", steps)
	}
	for _, step := range steps {
		if value, ok := step.Payload["phase"].(string); ok && value == "canary_gate" {
			t.Fatalf("expected canary gate not to run before edge bundle confirmation, got %+v", steps)
		}
	}
}

func TestSafeZeroDowntimeRolloutSkipsCandidateWhenPodTemplateUnchanged(t *testing.T) {
	t.Parallel()

	stateStore, previous, candidate, op := newSafeRolloutTestState(t)
	candidate.Spec.Image = previous.Spec.Image

	svc := &Service{Store: stateStore, Logger: log.New(io.Discard, "", 0)}
	state, err := svc.prepareSafeZeroDowntimeRollout(context.Background(), op, previous, candidate)
	if err != nil {
		t.Fatalf("prepare safe rollout: %v", err)
	}
	if state != nil {
		t.Fatalf("expected no safe rollout state for unchanged pod template, got %+v", state)
	}
	releases, err := stateStore.ListAppReleases(model.AppReleaseFilter{TenantID: candidate.TenantID, AppID: candidate.ID, PlatformAdmin: true})
	if err != nil {
		t.Fatalf("list releases: %v", err)
	}
	if len(releases) != 1 || releases[0].Role != model.AppReleaseRoleStable {
		t.Fatalf("expected only stable release for unchanged pod template, got %+v", releases)
	}
	policy, err := stateStore.GetAppTrafficPolicy(candidate.TenantID, true, candidate.ID)
	if err != nil {
		t.Fatalf("get traffic policy: %v", err)
	}
	if policy.Mode != model.AppTrafficModeSingle || policy.StableReleaseID != releases[0].ID || policy.CandidateReleaseID != "" || policy.StableWeight != 100 {
		t.Fatalf("expected stable-only traffic policy for unchanged pod template, got %+v", policy)
	}
	steps, err := stateStore.ListReleaseSteps(candidate.TenantID, true, "attempt_safe")
	if err != nil {
		t.Fatalf("list release steps: %v", err)
	}
	if !releaseStepsContainPhase(steps, "candidate_create", model.ReleaseStepStatusSkipped) {
		t.Fatalf("expected skipped candidate_create step for unchanged pod template, got %+v", steps)
	}
}

func TestSafeZeroDowntimeRolloutCandidateRolloutFailureAutoAborts(t *testing.T) {
	t.Parallel()

	stateStore, previous, candidate, op := newSafeRolloutTestState(t)
	svc := &Service{
		Store:  stateStore,
		Logger: log.New(io.Discard, "", 0),
	}
	state, err := svc.prepareSafeZeroDowntimeRollout(context.Background(), op, previous, candidate)
	if err != nil {
		t.Fatalf("prepare safe rollout: %v", err)
	}

	reason := "candidate revision rollout failed: ImagePullBackOff"
	if err := svc.abortSafeZeroDowntimeRollout(context.Background(), op, state, reason); err != nil {
		t.Fatalf("abort safe rollout: %v", err)
	}
	updatedCandidate, err := stateStore.GetAppRelease(candidate.TenantID, true, state.Candidate.ID)
	if err != nil {
		t.Fatalf("get candidate release: %v", err)
	}
	if updatedCandidate.Status != model.AppReleaseStatusFailed || !strings.Contains(updatedCandidate.StatusReason, "ImagePullBackOff") {
		t.Fatalf("expected failed candidate release with image pull reason, got %+v", updatedCandidate)
	}
	policy, err := stateStore.GetAppTrafficPolicy(candidate.TenantID, true, candidate.ID)
	if err != nil {
		t.Fatalf("get traffic policy: %v", err)
	}
	if policy.Mode != model.AppTrafficModeSingle || policy.StableWeight != 100 || policy.CandidateWeight != 0 {
		t.Fatalf("expected stable-only traffic after candidate rollout failure, got %+v", policy)
	}
	steps, err := stateStore.ListReleaseSteps(candidate.TenantID, true, "attempt_safe")
	if err != nil {
		t.Fatalf("list release steps: %v", err)
	}
	if !releaseStepsContainPhase(steps, "abort", model.ReleaseStepStatusCompleted) {
		t.Fatalf("expected completed abort step, got %+v", steps)
	}
	events, err := stateStore.ListAuditEvents(candidate.TenantID, true, 100)
	if err != nil {
		t.Fatalf("list audit events: %v", err)
	}
	if !auditEventsContainAction(events, "app.release.abort.auto") {
		t.Fatalf("expected auto abort audit event, got %+v", events)
	}
}

func TestSafeZeroDowntimeRolloutFailureRestoresPreviousSpec(t *testing.T) {
	t.Parallel()

	stateStore, previous, candidate, op := newSafeRolloutTestState(t)
	restoreCalled := false
	restoreReason := ""
	svc := &Service{
		Store:  stateStore,
		Logger: log.New(io.Discard, "", 0),
		restoreSafeRolloutPreviousSpec: func(_ context.Context, gotOp model.Operation, gotState *safeRolloutState, reason string) error {
			restoreCalled = true
			restoreReason = reason
			if gotOp.ID != op.ID {
				t.Fatalf("expected operation %s, got %s", op.ID, gotOp.ID)
			}
			if gotState == nil || gotState.PreviousApp.Spec.Image != previous.Spec.Image {
				t.Fatalf("expected previous spec image %s, got %+v", previous.Spec.Image, gotState)
			}
			return nil
		},
	}
	state, err := svc.prepareSafeZeroDowntimeRollout(context.Background(), op, previous, candidate)
	if err != nil {
		t.Fatalf("prepare safe rollout: %v", err)
	}

	reason := "candidate rollout wait failed: pod api-abc container api failed: CrashLoopBackOff"
	if err := svc.abortSafeZeroDowntimeRollout(context.Background(), op, state, reason); err != nil {
		t.Fatalf("abort safe rollout: %v", err)
	}
	if !restoreCalled || !strings.Contains(restoreReason, "CrashLoopBackOff") {
		t.Fatalf("expected previous restore hook with rollout failure reason, called=%t reason=%q", restoreCalled, restoreReason)
	}
	steps, err := stateStore.ListReleaseSteps(candidate.TenantID, true, "attempt_safe")
	if err != nil {
		t.Fatalf("list release steps: %v", err)
	}
	if !releaseStepsContainPhase(steps, "abort", model.ReleaseStepStatusCompleted) {
		t.Fatalf("expected abort step before restore, got %+v", steps)
	}
}

func TestSafeZeroDowntimeRolloutWaitsForEdgeBundleBeforeStableAlignment(t *testing.T) {
	t.Parallel()

	stateStore, previous, candidate, op := newSafeRolloutTestState(t)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(upstream.Close)
	svc := &Service{
		Store:                         stateStore,
		Logger:                        log.New(io.Discard, "", 0),
		serviceURLForApp:              func(context.Context, model.App) string { return upstream.URL },
		safeRolloutEdgeBundleObserver: staticSafeRolloutEdgeObserver{observation: safeRolloutEdgeBundleObservation{Ready: false, RequiredNodes: 2, ReadyNodes: 1, Summary: map[string]any{"waiting_nodes": []string{"edge-de-1:serving_lkg"}}}},
	}
	state, err := svc.prepareSafeZeroDowntimeRollout(context.Background(), op, previous, candidate)
	if err != nil {
		t.Fatalf("prepare safe rollout: %v", err)
	}

	if err := svc.completeSafeZeroDowntimeRollout(context.Background(), op, state); err != nil {
		t.Fatalf("complete safe rollout: %v", err)
	}
	if state.StableAlignmentAllowed {
		t.Fatal("expected stable alignment to stay paused until edge route bundle is confirmed")
	}
	steps, err := stateStore.ListReleaseSteps(candidate.TenantID, true, "attempt_safe")
	if err != nil {
		t.Fatalf("list release steps: %v", err)
	}
	if !releaseStepsContainPhase(steps, "edge_bundle_wait", model.ReleaseStepStatusSkipped) ||
		releaseStepsContainPhase(steps, "stable_alignment", model.ReleaseStepStatusCompleted) {
		t.Fatalf("expected skipped edge bundle wait and no completed alignment step, got %+v", steps)
	}
}

func TestSafeZeroDowntimeRolloutPausesAlignmentWhenCandidateFailsRetireGate(t *testing.T) {
	t.Parallel()

	stateStore, previous, candidate, op := newSafeRolloutTestState(t)
	candidate.Spec.Continuity.ZeroDowntime.Gate = &model.AppReleaseGatePolicy{
		Probes: []model.AppReleaseProbe{{Name: "health", Method: http.MethodGet, Path: "/health", ExpectedStatus: http.StatusOK}},
	}
	var requests int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if requests >= 3 {
			http.Error(w, "candidate regressed", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(upstream.Close)
	svc := &Service{
		Store:                         stateStore,
		Logger:                        log.New(io.Discard, "", 0),
		serviceURLForApp:              func(context.Context, model.App) string { return upstream.URL },
		safeRolloutEdgeBundleObserver: staticSafeRolloutEdgeObserver{observation: safeRolloutEdgeBundleObservation{Ready: true, RequiredNodes: 1, ReadyNodes: 1, Summary: map[string]any{"ready_nodes": 1}}},
	}
	state, err := svc.prepareSafeZeroDowntimeRollout(context.Background(), op, previous, candidate)
	if err != nil {
		t.Fatalf("prepare safe rollout: %v", err)
	}

	if err := svc.completeSafeZeroDowntimeRollout(context.Background(), op, state); err != nil {
		t.Fatalf("complete safe rollout should pause retire instead of aborting promoted release: %v", err)
	}
	if state.StableAlignmentAllowed {
		t.Fatal("expected stable alignment to stay paused after candidate retire gate failure")
	}
	steps, err := stateStore.ListReleaseSteps(candidate.TenantID, true, "attempt_safe")
	if err != nil {
		t.Fatalf("list release steps: %v", err)
	}
	if !releaseStepsContainPhase(steps, "pre_alignment_retire_gate", model.ReleaseStepStatusSkipped) {
		t.Fatalf("expected skipped pre-alignment retire gate step, got %+v", steps)
	}
}

func TestSafeZeroDowntimeRolloutRetiresPreviousAfterDrainMetricsReachZero(t *testing.T) {
	t.Parallel()

	stateStore, previous, candidate, op := newSafeRolloutTestState(t)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(upstream.Close)
	svc := &Service{
		Store:                          stateStore,
		Logger:                         log.New(io.Discard, "", 0),
		serviceURLForApp:               func(context.Context, model.App) string { return upstream.URL },
		safeRolloutEdgeBundleObserver:  staticSafeRolloutEdgeObserver{observation: safeRolloutEdgeBundleObservation{Ready: true, RequiredNodes: 1, ReadyNodes: 1, Summary: map[string]any{"ready_nodes": 1}}},
		safeRolloutDrainMetricsQuerier: staticSafeRolloutDrainQuerier{metrics: safeRolloutDrainMetrics{Ready: true, ActiveConnections: 0, MaxActiveConnections: 4, FinalCount: 1, Summary: map[string]any{"active_connections": 0, "max_active_connections": 4}}},
	}
	state, err := svc.prepareSafeZeroDowntimeRollout(context.Background(), op, previous, candidate)
	if err != nil {
		t.Fatalf("prepare safe rollout: %v", err)
	}
	if err := svc.completeSafeZeroDowntimeRollout(context.Background(), op, state); err != nil {
		t.Fatalf("complete safe rollout: %v", err)
	}
	if !state.StableAlignmentAllowed {
		t.Fatal("expected stable alignment to be allowed after edge and candidate retire gates pass")
	}
	svc.finalizeSafeZeroDowntimePreviousRetire(context.Background(), op, state)

	retiredPrevious, err := stateStore.GetAppRelease(candidate.TenantID, true, state.StableRelease.ID)
	if err != nil {
		t.Fatalf("get previous release: %v", err)
	}
	if retiredPrevious.Role != model.AppReleaseRoleRetired || retiredPrevious.Status != model.AppReleaseStatusRetired {
		t.Fatalf("expected previous release retired after zero active drain metrics, got %+v", retiredPrevious)
	}
	steps, err := stateStore.ListReleaseSteps(candidate.TenantID, true, "attempt_safe")
	if err != nil {
		t.Fatalf("list release steps: %v", err)
	}
	if !releaseStepsContainPhase(steps, "previous_retire", model.ReleaseStepStatusCompleted) {
		t.Fatalf("expected completed previous_retire step, got %+v", steps)
	}
	events, err := stateStore.ListAuditEvents(candidate.TenantID, true, 100)
	if err != nil {
		t.Fatalf("list audit events: %v", err)
	}
	if !auditEventsContainAction(events, "app.release.previous_retired") {
		t.Fatalf("expected previous retired audit event, got %+v", events)
	}
}

func TestSafeZeroDowntimeRolloutRetiresPreviousWhenRevisionDeploymentGone(t *testing.T) {
	t.Parallel()

	stateStore, previous, candidate, op := newSafeRolloutTestState(t)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(upstream.Close)
	kubeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	t.Cleanup(kubeServer.Close)
	svc := &Service{
		Store:                         stateStore,
		Logger:                        log.New(io.Discard, "", 0),
		serviceURLForApp:              func(context.Context, model.App) string { return upstream.URL },
		safeRolloutEdgeBundleObserver: staticSafeRolloutEdgeObserver{observation: safeRolloutEdgeBundleObservation{Ready: true, RequiredNodes: 1, ReadyNodes: 1, Summary: map[string]any{"ready_nodes": 1}}},
		safeRolloutDrainMetricsQuerier: staticSafeRolloutDrainQuerier{metrics: safeRolloutDrainMetrics{
			Ready:             false,
			ActiveConnections: 0,
			SampleCount:       0,
			FinalCount:        0,
			Summary:           map[string]any{"active_connections": 0, "sample_count": 0, "final_count": 0},
		}},
		newKubeClient: func(namespace string) (*kubeClient, error) {
			return &kubeClient{client: kubeServer.Client(), baseURL: kubeServer.URL, bearerToken: "test", namespace: namespace}, nil
		},
	}
	state, err := svc.prepareSafeZeroDowntimeRollout(context.Background(), op, previous, candidate)
	if err != nil {
		t.Fatalf("prepare safe rollout: %v", err)
	}
	state.StableRelease.DeploymentName = "app-demo-previous-candidate"
	if _, err := stateStore.UpdateAppRelease(state.StableRelease); err != nil {
		t.Fatalf("update stable release deployment name: %v", err)
	}
	if err := svc.completeSafeZeroDowntimeRollout(context.Background(), op, state); err != nil {
		t.Fatalf("complete safe rollout: %v", err)
	}
	svc.finalizeSafeZeroDowntimePreviousRetire(context.Background(), op, state)

	retiredPrevious, err := stateStore.GetAppRelease(candidate.TenantID, true, state.StableRelease.ID)
	if err != nil {
		t.Fatalf("get previous release: %v", err)
	}
	if retiredPrevious.Role != model.AppReleaseRoleRetired || retiredPrevious.Status != model.AppReleaseStatusRetired {
		t.Fatalf("expected missing previous revision deployment to retire previous release, got %+v", retiredPrevious)
	}
}

func TestSafeZeroDowntimeRolloutPausesPreviousRetireWhenDrainMetricsStillActive(t *testing.T) {
	t.Parallel()

	stateStore, previous, candidate, op := newSafeRolloutTestState(t)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(upstream.Close)
	svc := &Service{
		Store:                          stateStore,
		Logger:                         log.New(io.Discard, "", 0),
		serviceURLForApp:               func(context.Context, model.App) string { return upstream.URL },
		safeRolloutEdgeBundleObserver:  staticSafeRolloutEdgeObserver{observation: safeRolloutEdgeBundleObservation{Ready: true, RequiredNodes: 1, ReadyNodes: 1, Summary: map[string]any{"ready_nodes": 1}}},
		safeRolloutDrainMetricsQuerier: staticSafeRolloutDrainQuerier{metrics: safeRolloutDrainMetrics{Ready: false, ActiveConnections: 2, MaxActiveConnections: 4, SampleCount: 1, Summary: map[string]any{"active_connections": 2, "max_active_connections": 4}}},
	}
	state, err := svc.prepareSafeZeroDowntimeRollout(context.Background(), op, previous, candidate)
	if err != nil {
		t.Fatalf("prepare safe rollout: %v", err)
	}
	if err := svc.completeSafeZeroDowntimeRollout(context.Background(), op, state); err != nil {
		t.Fatalf("complete safe rollout: %v", err)
	}
	svc.finalizeSafeZeroDowntimePreviousRetire(context.Background(), op, state)

	drainingPrevious, err := stateStore.GetAppRelease(candidate.TenantID, true, state.StableRelease.ID)
	if err != nil {
		t.Fatalf("get previous release: %v", err)
	}
	if drainingPrevious.Role != model.AppReleaseRolePrevious || drainingPrevious.Status != model.AppReleaseStatusDraining {
		t.Fatalf("expected previous release to remain draining while active connections exist, got %+v", drainingPrevious)
	}
	steps, err := stateStore.ListReleaseSteps(candidate.TenantID, true, "attempt_safe")
	if err != nil {
		t.Fatalf("list release steps: %v", err)
	}
	if !releaseStepsContainPhase(steps, "previous_retire", model.ReleaseStepStatusSkipped) {
		t.Fatalf("expected skipped previous_retire step, got %+v", steps)
	}
}

func TestSafeZeroDowntimeRolloutIntegrationStatelessAppSuccess(t *testing.T) {
	t.Parallel()

	stateStore, previous, candidate, op := newSafeRolloutTestState(t)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(upstream.Close)
	svc := newSafeRolloutIntegrationService(stateStore, upstream.URL, safeRolloutDrainMetrics{
		Ready:                true,
		ActiveConnections:    0,
		MaxActiveConnections: 1,
		FinalCount:           1,
		Summary:              map[string]any{"active_connections": 0, "max_active_connections": 1},
	})
	state, err := svc.prepareSafeZeroDowntimeRollout(context.Background(), op, previous, candidate)
	if err != nil {
		t.Fatalf("prepare safe rollout: %v", err)
	}
	if err := svc.completeSafeZeroDowntimeRollout(context.Background(), op, state); err != nil {
		t.Fatalf("complete safe rollout: %v", err)
	}
	if !state.StableAlignmentAllowed {
		t.Fatal("expected stable alignment to be allowed after successful safe rollout gates")
	}
	svc.finalizeSafeZeroDowntimePreviousRetire(context.Background(), op, state)

	policy, err := stateStore.GetAppTrafficPolicy(candidate.TenantID, true, candidate.ID)
	if err != nil {
		t.Fatalf("get traffic policy: %v", err)
	}
	if policy.Mode != model.AppTrafficModeSingle || policy.StableReleaseID != state.Candidate.ID || policy.CandidateReleaseID != "" || policy.StableWeight != 100 || policy.CandidateWeight != 0 {
		t.Fatalf("expected promoted candidate to receive all traffic, got %+v", policy)
	}
	promoted, err := stateStore.GetAppRelease(candidate.TenantID, true, state.Candidate.ID)
	if err != nil {
		t.Fatalf("get promoted candidate: %v", err)
	}
	if promoted.Role != model.AppReleaseRoleStable || promoted.Status != model.AppReleaseStatusServing || promoted.PromotedAt == nil {
		t.Fatalf("expected candidate promoted to serving stable, got %+v", promoted)
	}
	previousRelease, err := stateStore.GetAppRelease(candidate.TenantID, true, state.StableRelease.ID)
	if err != nil {
		t.Fatalf("get previous release: %v", err)
	}
	if previousRelease.Role != model.AppReleaseRoleRetired || previousRelease.Status != model.AppReleaseStatusRetired {
		t.Fatalf("expected drained previous release retired, got %+v", previousRelease)
	}
	steps, err := stateStore.ListReleaseSteps(candidate.TenantID, true, "attempt_safe")
	if err != nil {
		t.Fatalf("list release steps: %v", err)
	}
	for _, phase := range []string{"candidate_create", "candidate_ready", "gate_check", "final_gate", "promote", "edge_bundle_wait", "pre_alignment_retire_gate", "pre_previous_retire_gate", "previous_retire"} {
		if !releaseStepsContainPhase(steps, phase, model.ReleaseStepStatusCompleted) {
			t.Fatalf("expected completed phase %q, got %+v", phase, steps)
		}
	}
}

func TestSafeZeroDowntimeRolloutIntegrationLongRequestKeepsPreviousDraining(t *testing.T) {
	t.Parallel()

	stateStore, previous, candidate, op := newSafeRolloutTestState(t)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(upstream.Close)
	svc := newSafeRolloutIntegrationService(stateStore, upstream.URL, safeRolloutDrainMetrics{
		Ready:                false,
		ActiveConnections:    1,
		MaxActiveConnections: 1,
		SampleCount:          3,
		Summary:              map[string]any{"active_connections": 1, "max_active_connections": 1},
	})
	state, err := svc.prepareSafeZeroDowntimeRollout(context.Background(), op, previous, candidate)
	if err != nil {
		t.Fatalf("prepare safe rollout: %v", err)
	}
	if err := svc.completeSafeZeroDowntimeRollout(context.Background(), op, state); err != nil {
		t.Fatalf("complete safe rollout: %v", err)
	}
	svc.finalizeSafeZeroDowntimePreviousRetire(context.Background(), op, state)

	previousRelease, err := stateStore.GetAppRelease(candidate.TenantID, true, state.StableRelease.ID)
	if err != nil {
		t.Fatalf("get previous release: %v", err)
	}
	if previousRelease.Role != model.AppReleaseRolePrevious || previousRelease.Status != model.AppReleaseStatusDraining {
		t.Fatalf("expected previous release to keep draining while long request is active, got %+v", previousRelease)
	}
	policy, err := stateStore.GetAppTrafficPolicy(candidate.TenantID, true, candidate.ID)
	if err != nil {
		t.Fatalf("get traffic policy: %v", err)
	}
	if policy.StableReleaseID != state.Candidate.ID || policy.CandidateReleaseID != "" || policy.StableWeight != 100 {
		t.Fatalf("expected new requests to stay on promoted candidate while previous drains, got %+v", policy)
	}
	steps, err := stateStore.ListReleaseSteps(candidate.TenantID, true, "attempt_safe")
	if err != nil {
		t.Fatalf("list release steps: %v", err)
	}
	if !releaseStepsContainPhase(steps, "previous_retire", model.ReleaseStepStatusSkipped) {
		t.Fatalf("expected previous retire to pause for active long request, got %+v", steps)
	}
}

func TestSafeZeroDowntimeRolloutIntegrationCandidateCrashLoopKeepsStable(t *testing.T) {
	t.Parallel()

	stateStore, previous, candidate, op := newSafeRolloutTestState(t)
	svc := &Service{
		Store:  stateStore,
		Logger: log.New(io.Discard, "", 0),
	}
	state, err := svc.prepareSafeZeroDowntimeRollout(context.Background(), op, previous, candidate)
	if err != nil {
		t.Fatalf("prepare safe rollout: %v", err)
	}
	if err := svc.abortSafeZeroDowntimeRollout(context.Background(), op, state, "candidate rollout failed: CrashLoopBackOff"); err != nil {
		t.Fatalf("abort safe rollout: %v", err)
	}

	policy, err := stateStore.GetAppTrafficPolicy(candidate.TenantID, true, candidate.ID)
	if err != nil {
		t.Fatalf("get traffic policy: %v", err)
	}
	if policy.StableReleaseID != state.StableRelease.ID || policy.StableWeight != 100 || policy.CandidateWeight != 0 {
		t.Fatalf("expected stable release to keep all traffic after crashloop, got %+v", policy)
	}
	stable, err := stateStore.GetAppRelease(candidate.TenantID, true, state.StableRelease.ID)
	if err != nil {
		t.Fatalf("get stable release: %v", err)
	}
	if stable.Role != model.AppReleaseRoleStable || stable.Status != model.AppReleaseStatusReady {
		t.Fatalf("expected existing stable release to remain ready stable, got %+v", stable)
	}
	failedCandidate, err := stateStore.GetAppRelease(candidate.TenantID, true, state.Candidate.ID)
	if err != nil {
		t.Fatalf("get failed candidate: %v", err)
	}
	if failedCandidate.Status != model.AppReleaseStatusFailed || !strings.Contains(failedCandidate.StatusReason, "CrashLoopBackOff") {
		t.Fatalf("expected failed crashloop candidate, got %+v", failedCandidate)
	}
}

func TestSafeZeroDowntimeRolloutIntegrationBusinessProbeFailureKeepsStable(t *testing.T) {
	t.Parallel()

	stateStore, previous, candidate, op := newSafeRolloutTestState(t)
	candidate.Spec.Continuity.ZeroDowntime.Gate = &model.AppReleaseGatePolicy{
		Probes: []model.AppReleaseProbe{{Name: "health", Method: http.MethodGet, Path: "/health", ExpectedStatus: http.StatusOK}},
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "business readiness failed", http.StatusServiceUnavailable)
	}))
	t.Cleanup(upstream.Close)
	svc := &Service{
		Store:            stateStore,
		Logger:           log.New(io.Discard, "", 0),
		serviceURLForApp: func(context.Context, model.App) string { return upstream.URL },
	}
	state, err := svc.prepareSafeZeroDowntimeRollout(context.Background(), op, previous, candidate)
	if err != nil {
		t.Fatalf("prepare safe rollout: %v", err)
	}

	err = svc.completeSafeZeroDowntimeRollout(context.Background(), op, state)
	if err == nil || !strings.Contains(err.Error(), "gate failed") {
		t.Fatalf("expected business probe gate failure, got %v", err)
	}
	policy, err := stateStore.GetAppTrafficPolicy(candidate.TenantID, true, candidate.ID)
	if err != nil {
		t.Fatalf("get traffic policy: %v", err)
	}
	if policy.StableReleaseID != state.StableRelease.ID || policy.StableWeight != 100 || policy.CandidateWeight != 0 {
		t.Fatalf("expected stable release to keep all traffic after probe failure, got %+v", policy)
	}
	stable, err := stateStore.GetAppRelease(candidate.TenantID, true, state.StableRelease.ID)
	if err != nil {
		t.Fatalf("get stable release: %v", err)
	}
	if stable.Role != model.AppReleaseRoleStable {
		t.Fatalf("expected existing stable release to remain stable, got %+v", stable)
	}
}

func TestSafeZeroDowntimeRolloutIntegrationPreviousStableRollbackWindowRecoverable(t *testing.T) {
	t.Parallel()

	stateStore, previous, candidate, op := newSafeRolloutTestState(t)
	previous.Spec.Continuity.ZeroDowntime.RollbackWindowSeconds = 300
	candidate.Spec.Continuity.ZeroDowntime.RollbackWindowSeconds = 300
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(upstream.Close)
	svc := newSafeRolloutIntegrationService(stateStore, upstream.URL, safeRolloutDrainMetrics{
		Ready:                false,
		ActiveConnections:    1,
		MaxActiveConnections: 2,
		SampleCount:          1,
		Summary:              map[string]any{"active_connections": 1, "max_active_connections": 2},
	})
	state, err := svc.prepareSafeZeroDowntimeRollout(context.Background(), op, previous, candidate)
	if err != nil {
		t.Fatalf("prepare safe rollout: %v", err)
	}
	if err := svc.completeSafeZeroDowntimeRollout(context.Background(), op, state); err != nil {
		t.Fatalf("complete safe rollout: %v", err)
	}
	svc.finalizeSafeZeroDowntimePreviousRetire(context.Background(), op, state)

	previousRelease, err := stateStore.GetAppRelease(candidate.TenantID, true, state.StableRelease.ID)
	if err != nil {
		t.Fatalf("get previous release: %v", err)
	}
	if previousRelease.Role != model.AppReleaseRolePrevious || previousRelease.Status != model.AppReleaseStatusDraining || previousRelease.RetentionUntil == nil {
		t.Fatalf("expected previous release retained as rollback target, got %+v", previousRelease)
	}
	promotedCandidate, err := stateStore.GetAppRelease(candidate.TenantID, true, state.Candidate.ID)
	if err != nil {
		t.Fatalf("get promoted candidate: %v", err)
	}
	if promotedCandidate.RollbackTargetID != previousRelease.ID {
		t.Fatalf("expected promoted candidate to point at rollback target %s, got %+v", previousRelease.ID, promotedCandidate)
	}
	principal := model.Principal{TenantID: candidate.TenantID, ActorType: model.ActorTypeSystem, ActorID: "safe-rollout-integration-test"}
	rollbackPolicy, err := svc.appReleaseService().PromoteRelease(context.Background(), principal, candidate, previousRelease, 100)
	if err != nil {
		t.Fatalf("restore previous stable release: %v", err)
	}
	if rollbackPolicy.StableReleaseID != previousRelease.ID || rollbackPolicy.CandidateReleaseID != "" || rollbackPolicy.StableWeight != 100 {
		t.Fatalf("expected rollback to restore previous stable traffic, got %+v", rollbackPolicy)
	}
	restored, err := stateStore.GetAppRelease(candidate.TenantID, true, previousRelease.ID)
	if err != nil {
		t.Fatalf("get restored previous release: %v", err)
	}
	if restored.Role != model.AppReleaseRoleStable || restored.Status != model.AppReleaseStatusServing {
		t.Fatalf("expected previous release restored to serving stable, got %+v", restored)
	}
}

func TestSafeRolloutEdgeObserverRejectsServingLKGAndStaleHeartbeat(t *testing.T) {
	t.Parallel()

	now := time.Unix(1700000000, 0).UTC()
	readyHeartbeat := now.Add(time.Second)
	observer := storeSafeRolloutEdgeBundleObserver{
		Store: staticEdgeNodeLister{nodes: []model.EdgeNode{
			{
				ID:                 "edge-live",
				EdgeGroupID:        "edge-group-country-us",
				Status:             model.EdgeHealthHealthy,
				Healthy:            true,
				CaddyRouteCount:    10,
				RouteBundleVersion: "routegen_live",
				ServingGeneration:  "routegen_live",
				LastHeartbeatAt:    &readyHeartbeat,
			},
			{
				ID:                 "edge-lkg",
				EdgeGroupID:        "edge-group-country-de",
				Status:             model.EdgeHealthHealthy,
				Healthy:            true,
				CaddyRouteCount:    10,
				RouteBundleVersion: "routegen_new",
				ServingGeneration:  "routegen_old",
				LKGGeneration:      "routegen_old",
				LastHeartbeatAt:    &readyHeartbeat,
			},
		}},
		Now: func() time.Time { return now },
	}
	observation, err := observer.observe(model.App{ID: "app"}, model.AppRelease{ID: "rel"}, now)
	if err != nil {
		t.Fatalf("wait edge bundle: %v", err)
	}
	if observation.Ready || observation.RequiredNodes != 2 || observation.ReadyNodes != 1 {
		t.Fatalf("expected one waiting LKG node, got %+v", observation)
	}
	if len(observation.WaitingNodes) != 1 || !strings.Contains(observation.WaitingNodes[0], "serving_lkg") {
		t.Fatalf("expected serving_lkg waiting reason, got %+v", observation.WaitingNodes)
	}
}

func TestSafeRolloutDrainMetricsParserRequiresFinalZeroActive(t *testing.T) {
	t.Parallel()

	metrics := safeRolloutDrainMetrics{Summary: map[string]any{}}
	safeRolloutApplyDrainLogLine(&metrics, "fugue_drain_sample active_connections=3 states=ESTABLISHED:3 waited_ms=1000")
	safeRolloutApplyDrainLogLine(&metrics, "fugue_drain_complete reason=idle waited_ms=3200 active_connections=0 max_active_connections=3 observer_errors=0")
	metrics.Ready = metrics.FinalCount > 0 && metrics.ActiveConnections == 0
	if !metrics.Ready || metrics.SampleCount != 1 || metrics.FinalCount != 1 || metrics.MaxActiveConnections != 3 {
		t.Fatalf("expected zero-active final drain metrics to be ready, got %+v", metrics)
	}

	active := safeRolloutDrainMetrics{Summary: map[string]any{}}
	safeRolloutApplyDrainLogLine(&active, "fugue_drain_complete reason=timeout waited_ms=600000 active_connections=2 max_active_connections=5 observer_errors=0")
	active.Ready = active.FinalCount > 0 && active.ActiveConnections == 0
	if active.Ready {
		t.Fatalf("expected non-zero active drain metrics to block retire, got %+v", active)
	}

	timedOut := safeRolloutDrainMetrics{Summary: map[string]any{}}
	safeRolloutApplyDrainLogLine(&timedOut, "fugue_drain_complete reason=timeout waited_ms=600000 active_connections=0 max_active_connections=5 observer_errors=0")
	unsafeFinalReason, _ := timedOut.Summary["unsafe_final_reason"].(bool)
	timedOut.Ready = timedOut.FinalCount > 0 && timedOut.ActiveConnections == 0 && !unsafeFinalReason
	if timedOut.Ready {
		t.Fatalf("expected timeout final reason to block retire even with zero active connections, got %+v", timedOut)
	}
}

type staticEdgeNodeLister struct {
	nodes []model.EdgeNode
}

func (l staticEdgeNodeLister) ListEdgeNodes(string) ([]model.EdgeNode, []model.EdgeGroup, error) {
	return l.nodes, nil, nil
}

type staticSafeRolloutEdgeObserver struct {
	observation safeRolloutEdgeBundleObservation
	err         error
}

func (o staticSafeRolloutEdgeObserver) WaitForSafeRolloutEdgeRouteBundle(context.Context, model.App, model.AppRelease, time.Time) (safeRolloutEdgeBundleObservation, error) {
	return o.observation, o.err
}

type staticSafeRolloutDrainQuerier struct {
	metrics safeRolloutDrainMetrics
	err     error
}

func (q staticSafeRolloutDrainQuerier) QuerySafeRolloutDrainMetrics(context.Context, model.App, model.AppRelease, time.Time) (safeRolloutDrainMetrics, error) {
	return q.metrics, q.err
}

type sequencedReleaseGateMetricsQuerier struct {
	mu      sync.Mutex
	metrics []map[string]any
}

func (q *sequencedReleaseGateMetricsQuerier) QueryReleaseGateMetrics(context.Context, string, string, string, time.Duration) (map[string]any, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.metrics) == 0 {
		return map[string]any{"request_count": 1}, nil
	}
	out := q.metrics[0]
	q.metrics = q.metrics[1:]
	return out, nil
}

type releaseMetricsByIDQuerier struct {
	metrics map[string]map[string]any
}

func (q releaseMetricsByIDQuerier) QueryReleaseGateMetrics(_ context.Context, _ string, releaseID, releaseRole string, _ time.Duration) (map[string]any, error) {
	if metrics, ok := q.metrics[releaseID]; ok {
		return metrics, nil
	}
	if metrics, ok := q.metrics[releaseRole]; ok {
		return metrics, nil
	}
	return map[string]any{}, nil
}

type rawPreferredReleaseMetricsQuerier struct {
	rollup   map[string]any
	raw      map[string]map[string]any
	rawCalls int
}

func (q *rawPreferredReleaseMetricsQuerier) QueryReleaseGateMetrics(context.Context, string, string, string, time.Duration) (map[string]any, error) {
	if q.rollup != nil {
		return q.rollup, nil
	}
	return map[string]any{}, nil
}

func (q *rawPreferredReleaseMetricsQuerier) QueryReleaseGateRawMetrics(_ context.Context, _ string, releaseID, releaseRole string, _ time.Duration) (map[string]any, error) {
	q.rawCalls++
	if metrics, ok := q.raw[releaseID]; ok {
		return metrics, nil
	}
	if metrics, ok := q.raw[releaseRole]; ok {
		return metrics, nil
	}
	return map[string]any{}, nil
}

func newSafeRolloutIntegrationService(stateStore *store.Store, upstreamURL string, drainMetrics safeRolloutDrainMetrics) *Service {
	return &Service{
		Store:                          stateStore,
		Logger:                         log.New(io.Discard, "", 0),
		serviceURLForApp:               func(context.Context, model.App) string { return upstreamURL },
		safeRolloutEdgeBundleObserver:  staticSafeRolloutEdgeObserver{observation: safeRolloutEdgeBundleObservation{Ready: true, RequiredNodes: 2, ReadyNodes: 2, Summary: map[string]any{"ready_nodes": 2}}},
		safeRolloutDrainMetricsQuerier: staticSafeRolloutDrainQuerier{metrics: drainMetrics},
	}
}

func newSafeRolloutTestState(t *testing.T) (*store.Store, model.App, model.App, model.Operation) {
	t.Helper()
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Safe Rollout Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "safe", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	continuity := &model.AppContinuityPolicy{ZeroDowntime: &model.AppZeroDowntimePolicy{
		Enabled:  true,
		Mode:     model.AppZeroDowntimeModeSafe,
		Strategy: model.AppZeroDowntimeStrategyStableCandidate,
		Canary: &model.AppRolloutCanarySpec{
			Enabled:               false,
			InitialWeight:         0,
			MaxWeight:             100,
			MinObservationSeconds: 0,
		},
	}}
	previous, err := stateStore.CreateApp(tenant.ID, project.ID, "api", "", model.AppSpec{
		Image:      "ghcr.io/example/api:v1",
		Ports:      []int{8080},
		Replicas:   1,
		Continuity: continuity,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	previous.Status.CurrentReplicas = 1
	candidate := previous
	candidate.Spec.Image = "ghcr.io/example/api:v2"
	op, err := stateStore.CreateOperation(model.Operation{
		TenantID:      tenant.ID,
		Type:          model.OperationTypeDeploy,
		AppID:         previous.ID,
		DesiredSpec:   &candidate.Spec,
		ExecutionMode: model.ExecutionModeManaged,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}
	if _, err := stateStore.CreateReleaseAttempt(model.ReleaseAttempt{
		ID:                "attempt_safe",
		TenantID:          tenant.ID,
		ProjectID:         project.ID,
		AppID:             previous.ID,
		TriggerType:       model.ReleaseAttemptTriggerManualDeploy,
		TriggerActorType:  model.ReleaseAttemptActorSystem,
		SourceOperationID: op.ID,
		RootOperationID:   op.ID,
		Status:            model.ReleaseAttemptStatusRollingOut,
		Confidence:        model.OperationEvidenceConfidenceEvidenceBacked,
	}); err != nil {
		t.Fatalf("create release attempt: %v", err)
	}
	return stateStore, previous, candidate, op
}

func releaseStepsContainPhase(steps []model.ReleaseStep, phase, status string) bool {
	for _, step := range steps {
		if step.Status != status {
			continue
		}
		if value, ok := step.Payload["phase"].(string); ok && value == phase {
			return true
		}
	}
	return false
}

func safeRolloutEvidenceHasType(items []model.OperationEvidence, typ string) bool {
	for _, item := range items {
		if item.Type == typ {
			return true
		}
	}
	return false
}

func auditEventsContainAction(items []model.AuditEvent, action string) bool {
	for _, item := range items {
		if item.Action == action {
			return true
		}
	}
	return false
}
