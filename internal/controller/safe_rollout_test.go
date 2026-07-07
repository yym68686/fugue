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
