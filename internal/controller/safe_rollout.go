package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/releaseflow"
	"fugue/internal/runtime"
)

type safeRolloutState struct {
	Enabled       bool
	PreviousApp   model.App
	CandidateApp  model.App
	StableRelease model.AppRelease
	Candidate     model.AppRelease
}

func (s *Service) prepareSafeZeroDowntimeRollout(ctx context.Context, op model.Operation, previous, candidate model.App) (*safeRolloutState, error) {
	if op.Type != model.OperationTypeDeploy ||
		!model.AppSafeZeroDowntimeRolloutEnabled(previous.Spec) ||
		!model.AppSafeZeroDowntimeRolloutEnabled(candidate.Spec) {
		return nil, nil
	}
	if !model.AppExposesPublicService(candidate.Spec) {
		return nil, fmt.Errorf("safe zero downtime rollout requires a public or internal cluster service")
	}
	if candidate.Spec.PersistentStorage != nil && !model.AppPersistentStorageSpecUsesSharedProjectRWX(candidate.Spec.PersistentStorage) {
		return nil, fmt.Errorf("safe zero downtime rollout does not support non-RWX persistent storage yet")
	}
	if candidate.Spec.Workspace != nil {
		return nil, fmt.Errorf("safe zero downtime rollout does not support workspace storage yet")
	}
	principal := model.Principal{
		TenantID:  candidate.TenantID,
		ActorType: model.ActorTypeSystem,
		ActorID:   "safe-rollout-controller",
	}
	service := s.appReleaseService()
	stable, err := service.EnsureStableRelease(ctx, previous)
	if err != nil {
		return nil, fmt.Errorf("ensure stable release before safe rollout: %w", err)
	}
	if _, err := service.EnsureStableTrafficPolicy(ctx, principal, previous); err != nil {
		return nil, fmt.Errorf("ensure stable traffic policy before safe rollout: %w", err)
	}
	specSnapshot := candidate.Spec
	candidateRelease, err := service.CreateRelease(ctx, candidate, releaseflow.CreateReleaseRequest{
		Role:             model.AppReleaseRoleCandidate,
		SourceRef:        releaseflow.AppReleaseSourceRef(candidate),
		ResolvedImageRef: candidate.Spec.Image,
		RuntimeID:        candidate.Spec.RuntimeID,
		Status:           model.AppReleaseStatusCreating,
		SpecSnapshot:     &specSnapshot,
	})
	if err != nil {
		return nil, fmt.Errorf("create candidate release before safe rollout: %w", err)
	}
	s.recordSafeRolloutReleaseStep(op, candidate, "candidate_create", model.ReleaseStepStatusCompleted, "candidate release created", candidateRelease.ID, map[string]any{
		"stable_release_id":    stable.ID,
		"candidate_release_id": candidateRelease.ID,
	})
	return &safeRolloutState{Enabled: true, PreviousApp: previous, CandidateApp: candidate, StableRelease: stable, Candidate: candidateRelease}, nil
}

func (s *Service) completeSafeZeroDowntimeRollout(ctx context.Context, op model.Operation, state *safeRolloutState) error {
	if state == nil || !state.Enabled {
		return nil
	}
	service := s.appReleaseService()
	now := time.Now().UTC()
	candidate := state.Candidate
	candidate.UpstreamURL = controllerServiceURLForApp(state.CandidateApp)
	candidate.DeploymentName = runtime.RuntimeAppResourceName(state.CandidateApp)
	candidate.ServiceName = runtime.RuntimeAppServiceName(state.CandidateApp)
	candidate.Status = model.AppReleaseStatusReady
	candidate.ReadyAt = releaseflow.FirstNonNilTime(candidate.ReadyAt, &now)
	updated, err := s.Store.UpdateAppRelease(candidate)
	if err != nil {
		return fmt.Errorf("mark candidate release ready: %w", err)
	}
	state.Candidate = updated
	s.recordSafeRolloutReleaseStep(op, state.CandidateApp, "candidate_ready", model.ReleaseStepStatusCompleted, "candidate rollout ready", updated.ID, map[string]any{
		"upstream_url": updated.UpstreamURL,
	})

	policy := releaseflow.ReleaseGateEvaluator{}.NormalizePolicy(nil)
	if continuity := model.NormalizeAppContinuityPolicy(state.CandidateApp.Spec.Continuity); continuity != nil && continuity.ZeroDowntime != nil && continuity.ZeroDowntime.Gate != nil {
		policy = releaseflow.ReleaseGateEvaluator{}.NormalizePolicy(continuity.ZeroDowntime.Gate)
	}
	gate := s.releaseGateEvaluator().Evaluate(ctx, state.CandidateApp, state.Candidate, policy)
	if gate.Status == model.AppReleaseGateStatusFail {
		_ = s.abortSafeZeroDowntimeRollout(ctx, op, state, "candidate gate failed: "+strings.Join(gate.Failures, "; "))
		return fmt.Errorf("safe zero downtime rollout gate failed: %s", strings.Join(gate.Failures, "; "))
	}
	s.recordSafeRolloutReleaseStep(op, state.CandidateApp, "gate_check", model.ReleaseStepStatusCompleted, "candidate gate passed", state.Candidate.ID, map[string]any{
		"gate": gate,
	})

	principal := model.Principal{TenantID: state.CandidateApp.TenantID, ActorType: model.ActorTypeSystem, ActorID: "safe-rollout-controller"}
	traffic, err := service.PromoteRelease(ctx, principal, state.CandidateApp, state.Candidate, 100)
	if err != nil {
		return fmt.Errorf("promote candidate release after safe rollout gate: %w", err)
	}
	s.recordSafeRolloutReleaseStep(op, state.CandidateApp, "promote", model.ReleaseStepStatusCompleted, "candidate promoted to stable", state.Candidate.ID, map[string]any{
		"traffic_policy_id": traffic.ID,
	})
	return nil
}

func (s *Service) abortSafeZeroDowntimeRollout(ctx context.Context, op model.Operation, state *safeRolloutState, reason string) error {
	if state == nil || !state.Enabled {
		return nil
	}
	principal := model.Principal{TenantID: state.CandidateApp.TenantID, ActorType: model.ActorTypeSystem, ActorID: "safe-rollout-controller"}
	service := s.appReleaseService()
	if _, err := service.AbortRelease(ctx, principal, state.CandidateApp, state.Candidate, true, reason); err != nil {
		return err
	}
	s.recordSafeRolloutReleaseStep(op, state.CandidateApp, "abort", model.ReleaseStepStatusCompleted, reason, state.Candidate.ID, nil)
	return s.restoreSafeZeroDowntimePreviousSpec(ctx, op, state, reason)
}

func (s *Service) restoreSafeZeroDowntimePreviousSpec(ctx context.Context, op model.Operation, state *safeRolloutState, reason string) error {
	if state == nil || !state.Enabled || !s.Config.KubectlApply {
		return nil
	}
	previous := state.PreviousApp
	scheduling, err := s.managedSchedulingConstraintsForApp(ctx, previous)
	if err != nil {
		return fmt.Errorf("resolve previous scheduling for safe rollout restore: %w", err)
	}
	applyCtx := withManagedAppApplySource(ctx, managedAppApplySourceOperation, op.ID)
	if err := s.applyManagedAppDesiredState(applyCtx, previous, scheduling); err != nil {
		s.recordSafeRolloutReleaseStep(op, state.CandidateApp, "restore_previous", model.ReleaseStepStatusFailed, "restore previous spec failed: "+err.Error(), state.Candidate.ID, nil)
		return fmt.Errorf("restore previous desired state after safe rollout failure: %w", err)
	}
	result := s.waitForManagedAppRolloutResultWithScheduling(ctx, previous, op.ID, scheduling)
	if err := result.Error(); err != nil {
		s.recordSafeRolloutReleaseStep(op, state.CandidateApp, "restore_previous", model.ReleaseStepStatusFailed, "restore previous rollout failed: "+err.Error(), state.Candidate.ID, map[string]any{
			"rollout_result": result,
		})
		return fmt.Errorf("wait previous desired state after safe rollout restore: %w", err)
	}
	s.recordSafeRolloutReleaseStep(op, state.CandidateApp, "restore_previous", model.ReleaseStepStatusCompleted, "previous stable desired state restored", state.Candidate.ID, map[string]any{
		"reason": reason,
	})
	return nil
}

func (s *Service) appReleaseService() releaseflow.AppReleaseService {
	return releaseflow.AppReleaseService{
		Store:            s.Store,
		ServiceURLForApp: func(ctx context.Context, app model.App) string { return controllerServiceURLForApp(app) },
	}
}

func (s *Service) releaseGateEvaluator() releaseflow.ReleaseGateEvaluator {
	return releaseflow.ReleaseGateEvaluator{}
}

func controllerServiceURLForApp(app model.App) string {
	port := 80
	if app.Route != nil && app.Route.ServicePort > 0 {
		port = app.Route.ServicePort
	} else if len(app.Spec.Ports) > 0 && app.Spec.Ports[0] > 0 {
		port = app.Spec.Ports[0]
	}
	return "http://" + runtime.RuntimeAppServiceName(app) + "." + runtime.NamespaceForTenant(app.TenantID) + ".svc.cluster.local:" + strconv.Itoa(port)
}

func (s *Service) recordSafeRolloutReleaseStep(op model.Operation, app model.App, phase, status, summary, releaseID string, payload map[string]any) {
	if s == nil || s.Store == nil {
		return
	}
	attempt, found, err := s.Store.FindReleaseAttemptForOperation(op.ID)
	if err != nil || !found {
		return
	}
	if payload == nil {
		payload = map[string]any{}
	}
	payload["phase"] = phase
	if strings.TrimSpace(releaseID) != "" {
		payload["app_release_id"] = releaseID
	}
	s.recordReleaseStepBestEffort(model.ReleaseStep{
		TenantID:         app.TenantID,
		ReleaseAttemptID: attempt.ID,
		OperationID:      op.ID,
		Type:             model.ReleaseStepTypeHealthCheck,
		Status:           status,
		Summary:          strings.TrimSpace(summary),
		Payload:          payload,
	})
}
