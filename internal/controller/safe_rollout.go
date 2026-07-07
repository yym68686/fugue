package controller

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/releaseflow"
	"fugue/internal/runtime"
)

type safeRolloutState struct {
	Enabled                bool
	PreviousApp            model.App
	CandidateApp           model.App
	StableRelease          model.AppRelease
	Candidate              model.AppRelease
	StableAlignmentAllowed bool
}

type safeRolloutPlan struct {
	Gate        model.AppReleaseGatePolicy
	Canary      model.AppRolloutCanarySpec
	CanarySteps []int
}

func (s *Service) prepareSafeZeroDowntimeRollout(ctx context.Context, op model.Operation, previous, candidate model.App, schedulingValues ...runtime.SchedulingConstraints) (*safeRolloutState, error) {
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
	scheduling := runtime.SchedulingConstraints{}
	if len(schedulingValues) > 0 {
		scheduling = schedulingValues[0]
	}
	if !s.safeRolloutCandidateChangesPodTemplate(previous, candidate, scheduling) {
		s.recordSafeRolloutReleaseStep(op, candidate, "candidate_create", model.ReleaseStepStatusSkipped, "safe rollout skipped: candidate pod template unchanged", stable.ID, map[string]any{
			"stable_release_id":     stable.ID,
			"previous_release_key":  strings.TrimSpace(s.Renderer.ManagedAppReleaseKey(s.Renderer.PrepareApp(previous), scheduling)),
			"candidate_release_key": strings.TrimSpace(s.Renderer.ManagedAppReleaseKey(s.Renderer.PrepareApp(candidate), scheduling)),
		})
		return nil, nil
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

func (s *Service) safeRolloutCandidateChangesPodTemplate(previous, candidate model.App, scheduling runtime.SchedulingConstraints) bool {
	previousKey := strings.TrimSpace(s.Renderer.ManagedAppReleaseKey(s.Renderer.PrepareApp(previous), scheduling))
	candidateKey := strings.TrimSpace(s.Renderer.ManagedAppReleaseKey(s.Renderer.PrepareApp(candidate), scheduling))
	if previousKey == "" || candidateKey == "" {
		return true
	}
	return previousKey != candidateKey
}

func (s *Service) completeSafeZeroDowntimeRollout(ctx context.Context, op model.Operation, state *safeRolloutState) error {
	if state == nil || !state.Enabled {
		return nil
	}
	service := s.appReleaseService()
	now := time.Now().UTC()
	candidate := state.Candidate
	revision := safeRolloutCandidateRevision(candidate.ID)
	candidate.UpstreamURL = s.controllerRevisionServiceURLForApp(ctx, state.CandidateApp, revision)
	candidate.DeploymentName = runtime.RuntimeAppResourceNameWithOptions(s.Renderer.PrepareApp(state.CandidateApp), runtime.RenderOptions{StrictDrain: s.Renderer.StrictDrain, Revision: revision})
	candidate.ServiceName = runtime.RuntimeAppServiceNameWithOptions(s.Renderer.PrepareApp(state.CandidateApp), runtime.RenderOptions{StrictDrain: s.Renderer.StrictDrain, Revision: revision})
	candidate.Status = model.AppReleaseStatusReady
	candidate.StatusReason = ""
	candidate.ReleaseMessage = "candidate rollout ready"
	candidate.ReadyAt = releaseflow.FirstNonNilTime(candidate.ReadyAt, &now)
	updated, err := s.Store.UpdateAppRelease(candidate)
	if err != nil {
		return fmt.Errorf("mark candidate release ready: %w", err)
	}
	state.Candidate = updated
	s.recordSafeRolloutReleaseStep(op, state.CandidateApp, "candidate_ready", model.ReleaseStepStatusCompleted, "candidate rollout ready", updated.ID, map[string]any{
		"upstream_url": updated.UpstreamURL,
	})

	plan := s.safeRolloutPlanForApp(state.CandidateApp)
	initialPolicy := plan.Gate
	initialPolicy.MinCandidateRequests = 0
	gate := s.releaseGateEvaluator().Evaluate(ctx, state.CandidateApp, state.Candidate, initialPolicy)
	if gate.Status == model.AppReleaseGateStatusFail {
		evidenceID := s.recordSafeRolloutGateFailureEvidence(op, state.CandidateApp, state.Candidate, gate, "candidate active gate failed")
		s.recordSafeRolloutReleaseStep(op, state.CandidateApp, "gate_check", model.ReleaseStepStatusFailed, "candidate gate failed", state.Candidate.ID, map[string]any{
			"gate":        gate,
			"evidence_id": evidenceID,
		})
		s.appendSafeRolloutAuditEvent(state.CandidateApp, "app.release.gate.fail", state.Candidate.ID, map[string]string{
			"operation_id": op.ID,
			"phase":        "gate_check",
			"evidence_id":  evidenceID,
		})
		_ = s.abortSafeZeroDowntimeRollout(ctx, op, state, "candidate gate failed: "+strings.Join(gate.Failures, "; "))
		return fmt.Errorf("safe zero downtime rollout gate failed: %s", strings.Join(gate.Failures, "; "))
	}
	s.recordSafeRolloutReleaseStep(op, state.CandidateApp, "gate_check", model.ReleaseStepStatusCompleted, "candidate gate passed", state.Candidate.ID, map[string]any{
		"gate": gate,
	})
	s.appendSafeRolloutAuditEvent(state.CandidateApp, "app.release.gate.pass", state.Candidate.ID, map[string]string{
		"operation_id": op.ID,
		"phase":        "gate_check",
	})

	principal := model.Principal{TenantID: state.CandidateApp.TenantID, ActorType: model.ActorTypeSystem, ActorID: "safe-rollout-controller"}
	observedCanaryTraffic := false
	if plan.Canary.Enabled && len(plan.CanarySteps) > 0 {
		for index, weight := range plan.CanarySteps {
			if weight <= 0 || weight >= 100 {
				continue
			}
			traffic, err := service.PromoteRelease(ctx, principal, state.CandidateApp, state.Candidate, weight)
			if err != nil {
				return fmt.Errorf("shift safe rollout canary to %d%%: %w", weight, err)
			}
			s.recordSafeRolloutReleaseStep(op, state.CandidateApp, "canary_shift", model.ReleaseStepStatusCompleted, fmt.Sprintf("candidate canary shifted to %d%%", weight), state.Candidate.ID, map[string]any{
				"candidate_weight":  weight,
				"stable_weight":     traffic.StableWeight,
				"traffic_policy_id": traffic.ID,
			})
			s.appendSafeRolloutAuditEvent(state.CandidateApp, "app.release.canary.shifted", state.Candidate.ID, map[string]string{
				"operation_id":     op.ID,
				"candidate_weight": fmt.Sprintf("%d", weight),
			})
			routePolicyUpdatedAt := time.Now().UTC()
			if !s.waitSafeRolloutCanaryEdgeRouteBundleApplied(ctx, op, state, weight, routePolicyUpdatedAt) {
				reason := fmt.Sprintf("candidate canary edge route bundle not applied at %d%%", weight)
				_ = s.abortSafeZeroDowntimeRollout(ctx, op, state, reason)
				return fmt.Errorf("safe zero downtime rollout canary edge route bundle wait failed at %d%%", weight)
			}
			observedCanaryTraffic = true
			if err := s.sleepSafeRolloutObservation(ctx, time.Duration(plan.Canary.MinObservationSeconds)*time.Second); err != nil {
				return err
			}
			gate = s.evaluateSafeRolloutCandidateCanaryGate(ctx, state, plan.Gate)
			if gate.Status == model.AppReleaseGateStatusFail {
				if safeRolloutCanaryGateOnlySampleDeficit(gate, plan.Gate) && safeRolloutHasLaterCanaryStep(plan.CanarySteps, index) {
					s.recordSafeRolloutReleaseStep(op, state.CandidateApp, "canary_gate", model.ReleaseStepStatusSkipped, fmt.Sprintf("candidate canary gate inconclusive at %d%%; continuing to next canary weight", weight), state.Candidate.ID, map[string]any{
						"candidate_weight": weight,
						"gate":             gate,
						"reason":           "candidate sample count below minimum",
					})
					s.appendSafeRolloutAuditEvent(state.CandidateApp, "app.release.gate.inconclusive", state.Candidate.ID, map[string]string{
						"operation_id":      op.ID,
						"phase":             "canary_gate",
						"candidate_weight":  fmt.Sprintf("%d", weight),
						"candidate_release": state.Candidate.ID,
						"reason":            "candidate sample count below minimum",
					})
					continue
				}
				evidenceID := s.recordSafeRolloutGateFailureEvidence(op, state.CandidateApp, state.Candidate, gate, fmt.Sprintf("candidate canary gate failed at %d%%", weight))
				s.recordSafeRolloutReleaseStep(op, state.CandidateApp, "canary_gate", model.ReleaseStepStatusFailed, fmt.Sprintf("candidate canary gate failed at %d%%", weight), state.Candidate.ID, map[string]any{
					"candidate_weight": weight,
					"gate":             gate,
					"evidence_id":      evidenceID,
				})
				s.appendSafeRolloutAuditEvent(state.CandidateApp, "app.release.gate.fail", state.Candidate.ID, map[string]string{
					"operation_id":      op.ID,
					"phase":             "canary_gate",
					"candidate_weight":  fmt.Sprintf("%d", weight),
					"evidence_id":       evidenceID,
					"candidate_release": state.Candidate.ID,
				})
				_ = s.abortSafeZeroDowntimeRollout(ctx, op, state, fmt.Sprintf("candidate canary gate failed at %d%%: %s", weight, strings.Join(gate.Failures, "; ")))
				return fmt.Errorf("safe zero downtime rollout canary gate failed at %d%%: %s", weight, strings.Join(gate.Failures, "; "))
			}
			s.recordSafeRolloutReleaseStep(op, state.CandidateApp, "canary_gate", model.ReleaseStepStatusCompleted, fmt.Sprintf("candidate canary gate passed at %d%%", weight), state.Candidate.ID, map[string]any{
				"candidate_weight": weight,
				"gate":             gate,
			})
			s.appendSafeRolloutAuditEvent(state.CandidateApp, "app.release.gate.pass", state.Candidate.ID, map[string]string{
				"operation_id":      op.ID,
				"phase":             "canary_gate",
				"candidate_weight":  fmt.Sprintf("%d", weight),
				"candidate_release": state.Candidate.ID,
			})
		}
	}
	finalPolicy := plan.Gate
	if !observedCanaryTraffic {
		finalPolicy.MinCandidateRequests = 0
	}
	if observedCanaryTraffic {
		gate = s.evaluateSafeRolloutCandidateCanaryGate(ctx, state, finalPolicy)
	} else {
		gate = s.releaseGateEvaluator().Evaluate(ctx, state.CandidateApp, state.Candidate, finalPolicy)
	}
	if gate.Status == model.AppReleaseGateStatusFail {
		evidenceID := s.recordSafeRolloutGateFailureEvidence(op, state.CandidateApp, state.Candidate, gate, "candidate final gate failed")
		s.recordSafeRolloutReleaseStep(op, state.CandidateApp, "final_gate", model.ReleaseStepStatusFailed, "candidate final gate failed", state.Candidate.ID, map[string]any{
			"gate":        gate,
			"evidence_id": evidenceID,
		})
		s.appendSafeRolloutAuditEvent(state.CandidateApp, "app.release.gate.fail", state.Candidate.ID, map[string]string{
			"operation_id": op.ID,
			"phase":        "final_gate",
			"evidence_id":  evidenceID,
		})
		_ = s.abortSafeZeroDowntimeRollout(ctx, op, state, "candidate final gate failed: "+strings.Join(gate.Failures, "; "))
		return fmt.Errorf("safe zero downtime rollout final gate failed: %s", strings.Join(gate.Failures, "; "))
	}
	s.recordSafeRolloutReleaseStep(op, state.CandidateApp, "final_gate", model.ReleaseStepStatusCompleted, "candidate final gate passed", state.Candidate.ID, map[string]any{
		"gate": gate,
	})
	traffic, err := service.PromoteRelease(ctx, principal, state.CandidateApp, state.Candidate, 100)
	if err != nil {
		return fmt.Errorf("promote candidate release after safe rollout gate: %w", err)
	}
	if promoted, err := s.Store.GetAppRelease(state.CandidateApp.TenantID, true, state.Candidate.ID); err == nil {
		state.Candidate = promoted
	}
	s.recordSafeRolloutReleaseStep(op, state.CandidateApp, "promote", model.ReleaseStepStatusCompleted, "candidate promoted to stable", state.Candidate.ID, map[string]any{
		"traffic_policy_id": traffic.ID,
	})
	s.appendSafeRolloutAuditEvent(state.CandidateApp, "app.release.promote", state.Candidate.ID, map[string]string{
		"operation_id": op.ID,
		"mode":         "safe_zero_downtime",
	})
	if !s.waitSafeRolloutEdgeRouteBundleApplied(ctx, op, state) {
		return nil
	}
	if !s.recheckSafeRolloutCandidateBeforeRetire(ctx, op, state, "pre_alignment_retire_gate") {
		return nil
	}
	state.StableAlignmentAllowed = true
	return nil
}

func (s *Service) waitSafeRolloutCanaryEdgeRouteBundleApplied(ctx context.Context, op model.Operation, state *safeRolloutState, weight int, since time.Time) bool {
	if state == nil || !state.Enabled {
		return true
	}
	observer := s.edgeBundleObserverForSafeRollout()
	observation, err := observer.WaitForSafeRolloutEdgeRouteBundle(ctx, state.CandidateApp, state.Candidate, since)
	payload := map[string]any{
		"candidate_weight": weight,
		"observation":      observation.Summary,
	}
	if err != nil {
		payload["error"] = err.Error()
		s.recordSafeRolloutReleaseStep(op, state.CandidateApp, "canary_edge_bundle_wait", model.ReleaseStepStatusFailed, fmt.Sprintf("candidate canary edge route bundle confirmation failed at %d%%", weight), state.Candidate.ID, payload)
		s.appendSafeRolloutAuditEvent(state.CandidateApp, "app.release.canary.blocked", state.Candidate.ID, map[string]string{
			"operation_id":     op.ID,
			"phase":            "canary_edge_bundle_wait",
			"candidate_weight": fmt.Sprintf("%d", weight),
			"reason":           err.Error(),
		})
		return false
	}
	status := model.ReleaseStepStatusCompleted
	summary := fmt.Sprintf("edge route bundle applied for candidate canary at %d%%", weight)
	if !observation.Ready {
		status = model.ReleaseStepStatusFailed
		summary = fmt.Sprintf("candidate canary edge route bundle not applied at %d%%", weight)
	}
	s.recordSafeRolloutReleaseStep(op, state.CandidateApp, "canary_edge_bundle_wait", status, summary, state.Candidate.ID, payload)
	if !observation.Ready {
		s.appendSafeRolloutAuditEvent(state.CandidateApp, "app.release.canary.blocked", state.Candidate.ID, map[string]string{
			"operation_id":     op.ID,
			"phase":            "canary_edge_bundle_wait",
			"candidate_weight": fmt.Sprintf("%d", weight),
			"required_nodes":   fmt.Sprintf("%d", observation.RequiredNodes),
			"ready_nodes":      fmt.Sprintf("%d", observation.ReadyNodes),
		})
	}
	return observation.Ready
}

func (s *Service) waitSafeRolloutEdgeRouteBundleApplied(ctx context.Context, op model.Operation, state *safeRolloutState) bool {
	if state == nil || !state.Enabled {
		return true
	}
	observer := s.edgeBundleObserverForSafeRollout()
	since := time.Now().UTC()
	if state.Candidate.PromotedAt != nil {
		since = state.Candidate.PromotedAt.UTC()
	}
	observation, err := observer.WaitForSafeRolloutEdgeRouteBundle(ctx, state.CandidateApp, state.Candidate, since)
	payload := map[string]any{
		"observation": observation.Summary,
	}
	if err != nil {
		payload["error"] = err.Error()
		s.recordSafeRolloutReleaseStep(op, state.CandidateApp, "edge_bundle_wait", model.ReleaseStepStatusSkipped, "stable alignment paused: edge route bundle confirmation failed", state.Candidate.ID, payload)
		s.appendSafeRolloutAuditEvent(state.CandidateApp, "app.release.previous_retire.paused", state.Candidate.ID, map[string]string{
			"operation_id": op.ID,
			"phase":        "edge_bundle_wait",
			"reason":       err.Error(),
		})
		return false
	}
	status := model.ReleaseStepStatusCompleted
	summary := "edge route bundle applied before previous retire"
	if !observation.Ready {
		status = model.ReleaseStepStatusSkipped
		summary = "stable alignment paused: waiting for edge route bundle application"
	}
	s.recordSafeRolloutReleaseStep(op, state.CandidateApp, "edge_bundle_wait", status, summary, state.Candidate.ID, payload)
	if !observation.Ready {
		s.appendSafeRolloutAuditEvent(state.CandidateApp, "app.release.previous_retire.paused", state.Candidate.ID, map[string]string{
			"operation_id":   op.ID,
			"phase":          "edge_bundle_wait",
			"required_nodes": fmt.Sprintf("%d", observation.RequiredNodes),
			"ready_nodes":    fmt.Sprintf("%d", observation.ReadyNodes),
		})
	}
	return observation.Ready
}

func (s *Service) edgeBundleObserverForSafeRollout() safeRolloutEdgeBundleObserver {
	if s != nil && s.safeRolloutEdgeBundleObserver != nil {
		return s.safeRolloutEdgeBundleObserver
	}
	var sleep func(context.Context, time.Duration) error
	if s != nil {
		sleep = s.safeRolloutSleep
	}
	return storeSafeRolloutEdgeBundleObserver{Store: s.Store, Sleep: sleep}
}

func (s *Service) recheckSafeRolloutCandidateBeforeRetire(ctx context.Context, op model.Operation, state *safeRolloutState, phase string) bool {
	if state == nil || !state.Enabled {
		return true
	}
	gate := s.evaluateSafeRolloutCandidateRetireGate(ctx, state.CandidateApp, state.Candidate, s.safeRolloutPlanForApp(state.CandidateApp).Gate)
	status := model.ReleaseStepStatusCompleted
	summary := "candidate retire gate passed"
	if gate.Status == model.AppReleaseGateStatusFail {
		status = model.ReleaseStepStatusSkipped
		summary = "previous retire paused: candidate retire gate failed"
	}
	s.recordSafeRolloutReleaseStep(op, state.CandidateApp, phase, status, summary, state.Candidate.ID, map[string]any{
		"gate": gate,
	})
	if gate.Status == model.AppReleaseGateStatusFail {
		s.appendSafeRolloutAuditEvent(state.CandidateApp, "app.release.previous_retire.paused", state.Candidate.ID, map[string]string{
			"operation_id": op.ID,
			"phase":        phase,
			"reason":       strings.Join(gate.Failures, "; "),
		})
		return false
	}
	return true
}

func (s *Service) evaluateSafeRolloutCandidateRetireGate(ctx context.Context, app model.App, release model.AppRelease, policy model.AppReleaseGatePolicy) model.AppReleaseGateResult {
	evaluator := s.releaseGateEvaluator()
	window := time.Duration(policy.WindowSeconds) * time.Second
	if window <= 0 || window > time.Minute {
		window = time.Minute
	}
	gate := model.AppReleaseGateResult{
		Status:      model.AppReleaseGateStatusPass,
		ReleaseID:   release.ID,
		Role:        release.Role,
		Window:      window.String(),
		Evidence:    []string{},
		Warnings:    []string{},
		Failures:    []string{},
		Metrics:     map[string]any{},
		EvaluatedAt: time.Now().UTC(),
	}
	if len(policy.Probes) > 0 {
		gate.ProbeResults = evaluator.RunProbes(ctx, release, policy.Probes)
		for _, result := range gate.ProbeResults {
			if result.Status == model.AppReleaseGateStatusFail {
				gate.Failures = append(gate.Failures, fmt.Sprintf("probe %s failed: %s", firstNonEmptyString(result.Name, result.Path), result.Error))
			}
		}
	}
	if s.releaseGateMetricsQuerier != nil {
		if metrics, err := s.releaseGateMetricsQuerier.QueryReleaseGateMetrics(ctx, app.ID, release.ID, release.Role, window); err == nil {
			passivePolicy := policy
			passivePolicy.MinCandidateRequests = 0
			gate.Metrics = metrics
			gate.Evidence = append(gate.Evidence, releaseflow.ReleaseGateMetricEvidence(metrics)...)
			gate.Failures = append(gate.Failures, releaseflow.ReleaseGateMetricFailures(metrics, passivePolicy)...)
		} else {
			gate.Warnings = append(gate.Warnings, "short-window passive release metrics unavailable: "+err.Error())
		}
	} else {
		gate.Warnings = append(gate.Warnings, "short-window passive release metrics unavailable: metrics querier is not configured")
	}
	if len(gate.Failures) > 0 {
		gate.Status = model.AppReleaseGateStatusFail
	}
	return gate
}

func (s *Service) evaluateSafeRolloutCandidateCanaryGate(ctx context.Context, state *safeRolloutState, policy model.AppReleaseGatePolicy) model.AppReleaseGateResult {
	window := time.Duration(policy.WindowSeconds) * time.Second
	if window <= 0 {
		window = releaseflow.DefaultAppReleaseGateWindow
	}

	evaluator := s.releaseGateEvaluator()
	gate := model.AppReleaseGateResult{
		Status:      model.AppReleaseGateStatusPass,
		ReleaseID:   state.Candidate.ID,
		Role:        state.Candidate.Role,
		Window:      window.String(),
		Evidence:    []string{},
		Warnings:    []string{},
		Failures:    []string{},
		Metrics:     map[string]any{},
		EvaluatedAt: time.Now().UTC(),
	}
	if len(policy.Probes) > 0 {
		gate.ProbeResults = evaluator.RunProbes(ctx, state.Candidate, policy.Probes)
		for _, result := range gate.ProbeResults {
			if result.Status == model.AppReleaseGateStatusFail {
				gate.Failures = append(gate.Failures, fmt.Sprintf("probe %s failed: %s", firstNonEmptyString(result.Name, result.Path), result.Error))
			}
		}
	}
	if s.releaseGateMetricsQuerier == nil {
		gate.Warnings = append(gate.Warnings, "comparative release metrics unavailable: metrics querier is not configured")
		gate.Failures = append(gate.Failures, "comparative release metrics unavailable: metrics querier is not configured")
		gate.Status = model.AppReleaseGateStatusFail
		return gate
	}

	candidateMetrics, err := s.querySafeRolloutReleaseComparisonMetrics(ctx, state.CandidateApp.ID, state.Candidate.ID, state.Candidate.Role, window)
	if err != nil {
		gate.Warnings = append(gate.Warnings, "comparative candidate release metrics unavailable: "+err.Error())
		gate.Failures = append(gate.Failures, "comparative candidate release metrics unavailable: "+err.Error())
		gate.Status = model.AppReleaseGateStatusFail
		return gate
	}
	gate.Metrics = candidateMetrics
	gate.Evidence = append(gate.Evidence, releaseflow.ReleaseGateMetricEvidence(candidateMetrics)...)
	gate.Failures = append(gate.Failures, releaseflow.ReleaseGateMetricFailures(candidateMetrics, policy)...)

	stableMetrics, err := s.querySafeRolloutStableComparisonMetrics(ctx, state, window)
	if err != nil {
		gate.Warnings = append(gate.Warnings, "comparative stable release metrics unavailable: "+err.Error())
		gate.Failures = append(gate.Failures, "comparative stable release metrics unavailable: "+err.Error())
		gate.Status = model.AppReleaseGateStatusFail
		return gate
	}
	comparison := releaseflow.BuildReleaseGateComparisonMetrics(candidateMetrics, stableMetrics)
	if gate.Metrics == nil {
		gate.Metrics = map[string]any{}
	}
	gate.Metrics["stable_request_count"] = releaseflow.FloatMetric(stableMetrics, "request_count")
	gate.Metrics["stable_error_5xx_rate"] = releaseflow.FloatMetric(stableMetrics, "error_5xx_rate")
	gate.Metrics["stable_edge_upstream_error_rate"] = releaseflow.FloatMetric(stableMetrics, "edge_upstream_error_rate")
	gate.Metrics["stable_status_2xx_rate"] = releaseflow.FloatMetric(stableMetrics, "status_2xx_rate")
	gate.Metrics["stable_status_4xx_rate"] = releaseflow.FloatMetric(stableMetrics, "status_4xx_rate")
	gate.Metrics["stable_p95_ttfb_ms"] = releaseflow.FloatMetric(stableMetrics, "p95_ttfb_ms")
	gate.Metrics["stable_p99_duration_ms"] = releaseflow.FloatMetric(stableMetrics, "p99_duration_ms")
	gate.Metrics["comparison"] = comparison
	gate.Evidence = append(gate.Evidence, releaseflow.ReleaseGateComparisonEvidence(comparison)...)
	gate.Failures = append(gate.Failures, releaseflow.ReleaseGateComparisonFailures(comparison, policy)...)
	if len(gate.Failures) > 0 {
		gate.Status = model.AppReleaseGateStatusFail
	}
	return gate
}

func (s *Service) querySafeRolloutStableComparisonMetrics(ctx context.Context, state *safeRolloutState, window time.Duration) (map[string]any, error) {
	return s.querySafeRolloutReleaseComparisonMetrics(ctx, state.CandidateApp.ID, state.StableRelease.ID, model.AppReleaseRoleStable, window)
}

func (s *Service) querySafeRolloutReleaseComparisonMetrics(ctx context.Context, appID, releaseID, releaseRole string, window time.Duration) (map[string]any, error) {
	if rawQuerier, ok := s.releaseGateMetricsQuerier.(releaseflow.ReleaseGateRawMetricsQuerier); ok {
		if metrics, err := rawQuerier.QueryReleaseGateRawMetrics(ctx, appID, releaseID, releaseRole, window); err == nil {
			return metrics, nil
		} else {
			return nil, err
		}
	}
	return s.releaseGateMetricsQuerier.QueryReleaseGateMetrics(ctx, appID, releaseID, releaseRole, window)
}

func (s *Service) finalizeSafeZeroDowntimePreviousRetire(ctx context.Context, op model.Operation, state *safeRolloutState) {
	if state == nil || !state.Enabled || !state.StableAlignmentAllowed {
		return
	}
	if !s.recheckSafeRolloutCandidateBeforeRetire(ctx, op, state, "pre_previous_retire_gate") {
		return
	}
	policy, err := s.Store.GetAppTrafficPolicy(state.CandidateApp.TenantID, true, state.CandidateApp.ID)
	if err != nil {
		s.recordSafeRolloutReleaseStep(op, state.CandidateApp, "previous_retire", model.ReleaseStepStatusSkipped, "previous retire paused: traffic policy unavailable", state.Candidate.ID, map[string]any{"error": err.Error()})
		return
	}
	if strings.TrimSpace(policy.StableReleaseID) != strings.TrimSpace(state.Candidate.ID) ||
		strings.TrimSpace(policy.CandidateReleaseID) != "" ||
		policy.StableWeight != 100 ||
		policy.CandidateWeight != 0 {
		s.recordSafeRolloutReleaseStep(op, state.CandidateApp, "previous_retire", model.ReleaseStepStatusSkipped, "previous retire paused: previous release may still receive traffic", state.Candidate.ID, map[string]any{
			"traffic_policy": policy,
		})
		s.appendSafeRolloutAuditEvent(state.CandidateApp, "app.release.previous_retire.paused", state.Candidate.ID, map[string]string{
			"operation_id": op.ID,
			"phase":        "previous_retire",
			"reason":       "traffic policy still references non-stable traffic",
		})
		return
	}
	previous, err := s.Store.GetAppRelease(state.CandidateApp.TenantID, true, state.StableRelease.ID)
	if err != nil {
		s.recordSafeRolloutReleaseStep(op, state.CandidateApp, "previous_retire", model.ReleaseStepStatusSkipped, "previous retire paused: previous release unavailable", state.Candidate.ID, map[string]any{"error": err.Error()})
		return
	}
	metrics, ok := s.querySafeRolloutDrainMetrics(ctx, op, state.CandidateApp, previous, state.Candidate.PromotedAt)
	if !ok {
		return
	}
	retired, err := s.appReleaseService().RetireRelease(ctx, state.CandidateApp, previous, "safe rollout previous stable drained")
	if err != nil {
		s.recordSafeRolloutReleaseStep(op, state.CandidateApp, "previous_retire", model.ReleaseStepStatusSkipped, "previous retire paused: release record update failed", state.Candidate.ID, map[string]any{"error": err.Error(), "drain_metrics": metrics.Summary})
		return
	}
	s.recordSafeRolloutReleaseStep(op, state.CandidateApp, "previous_retire", model.ReleaseStepStatusCompleted, "previous stable retired after drain metrics reached zero", retired.ID, map[string]any{
		"drain_metrics": metrics.Summary,
	})
	s.appendSafeRolloutAuditEvent(state.CandidateApp, "app.release.previous_retired", retired.ID, map[string]string{
		"operation_id":        op.ID,
		"candidate_release":   state.Candidate.ID,
		"active_connections":  fmt.Sprintf("%d", metrics.ActiveConnections),
		"max_active_requests": fmt.Sprintf("%d", metrics.MaxActiveConnections),
	})
}

func (s *Service) querySafeRolloutDrainMetrics(ctx context.Context, op model.Operation, app model.App, previous model.AppRelease, promotedAt *time.Time) (safeRolloutDrainMetrics, bool) {
	if s == nil || s.safeRolloutDrainMetricsQuerier == nil {
		s.recordSafeRolloutReleaseStep(op, app, "previous_retire", model.ReleaseStepStatusSkipped, "previous retire paused: drain metrics querier is not configured", previous.ID, nil)
		s.appendSafeRolloutAuditEvent(app, "app.release.previous_retire.paused", previous.ID, map[string]string{
			"operation_id": op.ID,
			"phase":        "previous_retire",
			"reason":       "drain metrics querier is not configured",
		})
		return safeRolloutDrainMetrics{}, false
	}
	since := time.Now().UTC().Add(-safeRolloutDrainMetricsLookback)
	if promotedAt != nil {
		since = promotedAt.UTC()
	}
	metrics, err := s.safeRolloutDrainMetricsQuerier.QuerySafeRolloutDrainMetrics(ctx, app, previous, since)
	if err != nil {
		s.recordSafeRolloutReleaseStep(op, app, "previous_retire", model.ReleaseStepStatusSkipped, "previous retire paused: drain metrics unavailable", previous.ID, map[string]any{"error": err.Error()})
		s.appendSafeRolloutAuditEvent(app, "app.release.previous_retire.paused", previous.ID, map[string]string{
			"operation_id": op.ID,
			"phase":        "previous_retire",
			"reason":       "drain metrics unavailable",
		})
		return safeRolloutDrainMetrics{}, false
	}
	if safeRolloutDrainMetricsCanUseKubernetesRevisionAbsence(metrics) {
		inactive, summary, err := s.safeRolloutPreviousRevisionInactive(ctx, app, previous)
		if metrics.Summary == nil {
			metrics.Summary = map[string]any{}
		}
		for key, value := range summary {
			metrics.Summary[key] = value
		}
		if err != nil {
			metrics.Summary["kubernetes_revision_check_error"] = err.Error()
		} else if inactive {
			metrics.Ready = true
			metrics.Summary["ready"] = true
			metrics.Summary["drain_source"] = "kubernetes_previous_revision_inactive"
			return metrics, true
		}
	}
	if !metrics.Ready || metrics.ActiveConnections > 0 {
		s.recordSafeRolloutReleaseStep(op, app, "previous_retire", model.ReleaseStepStatusSkipped, "previous retire paused: active connections have not drained to zero", previous.ID, map[string]any{
			"drain_metrics": metrics.Summary,
		})
		s.appendSafeRolloutAuditEvent(app, "app.release.previous_retire.paused", previous.ID, map[string]string{
			"operation_id":       op.ID,
			"phase":              "previous_retire",
			"reason":             "active connections have not drained to zero",
			"active_connections": fmt.Sprintf("%d", metrics.ActiveConnections),
		})
		return metrics, false
	}
	return metrics, true
}

func safeRolloutDrainMetricsCanUseKubernetesRevisionAbsence(metrics safeRolloutDrainMetrics) bool {
	return !metrics.Ready &&
		metrics.ActiveConnections == 0 &&
		metrics.SampleCount == 0 &&
		metrics.FinalCount == 0
}

func (s *Service) safeRolloutPreviousRevisionInactive(ctx context.Context, app model.App, previous model.AppRelease) (bool, map[string]any, error) {
	deploymentName := strings.TrimSpace(previous.DeploymentName)
	summary := map[string]any{
		"previous_deployment": deploymentName,
	}
	if deploymentName == "" {
		summary["kubernetes_revision_check"] = "missing_previous_deployment_name"
		return false, summary, nil
	}
	currentStableName := runtime.RuntimeAppResourceNameWithOptions(s.Renderer.PrepareApp(app), runtime.RenderOptions{StrictDrain: s.Renderer.StrictDrain})
	if strings.TrimSpace(currentStableName) != "" && strings.EqualFold(deploymentName, currentStableName) {
		summary["kubernetes_revision_check"] = "previous_deployment_is_current_stable"
		return false, summary, nil
	}
	client, err := s.kubeClient()
	if err != nil {
		return false, summary, err
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	deployment, found, err := client.getDeployment(ctx, namespace, deploymentName)
	if err != nil {
		return false, summary, err
	}
	summary["previous_deployment_found"] = found
	if !found {
		summary["kubernetes_revision_check"] = "previous_deployment_not_found"
		return true, summary, nil
	}
	desiredReplicas := 0
	if deployment.Spec.Replicas != nil {
		desiredReplicas = *deployment.Spec.Replicas
	}
	summary["previous_deployment_desired_replicas"] = desiredReplicas
	summary["previous_deployment_status_replicas"] = deployment.Status.Replicas
	selector := safeRolloutLabelSelectorFromMap(deployment.Spec.Template.Metadata.Labels)
	pods, err := client.listPodsBySelector(ctx, namespace, selector)
	if err != nil {
		return false, summary, err
	}
	activePods := 0
	for _, pod := range pods {
		if strings.TrimSpace(pod.Metadata.DeletionTimestamp) != "" {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(pod.Status.Phase)) {
		case "succeeded", "failed":
			continue
		default:
			activePods++
		}
	}
	summary["previous_deployment_pods"] = len(pods)
	summary["previous_deployment_active_pods"] = activePods
	if desiredReplicas == 0 && deployment.Status.Replicas == 0 && activePods == 0 {
		summary["kubernetes_revision_check"] = "previous_deployment_scaled_to_zero"
		return true, summary, nil
	}
	summary["kubernetes_revision_check"] = "previous_deployment_still_present"
	return false, summary, nil
}

func safeRolloutLabelSelectorFromMap(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		if strings.TrimSpace(key) != "" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, strings.TrimSpace(key)+"="+strings.TrimSpace(labels[key]))
	}
	return strings.Join(parts, ",")
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
	s.appendSafeRolloutAuditEvent(state.CandidateApp, "app.release.abort.auto", state.Candidate.ID, map[string]string{
		"operation_id": op.ID,
		"reason":       reason,
	})
	if s.restoreSafeRolloutPreviousSpec != nil {
		return s.restoreSafeRolloutPreviousSpec(ctx, op, state, reason)
	}
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
		ServiceURLForApp: s.controllerServiceURLForApp,
	}
}

func safeRolloutCandidateRevision(releaseID string) runtime.AppRevisionRenderOptions {
	return runtime.AppRevisionRenderOptions{Role: runtime.AppRevisionRoleCandidate, ReleaseID: strings.TrimSpace(releaseID)}
}

func (s *Service) applySafeZeroDowntimeCandidateRevision(ctx context.Context, op model.Operation, state *safeRolloutState, scheduling runtime.SchedulingConstraints, postgresPlacements map[string][]runtime.SchedulingConstraints) error {
	if state == nil || !state.Enabled {
		return nil
	}
	client, err := s.kubeClient()
	if err != nil {
		return fmt.Errorf("initialize kubernetes candidate revision client: %w", err)
	}
	app := state.CandidateApp
	if normalizedApp, changed := s.normalizeManagedAppRuntimeImageRefs(app); changed {
		app = normalizedApp
		state.CandidateApp = normalizedApp
	}
	if err := validateManagedAppDeployableImage(app); err != nil {
		return err
	}
	app = s.appWithResolvedLaunchOverride(ctx, app)
	app = s.Renderer.PrepareApp(app)
	state.CandidateApp = app
	revision := safeRolloutCandidateRevision(state.Candidate.ID)
	objects := s.Renderer.BuildManagedAppRevisionChildObjects(app, scheduling, postgresPlacements, nil, revision)
	if err := client.applyObjects(ctx, objects); err != nil {
		return fmt.Errorf("apply candidate revision objects: %w", err)
	}
	if err := client.replaceObjectSpecsByKind(ctx, objects, "apps/v1", "Deployment"); err != nil {
		return fmt.Errorf("replace candidate revision deployment spec: %w", err)
	}
	s.recordSafeRolloutReleaseStep(op, app, "candidate_apply", model.ReleaseStepStatusCompleted, "candidate revision desired state applied", state.Candidate.ID, map[string]any{
		"deployment_name": runtime.RuntimeAppResourceNameWithOptions(app, runtime.RenderOptions{StrictDrain: s.Renderer.StrictDrain, Revision: revision}),
		"service_name":    runtime.RuntimeAppServiceNameWithOptions(app, runtime.RenderOptions{StrictDrain: s.Renderer.StrictDrain, Revision: revision}),
	})
	return nil
}

func (s *Service) releaseGateEvaluator() releaseflow.ReleaseGateEvaluator {
	return releaseflow.ReleaseGateEvaluator{
		MetricsQuerier: s.releaseGateMetricsQuerier,
		HTTPClient:     s.releaseGateHTTPClient,
	}
}

func (s *Service) controllerServiceURLForApp(ctx context.Context, app model.App) string {
	if s != nil && s.serviceURLForApp != nil {
		if url := strings.TrimSpace(s.serviceURLForApp(ctx, app)); url != "" {
			return url
		}
	}
	return controllerServiceURLForApp(app)
}

func (s *Service) controllerRevisionServiceURLForApp(ctx context.Context, app model.App, revision runtime.AppRevisionRenderOptions) string {
	if s != nil && s.serviceURLForApp != nil {
		if url := strings.TrimSpace(s.serviceURLForApp(ctx, app)); url != "" {
			return url
		}
	}
	return s.Renderer.AppRevisionServiceURL(app, revision)
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

func controllerReleaseGateMetricsQuerier(cfg config.ControllerConfig) releaseflow.ReleaseGateMetricsQuerier {
	cfg.Observability = cfg.Observability.Normalize()
	if strings.TrimSpace(cfg.Observability.ClickHouseDSN) == "" {
		return nil
	}
	return releaseflow.ClickHouseReleaseGateMetricsQuerier{
		DSN:             cfg.Observability.ClickHouseDSN,
		MaxPayloadBytes: cfg.Observability.ClickHouseQueryMaxPayloadBytes,
	}
}

func (s *Service) safeRolloutPlanForApp(app model.App) safeRolloutPlan {
	policy := releaseflow.ReleaseGateEvaluator{}.NormalizePolicy(nil)
	canary := model.AppRolloutCanarySpec{Enabled: false, InitialWeight: 0, MaxWeight: 100}
	if continuity := model.NormalizeAppContinuityPolicy(app.Spec.Continuity); continuity != nil && continuity.ZeroDowntime != nil {
		if continuity.ZeroDowntime.Gate != nil {
			policy = releaseflow.ReleaseGateEvaluator{}.NormalizePolicy(continuity.ZeroDowntime.Gate)
		}
		if continuity.ZeroDowntime.Canary != nil {
			canary = *continuity.ZeroDowntime.Canary
		}
	}
	return safeRolloutPlan{Gate: policy, Canary: canary, CanarySteps: safeRolloutCanarySteps(canary)}
}

func safeRolloutCanarySteps(canary model.AppRolloutCanarySpec) []int {
	if !canary.Enabled {
		return nil
	}
	seen := map[int]struct{}{}
	steps := []int{}
	add := func(weight int) {
		if weight < 0 {
			weight = 0
		}
		if canary.MaxWeight > 0 && weight > canary.MaxWeight {
			weight = canary.MaxWeight
		}
		if weight > 100 {
			weight = 100
		}
		if _, ok := seen[weight]; ok {
			return
		}
		seen[weight] = struct{}{}
		steps = append(steps, weight)
	}
	add(canary.InitialWeight)
	for _, weight := range canary.StepWeights {
		add(weight)
	}
	sort.Ints(steps)
	return steps
}

func safeRolloutHasLaterCanaryStep(steps []int, index int) bool {
	for nextIndex, weight := range steps {
		if nextIndex <= index {
			continue
		}
		if weight > 0 && weight < 100 {
			return true
		}
	}
	return false
}

func safeRolloutCanaryGateOnlySampleDeficit(gate model.AppReleaseGateResult, policy model.AppReleaseGatePolicy) bool {
	if policy.MinCandidateRequests <= 0 {
		return false
	}
	if releaseflow.FloatMetric(gate.Metrics, "request_count") >= float64(policy.MinCandidateRequests) {
		return false
	}
	if len(gate.Failures) == 0 {
		return false
	}
	for _, failure := range gate.Failures {
		if strings.Contains(failure, "request count") && strings.Contains(failure, "below minimum") {
			continue
		}
		return false
	}
	return true
}

func (s *Service) sleepSafeRolloutObservation(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	if s != nil && s.safeRolloutSleep != nil {
		return s.safeRolloutSleep(ctx, delay)
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (s *Service) recordSafeRolloutGateFailureEvidence(op model.Operation, app model.App, release model.AppRelease, gate model.AppReleaseGateResult, summary string) string {
	message := strings.Join(gate.Failures, "; ")
	if message == "" {
		message = "release gate failed"
	}
	return s.recordOperationEvidenceBestEffort(model.OperationEvidence{
		TenantID:        app.TenantID,
		ProjectID:       app.ProjectID,
		AppID:           app.ID,
		OperationID:     op.ID,
		Type:            model.OperationEvidenceTypeAppReleaseGateFailure,
		Source:          model.OperationEvidenceSourceController,
		Severity:        model.OperationEvidenceSeverityError,
		Confidence:      model.OperationEvidenceConfidenceEvidenceBacked,
		SubjectKind:     "app_release",
		SubjectName:     release.ID,
		Summary:         strings.TrimSpace(summary),
		Message:         message,
		RedactionStatus: model.OperationEvidenceRedactionRedacted,
		Payload: map[string]any{
			"release_id":   release.ID,
			"release_role": release.Role,
			"gate":         gate,
		},
	})
}

func (s *Service) appendSafeRolloutAuditEvent(app model.App, action, releaseID string, metadata map[string]string) {
	if s == nil || s.Store == nil {
		return
	}
	if metadata == nil {
		metadata = map[string]string{}
	}
	metadata["app_id"] = app.ID
	if releaseID != "" {
		metadata["app_release_id"] = releaseID
	}
	if err := s.Store.AppendAuditEvent(model.AuditEvent{
		TenantID:   app.TenantID,
		ActorType:  model.ActorTypeSystem,
		ActorID:    "safe-rollout-controller",
		Action:     strings.TrimSpace(action),
		TargetType: "app_release",
		TargetID:   releaseID,
		Metadata:   metadata,
	}); err != nil && s.Logger != nil {
		s.Logger.Printf("append safe rollout audit event failed app=%s release=%s action=%s: %v", app.ID, releaseID, action, err)
	}
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
