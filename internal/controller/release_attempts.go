package controller

import (
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/releaseflow"
)

func (s *Service) createImageTrackingReleaseAttemptBestEffort(app model.App, tracking model.AppImageTracking, op model.Operation, digest, triggerType, summary string) *model.ReleaseAttempt {
	if s == nil || s.Store == nil {
		return nil
	}
	actorID := strings.TrimSpace(s.Config.LeaderElectionIdentity)
	if actorID == "" {
		actorID = imageTrackingControllerPodName()
	}
	attempt, err := s.Store.CreateReleaseAttempt(model.ReleaseAttempt{
		TenantID:          app.TenantID,
		ProjectID:         app.ProjectID,
		AppID:             app.ID,
		TriggerType:       triggerType,
		TriggerActorType:  model.ReleaseAttemptActorSystem,
		TriggerActorID:    actorID,
		SourceOperationID: op.ID,
		RootOperationID:   op.ID,
		ImageRef:          tracking.ImageRef,
		TargetDigest:      digest,
		PreviousDigest:    tracking.LastDeployedDigest,
		Status:            model.ReleaseAttemptStatusImporting,
		Confidence:        model.OperationEvidenceConfidenceEvidenceBacked,
		Summary:           strings.TrimSpace(summary),
	})
	if err != nil {
		if s.Logger != nil {
			s.Logger.Printf("create image tracking release attempt failed app=%s op=%s: %v", app.ID, op.ID, err)
		}
		return nil
	}
	now := time.Now().UTC()
	s.recordReleaseStepBestEffort(model.ReleaseStep{
		TenantID:         app.TenantID,
		ReleaseAttemptID: attempt.ID,
		OperationID:      op.ID,
		Type:             model.ReleaseStepTypeTriggerReceived,
		Status:           model.ReleaseStepStatusCompleted,
		Summary:          strings.TrimSpace(summary),
		FinishedAt:       &now,
	})
	s.recordReleaseStepBestEffort(model.ReleaseStep{
		TenantID:         app.TenantID,
		ReleaseAttemptID: attempt.ID,
		OperationID:      op.ID,
		Type:             model.ReleaseStepTypeImageTrackingCheck,
		Status:           model.ReleaseStepStatusCompleted,
		Summary:          "tracked digest changed",
		FinishedAt:       &now,
		Payload: map[string]any{
			"image_ref":       tracking.ImageRef,
			"observed_digest": digest,
			"previous_digest": tracking.LastDeployedDigest,
			"tracking_id":     tracking.ID,
			"trigger_type":    triggerType,
		},
	})
	s.recordReleaseStepBestEffort(model.ReleaseStep{
		TenantID:         app.TenantID,
		ReleaseAttemptID: attempt.ID,
		OperationID:      op.ID,
		Type:             model.ReleaseStepTypeImageImport,
		Status:           model.ReleaseStepStatusPending,
		Summary:          "image import queued",
	})
	return &attempt
}

func (s *Service) recordReleaseStepBestEffort(step model.ReleaseStep) string {
	if s == nil || s.Store == nil {
		return ""
	}
	recorded, err := s.Store.RecordReleaseStep(step)
	if err != nil {
		if s.Logger != nil {
			s.Logger.Printf("record release step failed attempt=%s type=%s operation=%s: %v", step.ReleaseAttemptID, step.Type, step.OperationID, err)
		}
		return ""
	}
	return recorded.ID
}

func (s *Service) updateReleaseAttemptBestEffort(attempt model.ReleaseAttempt) {
	if s == nil || s.Store == nil || strings.TrimSpace(attempt.ID) == "" {
		return
	}
	attempt.UpdatedAt = time.Now().UTC()
	if _, err := s.Store.UpdateReleaseAttempt(attempt); err != nil && s.Logger != nil {
		s.Logger.Printf("update release attempt failed attempt=%s status=%s: %v", attempt.ID, attempt.Status, err)
	}
}

func (s *Service) markReleaseAttemptOperationRunning(op model.Operation, app model.App) {
	attempt, found, err := s.Store.FindReleaseAttemptForOperation(op.ID)
	if err != nil || !found {
		return
	}
	switch op.Type {
	case model.OperationTypeImport:
		attempt.Status = model.ReleaseAttemptStatusImporting
	case model.OperationTypeDeploy:
		attempt.Status = model.ReleaseAttemptStatusDeploying
	default:
		return
	}
	stepType := model.ReleaseStepTypeDeployApply
	if op.Type == model.OperationTypeImport {
		stepType = model.ReleaseStepTypeImageImport
	}
	s.updateReleaseAttemptBestEffort(attempt)
	s.recordReleaseStepBestEffort(model.ReleaseStep{
		TenantID:         app.TenantID,
		ReleaseAttemptID: attempt.ID,
		OperationID:      op.ID,
		Type:             stepType,
		Status:           model.ReleaseStepStatusRunning,
		Summary:          "operation started",
	})
}

func (s *Service) markReleaseAttemptRolloutWaiting(op model.Operation, app model.App) {
	attempt, found, err := s.Store.FindReleaseAttemptForOperation(op.ID)
	if err != nil || !found {
		return
	}
	attempt.Status = model.ReleaseAttemptStatusRollingOut
	s.updateReleaseAttemptBestEffort(attempt)
	s.recordReleaseStepBestEffort(model.ReleaseStep{
		TenantID:         app.TenantID,
		ReleaseAttemptID: attempt.ID,
		OperationID:      op.ID,
		Type:             model.ReleaseStepTypeRolloutWait,
		Status:           model.ReleaseStepStatusRunning,
		Summary:          "waiting for rollout",
	})
}

func (s *Service) completeReleaseAttemptForOperation(op model.Operation, app model.App, summary string) {
	attempt, found, err := s.Store.FindReleaseAttemptForOperation(op.ID)
	if err != nil || !found {
		return
	}
	now := time.Now().UTC()
	attempt.Status = model.ReleaseAttemptStatusCompleted
	attempt.Confidence = model.OperationEvidenceConfidenceConfirmed
	attempt.Summary = strings.TrimSpace(summary)
	attempt.FinishedAt = &now
	s.updateReleaseAttemptBestEffort(attempt)
	if digest := completedOperationDigest(op); digest != "" {
		s.recordReleaseStepBestEffort(model.ReleaseStep{
			TenantID:         app.TenantID,
			ReleaseAttemptID: attempt.ID,
			OperationID:      op.ID,
			Type:             model.ReleaseStepTypeMarkDeployedDigest,
			Status:           model.ReleaseStepStatusCompleted,
			Summary:          "marked tracked digest as deployed",
			FinishedAt:       &now,
			Payload: map[string]any{
				"digest": digest,
			},
		})
	}
	s.recordReleaseStepBestEffort(model.ReleaseStep{
		TenantID:         app.TenantID,
		ReleaseAttemptID: attempt.ID,
		OperationID:      op.ID,
		Type:             model.ReleaseStepTypeFinalize,
		Status:           model.ReleaseStepStatusCompleted,
		Summary:          "release attempt completed",
		FinishedAt:       &now,
	})
}

func (s *Service) failReleaseAttemptForOperation(op model.Operation, summary string) {
	if s == nil || s.Store == nil {
		return
	}
	attempt, found, err := s.Store.FindReleaseAttemptForOperation(op.ID)
	if err != nil || !found {
		return
	}
	now := time.Now().UTC()
	attempt.Status = model.ReleaseAttemptStatusFailed
	attempt.Confidence = model.OperationEvidenceConfidenceEvidenceBacked
	attempt.FailureOperationID = op.ID
	attempt.Summary = strings.TrimSpace(summary)
	attempt.FinishedAt = &now
	evidence, _ := s.Store.ListOperationEvidence(model.OperationEvidenceFilter{
		TenantID:      op.TenantID,
		PlatformAdmin: true,
		OperationID:   op.ID,
		Limit:         1,
	})
	if len(evidence) > 0 {
		attempt.FailureEvidenceID = evidence[len(evidence)-1].ID
		attempt.Confidence = evidence[len(evidence)-1].Confidence
	}
	s.updateReleaseAttemptBestEffort(attempt)
	s.recordReleaseStepBestEffort(model.ReleaseStep{
		TenantID:         op.TenantID,
		ReleaseAttemptID: attempt.ID,
		OperationID:      op.ID,
		Type:             model.ReleaseStepTypeFinalize,
		Status:           model.ReleaseStepStatusFailed,
		Summary:          strings.TrimSpace(summary),
		EvidenceID:       attempt.FailureEvidenceID,
		FinishedAt:       &now,
	})
}

func (s *Service) recordRolloutReadinessResultStep(op model.Operation, app model.App, result releaseflow.RolloutReadinessResult) {
	if s == nil || s.Store == nil {
		return
	}
	attempt, found, err := s.Store.FindReleaseAttemptForOperation(op.ID)
	if err != nil || !found {
		return
	}
	now := time.Now().UTC()
	status := model.ReleaseStepStatusCompleted
	summary := "managed app rollout ready"
	if !result.Ready {
		status = model.ReleaseStepStatusFailed
		summary = strings.TrimSpace(result.Message)
		if summary == "" && result.Err != nil {
			summary = result.Err.Error()
		}
		if summary == "" {
			summary = "managed app rollout failed"
		}
	}
	s.recordReleaseStepBestEffort(model.ReleaseStep{
		TenantID:         app.TenantID,
		ReleaseAttemptID: attempt.ID,
		OperationID:      op.ID,
		Type:             model.ReleaseStepTypeRolloutWait,
		Status:           status,
		Summary:          summary,
		EvidenceID:       result.EvidenceID,
		FinishedAt:       &now,
		Payload: map[string]any{
			"phase":                result.Phase,
			"ready":                result.Ready,
			"expected_release_key": result.ExpectedReleaseKey,
			"current_release_key":  result.CurrentReleaseKey,
			"scheduling_reason":    result.SchedulingReason,
			"pod_failure_reason":   result.PodFailureReason,
		},
	})
}

func (s *Service) recordImportQueuedDeployReleaseSteps(importOp model.Operation, app model.App, deployOp model.Operation) {
	if s == nil || s.Store == nil {
		return
	}
	attempt, found, err := s.Store.FindReleaseAttemptForOperation(importOp.ID)
	if err != nil || !found {
		return
	}
	attempt.Status = model.ReleaseAttemptStatusDeploying
	s.updateReleaseAttemptBestEffort(attempt)
	now := time.Now().UTC()
	s.recordReleaseStepBestEffort(model.ReleaseStep{
		TenantID:         app.TenantID,
		ReleaseAttemptID: attempt.ID,
		OperationID:      importOp.ID,
		Type:             model.ReleaseStepTypeImageImport,
		Status:           model.ReleaseStepStatusCompleted,
		Summary:          "image import completed",
		FinishedAt:       &now,
	})
	s.recordReleaseStepBestEffort(model.ReleaseStep{
		TenantID:         app.TenantID,
		ReleaseAttemptID: attempt.ID,
		OperationID:      deployOp.ID,
		Type:             model.ReleaseStepTypeDeployQueued,
		Status:           model.ReleaseStepStatusPending,
		Summary:          "deploy operation queued after import",
	})
}

func completedOperationDigest(op model.Operation) string {
	if op.DesiredSource != nil {
		if digest := model.ImageDigestFromReference(op.DesiredSource.ResolvedImageRef); digest != "" {
			return digest
		}
	}
	if op.DesiredSpec != nil {
		return model.ImageDigestFromReference(op.DesiredSpec.Image)
	}
	return ""
}
