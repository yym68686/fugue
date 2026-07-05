package controller

import (
	"context"
	"fmt"
	"strings"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func isRightSizingDeployOperation(op model.Operation) bool {
	if op.Type != model.OperationTypeDeploy ||
		strings.TrimSpace(op.RequestedByType) != model.ActorTypeSystem {
		return false
	}
	switch strings.TrimSpace(op.RequestedByID) {
	case model.OperationRequestedByRightSizing, model.OperationRequestedByRightSizingDownscale:
		return true
	default:
		return false
	}
}

func (s *Service) refuseRightSizingDowntimeIfNeeded(ctx context.Context, op model.Operation, app model.App, scheduling runtime.SchedulingConstraints, postgresPlacements map[string][]runtime.SchedulingConstraints) error {
	if !isRightSizingDeployOperation(op) {
		return nil
	}
	prepared := s.Renderer.PrepareApp(app)
	childObjects := s.Renderer.BuildManagedAppChildObjectsWithPlacements(prepared, scheduling, postgresPlacements, nil)
	deployment := firstManagedAppDeploymentObject(childObjects, runtime.RuntimeAppResourceName(prepared))
	decision := rightSizingDeploymentDowntimeDecision(prepared, deployment)
	if !decision.refused {
		return nil
	}
	message := "right-sizing deploy refused because it would require downtime for a serving app"
	attrs := map[string]any{
		"decision":         "downtime_refused",
		"operation_id":     op.ID,
		"requested_by_id":  strings.TrimSpace(op.RequestedByID),
		"strategy":         decision.strategy,
		"downtime_class":   decision.downtimeClass,
		"reason":           decision.reason,
		"rollout_mode":     decision.rolloutMode,
		"rollout_intent":   strings.TrimSpace(prepared.Spec.RolloutIntent),
		"cluster_service":  model.AppHasClusterService(prepared.Spec),
		"public_app":       model.AppExposesPublicService(prepared.Spec),
		"desired_replicas": prepared.Spec.Replicas,
	}
	s.logOperationAppEvent("blocked", "warning", op, prepared, message, attrs)
	s.logControllerAppEvent(ctx, "right_sizing_decision", "warning", prepared, message, attrs)
	return fmt.Errorf("%s", message)
}

type rightSizingDowntimeDecision struct {
	refused       bool
	strategy      string
	downtimeClass string
	reason        string
	rolloutMode   string
}

func rightSizingDeploymentDowntimeDecision(app model.App, deployment map[string]any) rightSizingDowntimeDecision {
	out := rightSizingDowntimeDecision{
		strategy: deploymentStrategyTypeFromObject(deployment),
	}
	annotations := objectStringMapValue(nestedObjectValue(deployment, "metadata", "annotations"))
	out.downtimeClass = strings.TrimSpace(annotations["fugue.io/downtime-class"])
	out.reason = strings.TrimSpace(annotations["fugue.io/rollout-reason"])
	out.rolloutMode = strings.TrimSpace(annotations["fugue.io/rollout-mode"])
	out.refused = app.Spec.Replicas > 0 &&
		model.AppHasClusterService(app.Spec) &&
		strings.EqualFold(strings.TrimSpace(out.strategy), "Recreate") &&
		strings.EqualFold(strings.TrimSpace(out.downtimeClass), "downtime-required")
	return out
}
