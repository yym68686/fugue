package controller

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

type zeroDowntimeRolloutGuardDecision struct {
	Refused            bool
	Reason             string
	PolicyMode         string
	RolloutIntent      string
	Strategy           string
	DowntimeClass      string
	RolloutMode        string
	PreviousReleaseKey string
	DesiredReleaseKey  string
	PodTemplateChanged bool
}

func (s *Service) refuseNonZeroDowntimeRolloutIfNeeded(
	ctx context.Context,
	op model.Operation,
	currentApp, desiredApp model.App,
	scheduling runtime.SchedulingConstraints,
) error {
	decision := s.zeroDowntimeRolloutGuardDecision(op, currentApp, desiredApp, scheduling)
	if !decision.Refused {
		return nil
	}

	message := fmt.Sprintf("zero-downtime %s refused: %s", strings.TrimSpace(op.Type), decision.Reason)
	attrs := map[string]any{
		"decision":             "zero_downtime_required",
		"operation_id":         strings.TrimSpace(op.ID),
		"operation_type":       strings.TrimSpace(op.Type),
		"policy_mode":          decision.PolicyMode,
		"rollout_intent":       decision.RolloutIntent,
		"strategy":             decision.Strategy,
		"downtime_class":       decision.DowntimeClass,
		"rollout_mode":         decision.RolloutMode,
		"reason":               decision.Reason,
		"previous_release_key": decision.PreviousReleaseKey,
		"desired_release_key":  decision.DesiredReleaseKey,
		"pod_template_changed": decision.PodTemplateChanged,
		"cluster_service":      model.AppHasClusterService(desiredApp.Spec),
		"desired_replicas":     desiredApp.Spec.Replicas,
		"storage_class":        zeroDowntimeAppStorageClass(desiredApp.Spec),
		"storage_mode":         zeroDowntimeAppStorageMode(desiredApp.Spec),
	}
	s.logOperationAppEvent("blocked", "warning", op, desiredApp, message, attrs)
	s.logControllerAppEvent(ctx, "zero_downtime_rollout_decision", "warning", desiredApp, message, attrs)
	return fmt.Errorf("%s", message)
}

func (s *Service) zeroDowntimeRolloutGuardDecision(
	op model.Operation,
	currentApp, desiredApp model.App,
	scheduling runtime.SchedulingConstraints,
) zeroDowntimeRolloutGuardDecision {
	decision := zeroDowntimeRolloutGuardDecision{
		PolicyMode:    zeroDowntimePolicyMode(currentApp.Spec, desiredApp.Spec),
		RolloutIntent: strings.TrimSpace(desiredApp.Spec.RolloutIntent),
	}
	if op.Type != model.OperationTypeDeploy && op.Type != model.OperationTypeMigrate {
		return decision
	}
	if decision.PolicyMode == "" {
		return decision
	}
	if currentApp.Spec.Replicas <= 0 || !model.AppHasClusterService(currentApp.Spec) {
		return decision
	}
	if desiredApp.Spec.Replicas <= 0 {
		decision.Refused = true
		decision.Reason = "the requested restart removes every serving replica while zero downtime is enabled"
		return decision
	}
	if !model.AppHasClusterService(desiredApp.Spec) {
		decision.Refused = true
		decision.Reason = "the requested restart removes the cluster service while zero downtime is enabled"
		return decision
	}

	decision.PreviousReleaseKey = strings.TrimSpace(s.Renderer.ManagedAppReleaseKey(s.Renderer.PrepareApp(currentApp), scheduling))
	decision.DesiredReleaseKey = strings.TrimSpace(s.Renderer.ManagedAppReleaseKey(s.Renderer.PrepareApp(desiredApp), scheduling))
	desiredDeployment := s.zeroDowntimeManagedAppDeployment(desiredApp, scheduling)
	decision.Strategy = deploymentStrategyTypeFromObject(desiredDeployment)
	desiredAnnotations := objectStringMapValue(nestedObjectValue(desiredDeployment, "metadata", "annotations"))
	decision.DowntimeClass = strings.TrimSpace(desiredAnnotations["fugue.io/downtime-class"])
	decision.RolloutMode = strings.TrimSpace(desiredAnnotations["fugue.io/rollout-mode"])
	if strings.TrimSpace(decision.Strategy) == "" {
		decision.Refused = true
		decision.Reason = "the rendered serving workload has no explicit deployment strategy while zero downtime is enabled"
		return decision
	}
	if strings.EqualFold(decision.Strategy, "Recreate") ||
		strings.EqualFold(decision.DowntimeClass, "downtime-required") ||
		strings.EqualFold(decision.RolloutMode, "isolated-singleton") {
		decision.Refused = true
		decision.Reason = "the rendered serving workload requires a Recreate rollout while zero downtime is enabled"
		return decision
	}
	previousTemplate := s.zeroDowntimeManagedAppPodTemplate(currentApp, scheduling)
	desiredTemplate := nestedObjectValue(desiredDeployment, "spec", "template")
	decision.PodTemplateChanged = !reflect.DeepEqual(previousTemplate, desiredTemplate)
	if !decision.PodTemplateChanged {
		return decision
	}
	if !appSupportsOnlineRolloutIntent(desiredApp) {
		decision.Refused = true
		decision.Reason = zeroDowntimeUnsupportedTopologyReason(desiredApp.Spec)
		return decision
	}
	if !appHasOnlineRolloutIntent(desiredApp) {
		decision.Refused = true
		decision.Reason = "the requested restart has no validated online rollout plan"
	}
	return decision
}

func (s *Service) zeroDowntimeManagedAppPodTemplate(app model.App, scheduling runtime.SchedulingConstraints) any {
	return nestedObjectValue(s.zeroDowntimeManagedAppDeployment(app, scheduling), "spec", "template")
}

func (s *Service) zeroDowntimeManagedAppDeployment(app model.App, scheduling runtime.SchedulingConstraints) map[string]any {
	prepared := s.Renderer.PrepareApp(app)
	objects := s.Renderer.BuildManagedAppChildObjects(prepared, scheduling, nil)
	return firstManagedAppDeploymentObject(objects, runtime.RuntimeAppResourceName(prepared))
}

func zeroDowntimePolicyMode(specs ...model.AppSpec) string {
	for index := len(specs) - 1; index >= 0; index-- {
		policy := model.NormalizeAppContinuityPolicy(specs[index].Continuity)
		if policy != nil && policy.ZeroDowntime != nil && policy.ZeroDowntime.Enabled {
			return strings.TrimSpace(policy.ZeroDowntime.Mode)
		}
	}
	return ""
}

func zeroDowntimeUnsupportedTopologyReason(spec model.AppSpec) string {
	if spec.Workspace != nil {
		return model.AppStorageClassSameNodeOnlineMountUnsupportedSummary(spec.Workspace.StorageClassName)
	}
	if spec.PersistentStorage != nil {
		if model.AppPersistentStorageSpecUsesSharedProjectRWX(spec.PersistentStorage) {
			return "shared RWX storage could not be validated for an online rollout"
		}
		return model.AppStorageClassSameNodeOnlineMountUnsupportedSummary(spec.PersistentStorage.StorageClassName)
	}
	return "the service topology cannot run an old and replacement pod concurrently"
}

func zeroDowntimeAppStorageClass(spec model.AppSpec) string {
	if spec.Workspace != nil {
		return strings.TrimSpace(spec.Workspace.StorageClassName)
	}
	if spec.PersistentStorage != nil {
		return strings.TrimSpace(spec.PersistentStorage.StorageClassName)
	}
	return ""
}

func zeroDowntimeAppStorageMode(spec model.AppSpec) string {
	if spec.Workspace != nil {
		return "workspace"
	}
	if spec.PersistentStorage != nil {
		return strings.TrimSpace(spec.PersistentStorage.Mode)
	}
	return "stateless"
}
