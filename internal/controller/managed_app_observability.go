package controller

import (
	"context"
	"fmt"
	"strings"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

type managedAppRolloutDecision struct {
	Source         string
	OperationID    string
	RolloutIntent  string
	Strategy       string
	DowntimeClass  string
	Reason         string
	RolloutMode    string
	OldReleaseKey  string
	NewReleaseKey  string
	OldReplicaSet  string
	NewReplicaSet  string
	DeploymentName string
	ManagedAppName string
	Namespace      string
}

func managedAppRolloutDecisionFromObjects(ctx context.Context, namespace string, managed runtime.ManagedAppObject, app model.App, objects []map[string]any) managedAppRolloutDecision {
	applyCtx := managedAppApplySourceFromContext(ctx)
	decision := managedAppRolloutDecision{
		Source:         applyCtx.Source,
		OperationID:    applyCtx.OperationID,
		RolloutIntent:  strings.TrimSpace(app.Spec.RolloutIntent),
		OldReleaseKey:  strings.TrimSpace(managed.Status.CurrentReleaseKey),
		NewReleaseKey:  strings.TrimSpace(runtime.ManagedAppReleaseKey(app, managed.Spec.Scheduling)),
		DeploymentName: runtime.RuntimeAppResourceName(app),
		ManagedAppName: strings.TrimSpace(managed.Metadata.Name),
		Namespace:      strings.TrimSpace(namespace),
	}
	if decision.ManagedAppName == "" {
		decision.ManagedAppName = runtime.ManagedAppResourceName(app)
	}
	deployment := firstManagedAppDeploymentObject(objects, decision.DeploymentName)
	if len(deployment) == 0 {
		return decision
	}
	decision.Strategy = deploymentStrategyTypeFromObject(deployment)
	annotations := objectStringMap(nestedObjectMap(deployment, "metadata", "annotations"))
	decision.DowntimeClass = strings.TrimSpace(annotations["fugue.io/downtime-class"])
	decision.Reason = strings.TrimSpace(annotations["fugue.io/rollout-reason"])
	decision.RolloutMode = strings.TrimSpace(annotations["fugue.io/rollout-mode"])
	if decision.NewReleaseKey == "" {
		decision.NewReleaseKey = strings.TrimSpace(annotations[runtime.FugueAnnotationReleaseKey])
	}
	return decision
}

func firstManagedAppDeploymentObject(objects []map[string]any, name string) map[string]any {
	name = strings.TrimSpace(name)
	for _, object := range objects {
		if strings.TrimSpace(stringFromMap(object, "kind")) != "Deployment" {
			continue
		}
		metadata := nestedObjectMap(object, "metadata")
		if name == "" || strings.TrimSpace(stringFromMap(metadata, "name")) == name {
			return object
		}
	}
	return nil
}

func deploymentStrategyTypeFromObject(object map[string]any) string {
	spec := nestedObjectMap(object, "spec")
	strategy := nestedObjectMap(spec, "strategy")
	return strings.TrimSpace(stringFromMap(strategy, "type"))
}

func nestedObjectMap(root map[string]any, path ...string) map[string]any {
	current := root
	for _, part := range path {
		if current == nil {
			return nil
		}
		next, _ := current[part].(map[string]any)
		if next == nil {
			return nil
		}
		current = next
	}
	return current
}

func objectStringMap(in map[string]any) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		if text, ok := value.(string); ok {
			out[key] = text
		}
	}
	return out
}

func stringFromMap(values map[string]any, key string) string {
	if len(values) == 0 {
		return ""
	}
	text, _ := values[key].(string)
	return strings.TrimSpace(text)
}

func (s *Service) recordManagedAppRolloutDecision(ctx context.Context, app model.App, decision managedAppRolloutDecision) {
	if strings.TrimSpace(decision.Strategy) == "" {
		return
	}
	attrs := map[string]any{
		"source":           decision.Source,
		"operation_id":     decision.OperationID,
		"rollout_intent":   decision.RolloutIntent,
		"strategy":         decision.Strategy,
		"downtime_class":   decision.DowntimeClass,
		"reason":           decision.Reason,
		"rollout_mode":     decision.RolloutMode,
		"old_release_key":  decision.OldReleaseKey,
		"new_release_key":  decision.NewReleaseKey,
		"old_replica_set":  decision.OldReplicaSet,
		"new_replica_set":  decision.NewReplicaSet,
		"deployment_name":  decision.DeploymentName,
		"managed_app_name": decision.ManagedAppName,
		"namespace":        decision.Namespace,
		"desired_replicas": app.Spec.Replicas,
		"public_app":       model.AppExposesPublicService(app.Spec),
		"cluster_service":  model.AppHasClusterService(app.Spec),
		"deployment_id":    decision.NewReleaseKey,
	}
	s.logControllerAppEvent(ctx, "managed_app_rollout_decision", "info", app, "managed app rollout decision", attrs)
	if managedAppDecisionRequiresDowntimeWarning(app, decision) {
		s.logControllerAppEvent(ctx, "managed_app_downtime_required", "warning", app, "managed app rollout requires downtime", attrs)
	}
}

func managedAppDecisionRequiresDowntimeWarning(app model.App, decision managedAppRolloutDecision) bool {
	return model.AppExposesPublicService(app.Spec) &&
		app.Spec.Replicas > 0 &&
		strings.EqualFold(strings.TrimSpace(decision.Strategy), "Recreate") &&
		strings.EqualFold(strings.TrimSpace(decision.DowntimeClass), "downtime-required")
}

func (s *Service) latestManagedAppReplicaSetName(ctx context.Context, client *kubeClient, namespace string, app model.App) string {
	if client == nil {
		return ""
	}
	replicaSets, err := client.listReplicaSetsBySelector(ctx, namespace, managedAppPodLabelSelector(app))
	if err != nil {
		if s.Logger != nil {
			s.Logger.Printf("list managed app replica sets app=%s failed: %v", app.ID, err)
		}
		return ""
	}
	for _, replicaSet := range replicaSets {
		name := strings.TrimSpace(replicaSet.Metadata.Name)
		if name != "" {
			return name
		}
	}
	return ""
}

func (s *Service) sampleManagedAppReadyEndpoints(ctx context.Context, client *kubeClient, namespace string, app model.App, status runtime.ManagedAppStatus) {
	if !s.controllerObservabilityEndpointConfigured() || client == nil || !model.AppHasClusterService(app.Spec) {
		return
	}
	serviceName := runtime.RuntimeAppServiceName(app)
	slices, err := client.listEndpointSlicesForService(ctx, namespace, serviceName)
	if err != nil {
		if s.Logger != nil {
			s.Logger.Printf("list endpoint slices app=%s service=%s failed: %v", app.ID, serviceName, err)
		}
		return
	}
	ready, total := countReadyEndpointAddresses(slices)
	endpointSource := "endpointslice"
	if len(slices) == 0 && total == 0 {
		endpoints, found, err := client.getEndpointsForService(ctx, namespace, serviceName)
		if err != nil {
			if s.Logger != nil {
				s.Logger.Printf("get endpoints app=%s service=%s failed: %v", app.ID, serviceName, err)
			}
			return
		}
		if found {
			ready, total = countReadyLegacyEndpointAddresses(endpoints)
			endpointSource = "endpoints"
		} else {
			endpointSource = "none"
		}
	}
	eventType := "managed_app_ready_endpoints_sample"
	severity := "info"
	message := "managed app service ready endpoints sampled"
	if app.Spec.Replicas > 0 && ready == 0 {
		eventType = "managed_app_ready_endpoints_empty"
		severity = "warning"
		message = "managed app service has zero ready endpoints"
	}
	s.logControllerAppEvent(ctx, eventType, severity, app, message, map[string]any{
		"service_name":        serviceName,
		"namespace":           namespace,
		"endpoint_source":     endpointSource,
		"ready_endpoints":     ready,
		"total_endpoints":     total,
		"desired_replicas":    app.Spec.Replicas,
		"ready_replicas":      status.ReadyReplicas,
		"phase":               status.Phase,
		"current_release_key": status.CurrentReleaseKey,
		"pending_release_key": status.PendingReleaseKey,
	})
}

func countReadyEndpointAddresses(slices []kubeEndpointSlice) (int, int) {
	ready := 0
	total := 0
	for _, slice := range slices {
		for _, endpoint := range slice.Endpoints {
			addressCount := len(endpoint.Addresses)
			if addressCount == 0 {
				continue
			}
			total += addressCount
			if endpointTerminating(endpoint) || !endpointReady(endpoint) {
				continue
			}
			ready += addressCount
		}
	}
	return ready, total
}

func countReadyLegacyEndpointAddresses(endpoints kubeEndpoints) (int, int) {
	ready := 0
	total := 0
	for _, subset := range endpoints.Subsets {
		ready += len(subset.Addresses)
		total += len(subset.Addresses) + len(subset.NotReadyAddresses)
	}
	return ready, total
}

func endpointReady(endpoint kubeEndpoint) bool {
	return endpoint.Conditions.Ready == nil || *endpoint.Conditions.Ready
}

func endpointTerminating(endpoint kubeEndpoint) bool {
	return endpoint.Conditions.Terminating != nil && *endpoint.Conditions.Terminating
}

func (s *Service) controllerObservabilityEndpointConfigured() bool {
	return strings.TrimSpace(s.Config.AppObservabilityEndpoint) != ""
}

func managedAppRolloutDecisionSummary(decision managedAppRolloutDecision) string {
	return fmt.Sprintf(
		"strategy=%s downtime_class=%s reason=%s rollout_intent=%s source=%s operation_id=%s",
		decision.Strategy,
		decision.DowntimeClass,
		decision.Reason,
		decision.RolloutIntent,
		decision.Source,
		decision.OperationID,
	)
}
