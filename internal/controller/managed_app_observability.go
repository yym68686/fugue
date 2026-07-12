package controller

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

const managedAppRolloutSnapshotMismatchLimit = 32

var managedAppRolloutSnapshotTimeType = reflect.TypeOf(time.Time{})

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

func managedAppRolloutDecisionFromObjects(ctx context.Context, namespace string, managed runtime.ManagedAppObject, app model.App, objects []map[string]any, releaseKey string) managedAppRolloutDecision {
	applyCtx := managedAppApplySourceFromContext(ctx)
	decision := managedAppRolloutDecision{
		Source:         applyCtx.Source,
		OperationID:    applyCtx.OperationID,
		RolloutIntent:  strings.TrimSpace(app.Spec.RolloutIntent),
		OldReleaseKey:  strings.TrimSpace(managed.Status.CurrentReleaseKey),
		NewReleaseKey:  strings.TrimSpace(releaseKey),
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
	annotations := objectStringMapValue(nestedObjectValue(deployment, "metadata", "annotations"))
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

func nestedObjectValue(root map[string]any, path ...string) any {
	var current any = root
	for _, part := range path {
		values, ok := current.(map[string]any)
		if !ok || values == nil {
			return nil
		}
		current = values[part]
	}
	return current
}

func nestedObjectMap(root map[string]any, path ...string) map[string]any {
	current := root
	for _, part := range path {
		if current == nil {
			return nil
		}
		next := objectMapValue(current[part])
		if len(next) == 0 {
			return nil
		}
		current = next
	}
	return current
}

func objectMapValue(raw any) map[string]any {
	switch values := raw.(type) {
	case map[string]any:
		return values
	case map[string]string:
		out := make(map[string]any, len(values))
		for key, value := range values {
			out[key] = value
		}
		return out
	default:
		return nil
	}
}

func objectStringMapValue(raw any) map[string]string {
	if raw == nil {
		return nil
	}
	switch values := raw.(type) {
	case map[string]string:
		return values
	case map[string]any:
		out := make(map[string]string, len(values))
		for key, value := range values {
			if text, ok := value.(string); ok {
				out[key] = text
			}
		}
		return out
	default:
		return nil
	}
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

func (s *Service) recordManagedAppOnlineRolloutSnapshotRejected(ctx context.Context, managedSnapshot, stored model.App) {
	mismatchFields := managedAppOnlineRolloutSnapshotMismatchFields(managedSnapshot, stored)
	if len(mismatchFields) == 0 {
		mismatchFields = []string{"unknown"}
	}
	s.logControllerAppEvent(ctx, "managed_app_online_rollout_snapshot_rejected", "warning", managedSnapshot, "managed app online rollout snapshot rejected in favor of stored desired state", map[string]any{
		"rollout_intent":  strings.TrimSpace(managedSnapshot.Spec.RolloutIntent),
		"mismatch_fields": mismatchFields,
	})
}

func managedAppOnlineRolloutSnapshotMismatchFields(managedSnapshot, stored model.App) []string {
	if !appHasOnlineRolloutIntent(managedSnapshot) {
		return []string{"Spec.RolloutIntent"}
	}

	managedIdentity := comparableManagedAppRolloutSnapshot(managedSnapshot)
	storedIdentity := comparableManagedAppRolloutSnapshot(stored)
	managedIdentity.Spec = model.AppSpec{}
	storedIdentity.Spec = model.AppSpec{}
	if fields := reflectMismatchFields("Identity", managedIdentity, storedIdentity); len(fields) > 0 {
		return fields
	}

	switch strings.TrimSpace(managedSnapshot.Spec.RolloutIntent) {
	case model.AppRolloutIntentOnlineImageUpdate:
		if strings.TrimSpace(managedSnapshot.Spec.Image) == "" || strings.TrimSpace(managedSnapshot.Spec.Image) != strings.TrimSpace(stored.Spec.Image) {
			return []string{"Spec.Image"}
		}
		return reflectMismatchFields("Spec", comparableImageOnlySpec(managedSnapshot.Spec), comparableImageOnlySpec(stored.Spec))
	case model.AppRolloutIntentOnlineResourceUpdate:
		if managedDeployOperationResourcesDiffer(managedSnapshot.Spec, stored.Spec) {
			return []string{"Spec.Resources"}
		}
		return reflectMismatchFields("Spec", comparableResourceOnlySpec(managedSnapshot.Spec), comparableResourceOnlySpec(stored.Spec))
	case model.AppRolloutIntentOnlineLifecycleUpdate:
		if managedSnapshot.Spec.TerminationGracePeriodSeconds != stored.Spec.TerminationGracePeriodSeconds {
			return []string{"Spec.TerminationGracePeriodSeconds"}
		}
		return reflectMismatchFields("Spec", comparableLifecycleOnlySpec(managedSnapshot.Spec), comparableLifecycleOnlySpec(stored.Spec))
	case model.AppRolloutIntentOnlineRestart:
		if strings.TrimSpace(managedSnapshot.Spec.RestartToken) == "" || strings.TrimSpace(managedSnapshot.Spec.RestartToken) != strings.TrimSpace(stored.Spec.RestartToken) {
			return []string{"Spec.RestartToken"}
		}
		return reflectMismatchFields("Spec", comparableRestartSpec(managedSnapshot.Spec), comparableRestartSpec(stored.Spec))
	case model.AppRolloutIntentOnlineConfigUpdate:
		return reflectMismatchFields("Snapshot", comparableManagedAppRolloutSnapshot(managedSnapshot), comparableManagedAppRolloutSnapshot(stored))
	default:
		return reflectMismatchFields("Snapshot", comparableManagedAppRolloutSnapshot(managedSnapshot), comparableManagedAppRolloutSnapshot(stored))
	}
}

func reflectMismatchFields(path string, left, right any) []string {
	fields := make([]string, 0, 4)
	appendReflectMismatchFields(&fields, path, reflect.ValueOf(left), reflect.ValueOf(right))
	return fields
}

func appendReflectMismatchFields(fields *[]string, path string, left, right reflect.Value) {
	if len(*fields) >= managedAppRolloutSnapshotMismatchLimit {
		return
	}
	if !left.IsValid() || !right.IsValid() {
		if left.IsValid() != right.IsValid() {
			*fields = append(*fields, path)
		}
		return
	}
	if left.Type() != right.Type() {
		*fields = append(*fields, path)
		return
	}
	if left.Type() == managedAppRolloutSnapshotTimeType {
		if !reflect.DeepEqual(left.Interface(), right.Interface()) {
			*fields = append(*fields, path)
		}
		return
	}

	switch left.Kind() {
	case reflect.Interface, reflect.Pointer:
		if left.IsNil() || right.IsNil() {
			if left.IsNil() != right.IsNil() {
				*fields = append(*fields, path)
			}
			return
		}
		appendReflectMismatchFields(fields, path, left.Elem(), right.Elem())
	case reflect.Struct:
		for index := 0; index < left.NumField(); index++ {
			field := left.Type().Field(index)
			if field.PkgPath != "" {
				continue
			}
			appendReflectMismatchFields(fields, path+"."+field.Name, left.Field(index), right.Field(index))
		}
	case reflect.Slice, reflect.Array:
		if left.Len() != right.Len() {
			*fields = append(*fields, path+".Length")
		}
		limit := left.Len()
		if right.Len() < limit {
			limit = right.Len()
		}
		for index := 0; index < limit; index++ {
			appendReflectMismatchFields(fields, fmt.Sprintf("%s[%d]", path, index), left.Index(index), right.Index(index))
		}
	case reflect.Map:
		if !reflect.DeepEqual(left.Interface(), right.Interface()) {
			*fields = append(*fields, path)
		}
	default:
		if !reflect.DeepEqual(left.Interface(), right.Interface()) {
			*fields = append(*fields, path)
		}
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
