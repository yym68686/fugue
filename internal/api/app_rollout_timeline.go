package api

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/runtime"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
)

const appRolloutTimelineDefaultWindow = 10 * time.Minute

type deploymentTimelineClient interface {
	getDeployment(ctx context.Context, namespace, name string) (appsv1.Deployment, bool, error)
}

type endpointSliceTimelineClient interface {
	listEndpointSlicesForService(ctx context.Context, namespace, serviceName string) ([]discoveryv1.EndpointSlice, error)
}

type eventTimelineClient interface {
	listEventsByInvolvedObjectName(ctx context.Context, namespace, name string) ([]corev1.Event, error)
}

func (s *Server) handleGetAppRolloutTimeline(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principalCanReadAppObservability(principal) {
		httpx.WriteError(w, http.StatusForbidden, "missing app.observability.read scope")
		return
	}
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}
	aroundRaw := strings.TrimSpace(r.URL.Query().Get("around"))
	if aroundRaw == "" {
		httpx.WriteError(w, http.StatusBadRequest, "around is required")
		return
	}
	windowSize, err := parseRolloutTimelineWindow(r.URL.Query().Get("window"))
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	source := s.appObservabilitySourceStatus(app.ID, "analytics", "rollout timeline query backend is not wired yet")
	around, aroundKind, err := s.resolveRolloutTimelineAround(r.Context(), app.ID, aroundRaw, source)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	window := appObservabilityWindow{
		Since: around.Add(-windowSize).UTC().Format(time.RFC3339Nano),
		Until: around.Add(windowSize).UTC().Format(time.RFC3339Nano),
	}

	timeline := map[string]any{
		"source": source,
		"app": map[string]any{
			"id":         app.ID,
			"name":       app.Name,
			"tenant_id":  app.TenantID,
			"project_id": app.ProjectID,
			"runtime_id": app.Spec.RuntimeID,
		},
		"around": map[string]any{
			"value": aroundRaw,
			"kind":  aroundKind,
			"time":  around.UTC().Format(time.RFC3339Nano),
		},
		"window":     window,
		"operations": s.rolloutTimelineOperations(app, window),
	}

	clickHouseWarnings := []string{}
	if source.Status != "disabled" && observabilityExporterActive(source.ActiveExporters, "analytics") {
		source.Status = "available"
		source.Available = true
		source.Reason = "rollout timeline query backend returned data"
		events, err := s.queryRolloutTimelineAppEvents(r.Context(), app.ID, window)
		if err != nil {
			source.Status = "degraded"
			source.Available = false
			source.Reason = err.Error()
			clickHouseWarnings = append(clickHouseWarnings, "app_events: "+err.Error())
		}
		requests, err := s.queryRolloutTimeline5xx(r.Context(), app.ID, window)
		if err != nil {
			source.Status = "degraded"
			source.Available = false
			source.Reason = err.Error()
			clickHouseWarnings = append(clickHouseWarnings, "request_facts: "+err.Error())
		}
		timeline["events"] = events
		timeline["requests_5xx"] = requests
	} else {
		timeline["events"] = []map[string]any{}
		timeline["requests_5xx"] = []map[string]any{}
	}

	kubernetes, kubeWarnings := s.rolloutTimelineKubernetes(r.Context(), app)
	timeline["kubernetes"] = kubernetes
	warnings := append(clickHouseWarnings, kubeWarnings...)
	if len(warnings) > 0 {
		timeline["warnings"] = warnings
	}
	timeline["source"] = source
	s.appendAudit(principal, "app.rollout.timeline.read", "app", app.ID, app.TenantID, map[string]string{
		"around_kind": aroundKind,
		"window":      windowSize.String(),
	})
	httpx.WriteJSON(w, http.StatusOK, timeline)
}

func parseRolloutTimelineWindow(raw string) (time.Duration, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return appRolloutTimelineDefaultWindow, nil
	}
	duration, err := time.ParseDuration(raw)
	if err != nil || duration <= 0 {
		return 0, fmt.Errorf("window must be a positive duration such as 10m")
	}
	if duration > appObservabilityMaxWindow/2 {
		return 0, fmt.Errorf("window cannot exceed %s", (appObservabilityMaxWindow / 2).String())
	}
	return duration, nil
}

func (s *Server) resolveRolloutTimelineAround(ctx context.Context, appID, raw string, source appObservabilitySourceStatus) (time.Time, string, error) {
	if parsed, err := parseAppObservabilityTimestamp(raw); err == nil {
		return parsed.UTC(), "time", nil
	}
	if source.Status == "disabled" || !observabilityExporterActive(source.ActiveExporters, "analytics") {
		return time.Time{}, "", fmt.Errorf("around must be RFC3339 when request analytics is unavailable")
	}
	query := "SELECT ts, app_id FROM request_facts WHERE (app_id = " + quoteClickHouseString(appID) + " OR app_id = '')" +
		" AND (request_id = " + quoteClickHouseString(raw) + " OR trace_id = " + quoteClickHouseString(raw) + ")" +
		" ORDER BY if(app_id = " + quoteClickHouseString(appID) + ", 0, 1), ts DESC LIMIT 1 FORMAT JSONEachRow"
	rows, err := s.queryAppObservabilityClickHouse(ctx, query)
	if err != nil {
		return time.Time{}, "", err
	}
	if len(rows) == 0 {
		return time.Time{}, "", fmt.Errorf("request id or trace id %q was not found in request_facts", raw)
	}
	parsed, err := parseAppObservabilityTimestamp(stringField(rows[0], "ts"))
	if err != nil {
		return time.Time{}, "", fmt.Errorf("request id %q resolved to an invalid timestamp", raw)
	}
	return parsed.UTC(), "request_id", nil
}

func (s *Server) rolloutTimelineOperations(app model.App, window appObservabilityWindow) []map[string]any {
	ops, err := s.store.ListOperationsByApp(app.TenantID, true, app.ID)
	if err != nil {
		return []map[string]any{{"warning": err.Error()}}
	}
	since, until, err := parseAppObservabilityWindowTimes(window)
	if err != nil {
		return nil
	}
	out := []map[string]any{}
	for _, op := range ops {
		if !operationOverlapsWindow(op, since, until) {
			continue
		}
		item := map[string]any{
			"id":                  op.ID,
			"type":                op.Type,
			"status":              op.Status,
			"execution_mode":      op.ExecutionMode,
			"requested_by_type":   op.RequestedByType,
			"target_runtime_id":   op.TargetRuntimeID,
			"assigned_runtime_id": op.AssignedRuntimeID,
			"created_at":          op.CreatedAt.UTC().Format(time.RFC3339Nano),
			"updated_at":          op.UpdatedAt.UTC().Format(time.RFC3339Nano),
			"result_message":      op.ResultMessage,
			"error_message":       op.ErrorMessage,
		}
		if op.StartedAt != nil {
			item["started_at"] = op.StartedAt.UTC().Format(time.RFC3339Nano)
		}
		if op.CompletedAt != nil {
			item["completed_at"] = op.CompletedAt.UTC().Format(time.RFC3339Nano)
		}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		return strings.TrimSpace(fmt.Sprint(out[i]["created_at"])) < strings.TrimSpace(fmt.Sprint(out[j]["created_at"]))
	})
	return out
}

func operationOverlapsWindow(op model.Operation, since, until time.Time) bool {
	for _, ts := range []time.Time{op.CreatedAt, op.UpdatedAt} {
		if !ts.IsZero() && !ts.Before(since) && !ts.After(until) {
			return true
		}
	}
	for _, ts := range []*time.Time{op.StartedAt, op.CompletedAt} {
		if ts != nil && !ts.IsZero() && !ts.Before(since) && !ts.After(until) {
			return true
		}
	}
	return false
}

func (s *Server) queryRolloutTimelineAppEvents(ctx context.Context, appID string, window appObservabilityWindow) ([]map[string]any, error) {
	since, until, err := parseAppObservabilityWindowTimes(window)
	if err != nil {
		return nil, err
	}
	query := "SELECT ts, event_type, severity, operation_id, deployment_id, runtime_id, pod, message, attributes_json " +
		"FROM app_events WHERE app_id = " + quoteClickHouseString(appID) +
		" AND ts >= " + clickHouseDateTime64Literal(since) +
		" AND ts <= " + clickHouseDateTime64Literal(until) +
		" ORDER BY ts ASC LIMIT 500 FORMAT JSONEachRow"
	rows, err := s.queryAppObservabilityClickHouse(ctx, query)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		out = append(out, map[string]any{
			"timestamp":     stringField(row, "ts"),
			"event_type":    stringField(row, "event_type"),
			"severity":      stringField(row, "severity"),
			"operation_id":  stringField(row, "operation_id"),
			"deployment_id": stringField(row, "deployment_id"),
			"runtime_id":    stringField(row, "runtime_id"),
			"pod":           stringField(row, "pod"),
			"message":       stringField(row, "message"),
			"attributes":    parseJSONMapField(row["attributes_json"]),
		})
	}
	return out, nil
}

func (s *Server) queryRolloutTimeline5xx(ctx context.Context, appID string, window appObservabilityWindow) ([]map[string]any, error) {
	since, until, err := parseAppObservabilityWindowTimes(window)
	if err != nil {
		return nil, err
	}
	query := "SELECT ts, trace_id, request_id, hostname, path_template, method, status_code, status_class, duration_ms, ttfb_ms, upstream_ms, error_type, edge_id, pod " +
		"FROM request_facts WHERE app_id = " + quoteClickHouseString(appID) +
		" AND ts >= " + clickHouseDateTime64Literal(since) +
		" AND ts <= " + clickHouseDateTime64Literal(until) +
		" AND status_code >= 500 ORDER BY ts ASC LIMIT 200 FORMAT JSONEachRow"
	rows, err := s.queryAppObservabilityClickHouse(ctx, query)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		out = append(out, map[string]any{
			"timestamp":    stringField(row, "ts"),
			"trace_id":     stringField(row, "trace_id"),
			"request_id":   stringField(row, "request_id"),
			"hostname":     stringField(row, "hostname"),
			"path":         stringField(row, "path_template"),
			"method":       stringField(row, "method"),
			"status_code":  row["status_code"],
			"status_class": stringField(row, "status_class"),
			"duration_ms":  row["duration_ms"],
			"ttfb_ms":      row["ttfb_ms"],
			"upstream_ms":  row["upstream_ms"],
			"error_type":   stringField(row, "error_type"),
			"edge_id":      stringField(row, "edge_id"),
			"pod":          stringField(row, "pod"),
		})
	}
	return out, nil
}

func (s *Server) rolloutTimelineKubernetes(ctx context.Context, app model.App) (map[string]any, []string) {
	namespace := runtime.NamespaceForTenant(app.TenantID)
	selector, _, err := runtimeLogTarget(app, "app")
	if err != nil {
		return map[string]any{}, []string{err.Error()}
	}
	client, err := s.newLogsClient(namespace)
	if err != nil {
		return map[string]any{}, []string{err.Error()}
	}
	warnings := []string{}
	result := map[string]any{
		"namespace": namespace,
		"selector":  selector,
	}
	pods, err := client.listPodsBySelector(ctx, namespace, selector)
	if err != nil {
		warnings = append(warnings, "pods: "+err.Error())
	} else {
		result["pods"] = rolloutTimelinePods(pods)
	}
	if rsClient, ok := any(client).(replicaSetInventoryClient); ok {
		replicaSets, err := rsClient.listReplicaSetsBySelector(ctx, namespace, selector)
		if err != nil {
			warnings = append(warnings, "replica_sets: "+err.Error())
		} else {
			result["replica_sets"] = rolloutTimelineReplicaSets(replicaSets)
		}
	}
	deploymentName := runtime.RuntimeAppResourceName(app)
	if deploymentClient, ok := any(client).(deploymentTimelineClient); ok {
		deployment, found, err := deploymentClient.getDeployment(ctx, namespace, deploymentName)
		if err != nil {
			warnings = append(warnings, "deployment: "+err.Error())
		} else if found {
			result["deployment"] = rolloutTimelineDeployment(deployment)
		}
	}
	serviceName := runtime.RuntimeAppServiceName(app)
	if endpointClient, ok := any(client).(endpointSliceTimelineClient); ok {
		slices, err := endpointClient.listEndpointSlicesForService(ctx, namespace, serviceName)
		if err != nil {
			warnings = append(warnings, "endpoints: "+err.Error())
		} else {
			result["endpoints"] = rolloutTimelineEndpointSummary(serviceName, slices)
		}
	}
	if eventClient, ok := any(client).(eventTimelineClient); ok {
		names := rolloutTimelineEventObjectNames(deploymentName, result)
		events := []map[string]any{}
		for _, name := range names {
			items, err := eventClient.listEventsByInvolvedObjectName(ctx, namespace, name)
			if err != nil {
				warnings = append(warnings, "events "+name+": "+err.Error())
				continue
			}
			events = append(events, rolloutTimelineKubernetesEvents(items)...)
		}
		sort.Slice(events, func(i, j int) bool {
			return strings.TrimSpace(fmt.Sprint(events[i]["timestamp"])) < strings.TrimSpace(fmt.Sprint(events[j]["timestamp"]))
		})
		result["events"] = events
	}
	return result, warnings
}

func rolloutTimelineDeployment(deployment appsv1.Deployment) map[string]any {
	return map[string]any{
		"name":                 deployment.Name,
		"generation":           deployment.Generation,
		"observed_generation":  deployment.Status.ObservedGeneration,
		"strategy":             string(deployment.Spec.Strategy.Type),
		"replicas":             ptrInt32Value(deployment.Spec.Replicas),
		"updated_replicas":     deployment.Status.UpdatedReplicas,
		"ready_replicas":       deployment.Status.ReadyReplicas,
		"available_replicas":   deployment.Status.AvailableReplicas,
		"unavailable_replicas": deployment.Status.UnavailableReplicas,
		"annotations":          deployment.Annotations,
	}
}

func rolloutTimelineReplicaSets(replicaSets []appsv1.ReplicaSet) []map[string]any {
	sort.Slice(replicaSets, func(i, j int) bool {
		return replicaSets[i].CreationTimestamp.Time.Before(replicaSets[j].CreationTimestamp.Time)
	})
	out := make([]map[string]any, 0, len(replicaSets))
	for _, rs := range replicaSets {
		out = append(out, map[string]any{
			"name":               rs.Name,
			"created_at":         rs.CreationTimestamp.Time.UTC().Format(time.RFC3339Nano),
			"revision":           rs.Annotations["deployment.kubernetes.io/revision"],
			"replicas":           ptrInt32Value(rs.Spec.Replicas),
			"current_replicas":   rs.Status.Replicas,
			"ready_replicas":     rs.Status.ReadyReplicas,
			"available_replicas": rs.Status.AvailableReplicas,
		})
	}
	return out
}

func rolloutTimelinePods(pods []kubePodInfo) []map[string]any {
	sort.Slice(pods, func(i, j int) bool {
		return pods[i].Metadata.CreationTimestamp.Before(pods[j].Metadata.CreationTimestamp)
	})
	out := make([]map[string]any, 0, len(pods))
	for _, pod := range pods {
		out = append(out, map[string]any{
			"name":       pod.Metadata.Name,
			"created_at": pod.Metadata.CreationTimestamp.UTC().Format(time.RFC3339Nano),
			"phase":      pod.Status.Phase,
			"ready":      logPodReady(pod),
			"node":       pod.Spec.NodeName,
			"pod_ip":     pod.Status.PodIP,
			"owner":      ownerReferenceFromLogPod(pod),
			"reason":     pod.Status.Reason,
			"message":    pod.Status.Message,
		})
	}
	return out
}

func rolloutTimelineEndpointSummary(serviceName string, slices []discoveryv1.EndpointSlice) map[string]any {
	ready := 0
	total := 0
	for _, slice := range slices {
		for _, endpoint := range slice.Endpoints {
			addressCount := len(endpoint.Addresses)
			if addressCount == 0 {
				continue
			}
			total += addressCount
			if endpoint.Conditions.Terminating != nil && *endpoint.Conditions.Terminating {
				continue
			}
			if endpoint.Conditions.Ready == nil || *endpoint.Conditions.Ready {
				ready += addressCount
			}
		}
	}
	return map[string]any{
		"service_name":    serviceName,
		"ready_endpoints": ready,
		"total_endpoints": total,
		"slice_count":     len(slices),
	}
}

func rolloutTimelineEventObjectNames(deploymentName string, kubernetes map[string]any) []string {
	names := map[string]struct{}{}
	if deploymentName != "" {
		names[deploymentName] = struct{}{}
	}
	if replicaSets, _ := kubernetes["replica_sets"].([]map[string]any); len(replicaSets) > 0 {
		for _, item := range replicaSets {
			if name := strings.TrimSpace(fmt.Sprint(item["name"])); name != "" {
				names[name] = struct{}{}
			}
		}
	}
	if pods, _ := kubernetes["pods"].([]map[string]any); len(pods) > 0 {
		for _, item := range pods {
			if name := strings.TrimSpace(fmt.Sprint(item["name"])); name != "" {
				names[name] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(names))
	for name := range names {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func rolloutTimelineKubernetesEvents(events []corev1.Event) []map[string]any {
	out := make([]map[string]any, 0, len(events))
	for _, event := range events {
		ts := event.LastTimestamp.Time
		if ts.IsZero() {
			ts = event.EventTime.Time
		}
		if ts.IsZero() {
			ts = event.FirstTimestamp.Time
		}
		out = append(out, map[string]any{
			"timestamp":           ts.UTC().Format(time.RFC3339Nano),
			"type":                event.Type,
			"reason":              event.Reason,
			"message":             event.Message,
			"involved_kind":       event.InvolvedObject.Kind,
			"involved_name":       event.InvolvedObject.Name,
			"count":               event.Count,
			"reporting_component": event.ReportingController,
		})
	}
	return out
}

func ptrInt32Value(value *int32) int32 {
	if value == nil {
		return 0
	}
	return *value
}
