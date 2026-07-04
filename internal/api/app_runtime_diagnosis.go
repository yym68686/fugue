package api

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/runtime"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

const (
	appDiagnosisEventLimit         = 12
	appDiagnosisHTTPProbeTimeout   = 2 * time.Second
	appDiagnosisHTTPProbeUserAgent = "fugue-app-diagnosis/1.0"
	appDiagnosisDrainAgentName     = "fugue-drain-agent"
)

var appDiagnosisHTTPProbePaths = []string{"/healthz", "/"}

type appDiagnosis struct {
	Category       string                     `json:"category"`
	Summary        string                     `json:"summary"`
	Hint           string                     `json:"hint,omitempty"`
	Component      string                     `json:"component,omitempty"`
	Namespace      string                     `json:"namespace,omitempty"`
	Selector       string                     `json:"selector,omitempty"`
	ImplicatedNode string                     `json:"implicated_node,omitempty"`
	ImplicatedPod  string                     `json:"implicated_pod,omitempty"`
	LivePods       int                        `json:"live_pods,omitempty"`
	ReadyPods      int                        `json:"ready_pods,omitempty"`
	Evidence       []string                   `json:"evidence"`
	Warnings       []string                   `json:"warnings"`
	Events         []model.ClusterEvent       `json:"events"`
	ImageTracking  *appImageTrackingDiagnosis `json:"image_tracking,omitempty"`
}

func (s *Server) handleGetAppDiagnosis(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	component := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("component")))
	if component == "" {
		component = "app"
	}
	if component != "app" && component != "postgres" {
		httpx.WriteError(w, http.StatusBadRequest, "component must be app or postgres")
		return
	}

	var (
		app     model.App
		allowed bool
	)
	if component == "postgres" {
		app, allowed = s.loadAuthorizedApp(w, r, principal)
	} else {
		app, allowed = s.loadAuthorizedAppMetadata(w, r, principal)
	}
	if !allowed {
		return
	}

	diagnosis, err := s.diagnoseAppRuntime(r, app, component)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	if component == "app" {
		s.attachAppImageTrackingEvidence(r.Context(), principal, app, &diagnosis)
	}

	s.appendAudit(principal, "app.diagnosis.read", "app", app.ID, app.TenantID, map[string]string{
		"component": component,
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"diagnosis": diagnosis})
}

func (s *Server) attachAppImageTrackingEvidence(ctx context.Context, principal model.Principal, app model.App, diagnosis *appDiagnosis) {
	if s == nil || diagnosis == nil {
		return
	}
	imageDiagnosis, err := s.buildAppImageTrackingDiagnosis(ctx, principal, app, false)
	if err != nil {
		diagnosis.Warnings = append(diagnosis.Warnings, fmt.Sprintf("image tracking diagnosis unavailable: %v", err))
		return
	}
	if imageDiagnosis.Tracking == nil && len(imageDiagnosis.RecentChecks) == 0 {
		return
	}
	diagnosis.ImageTracking = &imageDiagnosis
	diagnosis.Evidence = appendUniqueString(diagnosis.Evidence, "image tracking: "+imageDiagnosis.Summary)
	if imageDiagnosis.LatestCheck != nil {
		diagnosis.Evidence = appendUniqueString(diagnosis.Evidence, fmt.Sprintf(
			"image tracking latest decision=%s checked_at=%s",
			imageDiagnosis.LatestCheck.Decision,
			imageDiagnosis.LatestCheck.CheckedAt.Format(time.RFC3339),
		))
	}
}

func (s *Server) diagnoseAppRuntime(r *http.Request, app model.App, component string) (appDiagnosis, error) {
	diagnosis := appDiagnosis{
		Category:  "unknown",
		Component: component,
		Evidence:  []string{},
		Warnings:  []string{},
		Events:    []model.ClusterEvent{},
	}

	runtimeObj, err := s.store.GetRuntime(app.Spec.RuntimeID)
	if err != nil {
		return appDiagnosis{}, err
	}
	if runtimeObj.Type == model.RuntimeTypeExternalOwned {
		diagnosis.Category = "external-runtime"
		diagnosis.Summary = "runtime diagnosis is only available for managed runtimes"
		return diagnosis, nil
	}

	namespace := runtime.NamespaceForTenant(app.TenantID)
	logClient, err := s.newLogsClient(namespace)
	if err != nil {
		return appDiagnosis{}, err
	}
	selector, _, err := runtimeLogTarget(app, component)
	if err != nil {
		return appDiagnosis{}, err
	}
	diagnosis.Namespace = namespace
	diagnosis.Selector = selector

	pods, err := logClient.listPodsBySelector(r.Context(), namespace, selector)
	if err != nil {
		return appDiagnosis{}, err
	}
	sortPodsByCreation(pods)
	activePods := activeLogPods(pods)
	diagnosis.LivePods = len(activePods)
	diagnosis.ReadyPods = countReadyLogPods(activePods)

	clusterClient, clusterErr := s.requireClusterNodeClient()
	if clusterErr != nil {
		diagnosis.Warnings = append(diagnosis.Warnings, fmt.Sprintf("cluster diagnostics unavailable: %v", clusterErr))
	} else {
		defer clusterClient.closeIdleConnections()
	}

	namespaceEvents := []model.ClusterEvent{}
	var rawNamespaceEvents []coreEventOrZero
	if clusterErr == nil {
		if events, err := clusterClient.listClusterEvents(r.Context(), namespace); err != nil {
			diagnosis.Warnings = append(diagnosis.Warnings, fmt.Sprintf("namespace events unavailable: %v", err))
		} else {
			rawNamespaceEvents = wrapCoreEvents(events)
			namespaceEvents = filterAppDiagnosisEvents(events, pods)
			diagnosis.Events = namespaceEvents
		}
	}

	var snapshots []clusterNodeSnapshot
	if clusterErr == nil {
		if inventory, err := s.loadClusterNodeInventory(r.Context()); err != nil {
			diagnosis.Warnings = append(diagnosis.Warnings, fmt.Sprintf("cluster node inventory unavailable: %v", err))
		} else {
			snapshots = inventory
		}
	}
	platformEnvDrift := false
	if clusterErr == nil && component == "app" {
		platformEnvDrift = s.appendAppPlatformEnvDrift(r.Context(), clusterClient, app, namespace, &diagnosis)
	}
	if component == "app" {
		s.appendAppStrictDrainEvidence(r.Context(), clusterClient, logClient, app, namespace, pods, &diagnosis)
	}

	podSummaries := summarizeAppDiagnosisPods(pods)
	for _, summary := range podSummaries {
		diagnosis.Evidence = appendUniqueString(diagnosis.Evidence, summary)
	}
	crashLogEvidence := ""
	if len(podSummaries) > 0 {
		if pod := newestProblemPod(pods); pod != nil {
			crashLogEvidence = readAppDiagnosisCrashEvidence(r.Context(), logClient, namespace, *pod)
		}
	}
	processExitSummary := newestServiceProcessExitSummary(pods)

	evictedPod := newestEvictedPod(pods)
	schedulingEvent := newestFailedSchedulingEventForPods(rawNamespaceEvents, pods)
	volumeAffinityConflict := schedulingEvent != nil && containsVolumeAffinityConflict(schedulingEvent.Message)
	if schedulingEvent != nil {
		diagnosis.Evidence = appendUniqueString(diagnosis.Evidence, "scheduling: "+strings.TrimSpace(schedulingEvent.Message))
	}

	var nodeSnapshot *clusterNodeSnapshot
	if evictedPod != nil {
		diagnosis.ImplicatedNode = strings.TrimSpace(evictedPod.Spec.NodeName)
		diagnosis.ImplicatedPod = strings.TrimSpace(evictedPod.Metadata.Name)
	}
	if diagnosis.ImplicatedNode != "" {
		if snapshot, ok := findClusterNodeSnapshotByName(snapshots, diagnosis.ImplicatedNode); ok {
			nodeSnapshot = &snapshot
			if clusterNodeConditionIsTrue(snapshot.node, clusterNodeConditionDisk) {
				diagnosis.Evidence = appendUniqueString(diagnosis.Evidence, fmt.Sprintf("node %s condition DiskPressure=True", snapshot.node.Name))
			}
		}
	}
	if diagnosis.ImplicatedPod == "" {
		if pod := newestProblemPod(pods); pod != nil {
			diagnosis.ImplicatedPod = strings.TrimSpace(pod.Metadata.Name)
			if diagnosis.ImplicatedNode == "" {
				diagnosis.ImplicatedNode = strings.TrimSpace(pod.Spec.NodeName)
			}
		}
	}

	httpProbe := appHTTPProbeDiagnosis{}
	if component == "app" && diagnosis.ReadyPods > 0 {
		httpProbe = s.diagnoseAppHTTPAvailability(r.Context(), app)
		for _, evidence := range httpProbe.evidence {
			diagnosis.Evidence = appendUniqueString(diagnosis.Evidence, evidence)
		}
	}

	switch {
	case diagnosis.ReadyPods > 0 && httpProbe.attempted && httpProbe.timedOut:
		diagnosis.Category = "http-timeout"
		diagnosis.Summary = fmt.Sprintf("%d/%d runtime pods are ready, but the internal HTTP probe timed out", diagnosis.ReadyPods, diagnosis.LivePods)
		diagnosis.Hint = fmt.Sprintf("Probe the internal service with fugue app request %s /healthz and inspect runtime logs with fugue app logs runtime %s --previous.", strings.TrimSpace(app.Name), strings.TrimSpace(app.Name))
	case diagnosis.ReadyPods > 0 && httpProbe.attempted && httpProbe.unhealthy:
		diagnosis.Category = "http-unhealthy"
		diagnosis.Summary = fmt.Sprintf("%d/%d runtime pods are ready, but the internal HTTP health probe reported an unhealthy response", diagnosis.ReadyPods, diagnosis.LivePods)
		diagnosis.Hint = fmt.Sprintf("Probe the internal service with fugue app request %s /healthz and inspect runtime logs with fugue app logs runtime %s --previous.", strings.TrimSpace(app.Name), strings.TrimSpace(app.Name))
	case diagnosis.ReadyPods > 0 && httpProbe.attempted && !httpProbe.responsive:
		diagnosis.Category = "http-unreachable"
		diagnosis.Summary = fmt.Sprintf("%d/%d runtime pods are ready, but the internal HTTP probe could not reach the app", diagnosis.ReadyPods, diagnosis.LivePods)
		diagnosis.Hint = fmt.Sprintf("Probe the internal service with fugue app request %s /healthz and inspect runtime logs with fugue app logs runtime %s --previous.", strings.TrimSpace(app.Name), strings.TrimSpace(app.Name))
	case diagnosis.ReadyPods > 0 && schedulingEvent != nil:
		diagnosis.Category = "rollout-unschedulable"
		diagnosis.Summary = firstNonEmptyString(
			fmt.Sprintf("%d/%d runtime pods are ready, but a replacement pod is unschedulable: %s", diagnosis.ReadyPods, diagnosis.LivePods, strings.TrimSpace(schedulingEvent.Message)),
			fmt.Sprintf("%d/%d runtime pods are ready, but a replacement pod is unschedulable", diagnosis.ReadyPods, diagnosis.LivePods),
		)
		diagnosis.Hint = buildAppDiagnosisHint(app, diagnosis.ImplicatedNode)
	case diagnosis.ReadyPods > 0:
		diagnosis.Category = "available"
		diagnosis.Summary = fmt.Sprintf("%d/%d runtime pods are ready", diagnosis.ReadyPods, diagnosis.LivePods)
	case evictedPod != nil && nodeSnapshot != nil && clusterNodeConditionIsTrue(nodeSnapshot.node, clusterNodeConditionDisk) && volumeAffinityConflict:
		diagnosis.Category = "evicted-disk-pressure-volume-affinity"
		diagnosis.Summary = fmt.Sprintf(
			"pod %s was evicted on node %s after disk pressure, and the replacement pod is now blocked by volume node affinity",
			strings.TrimSpace(evictedPod.Metadata.Name),
			strings.TrimSpace(evictedPod.Spec.NodeName),
		)
		diagnosis.Hint = buildAppDiagnosisHint(app, diagnosis.ImplicatedNode)
	case volumeAffinityConflict:
		diagnosis.Category = "volume-affinity-conflict"
		diagnosis.Summary = firstNonEmptyString(
			fmt.Sprintf("replacement pod is unschedulable because the PVC has a node-affinity conflict: %s", strings.TrimSpace(schedulingEvent.Message)),
			"replacement pod is unschedulable because the PVC has a node-affinity conflict",
		)
		diagnosis.Hint = buildAppDiagnosisHint(app, diagnosis.ImplicatedNode)
	case schedulingEvent != nil:
		diagnosis.Category = "unschedulable"
		diagnosis.Summary = firstNonEmptyString(
			strings.TrimSpace(schedulingEvent.Message),
			"runtime pod is pending because Kubernetes could not schedule it",
		)
		diagnosis.Hint = buildAppDiagnosisHint(app, diagnosis.ImplicatedNode)
	case evictedPod != nil && nodeSnapshot != nil && clusterNodeConditionIsTrue(nodeSnapshot.node, clusterNodeConditionDisk):
		diagnosis.Category = "evicted-disk-pressure"
		diagnosis.Summary = fmt.Sprintf(
			"pod %s was evicted on node %s after disk pressure",
			strings.TrimSpace(evictedPod.Metadata.Name),
			strings.TrimSpace(evictedPod.Spec.NodeName),
		)
		diagnosis.Hint = buildAppDiagnosisHint(app, diagnosis.ImplicatedNode)
	case evictedPod != nil:
		diagnosis.Category = "evicted"
		diagnosis.Summary = summarizeKubePodFailure(*evictedPod)
		diagnosis.Hint = buildAppDiagnosisHint(app, diagnosis.ImplicatedNode)
	case processExitSummary != "":
		diagnosis.Category = "process-exited"
		diagnosis.Summary = processExitSummary
		diagnosis.Hint = fmt.Sprintf("Configure a startup command for %s that keeps the service process running, then redeploy.", strings.TrimSpace(app.Name))
	case len(podSummaries) > 0:
		diagnosis.Category = "pod-failure"
		diagnosis.Summary = podSummaries[0]
		diagnosis.Hint = fmt.Sprintf("Inspect pod history with fugue app logs pods %s and runtime logs with fugue app logs runtime %s --previous.", strings.TrimSpace(app.Name), strings.TrimSpace(app.Name))
		if crashLogEvidence != "" {
			diagnosis.Evidence = appendUniqueString(diagnosis.Evidence, crashLogEvidence)
		}
	case platformEnvDrift:
		diagnosis.Category = "platform-env-drift"
		diagnosis.Summary = "live deployment platform env differs from the app spec"
		diagnosis.Hint = fmt.Sprintf("Inspect the app deployment and trigger a reconcile for %s; stale ARGUS_* or FUGUE_* overrides can point workloads at deleted runtime images.", strings.TrimSpace(app.Name))
	case len(pods) == 0:
		diagnosis.Category = "no-pods"
		diagnosis.Summary = "no runtime pods currently match the app selector"
		diagnosis.Hint = fmt.Sprintf("Inspect the app state with fugue app overview %s and the latest operations with fugue operation ls --app %s.", strings.TrimSpace(app.Name), strings.TrimSpace(app.Name))
	default:
		diagnosis.Category = "no-ready-pods"
		diagnosis.Summary = fmt.Sprintf("0/%d runtime pods are ready", diagnosis.LivePods)
		diagnosis.Hint = fmt.Sprintf("Inspect pod history with fugue app logs pods %s and runtime logs with fugue app logs runtime %s --previous.", strings.TrimSpace(app.Name), strings.TrimSpace(app.Name))
	}

	if diagnosis.Summary == "" {
		diagnosis.Summary = "no single runtime root cause was identified"
	}
	return diagnosis, nil
}

type appHTTPProbeDiagnosis struct {
	attempted  bool
	responsive bool
	timedOut   bool
	unhealthy  bool
	evidence   []string
}

func (s *Server) diagnoseAppHTTPAvailability(ctx context.Context, app model.App) appHTTPProbeDiagnosis {
	diagnosis := appHTTPProbeDiagnosis{}
	if !model.AppHasClusterService(app.Spec) {
		return diagnosis
	}

	client := s.appRequestHTTPClient
	if client == nil {
		client = http.DefaultClient
	}

	for _, requestPath := range appDiagnosisHTTPProbePaths {
		target, err := s.resolveAppInternalRequestURL(ctx, app, requestPath, nil)
		if err != nil {
			diagnosis.attempted = true
			diagnosis.evidence = append(diagnosis.evidence, fmt.Sprintf("http probe GET %s could not resolve internal service URL: %v", requestPath, err))
			continue
		}

		probeCtx, cancel := context.WithTimeout(ctx, appDiagnosisHTTPProbeTimeout)
		startedAt := time.Now()
		req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, target, nil)
		if err != nil {
			cancel()
			diagnosis.attempted = true
			diagnosis.evidence = append(diagnosis.evidence, fmt.Sprintf("http probe GET %s could not create request: %v", requestPath, err))
			continue
		}
		req.Header.Set("Accept", "*/*")
		req.Header.Set("User-Agent", appDiagnosisHTTPProbeUserAgent)

		resp, err := client.Do(req)
		elapsed := time.Since(startedAt).Round(time.Millisecond)
		cancel()
		diagnosis.attempted = true
		if err != nil {
			if appDiagnosisHTTPProbeTimedOut(err) {
				diagnosis.timedOut = true
			}
			diagnosis.evidence = append(diagnosis.evidence, fmt.Sprintf("http probe GET %s failed after %s: %v", requestPath, elapsed, err))
			continue
		}
		if resp == nil {
			diagnosis.evidence = append(diagnosis.evidence, fmt.Sprintf("http probe GET %s failed after %s: empty response", requestPath, elapsed))
			continue
		}
		if resp.Body != nil {
			_ = resp.Body.Close()
		}
		status := strings.TrimSpace(resp.Status)
		if status == "" {
			status = fmt.Sprintf("status=%d", resp.StatusCode)
		}
		diagnosis.responsive = true
		diagnosis.evidence = append(diagnosis.evidence, fmt.Sprintf("http probe GET %s returned %s after %s", requestPath, status, elapsed))
		if appDiagnosisHTTPProbeUnhealthyStatus(resp.StatusCode) {
			diagnosis.unhealthy = true
			return diagnosis
		}
		if requestPath == "/healthz" && appDiagnosisHTTPProbeFallbackStatus(resp.StatusCode) {
			continue
		}
		return diagnosis
	}

	return diagnosis
}

func appDiagnosisHTTPProbeUnhealthyStatus(statusCode int) bool {
	if statusCode <= 0 {
		return false
	}
	return statusCode >= 500
}

func appDiagnosisHTTPProbeFallbackStatus(statusCode int) bool {
	return statusCode == http.StatusNotFound || statusCode == http.StatusMethodNotAllowed
}

func appDiagnosisHTTPProbeTimedOut(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	normalized := strings.ToLower(err.Error())
	return strings.Contains(normalized, "timeout") || strings.Contains(normalized, "deadline exceeded")
}

func (s *Server) appendAppStrictDrainEvidence(ctx context.Context, client *clusterNodeClient, logClient appLogsClient, app model.App, namespace string, pods []kubePodInfo, diagnosis *appDiagnosis) {
	if diagnosis == nil {
		return
	}
	drainMode := ""
	if client != nil {
		deploymentName := runtime.RuntimeAppResourceName(app)
		deployment, found, err := client.readDeploymentObject(ctx, namespace, deploymentName)
		if err != nil {
			diagnosis.Warnings = appendUniqueString(diagnosis.Warnings, fmt.Sprintf("strict drain inspection unavailable: %v", err))
		} else if found {
			evidence, mode := appStrictDrainDeploymentEvidence(deployment)
			drainMode = mode
			if evidence != "" {
				diagnosis.Evidence = appendUniqueString(diagnosis.Evidence, evidence)
			}
		}
	}
	if strings.EqualFold(drainMode, "connection-aware") || appPodsIncludeDrainAgent(pods) {
		if podName, line := latestDrainAgentResultLine(ctx, logClient, namespace, pods); line != "" {
			diagnosis.Evidence = appendUniqueString(diagnosis.Evidence, fmt.Sprintf("strict drain recent result pod=%s %s", podName, line))
		}
	}
}

func appStrictDrainDeploymentEvidence(deployment appsv1.Deployment) (string, string) {
	mode := appStrictDrainAnnotation(deployment, "fugue.io/drain-mode")
	if mode == "" {
		return "", ""
	}
	parts := []string{
		"strict drain mode=" + mode,
	}
	if timeout := appStrictDrainAnnotation(deployment, "fugue.io/drain-timeout-seconds"); timeout != "" {
		parts = append(parts, "timeout_seconds="+timeout)
	}
	if quiet := appStrictDrainAnnotation(deployment, "fugue.io/drain-quiet-period-seconds"); quiet != "" {
		parts = append(parts, "quiet_period_seconds="+quiet)
	}
	if port := appStrictDrainAnnotation(deployment, "fugue.io/drain-agent-port"); port != "" {
		parts = append(parts, "agent_port="+port)
	}
	if grace := appStrictDrainAnnotation(deployment, "fugue.io/termination-grace-min-seconds"); grace != "" {
		parts = append(parts, "termination_grace_min_seconds="+grace)
	}
	return strings.Join(parts, " "), mode
}

func appStrictDrainAnnotation(deployment appsv1.Deployment, key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return ""
	}
	if value := strings.TrimSpace(deployment.Spec.Template.Annotations[key]); value != "" {
		return value
	}
	return strings.TrimSpace(deployment.Annotations[key])
}

func appPodsIncludeDrainAgent(pods []kubePodInfo) bool {
	for _, pod := range pods {
		if podIncludesDrainAgent(pod) {
			return true
		}
	}
	return false
}

func podIncludesDrainAgent(pod kubePodInfo) bool {
	for _, container := range pod.Spec.InitContainers {
		if strings.TrimSpace(container.Name) == appDiagnosisDrainAgentName {
			return true
		}
	}
	for _, container := range pod.Spec.Containers {
		if strings.TrimSpace(container.Name) == appDiagnosisDrainAgentName {
			return true
		}
	}
	for _, status := range pod.Status.InitContainerStatuses {
		if strings.TrimSpace(status.Name) == appDiagnosisDrainAgentName {
			return true
		}
	}
	for _, status := range pod.Status.ContainerStatuses {
		if strings.TrimSpace(status.Name) == appDiagnosisDrainAgentName {
			return true
		}
	}
	return false
}

func latestDrainAgentResultLine(ctx context.Context, logClient appLogsClient, namespace string, pods []kubePodInfo) (string, string) {
	if logClient == nil || len(pods) == 0 {
		return "", ""
	}
	for index := len(pods) - 1; index >= 0; index-- {
		pod := pods[index]
		if !podIncludesDrainAgent(pod) {
			continue
		}
		logs, err := logClient.readPodLogs(ctx, namespace, strings.TrimSpace(pod.Metadata.Name), kubeLogOptions{
			Container: appDiagnosisDrainAgentName,
			TailLines: 80,
		})
		if err != nil {
			continue
		}
		if line := lastDrainAgentLine(logs, "fugue_drain_complete"); line != "" {
			return strings.TrimSpace(pod.Metadata.Name), line
		}
		if line := lastDrainAgentLine(logs, "fugue_drain_start"); line != "" {
			return strings.TrimSpace(pod.Metadata.Name), line
		}
	}
	return "", ""
}

func lastDrainAgentLine(logs, marker string) string {
	lines := strings.Split(logs, "\n")
	for index := len(lines) - 1; index >= 0; index-- {
		line := strings.TrimSpace(lines[index])
		if line == "" || !strings.Contains(line, marker) {
			continue
		}
		if len(line) > 300 {
			line = strings.TrimSpace(line[:300]) + "..."
		}
		return line
	}
	return ""
}

func (s *Server) appendAppPlatformEnvDrift(ctx context.Context, client *clusterNodeClient, app model.App, namespace string, diagnosis *appDiagnosis) bool {
	if client == nil || diagnosis == nil {
		return false
	}
	deploymentName := runtime.RuntimeAppResourceName(app)
	deployment, found, err := client.readDeploymentObject(ctx, namespace, deploymentName)
	if err != nil {
		diagnosis.Warnings = append(diagnosis.Warnings, fmt.Sprintf("deployment env drift inspection unavailable: %v", err))
		return false
	}
	if !found {
		return false
	}
	drift := appPlatformEnvDrift(app, deployment)
	for _, evidence := range drift.evidence {
		diagnosis.Evidence = appendUniqueString(diagnosis.Evidence, evidence)
	}
	for _, warning := range drift.warnings {
		diagnosis.Warnings = appendUniqueString(diagnosis.Warnings, warning)
	}
	return len(drift.evidence) > 0 || len(drift.warnings) > 0
}

type appPlatformEnvDriftReport struct {
	evidence []string
	warnings []string
}

func appPlatformEnvDrift(app model.App, deployment appsv1.Deployment) appPlatformEnvDriftReport {
	expectedSpecEnv := make(map[string]string)
	expectedPlatformNames := knownInjectedPlatformEnvNames(app)
	for key, value := range app.Spec.Env {
		key = strings.TrimSpace(key)
		if key == "" || !isManagedPlatformEnvName(key) {
			continue
		}
		expectedSpecEnv[key] = value
		expectedPlatformNames[key] = struct{}{}
	}

	liveEnv := make(map[string]string)
	for _, container := range deployment.Spec.Template.Spec.Containers {
		for _, env := range container.Env {
			key := strings.TrimSpace(env.Name)
			if key == "" || !isManagedPlatformEnvName(key) {
				continue
			}
			liveEnv[key] = env.Value
		}
	}

	var extra []string
	var missing []string
	var changed []string
	for key := range liveEnv {
		if model.IsFugueInjectedAppEnvName(key) {
			continue
		}
		if _, expected := expectedPlatformNames[key]; !expected {
			extra = append(extra, key)
		}
	}
	for key, expectedValue := range expectedSpecEnv {
		liveValue, ok := liveEnv[key]
		if !ok {
			missing = append(missing, key)
			continue
		}
		if liveValue != expectedValue {
			changed = append(changed, key)
		}
	}
	sort.Strings(extra)
	sort.Strings(missing)
	sort.Strings(changed)

	report := appPlatformEnvDriftReport{}
	if len(extra) > 0 {
		report.evidence = append(report.evidence, fmt.Sprintf("deployment %s/%s has unmanaged platform env not present in app spec: %s", deployment.Namespace, deployment.Name, strings.Join(extra, ", ")))
	}
	if len(missing) > 0 {
		report.evidence = append(report.evidence, fmt.Sprintf("deployment %s/%s is missing app spec platform env: %s", deployment.Namespace, deployment.Name, strings.Join(missing, ", ")))
	}
	if len(changed) > 0 {
		report.evidence = append(report.evidence, fmt.Sprintf("deployment %s/%s has platform env values that differ from app spec: %s", deployment.Namespace, deployment.Name, strings.Join(changed, ", ")))
	}
	if len(report.evidence) > 0 {
		report.warnings = append(report.warnings, "live deployment platform env drift detected")
	}
	return report
}

func knownInjectedPlatformEnvNames(app model.App) map[string]struct{} {
	names := map[string]struct{}{
		"FUGUE_TENANT_ID":  {},
		"FUGUE_PROJECT_ID": {},
		"FUGUE_APP_ID":     {},
		"FUGUE_APP_NAME":   {},
		"FUGUE_RUNTIME_ID": {},
		"FUGUE_API_URL":    {},
		"FUGUE_BASE_URL":   {},
		"FUGUE_TOKEN":      {},
	}
	if app.Route != nil {
		names["FUGUE_APP_HOSTNAME"] = struct{}{}
		names["FUGUE_APP_URL"] = struct{}{}
	}
	return names
}

func isManagedPlatformEnvName(name string) bool {
	name = strings.TrimSpace(name)
	return strings.HasPrefix(name, "ARGUS_") || strings.HasPrefix(name, "FUGUE_")
}

type coreEventOrZero struct {
	Name      string
	Message   string
	Namespace string
	Event     model.ClusterEvent
}

func wrapCoreEvents(events []corev1.Event) []coreEventOrZero {
	out := make([]coreEventOrZero, 0, len(events))
	for _, event := range events {
		out = append(out, coreEventOrZero{
			Name:      strings.TrimSpace(event.InvolvedObject.Name),
			Message:   strings.TrimSpace(event.Message),
			Namespace: strings.TrimSpace(event.Namespace),
			Event:     clusterEventFromCore(event),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		left := clusterEventSortTime(out[i].Event)
		right := clusterEventSortTime(out[j].Event)
		if !left.Equal(right) {
			return left.After(right)
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func filterAppDiagnosisEvents(events []corev1.Event, pods []kubePodInfo) []model.ClusterEvent {
	if len(events) == 0 || len(pods) == 0 {
		return []model.ClusterEvent{}
	}
	podNames := make(map[string]struct{}, len(pods))
	for _, pod := range pods {
		if name := strings.TrimSpace(pod.Metadata.Name); name != "" {
			podNames[name] = struct{}{}
		}
	}
	out := make([]model.ClusterEvent, 0, len(events))
	for _, event := range events {
		if strings.TrimSpace(event.InvolvedObject.Kind) != "Pod" {
			continue
		}
		if _, ok := podNames[strings.TrimSpace(event.InvolvedObject.Name)]; !ok {
			continue
		}
		out = append(out, clusterEventFromCore(event))
	}
	sort.Slice(out, func(i, j int) bool {
		left := clusterEventSortTime(out[i])
		right := clusterEventSortTime(out[j])
		if !left.Equal(right) {
			return left.After(right)
		}
		return out[i].Name < out[j].Name
	})
	if len(out) > appDiagnosisEventLimit {
		out = out[:appDiagnosisEventLimit]
	}
	return out
}

func newestFailedSchedulingEvent(events []coreEventOrZero) *coreEventOrZero {
	for index := range events {
		if strings.EqualFold(strings.TrimSpace(events[index].Event.Reason), "FailedScheduling") {
			return &events[index]
		}
	}
	return nil
}

func newestFailedSchedulingEventForPods(events []coreEventOrZero, pods []kubePodInfo) *coreEventOrZero {
	if len(events) == 0 || len(pods) == 0 {
		return nil
	}
	podNames := make(map[string]struct{}, len(pods))
	for _, pod := range pods {
		if name := strings.TrimSpace(pod.Metadata.Name); name != "" {
			podNames[name] = struct{}{}
		}
	}
	for index := range events {
		if _, ok := podNames[strings.TrimSpace(events[index].Name)]; !ok {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(events[index].Event.Reason), "FailedScheduling") {
			return &events[index]
		}
	}
	return nil
}

func summarizeAppDiagnosisPods(pods []kubePodInfo) []string {
	out := make([]string, 0, len(pods))
	for _, pod := range pods {
		if summary := summarizeKubePodFailure(pod); summary != "" {
			out = append(out, summary)
		}
	}
	return out
}

func newestEvictedPod(pods []kubePodInfo) *kubePodInfo {
	for index := len(pods) - 1; index >= 0; index-- {
		if strings.EqualFold(strings.TrimSpace(pods[index].Status.Reason), "Evicted") {
			return &pods[index]
		}
	}
	return nil
}

func newestProblemPod(pods []kubePodInfo) *kubePodInfo {
	for index := len(pods) - 1; index >= 0; index-- {
		if summarizeKubePodFailure(pods[index]) != "" || !logPodReady(pods[index]) {
			return &pods[index]
		}
	}
	return nil
}

func newestServiceProcessExitSummary(pods []kubePodInfo) string {
	for index := len(pods) - 1; index >= 0; index-- {
		if summary := summarizeServiceProcessExit(pods[index]); summary != "" {
			return summary
		}
	}
	return ""
}

func summarizeServiceProcessExit(pod kubePodInfo) string {
	prefix := "pod " + strings.TrimSpace(pod.Metadata.Name)
	if node := strings.TrimSpace(pod.Spec.NodeName); node != "" {
		prefix += " on node " + node
	}
	for _, status := range pod.Status.ContainerStatuses {
		subject := prefix
		if name := strings.TrimSpace(status.Name); name != "" {
			subject += " container " + name
		}
		if status.State.Terminated != nil && serviceProcessExitedSuccessfully(*status.State.Terminated) && !status.Ready {
			return subject + " exited successfully instead of staying online"
		}
		if status.LastState.Terminated != nil && serviceProcessExitedSuccessfully(*status.LastState.Terminated) && !status.Ready && status.State.Waiting != nil {
			reason := strings.TrimSpace(status.State.Waiting.Reason)
			message := strings.TrimSpace(status.State.Waiting.Message)
			switch {
			case reason != "" && message != "":
				return fmt.Sprintf("%s exited successfully and is now waiting: %s: %s", subject, reason, message)
			case reason != "":
				return fmt.Sprintf("%s exited successfully and is now waiting: %s", subject, reason)
			case message != "":
				return fmt.Sprintf("%s exited successfully and is now waiting: %s", subject, message)
			default:
				return subject + " exited successfully and is now waiting to restart"
			}
		}
	}
	return ""
}

func serviceProcessExitedSuccessfully(detail kubeStateDetail) bool {
	return detail.ExitCode == 0 && strings.EqualFold(strings.TrimSpace(detail.Reason), "Completed")
}

func readAppDiagnosisCrashEvidence(ctx context.Context, logClient appLogsClient, namespace string, pod kubePodInfo) string {
	container := appDiagnosisProblemContainerName(pod)
	if container == "" {
		return ""
	}
	for _, previous := range []bool{true, false} {
		logs, err := logClient.readPodLogs(ctx, namespace, strings.TrimSpace(pod.Metadata.Name), kubeLogOptions{
			Container: container,
			TailLines: 40,
			Previous:  previous,
		})
		if err != nil {
			continue
		}
		snippet := summarizeAppDiagnosisLogSnippet(logs)
		if snippet == "" {
			continue
		}
		source := "previous"
		if !previous {
			source = "current"
		}
		return fmt.Sprintf("%s log %s: %s", container, source, snippet)
	}
	return ""
}

func appDiagnosisProblemContainerName(pod kubePodInfo) string {
	for _, status := range pod.Status.ContainerStatuses {
		switch {
		case status.State.Waiting != nil:
			return strings.TrimSpace(status.Name)
		case status.State.Terminated != nil:
			return strings.TrimSpace(status.Name)
		case status.LastState.Terminated != nil:
			return strings.TrimSpace(status.Name)
		case !status.Ready:
			return strings.TrimSpace(status.Name)
		}
	}
	for _, status := range pod.Status.InitContainerStatuses {
		switch {
		case status.State.Waiting != nil:
			return strings.TrimSpace(status.Name)
		case status.State.Terminated != nil:
			return strings.TrimSpace(status.Name)
		case status.LastState.Terminated != nil:
			return strings.TrimSpace(status.Name)
		case !status.Ready:
			return strings.TrimSpace(status.Name)
		}
	}
	if len(pod.Spec.Containers) > 0 {
		return strings.TrimSpace(pod.Spec.Containers[0].Name)
	}
	if len(pod.Spec.InitContainers) > 0 {
		return strings.TrimSpace(pod.Spec.InitContainers[0].Name)
	}
	return ""
}

func summarizeAppDiagnosisLogSnippet(logs string) string {
	lines := strings.Split(logs, "\n")
	for index := len(lines) - 1; index >= 0; index-- {
		line := strings.TrimSpace(lines[index])
		if line == "" {
			continue
		}
		if len(line) > 200 {
			line = strings.TrimSpace(line[:200]) + "..."
		}
		return line
	}
	return ""
}

func countReadyLogPods(pods []kubePodInfo) int {
	count := 0
	for _, pod := range pods {
		if logPodReady(pod) {
			count++
		}
	}
	return count
}

func activeLogPods(pods []kubePodInfo) []kubePodInfo {
	if len(pods) == 0 {
		return []kubePodInfo{}
	}
	active := make([]kubePodInfo, 0, len(pods))
	for _, pod := range pods {
		if !logPodTerminal(pod) {
			active = append(active, pod)
		}
	}
	return active
}

func logPodTerminal(pod kubePodInfo) bool {
	switch strings.ToLower(strings.TrimSpace(pod.Status.Phase)) {
	case "failed", "succeeded":
		return true
	default:
		return false
	}
}

func containsVolumeAffinityConflict(message string) bool {
	normalized := strings.ToLower(strings.TrimSpace(message))
	return strings.Contains(normalized, "volume node affinity conflict") ||
		strings.Contains(normalized, "had volume node affinity conflict") ||
		strings.Contains(normalized, "volume affinity")
}

func buildAppDiagnosisHint(app model.App, nodeName string) string {
	if strings.TrimSpace(nodeName) != "" {
		return fmt.Sprintf(
			"Inspect pod history with fugue app logs pods %s. If you have admin access, run fugue admin cluster node inspect %s for host disk, kubelet journal, and metrics evidence.",
			strings.TrimSpace(app.Name),
			strings.TrimSpace(nodeName),
		)
	}
	return fmt.Sprintf(
		"Inspect pod history with fugue app logs pods %s and runtime logs with fugue app logs runtime %s --previous.",
		strings.TrimSpace(app.Name),
		strings.TrimSpace(app.Name),
	)
}
