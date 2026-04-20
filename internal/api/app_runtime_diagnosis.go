package api

import (
	"fmt"
	"net/http"
	"sort"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/runtime"

	corev1 "k8s.io/api/core/v1"
)

const appDiagnosisEventLimit = 12

type appDiagnosis struct {
	Category       string               `json:"category"`
	Summary        string               `json:"summary"`
	Hint           string               `json:"hint,omitempty"`
	Component      string               `json:"component,omitempty"`
	Namespace      string               `json:"namespace,omitempty"`
	Selector       string               `json:"selector,omitempty"`
	ImplicatedNode string               `json:"implicated_node,omitempty"`
	ImplicatedPod  string               `json:"implicated_pod,omitempty"`
	LivePods       int                  `json:"live_pods,omitempty"`
	ReadyPods      int                  `json:"ready_pods,omitempty"`
	Evidence       []string             `json:"evidence"`
	Warnings       []string             `json:"warnings"`
	Events         []model.ClusterEvent `json:"events"`
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

	s.appendAudit(principal, "app.diagnosis.read", "app", app.ID, app.TenantID, map[string]string{
		"component": component,
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"diagnosis": diagnosis})
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

	podSummaries := summarizeAppDiagnosisPods(pods)
	for _, summary := range podSummaries {
		diagnosis.Evidence = appendUniqueString(diagnosis.Evidence, summary)
	}

	evictedPod := newestEvictedPod(pods)
	schedulingEvent := newestFailedSchedulingEvent(rawNamespaceEvents)
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

	switch {
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
	case len(podSummaries) > 0:
		diagnosis.Category = "pod-failure"
		diagnosis.Summary = podSummaries[0]
		diagnosis.Hint = fmt.Sprintf("Inspect pod history with fugue app logs pods %s and runtime logs with fugue app logs runtime %s --previous.", strings.TrimSpace(app.Name), strings.TrimSpace(app.Name))
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
