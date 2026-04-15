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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type replicaSetInventoryClient interface {
	listReplicaSetsBySelector(ctx context.Context, namespace, selector string) ([]appsv1.ReplicaSet, error)
}

func (s *Server) handleGetAppRuntimePods(w http.ResponseWriter, r *http.Request) {
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

	runtimeObj, err := s.store.GetRuntime(app.Spec.RuntimeID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if runtimeObj.Type == model.RuntimeTypeExternalOwned {
		httpx.WriteError(w, http.StatusBadRequest, "runtime pod inventory is only available for managed runtimes")
		return
	}

	namespace := runtime.NamespaceForTenant(app.TenantID)
	logClient, err := s.newLogsClient(namespace)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	selector, containerName, err := runtimeLogTarget(app, component)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	pods, err := logClient.listPodsBySelector(r.Context(), namespace, selector)
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}

	inventory := buildAppRuntimePodInventory(component, namespace, selector, containerName, pods)
	if component == "app" {
		if replicaSets, warn := loadAppReplicaSetHistory(r.Context(), logClient, namespace, selector); warn != "" {
			inventory.Warnings = append(inventory.Warnings, warn)
		} else {
			inventory.Groups = mergeReplicaSetGroups(inventory.Groups, replicaSets)
		}
	}

	s.appendAudit(principal, "app.runtime_pods.read", "app", app.ID, app.TenantID, map[string]string{"component": component})
	httpx.WriteJSON(w, http.StatusOK, inventory)
}

func loadAppReplicaSetHistory(ctx context.Context, logClient appLogsClient, namespace, selector string) ([]appsv1.ReplicaSet, string) {
	replicaSetClient, ok := any(logClient).(replicaSetInventoryClient)
	if !ok {
		return nil, "replicaset history is unavailable for this logs client"
	}
	replicaSets, err := replicaSetClient.listReplicaSetsBySelector(ctx, namespace, selector)
	if err != nil {
		return nil, fmt.Sprintf("replicaset history unavailable: %v", err)
	}
	return replicaSets, ""
}

func buildAppRuntimePodInventory(component, namespace, selector, containerName string, pods []kubePodInfo) model.AppRuntimePodInventory {
	groups := appRuntimePodGroupsFromPods(namespace, pods)
	if groups == nil {
		groups = []model.AppRuntimePodGroup{}
	}
	inventory := model.AppRuntimePodInventory{
		Component: component,
		Namespace: namespace,
		Selector:  selector,
		Container: containerName,
		Groups:    groups,
		Warnings:  []string{},
	}
	return inventory
}

func mergeReplicaSetGroups(groups []model.AppRuntimePodGroup, replicaSets []appsv1.ReplicaSet) []model.AppRuntimePodGroup {
	if len(replicaSets) == 0 {
		return groups
	}

	byOwner := make(map[string]model.AppRuntimePodGroup, len(groups))
	for _, group := range groups {
		byOwner[appRuntimePodGroupKey(group.OwnerKind, group.OwnerName)] = group
	}

	sort.Slice(replicaSets, func(i, j int) bool {
		left := replicaSets[i].CreationTimestamp.Time
		right := replicaSets[j].CreationTimestamp.Time
		if !left.Equal(right) {
			return left.After(right)
		}
		return replicaSets[i].Name < replicaSets[j].Name
	})

	merged := make([]model.AppRuntimePodGroup, 0, len(replicaSets)+len(groups))
	seen := make(map[string]struct{}, len(replicaSets))
	for _, replicaSet := range replicaSets {
		key := appRuntimePodGroupKey("ReplicaSet", strings.TrimSpace(replicaSet.Name))
		existing := byOwner[key]
		group := model.AppRuntimePodGroup{
			OwnerKind:         "ReplicaSet",
			OwnerName:         strings.TrimSpace(replicaSet.Name),
			Parent:            ownerReferenceFromMeta(replicaSet.OwnerReferences),
			Revision:          strings.TrimSpace(replicaSet.Annotations["deployment.kubernetes.io/revision"]),
			CreatedAt:         cloneOptionalTime(&replicaSet.CreationTimestamp),
			DesiredReplicas:   replicaSet.Spec.Replicas,
			ReadyReplicas:     int32Ptr(replicaSet.Status.ReadyReplicas),
			AvailableReplicas: int32Ptr(replicaSet.Status.AvailableReplicas),
			CurrentReplicas:   int32Ptr(replicaSet.Status.Replicas),
			Containers:        workloadContainers(replicaSet.Spec.Template.Spec.Containers),
			Pods:              sortAppRuntimePods(existing.Pods),
		}
		merged = append(merged, group)
		seen[key] = struct{}{}
	}

	for _, group := range groups {
		key := appRuntimePodGroupKey(group.OwnerKind, group.OwnerName)
		if _, ok := seen[key]; ok {
			continue
		}
		group.Pods = sortAppRuntimePods(group.Pods)
		merged = append(merged, group)
	}
	return sortAppRuntimePodGroups(merged)
}

func appRuntimePodGroupsFromPods(namespace string, pods []kubePodInfo) []model.AppRuntimePodGroup {
	if len(pods) == 0 {
		return nil
	}

	groups := map[string]*model.AppRuntimePodGroup{}
	for _, pod := range pods {
		owner := ownerReferenceFromLogPod(pod)
		ownerKind := "Pod"
		ownerName := strings.TrimSpace(pod.Metadata.Name)
		if owner != nil {
			ownerKind = owner.Kind
			ownerName = owner.Name
		}
		key := appRuntimePodGroupKey(ownerKind, ownerName)
		group, ok := groups[key]
		if !ok {
			group = &model.AppRuntimePodGroup{
				OwnerKind: ownerKind,
				OwnerName: ownerName,
				Pods:      []model.ClusterPod{},
			}
			groups[key] = group
		}
		group.Pods = append(group.Pods, clusterPodFromLogPod(namespace, pod))
		if group.CreatedAt == nil {
			group.CreatedAt = firstNonNilTime(group.CreatedAt, pod.Status.StartTime, timePtrFromTime(pod.Metadata.CreationTimestamp))
		}
		if len(group.Containers) == 0 {
			group.Containers = clusterWorkloadContainersFromLogPod(pod)
		}
	}

	out := make([]model.AppRuntimePodGroup, 0, len(groups))
	for _, group := range groups {
		group.Pods = sortAppRuntimePods(group.Pods)
		out = append(out, *group)
	}
	return sortAppRuntimePodGroups(out)
}

func clusterPodFromLogPod(namespace string, pod kubePodInfo) model.ClusterPod {
	namespace = firstNonEmptyString(strings.TrimSpace(pod.Metadata.Namespace), strings.TrimSpace(namespace))
	out := model.ClusterPod{
		Namespace:  namespace,
		Name:       strings.TrimSpace(pod.Metadata.Name),
		Phase:      strings.TrimSpace(pod.Status.Phase),
		NodeName:   strings.TrimSpace(pod.Spec.NodeName),
		PodIP:      strings.TrimSpace(pod.Status.PodIP),
		HostIP:     strings.TrimSpace(pod.Status.HostIP),
		QOSClass:   strings.TrimSpace(pod.Status.QOSClass),
		Labels:     cloneStringMap(pod.Metadata.Labels),
		Ready:      logPodReady(pod),
		StartTime:  firstNonNilTime(nil, pod.Status.StartTime, timePtrFromTime(pod.Metadata.CreationTimestamp)),
		Containers: logPodContainers(pod),
	}
	if owner := ownerReferenceFromLogPod(pod); owner != nil {
		out.Owner = owner
	}
	return out
}

func logPodReady(pod kubePodInfo) bool {
	statuses := append([]kubeContainerStatus(nil), pod.Status.ContainerStatuses...)
	if len(statuses) == 0 {
		return false
	}
	for _, status := range statuses {
		if !status.Ready {
			return false
		}
	}
	return true
}

func logPodContainers(pod kubePodInfo) []model.ClusterPodContainer {
	specImages := map[string]string{}
	for _, container := range pod.Spec.InitContainers {
		specImages[container.Name] = strings.TrimSpace(container.Image)
	}
	for _, container := range pod.Spec.Containers {
		specImages[container.Name] = strings.TrimSpace(container.Image)
	}

	out := make([]model.ClusterPodContainer, 0, len(pod.Status.InitContainerStatuses)+len(pod.Status.ContainerStatuses))
	appendStatus := func(status kubeContainerStatus) {
		state, reason, message := podContainerStateFromLogStatus(status)
		out = append(out, model.ClusterPodContainer{
			Name:         strings.TrimSpace(status.Name),
			Image:        firstNonEmptyString(strings.TrimSpace(status.Image), specImages[status.Name]),
			Ready:        status.Ready,
			RestartCount: status.RestartCount,
			State:        state,
			Reason:       reason,
			Message:      message,
		})
	}
	for _, status := range pod.Status.InitContainerStatuses {
		appendStatus(status)
	}
	for _, status := range pod.Status.ContainerStatuses {
		appendStatus(status)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out
}

func clusterWorkloadContainersFromLogPod(pod kubePodInfo) []model.ClusterWorkloadContainer {
	containers := make([]model.ClusterWorkloadContainer, 0, len(pod.Spec.Containers))
	for _, container := range pod.Spec.Containers {
		containers = append(containers, model.ClusterWorkloadContainer{
			Name:  strings.TrimSpace(container.Name),
			Image: strings.TrimSpace(container.Image),
		})
	}
	sort.Slice(containers, func(i, j int) bool {
		return containers[i].Name < containers[j].Name
	})
	return containers
}

func podContainerStateFromLogStatus(status kubeContainerStatus) (string, string, string) {
	switch {
	case status.State.Running != nil:
		return "running", "", ""
	case status.State.Waiting != nil:
		return "waiting", strings.TrimSpace(status.State.Waiting.Reason), strings.TrimSpace(status.State.Waiting.Message)
	case status.State.Terminated != nil:
		return "terminated", strings.TrimSpace(status.State.Terminated.Reason), strings.TrimSpace(status.State.Terminated.Message)
	default:
		return "", "", ""
	}
}

func ownerReferenceFromLogPod(pod kubePodInfo) *model.ClusterPodOwner {
	return ownerReferenceFromLogPodRefs(pod.Metadata.OwnerReferences)
}

func ownerReferenceFromLogPodRefs(refs []struct {
	Kind string `json:"kind,omitempty"`
	Name string `json:"name,omitempty"`
}) *model.ClusterPodOwner {
	if len(refs) == 0 {
		return nil
	}
	return &model.ClusterPodOwner{
		Kind: strings.TrimSpace(refs[0].Kind),
		Name: strings.TrimSpace(refs[0].Name),
	}
}

func ownerReferenceFromMeta(refs []metav1.OwnerReference) *model.ClusterPodOwner {
	if len(refs) == 0 {
		return nil
	}
	return &model.ClusterPodOwner{
		Kind: strings.TrimSpace(refs[0].Kind),
		Name: strings.TrimSpace(refs[0].Name),
	}
}

func appRuntimePodGroupKey(kind, name string) string {
	return strings.TrimSpace(kind) + "\x00" + strings.TrimSpace(name)
}

func sortAppRuntimePodGroups(groups []model.AppRuntimePodGroup) []model.AppRuntimePodGroup {
	if len(groups) == 0 {
		return []model.AppRuntimePodGroup{}
	}
	sorted := append([]model.AppRuntimePodGroup(nil), groups...)
	sort.Slice(sorted, func(i, j int) bool {
		left := appRuntimePodGroupSortTime(sorted[i])
		right := appRuntimePodGroupSortTime(sorted[j])
		if !left.Equal(right) {
			return left.After(right)
		}
		if sorted[i].OwnerKind != sorted[j].OwnerKind {
			return sorted[i].OwnerKind < sorted[j].OwnerKind
		}
		return sorted[i].OwnerName < sorted[j].OwnerName
	})
	return sorted
}

func appRuntimePodGroupSortTime(group model.AppRuntimePodGroup) time.Time {
	if group.CreatedAt != nil && !group.CreatedAt.IsZero() {
		return group.CreatedAt.UTC()
	}
	for _, pod := range group.Pods {
		if pod.StartTime != nil && !pod.StartTime.IsZero() {
			return pod.StartTime.UTC()
		}
	}
	return time.Time{}
}

func sortAppRuntimePods(pods []model.ClusterPod) []model.ClusterPod {
	if len(pods) == 0 {
		return []model.ClusterPod{}
	}
	sorted := append([]model.ClusterPod(nil), pods...)
	sort.Slice(sorted, func(i, j int) bool {
		left := time.Time{}
		right := time.Time{}
		if sorted[i].StartTime != nil {
			left = sorted[i].StartTime.UTC()
		}
		if sorted[j].StartTime != nil {
			right = sorted[j].StartTime.UTC()
		}
		if !left.Equal(right) {
			return left.After(right)
		}
		return sorted[i].Name < sorted[j].Name
	})
	return sorted
}

func firstNonNilTime(values ...*time.Time) *time.Time {
	for _, value := range values {
		if value == nil || value.IsZero() {
			continue
		}
		copied := value.UTC()
		return &copied
	}
	return nil
}

func timePtrFromTime(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	copied := value.UTC()
	return &copied
}
