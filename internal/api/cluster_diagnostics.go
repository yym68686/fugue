package api

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	clusterProbeDefaultTimeout = 5 * time.Second
	clusterExecTimeout         = 30 * time.Second
)

func (s *Server) handleListClusterPods(w http.ResponseWriter, r *http.Request) {
	principal, ok := requirePlatformAdmin(w, r)
	if !ok {
		return
	}

	client, err := s.requireClusterNodeClient()
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	includeTerminated, err := readBoolQuery(r, "include_terminated", false)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	pods, err := client.listCorePods(
		r.Context(),
		readOptionalStringQuery(r, "namespace"),
		readOptionalStringQuery(r, "label_selector"),
	)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	nodeName := readOptionalStringQuery(r, "node")
	out := make([]model.ClusterPod, 0, len(pods))
	for _, pod := range pods {
		if nodeName != "" && !strings.EqualFold(strings.TrimSpace(pod.Spec.NodeName), nodeName) {
			continue
		}
		if !includeTerminated {
			switch pod.Status.Phase {
			case corev1.PodSucceeded, corev1.PodFailed:
				continue
			}
		}
		out = append(out, clusterPodFromCore(pod))
	}
	sortClusterPods(out)

	s.appendAudit(principal, "cluster.pods.list", "cluster", "pods", principal.TenantID, map[string]string{
		"namespace": readOptionalStringQuery(r, "namespace"),
		"node":      nodeName,
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"cluster_pods": out})
}

func (s *Server) handleListClusterEvents(w http.ResponseWriter, r *http.Request) {
	principal, ok := requirePlatformAdmin(w, r)
	if !ok {
		return
	}

	client, err := s.requireClusterNodeClient()
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	limit, err := readIntQuery(r, "limit", 0)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	events, err := client.listCoreEvents(r.Context(), readOptionalStringQuery(r, "namespace"))
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	filterKind := strings.TrimSpace(readOptionalStringQuery(r, "kind"))
	filterName := strings.TrimSpace(readOptionalStringQuery(r, "name"))
	filterType := strings.TrimSpace(readOptionalStringQuery(r, "type"))
	filterReason := strings.TrimSpace(readOptionalStringQuery(r, "reason"))

	out := make([]model.ClusterEvent, 0, len(events))
	for _, event := range events {
		if filterKind != "" && !strings.EqualFold(strings.TrimSpace(event.InvolvedObject.Kind), filterKind) {
			continue
		}
		if filterName != "" && !strings.EqualFold(strings.TrimSpace(event.InvolvedObject.Name), filterName) {
			continue
		}
		if filterType != "" && !strings.EqualFold(strings.TrimSpace(event.Type), filterType) {
			continue
		}
		if filterReason != "" && !strings.EqualFold(strings.TrimSpace(event.Reason), filterReason) {
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
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		if out[i].ObjectKind != out[j].ObjectKind {
			return out[i].ObjectKind < out[j].ObjectKind
		}
		if out[i].ObjectName != out[j].ObjectName {
			return out[i].ObjectName < out[j].ObjectName
		}
		return out[i].Name < out[j].Name
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}

	s.appendAudit(principal, "cluster.events.list", "cluster", "events", principal.TenantID, map[string]string{
		"namespace": readOptionalStringQuery(r, "namespace"),
		"kind":      filterKind,
		"name":      filterName,
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"events": out})
}

func (s *Server) handleGetClusterLogs(w http.ResponseWriter, r *http.Request) {
	principal, ok := requirePlatformAdmin(w, r)
	if !ok {
		return
	}

	namespace := strings.TrimSpace(readOptionalStringQuery(r, "namespace"))
	podName := strings.TrimSpace(readOptionalStringQuery(r, "pod"))
	if namespace == "" || podName == "" {
		httpx.WriteError(w, http.StatusBadRequest, "namespace and pod are required")
		return
	}

	tailLines, err := readIntQuery(r, "tail_lines", 200)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	previous, err := readBoolQuery(r, "previous", false)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	logClient, err := s.newLogsClient(namespace)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	logs, err := logClient.readPodLogs(r.Context(), namespace, podName, kubeLogOptions{
		Container: strings.TrimSpace(readOptionalStringQuery(r, "container")),
		TailLines: tailLines,
		Previous:  previous,
	})
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	containerName := strings.TrimSpace(readOptionalStringQuery(r, "container"))
	s.appendAudit(principal, "cluster.logs.read", "pod", namespace+"/"+podName, principal.TenantID, map[string]string{
		"namespace": namespace,
		"pod":       podName,
		"container": containerName,
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"namespace": namespace,
		"pod":       podName,
		"container": containerName,
		"logs":      logs,
	})
}

func (s *Server) handleExecClusterPod(w http.ResponseWriter, r *http.Request) {
	principal, ok := requirePlatformAdmin(w, r)
	if !ok {
		return
	}

	var req struct {
		Namespace    string   `json:"namespace"`
		Pod          string   `json:"pod"`
		Container    string   `json:"container"`
		Command      []string `json:"command"`
		Retries      int      `json:"retries"`
		RetryDelayMS int      `json:"retry_delay_ms"`
		TimeoutMS    int      `json:"timeout_ms"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	req.Namespace = strings.TrimSpace(req.Namespace)
	req.Pod = strings.TrimSpace(req.Pod)
	req.Container = strings.TrimSpace(req.Container)
	req.Command = trimStringSlice(req.Command)
	if req.Namespace == "" || req.Pod == "" || len(req.Command) == 0 {
		httpx.WriteError(w, http.StatusBadRequest, "namespace, pod, and command are required")
		return
	}
	if req.Retries < 0 {
		httpx.WriteError(w, http.StatusBadRequest, "retries cannot be negative")
		return
	}
	if req.RetryDelayMS < 0 {
		httpx.WriteError(w, http.StatusBadRequest, "retry_delay_ms cannot be negative")
		return
	}
	if req.TimeoutMS < 0 {
		httpx.WriteError(w, http.StatusBadRequest, "timeout_ms cannot be negative")
		return
	}

	runner := s.filesystemExecRunner
	if runner == nil {
		runner = kubeFilesystemExecRunner{}
	}
	timeout := clusterExecTimeout
	if req.TimeoutMS > 0 {
		timeout = time.Duration(req.TimeoutMS) * time.Millisecond
	}
	commandCtx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	output, attempts, err := runClusterExecWithRetries(
		commandCtx,
		runner,
		req.Namespace,
		req.Pod,
		req.Container,
		req.Command,
		req.Retries,
		time.Duration(req.RetryDelayMS)*time.Millisecond,
	)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	s.appendAudit(principal, "cluster.exec", "pod", req.Namespace+"/"+req.Pod, principal.TenantID, map[string]string{
		"namespace": req.Namespace,
		"pod":       req.Pod,
		"container": req.Container,
		"command":   strings.Join(req.Command, " "),
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"namespace":     req.Namespace,
		"pod":           req.Pod,
		"container":     req.Container,
		"command":       req.Command,
		"output":        string(output),
		"attempt_count": attempts,
	})
}

func (s *Server) handleResolveClusterDNS(w http.ResponseWriter, r *http.Request) {
	principal, ok := requirePlatformAdmin(w, r)
	if !ok {
		return
	}

	recordType := strings.TrimSpace(readOptionalStringQuery(r, "type"))
	if recordType == "" {
		recordType = "A"
	}
	result, err := resolveClusterDNS(
		r.Context(),
		strings.TrimSpace(readOptionalStringQuery(r, "name")),
		strings.TrimSpace(readOptionalStringQuery(r, "server")),
		recordType,
		clusterProbeDefaultTimeout,
	)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.appendAudit(principal, "cluster.dns.resolve", "cluster", result.Name, principal.TenantID, map[string]string{
		"name":        result.Name,
		"record_type": result.RecordType,
		"server":      result.Server,
	})
	httpx.WriteJSON(w, http.StatusOK, result)
}

func (s *Server) handleConnectClusterNetwork(w http.ResponseWriter, r *http.Request) {
	principal, ok := requirePlatformAdmin(w, r)
	if !ok {
		return
	}

	timeout, err := readDurationMillisQuery(r, "timeout_ms", clusterProbeDefaultTimeout)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	result, err := probeClusterNetwork(
		r.Context(),
		strings.TrimSpace(readOptionalStringQuery(r, "target")),
		timeout,
	)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.appendAudit(principal, "cluster.net.connect", "cluster", result.Target, principal.TenantID, map[string]string{
		"target": result.Target,
	})
	httpx.WriteJSON(w, http.StatusOK, result)
}

func (s *Server) handleProbeClusterTLS(w http.ResponseWriter, r *http.Request) {
	principal, ok := requirePlatformAdmin(w, r)
	if !ok {
		return
	}

	timeout, err := readDurationMillisQuery(r, "timeout_ms", clusterProbeDefaultTimeout)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	result, err := probeClusterTLS(
		r.Context(),
		strings.TrimSpace(readOptionalStringQuery(r, "target")),
		strings.TrimSpace(readOptionalStringQuery(r, "server_name")),
		timeout,
	)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.appendAudit(principal, "cluster.tls.probe", "cluster", result.Target, principal.TenantID, map[string]string{
		"target":      result.Target,
		"server_name": result.ServerName,
	})
	httpx.WriteJSON(w, http.StatusOK, result)
}

func (s *Server) handleGetClusterWorkload(w http.ResponseWriter, r *http.Request) {
	principal, ok := requirePlatformAdmin(w, r)
	if !ok {
		return
	}

	client, err := s.requireClusterNodeClient()
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	workload, err := client.readClusterWorkloadDetail(
		r.Context(),
		r.PathValue("namespace"),
		r.PathValue("kind"),
		r.PathValue("name"),
	)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	s.appendAudit(principal, "cluster.workload.read", "workload", workload.Namespace+"/"+workload.Kind+"/"+workload.Name, principal.TenantID, nil)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"workload": workload})
}

func (s *Server) handleGetClusterRolloutStatus(w http.ResponseWriter, r *http.Request) {
	principal, ok := requirePlatformAdmin(w, r)
	if !ok {
		return
	}

	client, err := s.requireClusterNodeClient()
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	workload, err := client.readClusterWorkloadDetail(
		r.Context(),
		r.PathValue("namespace"),
		r.PathValue("kind"),
		r.PathValue("name"),
	)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	rollout := rolloutStatusFromWorkload(workload)
	s.appendAudit(principal, "cluster.rollout.status", "workload", workload.Namespace+"/"+workload.Kind+"/"+workload.Name, principal.TenantID, nil)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"rollout": rollout})
}

func requirePlatformAdmin(w http.ResponseWriter, r *http.Request) (model.Principal, bool) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return model.Principal{}, false
	}
	return principal, true
}

func (s *Server) requireClusterNodeClient() (*clusterNodeClient, error) {
	clientFactory := s.newClusterNodeClient
	if clientFactory == nil {
		clientFactory = newClusterNodeClient
	}
	return clientFactory()
}

func (c *clusterNodeClient) listCorePods(ctx context.Context, namespace, selector string) ([]corev1.Pod, error) {
	query := url.Values{}
	if strings.TrimSpace(selector) != "" {
		query.Set("labelSelector", strings.TrimSpace(selector))
	}

	var pods corev1.PodList
	apiPath := "/api/v1/pods"
	if ns := strings.TrimSpace(namespace); ns != "" {
		apiPath = "/api/v1/namespaces/" + url.PathEscape(ns) + "/pods"
	}
	if encoded := query.Encode(); encoded != "" {
		apiPath += "?" + encoded
	}
	if err := c.doJSON(ctx, http.MethodGet, apiPath, &pods); err != nil {
		return nil, err
	}
	return pods.Items, nil
}

func (c *clusterNodeClient) listCoreEvents(ctx context.Context, namespace string) ([]corev1.Event, error) {
	var events corev1.EventList
	apiPath := "/api/v1/events"
	if ns := strings.TrimSpace(namespace); ns != "" {
		apiPath = "/api/v1/namespaces/" + url.PathEscape(ns) + "/events"
	}
	if err := c.doJSON(ctx, http.MethodGet, apiPath, &events); err != nil {
		return nil, err
	}
	return events.Items, nil
}

func (c *clusterNodeClient) readClusterWorkloadDetail(
	ctx context.Context,
	namespace string,
	kind string,
	name string,
) (model.ClusterWorkloadDetail, error) {
	namespace = strings.TrimSpace(namespace)
	name = strings.TrimSpace(name)
	kind, err := normalizeClusterWorkloadKind(kind)
	if err != nil {
		return model.ClusterWorkloadDetail{}, err
	}
	if namespace == "" || name == "" {
		return model.ClusterWorkloadDetail{}, fmt.Errorf("namespace and name are required")
	}

	switch kind {
	case "deployment":
		var workload appsv1.Deployment
		if err := c.doJSON(ctx, http.MethodGet, clusterWorkloadAPIPath(namespace, kind, name), &workload); err != nil {
			return model.ClusterWorkloadDetail{}, err
		}
		return c.clusterWorkloadDetailForDeployment(ctx, workload)
	case "daemonset":
		var workload appsv1.DaemonSet
		if err := c.doJSON(ctx, http.MethodGet, clusterWorkloadAPIPath(namespace, kind, name), &workload); err != nil {
			return model.ClusterWorkloadDetail{}, err
		}
		return c.clusterWorkloadDetailForDaemonSet(ctx, workload)
	case "statefulset":
		var workload appsv1.StatefulSet
		if err := c.doJSON(ctx, http.MethodGet, clusterWorkloadAPIPath(namespace, kind, name), &workload); err != nil {
			return model.ClusterWorkloadDetail{}, err
		}
		return c.clusterWorkloadDetailForStatefulSet(ctx, workload)
	case "pod":
		var workload corev1.Pod
		if err := c.doJSON(ctx, http.MethodGet, clusterWorkloadAPIPath(namespace, kind, name), &workload); err != nil {
			return model.ClusterWorkloadDetail{}, err
		}
		return clusterWorkloadDetailForPod(workload), nil
	default:
		return model.ClusterWorkloadDetail{}, fmt.Errorf("unsupported workload kind %q", kind)
	}
}

func (c *clusterNodeClient) clusterWorkloadDetailForDeployment(ctx context.Context, workload appsv1.Deployment) (model.ClusterWorkloadDetail, error) {
	selector, pods, err := c.loadWorkloadPods(ctx, workload.Namespace, workload.Spec.Selector)
	if err != nil {
		return model.ClusterWorkloadDetail{}, err
	}
	manifest, err := manifestMap(workload)
	if err != nil {
		return model.ClusterWorkloadDetail{}, err
	}
	return model.ClusterWorkloadDetail{
		APIVersion:        workload.APIVersion,
		Kind:              workload.Kind,
		Namespace:         workload.Namespace,
		Name:              workload.Name,
		Selector:          selector,
		Labels:            cloneStringMap(workload.Labels),
		Annotations:       cloneStringMap(workload.Annotations),
		NodeSelector:      cloneStringMap(workload.Spec.Template.Spec.NodeSelector),
		Tolerations:       formatTolerations(workload.Spec.Template.Spec.Tolerations),
		Containers:        workloadContainers(workload.Spec.Template.Spec.Containers),
		InitContainers:    workloadContainers(workload.Spec.Template.Spec.InitContainers),
		DesiredReplicas:   workload.Spec.Replicas,
		ReadyReplicas:     int32Ptr(workload.Status.ReadyReplicas),
		UpdatedReplicas:   int32Ptr(workload.Status.UpdatedReplicas),
		AvailableReplicas: int32Ptr(workload.Status.AvailableReplicas),
		CurrentReplicas:   int32Ptr(workload.Status.Replicas),
		Conditions:        deploymentConditions(workload.Status.Conditions),
		Pods:              pods,
		Manifest:          manifest,
	}, nil
}

func (c *clusterNodeClient) clusterWorkloadDetailForDaemonSet(ctx context.Context, workload appsv1.DaemonSet) (model.ClusterWorkloadDetail, error) {
	selector, pods, err := c.loadWorkloadPods(ctx, workload.Namespace, workload.Spec.Selector)
	if err != nil {
		return model.ClusterWorkloadDetail{}, err
	}
	manifest, err := manifestMap(workload)
	if err != nil {
		return model.ClusterWorkloadDetail{}, err
	}
	return model.ClusterWorkloadDetail{
		APIVersion:        workload.APIVersion,
		Kind:              workload.Kind,
		Namespace:         workload.Namespace,
		Name:              workload.Name,
		Selector:          selector,
		Labels:            cloneStringMap(workload.Labels),
		Annotations:       cloneStringMap(workload.Annotations),
		NodeSelector:      cloneStringMap(workload.Spec.Template.Spec.NodeSelector),
		Tolerations:       formatTolerations(workload.Spec.Template.Spec.Tolerations),
		Containers:        workloadContainers(workload.Spec.Template.Spec.Containers),
		InitContainers:    workloadContainers(workload.Spec.Template.Spec.InitContainers),
		DesiredReplicas:   int32Ptr(workload.Status.DesiredNumberScheduled),
		ReadyReplicas:     int32Ptr(workload.Status.NumberReady),
		UpdatedReplicas:   int32Ptr(workload.Status.UpdatedNumberScheduled),
		AvailableReplicas: int32Ptr(workload.Status.NumberAvailable),
		CurrentReplicas:   int32Ptr(workload.Status.CurrentNumberScheduled),
		Conditions:        daemonSetConditions(workload.Status.Conditions),
		Pods:              pods,
		Manifest:          manifest,
	}, nil
}

func (c *clusterNodeClient) clusterWorkloadDetailForStatefulSet(ctx context.Context, workload appsv1.StatefulSet) (model.ClusterWorkloadDetail, error) {
	selector, pods, err := c.loadWorkloadPods(ctx, workload.Namespace, workload.Spec.Selector)
	if err != nil {
		return model.ClusterWorkloadDetail{}, err
	}
	manifest, err := manifestMap(workload)
	if err != nil {
		return model.ClusterWorkloadDetail{}, err
	}
	return model.ClusterWorkloadDetail{
		APIVersion:        workload.APIVersion,
		Kind:              workload.Kind,
		Namespace:         workload.Namespace,
		Name:              workload.Name,
		Selector:          selector,
		Labels:            cloneStringMap(workload.Labels),
		Annotations:       cloneStringMap(workload.Annotations),
		NodeSelector:      cloneStringMap(workload.Spec.Template.Spec.NodeSelector),
		Tolerations:       formatTolerations(workload.Spec.Template.Spec.Tolerations),
		Containers:        workloadContainers(workload.Spec.Template.Spec.Containers),
		InitContainers:    workloadContainers(workload.Spec.Template.Spec.InitContainers),
		DesiredReplicas:   workload.Spec.Replicas,
		ReadyReplicas:     int32Ptr(workload.Status.ReadyReplicas),
		UpdatedReplicas:   int32Ptr(workload.Status.UpdatedReplicas),
		AvailableReplicas: int32Ptr(workload.Status.AvailableReplicas),
		CurrentReplicas:   int32Ptr(workload.Status.CurrentReplicas),
		Conditions:        statefulSetConditions(workload.Status.Conditions),
		Pods:              pods,
		Manifest:          manifest,
	}, nil
}

func clusterWorkloadDetailForPod(workload corev1.Pod) model.ClusterWorkloadDetail {
	manifest, _ := manifestMap(workload)
	return model.ClusterWorkloadDetail{
		APIVersion:     workload.APIVersion,
		Kind:           workload.Kind,
		Namespace:      workload.Namespace,
		Name:           workload.Name,
		Labels:         cloneStringMap(workload.Labels),
		Annotations:    cloneStringMap(workload.Annotations),
		NodeSelector:   cloneStringMap(workload.Spec.NodeSelector),
		Tolerations:    formatTolerations(workload.Spec.Tolerations),
		Containers:     workloadContainers(workload.Spec.Containers),
		InitContainers: workloadContainers(workload.Spec.InitContainers),
		Conditions:     podConditions(workload.Status.Conditions),
		Pods:           []model.ClusterPod{clusterPodFromCore(workload)},
		Manifest:       manifest,
	}
}

func (c *clusterNodeClient) loadWorkloadPods(
	ctx context.Context,
	namespace string,
	selector *metav1.LabelSelector,
) (string, []model.ClusterPod, error) {
	if selector == nil {
		return "", nil, nil
	}
	compiled, err := metav1.LabelSelectorAsSelector(selector)
	if err != nil {
		return "", nil, fmt.Errorf("build workload selector: %w", err)
	}
	selectorText := compiled.String()
	if selectorText == "" {
		return "", nil, nil
	}
	pods, err := c.listCorePods(ctx, namespace, selectorText)
	if err != nil {
		return "", nil, fmt.Errorf("list workload pods: %w", err)
	}
	out := make([]model.ClusterPod, 0, len(pods))
	for _, pod := range pods {
		out = append(out, clusterPodFromCore(pod))
	}
	sortClusterPods(out)
	return selectorText, out, nil
}

func clusterWorkloadAPIPath(namespace, kind, name string) string {
	namespace = url.PathEscape(strings.TrimSpace(namespace))
	name = url.PathEscape(strings.TrimSpace(name))
	switch kind {
	case "deployment":
		return "/apis/apps/v1/namespaces/" + namespace + "/deployments/" + name
	case "daemonset":
		return "/apis/apps/v1/namespaces/" + namespace + "/daemonsets/" + name
	case "statefulset":
		return "/apis/apps/v1/namespaces/" + namespace + "/statefulsets/" + name
	case "pod":
		return "/api/v1/namespaces/" + namespace + "/pods/" + name
	default:
		return ""
	}
}

func normalizeClusterWorkloadKind(kind string) (string, error) {
	switch strings.TrimSpace(strings.ToLower(kind)) {
	case "deployment", "deploy", "deployments":
		return "deployment", nil
	case "daemonset", "daemonsets", "ds":
		return "daemonset", nil
	case "statefulset", "statefulsets", "sts":
		return "statefulset", nil
	case "pod", "pods":
		return "pod", nil
	default:
		return "", fmt.Errorf("unsupported workload kind %q", kind)
	}
}

func sortClusterPods(pods []model.ClusterPod) {
	sort.Slice(pods, func(i, j int) bool {
		if pods[i].Namespace != pods[j].Namespace {
			return pods[i].Namespace < pods[j].Namespace
		}
		if pods[i].NodeName != pods[j].NodeName {
			return pods[i].NodeName < pods[j].NodeName
		}
		return pods[i].Name < pods[j].Name
	})
}

func clusterPodFromCore(pod corev1.Pod) model.ClusterPod {
	out := model.ClusterPod{
		Namespace:  strings.TrimSpace(pod.Namespace),
		Name:       strings.TrimSpace(pod.Name),
		Phase:      string(pod.Status.Phase),
		NodeName:   strings.TrimSpace(pod.Spec.NodeName),
		PodIP:      strings.TrimSpace(pod.Status.PodIP),
		HostIP:     strings.TrimSpace(pod.Status.HostIP),
		QOSClass:   string(pod.Status.QOSClass),
		Labels:     cloneStringMap(pod.Labels),
		Ready:      podReady(pod),
		StartTime:  cloneOptionalTime(pod.Status.StartTime),
		Containers: podContainers(pod),
	}
	if len(pod.OwnerReferences) > 0 {
		out.Owner = &model.ClusterPodOwner{
			Kind: strings.TrimSpace(pod.OwnerReferences[0].Kind),
			Name: strings.TrimSpace(pod.OwnerReferences[0].Name),
		}
	}
	return out
}

func podContainers(pod corev1.Pod) []model.ClusterPodContainer {
	specImages := make(map[string]string, len(pod.Spec.InitContainers)+len(pod.Spec.Containers))
	for _, container := range pod.Spec.InitContainers {
		specImages[container.Name] = container.Image
	}
	for _, container := range pod.Spec.Containers {
		specImages[container.Name] = container.Image
	}

	out := make([]model.ClusterPodContainer, 0, len(pod.Status.InitContainerStatuses)+len(pod.Status.ContainerStatuses))
	appendStatus := func(status corev1.ContainerStatus) {
		state, reason, message := podContainerState(status)
		out = append(out, model.ClusterPodContainer{
			Name:         strings.TrimSpace(status.Name),
			Image:        firstNonEmptyString(specImages[status.Name], status.Image),
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

func podContainerState(status corev1.ContainerStatus) (string, string, string) {
	switch {
	case status.State.Running != nil:
		return "running", "", ""
	case status.State.Waiting != nil:
		return "waiting", strings.TrimSpace(status.State.Waiting.Reason), strings.TrimSpace(status.State.Waiting.Message)
	case status.State.Terminated != nil:
		return "terminated", strings.TrimSpace(status.State.Terminated.Reason), strings.TrimSpace(status.State.Terminated.Message)
	default:
		return "unknown", "", ""
	}
}

func podReady(pod corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func clusterEventFromCore(event corev1.Event) model.ClusterEvent {
	return model.ClusterEvent{
		Namespace:       strings.TrimSpace(event.Namespace),
		Name:            strings.TrimSpace(event.Name),
		Type:            strings.TrimSpace(event.Type),
		Reason:          strings.TrimSpace(event.Reason),
		Message:         strings.TrimSpace(event.Message),
		ObjectKind:      strings.TrimSpace(event.InvolvedObject.Kind),
		ObjectName:      strings.TrimSpace(event.InvolvedObject.Name),
		ObjectNamespace: strings.TrimSpace(event.InvolvedObject.Namespace),
		Count:           event.Count,
		FirstTimestamp:  nonZeroTime(event.FirstTimestamp.Time),
		LastTimestamp:   nonZeroTime(event.LastTimestamp.Time),
		EventTime:       nonZeroTime(event.EventTime.Time),
	}
}

func clusterEventSortTime(event model.ClusterEvent) time.Time {
	for _, candidate := range []*time.Time{event.EventTime, event.LastTimestamp, event.FirstTimestamp} {
		if candidate != nil {
			return candidate.UTC()
		}
	}
	return time.Time{}
}

func workloadContainers(containers []corev1.Container) []model.ClusterWorkloadContainer {
	if len(containers) == 0 {
		return nil
	}
	out := make([]model.ClusterWorkloadContainer, 0, len(containers))
	for _, container := range containers {
		out = append(out, model.ClusterWorkloadContainer{
			Name:  strings.TrimSpace(container.Name),
			Image: strings.TrimSpace(container.Image),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out
}

func formatTolerations(tolerations []corev1.Toleration) []string {
	if len(tolerations) == 0 {
		return nil
	}
	out := make([]string, 0, len(tolerations))
	for _, toleration := range tolerations {
		parts := make([]string, 0, 4)
		if key := strings.TrimSpace(toleration.Key); key != "" {
			part := key
			if value := strings.TrimSpace(toleration.Value); value != "" {
				part += "=" + value
			}
			parts = append(parts, part)
		}
		if operator := strings.TrimSpace(string(toleration.Operator)); operator != "" {
			parts = append(parts, "op="+operator)
		}
		if effect := strings.TrimSpace(string(toleration.Effect)); effect != "" {
			parts = append(parts, "effect="+effect)
		}
		if toleration.TolerationSeconds != nil {
			parts = append(parts, "seconds="+strconv.FormatInt(int64(*toleration.TolerationSeconds), 10))
		}
		out = append(out, strings.Join(parts, " "))
	}
	sort.Strings(out)
	return out
}

func deploymentConditions(conditions []appsv1.DeploymentCondition) []model.ClusterWorkloadCondition {
	out := make([]model.ClusterWorkloadCondition, 0, len(conditions))
	for _, condition := range conditions {
		out = append(out, model.ClusterWorkloadCondition{
			Type:    string(condition.Type),
			Status:  string(condition.Status),
			Reason:  strings.TrimSpace(condition.Reason),
			Message: strings.TrimSpace(condition.Message),
		})
	}
	return out
}

func daemonSetConditions(conditions []appsv1.DaemonSetCondition) []model.ClusterWorkloadCondition {
	out := make([]model.ClusterWorkloadCondition, 0, len(conditions))
	for _, condition := range conditions {
		out = append(out, model.ClusterWorkloadCondition{
			Type:    string(condition.Type),
			Status:  string(condition.Status),
			Reason:  strings.TrimSpace(condition.Reason),
			Message: strings.TrimSpace(condition.Message),
		})
	}
	return out
}

func statefulSetConditions(conditions []appsv1.StatefulSetCondition) []model.ClusterWorkloadCondition {
	out := make([]model.ClusterWorkloadCondition, 0, len(conditions))
	for _, condition := range conditions {
		out = append(out, model.ClusterWorkloadCondition{
			Type:    string(condition.Type),
			Status:  string(condition.Status),
			Reason:  strings.TrimSpace(condition.Reason),
			Message: strings.TrimSpace(condition.Message),
		})
	}
	return out
}

func podConditions(conditions []corev1.PodCondition) []model.ClusterWorkloadCondition {
	out := make([]model.ClusterWorkloadCondition, 0, len(conditions))
	for _, condition := range conditions {
		out = append(out, model.ClusterWorkloadCondition{
			Type:    string(condition.Type),
			Status:  string(condition.Status),
			Reason:  strings.TrimSpace(condition.Reason),
			Message: strings.TrimSpace(condition.Message),
		})
	}
	return out
}

func rolloutStatusFromWorkload(workload model.ClusterWorkloadDetail) model.ClusterRolloutStatus {
	status := model.ClusterRolloutStatus{
		Kind:              workload.Kind,
		Namespace:         workload.Namespace,
		Name:              workload.Name,
		DesiredReplicas:   workload.DesiredReplicas,
		ReadyReplicas:     workload.ReadyReplicas,
		UpdatedReplicas:   workload.UpdatedReplicas,
		AvailableReplicas: workload.AvailableReplicas,
		Conditions:        append([]model.ClusterWorkloadCondition(nil), workload.Conditions...),
		ObservedAt:        time.Now().UTC(),
	}

	desired := derefInt32(workload.DesiredReplicas)
	ready := derefInt32(workload.ReadyReplicas)
	updated := derefInt32(workload.UpdatedReplicas)
	available := derefInt32(workload.AvailableReplicas)

	switch strings.ToLower(strings.TrimSpace(workload.Kind)) {
	case "pod":
		if len(workload.Pods) > 0 && workload.Pods[0].Ready {
			status.Status = "ready"
		} else if len(workload.Pods) > 0 {
			status.Status = strings.ToLower(strings.TrimSpace(workload.Pods[0].Phase))
		} else {
			status.Status = "unknown"
		}
	default:
		if desired > 0 && ready >= desired && updated >= desired && available >= desired {
			status.Status = "ready"
		} else if ready > 0 || updated > 0 || available > 0 {
			status.Status = "rolling"
		} else if desired == 0 {
			status.Status = "scaled-down"
		} else {
			status.Status = "degraded"
		}
	}

	if len(workload.Conditions) > 0 {
		parts := make([]string, 0, len(workload.Conditions))
		for _, condition := range workload.Conditions {
			if strings.TrimSpace(condition.Message) != "" {
				parts = append(parts, condition.Type+": "+strings.TrimSpace(condition.Message))
			}
		}
		status.Message = strings.Join(parts, " | ")
	}
	return status
}

func resolveClusterDNS(ctx context.Context, name, server, recordType string, timeout time.Duration) (model.ClusterDNSResolveResult, error) {
	name = strings.TrimSpace(name)
	recordType = strings.ToUpper(strings.TrimSpace(recordType))
	if name == "" {
		return model.ClusterDNSResolveResult{}, fmt.Errorf("name is required")
	}
	if timeout <= 0 {
		timeout = clusterProbeDefaultTimeout
	}
	if recordType == "" {
		recordType = "A"
	}
	if _, err := normalizeClusterDNSServer(server); err != nil {
		return model.ClusterDNSResolveResult{}, err
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	resolver := clusterDNSResolver(server)
	result := model.ClusterDNSResolveResult{
		Name:       name,
		Server:     normalizeClusterDNSServerValue(server),
		RecordType: recordType,
		Answers:    []model.ClusterDNSAnswer{},
		ObservedAt: time.Now().UTC(),
	}

	appendAnswer := func(values ...string) {
		for _, value := range values {
			value = strings.TrimSpace(value)
			if value == "" {
				continue
			}
			result.Answers = append(result.Answers, model.ClusterDNSAnswer{Type: recordType, Value: value})
		}
	}

	switch recordType {
	case "A":
		values, err := resolver.LookupIP(ctx, "ip4", name)
		if err != nil {
			result.Error = err.Error()
		} else {
			for _, value := range values {
				appendAnswer(value.String())
			}
		}
	case "AAAA":
		values, err := resolver.LookupIP(ctx, "ip6", name)
		if err != nil {
			result.Error = err.Error()
		} else {
			for _, value := range values {
				appendAnswer(value.String())
			}
		}
	case "CNAME":
		value, err := resolver.LookupCNAME(ctx, name)
		if err != nil {
			result.Error = err.Error()
		} else {
			appendAnswer(value)
		}
	case "TXT":
		values, err := resolver.LookupTXT(ctx, name)
		if err != nil {
			result.Error = err.Error()
		} else {
			appendAnswer(values...)
		}
	case "MX":
		values, err := resolver.LookupMX(ctx, name)
		if err != nil {
			result.Error = err.Error()
		} else {
			for _, value := range values {
				appendAnswer(value.Host)
			}
		}
	case "NS":
		values, err := resolver.LookupNS(ctx, name)
		if err != nil {
			result.Error = err.Error()
		} else {
			for _, value := range values {
				appendAnswer(value.Host)
			}
		}
	default:
		return model.ClusterDNSResolveResult{}, fmt.Errorf("unsupported DNS record type %q", recordType)
	}

	sort.Slice(result.Answers, func(i, j int) bool {
		return result.Answers[i].Value < result.Answers[j].Value
	})
	return result, nil
}

func probeClusterNetwork(ctx context.Context, target string, timeout time.Duration) (model.ClusterNetworkConnectResult, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		return model.ClusterNetworkConnectResult{}, fmt.Errorf("target is required")
	}
	host, _, err := net.SplitHostPort(target)
	if err != nil {
		return model.ClusterNetworkConnectResult{}, fmt.Errorf("target must be host:port: %w", err)
	}
	if timeout <= 0 {
		timeout = clusterProbeDefaultTimeout
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	result := model.ClusterNetworkConnectResult{
		Target:     target,
		Network:    "tcp",
		ObservedAt: time.Now().UTC(),
	}

	resolved, lookupErr := net.DefaultResolver.LookupIPAddr(ctx, host)
	if lookupErr == nil {
		result.ResolvedAddresses = make([]string, 0, len(resolved))
		for _, address := range resolved {
			result.ResolvedAddresses = append(result.ResolvedAddresses, address.IP.String())
		}
		sort.Strings(result.ResolvedAddresses)
	}

	dialer := net.Dialer{Timeout: timeout}
	startedAt := time.Now()
	conn, err := dialer.DialContext(ctx, "tcp", target)
	result.DurationMillis = time.Since(startedAt).Milliseconds()
	if err != nil {
		result.Error = err.Error()
		return result, nil
	}
	defer conn.Close()

	result.Success = true
	result.RemoteAddr = conn.RemoteAddr().String()
	return result, nil
}

func probeClusterTLS(ctx context.Context, target, serverName string, timeout time.Duration) (model.ClusterTLSProbeResult, error) {
	target = strings.TrimSpace(target)
	serverName = strings.TrimSpace(serverName)
	if target == "" {
		return model.ClusterTLSProbeResult{}, fmt.Errorf("target is required")
	}
	host, _, err := net.SplitHostPort(target)
	if err != nil {
		return model.ClusterTLSProbeResult{}, fmt.Errorf("target must be host:port: %w", err)
	}
	if timeout <= 0 {
		timeout = clusterProbeDefaultTimeout
	}
	if serverName == "" && net.ParseIP(host) == nil {
		serverName = host
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	result := model.ClusterTLSProbeResult{
		Target:     target,
		ServerName: serverName,
		ObservedAt: time.Now().UTC(),
	}

	dialer := &net.Dialer{Timeout: timeout}
	startedAt := time.Now()
	conn, err := tls.DialWithDialer(dialer, "tcp", target, &tls.Config{
		ServerName:         serverName,
		InsecureSkipVerify: true,
	})
	result.DurationMillis = time.Since(startedAt).Milliseconds()
	if err != nil {
		result.Error = err.Error()
		return result, nil
	}
	defer conn.Close()

	state := conn.ConnectionState()
	result.Success = true
	result.Version = tlsVersionString(state.Version)
	result.CipherSuite = tls.CipherSuiteName(state.CipherSuite)
	result.NegotiatedProtocol = strings.TrimSpace(state.NegotiatedProtocol)
	result.PeerCertificates = clusterPeerCertificates(state.PeerCertificates)

	if serverName != "" && len(state.PeerCertificates) > 0 {
		pool, _ := x509.SystemCertPool()
		options := x509.VerifyOptions{
			DNSName:       serverName,
			Roots:         pool,
			Intermediates: x509.NewCertPool(),
		}
		for _, certificate := range state.PeerCertificates[1:] {
			options.Intermediates.AddCert(certificate)
		}
		if _, err := state.PeerCertificates[0].Verify(options); err != nil {
			result.VerificationError = err.Error()
		} else {
			result.Verified = true
		}
	}

	return result, nil
}

func clusterPeerCertificates(certificates []*x509.Certificate) []model.ClusterTLSPeerCertificate {
	if len(certificates) == 0 {
		return nil
	}
	out := make([]model.ClusterTLSPeerCertificate, 0, len(certificates))
	for _, certificate := range certificates {
		if certificate == nil {
			continue
		}
		out = append(out, model.ClusterTLSPeerCertificate{
			Subject:     certificate.Subject.String(),
			Issuer:      certificate.Issuer.String(),
			SHA256:      sha256Fingerprint(certificate.Raw),
			DNSNames:    append([]string(nil), certificate.DNSNames...),
			IPAddresses: ipStrings(certificate.IPAddresses),
			NotBefore:   certificate.NotBefore.UTC(),
			NotAfter:    certificate.NotAfter.UTC(),
		})
	}
	return out
}

func sha256Fingerprint(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func tlsVersionString(version uint16) string {
	switch version {
	case tls.VersionTLS10:
		return "TLS1.0"
	case tls.VersionTLS11:
		return "TLS1.1"
	case tls.VersionTLS12:
		return "TLS1.2"
	case tls.VersionTLS13:
		return "TLS1.3"
	default:
		return fmt.Sprintf("0x%x", version)
	}
}

func clusterDNSResolver(server string) *net.Resolver {
	target := normalizeClusterDNSServerValue(server)
	if target == "" {
		return net.DefaultResolver
	}

	return &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
			dialer := net.Dialer{}
			return dialer.DialContext(ctx, network, target)
		},
	}
}

func normalizeClusterDNSServer(raw string) (string, error) {
	value := normalizeClusterDNSServerValue(raw)
	if value == "" {
		return "", nil
	}
	host, port, err := net.SplitHostPort(value)
	if err != nil {
		return "", fmt.Errorf("server must be an IP address or host[:port]")
	}
	if strings.TrimSpace(host) == "" || strings.TrimSpace(port) == "" {
		return "", fmt.Errorf("server must be an IP address or host[:port]")
	}
	return value, nil
}

func normalizeClusterDNSServerValue(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if _, _, err := net.SplitHostPort(raw); err == nil {
		return raw
	}
	return net.JoinHostPort(raw, "53")
}

func runClusterExecWithRetries(
	ctx context.Context,
	runner filesystemPodExecRunner,
	namespace string,
	podName string,
	containerName string,
	command []string,
	retries int,
	retryDelay time.Duration,
) ([]byte, int, error) {
	if retries < 0 {
		retries = 0
	}
	if retryDelay <= 0 {
		retryDelay = 250 * time.Millisecond
	}
	attempts := 0
	for {
		attempts++
		output, err := runner.Run(ctx, namespace, podName, containerName, nil, command...)
		if err == nil {
			return output, attempts, nil
		}
		if attempts >= retries+1 || !isRetryableClusterExecError(err) {
			return nil, attempts, err
		}
		select {
		case <-ctx.Done():
			return nil, attempts, ctx.Err()
		case <-time.After(retryDelay):
		}
	}
}

func isRetryableClusterExecError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	text := strings.ToLower(strings.TrimSpace(err.Error()))
	for _, marker := range []string{
		"unexpected eof",
		"eof",
		"connection reset by peer",
		"client connection lost",
		"stream error",
		"websocket: close 1006",
	} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func manifestMap(value any) (map[string]any, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal workload manifest: %w", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("decode workload manifest: %w", err)
	}
	return out, nil
}

func readIntQuery(r *http.Request, key string, defaultValue int) (int, error) {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return defaultValue, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return defaultValue, fmt.Errorf("%s must be an integer", key)
	}
	return value, nil
}

func readDurationMillisQuery(r *http.Request, key string, defaultValue time.Duration) (time.Duration, error) {
	value, err := readIntQuery(r, key, int(defaultValue/time.Millisecond))
	if err != nil {
		return defaultValue, err
	}
	if value <= 0 {
		return defaultValue, nil
	}
	return time.Duration(value) * time.Millisecond, nil
}

func trimStringSlice(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		out = append(out, value)
	}
	return out
}

func cloneOptionalTime(value *metav1.Time) *time.Time {
	if value == nil || value.IsZero() {
		return nil
	}
	copy := value.UTC()
	return &copy
}

func nonZeroTime(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	copy := value.UTC()
	return &copy
}

func int32Ptr(value int32) *int32 {
	copy := value
	return &copy
}

func derefInt32(value *int32) int32 {
	if value == nil {
		return 0
	}
	return *value
}

func ipStrings(values []net.IP) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, value.String())
	}
	sort.Strings(out)
	return out
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}
