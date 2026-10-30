package api

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type clusterPodMetrics struct {
	Metadata   metav1.ObjectMeta `json:"metadata"`
	Timestamp  metav1.Time       `json:"timestamp"`
	Containers []struct {
		Name  string              `json:"name"`
		Usage corev1.ResourceList `json:"usage"`
	} `json:"containers"`
}

func (s *Server) handleGetClusterResourceReport(w http.ResponseWriter, r *http.Request) {
	principal, ok := requirePlatformAdmin(w, r)
	if !ok {
		return
	}
	node := readOptionalStringQuery(r, "node")
	window, err := readIntQuery(r, "window_hours", defaultRightSizingWindowHours)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	minimum, err := readIntQuery(r, "min_samples", defaultRightSizingMinSamples)
	if err != nil || node == "" || window < 1 || window > maxRightSizingWindowHours || minimum < 1 {
		httpx.WriteError(w, http.StatusBadRequest, "node is required, window_hours must be 1..168 and min_samples must be positive")
		return
	}
	client, err := s.requireClusterNodeClient()
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	defer client.closeIdleConnections()
	var kubeNode corev1.Node
	if err := client.doJSON(r.Context(), http.MethodGet, "/api/v1/nodes/"+url.PathEscape(node), &kubeNode); err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	var pods corev1.PodList
	query := url.Values{"fieldSelector": {"spec.nodeName=" + node}}
	if err := client.doJSON(r.Context(), http.MethodGet, "/api/v1/pods?"+query.Encode(), &pods); err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	apps, err := s.store.ListAppsMetadata(principal.TenantID, true)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	services, err := s.store.ListBackingServices(principal.TenantID, true)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	var metrics struct {
		Items []clusterPodMetrics `json:"items"`
	}
	metricsErr := client.doJSON(r.Context(), http.MethodGet, "/apis/metrics.k8s.io/v1beta1/pods", &metrics)
	if metricsErr != nil {
		// Existing installations already allow node stats reads. Keep this
		// report usable without introducing a new metrics API permission.
		if summary, err := client.getNodeSummary(r.Context(), node); err == nil && summary != nil {
			metrics.Items = clusterResourceMetricsFromSummary(summary)
			metricsErr = nil
		}
	}
	report, groups := buildClusterResourceReport(node, pods.Items, metrics.Items, apps, services, time.Now().UTC())
	report.WindowHours, report.MinSamples = window, minimum
	if metricsErr != nil {
		report.Warnings = append(report.Warnings, "Live pod metrics unavailable; usage is unknown")
	}
	resolver := newClusterWorkloadResolver(apps, services)
	for i := range report.Services {
		row := &report.Services[i]
		var current *model.ResourceSpec
		workloadClass, serviceType, tenant := "", "", ""
		switch row.Kind {
		case model.ClusterNodeWorkloadKindApp:
			if app, exists := resolver.appsByID[row.ID]; exists {
				current, workloadClass, tenant = app.Spec.Resources, model.EffectiveWorkloadClass(app.Spec), app.TenantID
			}
		case model.ClusterNodeWorkloadKindBackingService:
			if service, exists := resolver.servicesByID[row.ID]; exists && service.Type == model.BackingServiceTypePostgres && service.Spec.Postgres != nil {
				resources := model.ManagedPostgresRuntimeResources(*service.Spec.Postgres)
				current, workloadClass, serviceType, tenant = &resources, model.WorkloadClassCritical, service.Type, service.TenantID
			}
		}
		if tenant == "" {
			continue
		}
		samples, err := s.store.ListResourceUsageSamples(tenant, rightSizingSampleTargetKind(row.Kind), row.ID, report.ObservedAt.Add(-time.Duration(window)*time.Hour))
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		rec := buildRightSizingRecommendation(row.Kind, row.ID, row.Name, serviceType, workloadClass, window, minimum, current, samples)
		row.Recommendation = &rec
		row.RecommendationReason = rec.Reason
		if !rec.Ready || rec.RequestTarget == nil || rec.Recommended == nil {
			continue
		}
		row.RecommendationReason = "ready"
		// Capacity and request targets have different meanings. Retain both,
		// and expose only dimensions backed by historical observations.
		for _, pod := range groups[clusterResourceKey(*row)] {
			if len(pod.Spec.Containers) == 0 {
				continue
			}
			if rec.PeakCPUUsageMilli != nil {
				addClusterResourceInt(&row.CPURecommendedMilliCores, clusterPodResourceRequests(pod, rec.RequestTarget, true, false).cpuMilliCores)
				addClusterResourceInt(&row.CPUCapacityMilliCores, clusterPodResourceRequests(pod, rec.Recommended, true, false).cpuMilliCores)
			}
			if rec.PeakMemoryUsageBytes != nil {
				addClusterResourceInt(&row.MemoryRecommendedBytes, clusterPodResourceRequests(pod, rec.RequestTarget, false, true).memoryBytes)
			}
		}
	}
	s.appendAudit(principal, "cluster.resources.get", "cluster", node, principal.TenantID, nil)
	httpx.WriteJSON(w, http.StatusOK, report)
}

func clusterResourceMetricsFromSummary(summary *kubeNodeSummary) []clusterPodMetrics {
	out := make([]clusterPodMetrics, 0, len(summary.Pods))
	for _, pod := range summary.Pods {
		metric := clusterPodMetrics{Metadata: metav1.ObjectMeta{Namespace: pod.PodRef.Namespace, Name: pod.PodRef.Name}}
		for _, container := range pod.Containers {
			cpuTime, cpuErr := time.Parse(time.RFC3339Nano, container.CPU.Time)
			memoryTime, memoryErr := time.Parse(time.RFC3339Nano, container.Memory.Time)
			if cpuErr != nil || memoryErr != nil || container.CPU.UsageNanoCores == nil || container.Memory.WorkingSetBytes == nil {
				continue
			}
			observed := cpuTime
			if memoryTime.Before(observed) {
				observed = memoryTime
			}
			if metric.Timestamp.IsZero() || observed.Before(metric.Timestamp.Time) {
				metric.Timestamp = metav1.NewTime(observed)
			}
			metric.Containers = append(metric.Containers, struct {
				Name  string              `json:"name"`
				Usage corev1.ResourceList `json:"usage"`
			}{Name: container.Name, Usage: corev1.ResourceList{
				corev1.ResourceCPU:    *resource.NewScaledQuantity(int64(*container.CPU.UsageNanoCores), resource.Nano),
				corev1.ResourceMemory: *resource.NewQuantity(int64(*container.Memory.WorkingSetBytes), resource.BinarySI),
			}})
		}
		out = append(out, metric)
	}
	return out
}

func buildClusterResourceReport(node string, pods []corev1.Pod, metrics []clusterPodMetrics, apps []model.App, services []model.BackingService, now time.Time) (model.ClusterResourceReport, map[string][]corev1.Pod) {
	report := model.ClusterResourceReport{Node: node, ObservedAt: now, Services: []model.ClusterServiceResources{}}
	resolver := newClusterWorkloadResolver(apps, services)
	measured := make(map[string]clusterPodMetrics, len(metrics))
	for _, metric := range metrics {
		age := now.Sub(metric.Timestamp.Time)
		if !metric.Timestamp.IsZero() && age >= -time.Minute && age <= 5*time.Minute {
			measured[clusterNamespacedResourceKey(metric.Metadata.Namespace, metric.Metadata.Name)] = metric
		}
	}
	groups := map[string][]corev1.Pod{}
	rows := map[string]*model.ClusterServiceResources{}
	for _, pod := range pods {
		if pod.Spec.NodeName != node || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		identity := clusterNodePod{}
		identity.Metadata.Namespace, identity.Metadata.Name, identity.Metadata.Labels = pod.Namespace, pod.Name, pod.Labels
		workload, managed := resolver.resolvePod(identity)
		row := model.ClusterServiceResources{Kind: workload.Kind, ID: workload.ID, Name: workload.Name, Namespace: pod.Namespace, RecommendationReason: "unsupported workload"}
		if !managed {
			row.Kind, row.Name = "Pod", pod.Name
			if cluster := pod.Labels[clusterNodeCNPGClusterLabel]; cluster != "" {
				row.Kind, row.Name = "Cluster", cluster
			} else {
				for _, owner := range pod.OwnerReferences {
					if owner.Controller != nil && *owner.Controller {
						row.Kind, row.Name = owner.Kind, owner.Name
						if hash := pod.Labels["pod-template-hash"]; owner.Kind == "ReplicaSet" && hash != "" && strings.HasSuffix(owner.Name, "-"+hash) {
							row.Kind, row.Name = "Deployment", strings.TrimSuffix(owner.Name, "-"+hash)
						}
						break
					}
				}
			}
			row.ID = row.Name
		}
		key := clusterResourceKey(row)
		entry := rows[key]
		if entry == nil {
			copy := row
			entry = &copy
			rows[key] = entry
		}
		groups[key] = append(groups[key], pod)
		entry.PodCount++
		requests := clusterPodResourceRequests(pod, nil, false, false)
		entry.CPURequestMilliCores += requests.cpuMilliCores
		entry.MemoryRequestBytes += requests.memoryBytes
		if metric, exists := measured[clusterNamespacedResourceKey(pod.Namespace, pod.Name)]; exists {
			cpu, memory, complete := clusterPodMeasuredUsage(pod, metric)
			if complete {
				if entry.CPUUsageMilliCores == nil {
					entry.CPUUsageMilliCores = new(float64)
				}
				*entry.CPUUsageMilliCores += cpu
				addClusterResourceInt(&entry.MemoryUsageBytes, memory)
				entry.MeasuredPods++
			}
		}
		report.TotalPods++
		report.CPURequestMilliCores += requests.cpuMilliCores
		report.MemoryRequestBytes += requests.memoryBytes
	}
	missing := 0
	for _, row := range rows {
		if row.MeasuredPods != row.PodCount {
			missing += row.PodCount - row.MeasuredPods
			row.CPUUsageMilliCores, row.MemoryUsageBytes = nil, nil
		}
		report.Services = append(report.Services, *row)
	}
	sort.Slice(report.Services, func(i, j int) bool {
		return clusterResourceKey(report.Services[i]) < clusterResourceKey(report.Services[j])
	})
	report.TotalServices = len(report.Services)
	if missing > 0 {
		report.Warnings = append(report.Warnings, fmt.Sprintf("%d pods have missing, stale or incomplete metrics; affected service usage is unknown", missing))
	}
	return report, groups
}

func clusterResourceKey(row model.ClusterServiceResources) string {
	return row.Namespace + "\x00" + row.Kind + "\x00" + row.ID
}

func addClusterResourceInt(target **int64, value int64) {
	if *target == nil {
		*target = new(int64)
	}
	**target += value
}

// Match the scheduler's regular-container sum, ordered restartable init
// sidecars, init peaks, pod-level requests and overhead. Replacing only the
// first managed main container matches the existing right-sizing sampler.
func clusterPodResourceRequests(pod corev1.Pod, target *model.ResourceSpec, cpu, memory bool) clusterNodeResourceRequests {
	copy := clusterNodePod{}
	convert := func(container corev1.Container) clusterNodeContainer {
		out := clusterNodeContainer{Name: container.Name, Resources: clusterNodeResourceRequirements{Requests: map[string]string{}}}
		if container.RestartPolicy != nil {
			out.RestartPolicy = string(*container.RestartPolicy)
		}
		for key, value := range container.Resources.Requests {
			out.Resources.Requests[string(key)] = value.String()
		}
		return out
	}
	for i, container := range pod.Spec.Containers {
		converted := convert(container)
		if i == 0 && target != nil {
			if cpu {
				converted.Resources.Requests["cpu"] = resource.NewMilliQuantity(target.CPUMilliCores, resource.DecimalSI).String()
			}
			if memory {
				converted.Resources.Requests["memory"] = resource.NewQuantity(target.MemoryMebibytes*1024*1024, resource.BinarySI).String()
			}
		}
		copy.Spec.Containers = append(copy.Spec.Containers, converted)
	}
	for _, container := range pod.Spec.InitContainers {
		copy.Spec.InitContainers = append(copy.Spec.InitContainers, convert(container))
	}
	requests := clusterNodeSinglePodRequests(copy)
	if pod.Spec.Resources != nil {
		if value, ok := pod.Spec.Resources.Requests[corev1.ResourceCPU]; ok {
			requests.cpuMilliCores = value.MilliValue()
		}
		if value, ok := pod.Spec.Resources.Requests[corev1.ResourceMemory]; ok {
			requests.memoryBytes = value.Value()
		}
	}
	if value, ok := pod.Spec.Overhead[corev1.ResourceCPU]; ok {
		requests.cpuMilliCores += value.MilliValue()
	}
	if value, ok := pod.Spec.Overhead[corev1.ResourceMemory]; ok {
		requests.memoryBytes += value.Value()
	}
	return requests
}

func clusterPodMeasuredUsage(pod corev1.Pod, metric clusterPodMetrics) (float64, int64, bool) {
	expected := map[string]bool{}
	for _, container := range pod.Spec.Containers {
		expected[container.Name] = false
	}
	for _, container := range pod.Spec.InitContainers {
		if container.RestartPolicy != nil && *container.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			expected[container.Name] = false
		}
	}
	var cpu float64
	var memory int64
	for _, container := range metric.Containers {
		cpuValue, haveCPU := container.Usage[corev1.ResourceCPU]
		memoryValue, haveMemory := container.Usage[corev1.ResourceMemory]
		if !haveCPU || !haveMemory {
			return 0, 0, false
		}
		cpu += cpuValue.AsApproximateFloat64() * 1000
		memory += memoryValue.Value()
		if _, exists := expected[container.Name]; exists {
			expected[container.Name] = true
		}
	}
	for _, found := range expected {
		if !found {
			return 0, 0, false
		}
	}
	return cpu, memory, len(expected) > 0
}
