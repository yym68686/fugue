package api

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/runtime"
)

const (
	clusterNodePodLabelSelector  = "app.kubernetes.io/managed-by=fugue,app.kubernetes.io/name"
	clusterNodeStatsConcurrency  = 4
	clusterNodeConditionReady    = "Ready"
	clusterNodeConditionMemory   = "MemoryPressure"
	clusterNodeConditionDisk     = "DiskPressure"
	clusterNodeConditionPID      = "PIDPressure"
	clusterNodeLabelRegion       = "topology.kubernetes.io/region"
	clusterNodeLabelLegacyRegion = "failure-domain.beta.kubernetes.io/region"
	clusterNodeLabelZone         = "topology.kubernetes.io/zone"
	clusterNodeLabelLegacyZone   = "failure-domain.beta.kubernetes.io/zone"
	clusterNodeLabelCountryCode  = "fugue.io/location-country-code"
	clusterNodeLabelPublicIP     = "fugue.io/public-ip"
	clusterNodeAnnotationCountry = "fugue.io/location-country"
)

var (
	kubeQuantityPattern  = regexp.MustCompile(`^([+-]?(?:\d+(?:\.\d+)?|\.\d+)(?:[eE][+-]?\d+)?)([a-zA-Z]{0,2})$`)
	clusterNodeCGNATCIDR = mustParseCIDR("100.64.0.0/10")
)

type clusterNodeClient struct {
	client      *http.Client
	baseURL     string
	bearerToken string
}

type clusterNodeSnapshot struct {
	node model.ClusterNode
	pods []clusterNodePod
}

type clusterWorkloadResolver struct {
	appsByID                map[string]model.App
	servicesByID            map[string]model.BackingService
	appsByNamespacedName    map[string]model.App
	servicesByNamespacedApp map[string]model.BackingService
}

type kubeNodeList struct {
	Items []kubeNode `json:"items"`
}

type kubeNode struct {
	Metadata struct {
		Name              string            `json:"name"`
		CreationTimestamp string            `json:"creationTimestamp"`
		Labels            map[string]string `json:"labels"`
		Annotations       map[string]string `json:"annotations"`
	} `json:"metadata"`
	Status struct {
		Addresses []struct {
			Type    string `json:"type"`
			Address string `json:"address"`
		} `json:"addresses"`
		Conditions  []kubeNodeCondition `json:"conditions"`
		Capacity    map[string]string   `json:"capacity"`
		Allocatable map[string]string   `json:"allocatable"`
		NodeInfo    struct {
			KubeletVersion   string `json:"kubeletVersion"`
			OSImage          string `json:"osImage"`
			KernelVersion    string `json:"kernelVersion"`
			ContainerRuntime string `json:"containerRuntimeVersion"`
		} `json:"nodeInfo"`
	} `json:"status"`
}

type kubeNodeCondition struct {
	Type               string `json:"type"`
	Status             string `json:"status"`
	Reason             string `json:"reason,omitempty"`
	Message            string `json:"message,omitempty"`
	LastTransitionTime string `json:"lastTransitionTime,omitempty"`
}

type clusterNodePodList struct {
	Items []clusterNodePod `json:"items"`
}

type clusterNodePod struct {
	Metadata struct {
		Name      string            `json:"name"`
		Namespace string            `json:"namespace"`
		Labels    map[string]string `json:"labels"`
	} `json:"metadata"`
	Spec struct {
		NodeName string `json:"nodeName,omitempty"`
	} `json:"spec"`
	Status struct {
		Phase string `json:"phase,omitempty"`
	} `json:"status"`
}

type kubeNodeSummary struct {
	Node kubeNodeSummaryNode `json:"node"`
}

type kubeNodeSummaryNode struct {
	NodeName string             `json:"nodeName,omitempty"`
	CPU      kubeNodeSummaryCPU `json:"cpu,omitempty"`
	Memory   kubeNodeSummaryMem `json:"memory,omitempty"`
	FS       kubeNodeSummaryFS  `json:"fs,omitempty"`
}

type kubeNodeSummaryCPU struct {
	UsageNanoCores *uint64 `json:"usageNanoCores,omitempty"`
}

type kubeNodeSummaryMem struct {
	AvailableBytes  *uint64 `json:"availableBytes,omitempty"`
	UsageBytes      *uint64 `json:"usageBytes,omitempty"`
	WorkingSetBytes *uint64 `json:"workingSetBytes,omitempty"`
}

type kubeNodeSummaryFS struct {
	AvailableBytes *uint64 `json:"availableBytes,omitempty"`
	CapacityBytes  *uint64 `json:"capacityBytes,omitempty"`
	UsedBytes      *uint64 `json:"usedBytes,omitempty"`
}

func (s *Server) handleListClusterNodes(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)

	clientFactory := s.newClusterNodeClient
	if clientFactory == nil {
		clientFactory = newClusterNodeClient
	}
	client, err := clientFactory()
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	snapshots, err := client.listClusterNodeInventory(r.Context())
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	runtimes, err := s.store.ListNodes(principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	apps, err := s.store.ListApps(principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	services, err := s.store.ListBackingServices(principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	runtimeByClusterNode := make(map[string]model.Runtime, len(runtimes))
	for _, runtimeObj := range runtimes {
		name := strings.TrimSpace(runtimeObj.ClusterNodeName)
		if name == "" {
			continue
		}
		if existing, ok := runtimeByClusterNode[name]; ok && existing.UpdatedAt.After(runtimeObj.UpdatedAt) {
			continue
		}
		runtimeByClusterNode[name] = runtimeObj
	}

	workloadResolver := newClusterWorkloadResolver(apps, services)

	filtered := make([]model.ClusterNode, 0, len(snapshots))
	for _, snapshot := range snapshots {
		node := snapshot.node
		workloads := workloadResolver.resolve(snapshot.pods)
		runtimeObj, ok := runtimeByClusterNode[node.Name]
		var runtimeForNode *model.Runtime
		if ok {
			runtimeForNode = &runtimeObj
		}
		node.PublicIP = resolveClusterNodePublicIP(node, runtimeForNode)
		if !principal.IsPlatformAdmin() && !ok && len(workloads) == 0 {
			continue
		}
		if ok {
			node.RuntimeID = runtimeObj.ID
			node.TenantID = runtimeObj.TenantID
		}
		node.Workloads = workloads
		filtered = append(filtered, node)
	}

	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].CreatedAt != nil && filtered[j].CreatedAt != nil && !filtered[i].CreatedAt.Equal(*filtered[j].CreatedAt) {
			return filtered[i].CreatedAt.Before(*filtered[j].CreatedAt)
		}
		return filtered[i].Name < filtered[j].Name
	})

	httpx.WriteJSON(w, http.StatusOK, map[string]any{"cluster_nodes": filtered})
}

func newClusterNodeClient() (*clusterNodeClient, error) {
	host := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_HOST"))
	port := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_PORT"))
	if host == "" || port == "" {
		return nil, fmt.Errorf("kubernetes service host/port is not available in the environment")
	}

	token, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err != nil {
		return nil, fmt.Errorf("read service account token: %w", err)
	}
	caData, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
	if err != nil {
		return nil, fmt.Errorf("read service account CA: %w", err)
	}
	rootCAs := x509.NewCertPool()
	if !rootCAs.AppendCertsFromPEM(caData) {
		return nil, fmt.Errorf("load service account CA")
	}

	return &clusterNodeClient{
		client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{RootCAs: rootCAs},
			},
			Timeout: 10 * time.Second,
		},
		baseURL:     "https://" + host + ":" + port,
		bearerToken: strings.TrimSpace(string(token)),
	}, nil
}

func (c *clusterNodeClient) listClusterNodeInventory(ctx context.Context) ([]clusterNodeSnapshot, error) {
	var nodeList kubeNodeList
	if err := c.doJSON(ctx, http.MethodGet, "/api/v1/nodes", &nodeList); err != nil {
		return nil, err
	}

	podsByNode, err := c.listFuguePodsByNode(ctx)
	if err != nil {
		return nil, err
	}

	summariesByNode, err := c.listNodeSummaries(ctx, nodeList.Items)
	if err != nil {
		return nil, err
	}

	snapshots := make([]clusterNodeSnapshot, 0, len(nodeList.Items))
	for _, item := range nodeList.Items {
		name := strings.TrimSpace(item.Metadata.Name)
		snapshots = append(snapshots, clusterNodeSnapshot{
			node: buildClusterNode(item, summariesByNode[name]),
			pods: podsByNode[name],
		})
	}
	return snapshots, nil
}

func (c *clusterNodeClient) listFuguePodsByNode(ctx context.Context) (map[string][]clusterNodePod, error) {
	query := url.Values{}
	query.Set("labelSelector", clusterNodePodLabelSelector)

	var podList clusterNodePodList
	if err := c.doJSON(ctx, http.MethodGet, "/api/v1/pods?"+query.Encode(), &podList); err != nil {
		return nil, err
	}

	podsByNode := make(map[string][]clusterNodePod)
	for _, pod := range podList.Items {
		nodeName := strings.TrimSpace(pod.Spec.NodeName)
		if nodeName == "" {
			continue
		}
		phase := strings.TrimSpace(pod.Status.Phase)
		if strings.EqualFold(phase, "Succeeded") || strings.EqualFold(phase, "Failed") {
			continue
		}
		podsByNode[nodeName] = append(podsByNode[nodeName], pod)
	}
	return podsByNode, nil
}

func (c *clusterNodeClient) listNodeSummaries(ctx context.Context, nodes []kubeNode) (map[string]*kubeNodeSummary, error) {
	nodeNames := make([]string, 0, len(nodes))
	for _, node := range nodes {
		name := strings.TrimSpace(node.Metadata.Name)
		if name == "" {
			continue
		}
		nodeNames = append(nodeNames, name)
	}
	if len(nodeNames) == 0 {
		return map[string]*kubeNodeSummary{}, nil
	}

	summaries := make(map[string]*kubeNodeSummary, len(nodeNames))
	var mu sync.Mutex
	var wg sync.WaitGroup
	errCh := make(chan error, len(nodeNames))
	sem := make(chan struct{}, clusterNodeStatsConcurrency)

	for _, nodeName := range nodeNames {
		nodeName := nodeName
		wg.Add(1)
		go func() {
			defer wg.Done()

			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
			defer func() { <-sem }()

			summary, err := c.getNodeSummary(ctx, nodeName)
			if err != nil {
				errCh <- fmt.Errorf("read stats summary for node %s: %w", nodeName, err)
				return
			}

			mu.Lock()
			summaries[nodeName] = summary
			mu.Unlock()
		}()
	}

	wg.Wait()
	close(errCh)

	var firstErr error
	for err := range errCh {
		if firstErr == nil {
			firstErr = err
		}
	}
	if len(summaries) == 0 && firstErr != nil {
		return nil, firstErr
	}
	return summaries, nil
}

func (c *clusterNodeClient) getNodeSummary(ctx context.Context, nodeName string) (*kubeNodeSummary, error) {
	var summary kubeNodeSummary
	apiPath := "/api/v1/nodes/" + url.PathEscape(strings.TrimSpace(nodeName)) + "/proxy/stats/summary"
	if err := c.doJSON(ctx, http.MethodGet, apiPath, &summary); err != nil {
		return nil, err
	}
	return &summary, nil
}

func buildClusterNode(node kubeNode, summary *kubeNodeSummary) model.ClusterNode {
	out := model.ClusterNode{
		Name:             strings.TrimSpace(node.Metadata.Name),
		Status:           kubeNodeReadyStatus(node),
		Roles:            kubeNodeRoles(node.Metadata.Labels),
		InternalIP:       kubeNodeAddress(node, "InternalIP"),
		ExternalIP:       kubeNodeAddress(node, "ExternalIP"),
		PublicIP:         kubeNodePublicIP(node),
		Region:           kubeNodeRegion(node.Metadata.Labels, node.Metadata.Annotations),
		Zone:             kubeNodeZone(node.Metadata.Labels),
		KubeletVersion:   strings.TrimSpace(node.Status.NodeInfo.KubeletVersion),
		OSImage:          strings.TrimSpace(node.Status.NodeInfo.OSImage),
		KernelVersion:    strings.TrimSpace(node.Status.NodeInfo.KernelVersion),
		ContainerRuntime: strings.TrimSpace(node.Status.NodeInfo.ContainerRuntime),
		Conditions:       buildClusterNodeConditions(node.Status.Conditions),
		CPU:              buildClusterNodeCPUStats(node, summary),
		Memory:           buildClusterNodeMemoryStats(node, summary),
		EphemeralStorage: buildClusterNodeStorageStats(node, summary),
	}
	if createdAt := parseClusterNodeTimestamp(node.Metadata.CreationTimestamp); createdAt != nil {
		out.CreatedAt = createdAt
	}
	return out
}

func buildClusterNodeConditions(conditions []kubeNodeCondition) map[string]model.ClusterNodeCondition {
	if len(conditions) == 0 {
		return nil
	}

	out := make(map[string]model.ClusterNodeCondition, len(conditions))
	for _, condition := range conditions {
		conditionType := strings.TrimSpace(condition.Type)
		if conditionType == "" {
			continue
		}
		item := model.ClusterNodeCondition{
			Status:  normalizeKubeConditionStatus(condition.Status),
			Reason:  strings.TrimSpace(condition.Reason),
			Message: strings.TrimSpace(condition.Message),
		}
		if transitionedAt := parseClusterNodeTimestamp(condition.LastTransitionTime); transitionedAt != nil {
			item.LastTransitionAt = transitionedAt
		}
		out[conditionType] = item
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func buildClusterNodeCPUStats(node kubeNode, summary *kubeNodeSummary) *model.ClusterNodeCPUStats {
	capacity := int64PointerFromQuantity(node.Status.Capacity["cpu"], parseCPUQuantityMilli)
	allocatable := int64PointerFromQuantity(node.Status.Allocatable["cpu"], parseCPUQuantityMilli)
	used := clusterNodeCPUMilliUsage(summary)

	stats := &model.ClusterNodeCPUStats{
		CapacityMilliCores:    capacity,
		AllocatableMilliCores: allocatable,
		UsedMilliCores:        used,
		UsagePercent:          usagePercent(used, allocatable, capacity),
	}
	if isEmptyClusterNodeCPUStats(stats) {
		return nil
	}
	return stats
}

func buildClusterNodeMemoryStats(node kubeNode, summary *kubeNodeSummary) *model.ClusterNodeMemoryStats {
	capacity := int64PointerFromQuantity(node.Status.Capacity["memory"], parseBytesQuantity)
	allocatable := int64PointerFromQuantity(node.Status.Allocatable["memory"], parseBytesQuantity)
	used := clusterNodeMemoryUsage(summary, capacity)

	stats := &model.ClusterNodeMemoryStats{
		CapacityBytes:    capacity,
		AllocatableBytes: allocatable,
		UsedBytes:        used,
		UsagePercent:     usagePercent(used, allocatable, capacity),
	}
	if isEmptyClusterNodeMemoryStats(stats) {
		return nil
	}
	return stats
}

func buildClusterNodeStorageStats(node kubeNode, summary *kubeNodeSummary) *model.ClusterNodeStorageStats {
	reportedCapacity := int64PointerFromQuantity(node.Status.Capacity["ephemeral-storage"], parseBytesQuantity)
	reportedAllocatable := int64PointerFromQuantity(node.Status.Allocatable["ephemeral-storage"], parseBytesQuantity)
	used, summaryCapacity := clusterNodeStorageUsage(summary)
	capacity, allocatable := reconcileClusterNodeStorageTotals(
		reportedCapacity,
		reportedAllocatable,
		summaryCapacity,
	)

	stats := &model.ClusterNodeStorageStats{
		CapacityBytes:    capacity,
		AllocatableBytes: allocatable,
		UsedBytes:        used,
		UsagePercent:     usagePercent(used, allocatable, capacity),
	}
	if isEmptyClusterNodeStorageStats(stats) {
		return nil
	}
	return stats
}

// Kubelet can leave node.status.ephemeral-storage stale after a root disk resize
// while stats/summary already reflects the new filesystem size. Use the summary
// capacity as the displayed total so used bytes, percent, and capacity stay in
// the same unit of account, and preserve the reported allocatable ratio when
// the scheduler reservation is available.
func reconcileClusterNodeStorageTotals(reportedCapacity, reportedAllocatable, summaryCapacity *int64) (*int64, *int64) {
	capacity := reportedCapacity
	allocatable := reportedAllocatable

	if summaryCapacity != nil && *summaryCapacity > 0 {
		capacity = summaryCapacity
		if scaled := scaleClusterNodeStorageAllocatable(reportedAllocatable, reportedCapacity, summaryCapacity); scaled != nil {
			allocatable = scaled
		}
	}

	if capacity != nil && allocatable != nil && *allocatable > *capacity {
		adjusted := *capacity
		allocatable = &adjusted
	}

	return capacity, allocatable
}

func scaleClusterNodeStorageAllocatable(reportedAllocatable, reportedCapacity, targetCapacity *int64) *int64 {
	if reportedAllocatable == nil {
		return nil
	}
	if targetCapacity == nil || *targetCapacity <= 0 {
		return reportedAllocatable
	}
	if reportedCapacity == nil || *reportedCapacity <= 0 {
		return nil
	}

	ratio := float64(*reportedAllocatable) / float64(*reportedCapacity)
	if ratio <= 0 {
		return nil
	}
	if ratio > 1 {
		ratio = 1
	}

	scaled := int64(math.Round(float64(*targetCapacity) * ratio))
	if scaled < 0 {
		return nil
	}
	if scaled > *targetCapacity {
		scaled = *targetCapacity
	}
	return &scaled
}

func clusterNodeCPUMilliUsage(summary *kubeNodeSummary) *int64 {
	if summary == nil || summary.Node.CPU.UsageNanoCores == nil {
		return nil
	}
	value := int64(math.Round(float64(*summary.Node.CPU.UsageNanoCores) / 1_000_000))
	return &value
}

func clusterNodeMemoryUsage(summary *kubeNodeSummary, capacity *int64) *int64 {
	if summary == nil {
		return nil
	}
	if summary.Node.Memory.WorkingSetBytes != nil {
		return uint64PointerToInt64(summary.Node.Memory.WorkingSetBytes)
	}
	if summary.Node.Memory.UsageBytes != nil {
		return uint64PointerToInt64(summary.Node.Memory.UsageBytes)
	}
	if summary.Node.Memory.AvailableBytes == nil || capacity == nil {
		return nil
	}
	if *summary.Node.Memory.AvailableBytes > uint64(*capacity) {
		return nil
	}
	value := *capacity - int64(*summary.Node.Memory.AvailableBytes)
	return &value
}

func clusterNodeStorageUsage(summary *kubeNodeSummary) (*int64, *int64) {
	if summary == nil {
		return nil, nil
	}

	var capacity *int64
	if summary.Node.FS.CapacityBytes != nil {
		capacity = uint64PointerToInt64(summary.Node.FS.CapacityBytes)
	}
	if summary.Node.FS.UsedBytes != nil {
		return uint64PointerToInt64(summary.Node.FS.UsedBytes), capacity
	}
	if summary.Node.FS.AvailableBytes == nil || summary.Node.FS.CapacityBytes == nil {
		return nil, capacity
	}
	if *summary.Node.FS.AvailableBytes > *summary.Node.FS.CapacityBytes {
		return nil, capacity
	}
	value := int64(*summary.Node.FS.CapacityBytes - *summary.Node.FS.AvailableBytes)
	return &value, capacity
}

func (c *clusterNodeClient) doJSON(ctx context.Context, method, apiPath string, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+apiPath, nil)
	if err != nil {
		return fmt.Errorf("create kubernetes request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.bearerToken)
	req.Header.Set("Accept", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("kubernetes request %s %s: %w", method, apiPath, err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("kubernetes request %s %s failed: status=%d body=%s", method, apiPath, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if out != nil && len(body) > 0 {
		if err := json.Unmarshal(body, out); err != nil {
			return fmt.Errorf("decode kubernetes response: %w", err)
		}
	}
	return nil
}

func newClusterWorkloadResolver(apps []model.App, services []model.BackingService) clusterWorkloadResolver {
	resolver := clusterWorkloadResolver{
		appsByID:                make(map[string]model.App, len(apps)),
		servicesByID:            make(map[string]model.BackingService, len(services)),
		appsByNamespacedName:    make(map[string]model.App, len(apps)),
		servicesByNamespacedApp: make(map[string]model.BackingService, len(services)),
	}

	appConflicts := make(map[string]struct{})
	for _, app := range apps {
		if id := strings.TrimSpace(app.ID); id != "" {
			resolver.appsByID[id] = app
		}
		key := clusterNamespacedResourceKey(runtime.NamespaceForTenant(app.TenantID), runtimeResourceName(app.Name))
		if key == "" {
			continue
		}
		if existing, ok := resolver.appsByNamespacedName[key]; ok && existing.ID != app.ID {
			appConflicts[key] = struct{}{}
			continue
		}
		if _, conflicted := appConflicts[key]; conflicted {
			continue
		}
		resolver.appsByNamespacedName[key] = app
	}
	for key := range appConflicts {
		delete(resolver.appsByNamespacedName, key)
	}

	serviceConflicts := make(map[string]struct{})
	for _, service := range services {
		if id := strings.TrimSpace(service.ID); id != "" {
			resolver.servicesByID[id] = service
		}
		key := clusterNamespacedResourceKey(runtime.NamespaceForTenant(service.TenantID), clusterBackingServiceResourceName(service))
		if key == "" {
			continue
		}
		if existing, ok := resolver.servicesByNamespacedApp[key]; ok && existing.ID != service.ID {
			serviceConflicts[key] = struct{}{}
			continue
		}
		if _, conflicted := serviceConflicts[key]; conflicted {
			continue
		}
		resolver.servicesByNamespacedApp[key] = service
	}
	for key := range serviceConflicts {
		delete(resolver.servicesByNamespacedApp, key)
	}

	return resolver
}

func (r clusterWorkloadResolver) resolve(pods []clusterNodePod) []model.ClusterNodeWorkload {
	if len(pods) == 0 {
		return nil
	}

	workloads := make(map[string]*model.ClusterNodeWorkload)
	for _, pod := range pods {
		workload, ok := r.resolvePod(pod)
		if !ok {
			continue
		}
		workloadKey := workload.Kind + "\x00" + workload.ID
		if workloadKey == "\x00" {
			continue
		}
		entry, exists := workloads[workloadKey]
		if !exists {
			copied := workload
			entry = &copied
			workloads[workloadKey] = entry
		}
		entry.Pods = append(entry.Pods, model.ClusterNodeWorkloadPod{
			Name:  strings.TrimSpace(pod.Metadata.Name),
			Phase: strings.TrimSpace(pod.Status.Phase),
		})
		entry.PodCount = len(entry.Pods)
	}

	if len(workloads) == 0 {
		return nil
	}

	out := make([]model.ClusterNodeWorkload, 0, len(workloads))
	for _, workload := range workloads {
		sort.Slice(workload.Pods, func(i, j int) bool {
			if workload.Pods[i].Name == workload.Pods[j].Name {
				return workload.Pods[i].Phase < workload.Pods[j].Phase
			}
			return workload.Pods[i].Name < workload.Pods[j].Name
		})
		workload.PodCount = len(workload.Pods)
		out = append(out, *workload)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind == out[j].Kind {
			if out[i].Name == out[j].Name {
				return out[i].ID < out[j].ID
			}
			return out[i].Name < out[j].Name
		}
		return out[i].Kind < out[j].Kind
	})

	return out
}

func (r clusterWorkloadResolver) resolvePod(pod clusterNodePod) (model.ClusterNodeWorkload, bool) {
	labels := pod.Metadata.Labels
	if len(labels) == 0 {
		return model.ClusterNodeWorkload{}, false
	}

	if appID := strings.TrimSpace(labels[runtime.FugueLabelAppID]); appID != "" {
		if app, ok := r.appsByID[appID]; ok {
			return clusterNodeWorkloadForApp(app, pod.Metadata.Namespace), true
		}
	}
	if serviceID := strings.TrimSpace(labels[runtime.FugueLabelBackingServiceID]); serviceID != "" {
		if service, ok := r.servicesByID[serviceID]; ok {
			return clusterNodeWorkloadForService(service, pod.Metadata.Namespace), true
		}
	}

	if !strings.EqualFold(strings.TrimSpace(labels[runtime.FugueLabelManagedBy]), runtime.FugueLabelManagedByValue) {
		return model.ClusterNodeWorkload{}, false
	}
	resourceName := strings.TrimSpace(labels[runtime.FugueLabelName])
	if resourceName == "" {
		return model.ClusterNodeWorkload{}, false
	}
	key := clusterNamespacedResourceKey(strings.TrimSpace(pod.Metadata.Namespace), resourceName)
	if key == "" {
		return model.ClusterNodeWorkload{}, false
	}

	if strings.EqualFold(strings.TrimSpace(labels[runtime.FugueLabelComponent]), "postgres") {
		service, ok := r.servicesByNamespacedApp[key]
		if !ok {
			return model.ClusterNodeWorkload{}, false
		}
		return clusterNodeWorkloadForService(service, pod.Metadata.Namespace), true
	}

	app, ok := r.appsByNamespacedName[key]
	if !ok {
		return model.ClusterNodeWorkload{}, false
	}
	return clusterNodeWorkloadForApp(app, pod.Metadata.Namespace), true
}

func clusterNodeWorkloadForApp(app model.App, namespace string) model.ClusterNodeWorkload {
	runtimeID := strings.TrimSpace(app.Status.CurrentRuntimeID)
	if runtimeID == "" {
		runtimeID = strings.TrimSpace(app.Spec.RuntimeID)
	}
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		namespace = runtime.NamespaceForTenant(app.TenantID)
	}
	return model.ClusterNodeWorkload{
		Kind:      model.ClusterNodeWorkloadKindApp,
		ID:        app.ID,
		Name:      app.Name,
		TenantID:  app.TenantID,
		ProjectID: app.ProjectID,
		RuntimeID: runtimeID,
		Namespace: namespace,
	}
}

func clusterNodeWorkloadForService(service model.BackingService, namespace string) model.ClusterNodeWorkload {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		namespace = runtime.NamespaceForTenant(service.TenantID)
	}
	return model.ClusterNodeWorkload{
		Kind:        model.ClusterNodeWorkloadKindBackingService,
		ID:          service.ID,
		Name:        service.Name,
		TenantID:    service.TenantID,
		ProjectID:   service.ProjectID,
		ServiceType: service.Type,
		OwnerAppID:  service.OwnerAppID,
		Namespace:   namespace,
	}
}

func clusterBackingServiceResourceName(service model.BackingService) string {
	if service.Spec.Postgres != nil {
		if serviceName := strings.TrimSpace(service.Spec.Postgres.ServiceName); serviceName != "" {
			return serviceName
		}
	}

	name := runtimeResourceName(service.Name)
	if name == "" {
		return ""
	}
	if strings.EqualFold(strings.TrimSpace(service.Type), model.BackingServiceTypePostgres) && !strings.HasSuffix(name, "-postgres") {
		return name + "-postgres"
	}
	return name
}

func clusterNamespacedResourceKey(namespace, resourceName string) string {
	namespace = strings.TrimSpace(namespace)
	resourceName = strings.TrimSpace(resourceName)
	if namespace == "" || resourceName == "" {
		return ""
	}
	return namespace + "\x00" + resourceName
}

func kubeNodeReadyStatus(node kubeNode) string {
	for _, condition := range node.Status.Conditions {
		if strings.EqualFold(strings.TrimSpace(condition.Type), clusterNodeConditionReady) {
			switch strings.ToLower(strings.TrimSpace(condition.Status)) {
			case "true":
				return "ready"
			case "false":
				return "not-ready"
			default:
				return "unknown"
			}
		}
	}
	return "unknown"
}

func kubeNodeRoles(labels map[string]string) []string {
	roles := make([]string, 0, 2)
	for key := range labels {
		if strings.HasPrefix(key, "node-role.kubernetes.io/") {
			role := strings.TrimPrefix(key, "node-role.kubernetes.io/")
			role = strings.TrimSpace(role)
			if role == "" {
				role = "worker"
			}
			roles = append(roles, role)
		}
	}
	if legacyRole := strings.TrimSpace(labels["kubernetes.io/role"]); legacyRole != "" {
		roles = append(roles, legacyRole)
	}
	if len(roles) == 0 {
		return nil
	}
	sort.Strings(roles)
	deduped := roles[:0]
	var prev string
	for _, role := range roles {
		if role == prev {
			continue
		}
		deduped = append(deduped, role)
		prev = role
	}
	return deduped
}

func kubeNodeAddress(node kubeNode, addressType string) string {
	for _, address := range node.Status.Addresses {
		if strings.EqualFold(strings.TrimSpace(address.Type), addressType) {
			return strings.TrimSpace(address.Address)
		}
	}
	return ""
}

func kubeNodePublicIP(node kubeNode) string {
	if value := publicIPLiteral(firstNodeLabel(node.Metadata.Labels, clusterNodeLabelPublicIP)); value != "" {
		return value
	}
	return publicIPLiteral(kubeNodeAddress(node, "ExternalIP"))
}

func resolveClusterNodePublicIP(node model.ClusterNode, runtimeObj *model.Runtime) string {
	if value := publicIPLiteral(node.PublicIP); value != "" {
		return value
	}
	if runtimeObj == nil {
		return ""
	}
	return publicIPFromEndpoint(runtimeObj.Endpoint)
}

func publicIPFromEndpoint(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return ""
	}
	if value := publicIPLiteral(endpoint); value != "" {
		return value
	}
	if parsed, err := url.Parse(endpoint); err == nil && parsed.Host != "" {
		return publicIPLiteral(parsed.Hostname())
	}
	host, _, err := net.SplitHostPort(endpoint)
	if err != nil {
		return ""
	}
	return publicIPLiteral(host)
}

func publicIPLiteral(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	ip := net.ParseIP(value)
	if ip == nil {
		return ""
	}
	if ip.IsUnspecified() || ip.IsLoopback() || ip.IsMulticast() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsPrivate() {
		return ""
	}
	if clusterNodeCGNATCIDR.Contains(ip) {
		return ""
	}
	return value
}

func mustParseCIDR(value string) *net.IPNet {
	_, network, err := net.ParseCIDR(value)
	if err != nil {
		panic(err)
	}
	return network
}

func kubeNodeRegion(labels, annotations map[string]string) string {
	if value := firstNodeLabel(labels, clusterNodeLabelRegion, clusterNodeLabelLegacyRegion, "region"); value != "" {
		return value
	}
	if value := firstNodeAnnotation(annotations, clusterNodeAnnotationCountry); value != "" {
		return value
	}
	if value := firstNodeLabel(labels, clusterNodeLabelCountryCode); value != "" {
		return strings.ToUpper(value)
	}
	return ""
}

func kubeNodeZone(labels map[string]string) string {
	return firstNodeLabel(labels, clusterNodeLabelZone, clusterNodeLabelLegacyZone, "zone")
}

func firstNodeLabel(labels map[string]string, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(labels[key]); value != "" {
			return value
		}
	}
	return ""
}

func firstNodeAnnotation(annotations map[string]string, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(annotations[key]); value != "" {
			return value
		}
	}
	return ""
}

func normalizeKubeConditionStatus(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "true":
		return "true"
	case "false":
		return "false"
	default:
		return "unknown"
	}
}

func int64PointerFromQuantity(value string, parse func(string) (int64, bool)) *int64 {
	parsed, ok := parse(value)
	if !ok {
		return nil
	}
	return &parsed
}

func uint64PointerToInt64(value *uint64) *int64 {
	if value == nil || *value > uint64(^uint64(0)>>1) {
		return nil
	}
	parsed := int64(*value)
	return &parsed
}

func usagePercent(used *int64, totals ...*int64) *float64 {
	if used == nil || *used < 0 {
		return nil
	}
	for _, total := range totals {
		if total == nil || *total <= 0 {
			continue
		}
		value := math.Round((float64(*used)/float64(*total))*1000) / 10
		return &value
	}
	return nil
}

func isEmptyClusterNodeCPUStats(stats *model.ClusterNodeCPUStats) bool {
	return stats == nil ||
		(stats.CapacityMilliCores == nil &&
			stats.AllocatableMilliCores == nil &&
			stats.UsedMilliCores == nil &&
			stats.UsagePercent == nil)
}

func isEmptyClusterNodeMemoryStats(stats *model.ClusterNodeMemoryStats) bool {
	return stats == nil ||
		(stats.CapacityBytes == nil &&
			stats.AllocatableBytes == nil &&
			stats.UsedBytes == nil &&
			stats.UsagePercent == nil)
}

func isEmptyClusterNodeStorageStats(stats *model.ClusterNodeStorageStats) bool {
	return stats == nil ||
		(stats.CapacityBytes == nil &&
			stats.AllocatableBytes == nil &&
			stats.UsedBytes == nil &&
			stats.UsagePercent == nil)
}

func parseCPUQuantityMilli(value string) (int64, bool) {
	number, suffix, ok := splitKubeQuantity(value)
	if !ok {
		return 0, false
	}

	multiplier := 0.0
	switch suffix {
	case "n":
		multiplier = 1.0 / 1_000_000
	case "u":
		multiplier = 1.0 / 1_000
	case "m":
		multiplier = 1
	case "":
		multiplier = 1_000
	case "k", "K":
		multiplier = 1_000_000
	case "M":
		multiplier = 1_000_000_000
	case "G":
		multiplier = 1_000_000_000_000
	case "T":
		multiplier = 1_000_000_000_000_000
	default:
		return 0, false
	}

	parsed, err := strconv.ParseFloat(number, 64)
	if err != nil {
		return 0, false
	}
	return int64(math.Round(parsed * multiplier)), true
}

func parseBytesQuantity(value string) (int64, bool) {
	number, suffix, ok := splitKubeQuantity(value)
	if !ok {
		return 0, false
	}

	multiplier := 0.0
	switch suffix {
	case "":
		multiplier = 1
	case "K":
		multiplier = 1_000
	case "M":
		multiplier = 1_000_000
	case "G":
		multiplier = 1_000_000_000
	case "T":
		multiplier = 1_000_000_000_000
	case "P":
		multiplier = 1_000_000_000_000_000
	case "E":
		multiplier = 1_000_000_000_000_000_000
	case "Ki":
		multiplier = 1 << 10
	case "Mi":
		multiplier = 1 << 20
	case "Gi":
		multiplier = 1 << 30
	case "Ti":
		multiplier = 1 << 40
	case "Pi":
		multiplier = 1 << 50
	case "Ei":
		multiplier = 1 << 60
	default:
		return 0, false
	}

	parsed, err := strconv.ParseFloat(number, 64)
	if err != nil {
		return 0, false
	}
	return int64(math.Round(parsed * multiplier)), true
}

func splitKubeQuantity(value string) (string, string, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", "", false
	}
	matches := kubeQuantityPattern.FindStringSubmatch(value)
	if len(matches) != 3 {
		return "", "", false
	}
	return matches[1], matches[2], true
}

func parseClusterNodeTimestamp(value string) *time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return nil
	}
	parsed = parsed.UTC()
	return &parsed
}
