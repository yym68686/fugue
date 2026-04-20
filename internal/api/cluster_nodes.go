package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
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
	"fugue/internal/store"
)

const (
	clusterNodeManagedPodLabelSelector  = "app.kubernetes.io/managed-by=fugue,app.kubernetes.io/name"
	clusterNodeCNPGPodLabelSelector     = "app.kubernetes.io/managed-by=cloudnative-pg,cnpg.io/cluster"
	clusterNodeCNPGClusterLabel         = "cnpg.io/cluster"
	clusterNodeStatsConcurrency         = 4
	clusterNodeConditionReady           = "Ready"
	clusterNodeConditionMemory          = "MemoryPressure"
	clusterNodeConditionDisk            = "DiskPressure"
	clusterNodeConditionPID             = "PIDPressure"
	clusterNodeLabelRegion              = "topology.kubernetes.io/region"
	clusterNodeLabelLegacyRegion        = "failure-domain.beta.kubernetes.io/region"
	clusterNodeLabelZone                = "topology.kubernetes.io/zone"
	clusterNodeLabelLegacyZone          = "failure-domain.beta.kubernetes.io/zone"
	clusterNodeLabelCountryCode         = "fugue.io/location-country-code"
	clusterNodeLabelPublicIP            = "fugue.io/public-ip"
	clusterNodeAnnotationCountry        = "fugue.io/location-country"
	clusterNodeAnnotationK3sHost        = "k3s.io/hostname"
	defaultClusterNodeInventoryCacheTTL = 30 * time.Second
	clusterNodeInventoryMaxStale        = 5 * time.Minute
	clusterNodeInventoryRefreshTimeout  = 5 * time.Second
	clusterNodeInventoryWarmInterval    = 25 * time.Second
	tenantSharedClusterNodeName         = "internal-cluster"
	tenantSharedClusterRegion           = "Multiple countries"
	tenantSharedRuntimeID               = "runtime_managed_shared"
	clusterJoinTokenNamespace           = "kube-system"
	clusterJoinTokenSecretPrefix        = "bootstrap-token-"
	clusterJoinTokenLabelManaged        = "fugue.pro/cluster-join-bootstrap"
	clusterJoinTokenLabelNodeKey        = "fugue.pro/node-key-id"
	clusterJoinTokenLabelRuntime        = "fugue.pro/runtime-id"
	clusterJoinTokenLabelValue          = "true"
	clusterJoinTokenAuthGroup           = "system:bootstrappers:k3s:default-node-token"
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
	node         model.ClusterNode
	identity     clusterNodeIdentity
	managedOwned bool
	sharedPool   bool
	countryCode  string
	runtimeID    string
	labels       map[string]string
	pods         []clusterNodePod
	summary      *kubeNodeSummary
}

type resolvedClusterNodeSnapshot struct {
	snapshot  clusterNodeSnapshot
	workloads []model.ClusterNodeWorkload
}

type clusterNodeIdentity struct {
	machineID  string
	systemUUID string
	hostname   string
}

type clusterWorkloadResolver struct {
	appsByID                map[string]model.App
	servicesByID            map[string]model.BackingService
	appsByNamespacedName    map[string]model.App
	servicesByNamespacedApp map[string]model.BackingService
}

type managedSharedLocationSyncState struct {
	mu       sync.Mutex
	lastHash string
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
	Spec struct {
		Taints []kubeNodeTaint `json:"taints"`
	} `json:"spec"`
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
			MachineID        string `json:"machineID"`
			SystemUUID       string `json:"systemUUID"`
		} `json:"nodeInfo"`
	} `json:"status"`
}

type kubeNodeTaint struct {
	Key    string `json:"key,omitempty"`
	Value  string `json:"value,omitempty"`
	Effect string `json:"effect,omitempty"`
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

type kubeSecretList struct {
	Items []kubeSecret `json:"items"`
}

type kubeSecret struct {
	Metadata struct {
		Name        string            `json:"name"`
		Labels      map[string]string `json:"labels"`
		Annotations map[string]string `json:"annotations"`
	} `json:"metadata"`
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
	Node kubeNodeSummaryNode  `json:"node"`
	Pods []kubeNodeSummaryPod `json:"pods,omitempty"`
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

type kubeNodeSummaryPod struct {
	PodRef           kubeNodeSummaryPodRef `json:"podRef"`
	CPU              kubeNodeSummaryCPU    `json:"cpu,omitempty"`
	Memory           kubeNodeSummaryMem    `json:"memory,omitempty"`
	EphemeralStorage kubeNodeSummaryFS     `json:"ephemeral-storage,omitempty"`
}

type kubeNodeSummaryPodRef struct {
	Name      string `json:"name,omitempty"`
	Namespace string `json:"namespace,omitempty"`
}

const clusterNodeInventoryCacheKey = "shared"

func (s *Server) loadClusterNodeInventory(ctx context.Context) ([]clusterNodeSnapshot, error) {
	timings := serverTimingFromContext(ctx)
	startedAt := time.Now()
	now := time.Now()

	if entry, ok := s.clusterNodeInventoryCache.getEntry(clusterNodeInventoryCacheKey); ok {
		if now.Before(entry.expiresAt) {
			timings.Add("cluster_inventory", time.Since(startedAt))
			return entry.value, nil
		}
		if now.Before(entry.expiresAt.Add(clusterNodeInventoryMaxStale)) {
			s.refreshClusterNodeInventoryAsync()
			timings.Add("cluster_inventory", time.Since(startedAt))
			return entry.value, nil
		}
	}

	snapshots, err := s.refreshClusterNodeInventory(ctx)
	timings.Add("cluster_inventory", time.Since(startedAt))
	if err != nil {
		return nil, err
	}

	return snapshots, nil
}

func (s *Server) StartBackgroundWarmers(ctx context.Context) {
	if s == nil || ctx == nil || !s.shouldWarmClusterNodeInventory() {
		return
	}
	s.startClusterNodeInventoryWarmLoop(ctx)
}

func (s *Server) shouldWarmClusterNodeInventory() bool {
	if s == nil {
		return false
	}
	if s.newClusterNodeClient != nil {
		return true
	}
	return strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_HOST")) != "" &&
		strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_PORT")) != ""
}

func (s *Server) startClusterNodeInventoryWarmLoop(ctx context.Context) {
	s.refreshClusterNodeInventoryAsync()

	go func() {
		ticker := time.NewTicker(clusterNodeInventoryWarmInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.refreshClusterNodeInventoryAsync()
			}
		}
	}()
}

func (s *Server) clusterNodeInventoryRefreshContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		return context.WithTimeout(context.Background(), clusterNodeInventoryRefreshTimeout)
	}
	return context.WithTimeout(parent, clusterNodeInventoryRefreshTimeout)
}

func (s *Server) fetchClusterNodeInventory(ctx context.Context) ([]clusterNodeSnapshot, error) {
	clientFactory := s.newClusterNodeClient
	if clientFactory == nil {
		clientFactory = newClusterNodeClient
	}

	client, err := clientFactory()
	if err != nil {
		return nil, err
	}

	refreshCtx, cancel := s.clusterNodeInventoryRefreshContext(ctx)
	defer cancel()

	return client.listClusterNodeInventory(refreshCtx)
}

func (s *Server) refreshClusterNodeInventory(ctx context.Context) ([]clusterNodeSnapshot, error) {
	return s.clusterNodeInventoryCache.do(
		clusterNodeInventoryCacheKey,
		func() ([]clusterNodeSnapshot, error) {
			return s.fetchClusterNodeInventory(ctx)
		},
	)
}

func (s *Server) refreshClusterNodeInventoryAsync() {
	if s == nil {
		return
	}

	ch := s.clusterNodeInventoryCache.group.DoChan(clusterNodeInventoryCacheKey, func() (any, error) {
		value, err := s.fetchClusterNodeInventory(context.Background())
		if err != nil {
			var zero []clusterNodeSnapshot
			return zero, err
		}
		s.clusterNodeInventoryCache.set(clusterNodeInventoryCacheKey, value)
		if err := s.syncBootstrapControlPlaneMachinesFromSnapshots(value); err != nil {
			var zero []clusterNodeSnapshot
			return zero, err
		}
		return value, nil
	})

	go func() {
		result, ok := <-ch
		if !ok || result.Err == nil || s.log == nil {
			return
		}
		s.log.Printf("cluster inventory background refresh error: %v", result.Err)
	}()
}

func (s *Server) handleListClusterNodes(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	timings := serverTimingFromContext(r.Context())
	syncLocations, err := readBoolQuery(r, "sync_locations", true)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	snapshots, err := s.loadClusterNodeInventory(r.Context())
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	if syncLocations {
		syncStartedAt := time.Now()
		if err := s.syncManagedSharedLocationRuntimesFromSnapshots(snapshots); err != nil {
			s.writeStoreError(w, err)
			return
		}
		timings.Add("runtime_sync", time.Since(syncStartedAt))
	}
	managedSharedRuntime, err := s.store.GetRuntime(tenantSharedRuntimeID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	_, defaultSharedDisplayRegion, _ := selectDefaultManagedSharedLocation(snapshots)

	storeNodesStartedAt := time.Now()
	runtimes, err := s.store.ListNodes(principal.TenantID, principal.IsPlatformAdmin())
	timings.Add("store_nodes", time.Since(storeNodesStartedAt))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	machines := []model.Machine(nil)
	if principal.IsPlatformAdmin() {
		storeMachinesStartedAt := time.Now()
		machines, err = s.store.ListMachines(principal.TenantID, true)
		timings.Add("store_machines", time.Since(storeMachinesStartedAt))
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		machines, err = s.ensureBootstrapControlPlaneMachines(snapshots, runtimes, machines)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
	}

	storeAppsStartedAt := time.Now()
	apps, err := s.store.ListApps(principal.TenantID, principal.IsPlatformAdmin())
	timings.Add("store_apps", time.Since(storeAppsStartedAt))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	storeServicesStartedAt := time.Now()
	services, err := s.store.ListBackingServices(principal.TenantID, principal.IsPlatformAdmin())
	timings.Add("store_services", time.Since(storeServicesStartedAt))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	workloadResolver := newClusterWorkloadResolver(apps, services)
	resolvedSnapshots := make([]resolvedClusterNodeSnapshot, 0, len(snapshots))
	for _, snapshot := range snapshots {
		resolvedSnapshots = append(resolvedSnapshots, resolvedClusterNodeSnapshot{
			snapshot:  snapshot,
			workloads: workloadResolver.resolve(snapshot.pods),
		})
	}
	filtered := buildVisibleClusterNodesFromResolved(
		principal,
		resolvedSnapshots,
		runtimes,
		machines,
		managedSharedRuntime,
		defaultSharedDisplayRegion,
	)

	httpx.WriteJSON(w, http.StatusOK, map[string]any{"cluster_nodes": filtered})
}

func (s *Server) syncBootstrapControlPlaneMachinesFromSnapshots(snapshots []clusterNodeSnapshot) error {
	if s == nil || len(snapshots) == 0 {
		return nil
	}

	runtimes, err := s.store.ListNodes("", true)
	if err != nil {
		return err
	}
	machines, err := s.store.ListMachines("", true)
	if err != nil {
		return err
	}
	_, err = s.ensureBootstrapControlPlaneMachines(snapshots, runtimes, machines)
	return err
}

// Tenant responses collapse the shared pool into one synthetic node so the
// control plane does not leak internal server count or per-node locations.
func buildTenantSharedClusterNode(snapshots []resolvedClusterNodeSnapshot, runtimeObj model.Runtime, defaultDisplayRegion string) (model.ClusterNode, bool) {
	if len(snapshots) == 0 {
		return model.ClusterNode{}, false
	}

	mergedWorkloads := make([]model.ClusterNodeWorkload, 0)
	workloadIndexes := make(map[string]int)
	status := "unknown"
	bestStatusRank := -1
	var createdAt *time.Time

	for _, snapshot := range snapshots {
		rank := clusterNodeStatusRank(snapshot.snapshot.node.Status)
		if rank > bestStatusRank {
			bestStatusRank = rank
			status = snapshot.snapshot.node.Status
		}

		if snapshot.snapshot.node.CreatedAt != nil {
			if createdAt == nil || snapshot.snapshot.node.CreatedAt.Before(*createdAt) {
				next := *snapshot.snapshot.node.CreatedAt
				createdAt = &next
			}
		}

		for _, workload := range snapshot.workloads {
			key := workload.Kind + "\x00" + workload.ID
			if index, ok := workloadIndexes[key]; ok {
				mergedWorkloads[index].PodCount += workload.PodCount
				mergedWorkloads[index].Pods = append(mergedWorkloads[index].Pods, workload.Pods...)
				continue
			}

			workloadIndexes[key] = len(mergedWorkloads)
			mergedWorkloads = append(mergedWorkloads, workload)
		}
	}

	sort.Slice(mergedWorkloads, func(i, j int) bool {
		if mergedWorkloads[i].Kind != mergedWorkloads[j].Kind {
			return mergedWorkloads[i].Kind < mergedWorkloads[j].Kind
		}
		if mergedWorkloads[i].Name != mergedWorkloads[j].Name {
			return mergedWorkloads[i].Name < mergedWorkloads[j].Name
		}
		return mergedWorkloads[i].ID < mergedWorkloads[j].ID
	})
	for index := range mergedWorkloads {
		sort.Slice(mergedWorkloads[index].Pods, func(i, j int) bool {
			return mergedWorkloads[index].Pods[i].Name < mergedWorkloads[index].Pods[j].Name
		})
	}
	displayRegion := strings.TrimSpace(defaultDisplayRegion)
	if region, ok := uniqueSharedWorkloadDisplayRegion(snapshots); ok {
		displayRegion = region
	} else if sharedLocationCount(snapshots) > 1 {
		displayRegion = tenantSharedClusterRegion
	} else if runtimeRegion := managedSharedRuntimeDisplayRegion(runtimeObj, snapshots); runtimeRegion != "" {
		displayRegion = runtimeRegion
	}
	if displayRegion == "" {
		displayRegion = tenantSharedClusterRegion
	}

	return model.ClusterNode{
		Name:      tenantSharedClusterNodeName,
		Status:    status,
		Region:    displayRegion,
		RuntimeID: tenantSharedRuntimeID,
		Workloads: mergedWorkloads,
		CreatedAt: createdAt,
	}, true
}

func (s *Server) syncManagedSharedLocationRuntimesFromSnapshots(snapshots []clusterNodeSnapshot) error {
	locations := sharedLocationLabelSet(snapshots)
	hash := managedSharedLocationSetHash(locations)

	s.managedSharedLocationSync.mu.Lock()
	defer s.managedSharedLocationSync.mu.Unlock()

	if hash == s.managedSharedLocationSync.lastHash {
		return nil
	}
	if err := s.store.SyncManagedSharedLocationRuntimes(locations); err != nil {
		return err
	}
	s.managedSharedLocationSync.lastHash = hash
	return nil
}

func (s *Server) trySyncManagedSharedLocationRuntimes(ctx context.Context) {
	snapshots, err := s.loadClusterNodeInventory(ctx)
	if err != nil {
		if s.log != nil {
			s.log.Printf("skip managed shared location sync after inventory failure: %v", err)
		}
		return
	}

	if err := s.syncManagedSharedLocationRuntimesFromSnapshots(snapshots); err != nil && s.log != nil {
		s.log.Printf("skip managed shared location sync after store failure: %v", err)
	}
}

func selectDefaultManagedSharedLocation(snapshots []clusterNodeSnapshot) (map[string]string, string, bool) {
	choose := func(requirePods bool) (clusterNodeSnapshot, bool) {
		var best clusterNodeSnapshot
		found := false
		for _, snapshot := range snapshots {
			if !sharedClusterSnapshotCandidate(snapshot) || !clusterNodeSnapshotHasLocation(snapshot) {
				continue
			}
			if requirePods && len(snapshot.pods) == 0 {
				continue
			}
			if !found || preferSharedLocationSnapshot(snapshot, best) {
				best = snapshot
				found = true
			}
		}
		return best, found
	}

	best, ok := choose(true)
	if !ok {
		best, ok = choose(false)
	}
	if !ok {
		return nil, "", false
	}

	return sharedLocationLabels(best), strings.TrimSpace(best.node.Region), true
}

func sharedClusterSnapshotCandidate(snapshot clusterNodeSnapshot) bool {
	if snapshot.sharedPool {
		return true
	}
	if snapshot.runtimeID == "" {
		return true
	}
	return strings.EqualFold(snapshot.runtimeID, tenantSharedRuntimeID)
}

func clusterNodeSnapshotHasLocation(snapshot clusterNodeSnapshot) bool {
	return snapshot.countryCode != "" || strings.TrimSpace(snapshot.node.Region) != ""
}

func sharedLocationLabels(snapshot clusterNodeSnapshot) map[string]string {
	if snapshot.countryCode != "" {
		return map[string]string{
			runtime.LocationCountryCodeLabelKey: snapshot.countryCode,
		}
	}
	if region := strings.TrimSpace(snapshot.node.Region); region != "" {
		return map[string]string{
			runtime.RegionLabelKey: region,
		}
	}
	return nil
}

func sharedLocationLabelSet(snapshots []clusterNodeSnapshot) []map[string]string {
	out := make([]map[string]string, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if !sharedClusterSnapshotCandidate(snapshot) || !clusterNodeSnapshotHasLocation(snapshot) {
			continue
		}
		out = append(out, sharedLocationLabels(snapshot))
	}
	return out
}

func managedSharedLocationSetHash(locations []map[string]string) string {
	if len(locations) == 0 {
		return "empty"
	}

	entries := make([]string, 0, len(locations))
	for _, labels := range locations {
		if len(labels) == 0 {
			continue
		}

		keys := make([]string, 0, len(labels))
		for key := range labels {
			keys = append(keys, key)
		}
		sort.Strings(keys)

		parts := make([]string, 0, len(keys))
		for _, key := range keys {
			parts = append(parts, key+"="+strings.TrimSpace(labels[key]))
		}
		entries = append(entries, strings.Join(parts, ","))
	}

	if len(entries) == 0 {
		return "empty"
	}

	sort.Strings(entries)
	sum := sha256.Sum256([]byte(strings.Join(entries, "\n")))
	return hex.EncodeToString(sum[:8])
}

func preferSharedLocationSnapshot(left, right clusterNodeSnapshot) bool {
	leftHasCountry := left.countryCode != ""
	rightHasCountry := right.countryCode != ""
	if leftHasCountry != rightHasCountry {
		return leftHasCountry
	}

	leftStatusRank := clusterNodeStatusRank(left.node.Status)
	rightStatusRank := clusterNodeStatusRank(right.node.Status)
	if leftStatusRank != rightStatusRank {
		return leftStatusRank > rightStatusRank
	}

	leftCreatedAt := clusterNodeSnapshotCreatedAt(left)
	rightCreatedAt := clusterNodeSnapshotCreatedAt(right)
	if !leftCreatedAt.Equal(rightCreatedAt) {
		return leftCreatedAt.Before(rightCreatedAt)
	}

	return left.node.Name < right.node.Name
}

func uniqueSharedWorkloadDisplayRegion(snapshots []resolvedClusterNodeSnapshot) (string, bool) {
	found := false
	var locationKey string
	var region string

	for _, snapshot := range snapshots {
		if len(snapshot.workloads) == 0 || !clusterNodeSnapshotHasLocation(snapshot.snapshot) {
			continue
		}

		nextKey := sharedLocationKey(snapshot.snapshot)
		nextRegion := strings.TrimSpace(snapshot.snapshot.node.Region)
		if !found {
			found = true
			locationKey = nextKey
			region = nextRegion
			continue
		}
		if locationKey != nextKey {
			return "", false
		}
	}

	if !found {
		return "", false
	}
	return region, true
}

func managedSharedRuntimeDisplayRegion(runtimeObj model.Runtime, snapshots []resolvedClusterNodeSnapshot) string {
	if runtimeObj.ID == "" {
		return ""
	}
	if countryCode := strings.TrimSpace(runtimeObj.Labels[runtime.LocationCountryCodeLabelKey]); countryCode != "" {
		countryCode = strings.ToLower(countryCode)
		for _, snapshot := range snapshots {
			if snapshot.snapshot.countryCode == countryCode && strings.TrimSpace(snapshot.snapshot.node.Region) != "" {
				return strings.TrimSpace(snapshot.snapshot.node.Region)
			}
		}
		return countryCode
	}
	if region := strings.TrimSpace(runtimeObj.Labels[runtime.RegionLabelKey]); region != "" {
		return region
	}
	return ""
}

func sharedLocationKey(snapshot clusterNodeSnapshot) string {
	if snapshot.countryCode != "" {
		return "country:" + snapshot.countryCode
	}
	return "region:" + strings.ToLower(strings.TrimSpace(snapshot.node.Region))
}

func sharedLocationCount(snapshots []resolvedClusterNodeSnapshot) int {
	if len(snapshots) == 0 {
		return 0
	}

	keys := map[string]struct{}{}
	for _, snapshot := range snapshots {
		if !clusterNodeSnapshotHasLocation(snapshot.snapshot) {
			continue
		}
		keys[sharedLocationKey(snapshot.snapshot)] = struct{}{}
	}
	return len(keys)
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

	podsByNode, err := c.listManagedPodsByNode(ctx)
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
			node:         buildClusterNode(item, summariesByNode[name]),
			identity:     buildClusterNodeIdentity(item),
			managedOwned: strings.EqualFold(firstNodeLabel(item.Metadata.Labels, runtime.NodeModeLabelKey), model.RuntimeTypeManagedOwned),
			sharedPool:   strings.EqualFold(firstNodeLabel(item.Metadata.Labels, runtime.SharedPoolLabelKey), runtime.SharedPoolLabelValue),
			countryCode:  kubeNodeCountryCode(item.Metadata.Labels),
			runtimeID:    firstNodeLabel(item.Metadata.Labels, runtime.RuntimeIDLabelKey),
			labels:       cloneNodeLabels(item.Metadata.Labels),
			pods:         podsByNode[name],
			summary:      summariesByNode[name],
		})
	}
	return snapshots, nil
}

func (c *clusterNodeClient) listManagedPodsByNode(ctx context.Context) (map[string][]clusterNodePod, error) {
	selectors := []string{
		clusterNodeManagedPodLabelSelector,
		clusterNodeCNPGPodLabelSelector,
	}
	podsByNode := make(map[string][]clusterNodePod)
	seenPods := make(map[string]struct{})

	for _, selector := range selectors {
		query := url.Values{}
		query.Set("labelSelector", selector)

		var podList clusterNodePodList
		if err := c.doJSON(ctx, http.MethodGet, "/api/v1/pods?"+query.Encode(), &podList); err != nil {
			return nil, err
		}

		for _, pod := range podList.Items {
			nodeName := strings.TrimSpace(pod.Spec.NodeName)
			if nodeName == "" {
				continue
			}
			phase := strings.TrimSpace(pod.Status.Phase)
			if strings.EqualFold(phase, "Succeeded") || strings.EqualFold(phase, "Failed") {
				continue
			}

			podKey := clusterNamespacedResourceKey(strings.TrimSpace(pod.Metadata.Namespace), strings.TrimSpace(pod.Metadata.Name))
			if podKey != "" {
				if _, exists := seenPods[podKey]; exists {
					continue
				}
				seenPods[podKey] = struct{}{}
			}

			podsByNode[nodeName] = append(podsByNode[nodeName], pod)
		}
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

func (c *clusterNodeClient) deleteNode(ctx context.Context, nodeName string) error {
	apiPath := "/api/v1/nodes/" + url.PathEscape(strings.TrimSpace(nodeName))
	return c.doJSON(ctx, http.MethodDelete, apiPath, nil)
}

func (c *clusterNodeClient) createBootstrapToken(ctx context.Context, nodeKeyID, runtimeID, caHash string, ttl time.Duration) (string, string, error) {
	tokenID, err := randomHex(3)
	if err != nil {
		return "", "", fmt.Errorf("generate bootstrap token id: %w", err)
	}
	tokenSecret, err := randomHex(8)
	if err != nil {
		return "", "", fmt.Errorf("generate bootstrap token secret: %w", err)
	}
	tokenID = strings.ToLower(strings.TrimSpace(tokenID))
	tokenSecret = strings.ToLower(strings.TrimSpace(tokenSecret))
	secretName := clusterJoinBootstrapSecretName(tokenID)
	secretLabels := map[string]string{
		clusterJoinTokenLabelManaged: clusterJoinTokenLabelValue,
		clusterJoinTokenLabelNodeKey: strings.TrimSpace(nodeKeyID),
	}
	if runtimeID = strings.TrimSpace(runtimeID); runtimeID != "" {
		secretLabels[clusterJoinTokenLabelRuntime] = runtimeID
	}
	payload := map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata": map[string]any{
			"name":      secretName,
			"namespace": clusterJoinTokenNamespace,
			"labels":    secretLabels,
		},
		"type": "bootstrap.kubernetes.io/token",
		"stringData": map[string]string{
			"description":                    "fugue join bootstrap token",
			"token-id":                       tokenID,
			"token-secret":                   tokenSecret,
			"expiration":                     time.Now().UTC().Add(ttl).Format(time.RFC3339),
			"usage-bootstrap-authentication": "true",
			"usage-bootstrap-signing":        "true",
			"auth-extra-groups":              clusterJoinTokenAuthGroup,
		},
	}
	if err := c.doJSONWithBody(ctx, http.MethodPost, "/api/v1/namespaces/"+clusterJoinTokenNamespace+"/secrets", payload, nil); err != nil {
		return "", "", err
	}
	token := tokenID + "." + tokenSecret
	if normalizedHash := normalizeClusterJoinCAHash(caHash); normalizedHash != "" {
		token = "K10" + normalizedHash + "::" + token
	}
	return token, tokenID, nil
}

func (c *clusterNodeClient) deleteBootstrapToken(ctx context.Context, tokenID string) error {
	apiPath := "/api/v1/namespaces/" + clusterJoinTokenNamespace + "/secrets/" + url.PathEscape(clusterJoinBootstrapSecretName(tokenID))
	return c.doJSON(ctx, http.MethodDelete, apiPath, nil)
}

func (c *clusterNodeClient) getSecret(ctx context.Context, namespace, name string) (kubeSecret, error) {
	var secret kubeSecret
	apiPath := "/api/v1/namespaces/" + url.PathEscape(strings.TrimSpace(namespace)) + "/secrets/" + url.PathEscape(strings.TrimSpace(name))
	if err := c.doJSON(ctx, http.MethodGet, apiPath, &secret); err != nil {
		return kubeSecret{}, err
	}
	return secret, nil
}

func (c *clusterNodeClient) deleteBootstrapTokenIfOwned(ctx context.Context, tokenID, nodeKeyID string) (bool, error) {
	tokenID = strings.TrimSpace(tokenID)
	nodeKeyID = strings.TrimSpace(nodeKeyID)
	if tokenID == "" || nodeKeyID == "" {
		return false, nil
	}
	secretName := clusterJoinBootstrapSecretName(tokenID)
	secret, err := c.getSecret(ctx, clusterJoinTokenNamespace, secretName)
	if err != nil {
		if isKubernetesNodeNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if strings.TrimSpace(secret.Metadata.Labels[clusterJoinTokenLabelNodeKey]) != nodeKeyID {
		return false, nil
	}
	if err := c.deleteBootstrapToken(ctx, tokenID); err != nil {
		if isKubernetesNodeNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (c *clusterNodeClient) deleteBootstrapTokensByNodeKey(ctx context.Context, nodeKeyID string) ([]string, error) {
	nodeKeyID = strings.TrimSpace(nodeKeyID)
	if nodeKeyID == "" {
		return nil, nil
	}
	query := url.Values{}
	query.Set("labelSelector", clusterJoinTokenLabelManaged+"="+clusterJoinTokenLabelValue+","+clusterJoinTokenLabelNodeKey+"="+nodeKeyID)
	var secretList kubeSecretList
	apiPath := "/api/v1/namespaces/" + clusterJoinTokenNamespace + "/secrets?" + query.Encode()
	if err := c.doJSON(ctx, http.MethodGet, apiPath, &secretList); err != nil {
		if isKubernetesNodeNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	deleted := make([]string, 0, len(secretList.Items))
	for _, secret := range secretList.Items {
		name := strings.TrimSpace(secret.Metadata.Name)
		if name == "" {
			continue
		}
		tokenID := clusterJoinBootstrapTokenIDFromSecretName(name)
		if tokenID == "" {
			continue
		}
		if err := c.deleteBootstrapToken(ctx, tokenID); err != nil {
			if isKubernetesNodeNotFound(err) {
				continue
			}
			return deleted, err
		}
		deleted = append(deleted, tokenID)
	}
	return deleted, nil
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

func buildClusterNodeIdentity(node kubeNode) clusterNodeIdentity {
	hostname := kubeNodeAddress(node, "Hostname")
	if hostname == "" {
		hostname = firstNodeAnnotation(node.Metadata.Annotations, clusterNodeAnnotationK3sHost)
	}
	return clusterNodeIdentity{
		machineID:  strings.TrimSpace(node.Status.NodeInfo.MachineID),
		systemUUID: strings.TrimSpace(node.Status.NodeInfo.SystemUUID),
		hostname:   strings.TrimSpace(hostname),
	}
}

func clusterNodeHasRole(node model.ClusterNode, role string) bool {
	role = strings.TrimSpace(role)
	if role == "" {
		return false
	}
	for _, candidate := range node.Roles {
		if strings.EqualFold(strings.TrimSpace(candidate), role) {
			return true
		}
	}
	return false
}

func shouldSeedBootstrapControlPlaneMachine(snapshot clusterNodeSnapshot) bool {
	if strings.TrimSpace(snapshot.node.Name) == "" {
		return false
	}
	return clusterNodeHasRole(snapshot.node, "control-plane")
}

func needsLegacyBootstrapControlPlaneMachineBackfill(snapshot clusterNodeSnapshot, machine model.Machine) bool {
	if !shouldSeedBootstrapControlPlaneMachine(snapshot) {
		return false
	}
	return store.NeedsLegacyPlatformMachinePolicyBackfill(machine, snapshot.labels)
}

func needsLegacyBootstrapControlPlaneRoleBackfill(snapshot clusterNodeSnapshot, machine model.Machine) bool {
	if !shouldSeedBootstrapControlPlaneMachine(snapshot) {
		return false
	}
	if model.NormalizeMachineScope(machine.Scope) != model.MachineScopePlatformNode {
		return false
	}
	if strings.TrimSpace(machine.TenantID) != "" || strings.TrimSpace(machine.RuntimeID) != "" {
		return false
	}
	if role := model.NormalizeMachineControlPlaneRole(machine.Policy.DesiredControlPlaneRole); role != "" && role != model.MachineControlPlaneRoleNone {
		return false
	}
	if _, ok := snapshot.labels[runtime.ControlPlaneDesiredRoleKey]; ok {
		return false
	}
	return true
}

func (s *Server) clusterNodePolicyAuditNodeNames() (map[string]struct{}, error) {
	events, err := s.store.ListAuditEvents("", true)
	if err != nil {
		return nil, err
	}
	nodeNames := make(map[string]struct{}, len(events))
	for _, event := range events {
		if event.Action != "cluster.node.policy" {
			continue
		}
		nodeName := strings.TrimSpace(event.TargetID)
		if nodeName == "" {
			nodeName = strings.TrimSpace(event.Metadata["cluster_node_name"])
		}
		if nodeName == "" {
			continue
		}
		nodeNames[nodeName] = struct{}{}
	}
	return nodeNames, nil
}

func bootstrapControlPlaneMachineName(snapshot clusterNodeSnapshot) string {
	if value := strings.TrimSpace(snapshot.identity.hostname); value != "" {
		return value
	}
	if value := strings.TrimSpace(snapshot.node.Name); value != "" {
		return value
	}
	if value := strings.TrimSpace(bootstrapControlPlaneMachineEndpoint(snapshot)); value != "" {
		return value
	}
	return "node"
}

func bootstrapControlPlaneMachineEndpoint(snapshot clusterNodeSnapshot) string {
	return coalesceNodeName(
		snapshot.node.PublicIP,
		snapshot.node.ExternalIP,
		snapshot.node.InternalIP,
	)
}

func bootstrapControlPlaneMachineFingerprint(snapshot clusterNodeSnapshot) string {
	switch {
	case strings.TrimSpace(snapshot.identity.machineID) != "":
		return strings.TrimSpace(snapshot.identity.machineID)
	case strings.TrimSpace(snapshot.identity.systemUUID) != "":
		return strings.TrimSpace(snapshot.identity.systemUUID)
	case strings.TrimSpace(snapshot.identity.hostname) != "":
		return strings.TrimSpace(snapshot.identity.hostname)
	default:
		return strings.TrimSpace(snapshot.node.Name)
	}
}

func (s *Server) ensureBootstrapControlPlaneMachines(snapshots []clusterNodeSnapshot, runtimes []model.Runtime, machines []model.Machine) ([]model.Machine, error) {
	if len(snapshots) == 0 {
		return machines, nil
	}
	runtimeByClusterNode := buildRuntimeByClusterNodeIndex(runtimes)
	_, machineByClusterNode := buildMachineIndexes(machines)
	roleBackfillCandidates := make(map[string]struct{})
	for _, snapshot := range snapshots {
		nodeName := strings.TrimSpace(snapshot.node.Name)
		if !shouldSeedBootstrapControlPlaneMachine(snapshot) {
			continue
		}
		if _, ok := runtimeByClusterNode[nodeName]; ok {
			continue
		}
		machine, ok := machineByClusterNode[nodeName]
		if !ok {
			continue
		}
		if needsLegacyBootstrapControlPlaneMachineBackfill(snapshot, machine) {
			continue
		}
		if needsLegacyBootstrapControlPlaneRoleBackfill(snapshot, machine) {
			roleBackfillCandidates[nodeName] = struct{}{}
		}
	}

	policyAuditNodeNames := map[string]struct{}{}
	if len(roleBackfillCandidates) > 0 {
		auditedNodeNames, err := s.clusterNodePolicyAuditNodeNames()
		if err != nil {
			return nil, err
		}
		policyAuditNodeNames = auditedNodeNames
	}

	changed := false
	for _, snapshot := range snapshots {
		nodeName := strings.TrimSpace(snapshot.node.Name)
		if !shouldSeedBootstrapControlPlaneMachine(snapshot) {
			continue
		}
		if _, ok := runtimeByClusterNode[nodeName]; ok {
			continue
		}
		if machine, ok := machineByClusterNode[nodeName]; ok {
			if !needsLegacyBootstrapControlPlaneMachineBackfill(snapshot, machine) {
				if _, candidate := roleBackfillCandidates[nodeName]; candidate {
					if _, audited := policyAuditNodeNames[nodeName]; !audited {
						nextPolicy := machine.Policy
						nextPolicy.DesiredControlPlaneRole = model.MachineControlPlaneRoleMember
						if _, err := s.store.SetMachinePolicyByClusterNodeName(nodeName, nextPolicy); err != nil {
							return nil, err
						}
						changed = true
					}
				}
				continue
			}
			if _, err := s.store.EnsurePlatformMachineForClusterNode(
				nodeName,
				bootstrapControlPlaneMachineEndpoint(snapshot),
				snapshot.labels,
				bootstrapControlPlaneMachineName(snapshot),
				bootstrapControlPlaneMachineFingerprint(snapshot),
			); err != nil {
				return nil, err
			}
			changed = true
			continue
		}
		if _, err := s.store.EnsurePlatformMachineForClusterNode(
			nodeName,
			bootstrapControlPlaneMachineEndpoint(snapshot),
			snapshot.labels,
			bootstrapControlPlaneMachineName(snapshot),
			bootstrapControlPlaneMachineFingerprint(snapshot),
		); err != nil {
			return nil, err
		}
		changed = true
	}
	if !changed {
		return machines, nil
	}
	return s.store.ListMachines("", true)
}

func clusterNodeSnapshotMatchesFingerprint(snapshot clusterNodeSnapshot, machineFingerprint string) bool {
	machineFingerprint = strings.TrimSpace(machineFingerprint)
	if machineFingerprint == "" {
		return false
	}
	return strings.EqualFold(snapshot.identity.machineID, machineFingerprint) ||
		strings.EqualFold(snapshot.identity.systemUUID, machineFingerprint)
}

func clusterNodeSnapshotManaged(snapshot clusterNodeSnapshot) bool {
	if snapshot.managedOwned {
		return true
	}
	return firstNodeLabel(
		snapshot.labels,
		runtime.MachineIDLabelKey,
		runtime.NodeKeyIDLabelKey,
		runtime.MachineScopeLabelKey,
	) != ""
}

func clusterNodeSnapshotIdentityKey(snapshot clusterNodeSnapshot) string {
	if !clusterNodeSnapshotManaged(snapshot) {
		return ""
	}
	if value := strings.ToLower(strings.TrimSpace(snapshot.identity.machineID)); value != "" {
		return "machine:" + value
	}
	if value := strings.ToLower(strings.TrimSpace(snapshot.identity.systemUUID)); value != "" {
		return "uuid:" + value
	}
	if value := strings.ToLower(strings.TrimSpace(snapshot.identity.hostname)); value != "" {
		return "host:" + value
	}
	return ""
}

func collapseClusterNodeSnapshots(snapshots []resolvedClusterNodeSnapshot, runtimeByClusterNode map[string]model.Runtime) []resolvedClusterNodeSnapshot {
	if len(snapshots) < 2 {
		return snapshots
	}

	collapsed := make([]resolvedClusterNodeSnapshot, 0, len(snapshots))
	grouped := make(map[string][]resolvedClusterNodeSnapshot)
	for _, snapshot := range snapshots {
		key := clusterNodeSnapshotIdentityKey(snapshot.snapshot)
		if key == "" {
			collapsed = append(collapsed, snapshot)
			continue
		}
		grouped[key] = append(grouped[key], snapshot)
	}

	for _, group := range grouped {
		if len(group) < 2 {
			collapsed = append(collapsed, group[0])
			continue
		}

		preferredIndex := 0
		for idx := 1; idx < len(group); idx++ {
			if preferClusterNodeSnapshot(group[idx], group[preferredIndex], runtimeByClusterNode) {
				preferredIndex = idx
			}
		}

		canCollapse := true
		for idx, snapshot := range group {
			if idx == preferredIndex {
				continue
			}
			if len(snapshot.workloads) > 0 {
				canCollapse = false
				break
			}
		}
		if canCollapse {
			collapsed = append(collapsed, group[preferredIndex])
			continue
		}
		collapsed = append(collapsed, group...)
	}

	return collapsed
}

func preferClusterNodeSnapshot(left, right resolvedClusterNodeSnapshot, runtimeByClusterNode map[string]model.Runtime) bool {
	leftStatusRank := clusterNodeStatusRank(left.snapshot.node.Status)
	rightStatusRank := clusterNodeStatusRank(right.snapshot.node.Status)
	if leftStatusRank != rightStatusRank {
		return leftStatusRank > rightStatusRank
	}

	leftUpdatedAt := clusterNodeSnapshotUpdatedAt(left.snapshot, runtimeByClusterNode)
	rightUpdatedAt := clusterNodeSnapshotUpdatedAt(right.snapshot, runtimeByClusterNode)
	if !leftUpdatedAt.Equal(rightUpdatedAt) {
		return leftUpdatedAt.After(rightUpdatedAt)
	}

	leftCreatedAt := clusterNodeSnapshotCreatedAt(left.snapshot)
	rightCreatedAt := clusterNodeSnapshotCreatedAt(right.snapshot)
	if !leftCreatedAt.Equal(rightCreatedAt) {
		return leftCreatedAt.After(rightCreatedAt)
	}

	return left.snapshot.node.Name > right.snapshot.node.Name
}

func clusterNodeSnapshotUpdatedAt(snapshot clusterNodeSnapshot, runtimeByClusterNode map[string]model.Runtime) time.Time {
	if runtimeObj, ok := runtimeByClusterNode[snapshot.node.Name]; ok {
		return runtimeObj.UpdatedAt
	}
	return clusterNodeSnapshotCreatedAt(snapshot)
}

func clusterNodeSnapshotCreatedAt(snapshot clusterNodeSnapshot) time.Time {
	if snapshot.node.CreatedAt != nil {
		return *snapshot.node.CreatedAt
	}
	return time.Time{}
}

func clusterNodeStatusRank(status string) int {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "ready":
		return 3
	case "not-ready":
		return 2
	case "unknown":
		return 1
	default:
		return 0
	}
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

func clusterJoinBootstrapSecretName(tokenID string) string {
	tokenID = strings.ToLower(strings.TrimSpace(tokenID))
	if tokenID == "" {
		return clusterJoinTokenSecretPrefix
	}
	return clusterJoinTokenSecretPrefix + tokenID
}

func clusterJoinBootstrapTokenIDFromSecretName(secretName string) string {
	secretName = strings.TrimSpace(secretName)
	if !strings.HasPrefix(secretName, clusterJoinTokenSecretPrefix) {
		return ""
	}
	return strings.TrimPrefix(secretName, clusterJoinTokenSecretPrefix)
}

func normalizeClusterJoinCAHash(raw string) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	raw = strings.TrimPrefix(raw, "sha256:")
	return raw
}

func (c *clusterNodeClient) doJSON(ctx context.Context, method, apiPath string, out any) error {
	return c.doJSONWithBody(ctx, method, apiPath, nil, out)
}

func (c *clusterNodeClient) doJSONWithBody(ctx context.Context, method, apiPath string, in, out any) error {
	var bodyReader io.Reader
	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("encode kubernetes request body: %w", err)
		}
		bodyReader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+apiPath, bodyReader)
	if err != nil {
		return fmt.Errorf("create kubernetes request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.bearerToken)
	req.Header.Set("Accept", "application/json")
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}

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
	if cnpgClusterName := strings.TrimSpace(labels[clusterNodeCNPGClusterLabel]); cnpgClusterName != "" {
		key := clusterNamespacedResourceKey(strings.TrimSpace(pod.Metadata.Namespace), cnpgClusterName)
		if service, ok := r.servicesByNamespacedApp[key]; ok {
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

func normalizeCountryDisplayName(value string) string {
	switch strings.TrimSpace(value) {
	case "":
		return ""
	case "Hong Kong SAR China":
		return "Hong Kong"
	case "Macao SAR China":
		return "Macao"
	default:
		return strings.TrimSpace(value)
	}
}

func kubeNodeRegion(labels, annotations map[string]string) string {
	if value := firstNodeLabel(labels, clusterNodeLabelRegion, clusterNodeLabelLegacyRegion, "region"); value != "" {
		return value
	}
	if value := firstNodeAnnotation(annotations, clusterNodeAnnotationCountry); value != "" {
		return normalizeCountryDisplayName(value)
	}
	if value := firstNodeLabel(labels, clusterNodeLabelCountryCode); value != "" {
		return strings.ToUpper(value)
	}
	return ""
}

func kubeNodeCountryCode(labels map[string]string) string {
	if value := firstNodeLabel(labels, clusterNodeLabelCountryCode); value != "" {
		return strings.ToLower(value)
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

func cloneNodeLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(labels))
	for key, value := range labels {
		cloned[key] = value
	}
	return cloned
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
