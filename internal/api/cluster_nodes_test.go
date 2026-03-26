package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func TestListClusterNodesIncludesMetricsConditionsAndWorkloads(t *testing.T) {
	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Cluster Nodes Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	_, nodeSecret, err := s.CreateNodeKey(tenant.ID, "cluster-node")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	_, runtimeObj, err := s.BootstrapClusterNode(nodeSecret, "worker-1", "https://worker-1.example.com", map[string]string{"zone": "test-a"}, "", "")
	if err != nil {
		t.Fatalf("bootstrap cluster node: %v", err)
	}
	_, apiKey, err := s.CreateAPIKey(tenant.ID, "viewer", []string{"project.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: runtimeObj.ID,
		Postgres: &model.AppPostgresSpec{
			Database: "demo",
			User:     "demo",
			Password: "secret",
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	app, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app: %v", err)
	}
	if len(app.BackingServices) != 1 {
		t.Fatalf("expected one backing service, got %d", len(app.BackingServices))
	}
	service := app.BackingServices[0]
	namespace := runtime.NamespaceForTenant(tenant.ID)

	kubeServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/api/v1/nodes":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"metadata": map[string]any{
							"name":              "worker-1",
							"creationTimestamp": "2026-03-25T00:00:00Z",
							"labels": map[string]string{
								"node-role.kubernetes.io/worker": "",
								"topology.kubernetes.io/region":  "ap-southeast-1",
								"topology.kubernetes.io/zone":    "ap-southeast-1a",
							},
						},
						"status": map[string]any{
							"addresses": []map[string]string{
								{"type": "InternalIP", "address": "10.0.0.10"},
								{"type": "ExternalIP", "address": "203.0.113.10"},
							},
							"conditions": []map[string]string{
								{
									"type":               "Ready",
									"status":             "True",
									"reason":             "KubeletReady",
									"message":            "kubelet is posting ready status",
									"lastTransitionTime": "2026-03-25T00:01:00Z",
								},
								{
									"type":               "MemoryPressure",
									"status":             "False",
									"reason":             "KubeletHasSufficientMemory",
									"message":            "kubelet has sufficient memory available",
									"lastTransitionTime": "2026-03-25T00:01:00Z",
								},
								{
									"type":               "DiskPressure",
									"status":             "False",
									"reason":             "KubeletHasNoDiskPressure",
									"message":            "kubelet has no disk pressure",
									"lastTransitionTime": "2026-03-25T00:01:00Z",
								},
								{
									"type":               "PIDPressure",
									"status":             "True",
									"reason":             "KubeletHasInsufficientPID",
									"message":            "kubelet has insufficient PID available",
									"lastTransitionTime": "2026-03-25T00:01:00Z",
								},
							},
							"capacity": map[string]string{
								"cpu":               "4",
								"memory":            "16Gi",
								"ephemeral-storage": "200Gi",
							},
							"allocatable": map[string]string{
								"cpu":               "3900m",
								"memory":            "15Gi",
								"ephemeral-storage": "180Gi",
							},
							"nodeInfo": map[string]string{
								"kubeletVersion":          "v1.32.2",
								"osImage":                 "Ubuntu 24.04.1 LTS",
								"kernelVersion":           "6.8.0",
								"containerRuntimeVersion": "containerd://2.0.0",
							},
						},
					},
				},
			})
		case "/api/v1/pods":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"metadata": map[string]any{
							"name":      "demo-7b95d6b54f-z2f4g",
							"namespace": namespace,
							"labels": map[string]string{
								"app.kubernetes.io/name":       "demo",
								"app.kubernetes.io/managed-by": "fugue",
							},
						},
						"spec": map[string]any{
							"nodeName": "worker-1",
						},
						"status": map[string]any{
							"phase": "Running",
						},
					},
					{
						"metadata": map[string]any{
							"name":      "demo-postgres-65b74ff98f-9hf6x",
							"namespace": namespace,
							"labels": map[string]string{
								"app.kubernetes.io/name":       service.Spec.Postgres.ServiceName,
								"app.kubernetes.io/managed-by": "fugue",
								"app.kubernetes.io/component":  "postgres",
							},
						},
						"spec": map[string]any{
							"nodeName": "worker-1",
						},
						"status": map[string]any{
							"phase": "Running",
						},
					},
				},
			})
		case "/api/v1/nodes/worker-1/proxy/stats/summary":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"node": map[string]any{
					"nodeName": "worker-1",
					"cpu": map[string]any{
						"usageNanoCores": 1_750_000_000,
					},
					"memory": map[string]any{
						"workingSetBytes": 8 * 1024 * 1024 * 1024,
					},
					"fs": map[string]any{
						"capacityBytes": 200 * 1024 * 1024 * 1024,
						"usedBytes":     50 * 1024 * 1024 * 1024,
					},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{
			client:      kubeServer.Client(),
			baseURL:     kubeServer.URL,
			bearerToken: "test-token",
		}, nil
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/cluster/nodes", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response struct {
		ClusterNodes []model.ClusterNode `json:"cluster_nodes"`
	}
	mustDecodeJSON(t, recorder, &response)

	if len(response.ClusterNodes) != 1 {
		t.Fatalf("expected one cluster node, got %d", len(response.ClusterNodes))
	}
	node := response.ClusterNodes[0]

	if node.Name != "worker-1" {
		t.Fatalf("expected worker-1, got %q", node.Name)
	}
	if node.Status != "ready" {
		t.Fatalf("expected ready status, got %q", node.Status)
	}
	if node.Region != "ap-southeast-1" {
		t.Fatalf("expected region ap-southeast-1, got %q", node.Region)
	}
	if node.Zone != "ap-southeast-1a" {
		t.Fatalf("expected zone ap-southeast-1a, got %q", node.Zone)
	}
	if node.PublicIP != "203.0.113.10" {
		t.Fatalf("expected public ip 203.0.113.10, got %q", node.PublicIP)
	}
	if node.RuntimeID != runtimeObj.ID {
		t.Fatalf("expected runtime id %q, got %q", runtimeObj.ID, node.RuntimeID)
	}
	if node.TenantID != tenant.ID {
		t.Fatalf("expected tenant id %q, got %q", tenant.ID, node.TenantID)
	}

	if got := node.Conditions["MemoryPressure"].Status; got != "false" {
		t.Fatalf("expected MemoryPressure=false, got %q", got)
	}
	if got := node.Conditions["PIDPressure"].Status; got != "true" {
		t.Fatalf("expected PIDPressure=true, got %q", got)
	}

	if node.CPU == nil || node.CPU.CapacityMilliCores == nil || *node.CPU.CapacityMilliCores != 4000 {
		t.Fatalf("expected cpu capacity 4000m, got %#v", node.CPU)
	}
	if node.CPU.AllocatableMilliCores == nil || *node.CPU.AllocatableMilliCores != 3900 {
		t.Fatalf("expected cpu allocatable 3900m, got %#v", node.CPU)
	}
	if node.CPU.UsedMilliCores == nil || *node.CPU.UsedMilliCores != 1750 {
		t.Fatalf("expected cpu used 1750m, got %#v", node.CPU)
	}

	memoryCapacity := int64(16 * 1024 * 1024 * 1024)
	memoryUsed := int64(8 * 1024 * 1024 * 1024)
	if node.Memory == nil || node.Memory.CapacityBytes == nil || *node.Memory.CapacityBytes != memoryCapacity {
		t.Fatalf("expected memory capacity %d, got %#v", memoryCapacity, node.Memory)
	}
	if node.Memory.UsedBytes == nil || *node.Memory.UsedBytes != memoryUsed {
		t.Fatalf("expected memory used %d, got %#v", memoryUsed, node.Memory)
	}

	storageCapacity := int64(200 * 1024 * 1024 * 1024)
	storageUsed := int64(50 * 1024 * 1024 * 1024)
	if node.EphemeralStorage == nil || node.EphemeralStorage.CapacityBytes == nil || *node.EphemeralStorage.CapacityBytes != storageCapacity {
		t.Fatalf("expected storage capacity %d, got %#v", storageCapacity, node.EphemeralStorage)
	}
	if node.EphemeralStorage.UsedBytes == nil || *node.EphemeralStorage.UsedBytes != storageUsed {
		t.Fatalf("expected storage used %d, got %#v", storageUsed, node.EphemeralStorage)
	}

	if len(node.Workloads) != 2 {
		t.Fatalf("expected two workloads, got %#v", node.Workloads)
	}

	appWorkload := node.Workloads[0]
	if appWorkload.Kind != model.ClusterNodeWorkloadKindApp {
		t.Fatalf("expected first workload kind app, got %#v", appWorkload)
	}
	if appWorkload.ID != app.ID {
		t.Fatalf("expected app workload id %q, got %q", app.ID, appWorkload.ID)
	}
	if appWorkload.PodCount != 1 || len(appWorkload.Pods) != 1 {
		t.Fatalf("expected one app pod, got %#v", appWorkload)
	}

	serviceWorkload := node.Workloads[1]
	if serviceWorkload.Kind != model.ClusterNodeWorkloadKindBackingService {
		t.Fatalf("expected second workload kind backing_service, got %#v", serviceWorkload)
	}
	if serviceWorkload.ID != service.ID {
		t.Fatalf("expected service workload id %q, got %q", service.ID, serviceWorkload.ID)
	}
	if serviceWorkload.PodCount != 1 || len(serviceWorkload.Pods) != 1 {
		t.Fatalf("expected one backing service pod, got %#v", serviceWorkload)
	}
}

func TestListClusterNodesIncludesSharedNodesHostingTenantWorkloads(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Shared Cluster Nodes Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	otherTenant, err := s.CreateTenant("Other Shared Cluster Nodes Tenant")
	if err != nil {
		t.Fatalf("create other tenant: %v", err)
	}

	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	otherProject, err := s.CreateProject(otherTenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create other project: %v", err)
	}

	_, apiKey, err := s.CreateAPIKey(tenant.ID, "viewer", []string{"project.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Postgres: &model.AppPostgresSpec{
			Database: "demo",
			User:     "demo",
			Password: "secret",
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	app, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app: %v", err)
	}
	if len(app.BackingServices) != 1 {
		t.Fatalf("expected one backing service, got %d", len(app.BackingServices))
	}
	service := app.BackingServices[0]

	otherApp, err := s.CreateApp(otherTenant.ID, otherProject.ID, "other-demo", "", model.AppSpec{
		Image:     "ghcr.io/example/other-demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatalf("create other app: %v", err)
	}

	namespace := runtime.NamespaceForTenant(tenant.ID)
	otherNamespace := runtime.NamespaceForTenant(otherTenant.ID)

	kubeServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/api/v1/nodes":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"metadata": map[string]any{
							"name":              "shared-tenant-node",
							"creationTimestamp": "2026-03-25T00:00:00Z",
							"labels": map[string]string{
								clusterNodeLabelCountryCode: "jp",
							},
							"annotations": map[string]string{
								clusterNodeAnnotationCountry: "Japan",
							},
						},
						"status": map[string]any{
							"conditions": []map[string]string{
								{
									"type":   "Ready",
									"status": "True",
								},
							},
						},
					},
					{
						"metadata": map[string]any{
							"name":              "shared-other-node",
							"creationTimestamp": "2026-03-25T00:00:01Z",
							"labels":            map[string]string{},
						},
						"status": map[string]any{
							"conditions": []map[string]string{
								{
									"type":   "Ready",
									"status": "True",
								},
							},
						},
					},
					{
						"metadata": map[string]any{
							"name":              "empty-node",
							"creationTimestamp": "2026-03-25T00:00:02Z",
							"labels":            map[string]string{},
						},
						"status": map[string]any{
							"conditions": []map[string]string{
								{
									"type":   "Ready",
									"status": "True",
								},
							},
						},
					},
				},
			})
		case "/api/v1/pods":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"metadata": map[string]any{
							"name":      "demo-7b95d6b54f-z2f4g",
							"namespace": namespace,
							"labels": map[string]string{
								runtime.FugueLabelName:      "demo",
								runtime.FugueLabelManagedBy: runtime.FugueLabelManagedByValue,
								runtime.FugueLabelAppID:     app.ID,
							},
						},
						"spec": map[string]any{
							"nodeName": "shared-tenant-node",
						},
						"status": map[string]any{
							"phase": "Running",
						},
					},
					{
						"metadata": map[string]any{
							"name":      "demo-postgres-65b74ff98f-9hf6x",
							"namespace": namespace,
							"labels": map[string]string{
								runtime.FugueLabelName:             service.Spec.Postgres.ServiceName,
								runtime.FugueLabelManagedBy:        runtime.FugueLabelManagedByValue,
								runtime.FugueLabelComponent:        "postgres",
								runtime.FugueLabelBackingServiceID: service.ID,
							},
						},
						"spec": map[string]any{
							"nodeName": "shared-tenant-node",
						},
						"status": map[string]any{
							"phase": "Running",
						},
					},
					{
						"metadata": map[string]any{
							"name":      "other-demo-7b95d6b54f-z2f4g",
							"namespace": otherNamespace,
							"labels": map[string]string{
								runtime.FugueLabelName:      "other-demo",
								runtime.FugueLabelManagedBy: runtime.FugueLabelManagedByValue,
								runtime.FugueLabelAppID:     otherApp.ID,
							},
						},
						"spec": map[string]any{
							"nodeName": "shared-other-node",
						},
						"status": map[string]any{
							"phase": "Running",
						},
					},
				},
			})
		default:
			if strings.HasPrefix(r.URL.Path, "/api/v1/nodes/") && strings.HasSuffix(r.URL.Path, "/proxy/stats/summary") {
				_ = json.NewEncoder(w).Encode(map[string]any{})
				return
			}
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{
			client:      kubeServer.Client(),
			baseURL:     kubeServer.URL,
			bearerToken: "test-token",
		}, nil
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/cluster/nodes", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response struct {
		ClusterNodes []model.ClusterNode `json:"cluster_nodes"`
	}
	mustDecodeJSON(t, recorder, &response)

	if len(response.ClusterNodes) != 1 {
		t.Fatalf("expected one visible shared cluster node, got %#v", response.ClusterNodes)
	}

	node := response.ClusterNodes[0]
	if node.Name != "shared-tenant-node" {
		t.Fatalf("expected shared-tenant-node, got %q", node.Name)
	}
	if node.Region != "Japan" {
		t.Fatalf("expected country-backed region label Japan, got %q", node.Region)
	}
	if node.RuntimeID != "" {
		t.Fatalf("expected shared node runtime id to stay empty, got %q", node.RuntimeID)
	}
	if node.TenantID != "" {
		t.Fatalf("expected shared node tenant id to stay empty, got %q", node.TenantID)
	}
	if len(node.Workloads) != 2 {
		t.Fatalf("expected two visible workloads on shared node, got %#v", node.Workloads)
	}

	if node.Workloads[0].Kind != model.ClusterNodeWorkloadKindApp || node.Workloads[0].ID != app.ID {
		t.Fatalf("expected first workload to be tenant app, got %#v", node.Workloads[0])
	}
	if node.Workloads[0].RuntimeID != "runtime_managed_shared" {
		t.Fatalf("expected app workload runtime id runtime_managed_shared, got %q", node.Workloads[0].RuntimeID)
	}
	if node.Workloads[1].Kind != model.ClusterNodeWorkloadKindBackingService || node.Workloads[1].ID != service.ID {
		t.Fatalf("expected second workload to be tenant backing service, got %#v", node.Workloads[1])
	}
}

func TestResolveClusterNodePublicIPFallsBackToRuntimeEndpoint(t *testing.T) {
	t.Parallel()

	node := model.ClusterNode{
		Name:       "worker-1",
		InternalIP: "10.0.0.10",
		ExternalIP: "100.64.0.10",
	}
	runtimeObj := model.Runtime{
		Endpoint: "https://203.0.113.20",
	}

	if got := resolveClusterNodePublicIP(node, &runtimeObj); got != "203.0.113.20" {
		t.Fatalf("expected runtime endpoint public ip, got %q", got)
	}
}

func TestKubeNodePublicIPPrefersExplicitLabel(t *testing.T) {
	t.Parallel()

	node := kubeNode{}
	node.Metadata.Labels = map[string]string{
		clusterNodeLabelPublicIP: "198.51.100.12",
	}
	node.Status.Addresses = []struct {
		Type    string `json:"type"`
		Address string `json:"address"`
	}{
		{Type: "ExternalIP", Address: "100.64.0.10"},
	}

	if got := kubeNodePublicIP(node); got != "198.51.100.12" {
		t.Fatalf("expected labeled public ip, got %q", got)
	}
}

func TestKubeNodeRegionFallbacksToGeolocatedCountry(t *testing.T) {
	t.Parallel()

	if got := kubeNodeRegion(
		map[string]string{clusterNodeLabelCountryCode: "us"},
		map[string]string{clusterNodeAnnotationCountry: "United States"},
	); got != "United States" {
		t.Fatalf("expected annotation-backed country name, got %q", got)
	}

	if got := kubeNodeRegion(
		map[string]string{clusterNodeLabelRegion: "us-central1"},
		map[string]string{clusterNodeAnnotationCountry: "United States"},
	); got != "us-central1" {
		t.Fatalf("expected explicit topology region to win, got %q", got)
	}

	if got := kubeNodeRegion(
		map[string]string{clusterNodeLabelCountryCode: "de"},
		nil,
	); got != "DE" {
		t.Fatalf("expected country code fallback, got %q", got)
	}
}

func TestBuildClusterNodeStorageStatsReconcilesStaleNodeCapacity(t *testing.T) {
	summaryCapacity := uint64(31_461_457_920)
	summaryUsed := uint64(11_341_619_200)

	node := kubeNode{}
	node.Status.Capacity = map[string]string{
		"ephemeral-storage": "10088732Ki",
	}
	node.Status.Allocatable = map[string]string{
		"ephemeral-storage": "9814318482",
	}

	stats := buildClusterNodeStorageStats(node, &kubeNodeSummary{
		Node: kubeNodeSummaryNode{
			FS: kubeNodeSummaryFS{
				CapacityBytes: &summaryCapacity,
				UsedBytes:     &summaryUsed,
			},
		},
	})
	if stats == nil {
		t.Fatal("expected storage stats")
	}

	const wantCapacity = int64(31_461_457_920)
	const wantAllocatable = int64(29_888_385_001)
	const wantUsed = int64(11_341_619_200)
	const wantPercent = 37.9

	if stats.CapacityBytes == nil || *stats.CapacityBytes != wantCapacity {
		t.Fatalf("expected reconciled storage capacity %d, got %#v", wantCapacity, stats)
	}
	if stats.AllocatableBytes == nil || *stats.AllocatableBytes != wantAllocatable {
		t.Fatalf("expected reconciled storage allocatable %d, got %#v", wantAllocatable, stats)
	}
	if stats.UsedBytes == nil || *stats.UsedBytes != wantUsed {
		t.Fatalf("expected storage used %d, got %#v", wantUsed, stats)
	}
	if stats.UsagePercent == nil || *stats.UsagePercent != wantPercent {
		t.Fatalf("expected storage usage percent %.1f, got %#v", wantPercent, stats)
	}
	if *stats.UsedBytes > *stats.CapacityBytes {
		t.Fatalf("expected used bytes <= capacity after reconciliation, got %#v", stats)
	}
}
