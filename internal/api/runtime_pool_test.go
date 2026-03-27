package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func TestSetRuntimePoolModeRequiresPlatformAdminAndReconcilesNode(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	owner, err := s.CreateTenant("Shared Pool Owner")
	if err != nil {
		t.Fatalf("create owner tenant: %v", err)
	}
	_, ownerKey, err := s.CreateAPIKey(owner.ID, "owner-runtime-writer", []string{"runtime.write"})
	if err != nil {
		t.Fatalf("create owner runtime writer key: %v", err)
	}
	_, adminKey, err := s.CreateAPIKey(owner.ID, "platform-admin", []string{"platform.admin"})
	if err != nil {
		t.Fatalf("create platform admin key: %v", err)
	}
	_, nodeSecret, err := s.CreateNodeKey(owner.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	_, runtimeObj, err := s.BootstrapClusterNode(nodeSecret, "shared-worker", "https://shared-worker.example.com", nil, "", "")
	if err != nil {
		t.Fatalf("bootstrap cluster node: %v", err)
	}

	var mu sync.Mutex
	nodeLabels := map[string]string{
		runtime.RuntimeIDLabelKey: runtimeObj.ID,
		runtime.TenantIDLabelKey:  owner.ID,
		runtime.NodeModeLabelKey:  model.RuntimeTypeManagedOwned,
	}
	nodeTaints := []kubeNodeTaint{{
		Key:    runtime.TenantTaintKey,
		Value:  owner.ID,
		Effect: "NoSchedule",
	}}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{
			client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				mu.Lock()
				defer mu.Unlock()

				switch {
				case req.Method == http.MethodGet && req.URL.Path == "/api/v1/nodes/"+runtimeObj.ClusterNodeName:
					return jsonHTTPResponse(http.StatusOK, map[string]any{
						"metadata": map[string]any{
							"name":   runtimeObj.ClusterNodeName,
							"labels": nodeLabels,
						},
						"spec": map[string]any{
							"taints": nodeTaints,
						},
					}), nil
				case req.Method == http.MethodPatch && req.URL.Path == "/api/v1/nodes/"+runtimeObj.ClusterNodeName:
					var patch struct {
						Metadata struct {
							Labels map[string]*string `json:"labels"`
						} `json:"metadata"`
						Spec struct {
							Taints []kubeNodeTaint `json:"taints"`
						} `json:"spec"`
					}
					if err := json.NewDecoder(req.Body).Decode(&patch); err != nil {
						return nil, err
					}
					for key, value := range patch.Metadata.Labels {
						if value == nil {
							delete(nodeLabels, key)
							continue
						}
						nodeLabels[key] = *value
					}
					if patch.Spec.Taints != nil {
						nodeTaints = patch.Spec.Taints
					}
					return jsonHTTPResponse(http.StatusOK, map[string]any{}), nil
				default:
					return jsonHTTPResponse(http.StatusNotFound, map[string]any{"error": "not found"}), nil
				}
			})},
			baseURL:     "https://kube.test",
			bearerToken: "test-token",
		}, nil
	}

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/runtimes/"+runtimeObj.ID+"/pool-mode", ownerKey, map[string]any{
		"pool_mode": model.RuntimePoolModeInternalShared,
	})
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected status %d for non-admin pool-mode change, got %d body=%s", http.StatusForbidden, recorder.Code, recorder.Body.String())
	}

	recorder = performJSONRequest(t, server, http.MethodPost, "/v1/runtimes/"+runtimeObj.ID+"/pool-mode", adminKey, map[string]any{
		"pool_mode": model.RuntimePoolModeInternalShared,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d for admin pool-mode change, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response struct {
		Runtime        model.Runtime `json:"runtime"`
		NodeReconciled bool          `json:"node_reconciled"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Runtime.PoolMode != model.RuntimePoolModeInternalShared {
		t.Fatalf("expected runtime pool mode %q, got %q", model.RuntimePoolModeInternalShared, response.Runtime.PoolMode)
	}
	if !response.NodeReconciled {
		t.Fatal("expected node reconciliation to report true")
	}

	mu.Lock()
	defer mu.Unlock()
	if got := nodeLabels[runtime.SharedPoolLabelKey]; got != runtime.SharedPoolLabelValue {
		t.Fatalf("expected node shared-pool label %q, got %q", runtime.SharedPoolLabelValue, got)
	}
	if len(nodeTaints) != 0 {
		t.Fatalf("expected tenant taint to be removed from shared-pool node, got %#v", nodeTaints)
	}
}

func TestListClusterNodesAggregatesContributedSharedPoolNodesForTenant(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	owner, err := s.CreateTenant("Pool Contributor")
	if err != nil {
		t.Fatalf("create owner tenant: %v", err)
	}
	viewer, err := s.CreateTenant("Pool Viewer")
	if err != nil {
		t.Fatalf("create viewer tenant: %v", err)
	}
	project, err := s.CreateProject(viewer.ID, "apps", "")
	if err != nil {
		t.Fatalf("create viewer project: %v", err)
	}
	_, viewerKey, err := s.CreateAPIKey(viewer.ID, "viewer", []string{"project.write"})
	if err != nil {
		t.Fatalf("create viewer key: %v", err)
	}
	_, nodeSecret, err := s.CreateNodeKey(owner.ID, "default")
	if err != nil {
		t.Fatalf("create owner node key: %v", err)
	}
	_, ownerRuntime, err := s.BootstrapClusterNode(nodeSecret, "shared-worker", "https://shared-worker.example.com", nil, "", "")
	if err != nil {
		t.Fatalf("bootstrap owner cluster node: %v", err)
	}
	if _, err := s.SetRuntimePoolMode(ownerRuntime.ID, model.RuntimePoolModeInternalShared); err != nil {
		t.Fatalf("set runtime pool mode: %v", err)
	}

	app, err := s.CreateApp(viewer.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: tenantSharedRuntimeID,
	})
	if err != nil {
		t.Fatalf("create viewer app: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{
			client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				switch {
				case req.Method == http.MethodGet && req.URL.Path == "/api/v1/nodes":
					return jsonHTTPResponse(http.StatusOK, map[string]any{
						"items": []map[string]any{
							{
								"metadata": map[string]any{
									"name":              ownerRuntime.ClusterNodeName,
									"creationTimestamp": "2026-03-26T00:00:00Z",
									"labels": map[string]string{
										runtime.RuntimeIDLabelKey:    ownerRuntime.ID,
										runtime.TenantIDLabelKey:     owner.ID,
										runtime.NodeModeLabelKey:     model.RuntimeTypeManagedOwned,
										runtime.SharedPoolLabelKey:   runtime.SharedPoolLabelValue,
										clusterNodeLabelCountryCode:  "jp",
										clusterNodeLabelRegion:       "ap-northeast-1",
										clusterNodeLabelLegacyRegion: "ap-northeast-1",
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
						},
					}), nil
				case req.Method == http.MethodGet && req.URL.Path == "/api/v1/pods":
					return jsonHTTPResponse(http.StatusOK, map[string]any{
						"items": []map[string]any{
							{
								"metadata": map[string]any{
									"name":      "demo-6d9f9b9d7c-2plqm",
									"namespace": runtime.NamespaceForTenant(viewer.ID),
									"labels": map[string]string{
										"app.kubernetes.io/name":       app.Name,
										"app.kubernetes.io/managed-by": "fugue",
									},
								},
								"spec": map[string]any{
									"nodeName": ownerRuntime.ClusterNodeName,
								},
								"status": map[string]any{
									"phase": "Running",
								},
							},
						},
					}), nil
				case req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/proxy/stats/summary"):
					return jsonHTTPResponse(http.StatusOK, map[string]any{"node": map[string]any{}}), nil
				default:
					return jsonHTTPResponse(http.StatusNotFound, map[string]any{"error": "not found"}), nil
				}
			})},
			baseURL:     "https://kube.test",
			bearerToken: "test-token",
		}, nil
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/cluster/nodes", viewerKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response struct {
		ClusterNodes []model.ClusterNode `json:"cluster_nodes"`
	}
	mustDecodeJSON(t, recorder, &response)
	if len(response.ClusterNodes) != 1 {
		t.Fatalf("expected one aggregated shared cluster node, got %#v", response.ClusterNodes)
	}

	node := response.ClusterNodes[0]
	if node.Name != tenantSharedClusterNodeName {
		t.Fatalf("expected aggregated shared node %q, got %q", tenantSharedClusterNodeName, node.Name)
	}
	if node.RuntimeID != tenantSharedRuntimeID {
		t.Fatalf("expected aggregated node runtime id %q, got %q", tenantSharedRuntimeID, node.RuntimeID)
	}
	if len(node.Workloads) != 1 || node.Workloads[0].ID != app.ID {
		t.Fatalf("expected aggregated shared node to include viewer app workload, got %#v", node.Workloads)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func jsonHTTPResponse(status int, body any) *http.Response {
	raw, _ := json.Marshal(body)
	return &http.Response{
		StatusCode: status,
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Body: io.NopCloser(bytes.NewReader(raw)),
	}
}
