package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func TestCreateNodeKeyAllowsEmptyBodyForTenantKey(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Empty Body Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, secret, err := s.CreateAPIKey(tenant.ID, "tenant-admin", []string{"runtime.attach"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	req := httptest.NewRequest(http.MethodPost, "/v1/node-keys", nil)
	req.Header.Set("Authorization", "Bearer "+secret)
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusCreated, recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	if !strings.Contains(body, `"label":"default"`) {
		t.Fatalf("expected default label in response body, got %s", body)
	}
	if !strings.Contains(body, `"secret":"fugue_nk_`) {
		t.Fatalf("expected node key secret in response body, got %s", body)
	}
}

func TestJoinClusterEnvIncludesMeshConfig(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Join Cluster Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, nodeSecret, err := s.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{
		ClusterJoinServer:           "https://100.64.0.1:6443",
		ClusterJoinToken:            "cluster-token",
		RegistryPullBase:            "10.128.0.2:30500",
		ClusterJoinRegistryEndpoint: "100.64.0.1:30500",
		ClusterJoinMeshProvider:     "tailscale",
		ClusterJoinMeshLoginServer:  "https://mesh.fugue.pro",
		ClusterJoinMeshAuthKey:      "tskey-example",
	})

	form := url.Values{}
	form.Set("node_key", nodeSecret)
	form.Set("node_name", "alicehk2")
	req := httptest.NewRequest(http.MethodPost, "/v1/nodes/join-cluster/env", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "FUGUE_JOIN_SERVER='https://100.64.0.1:6443'") {
		t.Fatalf("expected join server in response body, got %s", body)
	}
	if !strings.Contains(body, "FUGUE_JOIN_MESH_PROVIDER='tailscale'") {
		t.Fatalf("expected mesh provider in response body, got %s", body)
	}
	if !strings.Contains(body, "FUGUE_JOIN_REGISTRY_BASE='10.128.0.2:30500'") {
		t.Fatalf("expected registry base in response body, got %s", body)
	}
	if !strings.Contains(body, "FUGUE_JOIN_REGISTRY_ENDPOINT='100.64.0.1:30500'") {
		t.Fatalf("expected registry endpoint in response body, got %s", body)
	}
	if !strings.Contains(body, "FUGUE_JOIN_MESH_LOGIN_SERVER='https://mesh.fugue.pro'") {
		t.Fatalf("expected mesh login server in response body, got %s", body)
	}
	if !strings.Contains(body, "FUGUE_JOIN_MESH_AUTH_KEY='tskey-example'") {
		t.Fatalf("expected mesh auth key in response body, got %s", body)
	}
}

func TestJoinClusterInstallScriptAddsTopologyLabels(t *testing.T) {
	t.Parallel()

	var server Server
	script := server.joinClusterInstallScript("https://api.fugue.pro")

	for _, want := range []string{
		`FUGUE_NODE_PUBLIC_IP`,
		`FUGUE_NODE_REGION`,
		`FUGUE_NODE_ZONE`,
		`FUGUE_NODE_COUNTRY_CODE`,
		`https://ipapi.co/json/`,
		`fugue.io/public-ip`,
		`fugue.io/location-country-code`,
		`topology.kubernetes.io/region`,
		`topology.kubernetes.io/zone`,
		`FUGUE_JOIN_NODE_LABELS="$(append_location_node_labels)"`,
		`/v1/nodes/join-cluster/cleanup`,
		`--data-urlencode "machine_fingerprint=${machine_fingerprint}"`,
		`--data-urlencode "current_node_name=${FUGUE_JOIN_NODE_NAME}"`,
		`cleanup_stale_cluster_nodes`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("expected join-cluster install script to contain %q", want)
		}
	}
}

func TestJoinClusterCleanupRemovesStaleSameMachineNode(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Join Cluster Cleanup Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, nodeSecret, err := s.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}

	_, staleRuntime, err := s.BootstrapClusterNode(nodeSecret, "alicehk2", "https://alicehk2.example.com", nil, "alicehk2", "stale-store-fingerprint")
	if err != nil {
		t.Fatalf("bootstrap stale cluster node: %v", err)
	}
	_, currentRuntime, err := s.BootstrapClusterNode(nodeSecret, "fortedrape8", "https://fortedrape8.example.com", nil, "fortedrape8", "machine-123")
	if err != nil {
		t.Fatalf("bootstrap current cluster node: %v", err)
	}

	deletedNodes := make([]string, 0, 1)
	kubeServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/api/v1/nodes":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"metadata": map[string]any{
							"name": "alicehk2",
							"labels": map[string]string{
								runtime.NodeModeLabelKey:  model.RuntimeTypeManagedOwned,
								runtime.RuntimeIDLabelKey: staleRuntime.ID,
							},
						},
						"status": map[string]any{
							"conditions": []map[string]string{
								{"type": "Ready", "status": "False"},
							},
							"nodeInfo": map[string]string{
								"machineID": "machine-123",
							},
						},
					},
					{
						"metadata": map[string]any{
							"name": "fortedrape8",
							"labels": map[string]string{
								runtime.NodeModeLabelKey:  model.RuntimeTypeManagedOwned,
								runtime.RuntimeIDLabelKey: currentRuntime.ID,
							},
						},
						"status": map[string]any{
							"conditions": []map[string]string{
								{"type": "Ready", "status": "True"},
							},
							"nodeInfo": map[string]string{
								"machineID": "machine-123",
							},
						},
					},
				},
			})
		case "/api/v1/pods":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}})
		case "/api/v1/nodes/alicehk2/proxy/stats/summary", "/api/v1/nodes/fortedrape8/proxy/stats/summary":
			_ = json.NewEncoder(w).Encode(map[string]any{"node": map[string]any{}})
		case "/api/v1/nodes/alicehk2":
			if r.Method != http.MethodDelete {
				http.NotFound(w, r)
				return
			}
			deletedNodes = append(deletedNodes, "alicehk2")
			_ = json.NewEncoder(w).Encode(map[string]any{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	server := &Server{
		store:                       s,
		clusterJoinServer:           "https://100.64.0.1:6443",
		clusterJoinToken:            "cluster-token",
		registryPullBase:            "10.128.0.2:30500",
		clusterJoinRegistryEndpoint: "100.64.0.1:30500",
	}
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{
			client:      kubeServer.Client(),
			baseURL:     kubeServer.URL,
			bearerToken: "test-token",
		}, nil
	}

	form := url.Values{}
	form.Set("node_key", nodeSecret)
	form.Set("machine_fingerprint", "machine-123")
	form.Set("current_node_name", "fortedrape8")
	req := httptest.NewRequest(http.MethodPost, "/v1/nodes/join-cluster/cleanup", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	recorder := httptest.NewRecorder()

	server.handleJoinClusterCleanup(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "FUGUE_JOIN_CLEANUP_NODE_COUNT='1'") {
		t.Fatalf("expected cleanup node count in response body, got %s", body)
	}
	if !strings.Contains(body, "FUGUE_JOIN_CLEANUP_NODES='alicehk2'") {
		t.Fatalf("expected cleanup node list in response body, got %s", body)
	}
	if !strings.Contains(body, "FUGUE_JOIN_CLEANUP_RUNTIME_IDS='"+staleRuntime.ID+"'") {
		t.Fatalf("expected cleanup runtime list in response body, got %s", body)
	}
	if !slices.Equal(deletedNodes, []string{"alicehk2"}) {
		t.Fatalf("expected kube delete for alicehk2, got %+v", deletedNodes)
	}

	staleRuntime, err = s.GetRuntime(staleRuntime.ID)
	if err != nil {
		t.Fatalf("get stale runtime after cleanup: %v", err)
	}
	if staleRuntime.Status != model.RuntimeStatusOffline {
		t.Fatalf("expected stale runtime offline, got %q", staleRuntime.Status)
	}
	if staleRuntime.NodeKeyID != "" {
		t.Fatalf("expected stale runtime node key cleared, got %q", staleRuntime.NodeKeyID)
	}
	if staleRuntime.ClusterNodeName != "" {
		t.Fatalf("expected stale runtime cluster node name cleared, got %q", staleRuntime.ClusterNodeName)
	}
	if staleRuntime.FingerprintHash != "" || staleRuntime.FingerprintPrefix != "" {
		t.Fatalf("expected stale runtime fingerprint cleared, got prefix=%q hash=%q", staleRuntime.FingerprintPrefix, staleRuntime.FingerprintHash)
	}

	currentRuntime, err = s.GetRuntime(currentRuntime.ID)
	if err != nil {
		t.Fatalf("get current runtime after cleanup: %v", err)
	}
	if currentRuntime.Status != model.RuntimeStatusActive {
		t.Fatalf("expected current runtime active, got %q", currentRuntime.Status)
	}
	if currentRuntime.ClusterNodeName == "" {
		t.Fatal("expected current runtime to remain attached to its cluster node")
	}
}

func TestNodesEndpointIsDeprecatedCompatibilityView(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Nodes Compatibility Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, nodeSecret, err := s.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	if _, _, _, err := s.BootstrapNode(nodeSecret, "worker", "https://a.example.com", nil, "worker", "fingerprint-a"); err != nil {
		t.Fatalf("bootstrap node: %v", err)
	}
	_, apiSecret, err := s.CreateAPIKey(tenant.ID, "viewer", []string{"project.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	req := httptest.NewRequest(http.MethodGet, "/v1/nodes", nil)
	req.Header.Set("Authorization", "Bearer "+apiSecret)
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	if recorder.Header().Get("Deprecation") != "true" {
		t.Fatalf("expected deprecation header, got %q", recorder.Header().Get("Deprecation"))
	}
	if !strings.Contains(recorder.Header().Get("Warning"), "/v1/runtimes and /v1/cluster/nodes") {
		t.Fatalf("expected warning header, got %q", recorder.Header().Get("Warning"))
	}
}

func TestNodeKeyUsagesEndpointReturnsRuntimes(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Node Key Usages Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	key, nodeSecret, err := s.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	if _, _, _, err := s.BootstrapNode(nodeSecret, "worker", "https://a.example.com", nil, "worker", "fingerprint-a"); err != nil {
		t.Fatalf("bootstrap node: %v", err)
	}
	_, apiSecret, err := s.CreateAPIKey(tenant.ID, "viewer", []string{"project.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	req := httptest.NewRequest(http.MethodGet, "/v1/node-keys/"+key.ID+"/usages", nil)
	req.Header.Set("Authorization", "Bearer "+apiSecret)
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	if !strings.Contains(body, `"usage_count":1`) {
		t.Fatalf("expected usage_count in response body, got %s", body)
	}
	if !strings.Contains(body, `"runtimes":[`) {
		t.Fatalf("expected runtimes list in response body, got %s", body)
	}
}
