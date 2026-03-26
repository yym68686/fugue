package api

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/auth"
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
		`FUGUE_NODE_REGION`,
		`FUGUE_NODE_ZONE`,
		`metadata.google.internal/computeMetadata/v1/instance/zone`,
		`topology.kubernetes.io/region`,
		`topology.kubernetes.io/zone`,
		`FUGUE_JOIN_NODE_LABELS="$(append_topology_node_labels)"`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("expected join-cluster install script to contain %q", want)
		}
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
