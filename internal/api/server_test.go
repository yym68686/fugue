package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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

func TestReadyzReportsHealthyDependencies(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	kubeServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/apis/apps/v1/namespaces/fugue-system/deployments" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}})
			return
		}
		http.NotFound(w, r)
	}))
	defer kubeServer.Close()

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{
		ControlPlaneNamespace: "fugue-system",
	})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{
			client:      kubeServer.Client(),
			baseURL:     kubeServer.URL,
			bearerToken: "test-token",
		}, nil
	}

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	for _, want := range []string{
		`"status":"ok"`,
		`"store":{"status":"ok"}`,
		`"kubernetes_api":{"status":"ok"}`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("expected readyz response to contain %q, got %s", want, body)
		}
	}
}

func TestReadyzReportsKubernetesDependencyAsDegradedWithoutFailingReadiness(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{
		ControlPlaneNamespace: "fugue-system",
	})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return nil, errors.New("cluster unavailable")
	}

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	for _, want := range []string{
		`"status":"degraded"`,
		`"store":{"status":"ok"}`,
		`"kubernetes_api":{"status":"degraded","message":"cluster unavailable"}`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("expected readyz response to contain %q, got %s", want, body)
		}
	}
}

func TestReadyzCachesKubernetesDependencyDegradation(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{
		ControlPlaneNamespace: "fugue-system",
	})
	var clientFactoryCalls int32
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		atomic.AddInt32(&clientFactoryCalls, 1)
		return nil, errors.New("cluster unavailable")
	}

	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
		recorder := httptest.NewRecorder()

		server.Handler().ServeHTTP(recorder, req)

		if recorder.Code != http.StatusOK {
			t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
		}
		if body := recorder.Body.String(); !strings.Contains(body, `"kubernetes_api":{"status":"degraded","message":"cluster unavailable"}`) {
			t.Fatalf("expected cached degraded kubernetes status, got %s", body)
		}
	}

	if got := atomic.LoadInt32(&clientFactoryCalls); got != 1 {
		t.Fatalf("expected one kubernetes readiness check, got %d", got)
	}
}

func TestListAuditEventsDefaultsToRecentLimit(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Audit Events Limit Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, secret, err := s.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.read"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	now := time.Now().UTC()
	for i := 0; i < defaultAuditEventListLimit+5; i++ {
		if err := s.AppendAuditEvent(model.AuditEvent{
			TenantID:   tenant.ID,
			ActorType:  model.ActorTypeSystem,
			ActorID:    "test",
			Action:     "audit.test",
			TargetType: "event",
			TargetID:   fmt.Sprintf("event-%03d", i),
			CreatedAt:  now.Add(time.Duration(i) * time.Second),
		}); err != nil {
			t.Fatalf("append audit event %d: %v", i, err)
		}
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/audit-events", secret, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	var response struct {
		AuditEvents []model.AuditEvent `json:"audit_events"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode audit events: %v", err)
	}
	if len(response.AuditEvents) != defaultAuditEventListLimit {
		t.Fatalf("expected default limit %d, got %d", defaultAuditEventListLimit, len(response.AuditEvents))
	}
	if got := response.AuditEvents[0].TargetID; got != "event-204" {
		t.Fatalf("expected newest event first, got %q", got)
	}
	if got := response.AuditEvents[len(response.AuditEvents)-1].TargetID; got != "event-005" {
		t.Fatalf("expected oldest returned event to respect default limit, got %q", got)
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/audit-events?limit=3", secret, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	response.AuditEvents = nil
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode limited audit events: %v", err)
	}
	if len(response.AuditEvents) != 3 {
		t.Fatalf("expected three audit events, got %+v", response.AuditEvents)
	}
	if got := response.AuditEvents[2].TargetID; got != "event-202" {
		t.Fatalf("expected custom limit to keep three newest events, got %q", got)
	}

	recorder = performJSONRequest(t, server, http.MethodGet, "/v1/audit-events?limit=1001", secret, nil)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusBadRequest, recorder.Code, recorder.Body.String())
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
	key, nodeSecret, err := s.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	var createdSecret map[string]any
	kubeServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/namespaces/kube-system/secrets":
			if err := json.NewDecoder(r.Body).Decode(&createdSecret); err != nil {
				t.Fatalf("decode bootstrap secret request: %v", err)
			}
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"name": "bootstrap-token-created"}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{
		ClusterJoinServer:            "https://100.64.0.1:6443",
		ClusterJoinServerFallbacks:   "https://100.64.0.2:6443, https://100.64.0.3:6443",
		ClusterJoinCAHash:            "deadbeef",
		ClusterJoinBootstrapTokenTTL: time.Minute,
		RegistryPullBase:             "10.128.0.2:30500",
		ClusterJoinRegistryEndpoint:  "100.64.0.1:30500",
		ClusterJoinMeshProvider:      "tailscale",
		ClusterJoinMeshLoginServer:   "https://mesh.fugue.pro",
		ClusterJoinMeshAuthKey:       "tskey-example",
	})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{
			client:      kubeServer.Client(),
			baseURL:     kubeServer.URL,
			bearerToken: "test-token",
		}, nil
	}

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
	if !strings.Contains(body, "FUGUE_JOIN_SERVER_FALLBACKS='https://100.64.0.2:6443,https://100.64.0.3:6443'") {
		t.Fatalf("expected join server fallbacks in response body, got %s", body)
	}
	if !strings.Contains(body, "FUGUE_JOIN_TOKEN='K10deadbeef::") {
		t.Fatalf("expected secure bootstrap token in response body, got %s", body)
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
	if createdSecret == nil {
		t.Fatal("expected bootstrap secret request")
	}
	metadata := createdSecret["metadata"].(map[string]any)
	labels := metadata["labels"].(map[string]any)
	if labels[clusterJoinTokenLabelManaged] != clusterJoinTokenLabelValue {
		t.Fatalf("expected fugue bootstrap token label, got %#v", labels)
	}
	if labels[clusterJoinTokenLabelNodeKey] != key.ID {
		t.Fatalf("expected node key label %q, got %#v", key.ID, labels)
	}
	if strings.TrimSpace(labels[clusterJoinTokenLabelRuntime].(string)) == "" {
		t.Fatalf("expected runtime id label in bootstrap secret, got %#v", labels)
	}
	if got, _ := createdSecret["type"].(string); got != "bootstrap.kubernetes.io/token" {
		t.Fatalf("expected bootstrap token secret type, got %#v", createdSecret["type"])
	}
	stringData := createdSecret["stringData"].(map[string]any)
	tokenID, _ := stringData["token-id"].(string)
	if tokenID == "" {
		t.Fatalf("expected bootstrap token id in secret payload, got %#v", stringData)
	}
	if tokenSecret, _ := stringData["token-secret"].(string); tokenSecret == "" {
		t.Fatalf("expected bootstrap token secret in secret payload, got %#v", stringData)
	}
	if authUsage, _ := stringData["usage-bootstrap-authentication"].(string); authUsage != "true" {
		t.Fatalf("expected bootstrap auth usage in secret payload, got %#v", stringData)
	}
	if signingUsage, _ := stringData["usage-bootstrap-signing"].(string); signingUsage != "true" {
		t.Fatalf("expected bootstrap signing usage in secret payload, got %#v", stringData)
	}
	if authGroup, _ := stringData["auth-extra-groups"].(string); authGroup != clusterJoinTokenAuthGroup {
		t.Fatalf("expected bootstrap auth group %q, got %#v", clusterJoinTokenAuthGroup, stringData)
	}
	if !strings.Contains(body, "FUGUE_JOIN_BOOTSTRAP_TOKEN_ID='"+tokenID+"'") {
		t.Fatalf("expected bootstrap token id %q in response body, got %s", tokenID, body)
	}
}

func TestJoinClusterEnvNormalizesNodeNameAndWritesRequestedAudit(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Join Cluster Audit Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, nodeSecret, err := s.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}

	kubeServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/namespaces/kube-system/secrets":
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"name": "bootstrap-token-created"}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{
		ClusterJoinServer:            "https://100.64.0.1:6443",
		ClusterJoinCAHash:            "deadbeef",
		ClusterJoinBootstrapTokenTTL: time.Minute,
		RegistryPullBase:             "10.128.0.2:30500",
		ClusterJoinRegistryEndpoint:  "100.64.0.1:30500",
	})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{
			client:      kubeServer.Client(),
			baseURL:     kubeServer.URL,
			bearerToken: "test-token",
		}, nil
	}

	form := url.Values{}
	form.Set("node_key", nodeSecret)
	form.Set("node_name", "VM-0-17-ubuntu-2")
	form.Set("machine_name", "VM-0-17-ubuntu")
	form.Set("machine_fingerprint", "cluster-name-fingerprint")

	req := httptest.NewRequest(http.MethodPost, "/v1/nodes/join-cluster/env", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "FUGUE_JOIN_NODE_NAME='vm-0-17-ubuntu-2'") {
		t.Fatalf("expected normalized node name in response body, got %s", body)
	}

	events, err := s.ListAuditEvents(tenant.ID, false, 0)
	if err != nil {
		t.Fatalf("list audit events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected one audit event, got %+v", events)
	}
	if events[0].Action != auditActionNodeJoinClusterRequested {
		t.Fatalf("expected audit action %q, got %q", auditActionNodeJoinClusterRequested, events[0].Action)
	}
	if events[0].Metadata["name"] != "vm-0-17-ubuntu-2" {
		t.Fatalf("expected normalized audit node name, got %+v", events[0].Metadata)
	}
}

func TestJoinClusterEnvRejectsInvalidKubernetesNodeName(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Join Cluster Invalid Name Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, nodeSecret, err := s.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{
		ClusterJoinServer:            "https://100.64.0.1:6443",
		ClusterJoinCAHash:            "deadbeef",
		ClusterJoinBootstrapTokenTTL: time.Minute,
		RegistryPullBase:             "10.128.0.2:30500",
		ClusterJoinRegistryEndpoint:  "100.64.0.1:30500",
	})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		t.Fatal("cluster client should not be created for invalid node names")
		return nil, nil
	}

	form := url.Values{}
	form.Set("node_key", nodeSecret)
	form.Set("node_name", "VM_0_17_ubuntu")

	req := httptest.NewRequest(http.MethodPost, "/v1/nodes/join-cluster/env", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusBadRequest, recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "invalid cluster node name") {
		t.Fatalf("expected invalid node name error, got %s", recorder.Body.String())
	}

	events, err := s.ListAuditEvents(tenant.ID, false, 0)
	if err != nil {
		t.Fatalf("list audit events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no audit events for invalid node name, got %+v", events)
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
		`--data-urlencode "bootstrap_token_id=${FUGUE_JOIN_BOOTSTRAP_TOKEN_ID:-}"`,
		`cleanup_stale_cluster_nodes`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("expected join-cluster install script to contain %q", want)
		}
	}
}

func TestInstallScriptsUseConfiguredPublicAPIDomain(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	server := NewServer(stateStore, nil, nil, ServerConfig{
		APIPublicDomain:              "api.fugue.pro",
		ClusterJoinServer:            "https://k3s.example.com",
		ClusterJoinBootstrapTokenTTL: time.Minute,
		RegistryPushBase:             "registry.fugue.internal:5000",
		RegistryPullBase:             "registry.fugue.internal:5000",
		ClusterJoinRegistryEndpoint:  "127.0.0.1:30500",
	})

	for _, tc := range []struct {
		name string
		path string
		want string
	}{
		{
			name: "join cluster",
			path: "/install/join-cluster.sh",
			want: `FUGUE_API_BASE=${FUGUE_API_BASE:-"https://api.fugue.pro"}`,
		},
		{
			name: "node updater",
			path: "/install/node-updater.sh",
			want: `FUGUE_API_BASE="${FUGUE_API_BASE:-https://api.fugue.pro}"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			req.Host = "fugue-fugue.fugue-system.svc.cluster.local:80"
			recorder := httptest.NewRecorder()

			server.Handler().ServeHTTP(recorder, req)

			if recorder.Code != http.StatusOK {
				t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
			}
			body := recorder.Body.String()
			if !strings.Contains(body, tc.want) {
				t.Fatalf("expected script to contain %q, got:\n%s", tc.want, body[:min(len(body), 512)])
			}
			if strings.Contains(body, "fugue-fugue.fugue-system.svc.cluster.local") {
				t.Fatalf("script leaked internal service hostname")
			}
		})
	}
}

func TestJoinClusterInstallScriptAvoidsRedundantRestarts(t *testing.T) {
	t.Parallel()

	var server Server
	script := server.joinClusterInstallScript("https://api.fugue.pro")

	if got := strings.Count(script, `run_systemd_action_and_wait restart k3s-agent 900`); got != 1 {
		t.Fatalf("expected exactly one k3s-agent restart request in install script, got %d", got)
	}
	if strings.Contains(script, `systemctl restart tailscaled`) {
		t.Fatalf("expected tailscaled to avoid unconditional restarts")
	}

	for _, want := range []string{
		`log_step() {`,
		`format_duration() {`,
		`run_with_heartbeat() {`,
		`print_install_timeline() {`,
		`FUGUE_PROGRESS_HEARTBEAT_SECONDS="${FUGUE_PROGRESS_HEARTBEAT_SECONDS:-15}"`,
		`wait_for_systemd_unit_active() {`,
		`run_systemd_action_and_wait() {`,
		`write_file_if_changed`,
		`remove_file_if_present`,
		`cmp -s`,
		`restart_k3s_agent_if_needed "${k3s_restart_needed}"`,
		`state="$(systemctl is-active k3s-agent 2>/dev/null || true)"`,
		`if [ "${state}" != "active" ]; then`,
		`if [ "${state}" = "activating" ]; then`,
		`forcing a clean restart so it reloads the latest join configuration`,
		`forcing a clean restart with the updated configuration.`,
		`systemctl stop k3s-agent >/dev/null 2>&1 || true`,
		`systemctl reset-failed k3s-agent >/dev/null 2>&1 || true`,
		`run_systemd_action_and_wait start k3s-agent 900`,
		`run_systemd_action_and_wait start tailscaled 60`,
		`Waiting for ${unit} to become active`,
		`Heartbeat: long-running steps print progress every ${FUGUE_PROGRESS_HEARTBEAT_SECONDS}s so the install never looks stuck.`,
		`This is normal on a first install.`,
		`Downloading and installing k3s agent binaries`,
		`Installing NFS client tools`,
		`nfs-common`,
		`reconcile_cni_bridge_mtu`,
		`FLANNEL_MTU`,
		`configure_k3s_api_load_balancer() {`,
		`fugue-k3s-api-lb.service`,
		`haproxy -Ws -f /etc/fugue/k3s-api-lb.cfg`,
		`server: "%s"\n' "${FUGUE_JOIN_EFFECTIVE_SERVER:-${FUGUE_JOIN_SERVER}}"`,
		`Requesting join parameters from control plane...`,
		`Cluster node join finished in $(format_duration $(( $(date +%s) - script_started_at ))).`,
		`case "${http_code}" in`,
		`000|409|429|5??)`,
		`--max-time 5`,
		`INSTALL_K3S_SKIP_START="true"`,
		`sanitize_k3s_env_file() {`,
		`sanitize_k3s_agent_environment_files() {`,
		`/etc/systemd/system/k3s-agent.service.env`,
		`/etc/default/k3s-agent`,
		`/etc/sysconfig/k3s-agent`,
		`Sanitized stale K3S_* environment overrides from ${target_path}.`,
		`k3s-agent environment files are already free of stale K3S_* overrides.`,
		`ensure_k3s_agent_service_override() {`,
		`/etc/systemd/system/k3s-agent.service.d/10-fugue-managed.conf`,
		`Environment="K3S_URL="`,
		`Environment="K3S_TOKEN="`,
		`Environment="K3S_CONFIG_FILE="`,
		`Environment="K3S_NODE_LABEL="`,
		`ExecStart=${k3s_binary} agent`,
		`Updated k3s-agent systemd override to ignore stale installer settings.`,
		`ensure_k3s_agent_non_stub_resolv_conf() {`,
		`*stub-resolv.conf)`,
		`/run/systemd/resolve/resolv.conf`,
		`Pointed /etc/resolv.conf at /run/systemd/resolve/resolv.conf for k3s/containerd image pulls`,
		`host_resolv_conf_changed=1`,
		`Installing local DNS escape hatch`,
		`fugue-node-dns-escape-hatch.service`,
		`fugue-node-dns-escape-hatch.timer`,
		`render_service_host_records() {`,
		`detect_kube_dns_service_ip() {`,
		`Fugue node DNS escape hatch skipping because iptables is unavailable.`,
		`k3s_restart_needed=1`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("expected join-cluster install script to contain %q", want)
		}
	}
}

func TestJoinClusterInstallScriptSupportsResourceCaps(t *testing.T) {
	t.Parallel()

	var server Server
	script := server.joinClusterInstallScript("https://api.fugue.pro")

	for _, want := range []string{
		`FUGUE_LIMIT_CPU`,
		`FUGUE_LIMIT_MEMORY`,
		`FUGUE_LIMIT_DISK`,
		`FUGUE_LIMIT_DISK_PATH`,
		`--cpu LIMIT`,
		`--memory LIMIT`,
		`--disk LIMIT`,
		`parse_args "$@"`,
		`parse_cpu_millicores() {`,
		`parse_quantity_bytes() {`,
		`configure_resource_limits() {`,
		`system-reserved=${system_reserved}`,
		`ephemeral-storage=$(format_bytes_quantity "${reserved}")`,
		`printf 'kubelet-arg:\n'`,
		`FUGUE_KUBELET_SYSTEM_RESERVED="system-reserved=${system_reserved}"`,
		`resource_limit_cpu=${FUGUE_EFFECTIVE_LIMIT_CPU:-}`,
		`resource_limit_memory=${FUGUE_EFFECTIVE_LIMIT_MEMORY:-}`,
		`resource_limit_disk=${FUGUE_EFFECTIVE_LIMIT_DISK:-}`,
		`kubelet_system_reserved=${FUGUE_KUBELET_SYSTEM_RESERVED:-}`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("expected join-cluster install script to contain %q", want)
		}
	}
}

func TestJoinClusterInstallScriptHasValidBashSyntax(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}

	var server Server
	script := server.joinClusterInstallScript("https://api.fugue.pro")
	scriptPath := filepath.Join(t.TempDir(), "join-cluster.sh")
	if err := os.WriteFile(scriptPath, []byte(script), 0o700); err != nil {
		t.Fatalf("write join-cluster script: %v", err)
	}

	cmd := exec.Command("bash", "-n", scriptPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("bash -n %s: %v\n%s", scriptPath, err, output)
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
	key, nodeSecret, err := s.CreateNodeKey(tenant.ID, "default")
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
	deletedTokens := make([]string, 0, 1)
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
		case "/api/v1/namespaces/kube-system/secrets/bootstrap-token-abc123":
			switch r.Method {
			case http.MethodGet:
				_ = json.NewEncoder(w).Encode(map[string]any{
					"metadata": map[string]any{
						"name": "bootstrap-token-abc123",
						"labels": map[string]string{
							clusterJoinTokenLabelNodeKey: key.ID,
						},
					},
				})
			case http.MethodDelete:
				deletedTokens = append(deletedTokens, "abc123")
				_ = json.NewEncoder(w).Encode(map[string]any{})
			default:
				http.NotFound(w, r)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	server := &Server{
		store:                        s,
		clusterJoinServer:            "https://100.64.0.1:6443",
		clusterJoinBootstrapTokenTTL: time.Minute,
		registryPullBase:             "10.128.0.2:30500",
		clusterJoinRegistryEndpoint:  "100.64.0.1:30500",
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
	form.Set("bootstrap_token_id", "abc123")
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
	if !strings.Contains(body, "FUGUE_JOIN_CLEANUP_BOOTSTRAP_TOKEN_REMOVED='true'") {
		t.Fatalf("expected bootstrap token cleanup result in response body, got %s", body)
	}
	if !slices.Equal(deletedNodes, []string{"alicehk2"}) {
		t.Fatalf("expected kube delete for alicehk2, got %+v", deletedNodes)
	}
	if !slices.Equal(deletedTokens, []string{"abc123"}) {
		t.Fatalf("expected bootstrap token delete for abc123, got %+v", deletedTokens)
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

func TestRevokeNodeKeyCleansManagedOwnedNodesAndBootstrapTokens(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Revoke Cleanup Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	key, nodeSecret, err := s.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	runtimeObj, err := func() (model.Runtime, error) {
		_, runtimeObj, err := s.BootstrapClusterNode(nodeSecret, "worker-1", "https://worker-1.example.com", nil, "worker-1", "fingerprint-1")
		return runtimeObj, err
	}()
	if err != nil {
		t.Fatalf("bootstrap cluster node: %v", err)
	}
	_, apiSecret, err := s.CreateAPIKey(tenant.ID, "runtime-attach", []string{"runtime.attach"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	deletedNodes := make([]string, 0, 1)
	deletedTokens := make([]string, 0, 1)
	kubeServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/kube-system/secrets":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []map[string]any{
					{
						"metadata": map[string]any{
							"name": "bootstrap-token-abc123",
						},
					},
				},
			})
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v1/namespaces/kube-system/secrets/bootstrap-token-abc123":
			deletedTokens = append(deletedTokens, "abc123")
			_ = json.NewEncoder(w).Encode(map[string]any{})
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v1/nodes/"+runtimeObj.ClusterNodeName:
			deletedNodes = append(deletedNodes, runtimeObj.ClusterNodeName)
			_ = json.NewEncoder(w).Encode(map[string]any{})
		default:
			http.NotFound(w, r)
		}
	}))
	defer kubeServer.Close()

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{
		ClusterJoinServer:            "https://100.64.0.1:6443",
		ClusterJoinBootstrapTokenTTL: time.Minute,
		RegistryPullBase:             "10.128.0.2:30500",
		ClusterJoinRegistryEndpoint:  "100.64.0.1:30500",
	})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{
			client:      kubeServer.Client(),
			baseURL:     kubeServer.URL,
			bearerToken: "test-token",
		}, nil
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/node-keys/"+key.ID+"/revoke", nil)
	req.Header.Set("Authorization", "Bearer "+apiSecret)
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	if !strings.Contains(body, `"status":"revoked"`) {
		t.Fatalf("expected revoked node key in response body, got %s", body)
	}
	if !strings.Contains(body, `"deleted_cluster_nodes":["worker-1"]`) {
		t.Fatalf("expected deleted cluster node in response body, got %s", body)
	}
	if !strings.Contains(body, `"deleted_bootstrap_token_ids":["abc123"]`) {
		t.Fatalf("expected deleted bootstrap token id in response body, got %s", body)
	}
	if !slices.Equal(deletedNodes, []string{runtimeObj.ClusterNodeName}) {
		t.Fatalf("expected deleted managed-owned node %q, got %+v", runtimeObj.ClusterNodeName, deletedNodes)
	}
	if !slices.Equal(deletedTokens, []string{"abc123"}) {
		t.Fatalf("expected deleted bootstrap token abc123, got %+v", deletedTokens)
	}

	updatedRuntime, err := s.GetRuntime(runtimeObj.ID)
	if err != nil {
		t.Fatalf("get runtime after revoke cleanup: %v", err)
	}
	if updatedRuntime.Status != model.RuntimeStatusOffline {
		t.Fatalf("expected runtime offline after revoke cleanup, got %q", updatedRuntime.Status)
	}
	if updatedRuntime.NodeKeyID != "" {
		t.Fatalf("expected runtime node key cleared after revoke cleanup, got %q", updatedRuntime.NodeKeyID)
	}
	if updatedRuntime.ClusterNodeName != "" {
		t.Fatalf("expected runtime cluster node cleared after revoke cleanup, got %q", updatedRuntime.ClusterNodeName)
	}
}

func TestRevokeNodeKeyInvalidatesExternalRuntimeCredentialsWithoutClusterJoinConfig(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("External Revoke Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	key, nodeSecret, err := s.CreateNodeKey(tenant.ID, "default")
	if err != nil {
		t.Fatalf("create node key: %v", err)
	}
	runtimeObj, runtimeKey, err := func() (model.Runtime, string, error) {
		_, runtimeObj, runtimeKey, err := s.BootstrapNode(nodeSecret, "worker-1", "https://worker-1.example.com", nil, "worker-1", "fingerprint-1")
		return runtimeObj, runtimeKey, err
	}()
	if err != nil {
		t.Fatalf("bootstrap external node: %v", err)
	}
	if _, _, err := s.AuthenticateRuntimeKey(runtimeKey); err != nil {
		t.Fatalf("authenticate runtime key before revoke: %v", err)
	}
	_, apiSecret, err := s.CreateAPIKey(tenant.ID, "runtime-attach", []string{"runtime.attach"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	req := httptest.NewRequest(http.MethodPost, "/v1/node-keys/"+key.ID+"/revoke", nil)
	req.Header.Set("Authorization", "Bearer "+apiSecret)
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	if !strings.Contains(body, `"status":"revoked"`) {
		t.Fatalf("expected revoked node key in response body, got %s", body)
	}
	if !strings.Contains(body, `"detached_runtime_ids":["`+runtimeObj.ID+`"]`) {
		t.Fatalf("expected detached external runtime id in response body, got %s", body)
	}

	updatedRuntime, err := s.GetRuntime(runtimeObj.ID)
	if err != nil {
		t.Fatalf("get runtime after revoke cleanup: %v", err)
	}
	if updatedRuntime.Status != model.RuntimeStatusOffline {
		t.Fatalf("expected runtime offline after revoke cleanup, got %q", updatedRuntime.Status)
	}
	if updatedRuntime.NodeKeyID != "" {
		t.Fatalf("expected runtime node key cleared after revoke cleanup, got %q", updatedRuntime.NodeKeyID)
	}
	if updatedRuntime.AgentKeyPrefix != "" || updatedRuntime.AgentKeyHash != "" {
		t.Fatalf("expected runtime agent key cleared after revoke cleanup, got prefix=%q hash=%q", updatedRuntime.AgentKeyPrefix, updatedRuntime.AgentKeyHash)
	}
	if _, _, err := s.AuthenticateRuntimeKey(runtimeKey); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected runtime key invalid after revoke, got %v", err)
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

func TestListRuntimesAllowsSkippingManagedLocationSync(t *testing.T) {
	t.Parallel()

	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tenant, err := s.CreateTenant("Runtime Sync Skip Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, apiSecret, err := s.CreateAPIKey(tenant.ID, "viewer", []string{"project.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	clusterClientCalls := 0
	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		clusterClientCalls++
		return nil, errors.New("unexpected cluster inventory lookup")
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/runtimes?sync_locations=false", nil)
	req.Header.Set("Authorization", "Bearer "+apiSecret)
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	if clusterClientCalls != 0 {
		t.Fatalf("expected managed location sync to be skipped, got %d calls", clusterClientCalls)
	}

	serverTiming := recorder.Header().Get("Server-Timing")
	if !strings.Contains(serverTiming, "store_runtimes;dur=") {
		t.Fatalf("expected store_runtimes timing metric, got %q", serverTiming)
	}
	if strings.Contains(serverTiming, "runtime_sync;dur=") {
		t.Fatalf("expected runtime_sync timing metric to be absent, got %q", serverTiming)
	}
}
