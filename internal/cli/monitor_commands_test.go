package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOperationWatchOnceRendersMonitorSnapshot(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/operations/op_123" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
		_, _ = w.Write([]byte(`{"operation":{"id":"op_123","tenant_id":"tenant_123","app_id":"app_123","type":"deploy","status":"running","execution_mode":"managed","requested_by_type":"api-key","requested_by_id":"key_123","result_message":"waiting for route","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:01:00Z"}}`))
	}))
	defer server.Close()

	stdout, stderr, err := runMonitorCommand(server.URL, "operation", "watch", "op_123", "--once", "--sort", "STATUS")
	if err != nil {
		t.Fatalf("run operation watch: %v stderr=%s", err, stderr)
	}
	for _, want := range []string{"+ Operation op_123", "operation_id=op_123", "status=running", "timeline", "q quit", "resume=fugue operation watch op_123"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, stdout)
		}
	}
	if strings.Contains(stdout, "\x1b") {
		t.Fatalf("expected --color never output without ANSI, got %q", stdout)
	}
}

func TestProjectWatchOnceRendersMonitorSnapshot(t *testing.T) {
	t.Parallel()

	server := newProjectWatchMonitorServer(t)
	defer server.Close()

	stdout, stderr, err := runMonitorCommand(server.URL, "project", "watch", "demo", "--once", "--filter", "web")
	if err != nil {
		t.Fatalf("run project watch: %v stderr=%s", err, stderr)
	}
	for _, want := range []string{"+ Project demo", "project=demo", "apps=1", "web", "operations", "resume=fugue project watch demo"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, stdout)
		}
	}
}

func TestAdminClusterTopOnceAndJSON(t *testing.T) {
	t.Parallel()

	server := newAdminClusterTopServer(t)
	defer server.Close()

	stdout, stderr, err := runMonitorCommand(server.URL, "admin", "cluster", "top", "--once", "--search", "gcp1")
	if err != nil {
		t.Fatalf("run admin cluster top: %v stderr=%s", err, stderr)
	}
	for _, want := range []string{"+ Admin cluster top", "nodes=1 ready=1", "control_plane=ready", "gcp1", "node policy", "deploy-control-plane"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, stdout)
		}
	}

	stdout, stderr, err = runMonitorCommand(server.URL, "--json", "admin", "cluster", "top")
	if err != nil {
		t.Fatalf("run admin cluster top --json: %v stderr=%s", err, stderr)
	}
	var payload struct {
		ClusterNodes []struct {
			Name string `json:"name"`
		} `json:"cluster_nodes"`
		Runtimes []struct {
			Name string `json:"name"`
		} `json:"runtimes"`
	}
	if err := json.Unmarshal([]byte(stdout), &payload); err != nil {
		t.Fatalf("decode JSON: %v stdout=%s", err, stdout)
	}
	if len(payload.ClusterNodes) != 1 || payload.ClusterNodes[0].Name != "gcp1" || len(payload.Runtimes) != 1 {
		t.Fatalf("unexpected JSON payload: %+v", payload)
	}
}

func runMonitorCommand(baseURL string, args ...string) (string, string, error) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	commandArgs := append([]string{"--base-url", baseURL, "--token", "token", "--color", "never"}, args...)
	err := runWithStreams(commandArgs, &stdout, &stderr)
	return stdout.String(), stderr.String(), err
}

func newProjectWatchMonitorServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/projects"):
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_123","tenant_id":"tenant_123","name":"demo","slug":"demo","description":"demo project","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/console/projects/project_123":
			_, _ = w.Write([]byte(`{"project_id":"project_123","project_name":"demo","project":{"id":"project_123","tenant_id":"tenant_123","name":"demo","slug":"demo","description":"demo project","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"},"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"web","route":{"public_url":"https://web.example.com"},"spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}],"operations":[{"id":"op_123","tenant_id":"tenant_123","app_id":"app_123","type":"deploy","status":"running","execution_mode":"managed","requested_by_type":"api-key","requested_by_id":"key_123","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}],"cluster_nodes":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/console/gallery":
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_123","name":"demo","app_count":1,"service_count":0,"lifecycle":{"label":"live","live":true,"sync_mode":"auto","tone":"positive"},"resource_usage_snapshot":{},"service_badges":[]}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/domains":
			_, _ = w.Write([]byte(`{"domains":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/images":
			_, _ = w.Write([]byte(`{"summary":{"total_size_bytes":0,"reclaimable_size_bytes":0},"versions":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/runtime-pods":
			_, _ = w.Write([]byte(`{"component":"app","namespace":"tenant-123","selector":"app=web","container":"app","groups":[],"warnings":[]}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
}

func newAdminClusterTopServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/cluster/nodes":
			_, _ = w.Write([]byte(`{"cluster_nodes":[{"name":"gcp1","status":"ready","runtime_id":"runtime_managed_shared","roles":["control-plane"],"region":"us-west1","cpu":{"usage_percent":42},"memory":{"usage_percent":67},"policy":{"effective_app_runtime":true,"effective_builds":true,"effective_shared_pool":true,"effective_control_plane_role":"member"},"created_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_managed_shared","tenant_id":"tenant_123","name":"shared","type":"managed-shared","access_mode":"public","status":"active","cluster_node_name":"gcp1","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/cluster/control-plane":
			_, _ = w.Write([]byte(`{"control_plane":{"namespace":"fugue-system","release_instance":"fugue","version":"deadbeef","live_version":"deadbeef","status":"ready","observed_at":"2026-04-14T00:00:00Z","components":[{"component":"api","deployment_name":"fugue-api","image":"ghcr.io/acme/fugue-api:deadbeef","image_repository":"ghcr.io/acme/fugue-api","image_tag":"deadbeef","status":"ready","desired_replicas":2,"ready_replicas":2,"updated_replicas":2,"available_replicas":2}],"deploy_workflow":{"repository":"acme/fugue","workflow":"deploy-control-plane.yml","status":"completed","conclusion":"success","run_number":42,"head_sha":"deadbeef","head_branch":"main","observed_at":"2026-04-14T00:00:00Z"}}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/cluster/node-policies/status":
			_, _ = w.Write([]byte(`{"summary":{"total":1,"reconciled":1,"drifted":0,"ready":1,"disk_pressure":0,"blocked_by_health":0},"node_policies":[{"node_name":"gcp1","runtime_id":"runtime_managed_shared","ready":true,"disk_pressure":false,"node_schedulable":true,"reconciled":true,"block_rollout":false,"policy":{"effective_app_runtime":true,"effective_builds":true,"effective_shared_pool":true,"effective_control_plane_role":"member"}}]}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
}
