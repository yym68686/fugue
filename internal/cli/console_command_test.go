package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestConsolePlainPreviewLoadsReadOnlyPages(t *testing.T) {
	t.Parallel()

	server := newConsolePreviewServer(t)
	defer server.Close()

	stdout, stderr, err := runConsoleCommand(server.URL, "console", "--plain", "--admin", "--project", "demo", "--mouse", "--log-lines", "20")
	if err != nil {
		t.Fatalf("run console: %v stderr=%s", err, stderr)
	}
	for _, want := range []string{
		"+ Fugue console",
		"preview=true",
		"Projects",
		"Apps",
		"Logs",
		"Runtime",
		"Admin",
		"web",
		"runtime log line",
		"GitHub Actions deploy-control-plane.yml",
		"cancel operation action is not enabled",
		"mouse=optional",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, stdout)
		}
	}
	if strings.Contains(stdout, "\x1b") {
		t.Fatalf("expected --color never console output without ANSI, got %q", stdout)
	}
}

func TestConsoleJSONIsMachineReadable(t *testing.T) {
	t.Parallel()

	server := newConsolePreviewServer(t)
	defer server.Close()

	stdout, stderr, err := runConsoleCommand(server.URL, "--json", "console", "--project", "demo")
	if err != nil {
		t.Fatalf("run console --json: %v stderr=%s", err, stderr)
	}
	var view struct {
		Preview bool `json:"preview"`
		Tables  []struct {
			Title string `json:"title"`
		} `json:"tables"`
	}
	if err := json.Unmarshal([]byte(stdout), &view); err != nil {
		t.Fatalf("decode console JSON: %v stdout=%s", err, stdout)
	}
	if !view.Preview || len(view.Tables) == 0 {
		t.Fatalf("unexpected console JSON view: %+v", view)
	}
}

func TestConsoleDefaultProjectUsesStableID(t *testing.T) {
	t.Parallel()

	server := newConsolePreviewServer(t)
	defer server.Close()

	stdout, stderr, err := runConsoleCommand(server.URL, "console", "--plain")
	if err != nil {
		t.Fatalf("run console: %v stderr=%s", err, stderr)
	}
	if strings.Contains(stdout, "state=error") {
		t.Fatalf("expected default console project to load by id, got %q", stdout)
	}
	if !strings.Contains(stdout, "Project Pretty Name") || !strings.Contains(stdout, "web") {
		t.Fatalf("expected console to render default project detail, got %q", stdout)
	}
}

func runConsoleCommand(baseURL string, args ...string) (string, string, error) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	commandArgs := append([]string{"--base-url", baseURL, "--token", "token", "--color", "never"}, args...)
	err := runWithStreams(commandArgs, &stdout, &stderr)
	return stdout.String(), stderr.String(), err
}

func newConsolePreviewServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/console/gallery":
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_123","name":"Project Pretty Name","app_count":1,"service_count":0,"lifecycle":{"label":"live","live":true,"sync_mode":"auto","tone":"positive"},"resource_usage_snapshot":{},"service_badges":[]}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/projects"):
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_123","tenant_id":"tenant_123","name":"demo","slug":"demo","description":"demo project","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/console/projects/project_123":
			_, _ = w.Write([]byte(`{"project_id":"project_123","project_name":"demo","project":{"id":"project_123","tenant_id":"tenant_123","name":"demo","slug":"demo","description":"demo project","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"},"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"web","route":{"public_url":"https://web.example.com"},"spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}],"operations":[{"id":"op_123","tenant_id":"tenant_123","app_id":"app_123","type":"deploy","status":"completed","execution_mode":"managed","requested_by_type":"api-key","requested_by_id":"key_123","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}],"cluster_nodes":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/images":
			_, _ = w.Write([]byte(`{"summary":{"total_size_bytes":0,"reclaimable_size_bytes":0},"versions":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/runtime-pods":
			_, _ = w.Write([]byte(`{"component":"app","namespace":"tenant-123","selector":"app=web","container":"app","groups":[],"warnings":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/runtime-logs":
			_, _ = w.Write([]byte(`{"component":"app","available":true,"pod":"","source":"runtime","logs":"runtime log line\nsecond line\n"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_managed_shared","tenant_id":"tenant_123","name":"shared","type":"managed-shared","access_mode":"public","status":"active","cluster_node_name":"gcp1","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/cluster/nodes":
			_, _ = w.Write([]byte(`{"cluster_nodes":[{"name":"gcp1","status":"ready","runtime_id":"runtime_managed_shared","created_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/edge/nodes":
			_, _ = w.Write([]byte(`{"nodes":[{"id":"edge-us","edge_group_id":"default","status":"ready","healthy":true,"caddy_route_count":1,"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}],"groups":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/dns/nodes":
			_, _ = w.Write([]byte(`{"nodes":[{"id":"dns-us","edge_group_id":"default","zone":"example.com","status":"ready","healthy":true,"record_count":1,"udp_listen":true,"tcp_listen":true,"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/cluster/control-plane":
			_, _ = w.Write([]byte(`{"control_plane":{"namespace":"fugue-system","release_instance":"fugue","version":"deadbeef","live_version":"deadbeef","status":"ready","observed_at":"2026-04-14T00:00:00Z","components":[],"deploy_workflow":{"repository":"acme/fugue","workflow":"deploy-control-plane.yml","status":"completed","conclusion":"success","run_number":42,"head_sha":"deadbeef","head_branch":"main","observed_at":"2026-04-14T00:00:00Z"}}}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
}
