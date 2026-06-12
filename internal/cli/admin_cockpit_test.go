package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAdminCockpitReadOnlyOverview(t *testing.T) {
	t.Parallel()

	server := newAdminCockpitServer(t, adminCockpitFixtureModeNormal)
	defer server.Close()

	stdout, stderr, err := runAdminCockpitCommand(server.URL, "admin", "cockpit")
	if err != nil {
		t.Fatalf("run admin cockpit: %v stderr=%s", err, stderr)
	}
	for _, want := range []string{
		"+ Admin cockpit",
		"release_path=GitHub Actions deploy-control-plane.yml",
		"control_plane=ready",
		"route drilldown",
		"shared",
		"gcp1",
		"edge-us",
		"dns-us",
		"admin_writes=disabled_until_action_plan",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, stdout)
		}
	}
}

func TestAdminCockpitJSONAndEdgeCases(t *testing.T) {
	t.Parallel()

	for _, mode := range []adminCockpitFixtureMode{adminCockpitFixtureModeEmpty, adminCockpitFixtureModeOffline, adminCockpitFixtureModePermission} {
		t.Run(string(mode), func(t *testing.T) {
			server := newAdminCockpitServer(t, mode)
			defer server.Close()

			stdout, stderr, err := runAdminCockpitCommand(server.URL, "--json", "admin", "cockpit")
			if err != nil {
				t.Fatalf("run admin cockpit --json: %v stderr=%s", err, stderr)
			}
			var payload adminCockpitPayload
			if err := json.Unmarshal([]byte(stdout), &payload); err != nil {
				t.Fatalf("decode admin cockpit JSON: %v stdout=%s", err, stdout)
			}
			if payload.ReleasePath == "" {
				t.Fatalf("expected release path in payload: %+v", payload)
			}
			switch mode {
			case adminCockpitFixtureModeEmpty:
				if len(payload.ClusterNodes) != 0 {
					t.Fatalf("expected empty cluster nodes: %+v", payload.ClusterNodes)
				}
			case adminCockpitFixtureModeOffline:
				if len(payload.ClusterNodes) != 1 || payload.ClusterNodes[0].Status != "offline" {
					t.Fatalf("expected offline node: %+v", payload.ClusterNodes)
				}
			case adminCockpitFixtureModePermission:
				if len(payload.Warnings) == 0 || !strings.Contains(strings.Join(payload.Warnings, "\n"), "tenant resolve unavailable") {
					t.Fatalf("expected permission warning, got %+v", payload.Warnings)
				}
			}
		})
	}
}

func runAdminCockpitCommand(baseURL string, args ...string) (string, string, error) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	commandArgs := append([]string{"--base-url", baseURL, "--token", "token", "--color", "never"}, args...)
	err := runWithStreams(commandArgs, &stdout, &stderr)
	return stdout.String(), stderr.String(), err
}

type adminCockpitFixtureMode string

const (
	adminCockpitFixtureModeNormal     adminCockpitFixtureMode = "normal"
	adminCockpitFixtureModeEmpty      adminCockpitFixtureMode = "empty"
	adminCockpitFixtureModeOffline    adminCockpitFixtureMode = "offline"
	adminCockpitFixtureModePermission adminCockpitFixtureMode = "permission"
)

func newAdminCockpitServer(t *testing.T, mode adminCockpitFixtureMode) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			if mode == adminCockpitFixtureModePermission {
				http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
				return
			}
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/console/gallery":
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_123","name":"demo","app_count":1,"service_count":0,"lifecycle":{"label":"live","live":true,"sync_mode":"auto","tone":"positive"},"resource_usage_snapshot":{},"service_badges":[]}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_managed_shared","tenant_id":"tenant_123","name":"shared","type":"managed-shared","access_mode":"public","status":"active","cluster_node_name":"gcp1","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/cluster/nodes":
			switch mode {
			case adminCockpitFixtureModeEmpty:
				_, _ = w.Write([]byte(`{"cluster_nodes":[]}`))
			case adminCockpitFixtureModeOffline:
				_, _ = w.Write([]byte(`{"cluster_nodes":[{"name":"gcp1","status":"offline","runtime_id":"runtime_managed_shared","created_at":"2026-04-02T00:00:00Z"}]}`))
			default:
				_, _ = w.Write([]byte(`{"cluster_nodes":[{"name":"gcp1","status":"ready","runtime_id":"runtime_managed_shared","region":"us-west1","created_at":"2026-04-02T00:00:00Z"}]}`))
			}
		case r.Method == http.MethodGet && r.URL.Path == "/v1/cluster/node-policies/status":
			_, _ = w.Write([]byte(`{"summary":{"total":1,"reconciled":1,"drifted":0,"ready":1,"disk_pressure":0,"blocked_by_health":0},"node_policies":[{"node_name":"gcp1","runtime_id":"runtime_managed_shared","ready":true,"disk_pressure":false,"node_schedulable":true,"reconciled":true,"block_rollout":false}]}`))
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
