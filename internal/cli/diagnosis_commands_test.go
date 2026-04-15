package cli

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRunAppDiagnoseShowsSchedulingRootCause(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"degraded","current_replicas":0},"created_at":"2026-04-16T00:00:00Z","updated_at":"2026-04-16T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/diagnosis":
			_, _ = w.Write([]byte(`{"diagnosis":{
				"category":"evicted-disk-pressure-volume-affinity",
				"summary":"pod demo-7c9d89d4c6-evicted was evicted on node gcp1 after disk pressure, and the replacement pod is now blocked by volume node affinity",
				"hint":"Inspect pod history with fugue app logs pods demo. If you have admin access, run fugue admin cluster node inspect gcp1 for host disk, kubelet journal, and metrics evidence.",
				"component":"app",
				"namespace":"tenant-123",
				"selector":"app.kubernetes.io/name=demo",
				"implicated_node":"gcp1",
				"implicated_pod":"demo-7c9d89d4c6-evicted",
				"live_pods":1,
				"ready_pods":0,
				"evidence":[
					"node gcp1 condition DiskPressure=True",
					"scheduling: 0/4 nodes are available: 1 node(s) had volume node affinity conflict."
				],
				"events":[
					{
						"namespace":"tenant-123",
						"name":"demo-evicted.182739",
						"type":"Warning",
						"reason":"Evicted",
						"message":"The node had condition: [DiskPressure].",
						"object_kind":"Pod",
						"object_name":"demo-7c9d89d4c6-evicted",
						"count":1,
						"last_timestamp":"2026-04-16T00:00:00Z"
					}
				]
			}}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"app", "diagnose", "demo",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app diagnose: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		"category=evicted-disk-pressure-volume-affinity",
		"implicated_node=gcp1",
		"implicated_pod=demo-7c9d89d4c6-evicted",
		"evidence=node gcp1 condition DiskPressure=True",
		"volume node affinity",
		"events",
		"Evicted",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected app diagnose output to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppRequestWrapsErrorWithDiagnosis(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","ports":[8080],"replicas":1},"status":{"phase":"degraded","current_replicas":0},"created_at":"2026-04-16T00:00:00Z","updated_at":"2026-04-16T00:00:00Z"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/request":
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"request app endpoint: dial tcp 10.0.0.5:8080: connect: connection refused"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/diagnosis":
			_, _ = w.Write([]byte(`{"diagnosis":{
				"category":"volume-affinity-conflict",
				"summary":"replacement pod is unschedulable because the PVC has a node-affinity conflict",
				"hint":"Inspect pod history with fugue app logs pods demo. If you have admin access, run fugue admin cluster node inspect gcp1 for host disk, kubelet journal, and metrics evidence.",
				"component":"app",
				"namespace":"tenant-123",
				"selector":"app.kubernetes.io/name=demo",
				"implicated_node":"gcp1",
				"evidence":["scheduling: 0/4 nodes are available: 1 node(s) had volume node affinity conflict."]
			}}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"app", "request", "demo", "/admin/requests",
	}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected app request to fail")
	}

	text := err.Error()
	for _, want := range []string{
		"connection refused",
		"app diagnosis: replacement pod is unschedulable because the PVC has a node-affinity conflict",
		"hint: Inspect pod history with fugue app logs pods demo.",
		"evidence: scheduling: 0/4 nodes are available: 1 node(s) had volume node affinity conflict.",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("expected wrapped app request error to contain %q, got %q", want, text)
		}
	}
}

func TestRunAdminClusterNodeInspectShowsHostEvidence(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/cluster/nodes/gcp1/diagnosis" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"diagnosis":{
			"node":{"name":"gcp1","status":"ready","region":"us-central1","runtime_id":"runtime_gcp1"},
			"summary":"node shows disk pressure evidence and missing fresh metrics",
			"janitor_namespace":"fugue-system",
			"janitor_pod":"fugue-fugue-node-janitor-abc12",
			"filesystems":[
				{"filesystem":"/dev/sda1","mount_path":"/var/lib","size_bytes":10000000000,"used_bytes":9000000000,"available_bytes":1000000000,"used_percent":90}
			],
			"hot_paths":[
				{"path":"/var/lib/containerd","bytes":7000000000}
			],
			"journal":[
				{"timestamp":"2026-04-16T00:00:00Z","message":"eviction manager: attempting to reclaim ephemeral-storage"}
			],
			"events":[
				{
					"namespace":"tenant-123",
					"name":"demo.182739",
					"type":"Warning",
					"reason":"Evicted",
					"message":"The node had condition: [DiskPressure].",
					"object_kind":"Pod",
					"object_name":"demo-7c9d89d4c6-evicted",
					"count":1,
					"last_timestamp":"2026-04-16T00:00:00Z"
				}
			],
			"metrics":{
				"status":"missing",
				"summary":"node metrics are stale or unavailable",
				"evidence":[
					"metrics-server scrape missing for node gcp1",
					"stats/summary request failed on gcp1"
				],
				"warnings":[
					"fresh node summary unavailable: service unavailable"
				]
			},
			"warnings":[
				"cluster events history is partial"
			]
		}}`))
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"admin", "cluster", "node", "inspect", "gcp1",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run admin cluster node inspect: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		"node=gcp1",
		"summary=node shows disk pressure evidence and missing fresh metrics",
		"janitor_pod=fugue-fugue-node-janitor-abc12",
		"/var/lib",
		"/var/lib/containerd",
		"eviction manager: attempting to reclaim ephemeral-storage",
		"metrics",
		"status=missing",
		"metrics-server scrape missing for node gcp1",
		"events",
		"DiskPressure",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected node inspect output to contain %q, got %q", want, out)
		}
	}
}

func TestRunAdminUsersResolveShowsWorkspaceMapping(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/fugue/admin/pages/users/enrich" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		_, _ = w.Write([]byte(`{
			"enrichmentState":"ready",
			"errors":[],
			"summary":{"adminCount":0,"blockedCount":0,"deletedCount":0,"userCount":1},
			"users":[{
				"billing":{"tenantId":"tenant_123","balanceLabel":"$0","limitLabel":"$20","loadError":"","loading":false,"monthlyEstimateLabel":"$1/mo","statusLabel":"active","statusReason":""},
				"canBlock":false,
				"canDelete":false,
				"canDemoteAdmin":false,
				"canPromoteToAdmin":false,
				"canUnblock":false,
				"email":"user@example.com",
				"isAdmin":false,
				"lastLoginExact":"2026-04-16T00:00:00Z",
				"lastLoginLabel":"today",
				"name":"User",
				"provider":"GitHub",
				"serviceCount":2,
				"status":"Active",
				"statusTone":"positive",
				"usage":{"cpuLabel":"200m cpu","diskLabel":"1 GiB","imageLabel":"500 MiB","loading":false,"memoryLabel":"512 MiB","serviceCount":2,"serviceCountLabel":"2 services"},
				"verified":true,
				"workspace":{
					"adminKeyLabel":"admin key available",
					"defaultProjectId":"project_123",
					"defaultProjectName":"workspace-default",
					"firstAppId":"app_123",
					"tenantId":"tenant_123",
					"tenantName":"Workspace Alpha"
				}
			}]
		}`))
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--web-base-url", server.URL,
		"--token", "token",
		"admin", "users", "resolve", "user@example.com",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run admin users resolve: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		"email=user@example.com",
		"tenant_id=tenant_123",
		"tenant_name=Workspace Alpha",
		"default_project=workspace-default",
		"first_app_id=app_123",
		"workspace_admin_key=admin key available",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected admin users resolve output to contain %q, got %q", want, out)
		}
	}
}
