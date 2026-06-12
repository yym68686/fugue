package cli

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRichTextAppStatusOptIn(t *testing.T) {
	server := newRichAppStatusServer(t)
	defer server.Close()

	out := runRichCommand(t, server.URL, "app", "status", "demo")
	assertRichOutput(t, out, "+ App demo", "status [ready]", "route", "op_running")
}

func TestRichTextRequiresExperimentFlag(t *testing.T) {
	server := newRichAppStatusServer(t)
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"--interactive", "always",
		"--color", "never",
		"app", "status", "demo",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app status: %v stderr=%s", err, stderr.String())
	}
	out := stdout.String()
	if strings.Contains(out, "+ App demo") {
		t.Fatalf("expected rich output to stay disabled without experiment flag, got %q", out)
	}
	if !strings.Contains(out, "app=demo") {
		t.Fatalf("expected legacy text fallback, got %q", out)
	}
}

func TestRichTextAppOverviewOptIn(t *testing.T) {
	server := newAppOverviewSecretFixtureServer(t)
	defer server.Close()

	out := runRichCommand(t, server.URL, "app", "overview", "demo")
	assertRichOutput(t, out, "+ App demo", "+ Diagnosis", "category=", "next")
}

func TestRichTextAppDiagnoseOptIn(t *testing.T) {
	server := newRichAppDiagnoseServer(t)
	defer server.Close()

	out := runRichCommand(t, server.URL, "app", "diagnose", "demo")
	assertRichOutput(t, out, "+ Diagnosis volume-affinity-conflict", "summary=pod cannot schedule", "evidence=node affinity conflict")
}

func TestRichTextOperationExplainOptIn(t *testing.T) {
	server := newRichOperationExplainServer(t)
	defer server.Close()

	out := runRichCommand(t, server.URL, "operation", "explain", "op_123")
	assertRichOutput(t, out, "+ Operation op_123", "diagnosis", "category=builder-no-eligible-nodes")
}

func TestRichTextProjectOverviewOptIn(t *testing.T) {
	server := newRichProjectOverviewServer(t)
	defer server.Close()

	out := runRichCommand(t, server.URL, "project", "overview", "demo")
	assertRichOutput(t, out, "+ Project demo", "apps=1 services=1 operations=1", "APP", "web")
}

func runRichCommand(t *testing.T, baseURL string, args ...string) string {
	t.Helper()

	t.Setenv("FUGUE_CLI_RICH_TEXT", "1")
	t.Setenv("COLUMNS", "96")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	commandArgs := append([]string{"--base-url", baseURL, "--token", "token", "--interactive", "always", "--color", "never"}, args...)
	if err := runWithStreams(commandArgs, &stdout, &stderr); err != nil {
		t.Fatalf("run rich command %v: %v stderr=%s", args, err, stderr.String())
	}
	return stdout.String()
}

func assertRichOutput(t *testing.T, out string, wants ...string) {
	t.Helper()

	if strings.Contains(out, "\x1b") {
		t.Fatalf("expected --color never rich output to contain no ANSI, got %q", out)
	}
	for _, want := range wants {
		if !strings.Contains(out, want) {
			t.Fatalf("expected rich output to contain %q, got %q", want, out)
		}
	}
}

func newRichAppStatusServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","route":{"public_url":"https://demo.example.com"},"spec":{"runtime_id":"runtime_managed_shared","replicas":2},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","route":{"public_url":"https://demo.example.com"},"spec":{"runtime_id":"runtime_managed_shared","replicas":2},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations" && r.URL.Query().Get("app_id") == "app_123":
			_, _ = w.Write([]byte(`{"operations":[{"id":"op_running","tenant_id":"tenant_123","app_id":"app_123","type":"deploy","status":"running","execution_mode":"managed","requested_by_type":"api-key","requested_by_id":"key_123","result_message":"waiting for route","created_at":"2026-04-02T00:02:00Z","updated_at":"2026-04-02T00:02:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[]}`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/projects"):
			_, _ = w.Write([]byte(`{"projects":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[]}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
}

func newRichAppDiagnoseServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"degraded","current_replicas":0},"created_at":"2026-04-16T00:00:00Z","updated_at":"2026-04-16T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/diagnosis":
			_, _ = w.Write([]byte(`{"diagnosis":{"category":"volume-affinity-conflict","summary":"pod cannot schedule","hint":"inspect pods","component":"app","namespace":"tenant-123","selector":"app=demo","evidence":["node affinity conflict"],"warnings":[]}}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
}

func newRichOperationExplainServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations/op_123":
			_, _ = w.Write([]byte(`{"operation":{"id":"op_123","tenant_id":"tenant_123","app_id":"app_123","type":"import","status":"failed","error_message":"no eligible builder nodes","created_at":"2026-04-22T10:00:00Z","updated_at":"2026-04-22T10:10:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations/op_123/diagnosis":
			_, _ = w.Write([]byte(`{"diagnosis":{"category":"builder-no-eligible-nodes","summary":"no builder nodes passed checks","hint":"Check builder node policy.","evidence":["active builder reservations: reservation-a@gcp1"]}}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
}

func newRichProjectOverviewServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/projects"):
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_123","tenant_id":"tenant_123","name":"demo","slug":"demo","description":"demo project","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/console/gallery":
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_123","name":"demo","app_count":1,"service_count":1,"lifecycle":{"label":"live","live":true,"sync_mode":"auto","tone":"positive"},"resource_usage_snapshot":{},"service_badges":[{"kind":"postgres","label":"Postgres","meta":"1"}]}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/console/projects/project_123":
			_, _ = w.Write([]byte(`{"project_id":"project_123","project_name":"demo","project":{"id":"project_123","tenant_id":"tenant_123","name":"demo","slug":"demo","description":"demo project","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"},"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"web","route":{"public_url":"https://web.example.com"},"spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}],"operations":[{"id":"op_123","tenant_id":"tenant_123","app_id":"app_123","type":"deploy","status":"completed","execution_mode":"managed","requested_by_type":"api-key","requested_by_id":"key_123","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}],"cluster_nodes":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/backing-services":
			_, _ = w.Write([]byte(`{"backing_services":[{"id":"svc_123","tenant_id":"tenant_123","project_id":"project_123","owner_app_id":"app_123","name":"app-db","type":"postgres","provisioner":"fugue","status":"ready","spec":{"postgres":{"database":"app","user":"app","service_name":"app-db","runtime_id":"runtime_managed_shared","storage_size":"10Gi"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/domains":
			_, _ = w.Write([]byte(`{"domains":[]}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
}
