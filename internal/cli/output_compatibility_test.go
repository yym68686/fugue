package cli

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOutputCompatibilityAppStatusJSONBaseline(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":2},"status":{"phase":"ready","current_replicas":2},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":2},"status":{"phase":"ready","current_replicas":2},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations" && r.URL.Query().Get("app_id") == "app_123":
			_, _ = w.Write([]byte(`{"operations":[{"id":"op_running","tenant_id":"tenant_123","app_id":"app_123","type":"deploy","status":"running","execution_mode":"managed","requested_by_type":"api-key","requested_by_id":"key_123","created_at":"2026-04-02T00:02:00Z","updated_at":"2026-04-02T00:02:00Z"}]}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	stdout, stderr := runOutputCompatibilityCommand(t, "--base-url", server.URL, "--token", "token", "--json", "app", "status", "demo")
	assertNoANSI(t, "app status --json stdout", stdout)
	if stderr != "" {
		t.Fatalf("expected app status --json stderr to stay quiet, got %q", stderr)
	}
	var payload struct {
		App struct {
			ID     string `json:"id"`
			Name   string `json:"name"`
			Status struct {
				Phase           string `json:"phase"`
				CurrentReplicas int    `json:"current_replicas"`
			} `json:"status"`
		} `json:"app"`
		ActiveOperations []struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"active_operations"`
	}
	decodeJSONOutput(t, stdout, &payload)
	if payload.App.ID != "app_123" || payload.App.Name != "demo" || payload.App.Status.Phase != "ready" || payload.App.Status.CurrentReplicas != 2 {
		t.Fatalf("unexpected app status payload %+v", payload.App)
	}
	if len(payload.ActiveOperations) != 1 || payload.ActiveOperations[0].ID != "op_running" || payload.ActiveOperations[0].Status != "running" {
		t.Fatalf("unexpected active operations %+v", payload.ActiveOperations)
	}
}

func TestOutputCompatibilityAppOverviewJSONBaseline(t *testing.T) {
	t.Parallel()

	server := newAppOverviewSecretFixtureServer(t)
	defer server.Close()

	stdout, stderr := runOutputCompatibilityCommand(t, "--base-url", server.URL, "--token", "token", "--json", "app", "overview", "demo")
	assertNoANSI(t, "app overview --json stdout", stdout)
	if stderr != "" {
		t.Fatalf("expected app overview --json stderr to stay quiet, got %q", stderr)
	}
	var payload map[string]any
	decodeJSONOutput(t, stdout, &payload)
	for _, key := range []string{"app", "bindings", "backing_services", "operations"} {
		if _, ok := payload[key]; !ok {
			t.Fatalf("expected app overview JSON to include %q, got keys %+v", key, keysOfAnyMap(payload))
		}
	}
	for _, leaked := range []string{"repo-token-123", "db-secret-123", "operation-secret-123", "binding-secret-123"} {
		if strings.Contains(stdout, leaked) {
			t.Fatalf("expected app overview JSON to redact %q, got %q", leaked, stdout)
		}
	}
}

func TestOutputCompatibilityAppDiagnoseJSONBaseline(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"degraded","current_replicas":0},"created_at":"2026-04-16T00:00:00Z","updated_at":"2026-04-16T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/diagnosis":
			_, _ = w.Write([]byte(`{"diagnosis":{"category":"volume-affinity-conflict","summary":"replacement pod is unschedulable because the PVC has a node-affinity conflict","hint":"inspect pod history","component":"app","namespace":"tenant-123","selector":"app=demo","implicated_node":"gcp1","implicated_pod":"demo-abc","live_pods":1,"ready_pods":0,"evidence":["scheduling failed"],"warnings":[]}}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	stdout, stderr := runOutputCompatibilityCommand(t, "--base-url", server.URL, "--token", "token", "--json", "app", "diagnose", "demo")
	assertNoANSI(t, "app diagnose --json stdout", stdout)
	if stderr != "" {
		t.Fatalf("expected app diagnose --json stderr to stay quiet, got %q", stderr)
	}
	var payload struct {
		Diagnosis struct {
			Category       string   `json:"category"`
			Component      string   `json:"component"`
			ImplicatedNode string   `json:"implicated_node"`
			Evidence       []string `json:"evidence"`
		} `json:"diagnosis"`
	}
	decodeJSONOutput(t, stdout, &payload)
	if payload.Diagnosis.Category != "volume-affinity-conflict" || payload.Diagnosis.Component != "app" || payload.Diagnosis.ImplicatedNode != "gcp1" || len(payload.Diagnosis.Evidence) != 1 {
		t.Fatalf("unexpected app diagnose JSON %+v", payload.Diagnosis)
	}
}

func TestOutputCompatibilityOperationExplainJSONBaseline(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations/op_123":
			_, _ = w.Write([]byte(`{"operation":{"id":"op_123","tenant_id":"tenant_123","app_id":"app_123","type":"import","status":"failed","error_message":"select builder placement: no eligible builder nodes for profile heavy","created_at":"2026-04-22T10:00:00Z","updated_at":"2026-04-22T10:10:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations/op_123/diagnosis":
			_, _ = w.Write([]byte(`{"diagnosis":{"category":"builder-no-eligible-nodes","summary":"no builder nodes passed checks","hint":"Check builder node policy.","evidence":["active builder reservations: reservation-a@gcp1"],"builder_placement":{"profile":"heavy","build_strategy":"dockerfile"}}}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	stdout, stderr := runOutputCompatibilityCommand(t, "--base-url", server.URL, "--token", "token", "--json", "operation", "explain", "op_123")
	assertNoANSI(t, "operation explain --json stdout", stdout)
	if stderr != "" {
		t.Fatalf("expected operation explain --json stderr to stay quiet, got %q", stderr)
	}
	var payload struct {
		Operation struct {
			ID     string `json:"id"`
			Type   string `json:"type"`
			Status string `json:"status"`
		} `json:"operation"`
		Diagnosis struct {
			Category string `json:"category"`
		} `json:"diagnosis"`
	}
	decodeJSONOutput(t, stdout, &payload)
	if payload.Operation.ID != "op_123" || payload.Operation.Type != "import" || payload.Operation.Status != "failed" || payload.Diagnosis.Category != "builder-no-eligible-nodes" {
		t.Fatalf("unexpected operation explain JSON %+v", payload)
	}
}

func TestOutputCompatibilityProjectOverviewJSONBaseline(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/projects"):
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_123","tenant_id":"tenant_123","name":"demo","slug":"demo","description":"demo project","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/console/gallery":
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_123","name":"demo","app_count":1,"service_count":1,"lifecycle":{"label":"live","live":true,"sync_mode":"auto","tone":"positive"},"resource_usage_snapshot":{},"service_badges":[{"kind":"postgres","label":"Postgres","meta":"1"}]}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/console/projects/project_123":
			_, _ = w.Write([]byte(`{"project_id":"project_123","project_name":"demo","project":{"id":"project_123","tenant_id":"tenant_123","name":"demo","slug":"demo","description":"demo project","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"},"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"web","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}],"operations":[{"id":"op_123","tenant_id":"tenant_123","app_id":"app_123","type":"deploy","status":"completed","execution_mode":"managed","requested_by_type":"api-key","requested_by_id":"key_123","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}],"cluster_nodes":[{"name":"node-a","status":"ready","conditions":{},"created_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/backing-services":
			_, _ = w.Write([]byte(`{"backing_services":[{"id":"svc_123","tenant_id":"tenant_123","project_id":"project_123","owner_app_id":"app_123","name":"app-db","type":"postgres","provisioner":"fugue","status":"ready","spec":{"postgres":{"database":"app","user":"app","service_name":"app-db","runtime_id":"runtime_managed_shared","storage_size":"10Gi"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/domains":
			_, _ = w.Write([]byte(`{"domains":[{"app_id":"app_123","tenant_id":"tenant_123","hostname":"web.example.com","status":"verified","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	stdout, stderr := runOutputCompatibilityCommand(t, "--base-url", server.URL, "--token", "token", "--json", "project", "overview", "demo")
	assertNoANSI(t, "project overview --json stdout", stdout)
	if stderr != "" {
		t.Fatalf("expected project overview --json stderr to stay quiet, got %q", stderr)
	}
	var payload map[string]any
	decodeJSONOutput(t, stdout, &payload)
	for _, key := range []string{"project", "summary", "status", "services", "domains", "databases"} {
		if _, ok := payload[key]; !ok {
			t.Fatalf("expected project overview JSON to include %q, got keys %+v", key, keysOfAnyMap(payload))
		}
	}
}

func TestOutputCompatibilityCopySensitiveTextStaysPlainAndRawWhereExpected(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/env":
			_, _ = w.Write([]byte(`{"env":{"APP_PUBLIC_URL":"https://demo.example.com","GREETING":"hello world","SERVICE_KEY":"svc-secret"},"entries":[{"key":"APP_PUBLIC_URL","value":"https://demo.example.com","source":"app","source_ref":"spec.env"},{"key":"GREETING","value":"hello world","source":"app","source_ref":"spec.env"},{"key":"SERVICE_KEY","value":"svc-secret","source":"binding","source_ref":"postgres"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/database/query":
			_, _ = w.Write([]byte(`{"database":"demo","host":"db.internal","user":"demo","columns":[{"name":"status","database_type":"TEXT"}],"rows":[{"status":"ok"}],"row_count":1,"max_rows":100,"read_only":true,"duration_ms":12}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/raw":
			_, _ = w.Write([]byte(`{"ok":true}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	cases := []struct {
		name       string
		args       []string
		wantStdout []string
	}{
		{
			name: "env ls",
			args: []string{"--base-url", server.URL, "--token", "token", "env", "ls", "demo"},
			wantStdout: []string{
				"SERVICE_KEY",
				"svc-secret",
			},
		},
		{
			name:       "env export",
			args:       []string{"--base-url", server.URL, "--token", "token", "env", "export", "demo"},
			wantStdout: []string{"APP_PUBLIC_URL=https://demo.example.com", `GREETING="hello world"`, "SERVICE_KEY=svc-secret"},
		},
		{
			name:       "app db query",
			args:       []string{"--base-url", server.URL, "--token", "token", "app", "db", "query", "demo", "--sql", "select status"},
			wantStdout: []string{"database=demo", "status", "ok"},
		},
		{
			name:       "api request",
			args:       []string{"--base-url", server.URL, "--token", "token", "api", "request", "GET", "/v1/raw"},
			wantStdout: []string{"status=200 OK", `{"ok":true}`},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			stdout, _ := runOutputCompatibilityCommand(t, tc.args...)
			assertNoANSI(t, tc.name+" stdout", stdout)
			for _, want := range tc.wantStdout {
				if !strings.Contains(stdout, want) {
					t.Fatalf("expected stdout to contain %q, got %q", want, stdout)
				}
			}
		})
	}
}

func TestOutputCompatibilityDiagnosticBundleRedactsByDefault(t *testing.T) {
	t.Parallel()

	server := newDiagnosticEvidenceTestServer(t)
	defer server.Close()

	archivePath := t.TempDir() + "/bundle.zip"
	stdout, _ := runOutputCompatibilityCommand(t, "--base-url", server.URL, "--token", "token", "--json", "debug", "bundle", "demo", "--request-id", "req-123", "--archive", archivePath)
	assertNoANSI(t, "debug bundle --json stdout", stdout)
	for _, want := range []string{`"schema_version": "fugue.debug-bundle.v1"`, `"redacted": true`} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("expected debug bundle stdout to contain %q, got %q", want, stdout)
		}
	}
	backupStatus := readZipEntryForTest(t, archivePath, "backup-status.json")
	if !strings.Contains(backupStatus, redactedSecretValue) {
		t.Fatalf("expected backup status to contain redacted markers, got %s", backupStatus)
	}
	for _, leaked := range []string{"fugue-backups/private", "secret-sha256", "secret-manifest-digest", "backup_backend_private"} {
		if strings.Contains(backupStatus, leaked) {
			t.Fatalf("expected backup status to redact %q, got %s", leaked, backupStatus)
		}
	}
}

func readZipEntryForTest(t *testing.T, archivePath, entryName string) string {
	t.Helper()
	reader, err := zip.OpenReader(archivePath)
	if err != nil {
		t.Fatalf("open archive: %v", err)
	}
	defer reader.Close()
	for _, file := range reader.File {
		if file.Name != entryName {
			continue
		}
		entry, err := file.Open()
		if err != nil {
			t.Fatalf("open zip entry %s: %v", entryName, err)
		}
		defer entry.Close()
		payload, err := io.ReadAll(entry)
		if err != nil {
			t.Fatalf("read zip entry %s: %v", entryName, err)
		}
		return string(payload)
	}
	t.Fatalf("zip entry %s not found", entryName)
	return ""
}

func TestTerminalModeFlagsValidate(t *testing.T) {
	t.Parallel()

	stdout, stderr := runOutputCompatibilityCommand(t, "--color", "never", "--interactive", "never", "--json", "version")
	assertNoANSI(t, "version --json stdout", stdout)
	if stderr != "" {
		t.Fatalf("expected valid terminal modes to keep stderr quiet, got %q", stderr)
	}

	var invalidStdout bytes.Buffer
	var invalidStderr bytes.Buffer
	err := runWithStreams([]string{"--color", "sometimes", "version"}, &invalidStdout, &invalidStderr)
	if err == nil {
		t.Fatal("expected invalid --color to fail")
	}
	if !strings.Contains(err.Error(), "unsupported terminal mode") {
		t.Fatalf("expected terminal mode validation error, got %v", err)
	}
}

func runOutputCompatibilityCommand(t *testing.T, args ...string) (string, string) {
	t.Helper()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runWithStreams(args, &stdout, &stderr); err != nil {
		t.Fatalf("run %v: %v stderr=%s", args, err, stderr.String())
	}
	return stdout.String(), stderr.String()
}

func assertNoANSI(t *testing.T, name string, value string) {
	t.Helper()

	if strings.Contains(value, "\x1b") {
		t.Fatalf("expected %s to contain no ANSI/control escape bytes, got %q", name, value)
	}
}

func decodeJSONOutput(t *testing.T, raw string, target any) {
	t.Helper()

	if err := json.Unmarshal([]byte(raw), target); err != nil {
		t.Fatalf("decode JSON output: %v\n%s", err, raw)
	}
}

func keysOfAnyMap(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}
