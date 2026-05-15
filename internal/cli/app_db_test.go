package cli

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestBuildDeployManagedPostgresGeneratesPasswordWhenOmitted(t *testing.T) {
	t.Parallel()

	cli := newCLI(io.Discard, io.Discard)
	spec, err := cli.buildDeployManagedPostgres(nil, "demo", deployCommonOptions{
		ManagedPostgres:  true,
		PostgresDatabase: "app",
		PostgresUser:     "app",
	})
	if err != nil {
		t.Fatalf("build managed postgres spec: %v", err)
	}
	if spec == nil {
		t.Fatal("expected managed postgres spec")
	}
	if strings.TrimSpace(spec.Password) == "" {
		t.Fatal("expected generated managed postgres password")
	}
	if len(spec.Password) != 48 {
		t.Fatalf("expected 48-char hex password, got %q", spec.Password)
	}
	if _, err := hex.DecodeString(spec.Password); err != nil {
		t.Fatalf("expected hex password, got %q: %v", spec.Password, err)
	}
}

func TestRunAppDatabaseConfigureGeneratesPasswordAndRedactsJSONByDefault(t *testing.T) {
	t.Parallel()

	var gotDeploy struct {
		Spec model.AppSpec `json:"spec"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/deploy":
			if err := json.NewDecoder(r.Body).Decode(&gotDeploy); err != nil {
				t.Fatalf("decode deploy body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"operation":{"id":"op_123","app_id":"app_123","type":"deploy","status":"pending"}}`))
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
		"-o", "json",
		"app", "db", "configure", "demo",
		"--database", "app",
		"--user", "app",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app db configure: %v", err)
	}

	password := strings.TrimSpace(gotDeploy.Spec.Postgres.Password)
	if password == "" {
		t.Fatal("expected configure request to include generated password")
	}
	if len(password) != 48 {
		t.Fatalf("expected 48-char hex password, got %q", password)
	}
	if _, err := hex.DecodeString(password); err != nil {
		t.Fatalf("expected hex password, got %q: %v", password, err)
	}

	out := stdout.String()
	if !strings.Contains(out, `"password": "[redacted]"`) {
		t.Fatalf("expected redacted password in JSON output, got %q", out)
	}
	if strings.Contains(out, password) {
		t.Fatalf("expected JSON output to redact generated password %q, got %q", password, out)
	}
}

func TestRunAppDatabaseShowShowSecretsOptIn(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1,"postgres":{"database":"app","user":"app","password":"service-password-123"}},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/database/status":
			_, _ = w.Write([]byte(`{"status":{"app_id":"app_123","enabled":true,"service_name":"demo-postgres","owner":"app_123","runtime_id":"runtime_managed_shared","backup_status":"pending","restore_status":"pending","grant_verification":"passed","generated_at":"2026-04-02T00:00:00Z"},"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1,"postgres":{"database":"app","user":"app","password":"service-password-123"}},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	var redactedStdout bytes.Buffer
	var redactedStderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"-o", "json",
		"app", "db", "show", "demo",
	}, &redactedStdout, &redactedStderr)
	if err != nil {
		t.Fatalf("run app db show: %v", err)
	}
	redactedOut := redactedStdout.String()
	if !strings.Contains(redactedOut, `"password": "[redacted]"`) {
		t.Fatalf("expected redacted password in default output, got %q", redactedOut)
	}
	if strings.Contains(redactedOut, "service-password-123") {
		t.Fatalf("expected default output to redact password, got %q", redactedOut)
	}

	var showSecretsStdout bytes.Buffer
	var showSecretsStderr bytes.Buffer
	err = runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"-o", "json",
		"app", "db", "show", "demo",
		"--show-secrets",
	}, &showSecretsStdout, &showSecretsStderr)
	if err != nil {
		t.Fatalf("run app db show --show-secrets: %v", err)
	}
	showSecretsOut := showSecretsStdout.String()
	if !strings.Contains(showSecretsOut, `"password": "service-password-123"`) {
		t.Fatalf("expected show-secrets output to contain password, got %q", showSecretsOut)
	}
}

func TestRunAppDatabaseShowUsesOwnedBackingService(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_a","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_a","replicas":1},"status":{"phase":"ready","current_replicas":1},"backing_services":[{"id":"svc_pg","tenant_id":"tenant_123","project_id":"project_123","owner_app_id":"app_123","name":"demo","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"runtime_id":"runtime_a","database":"app","user":"app","password":"service-password-123","service_name":"demo-postgres","failover_target_runtime_id":"runtime_b"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}],"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/database/status":
			_, _ = w.Write([]byte(`{"status":{"app_id":"app_123","enabled":true,"service_name":"demo-postgres","owner":"app_123","runtime_id":"runtime_a","failover_runtime_id":"runtime_b","backup_status":"pending","restore_status":"pending","grant_verification":"passed","generated_at":"2026-04-02T00:00:00Z"},"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_a","replicas":1},"status":{"phase":"ready","current_replicas":1},"backing_services":[{"id":"svc_pg","tenant_id":"tenant_123","project_id":"project_123","owner_app_id":"app_123","name":"demo","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"runtime_id":"runtime_a","database":"app","user":"app","password":"service-password-123","service_name":"demo-postgres","failover_target_runtime_id":"runtime_b"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}],"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
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
		"-o", "json",
		"app", "db", "show", "demo",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app db show: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		`"database": "app"`,
		`"runtime_id": "runtime_a"`,
		`"failover_target_runtime_id": "runtime_b"`,
		`"password": "[redacted]"`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got %q", want, out)
		}
	}
	if strings.Contains(out, "service-password-123") {
		t.Fatalf("expected backing service password to be redacted, got %q", out)
	}
}

func TestRunAppDatabaseConfigureUsesOwnedBackingServiceAsBaseline(t *testing.T) {
	t.Parallel()

	var gotDeploy struct {
		Spec model.AppSpec `json:"spec"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_a","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_a","replicas":1},"status":{"phase":"ready","current_replicas":1},"backing_services":[{"id":"svc_pg","tenant_id":"tenant_123","project_id":"project_123","owner_app_id":"app_123","name":"demo","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"runtime_id":"runtime_a","database":"app","user":"app","password":"service-password-123","service_name":"demo-postgres","failover_target_runtime_id":"runtime_b","instances":2,"synchronous_replicas":1}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}],"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/deploy":
			if err := json.NewDecoder(r.Body).Decode(&gotDeploy); err != nil {
				t.Fatalf("decode deploy body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"operation":{"id":"op_123","app_id":"app_123","type":"deploy","status":"pending"}}`))
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
		"app", "db", "configure", "demo",
		"--clear-failover",
		"--instances", "1",
		"--sync-replicas", "0",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app db configure: %v", err)
	}

	if gotDeploy.Spec.Postgres == nil {
		t.Fatalf("expected deploy postgres spec")
	}
	postgres := gotDeploy.Spec.Postgres
	if postgres.Database != "app" || postgres.User != "app" || postgres.Password != "service-password-123" || postgres.ServiceName != "demo-postgres" {
		t.Fatalf("expected backing service fields to be preserved, got %+v", postgres)
	}
	if postgres.FailoverTargetRuntimeID != "" {
		t.Fatalf("expected failover target to be cleared, got %+v", postgres)
	}
	if postgres.Instances != 1 || postgres.SynchronousReplicas != 0 {
		t.Fatalf("expected single async postgres, got %+v", postgres)
	}
}

func TestRunAppDatabaseSwitchoverUsesOwnedBackingService(t *testing.T) {
	t.Parallel()

	var gotBody map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_a","replicas":1},"status":{"phase":"ready","current_runtime_id":"runtime_a","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_a","replicas":1},"status":{"phase":"ready","current_runtime_id":"runtime_a","current_replicas":1},"backing_services":[{"id":"svc_pg","tenant_id":"tenant_123","project_id":"project_123","owner_app_id":"app_123","name":"demo","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"runtime_id":"runtime_a","database":"app","user":"app","service_name":"demo-postgres","failover_target_runtime_id":"runtime_b"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}],"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/database/switchover":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode switchover body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"operation":{"id":"op_123","app_id":"app_123","type":"database-switchover","status":"pending"}}`))
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
		"app", "db", "switchover", "demo",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app db switchover: %v", err)
	}

	if gotBody["target_runtime_id"] != "runtime_b" {
		t.Fatalf("expected target_runtime_id runtime_b, got %+v", gotBody)
	}
	out := stdout.String()
	for _, want := range []string{"app_id=app_123", "operation_id=op_123", "target_runtime_id=runtime_b"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppDatabaseLocalizeUsesDatabaseEndpoint(t *testing.T) {
	t.Parallel()

	var gotBody map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_app","replicas":1},"status":{"phase":"ready","current_runtime_id":"runtime_app","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_app","replicas":1},"status":{"phase":"ready","current_runtime_id":"runtime_app","current_replicas":1},"backing_services":[{"id":"svc_pg","tenant_id":"tenant_123","project_id":"project_123","owner_app_id":"app_123","name":"demo","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"runtime_id":"runtime_db","database":"app","user":"app","service_name":"demo-postgres","failover_target_runtime_id":"runtime_b"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}],"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/database/localize":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode localize body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"operation":{"id":"op_123","app_id":"app_123","type":"database-localize","status":"pending","target_runtime_id":"runtime_app"}}`))
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
		"app", "db", "localize", "demo",
		"--node", "instance-us-1",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app db localize: %v", err)
	}

	if gotBody["target_node_name"] != "instance-us-1" {
		t.Fatalf("expected target_node_name instance-us-1, got %+v", gotBody)
	}
	out := stdout.String()
	for _, want := range []string{"app_id=app_123", "operation_id=op_123", "target_runtime_id=runtime_app", "target_node_name=instance-us-1"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}
