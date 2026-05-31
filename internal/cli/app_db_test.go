package cli

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/model"

	"github.com/gorilla/websocket"
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

func TestRunAppDatabaseConfigureAppliesPostgresResourceLimits(t *testing.T) {
	t.Parallel()

	var gotDeploy struct {
		Spec model.AppSpec `json:"spec"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1,"postgres":{"database":"app","user":"app","password":"existing-password","service_name":"demo-postgres","resources":{"cpu_millicores":250,"memory_mebibytes":512}}},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
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
		"--memory-mebibytes", "768",
		"--memory-limit-mebibytes", "1024",
		"--cpu-limit-millicores", "500",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app db configure: %v", err)
	}

	if gotDeploy.Spec.Postgres == nil || gotDeploy.Spec.Postgres.Resources == nil {
		t.Fatalf("expected deploy postgres resources, got %+v", gotDeploy.Spec.Postgres)
	}
	resources := gotDeploy.Spec.Postgres.Resources
	if resources.MemoryMebibytes != 768 || resources.MemoryLimitMebibytes != 1024 || resources.CPULimitMilliCores != 500 {
		t.Fatalf("unexpected postgres resources %+v", resources)
	}
}

func TestRunAppDatabaseImportUploadsDumpField(t *testing.T) {
	t.Parallel()

	dumpPath := filepath.Join(t.TempDir(), "dump.sql")
	if err := os.WriteFile(dumpPath, []byte("select 1;"), 0o600); err != nil {
		t.Fatalf("write dump: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/database/import":
			reader, err := r.MultipartReader()
			if err != nil {
				t.Fatalf("multipart reader: %v", err)
			}
			var gotRequest model.AppDatabaseImportRequest
			var gotDump string
			for {
				part, err := reader.NextPart()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("next multipart part: %v", err)
				}
				switch part.FormName() {
				case "request":
					if err := json.NewDecoder(part).Decode(&gotRequest); err != nil {
						t.Fatalf("decode request part: %v", err)
					}
				case "dump":
					raw, err := io.ReadAll(part)
					if err != nil {
						t.Fatalf("read dump part: %v", err)
					}
					gotDump = string(raw)
				}
			}
			if gotRequest.Format != "sql" || !gotRequest.Clean || gotDump != "select 1;" {
				t.Fatalf("unexpected multipart request=%+v dump=%q", gotRequest, gotDump)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"},"job":{"id":"dbimport_123","app_id":"app_123","tenant_id":"tenant_123","source_upload_id":"upload_123","format":"sql","status":"pending","retry_count":0,"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
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
		"app", "db", "import", "demo",
		"--dump", dumpPath,
		"--format", "sql",
		"--clean",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app db import: %v stderr=%s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "dbimport_123") {
		t.Fatalf("expected import job in output, got %q", stdout.String())
	}
}

func TestIsExpectedAppDatabaseTunnelCloseClassifiesConnectionTeardown(t *testing.T) {
	t.Parallel()

	for _, err := range []error{
		io.EOF,
		&websocket.CloseError{Code: websocket.CloseAbnormalClosure, Text: "abnormal closure"},
		&websocket.CloseError{Code: websocket.CloseNormalClosure, Text: "normal closure"},
		errors.New("read tcp 127.0.0.1:15432->127.0.0.1:53122: use of closed network connection"),
		errors.New("write tcp 127.0.0.1:15432->127.0.0.1:53122: broken pipe"),
		errors.New("read tcp 127.0.0.1:15432->127.0.0.1:53122: connection reset by peer"),
	} {
		if !isExpectedAppDatabaseTunnelClose(err) {
			t.Fatalf("expected %v to be treated as normal tunnel connection teardown", err)
		}
	}
	if isExpectedAppDatabaseTunnelClose(errors.New("websocket: bad handshake")) {
		t.Fatal("expected bad handshake to remain a connection failure")
	}
}

func TestRunAppDatabaseAccessJSONRedactsAppSecrets(t *testing.T) {
	t.Parallel()

	const appJSON = `{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1,"env":{"DB_PASSWORD":"db-secret-123"},"postgres":{"database":"app","user":"app","password":"service-password-123"}},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[` + appJSON + `]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/database/access":
			_, _ = w.Write([]byte(`{"app":` + appJSON + `,"grants":[{"id":"grant_123","app_id":"app_123","tenant_id":"tenant_123","label":"repair","mode":"read-write","status":"active","token_prefix":"abc123","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/database/access":
			_, _ = w.Write([]byte(`{"grant":{"id":"grant_123","app_id":"app_123","tenant_id":"tenant_123","label":"repair","mode":"read-write","status":"active","token_prefix":"abc123","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"},"secret":"fugue_db_secret_123"}`))
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/apps/app_123/database/access/grant_123":
			_, _ = w.Write([]byte(`{"removed":true}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	for _, args := range [][]string{
		{"app", "db", "access", "show", "demo"},
		{"app", "db", "access", "create", "demo", "--label", "repair"},
		{"app", "db", "access", "revoke", "demo", "grant_123"},
	} {
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		commandArgs := append([]string{"--base-url", server.URL, "--token", "token", "-o", "json"}, args...)
		if err := runWithStreams(commandArgs, &stdout, &stderr); err != nil {
			t.Fatalf("run %v: %v", args, err)
		}
		out := stdout.String()
		for _, want := range []string{`"DB_PASSWORD": "[redacted]"`, `"password": "[redacted]"`} {
			if !strings.Contains(out, want) {
				t.Fatalf("expected output for %v to contain %q, got %q", args, want, out)
			}
		}
		for _, secret := range []string{"db-secret-123", "service-password-123"} {
			if strings.Contains(out, secret) {
				t.Fatalf("expected output for %v to redact %q, got %q", args, secret, out)
			}
		}
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

func TestRunAppDatabaseRestorePlanOutputsGuardedPlan(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_a","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_a","replicas":1},"status":{"phase":"ready","current_replicas":1},"backing_services":[{"id":"svc_pg","tenant_id":"tenant_123","project_id":"project_123","owner_app_id":"app_123","name":"demo","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"runtime_id":"runtime_a","database":"app","user":"app","service_name":"demo-postgres"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}],"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/database/status":
			_, _ = w.Write([]byte(`{"status":{"app_id":"app_123","enabled":true,"service_name":"demo-postgres","owner":"app","runtime_id":"runtime_a","backup_status":"required","restore_status":"required","grant_verification":"required","generated_at":"2026-04-02T00:00:00Z"},"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_a","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/database/query":
			_, _ = w.Write([]byte(`{"database":"app","host":"demo-postgres-rw","user":"app","columns":[{"name":"database","database_type":"text"},{"name":"user","database_type":"text"},{"name":"user_table_count","database_type":"int8"}],"rows":[{"database":"app","user":"app","user_table_count":12}],"row_count":1,"max_rows":1,"read_only":true,"duration_ms":3}`))
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
		"app", "db", "restore", "plan", "demo",
		"--source-node", "node-a",
		"--source-pgdata", "/var/lib/rancher/k3s/storage/pvc-old/pgdata",
		"--expected-system-id", "7624486791372800022",
		"--table-min-rows", "users=1",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app db restore plan: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		`"source_node": "node-a"`,
		`"source_pgdata": "/var/lib/rancher/k3s/storage/pvc-old/pgdata"`,
		`"expected_system_id": "7624486791372800022"`,
		`"restore_mode": "plan_only"`,
		`"table": "users"`,
		`"current_probe"`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppDatabaseRestoreVerifyRunsReadOnlyChecks(t *testing.T) {
	t.Parallel()

	var tableQuerySeen bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_a","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_a","replicas":1,"postgres":{"database":"app","user":"app","service_name":"demo-postgres"}},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/database/query":
			var body struct {
				SQL string `json:"sql"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode query body: %v", err)
			}
			switch {
			case strings.Contains(body.SQL, "current_database()"):
				_, _ = w.Write([]byte(`{"database":"app","host":"demo-postgres-rw","user":"app","columns":[{"name":"database","database_type":"text"}],"rows":[{"database":"app"}],"row_count":1,"max_rows":1,"read_only":true,"duration_ms":3}`))
			case strings.Contains(body.SQL, `from "users"`):
				tableQuerySeen = true
				_, _ = w.Write([]byte(`{"database":"app","host":"demo-postgres-rw","user":"app","columns":[{"name":"row_count","database_type":"int8"}],"rows":[{"row_count":42}],"row_count":1,"max_rows":1,"read_only":true,"duration_ms":3}`))
			default:
				t.Fatalf("unexpected query SQL %q", body.SQL)
			}
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
		"app", "db", "restore", "verify", "demo",
		"--expected-database", "app",
		"--table-min-rows", "users=1",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app db restore verify: %v", err)
	}
	if !tableQuerySeen {
		t.Fatal("expected restore verify to query table row count")
	}
	out := stdout.String()
	for _, want := range []string{"checks_passed=true", "check=database_name status=pass", "check=table_min_rows:users status=pass"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppDatabaseRestoreVerifyUsesBoundManagedPostgresWithoutOwner(t *testing.T) {
	t.Parallel()

	var tableQuerySeen bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_a","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_a","replicas":1},"status":{"phase":"ready","current_replicas":1},"bindings":[{"id":"binding_123","tenant_id":"tenant_123","app_id":"app_123","service_id":"svc_pg","alias":"postgres","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}],"backing_services":[{"id":"svc_pg","tenant_id":"tenant_123","project_id":"project_123","name":"demo-db","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"runtime_id":"runtime_a","database":"app","user":"app","service_name":"demo-postgres"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}],"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/database/query":
			var body struct {
				SQL string `json:"sql"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode query body: %v", err)
			}
			switch {
			case strings.Contains(body.SQL, "current_database()"):
				_, _ = w.Write([]byte(`{"database":"app","host":"demo-postgres-rw","user":"app","columns":[{"name":"database","database_type":"text"}],"rows":[{"database":"app"}],"row_count":1,"max_rows":1,"read_only":true,"duration_ms":3}`))
			case strings.Contains(body.SQL, `from "users"`):
				tableQuerySeen = true
				_, _ = w.Write([]byte(`{"database":"app","host":"demo-postgres-rw","user":"app","columns":[{"name":"row_count","database_type":"int8"}],"rows":[{"row_count":7}],"row_count":1,"max_rows":1,"read_only":true,"duration_ms":3}`))
			default:
				t.Fatalf("unexpected query SQL %q", body.SQL)
			}
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
		"app", "db", "restore", "verify", "demo",
		"--expected-database", "app",
		"--table-min-rows", "users=1",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app db restore verify: %v", err)
	}
	if !tableQuerySeen {
		t.Fatal("expected restore verify to query table row count")
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
