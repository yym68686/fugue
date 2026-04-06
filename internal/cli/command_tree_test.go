package cli

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRootHelpListsSemanticCommands(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runWithStreams([]string{"--help"}, &stdout, &stderr); err != nil {
		t.Fatalf("run help: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		"Fugue is a semantic CLI over the Fugue control-plane API.",
		"export FUGUE_API_KEY=<your-api-key>",
		"Base URL defaults to FUGUE_BASE_URL, then FUGUE_API_URL, then https://api.fugue.pro.",
		"Tenant is auto-selected when your key only sees one tenant.",
		"Deploy and create flows default to the \"default\" project when you do not pass --project.",
		"deploy",
		"app",
		"project",
		"runtime",
		"service",
		"operation",
		"admin",
		"deploy inspect .",
		"deploy github owner/repo",
		"fugue app source show my-app",
		"fugue app failover configure my-app --app-to runtime-b",
		"fugue app binding bind my-app postgres",
		"fugue app release deploy my-app",
		"fugue app config put my-app /app/config.yaml --from-file config.yaml",
		"fugue app domain primary set my-app www.example.com",
		"fugue service postgres create app-db --runtime shared",
		"fugue operation ls --app my-app",
		"fugue runtime access show shared",
		"fugue project overview",
		"fugue project storage",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected help output to contain %q, got %q", want, out)
		}
	}
	for _, unwanted := range []string{
		"\n  template      ",
		"fugue app continuity enable my-app --app-to runtime-b",
		"fugue app deploy my-app",
		"fugue admin runtime access shared",
		"fugue project usage",
	} {
		if strings.Contains(out, unwanted) {
			t.Fatalf("expected help output to omit %q, got %q", unwanted, out)
		}
	}
}

func TestRunDeployGitHubSubcommandNormalizesOwnerRepo(t *testing.T) {
	t.Parallel()

	var gotRequest importGitHubRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/import-github":
			if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
				t.Fatalf("decode import github request: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","name":"demo"},"operation":{"id":"op_123"}}`))
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
		"deploy", "github", "example/demo",
		"--branch", "main",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run deploy github: %v", err)
	}

	if gotRequest.RepoURL != "https://github.com/example/demo" {
		t.Fatalf("expected normalized repo url, got %q", gotRequest.RepoURL)
	}
	if gotRequest.Branch != "main" {
		t.Fatalf("expected branch main, got %q", gotRequest.Branch)
	}
	if gotRequest.Name != "demo" {
		t.Fatalf("expected default app name demo, got %q", gotRequest.Name)
	}
	if got := stdout.String(); got != "app_id=app_123\noperation_id=op_123\n" {
		t.Fatalf("unexpected stdout %q", got)
	}
}

func TestRunAppScaleByNameUsesSemanticCommand(t *testing.T) {
	t.Parallel()

	var gotScaleBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/scale":
			if err := json.NewDecoder(r.Body).Decode(&gotScaleBody); err != nil {
				t.Fatalf("decode scale body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"operation":{"id":"op_123","app_id":"app_123"}}`))
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
		"app", "scale", "demo",
		"--replicas", "3",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app scale: %v", err)
	}

	if gotScaleBody["replicas"] != float64(3) {
		t.Fatalf("expected replicas=3, got %#v", gotScaleBody["replicas"])
	}
	output := stdout.String()
	if !strings.Contains(output, "app_id=app_123") {
		t.Fatalf("expected stdout to contain app id, got %q", output)
	}
	if !strings.Contains(output, "operation_id=op_123") {
		t.Fatalf("expected stdout to contain operation id, got %q", output)
	}
}

func TestRunAppContinuityAuditByNameUsesExplicitCommand(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":2},"status":{"phase":"ready","current_runtime_id":"runtime_managed_shared","current_replicas":2},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":2},"status":{"phase":"ready","current_runtime_id":"runtime_managed_shared","current_replicas":2},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_managed_shared","name":"shared","type":"managed-shared","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
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
		"app", "continuity", "audit", "demo",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app continuity audit: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		"app_id=app_123",
		"classification=ready",
		"summary=eligible for live transfer",
		"runtime_type=managed-shared",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppContinuityEnableUsesSemanticCommand(t *testing.T) {
	t.Parallel()

	var gotBody patchAppContinuityRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_a","replicas":1},"status":{"phase":"ready","current_runtime_id":"runtime_a","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_b","tenant_id":"tenant_123","name":"runtime-b","type":"external-owned","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/apps/app_123/continuity":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode continuity body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"app_failover":{"target_runtime_id":"runtime_b","auto":true},"operation":{"id":"op_123","app_id":"app_123"}}`))
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
		"app", "continuity", "enable", "demo",
		"--app-to", "runtime-b",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app continuity enable: %v", err)
	}

	if gotBody.AppFailover == nil || gotBody.AppFailover.TargetRuntimeID != "runtime_b" || !gotBody.AppFailover.Enabled {
		t.Fatalf("unexpected app failover request %+v", gotBody.AppFailover)
	}
	out := stdout.String()
	for _, want := range []string{"app_id=app_123", "operation_id=op_123", "app_failover_enabled=true", "app_failover_target_runtime_id=runtime_b"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppFailoverConfigureUsesNewSemanticCommand(t *testing.T) {
	t.Parallel()

	var gotBody patchAppContinuityRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_a","replicas":1},"status":{"phase":"ready","current_runtime_id":"runtime_a","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_b","tenant_id":"tenant_123","name":"runtime-b","type":"external-owned","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/apps/app_123/continuity":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode continuity body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"app_failover":{"target_runtime_id":"runtime_b","auto":true},"operation":{"id":"op_123","app_id":"app_123"}}`))
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
		"app", "failover", "configure", "demo",
		"--app-to", "runtime-b",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app failover configure: %v", err)
	}

	if gotBody.AppFailover == nil || gotBody.AppFailover.TargetRuntimeID != "runtime_b" || !gotBody.AppFailover.Enabled {
		t.Fatalf("unexpected app failover request %+v", gotBody.AppFailover)
	}
	out := stdout.String()
	for _, want := range []string{"app_id=app_123", "operation_id=op_123", "app_failover_enabled=true", "app_failover_target_runtime_id=runtime_b"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunDeployInspectLocalUsesUploadInspectEndpoint(t *testing.T) {
	t.Parallel()

	workDir := filepath.Join(t.TempDir(), "demo-stack")
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		t.Fatalf("mkdir work dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workDir, "Dockerfile"), []byte("FROM scratch\n"), 0o644); err != nil {
		t.Fatalf("write Dockerfile: %v", err)
	}

	var gotRequest importUploadRequest
	var archiveBytes []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/templates/inspect-upload":
			if err := r.ParseMultipartForm(8 << 20); err != nil {
				t.Fatalf("parse multipart form: %v", err)
			}
			if err := json.Unmarshal([]byte(r.FormValue("request")), &gotRequest); err != nil {
				t.Fatalf("decode request: %v", err)
			}
			archive, header, err := r.FormFile("archive")
			if err != nil {
				t.Fatalf("read archive part: %v", err)
			}
			defer archive.Close()
			archiveBytes, err = io.ReadAll(archive)
			if err != nil {
				t.Fatalf("read archive body: %v", err)
			}
			if header.Filename != "demo-stack.tgz" {
				t.Fatalf("expected archive filename demo-stack.tgz, got %q", header.Filename)
			}
			_, _ = w.Write([]byte(`{
  "upload":{
    "archive_filename":"demo-stack.tgz",
    "archive_sha256":"abc123",
    "archive_size_bytes":123,
    "default_app_name":"demo-stack",
    "source_kind":"compose",
    "source_path":"docker-compose.yml"
  },
  "compose_stack":{
    "compose_path":"docker-compose.yml",
    "primary_service":"web",
    "warnings":["missing HEALTHCHECK for web"],
    "inference_report":[{"level":"warning","category":"persistent-storage","service":"web","message":"editable persistent file detected"}],
    "services":[
      {
        "service":"web",
        "kind":"app",
        "service_type":"web",
        "backing_service":false,
        "build_strategy":"dockerfile",
        "internal_port":3000,
        "compose_service":"web",
        "published":true,
        "source_dir":"web",
        "dockerfile_path":"web/Dockerfile",
        "build_context_dir":"web",
        "binding_targets":["db"],
        "persistent_storage_seed_files":[{"path":"/workspace/config.yaml","mode":420,"seed_content":""}]
      }
    ]
  }
}`))
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
		"deploy", "inspect", workDir,
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run deploy inspect: %v", err)
	}

	if gotRequest.Name != "demo-stack" {
		t.Fatalf("expected request name demo-stack, got %+v", gotRequest)
	}
	if len(archiveBytes) == 0 {
		t.Fatal("expected archive bytes to be uploaded")
	}
	out := stdout.String()
	for _, want := range []string{
		"mode=inspect",
		"source=upload",
		"topology=compose_stack",
		"primary_service=web",
		"[services]",
		"BINDINGS",
		"[persistent_storage_seed_files]",
		"/workspace/config.yaml",
		"[warnings]",
		"[inference_report]",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunDeployInspectGitHubOwnerRepoUsesParentFlags(t *testing.T) {
	t.Parallel()

	var gotRequest inspectGitHubTemplateRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/templates/inspect-github":
			if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
				t.Fatalf("decode inspect github request: %v", err)
			}
			_, _ = w.Write([]byte(`{
  "repository":{
    "repo_url":"https://github.com/example/demo",
    "repo_visibility":"private",
    "repo_owner":"example",
    "repo_name":"demo",
    "branch":"main",
    "commit_sha":"abc123",
    "commit_committed_at":"2026-04-02T00:00:00Z",
    "default_app_name":"demo"
  },
  "fugue_manifest":{
    "manifest_path":"fugue.yaml",
    "primary_service":"web",
    "warnings":[],
    "inference_report":[],
    "services":[{"service":"web","kind":"app","service_type":"web","build_strategy":"dockerfile","internal_port":3000,"compose_service":"web","published":true,"source_dir":"web","dockerfile_path":"web/Dockerfile","build_context_dir":"web","binding_targets":[],"persistent_storage_seed_files":[]}]
  }
}`))
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
		"deploy", "inspect", "example/demo",
		"--branch", "main",
		"--private",
		"--repo-token", "secret",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run deploy inspect github shorthand: %v", err)
	}

	if gotRequest.RepoURL != "https://github.com/example/demo" || gotRequest.Branch != "main" || gotRequest.RepoVisibility != "private" || gotRequest.RepoAuthToken != "secret" {
		t.Fatalf("unexpected inspect github request %+v", gotRequest)
	}
	out := stdout.String()
	for _, want := range []string{"source=github", "repo_url=https://github.com/example/demo", "topology=fugue_manifest", "topology_path=fugue.yaml"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunDeployGitHubIncludesIdempotencyAndSeedFiles(t *testing.T) {
	t.Parallel()

	seedPath := filepath.Join(t.TempDir(), "seed.sql")
	if err := os.WriteFile(seedPath, []byte("create table demo();\n"), 0o644); err != nil {
		t.Fatalf("write seed file: %v", err)
	}

	var gotRequest importGitHubRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/import-github":
			if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
				t.Fatalf("decode github import request: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{
  "app":{"id":"app_123","name":"demo"},
  "operation":{"id":"op_123"},
  "idempotency":{"key":"import-123","status":"completed","replayed":true}
}`))
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
		"deploy", "github", "example/demo",
		"--idempotency-key", "import-123",
		"--seed-file", "web:/workspace/seed.sql=" + seedPath,
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run deploy github: %v", err)
	}

	if gotRequest.IdempotencyKey != "import-123" {
		t.Fatalf("expected idempotency key import-123, got %+v", gotRequest)
	}
	if len(gotRequest.PersistentStorageSeedFiles) != 1 {
		t.Fatalf("expected one seed file override, got %+v", gotRequest.PersistentStorageSeedFiles)
	}
	if gotRequest.PersistentStorageSeedFiles[0].Service != "web" || gotRequest.PersistentStorageSeedFiles[0].Path != "/workspace/seed.sql" {
		t.Fatalf("unexpected seed file override %+v", gotRequest.PersistentStorageSeedFiles[0])
	}
	if gotRequest.PersistentStorageSeedFiles[0].SeedContent != "create table demo();\n" {
		t.Fatalf("unexpected seed content %+v", gotRequest.PersistentStorageSeedFiles[0])
	}
	out := stdout.String()
	for _, want := range []string{
		"app_id=app_123",
		"operation_id=op_123",
		"idempotency_key=import-123",
		"idempotency_status=completed",
		"idempotency_replayed=true",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunServicePostgresCreateUsesTypedCommand(t *testing.T) {
	t.Parallel()

	var gotRequest createBackingServiceRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/projects"):
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_default","tenant_id":"tenant_123","name":"default","slug":"default","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/backing-services":
			if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
				t.Fatalf("decode backing service request: %v", err)
			}
			_, _ = w.Write([]byte(`{"backing_service":{"id":"svc_123","tenant_id":"tenant_123","project_id":"project_default","name":"app-db","type":"postgres","status":"active","spec":{"postgres":{"runtime_id":"runtime_managed_shared","database":"app","user":"app"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
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
		"service", "postgres", "create", "app-db",
		"--runtime", "shared",
		"--database", "app",
		"--user", "app",
		"--password", "secret",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run service postgres create: %v", err)
	}

	if gotRequest.Name != "app-db" || gotRequest.ProjectID != "project_default" {
		t.Fatalf("unexpected create request %+v", gotRequest)
	}
	if gotRequest.Spec.Postgres == nil || gotRequest.Spec.Postgres.RuntimeID != "runtime_managed_shared" {
		t.Fatalf("expected postgres runtime runtime_managed_shared, got %+v", gotRequest.Spec.Postgres)
	}
	out := stdout.String()
	for _, want := range []string{"service_id=svc_123", "name=app-db", "type=postgres", "runtime_id=runtime_managed_shared"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunProjectOverviewUsesConsoleEndpoints(t *testing.T) {
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
			_, _ = w.Write([]byte(`{
  "project_id":"project_123",
  "project_name":"demo",
  "project":{"id":"project_123","tenant_id":"tenant_123","name":"demo","slug":"demo","description":"demo project","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"},
  "apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"web","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}],
  "operations":[{"id":"op_123","tenant_id":"tenant_123","app_id":"app_123","type":"deploy","status":"completed","execution_mode":"managed","requested_by_type":"api-key","requested_by_id":"key_123","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}],
  "cluster_nodes":[{"name":"node-a","status":"ready","conditions":{},"created_at":"2026-04-02T00:00:00Z"}]
}`))
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
		"project", "overview", "demo",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run project overview: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{"project=demo", "lifecycle=live", "[apps]", "[operations]", "[cluster_nodes]"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunRuntimeAttachUsesSaferInstructions(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/runtimes/enroll-tokens":
			_, _ = w.Write([]byte(`{"enrollment_token":{"id":"token_123","label":"edge-a","prefix":"fgt_123","expires_at":"2026-04-10T00:00:00Z"},"secret":"super-secret-token"}`))
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
		"runtime", "attach", "edge-a",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run runtime attach: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{"secret=super-secret-token", "export_token=export FUGUE_ENROLL_TOKEN=<paste-secret-above>", "join_command=curl -fsSL " + server.URL + "/install/join-cluster.sh | sudo bash"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
	if strings.Contains(out, "FUGUE_ENROLL_TOKEN=super-secret-token") {
		t.Fatalf("expected attach instructions to avoid embedding the real secret, got %q", out)
	}
}

func TestRunAppFailoverRunByNameExecutesFailover(t *testing.T) {
	t.Parallel()

	var gotBody map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_a","replicas":1},"status":{"phase":"ready","current_runtime_id":"runtime_a","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_b","tenant_id":"tenant_123","name":"runtime-b","type":"external-owned","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/failover":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode failover body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"operation":{"id":"op_123","app_id":"app_123","type":"failover","status":"pending"}}`))
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
		"app", "failover", "run", "demo",
		"--to", "runtime_b",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app failover run: %v", err)
	}

	if gotBody["target_runtime_id"] != "runtime_b" {
		t.Fatalf("expected target_runtime_id runtime_b, got %+v", gotBody)
	}
	out := stdout.String()
	for _, want := range []string{"app_id=app_123", "operation_id=op_123"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppDeployShortcutUsesSemanticCommand(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/deploy":
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"operation":{"id":"op_123","app_id":"app_123"}}`))
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
		"app", "deploy", "demo",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app deploy shortcut: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{"app_id=app_123", "operation_id=op_123"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunEnvSetByNameUsesSemanticCommand(t *testing.T) {
	t.Parallel()

	var gotBody struct {
		Set    map[string]string `json:"set"`
		Delete []string          `json:"delete"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/apps/app_123/env":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode env patch body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"env":{"DEBUG":"1","FOO":"bar"},"operation":{"id":"op_123","app_id":"app_123"}}`))
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
		"env", "set", "demo",
		"FOO=bar",
		"DEBUG=1",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run env set: %v", err)
	}

	if gotBody.Set["FOO"] != "bar" || gotBody.Set["DEBUG"] != "1" {
		t.Fatalf("unexpected env set body %+v", gotBody.Set)
	}
	if len(gotBody.Delete) != 0 {
		t.Fatalf("expected no delete keys, got %+v", gotBody.Delete)
	}
	out := stdout.String()
	for _, want := range []string{"app_id=app_123", "operation_id=op_123", "DEBUG=1", "FOO=bar"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunDomainAddByNameUsesSemanticCommand(t *testing.T) {
	t.Parallel()

	var gotBody map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/domains":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode domain add body: %v", err)
			}
			_, _ = w.Write([]byte(`{"domain":{"hostname":"www.example.com","status":"pending","route_target":"target.example.net","updated_at":"2026-04-02T00:00:00Z","created_at":"2026-04-02T00:00:00Z"},"availability":{"hostname":"www.example.com","valid":true,"available":true,"current":false},"already_current":false}`))
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
		"domain", "add", "demo", "www.example.com",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run domain add: %v", err)
	}

	if gotBody["hostname"] != "www.example.com" {
		t.Fatalf("expected hostname www.example.com, got %+v", gotBody)
	}
	out := stdout.String()
	for _, want := range []string{"app_id=app_123", "hostname=www.example.com", "route_target=target.example.net", "available=true"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunFilesWriteByNameUsesSemanticCommand(t *testing.T) {
	t.Parallel()

	var gotBody struct {
		Files []struct {
			Path    string `json:"path"`
			Content string `json:"content"`
			Secret  bool   `json:"secret"`
			Mode    int32  `json:"mode"`
		} `json:"files"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPut && r.URL.Path == "/v1/apps/app_123/files":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode files put body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"files":[{"path":"/app/config.yaml","content":"port: 8080\n","secret":false,"mode":420}],"operation":{"id":"op_123","app_id":"app_123"}}`))
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
		"files", "write", "demo", "/app/config.yaml",
		"--content", "port: 8080\n",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run files write: %v", err)
	}

	if len(gotBody.Files) != 1 {
		t.Fatalf("expected one file in body, got %+v", gotBody.Files)
	}
	if gotBody.Files[0].Path != "/app/config.yaml" {
		t.Fatalf("expected file path /app/config.yaml, got %+v", gotBody.Files[0])
	}
	if gotBody.Files[0].Content != "port: 8080\n" {
		t.Fatalf("expected file content to be forwarded, got %+v", gotBody.Files[0])
	}
	out := stdout.String()
	for _, want := range []string{"app_id=app_123", "operation_id=op_123", "/app/config.yaml", "644"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunFilesReadByNameUsesSemanticCommand(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/files":
			_, _ = w.Write([]byte(`{"files":[{"path":"/app/config.yaml","content":"port: 8080\n","secret":false,"mode":420}]}`))
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
		"files", "read", "demo", "/app/config.yaml",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run files read: %v", err)
	}

	if got := stdout.String(); got != "port: 8080\n" {
		t.Fatalf("unexpected files read stdout %q", got)
	}
}

func TestRunWorkspaceReadUsesRelativePathWithinWorkspace(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1,"workspace":{"mount_path":"/workspace"}},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1,"workspace":{"mount_path":"/workspace"}},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/filesystem/file":
			if got := r.URL.Query().Get("path"); got != "/workspace/notes/hello.txt" {
				t.Fatalf("expected workspace path /workspace/notes/hello.txt, got %q", got)
			}
			_, _ = w.Write([]byte(`{"component":"app","pod":"demo-pod","path":"/workspace/notes/hello.txt","workspace_root":"/workspace","content":"hello from fugue\n","encoding":"utf-8","size":17,"mode":420,"modified_at":"2026-04-02T00:00:00Z","truncated":false}`))
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
		"workspace", "read", "demo", "notes/hello.txt",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run workspace read: %v", err)
	}

	if got := stdout.String(); got != "hello from fugue\n" {
		t.Fatalf("unexpected workspace read stdout %q", got)
	}
}

func TestRunAdminRuntimeAccessShowsSharingGrants(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_b","tenant_id":"tenant_123","name":"runtime-b","type":"external-owned","access_mode":"shared","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes/runtime_b":
			_, _ = w.Write([]byte(`{"runtime":{"id":"runtime_b","tenant_id":"tenant_123","name":"runtime-b","type":"external-owned","access_mode":"shared","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes/runtime_b/sharing":
			_, _ = w.Write([]byte(`{"runtime":{"id":"runtime_b","tenant_id":"tenant_123","name":"runtime-b","type":"external-owned","access_mode":"shared","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"},"grants":[{"runtime_id":"runtime_b","tenant_id":"tenant_999","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-03T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Owner","slug":"owner"},{"id":"tenant_999","name":"Acme","slug":"acme"}]}`))
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
		"admin", "runtime", "access", "runtime-b",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run admin runtime access: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{"runtime_id=runtime_b", "access_mode=shared", "grants=1", "Acme", "tenant_999"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}
