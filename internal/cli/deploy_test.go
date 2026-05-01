package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEffectiveBaseURLUsesCloudDefaultAndEnvAliases(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cli := newCLI(&stdout, &stderr)

	if got := cli.effectiveBaseURL(); got != defaultCloudBaseURL {
		t.Fatalf("expected default base url %q, got %q", defaultCloudBaseURL, got)
	}

	t.Setenv("FUGUE_API_URL", "https://api.example.com")
	if got := cli.effectiveBaseURL(); got != "https://api.example.com" {
		t.Fatalf("expected FUGUE_API_URL to be used, got %q", got)
	}

	t.Setenv("FUGUE_BASE_URL", "https://api.internal.example.com")
	if got := cli.effectiveBaseURL(); got != "https://api.internal.example.com" {
		t.Fatalf("expected FUGUE_BASE_URL to override FUGUE_API_URL, got %q", got)
	}
}

func TestNewClientMissingTokenErrorExplainsHowToConfigureAuth(t *testing.T) {
	_, err := NewClient(defaultCloudBaseURL, "")
	if err == nil {
		t.Fatalf("expected error")
	}
	message := err.Error()
	for _, want := range []string{"API key is required", "FUGUE_API_KEY", "FUGUE_BOOTSTRAP_KEY"} {
		if !strings.Contains(message, want) {
			t.Fatalf("expected error %q to contain %q", message, want)
		}
	}
}

func TestResolveTenantSelectionAutoSelectsSingleVisibleTenant(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/tenants" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
	}))
	defer server.Close()

	client, err := NewClient(server.URL, "token")
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	tenantID, err := resolveTenantSelection(client, "", "")
	if err != nil {
		t.Fatalf("resolve tenant: %v", err)
	}
	if tenantID != "tenant_123" {
		t.Fatalf("unexpected tenant id %q", tenantID)
	}
}

func TestResolveProjectSelectionSkipsLookupForDefaultProject(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected request to %s", r.URL.String())
	}))
	defer server.Close()

	client, err := NewClient(server.URL, "token")
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	projectID, projectRequest, err := resolveProjectSelection(client, "tenant_123", "", "default")
	if err != nil {
		t.Fatalf("resolve project: %v", err)
	}
	if projectID != "" {
		t.Fatalf("expected empty project id, got %q", projectID)
	}
	if projectRequest != nil {
		t.Fatalf("expected no project creation request, got %+v", projectRequest)
	}
}

func TestRunDeployWithRepoURLImportsGitHubAndLoadsEnv(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ".env"), []byte(strings.Join([]string{
		"# local deployment env",
		"OPENAI_API_KEY=sk-demo",
		"APP_ENV=production # inline comment",
		`GREETING="hello world"`,
	}, "\n")), 0o600); err != nil {
		t.Fatalf("write env file: %v", err)
	}

	var gotRequest importGitHubRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/templates/inspect-github":
			_, _ = w.Write([]byte(`{"repository":{"repo_url":"https://github.com/example/demo","repo_visibility":"public","repo_owner":"example","repo_name":"demo","branch":"main","default_app_name":"demo"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/import-github":
			if got := r.Header.Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
				t.Fatalf("expected application/json content type, got %q", got)
			}
			if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
				t.Fatalf("decode github import request: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"app":{"id":"app_123"},"operation":{"id":"op_123"}}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runDeployWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"--dir", dir,
		"--repo-url", "https://github.com/example/demo",
		"--branch", "main",
		"--build-strategy", "dockerfile",
		"--dockerfile-path", "Dockerfile",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run deploy: %v", err)
	}

	if gotRequest.RepoURL != "https://github.com/example/demo" {
		t.Fatalf("expected repo_url to be forwarded, got %q", gotRequest.RepoURL)
	}
	if gotRequest.Branch != "main" {
		t.Fatalf("expected branch main, got %q", gotRequest.Branch)
	}
	if gotRequest.Name != "demo" {
		t.Fatalf("expected default app name demo, got %q", gotRequest.Name)
	}
	if gotRequest.DockerfilePath != "Dockerfile" {
		t.Fatalf("expected dockerfile path to be forwarded, got %q", gotRequest.DockerfilePath)
	}
	if gotRequest.Env["OPENAI_API_KEY"] != "sk-demo" {
		t.Fatalf("expected OPENAI_API_KEY to be loaded, got %q", gotRequest.Env["OPENAI_API_KEY"])
	}
	if gotRequest.Env["APP_ENV"] != "production" {
		t.Fatalf("expected APP_ENV without inline comment, got %q", gotRequest.Env["APP_ENV"])
	}
	if gotRequest.Env["GREETING"] != "hello world" {
		t.Fatalf("expected quoted GREETING, got %q", gotRequest.Env["GREETING"])
	}
	if got := stdout.String(); got != "app_id=app_123\noperation_id=op_123\n" {
		t.Fatalf("unexpected stdout %q", got)
	}
	if !strings.Contains(stderr.String(), "Loaded 3 env vars") {
		t.Fatalf("expected stderr to mention loaded env vars, got %q", stderr.String())
	}
}

func TestRunDeployWithRepoURLUsesFugueManifestEnvFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeTestFile(t, filepath.Join(dir, "fugue.yaml"), "version: 1\nenv_file: .env.fugue\nservices: {}\n")
	writeTestFile(t, filepath.Join(dir, ".env"), "APP_ENV=wrong\n")
	writeTestFile(t, filepath.Join(dir, ".env.fugue"), "APP_ENV=fugue\nFUGUE_ONLY=true\n")

	var gotRequest importGitHubRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/templates/inspect-github":
			_, _ = w.Write([]byte(`{"repository":{"repo_url":"https://github.com/example/demo","repo_visibility":"public","repo_owner":"example","repo_name":"demo","branch":"main","default_app_name":"demo"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/import-github":
			if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
				t.Fatalf("decode github import request: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"app":{"id":"app_123"},"operation":{"id":"op_123"}}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runDeployWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"--dir", dir,
		"--repo-url", "https://github.com/example/demo",
		"--branch", "main",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run deploy: %v", err)
	}

	if gotRequest.Env["APP_ENV"] != "fugue" {
		t.Fatalf("expected APP_ENV from .env.fugue, got %q", gotRequest.Env["APP_ENV"])
	}
	if gotRequest.Env["FUGUE_ONLY"] != "true" {
		t.Fatalf("expected FUGUE_ONLY from .env.fugue, got %q", gotRequest.Env["FUGUE_ONLY"])
	}
	if strings.Contains(stderr.String(), string(filepath.Separator)+".env\n") {
		t.Fatalf("expected fugue manifest env file to override .env, got stderr %q", stderr.String())
	}
	if !strings.Contains(stderr.String(), ".env.fugue") {
		t.Fatalf("expected stderr to mention .env.fugue, got %q", stderr.String())
	}
}

func TestRunDeployExistingAppCanClearFiles(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte("FROM nginx:alpine\n"), 0o644); err != nil {
		t.Fatalf("write dockerfile: %v", err)
	}

	var gotRequest importUploadRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/import-upload":
			if err := r.ParseMultipartForm(32 << 20); err != nil {
				t.Fatalf("parse multipart form: %v", err)
			}
			if err := json.Unmarshal([]byte(r.FormValue("request")), &gotRequest); err != nil {
				t.Fatalf("decode upload import request: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","name":"demo"},"operation":{"id":"op_123","app_id":"app_123"}}`))
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
		"deploy", dir,
		"--app", "demo",
		"--clear-files",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run deploy existing app: %v", err)
	}

	if gotRequest.AppID != "app_123" {
		t.Fatalf("expected app_id app_123, got %+v", gotRequest)
	}
	if !gotRequest.ClearFiles {
		t.Fatalf("expected clear_files to be forwarded, got %+v", gotRequest)
	}
}

func TestRunDeployLocalDryRunKeepsTopologyModeWhenDefaultNameCollidesWithExistingApp(t *testing.T) {
	t.Parallel()

	repoRoot := filepath.Join(t.TempDir(), "argus")
	if err := os.MkdirAll(repoRoot, 0o755); err != nil {
		t.Fatalf("mkdir repo root: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoRoot, "fugue.yaml"), []byte(strings.TrimSpace(`
version: 1
primary_service: web

services:
  web:
    public: true
    port: 80
    build:
      strategy: dockerfile
      context: .
      dockerfile: Dockerfile
`)), 0o644); err != nil {
		t.Fatalf("write fugue manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(repoRoot, "Dockerfile"), []byte("FROM scratch\n"), 0o644); err != nil {
		t.Fatalf("write Dockerfile: %v", err)
	}

	var importRequest importUploadRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/projects":
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_default","name":"default","slug":"default"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_existing","tenant_id":"tenant_123","project_id":"project_default","name":"argus"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/templates/inspect-upload":
			if err := r.ParseMultipartForm(32 << 20); err != nil {
				t.Fatalf("parse inspect multipart form: %v", err)
			}
			var inspectRequest importUploadRequest
			if err := json.Unmarshal([]byte(r.FormValue("request")), &inspectRequest); err != nil {
				t.Fatalf("decode inspect request: %v", err)
			}
			if strings.TrimSpace(inspectRequest.AppID) != "" {
				t.Fatalf("inspect request unexpectedly pinned app_id=%q", inspectRequest.AppID)
			}
			_, _ = w.Write([]byte(`{"upload":{"default_app_name":"argus","source_kind":"fugue","source_path":"fugue.yaml"},"fugue_manifest":{"manifest_path":"fugue.yaml","primary_service":"web","services":[]}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/import-upload":
			if err := r.ParseMultipartForm(32 << 20); err != nil {
				t.Fatalf("parse import multipart form: %v", err)
			}
			if err := json.Unmarshal([]byte(r.FormValue("request")), &importRequest); err != nil {
				t.Fatalf("decode import request: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"plan":{"mode":"update_existing","delete_missing":false,"dry_run":true,"services":[{"service":"web","action":"update","app_name":"argus","build_strategy":"dockerfile"}],"delete_candidates":[],"warnings":[]},"fugue_manifest":{"manifest_path":"fugue.yaml","primary_service":"web","services":[]}}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runDeployWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"--project", "default",
		"--dir", repoRoot,
		"--update-existing",
		"--dry-run",
		"--json",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run local topology deploy dry-run: %v", err)
	}

	if strings.TrimSpace(importRequest.AppID) != "" {
		t.Fatalf("expected topology dry-run to avoid app_id pinning, got %q", importRequest.AppID)
	}
	if !importRequest.UpdateExisting {
		t.Fatalf("expected update_existing to be preserved")
	}
	if !importRequest.DryRun {
		t.Fatalf("expected dry_run to be preserved")
	}

	var payload importBundleJSON
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode stdout json: %v", err)
	}
	if payload.Plan == nil || !payload.Plan.DryRun {
		t.Fatalf("expected dry-run plan in stdout, got %s", stdout.String())
	}
	if payload.FugueManifest == nil {
		t.Fatalf("expected fugue_manifest in stdout, got %s", stdout.String())
	}
}
