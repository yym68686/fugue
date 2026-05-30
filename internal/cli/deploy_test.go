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

func TestResolveTenantSelectionMultipleTenantsSuggestsAccount(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/tenants" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_a","name":"Acme","slug":"acme"},{"id":"tenant_b","name":"Beta","slug":"beta"}]}`))
	}))
	defer server.Close()

	client, err := NewClient(server.URL, "token")
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	_, err = resolveTenantSelection(client, "", "")
	if err == nil {
		t.Fatalf("expected multiple tenant error")
	}
	for _, want := range []string{"multiple tenants are visible", "--account <email>", "--tenant <name>", "--tenant-id <id>"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected error %q to contain %q", err.Error(), want)
		}
	}
}

func TestResolveAppReferenceFallsBackToSingleAppInMatchedProject(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			if got := r.URL.Query().Get("include_live_status"); got != "false" {
				t.Fatalf("expected include_live_status=false, got %q", got)
			}
			if got := r.URL.Query().Get("include_resource_usage"); got != "false" {
				t.Fatalf("expected include_resource_usage=false, got %q", got)
			}
			switch {
			case r.URL.Query().Get("q") == "uni-api-web":
				_, _ = w.Write([]byte(`{"apps":[]}`))
			case r.URL.Query().Get("project_id") == "project_123":
				_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"api","spec":{"runtime_id":"runtime_123","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
			default:
				t.Fatalf("unexpected app list query %s", r.URL.RawQuery)
			}
		case r.Method == http.MethodGet && r.URL.Path == "/v1/projects":
			if got := r.URL.Query().Get("tenant_id"); got != "" {
				t.Fatalf("expected cross-tenant project lookup, got %q", got)
			}
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_123","tenant_id":"tenant_123","name":"uni-api-web","slug":"uni-api-web","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	client, err := NewClient(server.URL, "token")
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	app, err := resolveAppReference(client, "uni-api-web", "", "")
	if err != nil {
		t.Fatalf("resolve app: %v", err)
	}
	if app.ID != "app_123" {
		t.Fatalf("expected app_123, got %+v", app)
	}
}

func TestResolveAppReferenceUsesSlugQueryFallback(t *testing.T) {
	t.Parallel()

	var appListQueries []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			if got := r.URL.Query().Get("include_live_status"); got != "false" {
				t.Fatalf("expected include_live_status=false, got %q", got)
			}
			if got := r.URL.Query().Get("include_resource_usage"); got != "false" {
				t.Fatalf("expected include_resource_usage=false, got %q", got)
			}
			query := r.URL.Query().Get("q")
			appListQueries = append(appListQueries, query)
			if query == "My App" {
				_, _ = w.Write([]byte(`{"apps":[]}`))
				return
			}
			if query != "my-app" {
				t.Fatalf("unexpected app query %q", query)
			}
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"my-app","spec":{"runtime_id":"runtime_123","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	client, err := NewClient(server.URL, "token")
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	app, err := resolveAppReference(client, "My App", "", "")
	if err != nil {
		t.Fatalf("resolve app: %v", err)
	}
	if app.ID != "app_123" {
		t.Fatalf("expected app_123, got %+v", app)
	}
	if got := strings.Join(appListQueries, ","); got != "My App,my-app" {
		t.Fatalf("expected ref and slug queries, got %q", got)
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

func TestRunDeployLocalWithAccountResolvesWorkspaceThroughWeb(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeTestFile(t, filepath.Join(dir, "Dockerfile"), "FROM scratch\n")

	var importRequest importUploadRequest
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer admin-token" {
			t.Fatalf("unexpected api auth header %q", got)
		}
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/auth/context":
			_, _ = w.Write([]byte(`{"principal":{"actor_type":"bootstrap","actor_id":"bootstrap","scopes":["platform.admin"],"platform_admin":true}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/projects":
			if got := r.URL.Query().Get("tenant_id"); got != "tenant_acct" {
				t.Fatalf("expected tenant_acct project lookup, got %q", got)
			}
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_target","tenant_id":"tenant_acct","name":"uni-api-web","slug":"uni-api-web"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/templates/inspect-upload":
			if err := r.ParseMultipartForm(32 << 20); err != nil {
				t.Fatalf("parse inspect multipart form: %v", err)
			}
			_, _ = w.Write([]byte(`{"upload":{"default_app_name":"demo","source_kind":"archive","source_path":"."}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/import-upload":
			if err := r.ParseMultipartForm(32 << 20); err != nil {
				t.Fatalf("parse import multipart form: %v", err)
			}
			if err := json.Unmarshal([]byte(r.FormValue("request")), &importRequest); err != nil {
				t.Fatalf("decode import request: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"app":{"id":"app_created","name":"demo"},"operation":{"id":"op_created","app_id":"app_created"}}`))
		default:
			t.Fatalf("unexpected api request %s %s", r.Method, r.URL.String())
		}
	}))
	defer apiServer.Close()

	webServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/admin/workspaces/resolve" {
			t.Fatalf("unexpected web request %s %s", r.Method, r.URL.String())
		}
		if got := r.Header.Get("Authorization"); got != "Bearer admin-token" {
			t.Fatalf("unexpected web auth header %q", got)
		}
		if got := r.URL.Query().Get("email"); got != "user@example.com" {
			t.Fatalf("unexpected account query %q", got)
		}
		_, _ = w.Write([]byte(`{"email":"user@example.com","workspace":{"tenantId":"tenant_acct","tenantName":"User workspace","defaultProjectId":"project_default","defaultProjectName":"default"}}`))
	}))
	defer webServer.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runDeployWithStreams([]string{
		"--base-url", apiServer.URL,
		"--web-base-url", webServer.URL,
		"--token", "admin-token",
		"--account", "user@example.com",
		"--project", "uni-api-web",
		"--dir", dir,
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run account deploy: %v", err)
	}

	if importRequest.TenantID != "tenant_acct" {
		t.Fatalf("expected tenant from workspace resolver, got %+v", importRequest)
	}
	if importRequest.ProjectID != "project_target" {
		t.Fatalf("expected resolved project id, got %+v", importRequest)
	}
	if importRequest.Project != nil {
		t.Fatalf("expected no project creation request, got %+v", importRequest.Project)
	}
	if strings.Contains(string(mustMarshalJSON(t, importRequest)), "user@example.com") {
		t.Fatalf("account email leaked into control-plane request: %+v", importRequest)
	}
}

func TestRunDeployLocalWithAccountRequiresAdminKey(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeTestFile(t, filepath.Join(dir, "Dockerfile"), "FROM scratch\n")

	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/auth/context":
			_, _ = w.Write([]byte(`{"principal":{"actor_type":"tenant_key","actor_id":"key_123","tenant_id":"tenant_user","scopes":["tenant.deploy"],"platform_admin":false}}`))
		default:
			t.Fatalf("unexpected api request %s %s", r.Method, r.URL.String())
		}
	}))
	defer apiServer.Close()

	webServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("web resolver should not be called for non-admin key")
	}))
	defer webServer.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runDeployWithStreams([]string{
		"--base-url", apiServer.URL,
		"--web-base-url", webServer.URL,
		"--token", "user-token",
		"--account", "user@example.com",
		"--dir", dir,
		"--wait=false",
	}, &stdout, &stderr)
	if err == nil {
		t.Fatalf("expected non-admin account error")
	}
	if !strings.Contains(err.Error(), "--account requires a platform-admin or bootstrap key") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunDeployLocalOrdinaryUserAutoSelectsSingleWorkspace(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeTestFile(t, filepath.Join(dir, "Dockerfile"), "FROM scratch\n")

	var importRequest importUploadRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_user","name":"User workspace","slug":"user-workspace"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/projects":
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_default","tenant_id":"tenant_user","name":"default","slug":"default"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/templates/inspect-upload":
			if err := r.ParseMultipartForm(32 << 20); err != nil {
				t.Fatalf("parse inspect multipart form: %v", err)
			}
			_, _ = w.Write([]byte(`{"upload":{"default_app_name":"demo","source_kind":"archive","source_path":"."}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/import-upload":
			if err := r.ParseMultipartForm(32 << 20); err != nil {
				t.Fatalf("parse import multipart form: %v", err)
			}
			if err := json.Unmarshal([]byte(r.FormValue("request")), &importRequest); err != nil {
				t.Fatalf("decode import request: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"app":{"id":"app_user","name":"demo"},"operation":{"id":"op_user","app_id":"app_user"}}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runDeployWithStreams([]string{
		"--base-url", server.URL,
		"--token", "user-token",
		"--dir", dir,
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run ordinary deploy: %v", err)
	}
	if importRequest.TenantID != "tenant_user" {
		t.Fatalf("expected auto-selected tenant_user, got %+v", importRequest)
	}
}

func TestValidateLocalDeployPreflightRejectsIgnoredDockerfile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeTestFile(t, filepath.Join(dir, "Dockerfile"), "FROM scratch\n")
	writeTestFile(t, filepath.Join(dir, ".dockerignore"), "Dockerfile\n")

	err := validateLocalDeployPreflight(dir, deployCommonOptions{BuildStrategy: "dockerfile"})
	if err == nil {
		t.Fatalf("expected preflight error")
	}
	for _, want := range []string{"deploy preflight failed", "dockerfile_path", "excluded from the upload archive"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected error %q to contain %q", err.Error(), want)
		}
	}
}

func mustMarshalJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	return raw
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
