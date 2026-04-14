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

	"fugue/internal/model"
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
		"Web Base URL defaults to FUGUE_WEB_BASE_URL, then APP_BASE_URL, then a best-effort guess from the API base URL.",
		"Tenant is auto-selected when your key only sees one tenant.",
		"Deploy and create flows default to the \"default\" project when you do not pass --project.",
		"App and operation JSON output redacts secrets by default. Pass --show-secrets only when you explicitly need raw values.",
		"deploy",
		"app",
		"project",
		"runtime",
		"service",
		"api",
		"diagnose",
		"web",
		"operation",
		"admin",
		"deploy inspect .",
		"deploy github owner/repo",
		"fugue app overview my-app",
		"fugue app source show my-app",
		"fugue app failover policy set my-app --app-to runtime-b",
		"fugue app service attach my-app postgres",
		"fugue app redeploy my-app",
		"fugue app command set my-app --command \"python app.py\"",
		"fugue app config put my-app /app/config.yaml --from-file config.yaml",
		"fugue app storage set my-app --size 10Gi --mount /data",
		"fugue app domain primary set my-app www.example.com",
		"fugue service postgres create app-db --runtime shared",
		"fugue operation ls --app my-app",
		"fugue operation show op_123 --show-secrets",
		"fugue runtime access show shared",
		"fugue runtime doctor shared",
		"fugue project overview",
		"fugue project images usage",
		"fugue admin cluster status",
		"fugue admin cluster pods --namespace kube-system",
		"fugue admin cluster workload show kube-system deployment coredns",
		"fugue admin cluster dns resolve api.github.com --server 10.43.0.10",
		"fugue api request GET /v1/apps",
		"fugue diagnose timing -- app overview my-app",
		"fugue admin users ls",
		"fugue web diagnose admin-users",
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

func TestVisibleCommandsHaveLongAndExamples(t *testing.T) {
	t.Parallel()

	root := newCLI(io.Discard, io.Discard).newRootCommand()
	missing := undocumentedCommandsReport(root)
	if len(missing) != 0 {
		t.Fatalf("expected help docs for all visible commands:\n%s", strings.Join(missing, "\n"))
	}
}

func TestRunDeployImageSupportsIntentFlags(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(configPath, []byte("port: 8080\n"), 0o644); err != nil {
		t.Fatalf("write config file: %v", err)
	}
	secretPath := filepath.Join(t.TempDir(), "app.env")
	if err := os.WriteFile(secretPath, []byte("TOKEN=secret\n"), 0o600); err != nil {
		t.Fatalf("write secret file: %v", err)
	}
	settingsPath := filepath.Join(t.TempDir(), "settings.yaml")
	if err := os.WriteFile(settingsPath, []byte("theme: prod\n"), 0o644); err != nil {
		t.Fatalf("write settings file: %v", err)
	}

	var gotRequest importImageRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/import-image":
			if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
				t.Fatalf("decode image import request: %v", err)
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
		"deploy", "image", "ghcr.io/example/demo:latest",
		"--name", "demo",
		"--background",
		"--command", "python app.py",
		"--file", "/app/config.yaml=" + configPath,
		"--secret-file", "/app/.env:600=" + secretPath,
		"--storage-size", "20Gi",
		"--storage-class", "fast",
		"--mount", "/data",
		"--mount-file", "/app/settings.yaml=" + settingsPath,
		"--managed-postgres",
		"--postgres-database", "appdb",
		"--postgres-user", "app_user",
		"--postgres-password", "secret",
		"--postgres-storage-size", "5Gi",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run deploy image: %v", err)
	}

	if gotRequest.NetworkMode != model.AppNetworkModeBackground {
		t.Fatalf("expected background network mode, got %+v", gotRequest)
	}
	if gotRequest.StartupCommand == nil || *gotRequest.StartupCommand != "python app.py" {
		t.Fatalf("expected startup command to be forwarded, got %+v", gotRequest.StartupCommand)
	}
	if len(gotRequest.Files) != 2 {
		t.Fatalf("expected two declarative files, got %+v", gotRequest.Files)
	}
	fileByPath := map[string]model.AppFile{}
	for _, appFile := range gotRequest.Files {
		fileByPath[appFile.Path] = appFile
	}
	if fileByPath["/app/config.yaml"].Content != "port: 8080\n" || fileByPath["/app/config.yaml"].Secret {
		t.Fatalf("unexpected config file payload %+v", fileByPath["/app/config.yaml"])
	}
	if fileByPath["/app/.env"].Content != "TOKEN=secret\n" || !fileByPath["/app/.env"].Secret || fileByPath["/app/.env"].Mode != 0o600 {
		t.Fatalf("unexpected secret file payload %+v", fileByPath["/app/.env"])
	}
	if gotRequest.PersistentStorage == nil || gotRequest.PersistentStorage.StorageSize != "20Gi" || gotRequest.PersistentStorage.StorageClassName != "fast" {
		t.Fatalf("unexpected persistent storage payload %+v", gotRequest.PersistentStorage)
	}
	if len(gotRequest.PersistentStorage.Mounts) != 2 {
		t.Fatalf("expected two persistent storage mounts, got %+v", gotRequest.PersistentStorage.Mounts)
	}
	mountByPath := map[string]model.AppPersistentStorageMount{}
	for _, mount := range gotRequest.PersistentStorage.Mounts {
		mountByPath[mount.Path] = mount
	}
	if mountByPath["/data"].Kind != model.AppPersistentStorageMountKindDirectory {
		t.Fatalf("expected /data directory mount, got %+v", mountByPath["/data"])
	}
	if mountByPath["/app/settings.yaml"].Kind != model.AppPersistentStorageMountKindFile || mountByPath["/app/settings.yaml"].SeedContent != "theme: prod\n" {
		t.Fatalf("unexpected file mount %+v", mountByPath["/app/settings.yaml"])
	}
	if gotRequest.Postgres == nil || gotRequest.Postgres.Database != "appdb" || gotRequest.Postgres.User != "app_user" || gotRequest.Postgres.Password != "secret" || gotRequest.Postgres.StorageSize != "5Gi" {
		t.Fatalf("unexpected managed postgres payload %+v", gotRequest.Postgres)
	}
	if got := stdout.String(); got != "app_id=app_123\noperation_id=op_123\n" {
		t.Fatalf("unexpected stdout %q", got)
	}
}

func TestRunAppCommandSetUsesStartupCommandPatch(t *testing.T) {
	t.Parallel()

	var gotBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/apps/app_123":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode app patch body: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"},"operation":{"id":"op_123","app_id":"app_123"}}`))
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
		"app", "command", "set", "demo",
		"--command", "python app.py",
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app command set: %v", err)
	}

	if gotBody["startup_command"] != "python app.py" {
		t.Fatalf("expected startup_command patch, got %+v", gotBody)
	}
	out := stdout.String()
	for _, want := range []string{"app_id=app_123", "operation_id=op_123", "startup_command=python app.py"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppStorageSetBuildsPersistentStorageSpec(t *testing.T) {
	t.Parallel()

	settingsPath := filepath.Join(t.TempDir(), "settings.yaml")
	if err := os.WriteFile(settingsPath, []byte("theme: prod\n"), 0o644); err != nil {
		t.Fatalf("write settings file: %v", err)
	}

	var gotBody struct {
		Spec model.AppSpec `json:"spec"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/deploy":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode deploy body: %v", err)
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
		"app", "storage", "set", "demo",
		"--size", "20Gi",
		"--class", "fast",
		"--mount", "/data",
		"--mount-file", "/app/settings.yaml=" + settingsPath,
		"--wait=false",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app storage set: %v", err)
	}

	if gotBody.Spec.Workspace != nil {
		t.Fatalf("expected workspace to be cleared, got %+v", gotBody.Spec.Workspace)
	}
	if gotBody.Spec.PersistentStorage == nil || gotBody.Spec.PersistentStorage.StorageSize != "20Gi" || gotBody.Spec.PersistentStorage.StorageClassName != "fast" {
		t.Fatalf("unexpected persistent storage spec %+v", gotBody.Spec.PersistentStorage)
	}
	if len(gotBody.Spec.PersistentStorage.Mounts) != 2 {
		t.Fatalf("expected two mounts, got %+v", gotBody.Spec.PersistentStorage.Mounts)
	}
	out := stdout.String()
	for _, want := range []string{"app_id=app_123", "operation_id=op_123", "storage_mode=persistent_storage", "storage_size=20Gi"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppDatabaseSwitchoverUsesDatabaseEndpoint(t *testing.T) {
	t.Parallel()

	var gotBody map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_a","replicas":1,"postgres":{"runtime_id":"runtime_a","database":"app","user":"app","service_name":"demo-postgres","failover_target_runtime_id":"runtime_b"}},"status":{"phase":"ready","current_runtime_id":"runtime_a","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_a","replicas":1,"postgres":{"runtime_id":"runtime_a","database":"app","user":"app","service_name":"demo-postgres","failover_target_runtime_id":"runtime_b"}},"status":{"phase":"ready","current_runtime_id":"runtime_a","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_b","tenant_id":"tenant_123","name":"runtime-b","type":"external-owned","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
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
		"app", "db", "switchover", "demo", "runtime-b",
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

func TestRunProjectEditPatchesMetadata(t *testing.T) {
	t.Parallel()

	var gotBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/projects"):
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_123","tenant_id":"tenant_123","name":"demo","slug":"demo","description":"old","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPatch && r.URL.Path == "/v1/projects/project_123":
			if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
				t.Fatalf("decode patch project body: %v", err)
			}
			_, _ = w.Write([]byte(`{"project":{"id":"project_123","tenant_id":"tenant_123","name":"demo-v2","slug":"demo-v2","description":"new description","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-03T00:00:00Z"}}`))
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
		"project", "edit", "demo",
		"--name", "demo-v2",
		"--description", "new description",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run project edit: %v", err)
	}

	if gotBody["name"] != "demo-v2" || gotBody["description"] != "new description" {
		t.Fatalf("unexpected project patch body %+v", gotBody)
	}
	out := stdout.String()
	for _, want := range []string{"project=demo-v2", "tenant=Acme", "description=new description"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
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

func TestRunAppCreateStagesGitHubSource(t *testing.T) {
	t.Parallel()

	var gotRequest createAppRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/projects"):
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_default","tenant_id":"tenant_123","name":"default","slug":"default","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps":
			if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
				t.Fatalf("decode create app request: %v", err)
			}
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_default","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1,"ports":[8080]},"status":{"phase":"importing"},"source":{"type":"github-public","repo_url":"example/demo","repo_branch":"main","build_strategy":"buildpacks"},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
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
		"app", "create", "demo",
		"--github", "example/demo",
		"--branch", "main",
		"--build", "buildpacks",
		"--port", "8080",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app create: %v", err)
	}

	if gotRequest.Name != "demo" || gotRequest.TenantID != "tenant_123" || gotRequest.ProjectID != "" {
		t.Fatalf("unexpected create request %+v", gotRequest)
	}
	if gotRequest.Source == nil || gotRequest.Source.Type != model.AppSourceTypeGitHubPublic || gotRequest.Source.RepoURL != "example/demo" || gotRequest.Source.RepoBranch != "main" {
		t.Fatalf("unexpected staged source %+v", gotRequest.Source)
	}
	if gotRequest.Spec.Ports[0] != 8080 {
		t.Fatalf("expected service port 8080, got %+v", gotRequest.Spec.Ports)
	}
	out := stdout.String()
	for _, want := range []string{"app=demo", "phase=importing", "source=github-public", "next_step=fugue app rebuild demo"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
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
	if !strings.Contains(output, "app=demo") {
		t.Fatalf("expected stdout to contain app name, got %q", output)
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
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_managed_shared","tenant_id":"tenant_123","name":"shared","type":"managed-shared","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
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
	for _, want := range []string{"service=app-db", "project=default", "type=postgres", "runtime=shared"} {
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

func TestRunRuntimeOfferSetPublishesOffer(t *testing.T) {
	t.Parallel()

	var gotRequest setRuntimePublicOfferRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_123","tenant_id":"tenant_123","name":"edge-a","type":"external-owned","access_mode":"public","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes/runtime_123":
			_, _ = w.Write([]byte(`{"runtime":{"id":"runtime_123","tenant_id":"tenant_123","name":"edge-a","type":"external-owned","access_mode":"public","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/runtimes/runtime_123/public-offer":
			if err := json.NewDecoder(r.Body).Decode(&gotRequest); err != nil {
				t.Fatalf("decode runtime offer request: %v", err)
			}
			_, _ = w.Write([]byte(`{"runtime":{"id":"runtime_123","tenant_id":"tenant_123","name":"edge-a","type":"external-owned","access_mode":"public","status":"active","public_offer":{"reference_bundle":{"cpu_millicores":2000,"memory_mebibytes":4096,"storage_gibibytes":50},"reference_monthly_price_microcents":19990000,"free_storage":true,"price_book":{"currency":"USD","hours_per_month":730},"updated_at":"2026-04-02T00:00:00Z"},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
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
		"runtime", "offer", "set", "edge-a",
		"--cpu", "2000",
		"--memory", "4096",
		"--storage", "50",
		"--monthly-usd", "19.99",
		"--free-storage",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run runtime offer set: %v", err)
	}

	if gotRequest.ReferenceBundle.CPUMilliCores != 2000 || gotRequest.ReferenceBundle.MemoryMebibytes != 4096 || gotRequest.ReferenceBundle.StorageGibibytes != 50 {
		t.Fatalf("unexpected reference bundle %+v", gotRequest.ReferenceBundle)
	}
	if gotRequest.ReferenceMonthlyPriceMicroCents != 19_990_000 || !gotRequest.FreeStorage {
		t.Fatalf("unexpected offer request %+v", gotRequest)
	}
	out := stdout.String()
	for _, want := range []string{"runtime=edge-a", "tenant=Acme", "published=true", "reference_monthly_price=USD 19.99", "free_storage=true"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunRuntimeDeleteUsesSemanticCommand(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_123","tenant_id":"tenant_123","name":"edge-a","type":"external-owned","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes/runtime_123":
			_, _ = w.Write([]byte(`{"runtime":{"id":"runtime_123","tenant_id":"tenant_123","name":"edge-a","type":"external-owned","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/runtimes/runtime_123":
			_, _ = w.Write([]byte(`{"deleted":true,"runtime":{"id":"runtime_123","tenant_id":"tenant_123","name":"edge-a","type":"external-owned","status":"deleted","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-03T00:00:00Z"}}`))
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
		"runtime", "delete", "edge-a",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run runtime delete: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{"runtime=edge-a", "tenant=Acme", "deleted=true"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppOverviewAggregatesRelatedState(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_runtime_id":"runtime_managed_shared","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_runtime_id":"runtime_managed_shared","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/projects"):
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_123","tenant_id":"tenant_123","name":"demo","slug":"demo","description":"demo project","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_managed_shared","tenant_id":"tenant_123","name":"shared","type":"managed-shared","status":"active","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/domains":
			_, _ = w.Write([]byte(`{"domains":[{"hostname":"www.example.com","status":"active","tls_status":"ready","route_target":"demo.apps.example.com","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/bindings":
			_, _ = w.Write([]byte(`{"bindings":[{"id":"binding_123","tenant_id":"tenant_123","app_id":"app_123","service_id":"svc_123","alias":"postgres","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}],"backing_services":[{"id":"svc_123","tenant_id":"tenant_123","project_id":"project_123","name":"app-db","type":"postgres","status":"active","spec":{"postgres":{"runtime_id":"runtime_managed_shared","database":"app","user":"app"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations":
			_, _ = w.Write([]byte(`{"operations":[{"id":"op_123","tenant_id":"tenant_123","app_id":"app_123","type":"deploy","status":"completed","execution_mode":"managed","requested_by_type":"api-key","requested_by_id":"key_123","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/images":
			_, _ = w.Write([]byte(`{"app_id":"app_123","registry_configured":true,"summary":{"version_count":1,"current_version_count":1,"stale_version_count":0,"reclaimable_size_bytes":0},"versions":[{"image_ref":"registry.example.com/demo:abc123","status":"ready","current":true,"size_bytes":1048576}]}`))
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
		"app", "overview", "demo",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app overview: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{"app=demo", "project=demo", "runtime=shared", "domains", "www.example.com", "services", "postgres", "images", "versions=1", "operations", "op_123"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
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
	for _, want := range []string{"app=demo", "operation_id=op_123"} {
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
	for _, want := range []string{"app=demo", "operation_id=op_123"} {
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

func TestRunAppFilesystemListFallsBackToLiveFilesystem(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/filesystem/tree":
			if got := r.URL.Query().Get("path"); got != "/" {
				t.Fatalf("expected live filesystem path /, got %q", got)
			}
			if got := r.URL.Query().Get("component"); got != "app" {
				t.Fatalf("expected component=app, got %q", got)
			}
			_, _ = w.Write([]byte(`{"component":"app","pod":"demo-pod","path":"/","depth":1,"workspace_root":"/","entries":[{"name":"tmp","path":"/tmp","kind":"dir","size":0,"mode":493,"modified_at":"2026-04-02T00:00:00Z","has_children":true}]}`))
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
		"app", "fs", "ls", "demo", "/",
		"-o", "json",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app fs ls live: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{`"workspace_root": "/"`, `"/tmp"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected live fs output to contain %q, got %q", want, out)
		}
	}
}

func TestRunAPIRequestShowsRawResponse(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/apps" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer token" {
			t.Fatalf("expected bearer token auth, got %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Server-Timing", "app;dur=12.3")
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte(`{"error":"short and stout"}`))
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"-o", "json",
		"api", "request", "GET", "/v1/apps",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run api request: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{`"status_code": 418`, `"server_timing": "app;dur=12.3"`, `short and stout`} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected api request output to contain %q, got %q", want, out)
		}
	}
}

func TestRunDiagnoseTimingCapturesRequests(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			w.Header().Set("Server-Timing", "apps;dur=4.5")
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","description":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
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
		"diagnose", "timing", "--", "app", "ls",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run diagnose timing: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{`"command": [`, `/v1/apps`, `"status_code": 200`, `"server_timing": "apps;dur=4.5"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected diagnose timing output to contain %q, got %q", want, out)
		}
	}
}

func TestRunAdminUsersListUsesWebSnapshot(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/fugue/admin/pages/users" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer token" {
			t.Fatalf("expected bearer token auth, got %q", got)
		}
		_, _ = w.Write([]byte(`{
			"enrichmentState":"pending",
			"errors":[],
			"summary":{"adminCount":1,"blockedCount":0,"deletedCount":0,"userCount":1},
			"users":[{
				"billing":{"balanceLabel":"","limitLabel":"Loading billing…","loadError":"","loading":true,"monthlyEstimateLabel":"","statusLabel":"","statusReason":""},
				"canBlock":false,
				"canDelete":false,
				"canDemoteAdmin":false,
				"canPromoteToAdmin":false,
				"canUnblock":false,
				"email":"user@example.com",
				"isAdmin":true,
				"lastLoginExact":"2026-04-02T00:00:00Z",
				"lastLoginLabel":"today",
				"name":"User",
				"provider":"GitHub",
				"serviceCount":2,
				"status":"Active",
				"statusTone":"positive",
				"usage":{"cpuLabel":"200m cpu","diskLabel":"1 GiB","imageLabel":"500 MiB","loading":false,"memoryLabel":"512 MiB","serviceCount":2,"serviceCountLabel":"2 services"},
				"verified":true
			}]
		}`))
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--web-base-url", server.URL,
		"--token", "token",
		"-o", "json",
		"admin", "users", "ls",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run admin users ls: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{`"enrichmentState": "pending"`, `"email": "user@example.com"`, `"serviceCount": 2`} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected admin users output to contain %q, got %q", want, out)
		}
	}
}

func TestRunWebDiagnoseUsesAliasTarget(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/fugue/admin/pages/users" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--web-base-url", server.URL,
		"--token", "token",
		"-o", "json",
		"web", "diagnose", "admin-users",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run web diagnose: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{`/api/fugue/admin/pages/users`, `"status_code": 200`, `\"ok\":true`} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected web diagnose output to contain %q, got %q", want, out)
		}
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

func TestRunAppOverviewRedactsSecretsByDefault(t *testing.T) {
	t.Parallel()

	server := newAppOverviewSecretFixtureServer(t)
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"app", "overview", "demo",
		"-o", "json",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app overview: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		`"repo_auth_token": "[redacted]"`,
		`"DB_PASSWORD": "[redacted]"`,
		`"content": "[redacted]"`,
		`"seed_content": "[redacted]"`,
		`"password": "[redacted]"`,
		`"DATABASE_URL": "[redacted]"`,
		`"OP_SECRET": "[redacted]"`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected redacted overview output to contain %q, got %q", want, out)
		}
	}
	for _, secret := range []string{
		"repo-token-123",
		"db-secret-123",
		"TOKEN=runtime-secret",
		"seed-secret-123",
		"service-password-123",
		"postgres://demo:binding-secret-123@db",
		"operation-secret-123",
	} {
		if strings.Contains(out, secret) {
			t.Fatalf("expected overview output to redact %q, got %q", secret, out)
		}
	}
}

func TestRunAppOverviewShowSecretsOptIn(t *testing.T) {
	t.Parallel()

	server := newAppOverviewSecretFixtureServer(t)
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"app", "overview", "demo",
		"--show-secrets",
		"-o", "json",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app overview --show-secrets: %v", err)
	}

	out := stdout.String()
	for _, secret := range []string{
		`"repo_auth_token": "repo-token-123"`,
		`"DB_PASSWORD": "db-secret-123"`,
		`"content": "TOKEN=runtime-secret\n"`,
		`"seed_content": "seed-secret-123"`,
		`"password": "service-password-123"`,
		`"DATABASE_URL": "postgres://demo:binding-secret-123@db"`,
		`"OP_SECRET": "operation-secret-123"`,
	} {
		if !strings.Contains(out, secret) {
			t.Fatalf("expected overview output to contain %q, got %q", secret, out)
		}
	}
}

func TestRunOperationListRedactsSecretsByDefault(t *testing.T) {
	t.Parallel()

	server := newOperationSecretFixtureServer(t)
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"operation", "ls",
		"--app", "demo",
		"-o", "json",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run operation ls: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		`"password": "[redacted]"`,
		`"OP_SECRET": "[redacted]"`,
		`"repo_auth_token": "[redacted]"`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected redacted operation output to contain %q, got %q", want, out)
		}
	}
	for _, secret := range []string{
		"operation-db-password-123",
		"operation-secret-123",
		"operation-repo-token-123",
	} {
		if strings.Contains(out, secret) {
			t.Fatalf("expected operation output to redact %q, got %q", secret, out)
		}
	}
}

func TestRunOperationShowSecretsOptIn(t *testing.T) {
	t.Parallel()

	server := newOperationSecretFixtureServer(t)
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"operation", "show", "op_123",
		"--show-secrets",
		"-o", "json",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run operation show --show-secrets: %v", err)
	}

	out := stdout.String()
	for _, secret := range []string{
		`"password": "operation-db-password-123"`,
		`"OP_SECRET": "operation-secret-123"`,
		`"repo_auth_token": "operation-repo-token-123"`,
	} {
		if !strings.Contains(out, secret) {
			t.Fatalf("expected operation output to contain %q, got %q", secret, out)
		}
	}
}

func TestRunRuntimeDoctorManagedSharedIncludesLocationNodes(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_managed_shared","tenant_id":"tenant_123","name":"shared","type":"managed-shared","status":"active","endpoint":"https://shared.example.com","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runtimes/runtime_managed_shared":
			_, _ = w.Write([]byte(`{"runtime":{"id":"runtime_managed_shared","tenant_id":"tenant_123","name":"shared","type":"managed-shared","status":"active","endpoint":"https://shared.example.com","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/cluster/nodes":
			_, _ = w.Write([]byte(`{"cluster_nodes":[{"name":"gcp3","status":"ready","runtime_id":"runtime_managed_shared_loc_gcp3","conditions":{"Ready":{"status":"True"}},"created_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
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
		"runtime", "doctor", "shared",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run runtime doctor: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{"runtime=shared", "cluster_nodes=1", "gcp3"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAdminClusterStatusShowsDeployWorkflow(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/cluster/control-plane":
			_, _ = w.Write([]byte(`{
				"control_plane":{
					"namespace":"fugue-system",
					"release_instance":"fugue",
					"version":"deadbeef",
					"status":"ready",
					"observed_at":"2026-04-14T00:00:00Z",
					"deploy_workflow":{
						"repository":"acme/fugue",
						"workflow":"deploy-control-plane.yml",
						"status":"completed",
						"conclusion":"success",
						"run_number":42,
						"head_sha":"deadbeef",
						"head_branch":"main",
						"html_url":"https://github.com/acme/fugue/actions/runs/42",
						"observed_at":"2026-04-14T00:00:00Z"
					},
					"components":[]
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
		"admin", "cluster", "status",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run admin cluster status: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		"deploy_workflow_repository=acme/fugue",
		"deploy_workflow=deploy-control-plane.yml",
		"deploy_workflow_status=completed",
		"deploy_workflow_run_number=42",
		"deploy_workflow_head_sha=deadbeef",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func TestRunAdminClusterPodsListsSystemPods(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/cluster/pods":
			if got := r.URL.Query().Get("namespace"); got != "kube-system" {
				t.Fatalf("expected namespace filter kube-system, got %q", got)
			}
			_, _ = w.Write([]byte(`{"cluster_pods":[{"namespace":"kube-system","name":"coredns-abc","phase":"Running","ready":true,"node_name":"gcp1","owner":{"kind":"ReplicaSet","name":"coredns-85f7d9b4"},"containers":[{"name":"coredns","image":"coredns/coredns:v1.11.1","ready":true,"restart_count":1,"state":"running"}]}]}`))
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
		"admin", "cluster", "pods",
		"--namespace", "kube-system",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run admin cluster pods: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{"kube-system", "coredns-abc", "gcp1", "ReplicaSet/coredns-85f7d9b4"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected stdout to contain %q, got %q", want, out)
		}
	}
}

func newAppOverviewSecretFixtureServer(t *testing.T) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","source":{"type":"github-private","repo_url":"https://github.com/acme/demo","repo_auth_token":"repo-token-123"},"spec":{"image":"ghcr.io/acme/demo:latest","runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_runtime_id":"runtime_managed_shared","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","source":{"type":"github-private","repo_url":"https://github.com/acme/demo","repo_auth_token":"repo-token-123"},"spec":{"image":"ghcr.io/acme/demo:latest","runtime_id":"runtime_managed_shared","replicas":1,"env":{"DB_PASSWORD":"db-secret-123"},"files":[{"path":"/app/.env","content":"TOKEN=runtime-secret\n","secret":true}],"persistent_storage":{"storage_size":"10Gi","mounts":[{"kind":"file","path":"/data/seed.txt","seed_content":"seed-secret-123","secret":true}]},"postgres":{"database":"demo","user":"demo","password":"service-password-123"}},"status":{"phase":"ready","current_runtime_id":"runtime_managed_shared","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/domains":
			_, _ = w.Write([]byte(`{"domains":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/bindings":
			_, _ = w.Write([]byte(`{"bindings":[{"id":"binding_123","tenant_id":"tenant_123","app_id":"app_123","service_id":"svc_123","alias":"postgres","env":{"DATABASE_URL":"postgres://demo:binding-secret-123@db"},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}],"backing_services":[{"id":"svc_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo-db","type":"postgres","provisioner":"managed","status":"active","spec":{"postgres":{"runtime_id":"runtime_managed_shared","database":"demo","user":"demo","password":"service-password-123"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations":
			_, _ = w.Write([]byte(`{"operations":[{"id":"op_123","tenant_id":"tenant_123","app_id":"app_123","type":"deploy","status":"completed","execution_mode":"managed","requested_by_type":"api-key","requested_by_id":"key_123","desired_source":{"type":"github-private","repo_url":"https://github.com/acme/demo","repo_auth_token":"operation-repo-token-123"},"desired_spec":{"image":"ghcr.io/acme/demo:next","runtime_id":"runtime_managed_shared","replicas":1,"env":{"OP_SECRET":"operation-secret-123"},"postgres":{"database":"demo","user":"demo","password":"operation-db-password-123"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/images":
			_, _ = w.Write([]byte(`{"app_id":"app_123","registry_configured":true,"summary":{"version_count":1,"current_version_count":1,"stale_version_count":0,"reclaimable_size_bytes":0},"versions":[]}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
}

func newOperationSecretFixtureServer(t *testing.T) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"image":"ghcr.io/acme/demo:latest","runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations":
			if got := r.URL.Query().Get("app_id"); got != "app_123" {
				t.Fatalf("expected app_id filter app_123, got %q", got)
			}
			_, _ = w.Write([]byte(`{"operations":[{"id":"op_123","tenant_id":"tenant_123","app_id":"app_123","type":"deploy","status":"completed","execution_mode":"managed","requested_by_type":"api-key","requested_by_id":"key_123","desired_source":{"type":"github-private","repo_url":"https://github.com/acme/demo","repo_auth_token":"operation-repo-token-123"},"desired_spec":{"image":"ghcr.io/acme/demo:next","runtime_id":"runtime_managed_shared","replicas":1,"env":{"OP_SECRET":"operation-secret-123"},"postgres":{"database":"demo","user":"demo","password":"operation-db-password-123"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations/op_123":
			_, _ = w.Write([]byte(`{"operation":{"id":"op_123","tenant_id":"tenant_123","app_id":"app_123","type":"deploy","status":"completed","execution_mode":"managed","requested_by_type":"api-key","requested_by_id":"key_123","desired_source":{"type":"github-private","repo_url":"https://github.com/acme/demo","repo_auth_token":"operation-repo-token-123"},"desired_spec":{"image":"ghcr.io/acme/demo:next","runtime_id":"runtime_managed_shared","replicas":1,"env":{"OP_SECRET":"operation-secret-123"},"postgres":{"database":"demo","user":"demo","password":"operation-db-password-123"}},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
}
