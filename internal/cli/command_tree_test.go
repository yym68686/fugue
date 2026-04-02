package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
		"deploy",
		"app",
		"env",
		"domain",
		"workspace",
		"deploy github owner/repo",
		"fugue env list my-app",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected help output to contain %q, got %q", want, out)
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
