package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProjectCreateGitHubPreservesAccountAndExplicitRuntime(t *testing.T) {
	t.Setenv("FUGUE_SKIP_UPDATE_CHECK", "1")
	t.Setenv("FUGUE_GITHUB_TOKEN", "test-repository-token")
	posts := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/auth/context":
			_, _ = w.Write([]byte(`{"principal":{"platform_admin":true}}`))
		case "/api/admin/workspaces/resolve":
			if r.URL.Query().Get("email") != "owner@example.com" {
				t.Error("account lost")
			}
			_, _ = w.Write([]byte(`{"email":"owner@example.com","workspace":{"tenantId":"tenant_example","defaultProjectId":"project_existing"}}`))
		case "/v1/runtimes":
			_, _ = w.Write([]byte(`{"runtimes":[{"id":"runtime_example","name":"example-node","type":"managed-owned","status":"active"}]}`))
		case "/v1/apps/import-github":
			posts++
			var req importGitHubRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatal(err)
			}
			if req.TenantID != "tenant_example" || req.RuntimeID != "runtime_example" || req.Project == nil || req.Project.Name != "example-project" || req.ProjectID != "" || req.RepoAuthToken != "test-repository-token" || req.RepoVisibility != "private" {
				t.Errorf("account, new project, runtime or private credentials not forwarded")
			}
			_, _ = w.Write([]byte(`{"apps":[],"operations":[]}`))
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer backend.Close()
	var out, errs bytes.Buffer
	err := runWithStreams([]string{"--base-url", backend.URL, "--web-base-url", backend.URL, "--token", "test-admin-token", "--account", "owner@example.com", "--json", "project", "create", "example-project", "--github", "example/source", "--private", "--branch", "main", "--default-runtime", "example-node", "--wait=false"}, &out, &errs)
	if err != nil {
		t.Fatalf("create: %v stdout=%s stderr=%s", err, &out, &errs)
	}
	if posts != 1 {
		t.Fatalf("expected one import submission, got %d", posts)
	}
}
