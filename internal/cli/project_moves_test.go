package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProjectSplitAppsToShorthandBuildsStructuredTargets(t *testing.T) {
	t.Parallel()

	var splitRequest projectSplitRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tenants":
			_, _ = w.Write([]byte(`{"tenants":[{"id":"tenant_123","name":"Acme","slug":"acme"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/projects":
			if got := r.URL.Query().Get("tenant_id"); got != "tenant_123" {
				t.Fatalf("expected tenant_123 project lookup, got %q", got)
			}
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_source","tenant_id":"tenant_123","name":"default","slug":"default"},{"id":"project_backend","tenant_id":"tenant_123","name":"backend","slug":"backend"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/projects/project_source/split":
			if err := json.NewDecoder(r.Body).Decode(&splitRequest); err != nil {
				t.Fatalf("decode split request: %v", err)
			}
			_, _ = w.Write([]byte(`{"plan":{"dry_run":false,"source_project":{"id":"project_source","tenant_id":"tenant_123","name":"default","slug":"default"},"target_projects":[{"id":"project_backend","tenant_id":"tenant_123","name":"backend","slug":"backend"}],"apps":[],"backing_services":[],"bindings":[],"warnings":[],"blockers":[]}}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"--json",
		"project", "split", "default",
		"--apps", "api, worker",
		"--to", "backend",
		"--confirm",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("project split: %v", err)
	}

	if len(splitRequest.Targets) != 2 {
		t.Fatalf("expected two split targets, got %+v", splitRequest)
	}
	for _, target := range splitRequest.Targets {
		if target.TargetProjectID != "project_backend" {
			t.Fatalf("expected target project id, got %+v", splitRequest.Targets)
		}
		if strings.TrimSpace(target.TargetProjectName) != "" {
			t.Fatalf("expected resolved project id without name, got %+v", splitRequest.Targets)
		}
	}
	if splitRequest.Targets[0].AppName != "api" || splitRequest.Targets[1].AppName != "worker" {
		t.Fatalf("unexpected app targets %+v", splitRequest.Targets)
	}
	if !splitRequest.IncludeOwnedServices {
		t.Fatalf("expected include_owned_services default true")
	}
}
