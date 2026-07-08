package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDNSZoneAddWithAccountResolvesWorkspaceTarget(t *testing.T) {
	t.Parallel()

	var createRequest createHostedDNSZoneClientRequest
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
			_, _ = w.Write([]byte(`{"projects":[{"id":"project_target","tenant_id":"tenant_acct","name":"codex2","slug":"codex2","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/dns/zones":
			if err := json.NewDecoder(r.Body).Decode(&createRequest); err != nil {
				t.Fatalf("decode create zone request: %v", err)
			}
			_, _ = w.Write([]byte(`{"zone":{"id":"zone_123","tenant_id":"tenant_acct","project_id":"project_target","zone_name":"example.com","status":"pending_delegation","delegation_status":"pending","expected_nameservers":["ns1.dns.fugue.pro","ns2.dns.fugue.pro"],"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
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
	err := runWithStreams([]string{
		"--base-url", apiServer.URL,
		"--web-base-url", webServer.URL,
		"--token", "admin-token",
		"--account", "user@example.com",
		"--project", "codex2",
		"--json",
		"dns", "zone", "add", "example.com",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run dns zone add: %v", err)
	}

	if createRequest.ZoneName != "example.com" {
		t.Fatalf("expected zone name example.com, got %+v", createRequest)
	}
	if createRequest.TenantID != "tenant_acct" {
		t.Fatalf("expected resolved tenant id, got %+v", createRequest)
	}
	if createRequest.ProjectID != "project_target" {
		t.Fatalf("expected resolved project id, got %+v", createRequest)
	}
}
