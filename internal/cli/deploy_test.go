package cli

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

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
