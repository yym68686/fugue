package cli

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestListAppsIncludesLiveStatusByDefault(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/apps" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if got := r.URL.Query().Get("include_live_status"); got != "true" {
			t.Fatalf("expected include_live_status=true, got %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"apps":[]}`))
	}))
	defer server.Close()

	client, err := newClientWithOptions(server.URL, "token", clientOptions{
		RequireToken:   true,
		ReadRetryCount: 0,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	if _, err := client.ListApps(); err != nil {
		t.Fatalf("list apps: %v", err)
	}
}
