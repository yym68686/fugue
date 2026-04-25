package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRunAppStatusJSONIncludesActiveOperations(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations" && r.URL.Query().Get("app_id") == "app_123":
			_, _ = w.Write([]byte(`{"operations":[{"id":"op_running","tenant_id":"tenant_123","app_id":"app_123","type":"deploy","status":"running","execution_mode":"managed","requested_by_type":"api-key","requested_by_id":"key_123","created_at":"2026-04-02T00:02:00Z","updated_at":"2026-04-02T00:02:00Z"},{"id":"op_done","tenant_id":"tenant_123","app_id":"app_123","type":"deploy","status":"completed","execution_mode":"managed","requested_by_type":"api-key","requested_by_id":"key_123","created_at":"2026-04-02T00:00:00Z","updated_at":"2026-04-02T00:01:00Z"}]}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{"--base-url", server.URL, "--token", "token", "--json", "app", "status", "demo"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app status: %v stderr=%s", err, stderr.String())
	}

	var payload struct {
		ActiveOperations []struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"active_operations"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &payload); err != nil {
		t.Fatalf("decode stdout: %v\n%s", err, stdout.String())
	}
	if len(payload.ActiveOperations) != 1 || payload.ActiveOperations[0].ID != "op_running" || payload.ActiveOperations[0].Status != "running" {
		t.Fatalf("unexpected active operations %+v", payload.ActiveOperations)
	}
}
