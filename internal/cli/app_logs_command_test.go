package cli

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRunAppRuntimeLogsGrepFiltersFollowOutput(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-20T00:00:00Z","updated_at":"2026-04-20T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/runtime-logs/stream":
			if got := r.URL.Query().Get("follow"); got != "true" {
				t.Fatalf("expected follow=true, got %q", got)
			}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprint(w, strings.Join([]string{
				"event: ready",
				`data: {"cursor":"c0","stream":"runtime","follow":true,"component":"app","namespace":"tenant-123","selector":"app=demo","container":"app"}`,
				"",
				"event: state",
				`data: {"cursor":"c0","component":"app","namespace":"tenant-123","selector":"app=demo","container":"app","pods":["demo-abc"],"follow":true}`,
				"",
				"event: log",
				`data: {"cursor":"c1","source":{"stream":"runtime","namespace":"tenant-123","component":"app","pod":"demo-abc","container":"app"},"timestamp":"2026-04-20T00:00:00Z","line":"INFO uvicorn access log"}`,
				"",
				"event: log",
				`data: {"cursor":"c2","source":{"stream":"runtime","namespace":"tenant-123","component":"app","pod":"demo-abc","container":"app"},"timestamp":"2026-04-20T00:00:01Z","line":"2026-04-20 00:00:01 - uni-api - INFO - request routed"}`,
				"",
				"event: end",
				`data: {"cursor":"c2","reason":"snapshot_complete"}`,
				"",
			}, "\n"))
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
		"app", "logs", "runtime", "demo",
		"--follow",
		"--grep", "uni-api",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app runtime logs --grep: %v stderr=%s", err, stderr.String())
	}

	out := stdout.String()
	if !strings.Contains(out, "uni-api") {
		t.Fatalf("expected grep output to include matching line, got %q", out)
	}
	if strings.Contains(out, "uvicorn") {
		t.Fatalf("expected grep output to omit non-matching line, got %q", out)
	}
}

func TestRunAppStatusRedactsUnmarkedSeedContentByDefault(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		appJSON := `{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1,"persistent_storage":{"storage_size":"10Gi","mounts":[{"kind":"file","path":"/app/config.yaml","seed_content":"token: seed-secret-123\n","secret":false}]}},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-20T00:00:00Z","updated_at":"2026-04-20T00:00:00Z"}`
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = fmt.Fprintf(w, `{"apps":[%s]}`, appJSON)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = fmt.Fprintf(w, `{"app":%s}`, appJSON)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/operations":
			_, _ = w.Write([]byte(`{"operations":[]}`))
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
		"app", "status", "demo",
		"-o", "json",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app status: %v stderr=%s", err, stderr.String())
	}

	out := stdout.String()
	if !strings.Contains(out, `"seed_content": "[redacted]"`) {
		t.Fatalf("expected seed content to be redacted, got %q", out)
	}
	if strings.Contains(out, "seed-secret-123") {
		t.Fatalf("expected seed content secret to be absent, got %q", out)
	}
}
