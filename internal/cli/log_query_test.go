package cli

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRunLogsQueryEmitsStructuredEntriesAndCorrelations(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-20T00:00:00Z","updated_at":"2026-04-20T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-20T00:00:00Z","updated_at":"2026-04-20T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/runtime-logs/stream":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprint(w, strings.Join([]string{
				"event: ready",
				`data: {"cursor":"c0","stream":"runtime","follow":false,"component":"app","namespace":"tenant-123","selector":"app=demo","container":"app"}`,
				"",
				"event: state",
				`data: {"cursor":"c0","component":"app","namespace":"tenant-123","selector":"app=demo","container":"app","pods":["demo-abc"],"follow":false}`,
				"",
				"event: log",
				`data: {"cursor":"c1","source":{"stream":"runtime","namespace":"tenant-123","component":"app","pod":"demo-abc","container":"app"},"timestamp":"2026-04-20T00:00:00Z","line":"{\"event\":\"request.start\",\"request_id\":\"req-123\",\"method\":\"POST\",\"path\":\"/chat\",\"status\":200,\"message\":\"request started\"}"}`,
				"",
				"event: log",
				`data: {"cursor":"c2","source":{"stream":"runtime","namespace":"tenant-123","component":"app","pod":"demo-abc","container":"app"},"timestamp":"2026-04-20T00:00:01Z","line":"time=\"2026-04-20T00:00:01Z\" event=first_body request_id=req-123 method=POST path=/chat status=200 msg=\"first body byte\""}`,
				"",
				"event: log",
				`data: {"cursor":"c3","source":{"stream":"runtime","namespace":"tenant-123","component":"app","pod":"demo-abc","container":"app"},"timestamp":"2026-04-20T00:00:02Z","line":"2026-04-20T00:00:02Z event=request.complete request_id=req-123 method=POST path=/chat status=200 msg=done"}`,
				"",
				"event: end",
				`data: {"cursor":"c4","reason":"snapshot_complete"}`,
				"",
			}, "\n"))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	t.Setenv("FUGUE_BASE_URL", server.URL)
	t.Setenv("FUGUE_API_KEY", "test-token")

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--redact=false",
		"--confirm-raw-output",
		"--json",
		"logs", "query", "demo",
		"--request-id", "req-123",
		"--since", "2026-04-19T23:00:00Z",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run logs query: %v", err)
	}

	out := stdout.String()
	for _, want := range []string{
		`"schema_version": "fugue.logs-query.v1"`,
		`"backend_status": "ok"`,
		`"result_status": "ok"`,
		`"request_id": "req-123"`,
		`"method": "POST"`,
		`"path": "/chat"`,
		`"status": "200"`,
		`"start_at": "2026-04-20T00:00:00Z"`,
		`"first_body_at": "2026-04-20T00:00:01Z"`,
		`"end_at": "2026-04-20T00:00:02Z"`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected logs query output to contain %q, got %q", want, out)
		}
	}
}

func TestRunLogsQueryDistinguishesEmptyResults(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-20T00:00:00Z","updated_at":"2026-04-20T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-20T00:00:00Z","updated_at":"2026-04-20T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/runtime-logs/stream":
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"no matching pods found"}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	t.Setenv("FUGUE_BASE_URL", server.URL)
	t.Setenv("FUGUE_API_KEY", "test-token")

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--redact=false",
		"--confirm-raw-output",
		"--json",
		"logs", "query", "demo",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run logs query empty: %v", err)
	}
	out := stdout.String()
	for _, want := range []string{
		`"backend_status": "ok"`,
		`"result_status": "empty"`,
		`"summary": "no runtime log entries matched because no live pods were available"`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected empty logs query output to contain %q, got %q", want, out)
		}
	}
}

func TestRunLogsQueryDistinguishesBackendUnavailable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-20T00:00:00Z","updated_at":"2026-04-20T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123":
			_, _ = w.Write([]byte(`{"app":{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-20T00:00:00Z","updated_at":"2026-04-20T00:00:00Z"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/runtime-logs/stream":
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"kubernetes service host/port is not available in the environment"}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	t.Setenv("FUGUE_BASE_URL", server.URL)
	t.Setenv("FUGUE_API_KEY", "test-token")

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--redact=false",
		"--confirm-raw-output",
		"--json",
		"logs", "query", "demo",
	}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected logs query to fail when backend is unavailable")
	}
	out := stdout.String()
	for _, want := range []string{
		`"backend_status": "unavailable"`,
		`"result_status": "backend_unavailable"`,
		`"summary": "the runtime log backend is unavailable"`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected backend-unavailable logs query output to contain %q, got %q", want, out)
		}
	}
}

func TestFilterRawLogTextForTimeWindow(t *testing.T) {
	t.Parallel()

	since := time.Date(2026, 4, 20, 0, 0, 1, 0, time.UTC)
	until := time.Date(2026, 4, 20, 0, 0, 2, 0, time.UTC)
	filtered := filterRawLogTextForTimeWindow(strings.Join([]string{
		"2026-04-20T00:00:00Z request start",
		"2026-04-20T00:00:01Z first body",
		"2026-04-20T00:00:02Z request end",
		"2026-04-20T00:00:03Z later noise",
	}, "\n"), logsQueryTimeWindow{Since: &since, Until: &until})
	if strings.Contains(filtered, "request start") || strings.Contains(filtered, "later noise") {
		t.Fatalf("expected out-of-window lines to be removed, got %q", filtered)
	}
	for _, want := range []string{"first body", "request end"} {
		if !strings.Contains(filtered, want) {
			t.Fatalf("expected filtered output to contain %q, got %q", want, filtered)
		}
	}
}
