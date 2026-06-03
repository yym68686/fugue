package cli

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunAppRuntimeLogsGrepFiltersFollowOutput(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			if got := r.URL.Query().Get("q"); got != "demo" {
				t.Fatalf("expected q=demo, got %q", got)
			}
			if got := r.URL.Query().Get("include_live_status"); got != "false" {
				t.Fatalf("expected include_live_status=false, got %q", got)
			}
			if got := r.URL.Query().Get("include_resource_usage"); got != "false" {
				t.Fatalf("expected include_resource_usage=false, got %q", got)
			}
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

func TestRunAppRuntimeLogsFollowReconnectsWithCursor(t *testing.T) {
	t.Parallel()

	var streamRequests atomic.Int32
	var sawResume atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-20T00:00:00Z","updated_at":"2026-04-20T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/runtime-logs/stream":
			if got := r.URL.Query().Get("follow"); got != "true" {
				t.Errorf("expected follow=true, got %q", got)
			}
			w.Header().Set("Content-Type", "text/event-stream")
			switch req := streamRequests.Add(1); req {
			case 1:
				if got := r.Header.Get("Last-Event-ID"); got != "" {
					t.Errorf("expected first request without Last-Event-ID, got %q", got)
				}
				_, _ = fmt.Fprint(w, strings.Join([]string{
					"retry: 1",
					"",
					"id: c0",
					"event: ready",
					`data: {"cursor":"c0","stream":"runtime","follow":true,"component":"app","namespace":"tenant-123","selector":"app=demo","container":"app"}`,
					"",
					"id: c1",
					"event: log",
					`data: {"cursor":"c1","source":{"stream":"runtime","namespace":"tenant-123","component":"app","pod":"demo-abc","container":"app"},"timestamp":"2026-04-20T00:00:00Z","line":"first uni-api line"}`,
					"",
				}, "\n"))
				if flusher, ok := w.(http.Flusher); ok {
					flusher.Flush()
				}
			case 2:
				if got := r.Header.Get("Last-Event-ID"); got != "c1" {
					t.Errorf("expected resume cursor c1, got %q", got)
				}
				sawResume.Store(true)
				_, _ = fmt.Fprint(w, strings.Join([]string{
					"id: c2",
					"event: log",
					`data: {"cursor":"c2","source":{"stream":"runtime","namespace":"tenant-123","component":"app","pod":"demo-abc","container":"app"},"timestamp":"2026-04-20T00:00:01Z","line":"second uni-api line"}`,
					"",
					"id: c2",
					"event: end",
					`data: {"cursor":"c2","reason":"snapshot_complete"}`,
					"",
				}, "\n"))
			default:
				t.Errorf("unexpected stream request #%d", req)
			}
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
		t.Fatalf("run app runtime logs --follow: %v stderr=%s", err, stderr.String())
	}

	out := stdout.String()
	if streamRequests.Load() != 2 {
		t.Fatalf("expected 2 stream requests, got %d output=%q", streamRequests.Load(), out)
	}
	if !sawResume.Load() {
		t.Fatal("expected the second stream request to resume with Last-Event-ID")
	}
	if strings.Count(out, "first uni-api line") != 1 {
		t.Fatalf("expected first line exactly once, got %q", out)
	}
	if strings.Count(out, "second uni-api line") != 1 {
		t.Fatalf("expected second line exactly once, got %q", out)
	}
}

func TestRunAppRuntimeLogsFollowDoesNotBlockOnSlowTextOutput(t *testing.T) {
	t.Parallel()

	const logEvents = runtimeFollowOutputBuffer + 64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-20T00:00:00Z","updated_at":"2026-04-20T00:00:00Z"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/runtime-logs/stream":
			if got := r.URL.Query().Get("follow"); got != "true" {
				t.Errorf("expected follow=true, got %q", got)
			}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprint(w, strings.Join([]string{
				"event: ready",
				`data: {"cursor":"c0","stream":"runtime","follow":true,"component":"app","namespace":"tenant-123","selector":"app=demo","container":"app"}`,
				"",
				"event: state",
				`data: {"cursor":"c0","component":"app","namespace":"tenant-123","selector":"app=demo","container":"app","pods":["demo-abc"],"follow":true}`,
				"",
			}, "\n")+"\n")
			for i := range logEvents {
				_, _ = fmt.Fprintf(w, "event: log\n")
				_, _ = fmt.Fprintf(w, `data: {"cursor":"c%d","source":{"stream":"runtime","namespace":"tenant-123","component":"app","pod":"demo-abc","container":"app"},"timestamp":"2026-04-20T00:00:00Z","line":"uni-api line %d"}`+"\n\n", i+1, i)
			}
			_, _ = fmt.Fprint(w, "event: end\n")
			_, _ = fmt.Fprint(w, `data: {"cursor":"c-end","reason":"snapshot_complete"}`+"\n\n")
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	stdout := newBlockingWriter()
	defer stdout.release()
	var stderr bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- runWithStreams([]string{
			"--base-url", server.URL,
			"--token", "token",
			"app", "logs", "runtime", "demo",
			"--follow",
			"--grep", "uni-api",
		}, stdout, &stderr)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run app runtime logs --follow: %v stderr=%s", err, stderr.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runtime log follow blocked on slow stdout")
	}
	if !strings.Contains(stderr.String(), "dropped_queued_lines=") {
		t.Fatalf("expected slow-consumer warning, got stderr=%q", stderr.String())
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

type blockingWriter struct {
	releaseOnce sync.Once
	released    chan struct{}
}

func newBlockingWriter() *blockingWriter {
	return &blockingWriter{released: make(chan struct{})}
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	<-w.released
	return len(p), nil
}

func (w *blockingWriter) release() {
	w.releaseOnce.Do(func() {
		close(w.released)
	})
}
