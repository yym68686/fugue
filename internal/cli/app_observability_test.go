package cli

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRunAppMetricsUsesObservabilityEndpoint(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			writeObservabilityTestApp(w)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/observability/metrics/summary":
			if got := r.URL.Query().Get("since"); got != "30m" {
				t.Fatalf("expected since=30m, got %q", got)
			}
			writeDisabledObservabilityResponse(w, `{"metrics":[]}`)
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
		"app", "metrics", "demo",
		"--since", "30m",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app metrics: %v stderr=%s", err, stderr.String())
	}
	out := stdout.String()
	for _, want := range []string{"observability_status=disabled", "reason=observability is disabled", "metrics=0"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppMetricsQueryUsesObservabilityEndpoint(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			writeObservabilityTestApp(w)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/observability/metrics/query":
			if got := r.URL.Query().Get("since"); got != "5m" {
				t.Fatalf("expected since=5m, got %q", got)
			}
			if got := r.URL.Query().Get("query"); got != "p95 latency" {
				t.Fatalf("expected query=p95 latency, got %q", got)
			}
			writeDisabledObservabilityResponse(w, `{"query":"p95 latency","metrics":[]}`)
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
		"app", "metrics", "demo",
		"--since", "5m",
		"--query", "p95 latency",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app metrics --query: %v stderr=%s", err, stderr.String())
	}
	out := stdout.String()
	for _, want := range []string{"observability_status=disabled", "query=p95 latency", "metrics=0"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got %q", want, out)
		}
	}
}

func TestRunAppLogsQueryUsesObservabilityEndpoint(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			writeObservabilityTestApp(w)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/observability/logs/query":
			if got := r.URL.Query().Get("grep"); got != "timeout" {
				t.Fatalf("expected grep=timeout, got %q", got)
			}
			if got := r.URL.Query().Get("level"); got != "error" {
				t.Fatalf("expected level=error, got %q", got)
			}
			if got := r.URL.Query().Get("trace_id"); got != "trace_123" {
				t.Fatalf("expected trace_id=trace_123, got %q", got)
			}
			writeDisabledObservabilityResponse(w, `{"logs":[]}`)
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
		"app", "logs", "query", "demo",
		"--grep", "timeout",
		"--level", "error",
		"--trace", "trace_123",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app logs query: %v stderr=%s", err, stderr.String())
	}
	if out := stdout.String(); !strings.Contains(out, "logs=0") || !strings.Contains(out, "observability_status=disabled") {
		t.Fatalf("expected disabled observability log output, got %q", out)
	}
}

func TestRunAppLogsQueryKeepsBusinessTableCompatibility(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			writeObservabilityTestApp(w)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/apps/app_123/database/query":
			_, _ = w.Write([]byte(`{"database":"app","host":"postgres","user":"app","columns":[{"name":"status","database_type":"text"}],"rows":[{"status":"500"}],"row_count":1,"max_rows":200,"truncated":false,"read_only":true,"duration_ms":3}`))
		case strings.Contains(r.URL.Path, "/observability/"):
			t.Fatalf("expected app logs query --table to use business database query, got %s", r.URL.Path)
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
		"app", "logs", "query", "demo",
		"--table", "gateway_request_logs",
		"--match", "status=500",
		"--since", "1h",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run compatible app logs query --table: %v stderr=%s", err, stderr.String())
	}
	if !strings.Contains(stdout.String(), "500") {
		t.Fatalf("expected business table query output, got %q", stdout.String())
	}
}

func TestRunAppRequestsAndTracesUseObservabilityEndpoints(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			writeObservabilityTestApp(w)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/observability/requests":
			if got := r.URL.Query().Get("slow"); got != "true" {
				t.Fatalf("expected slow=true, got %q", got)
			}
			if got := r.URL.Query().Get("errors"); got != "true" {
				t.Fatalf("expected errors=true, got %q", got)
			}
			writeDisabledObservabilityResponse(w, `{"requests":[]}`)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/observability/traces/trace_123":
			writeDisabledObservabilityResponse(w, `{"trace_id":"trace_123","spans":[]}`)
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	var requestsOut bytes.Buffer
	var requestsErr bytes.Buffer
	if err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"app", "requests", "demo",
		"--slow",
		"--errors",
	}, &requestsOut, &requestsErr); err != nil {
		t.Fatalf("run app requests: %v stderr=%s", err, requestsErr.String())
	}
	if !strings.Contains(requestsOut.String(), "requests=0") {
		t.Fatalf("expected request summary output, got %q", requestsOut.String())
	}

	var tracesOut bytes.Buffer
	var tracesErr bytes.Buffer
	if err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"app", "traces", "demo", "trace_123",
	}, &tracesOut, &tracesErr); err != nil {
		t.Fatalf("run app traces: %v stderr=%s", err, tracesErr.String())
	}
	if out := tracesOut.String(); !strings.Contains(out, "trace_id=trace_123") || !strings.Contains(out, "spans=0") {
		t.Fatalf("expected trace output, got %q", out)
	}
}

func TestRunAppRequestsFollowUsesObservabilityStream(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			writeObservabilityTestApp(w)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/observability/requests/stream":
			if got := r.URL.Query().Get("follow"); got != "true" {
				t.Fatalf("expected follow=true, got %q", got)
			}
			if got := r.URL.Query().Get("limit"); got != "200" {
				t.Fatalf("expected limit=200, got %q", got)
			}
			w.Header().Set("Content-Type", "text/event-stream")
			events := []string{
				`id: c0`,
				`event: ready`,
				`data: {"cursor":"c0","source":{"available":true,"status":"available","mode":"instrumented","retention":"24h0m0s","active_exporters":["analytics"],"reason":"request analytics stream is available"},"window":{"since":"2026-04-20T00:00:00Z","until":"2026-04-20T00:01:00Z"},"follow":true}`,
				``,
				`id: c1`,
				`event: request`,
				`data: {"cursor":"c1","request":{"timestamp":"2026-04-20T00:00:01Z","status_code":503,"duration_ms":1200,"summary":{"stage":"retry"},"route":"/v1/items"}}`,
				``,
				`id: c2`,
				`event: end`,
				`data: {"cursor":"c2","reason":"snapshot complete"}`,
				``,
			}
			_, _ = w.Write([]byte(strings.Join(events, "\n")))
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
		"app", "requests", "demo",
		"--follow",
		"--fields", "timestamp,status,duration,summary.stage",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app requests --follow: %v stderr=%s", err, stderr.String())
	}
	out := stdout.String()
	for _, want := range []string{"TIMESTAMP\tSTATUS_CODE\tDURATION_MS\tSUMMARY_STAGE", "2026-04-20T00:00:01Z\t503\t1200\tretry"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got %q", want, out)
		}
	}
	if !strings.Contains(stderr.String(), "observability_status=available") {
		t.Fatalf("expected status on stderr, got %q", stderr.String())
	}
}

func TestRunAppDiagnoseWindowUsesObservabilityEndpoint(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			writeObservabilityTestApp(w)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps/app_123/observability/diagnosis":
			if got := r.URL.Query().Get("since"); got != "5m" {
				t.Fatalf("expected since=5m, got %q", got)
			}
			writeDisabledObservabilityResponse(w, `{"diagnosis":{"bottleneck":"unavailable","confidence":0,"evidence":["observability is disabled"],"next_actions":["enable observability"]}}`)
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
		"app", "diagnose", "demo",
		"--window", "5m",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run app diagnose --window: %v stderr=%s", err, stderr.String())
	}
	out := stdout.String()
	for _, want := range []string{"bottleneck=unavailable", "evidence=observability is disabled", "next_action=enable observability"} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got %q", want, out)
		}
	}
}

func writeObservabilityTestApp(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo","spec":{"runtime_id":"runtime_managed_shared","replicas":1},"status":{"phase":"ready","current_replicas":1},"created_at":"2026-04-20T00:00:00Z","updated_at":"2026-04-20T00:00:00Z"}]}`))
}

func writeDisabledObservabilityResponse(w http.ResponseWriter, bodyFields string) {
	w.Header().Set("Content-Type", "application/json")
	fields := strings.TrimSpace(bodyFields)
	fields = strings.TrimPrefix(fields, "{")
	fields = strings.TrimSuffix(fields, "}")
	_, _ = fmt.Fprintf(w, `{"source":{"available":false,"status":"disabled","mode":"disabled","retention":"24h0m0s","active_exporters":[],"reason":"observability is disabled"},"window":{"since":"2026-04-20T00:00:00Z","until":"2026-04-20T01:00:00Z"},%s}`, fields)
}
