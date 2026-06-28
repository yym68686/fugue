package observability

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/golang/snappy"
)

func TestNewConfiguredExporterUsesConfiguredExporters(t *testing.T) {
	cfg := Config{
		Enabled:               true,
		MetricsRemoteWriteURL: "https://metrics.example.test/api/v1/write",
		LokiURL:               "https://loki.example.test",
		ClickHouseDSN:         "http://clickhouse.example.test:8123?database=fugue_observability",
		OTLPEndpoint:          "otel.example.test:4317",
	}.Normalize()

	exporters := cfg.Exporters()
	if strings.Join(exporters, ",") != "analytics,logs,metrics" {
		t.Fatalf("expected configured exporters to be active, got %+v", exporters)
	}
	if got := NewConfiguredExporter(cfg, nil).Name(); got != "analytics,logs,metrics" {
		t.Fatalf("unexpected configured exporter name: %s", got)
	}
}

func TestPrometheusRemoteWriteExporterPushesMetricEventsWithAllowedLabels(t *testing.T) {
	var compressed []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/write" {
			t.Fatalf("unexpected remote write path: %s", r.URL.Path)
		}
		if got := r.Header.Get("Content-Type"); got != "application/x-protobuf" {
			t.Fatalf("unexpected content type: %s", got)
		}
		if got := r.Header.Get("Content-Encoding"); got != "snappy" {
			t.Fatalf("unexpected content encoding: %s", got)
		}
		if got := r.Header.Get("X-Prometheus-Remote-Write-Version"); got != "0.1.0" {
			t.Fatalf("unexpected remote write version: %s", got)
		}
		compressed = readAllBytes(t, r)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	exporter := PrometheusRemoteWriteExporter{Client: server.Client(), RemoteWriteURL: server.URL + "/api/v1/write"}
	err := exporter.Export(context.Background(), []Event{
		{
			Timestamp: time.Unix(10, 20).UTC(),
			Kind:      EventKindMetric,
			Source:    "scrape",
			Attributes: map[string]string{
				"metric":       "fugue_telemetry_scrape_samples",
				"sample_count": "2",
				"tenant_id":    "tenant_123",
				"project_id":   "project_123",
				"app_id":       "app_123",
				"runtime_id":   "runtime_123",
				"component":    "runtime",
				"status_class": "2xx",
				"trace_id":     "trace-high-cardinality",
				"user_id":      "user-secret",
			},
		},
		{Timestamp: time.Unix(11, 0).UTC(), Kind: EventKindLog, Message: "ignored by metrics"},
	})
	if err != nil {
		t.Fatalf("export remote write payload: %v", err)
	}
	decoded, err := snappy.Decode(nil, compressed)
	if err != nil {
		t.Fatalf("decode remote write payload: %v", err)
	}
	text := string(decoded)
	for _, want := range []string{"fugue_telemetry_scrape_samples", "tenant_123", "project_123", "app_123", "runtime_123", "runtime", "2xx"} {
		if !strings.Contains(text, want) {
			t.Fatalf("remote write payload missing %q in %q", want, text)
		}
	}
	for _, denied := range []string{"trace-high-cardinality", "user-secret"} {
		if strings.Contains(text, denied) {
			t.Fatalf("remote write payload retained denied label %q in %q", denied, text)
		}
	}
}

func TestPrometheusRemoteWriteExporterDropsDuplicateAndOutOfOrderSamples(t *testing.T) {
	requests := 0
	sampleCounts := []int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		decoded, err := snappy.Decode(nil, readAllBytes(t, r))
		if err != nil {
			t.Fatalf("decode remote write payload: %v", err)
		}
		sampleCounts = append(sampleCounts, strings.Count(string(decoded), "app_request_duration_ms"))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	exporter := PrometheusRemoteWriteExporter{Client: server.Client(), RemoteWriteURL: server.URL}
	base := time.Unix(100, 0).UTC()
	events := []Event{
		prometheusMetricEvent(base.Add(time.Second), "10"),
		prometheusMetricEvent(base.Add(time.Second), "20"),
		prometheusMetricEvent(base, "5"),
		prometheusMetricEvent(base.Add(2*time.Second), "30"),
	}
	if err := exporter.Export(context.Background(), events); err != nil {
		t.Fatalf("export remote write payload: %v", err)
	}
	if requests != 1 || len(sampleCounts) != 1 || sampleCounts[0] != 3 {
		t.Fatalf("expected one request with three deduplicated monotonic samples, requests=%d samples=%+v", requests, sampleCounts)
	}

	if err := exporter.Export(context.Background(), events); err != nil {
		t.Fatalf("export stale remote write payload: %v", err)
	}
	if requests != 1 {
		t.Fatalf("expected stale samples to be skipped without a second request, got %d requests", requests)
	}

	if err := exporter.Export(context.Background(), []Event{prometheusMetricEvent(base.Add(3*time.Second), "40")}); err != nil {
		t.Fatalf("export newer remote write payload: %v", err)
	}
	if requests != 2 || len(sampleCounts) != 2 || sampleCounts[1] != 1 {
		t.Fatalf("expected newer sample to be sent once, requests=%d samples=%+v", requests, sampleCounts)
	}
}

func TestMultiExporterRetriesOnlyFailedExporters(t *testing.T) {
	success := &recordingExporter{name: "analytics"}
	flaky := &recordingExporter{name: "metrics", failures: 1}
	multi := MultiExporter{exporters: []Exporter{success, flaky}}

	if err := multi.ExportWithRetry(context.Background(), []Event{{Kind: EventKindLog}}, 3); err != nil {
		t.Fatalf("export with retry: %v", err)
	}
	if success.calls != 1 {
		t.Fatalf("successful exporter should not be retried, got %d calls", success.calls)
	}
	if flaky.calls != 2 {
		t.Fatalf("failed exporter should be retried until success, got %d calls", flaky.calls)
	}
}

func TestLokiExporterPushesLogEventsWithAllowedLabels(t *testing.T) {
	var payload lokiPushRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/loki/api/v1/push" {
			t.Fatalf("unexpected Loki path: %s", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode Loki payload: %v", err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	exporter := LokiExporter{Client: server.Client(), PushURL: normalizeLokiPushURL(server.URL)}
	err := exporter.Export(context.Background(), []Event{
		{
			Timestamp: time.Unix(10, 20).UTC(),
			Kind:      EventKindLog,
			Source:    "runtime",
			Message:   "request finished",
			Attributes: map[string]string{
				"tenant_id":  "tenant_123",
				"project_id": "project_123",
				"app_id":     "app_123",
				"runtime_id": "runtime_123",
				"namespace":  "fg-tenant",
				"component":  "runtime",
				"level":      "info",
				"trace_id":   "trace-high-cardinality",
			},
		},
		{Timestamp: time.Unix(11, 0).UTC(), Kind: EventKindSpan, Message: "ignored by Loki"},
	})
	if err != nil {
		t.Fatalf("export Loki payload: %v", err)
	}
	if len(payload.Streams) != 1 || len(payload.Streams[0].Values) != 1 {
		t.Fatalf("unexpected Loki streams: %+v", payload)
	}
	labels := payload.Streams[0].Stream
	for _, key := range []string{"tenant_id", "project_id", "app_id", "runtime_id", "namespace", "component", "level"} {
		if labels[key] == "" {
			t.Fatalf("expected Loki label %s in %+v", key, labels)
		}
	}
	if _, ok := labels["trace_id"]; ok {
		t.Fatalf("trace_id must not be a Loki label: %+v", labels)
	}
	if !strings.Contains(payload.Streams[0].Values[0][1], "request finished") {
		t.Fatalf("expected log body in Loki value: %+v", payload.Streams[0].Values[0])
	}
}

func TestClickHouseExporterRoutesStructuredEvents(t *testing.T) {
	type insert struct {
		table string
		body  string
	}
	var inserts []insert
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		body := readAllString(t, r)
		inserts = append(inserts, insert{table: query, body: body})
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	exporter := NewClickHouseExporter(server.URL+"?database=fugue_observability", server.Client())
	err := exporter.Export(context.Background(), []Event{
		{
			Timestamp: time.Unix(100, 0).UTC(),
			Kind:      EventKindLog,
			Message:   "plain stdout should stay out of ClickHouse",
		},
		{
			Timestamp: time.Unix(101, 0).UTC(),
			Kind:      EventKindLog,
			Message:   "request summary",
			Attributes: map[string]string{
				"event_type":       "request_summary",
				"tenant_id":        "tenant_123",
				"project_id":       "project_123",
				"app_id":           "app_123",
				"trace_id":         "trace_123",
				"request_id":       "request_123",
				"status_code":      "200",
				"duration_ms":      "42",
				"model":            "gpt-5.5",
				"provider":         "primary",
				"role":             "reader",
				"summary.category": "demo",
			},
		},
		{
			Timestamp: time.Unix(102, 0).UTC(),
			Kind:      EventKindSpan,
			Message:   "stage finished",
			Attributes: map[string]string{
				"tenant_id":  "tenant_123",
				"project_id": "project_123",
				"app_id":     "app_123",
				"trace_id":   "trace_123",
				"span_id":    "span_123",
				"stage":      "dependency_wait",
				"stage_ms":   "17",
			},
		},
		{
			Timestamp: time.Unix(103, 0).UTC(),
			Kind:      EventKindLog,
			Message:   "deployment finished",
			Attributes: map[string]string{
				"event_type":      "deploy_event",
				"tenant_id":       "tenant_123",
				"project_id":      "project_123",
				"app_id":          "app_123",
				"severity":        "info",
				"attributes_json": `{"phase":"rollout"}`,
			},
		},
		{
			Timestamp: time.Unix(104, 0).UTC(),
			Kind:      EventKindLog,
			Message:   "edge body read is slow",
			Attributes: map[string]string{
				"event_type":      "edge_request_body_buffer_slow",
				"tenant_id":       "tenant_123",
				"app_id":          "app_123",
				"runtime_id":      "runtime_123",
				"severity":        "warning",
				"edge_request_id": "edge_req_123",
				"elapsed_ms":      "30000",
			},
		},
		{
			Timestamp: time.Unix(105, 0).UTC(),
			Kind:      EventKindLog,
			Message:   "edge front tcp connection",
			Attributes: map[string]string{
				"event_type":        "edge_front_tcp_connection",
				"edge_id":           "edge_123",
				"severity":          "info",
				"downstream_remote": "203.0.113.10:45678",
			},
		},
	})
	if err != nil {
		t.Fatalf("export ClickHouse rows: %v", err)
	}
	if len(inserts) != 3 {
		t.Fatalf("expected three ClickHouse inserts, got %+v", inserts)
	}
	joined := inserts[0].table + "\n" + inserts[1].table + "\n" + inserts[2].table + "\n" + inserts[0].body + inserts[1].body + inserts[2].body
	for _, want := range []string{
		"INSERT INTO fugue_observability.request_facts FORMAT JSONEachRow",
		"INSERT INTO fugue_observability.request_spans FORMAT JSONEachRow",
		"INSERT INTO fugue_observability.app_events FORMAT JSONEachRow",
		`"duration_ms":42`,
		`\"category\":\"demo\"`,
		`\"model\":\"gpt-5.5\"`,
		`\"provider\":\"primary\"`,
		`\"role\":\"reader\"`,
		`"stage_ms":17`,
		`"event_type":"deploy_event"`,
		`"event_type":"edge_request_body_buffer_slow"`,
		`"event_type":"edge_front_tcp_connection"`,
		`\"edge_request_id\":\"edge_req_123\"`,
		`\"downstream_remote\":\"203.0.113.10:45678\"`,
		`"attributes_json":"{\"phase\":\"rollout\"}"`,
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("ClickHouse export missing %q in:\n%s", want, joined)
		}
	}
}

func TestParseClickHouseTargetSupportsClickHouseScheme(t *testing.T) {
	target, err := parseClickHouseTarget("clickhouse://user:pass@clickhouse.internal/fugue_observability?secure=true")
	if err != nil {
		t.Fatalf("parse ClickHouse target: %v", err)
	}
	if target.URL.Scheme != "https" || target.URL.Host != "clickhouse.internal:8123" {
		t.Fatalf("unexpected HTTP endpoint: %s", target.URL.String())
	}
	if target.Database != "fugue_observability" || target.Username != "user" || target.Password != "pass" {
		t.Fatalf("unexpected target: %+v", target)
	}
}

func TestClickHouseExporterQueriesJSONEachRow(t *testing.T) {
	var queryText string
	var database string
	var username string
	var password string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queryText = r.URL.Query().Get("query")
		database = r.URL.Query().Get("database")
		username, password, _ = r.BasicAuth()
		_, _ = w.Write([]byte(`{"trace_id":"trace_123","stage":"db","stage_ms":12}` + "\n"))
		_, _ = w.Write([]byte(`{"trace_id":"trace_123","stage":"stream","stage_ms":34}` + "\n"))
	}))
	defer server.Close()

	exporter := NewClickHouseExporter("http://user:pass@"+strings.TrimPrefix(server.URL, "http://")+"?database=fugue_observability", server.Client())
	rows, err := exporter.QueryJSONEachRow(context.Background(), "SELECT * FROM request_spans FORMAT JSONEachRow", DefaultMaxPayloadBytes)
	if err != nil {
		t.Fatalf("query ClickHouse rows: %v", err)
	}
	if queryText != "SELECT * FROM request_spans FORMAT JSONEachRow" {
		t.Fatalf("unexpected ClickHouse query: %q", queryText)
	}
	if database != "fugue_observability" {
		t.Fatalf("unexpected database: %q", database)
	}
	if username != "user" || password != "pass" {
		t.Fatalf("expected basic auth credentials to be forwarded")
	}
	if len(rows) != 2 || rows[0]["stage"] != "db" || rows[1]["stage"] != "stream" {
		t.Fatalf("unexpected rows: %+v", rows)
	}
}

func TestClickHouseExporterDetectsTruncatedJSONEachRowResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"trace_id":"trace_123","stage":"` + strings.Repeat("x", 128) + `"}` + "\n"))
	}))
	defer server.Close()

	exporter := NewClickHouseExporter(server.URL+"?database=fugue_observability", server.Client())
	_, err := exporter.QueryJSONEachRow(context.Background(), "SELECT * FROM request_spans FORMAT JSONEachRow", 64)
	if err == nil {
		t.Fatal("expected max payload error")
	}
	if !strings.Contains(err.Error(), "ClickHouse JSONEachRow response exceeded max payload bytes (64)") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestStructuredLogCanBecomeSpanEvent(t *testing.T) {
	event, redacted := EventFromLogLine("runtime", `{"event_type":"request_span","stage":"db","stage_ms":12,"token":"secret"}`)
	if redacted == 0 {
		t.Fatal("expected secret redaction")
	}
	if event.Kind != EventKindSpan {
		t.Fatalf("expected span kind, got %+v", event)
	}
	if event.Attributes["stage"] != "db" || event.Attributes["stage_ms"] != "12" {
		t.Fatalf("expected structured span attributes, got %+v", event.Attributes)
	}
	if _, ok := event.Attributes["token"]; ok {
		t.Fatalf("secret field was retained: %+v", event.Attributes)
	}
}

func prometheusMetricEvent(timestamp time.Time, value string) Event {
	return Event{
		Timestamp: timestamp,
		Kind:      EventKindMetric,
		Source:    "unit",
		Attributes: map[string]string{
			"metric":       "app_request_duration_ms",
			"value":        value,
			"tenant_id":    "tenant_123",
			"project_id":   "project_123",
			"app_id":       "app_123",
			"runtime_id":   "runtime_123",
			"component":    "api",
			"method":       "POST",
			"route_id":     "/v1/responses",
			"status_class": "2xx",
		},
	}
}

type recordingExporter struct {
	name     string
	failures int
	calls    int
}

func (e *recordingExporter) Name() string {
	return e.name
}

func (e *recordingExporter) Export(context.Context, []Event) error {
	e.calls++
	if e.calls <= e.failures {
		return errors.New("temporary exporter failure")
	}
	return nil
}

func readAllString(t *testing.T, r *http.Request) string {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read request body: %v", err)
	}
	return string(body)
}

func readAllBytes(t *testing.T, r *http.Request) []byte {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read request body: %v", err)
	}
	return body
}

func TestNormalizeLokiPushURLPreservesExplicitPushPath(t *testing.T) {
	raw := "https://logs.example.test/custom/loki/api/v1/push"
	got := normalizeLokiPushURL(raw)
	if _, err := url.Parse(got); err != nil {
		t.Fatalf("normalized Loki URL should parse: %v", err)
	}
	if got != raw {
		t.Fatalf("expected explicit push path to be preserved, got %s", got)
	}
}
