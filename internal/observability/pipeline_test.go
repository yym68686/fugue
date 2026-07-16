package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPipelineInjectsIdentityAndDropsMetricSecrets(t *testing.T) {
	pipeline := NewPipeline(Config{
		Enabled:          true,
		QueueSize:        2,
		MemoryLimitBytes: 4096,
		Identity: Identity{
			TenantID:  "tenant_123",
			ProjectID: "project_123",
			AppID:     "app_123",
			RuntimeID: "runtime_123",
			Component: "runtime",
		},
	}, nil)

	if !pipeline.Ingest(context.Background(), Event{
		Kind:    EventKindMetric,
		Source:  "unit",
		Message: "authorization=secret",
		Attributes: map[string]string{
			"trace_id":      "trace-123",
			"status_class":  "2xx",
			"access_token":  "secret",
			"custom_metric": "value",
		},
	}) {
		t.Fatal("expected event to be queued")
	}
	event := <-pipeline.queue
	if event.Message != "authorization=[REDACTED]" {
		t.Fatalf("message was not redacted: %q", event.Message)
	}
	for _, key := range []string{"trace_id", "access_token"} {
		if _, ok := event.Attributes[key]; ok {
			t.Fatalf("expected metric attribute %q to be dropped: %+v", key, event.Attributes)
		}
	}
	for key, want := range map[string]string{
		"tenant_id":    "tenant_123",
		"project_id":   "project_123",
		"app_id":       "app_123",
		"runtime_id":   "runtime_123",
		"component":    "runtime",
		"status_class": "2xx",
	} {
		if got := event.Attributes[key]; got != want {
			t.Fatalf("expected %s=%q, got %q in %+v", key, want, got, event.Attributes)
		}
	}
	if pipeline.Snapshot().Redacted == 0 {
		t.Fatal("expected redaction counter to increase")
	}
}

func TestPipelineIdentityDoesNotOverrideEventResourceAttributes(t *testing.T) {
	pipeline := NewPipeline(Config{
		Enabled:          true,
		QueueSize:        2,
		MemoryLimitBytes: 4096,
		Identity: Identity{
			TenantID:  "tenant_agent",
			ProjectID: "project_agent",
			AppID:     "app_agent",
			RuntimeID: "runtime_agent",
			Component: "telemetry-agent",
		},
	}, nil)

	if !pipeline.Ingest(context.Background(), Event{
		Kind:   EventKindLog,
		Source: "kubernetes://fg-tenant/app-pod/app",
		Attributes: map[string]string{
			"tenant_id":  "tenant_app",
			"project_id": "project_app",
			"app_id":     "app_app",
			"runtime_id": "runtime_app",
			"component":  "app-container",
		},
	}) {
		t.Fatal("expected event to be queued")
	}

	event := <-pipeline.queue
	for key, want := range map[string]string{
		"tenant_id":  "tenant_app",
		"project_id": "project_app",
		"app_id":     "app_app",
		"runtime_id": "runtime_app",
		"component":  "app-container",
	} {
		if got := event.Attributes[key]; got != want {
			t.Fatalf("expected %s=%q, got %q in %+v", key, want, got, event.Attributes)
		}
	}
}

func TestPipelineQueueAndMemoryLimitDropsWithoutBlocking(t *testing.T) {
	pipeline := NewPipeline(Config{
		Enabled:          true,
		QueueSize:        1,
		MemoryLimitBytes: 120,
	}, nil)
	ok := pipeline.Ingest(context.Background(), Event{Kind: EventKindLog, Source: "unit", Message: strings.Repeat("a", 20)})
	if !ok {
		t.Fatal("expected first event to be queued")
	}
	ok = pipeline.Ingest(context.Background(), Event{Kind: EventKindLog, Source: "unit", Message: strings.Repeat("b", 200)})
	if ok {
		t.Fatal("expected oversized event to be dropped")
	}
	snap := pipeline.Snapshot()
	if snap.Dropped == 0 || snap.QueueDepth != 1 {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}
}

func TestPipelineEnforcesTenantTelemetryQuota(t *testing.T) {
	pipeline := NewPipeline(Config{
		Enabled:                   true,
		QueueSize:                 4,
		MemoryLimitBytes:          4096,
		TenantEventQuotaPerMinute: 1,
	}, nil)
	event := Event{
		Kind:   EventKindLog,
		Source: "unit",
		Attributes: map[string]string{
			"tenant_id": "tenant_123",
			"app_id":    "app_123",
		},
	}
	if !pipeline.Ingest(context.Background(), event) {
		t.Fatal("expected first tenant event to be queued")
	}
	if pipeline.Ingest(context.Background(), event) {
		t.Fatal("expected second tenant event to be quota dropped")
	}
	snap := pipeline.Snapshot()
	if snap.QueueDepth != 1 || snap.QuotaDropped != 1 || snap.Dropped != 1 {
		t.Fatalf("unexpected quota snapshot: %+v", snap)
	}
}

func TestPipelineEnforcesAppTelemetryQuota(t *testing.T) {
	pipeline := NewPipeline(Config{
		Enabled:                true,
		QueueSize:              4,
		MemoryLimitBytes:       4096,
		AppEventQuotaPerMinute: 1,
	}, nil)
	event := Event{
		Kind:   EventKindLog,
		Source: "unit",
		Attributes: map[string]string{
			"tenant_id": "tenant_123",
			"app_id":    "app_123",
		},
	}
	if !pipeline.Ingest(context.Background(), event) {
		t.Fatal("expected first app event to be queued")
	}
	if pipeline.Ingest(context.Background(), event) {
		t.Fatal("expected second app event to be quota dropped")
	}
	metrics := pipeline.PrometheusMetrics()
	for _, want := range []string{
		`fugue_telemetry_pipeline_events_total{outcome="quota_dropped"} 1`,
		`fugue_telemetry_tenant_events_total{tenant_id="tenant_123",outcome="received"} 1`,
		`fugue_telemetry_tenant_events_total{tenant_id="tenant_123",outcome="dropped"} 1`,
		`fugue_telemetry_tenant_events_total{tenant_id="tenant_123",outcome="quota_dropped"} 1`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("expected quota meter %q, got:\n%s", want, metrics)
		}
	}
}

func TestOTLPHTTPReceiverAcceptsJSONWithoutStoringPayload(t *testing.T) {
	pipeline := NewPipeline(Config{
		Enabled:          true,
		QueueSize:        4,
		MaxPayloadBytes:  1024,
		MemoryLimitBytes: 4096,
	}, nil)
	payload := bytes.NewBufferString(`{"resourceSpans":[{"secret":"should-not-be-stored"}]}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/traces", payload)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	pipeline.HandleOTLPHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d body=%s", rec.Code, rec.Body.String())
	}
	event := <-pipeline.queue
	if strings.Contains(event.Message, "should-not-be-stored") {
		t.Fatalf("raw OTLP payload leaked into event: %+v", event)
	}
	if event.Kind != EventKindSpan || event.Attributes["path"] != "/v1/traces" {
		t.Fatalf("unexpected OTLP event: %+v", event)
	}
}

func TestOTLPHTTPReceiverParsesJSONSpans(t *testing.T) {
	pipeline := NewPipeline(Config{
		Enabled:          true,
		QueueSize:        4,
		MaxPayloadBytes:  4096,
		MemoryLimitBytes: 4096,
	}, nil)
	payload := bytes.NewBufferString(`{
		"resourceSpans": [{
			"resource": {"attributes": [
				{"key": "service.name", "value": {"stringValue": "checkout-api"}},
				{"key": "fugue.tenant_id", "value": {"stringValue": "tenant_123"}},
				{"key": "fugue.project_id", "value": {"stringValue": "project_123"}},
				{"key": "fugue.app_id", "value": {"stringValue": "app_123"}}
			]},
			"scopeSpans": [{
				"spans": [{
					"traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
					"spanId": "00f067aa0ba902b7",
					"parentSpanId": "0000000000000001",
					"name": "db_wait",
					"startTimeUnixNano": "1717632000000000000",
					"endTimeUnixNano": "1717632000017000000",
					"attributes": [
						{"key": "fugue.request_id", "value": {"stringValue": "req_123"}},
						{"key": "http.response.status_code", "value": {"intValue": "503"}},
						{"key": "Authorization", "value": {"stringValue": "Bearer secret"}}
					],
					"status": {"code": "STATUS_CODE_ERROR", "message": "upstream timeout"}
				}]
			}]
		}]
	}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/traces", payload)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	pipeline.HandleOTLPHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d body=%s", rec.Code, rec.Body.String())
	}
	event := <-pipeline.queue
	for key, want := range map[string]string{
		"event_type":     "request_span",
		"service":        "checkout-api",
		"tenant_id":      "tenant_123",
		"project_id":     "project_123",
		"app_id":         "app_123",
		"trace_id":       "4bf92f3577b34da6a3ce929d0e0e4736",
		"span_id":        "00f067aa0ba902b7",
		"parent_span_id": "0000000000000001",
		"stage":          "db_wait",
		"stage_ms":       "17",
		"request_id":     "req_123",
		"status_code":    "503",
		"error_type":     "upstream timeout",
	} {
		if got := event.Attributes[key]; got != want {
			t.Fatalf("expected %s=%q, got %q in %+v", key, want, got, event.Attributes)
		}
	}
	if _, ok := event.Attributes["Authorization"]; ok {
		t.Fatalf("secret-like OTLP attribute was retained: %+v", event.Attributes)
	}
	if event.Kind != EventKindSpan || event.Message != "otlp span accepted" {
		t.Fatalf("unexpected event: %+v", event)
	}
}

func TestOTLPHTTPReceiverParsesStructuredJSONRequestSummary(t *testing.T) {
	pipeline := NewPipeline(Config{
		Enabled:          true,
		QueueSize:        4,
		MaxPayloadBytes:  4096,
		MemoryLimitBytes: 4096,
	}, nil)
	payload := bytes.NewBufferString(`{
		"events": [{
			"timestamp": "2026-06-06T00:00:00Z",
			"event_type": "request_summary",
			"message": "request finished token=secret",
			"attributes": {
				"tenant_id": "tenant_123",
				"project_id": "project_123",
				"app_id": "app_123",
				"trace_id": "trace_123",
				"request_id": "request_123",
				"status_code": 200,
				"duration_ms": 42,
				"Authorization": "Bearer secret"
			},
			"summary": {
				"scenario": "pilot",
				"mode": "streaming"
			}
		}]
	}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/logs", payload)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	pipeline.HandleOTLPHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d body=%s", rec.Code, rec.Body.String())
	}
	event := <-pipeline.queue
	if event.Kind != EventKindLog {
		t.Fatalf("expected log event, got %+v", event)
	}
	for key, want := range map[string]string{
		"event_type":       "request_summary",
		"tenant_id":        "tenant_123",
		"project_id":       "project_123",
		"app_id":           "app_123",
		"trace_id":         "trace_123",
		"request_id":       "request_123",
		"status_code":      "200",
		"duration_ms":      "42",
		"summary.scenario": "pilot",
		"summary.mode":     "streaming",
		"otlp_path":        "/v1/logs",
	} {
		if got := event.Attributes[key]; got != want {
			t.Fatalf("expected %s=%q, got %q in %+v", key, want, got, event.Attributes)
		}
	}
	if _, ok := event.Attributes["Authorization"]; ok {
		t.Fatalf("secret-like structured attribute was retained: %+v", event.Attributes)
	}
	if event.Message != "request finished token=[REDACTED]" {
		t.Fatalf("message was not redacted: %q", event.Message)
	}
}

func TestOTLPHTTPReceiverExportsRequestAndAdmissionContractsToClickHouse(t *testing.T) {
	type capturedInsert struct {
		method      string
		query       string
		contentType string
		body        []byte
		err         error
	}
	inserts := make(chan capturedInsert, 2)
	clickHouse := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		inserts <- capturedInsert{
			method:      r.Method,
			query:       r.URL.Query().Get("query"),
			contentType: r.Header.Get("Content-Type"),
			body:        body,
			err:         err,
		}
		if err != nil {
			http.Error(w, "failed to read insert", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(clickHouse.Close)

	pipeline := NewPipeline(Config{
		Enabled:          true,
		ClickHouseDSN:    clickHouse.URL + "?database=fugue_observability",
		ExportTimeout:    time.Second,
		QueueSize:        4,
		BatchSize:        1,
		MaxPayloadBytes:  32 << 10,
		MemoryLimitBytes: 1 << 20,
		RetryMaxAttempts: 1,
	}, nil)
	if err := pipeline.Start(context.Background()); err != nil {
		t.Fatalf("start telemetry pipeline: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := pipeline.Stop(ctx); err != nil {
			t.Errorf("stop telemetry pipeline: %v", err)
		}
	})

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/logs", pipeline.HandleOTLPHTTP)
	receiver := httptest.NewServer(mux)
	t.Cleanup(receiver.Close)

	summary := map[string]any{
		"endpoint":    "/v1/responses",
		"statusCode":  http.StatusServiceUnavailable,
		"routeSource": "zero_zero",
		"ttftMs":      240,
	}
	summaryJSON, err := json.Marshal(summary)
	if err != nil {
		t.Fatalf("marshal request summary: %v", err)
	}
	admissionSummary := map[string]any{
		"decision":                        "reject",
		"reason":                          "large_body_capacity_exhausted",
		"request_self_lease_id":           "lease_contract",
		"request_self_request_id":         "request_rejected_contract",
		"request_self_trace_id":           "trace_rejected_contract",
		"runtime_global_large_body_limit": 1,
		"blocking_holders": []map[string]any{{
			"claim_id":   "claim_contract",
			"lease_id":   "lease_holder_contract",
			"request_id": "request_holder_contract",
			"trace_id":   "trace_holder_contract",
		}},
	}
	admissionSummaryJSON, err := json.Marshal(admissionSummary)
	if err != nil {
		t.Fatalf("marshal admission summary: %v", err)
	}
	payload, err := json.Marshal(map[string]any{
		"service": "uni-api-web-api",
		"events": []map[string]any{{
			"kind":          "log",
			"event_type":    "request_summary",
			"source":        "uni-api-web-api",
			"message":       "request_summary",
			"timestamp":     "2026-07-16T03:16:02.720Z",
			"id":            "event_zero_zero_contract",
			"tenant_id":     "tenant_contract",
			"project_id":    "project_contract",
			"app_id":        "app_zero_zero_contract",
			"runtime_id":    "runtime_contract",
			"trace_id":      "trace_zero_zero_contract",
			"span_id":       "span_zero_zero_contract",
			"request_id":    "request_zero_zero_contract",
			"path":          "/v1/responses",
			"path_template": "/v1/responses",
			"method":        http.MethodPost,
			"route_id":      "/v1/responses",
			"status_code":   http.StatusServiceUnavailable,
			"status_class":  "5xx",
			"duration_ms":   6627,
			"ttft_ms":       240,
			"streaming":     true,
			"summary":       summary,
			"summary_json":  string(summaryJSON),
		}, {
			"kind":       "log",
			"event_type": "large_body_admission_decision",
			"source":     "uni-api-ember",
			"message":    "large body admission reject",
			"timestamp":  "2026-07-16T03:16:03.000Z",
			"tenant_id":  "tenant_contract",
			"project_id": "project_contract",
			"app_id":     "app_ember_contract",
			"runtime_id": "runtime_ember_contract",
			"trace_id":   "trace_rejected_contract",
			"request_id": "request_rejected_contract",
			"attributes": map[string]any{
				"fugue_table":                     "app_events",
				"severity":                        "warning",
				"decision":                        "reject",
				"reason":                          "large_body_capacity_exhausted",
				"runtime_global_large_body_limit": 1,
			},
			"summary":      admissionSummary,
			"summary_json": string(admissionSummaryJSON),
		}},
	})
	if err != nil {
		t.Fatalf("marshal 0-0 request summary envelope: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, receiver.URL+"/v1/logs", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("create telemetry request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := receiver.Client().Do(req)
	if err != nil {
		t.Fatalf("post telemetry request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 202, got %d body=%s", resp.StatusCode, body)
	}

	var requestInsert, appInsert capturedInsert
	for received := 0; received < 2; received++ {
		select {
		case insert := <-inserts:
			switch insert.query {
			case "INSERT INTO fugue_observability.request_facts FORMAT JSONEachRow":
				requestInsert = insert
			case "INSERT INTO fugue_observability.app_events FORMAT JSONEachRow":
				appInsert = insert
			default:
				t.Fatalf("unexpected ClickHouse query=%q body=%s", insert.query, insert.body)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for ClickHouse inserts")
		}
	}
	insert := requestInsert
	if insert.err != nil {
		t.Fatalf("read ClickHouse insert: %v", insert.err)
	}
	if insert.method != http.MethodPost || insert.contentType != "application/json" {
		t.Fatalf("unexpected ClickHouse request method=%q content_type=%q", insert.method, insert.contentType)
	}
	const wantQuery = "INSERT INTO fugue_observability.request_facts FORMAT JSONEachRow"
	if insert.query != wantQuery {
		t.Fatalf("ClickHouse query=%q, want %q; body=%s", insert.query, wantQuery, insert.body)
	}
	if strings.Contains(string(insert.body), "request_fact_incomplete") {
		t.Fatalf("complete request summary was rerouted as incomplete: %s", insert.body)
	}
	var row requestFactRow
	if err := json.Unmarshal(bytes.TrimSpace(insert.body), &row); err != nil {
		t.Fatalf("decode request_facts JSONEachRow body: %v\nbody=%s", err, insert.body)
	}
	if row.AppID != "app_zero_zero_contract" ||
		row.PathTemplate != "/v1/responses" ||
		row.StatusCode != http.StatusServiceUnavailable ||
		row.StatusClass != "5xx" ||
		row.Method != http.MethodPost ||
		row.RouteID != "/v1/responses" ||
		row.DurationMS != 6627 ||
		row.TTFBMS != 0 ||
		!row.Streaming {
		t.Fatalf("0-0 request summary fields were not preserved in request_facts: %+v", row)
	}
	if !strings.Contains(row.SummaryJSON, `"ttftMs":240`) {
		t.Fatalf("0-0 TTFT was not preserved with its real semantics: %s", row.SummaryJSON)
	}
	if appInsert.err != nil {
		t.Fatalf("read admission app_events insert: %v", appInsert.err)
	}
	var appRow appEventRow
	if err := json.Unmarshal(bytes.TrimSpace(appInsert.body), &appRow); err != nil {
		t.Fatalf("decode app_events JSONEachRow body: %v\nbody=%s", err, appInsert.body)
	}
	if appRow.EventType != "large_body_admission_decision" ||
		appRow.AppID != "app_ember_contract" ||
		appRow.Severity != "warning" {
		t.Fatalf("admission decision was not preserved in app_events: %+v", appRow)
	}
	for _, value := range []string{
		"claim_contract",
		"lease_contract",
		"lease_holder_contract",
		"request_rejected_contract",
		"trace_rejected_contract",
		"request_holder_contract",
		"trace_holder_contract",
		"large_body_capacity_exhausted",
	} {
		if !strings.Contains(appRow.AttributesJSON, value) {
			t.Fatalf("admission app_events row lost %q: %s", value, appRow.AttributesJSON)
		}
	}
}

func TestOTLPHTTPReceiverParsesStructuredJSONMetrics(t *testing.T) {
	pipeline := NewPipeline(Config{
		Enabled:          true,
		QueueSize:        4,
		MaxPayloadBytes:  4096,
		MemoryLimitBytes: 4096,
	}, nil)
	payload := bytes.NewBufferString(`{
		"metric": "app_inflight_requests",
		"value": 7,
		"attributes": {
			"tenant_id": "tenant_123",
			"project_id": "project_123",
			"app_id": "app_123",
			"runtime_id": "runtime_123",
			"component": "pilot",
			"trace_id": "trace-high-cardinality"
		}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/metrics", payload)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	pipeline.HandleOTLPHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d body=%s", rec.Code, rec.Body.String())
	}
	event := <-pipeline.queue
	if event.Kind != EventKindMetric {
		t.Fatalf("expected metric event, got %+v", event)
	}
	for key, want := range map[string]string{
		"metric":     "app_inflight_requests",
		"value":      "7",
		"tenant_id":  "tenant_123",
		"project_id": "project_123",
		"app_id":     "app_123",
		"runtime_id": "runtime_123",
		"component":  "pilot",
	} {
		if got := event.Attributes[key]; got != want {
			t.Fatalf("expected %s=%q, got %q in %+v", key, want, got, event.Attributes)
		}
	}
	if _, ok := event.Attributes["trace_id"]; ok {
		t.Fatalf("metric retained high-cardinality trace_id: %+v", event.Attributes)
	}
}

func TestOTLPHTTPReceiverRejectsLargePayload(t *testing.T) {
	pipeline := NewPipeline(Config{
		Enabled:          true,
		QueueSize:        4,
		MaxPayloadBytes:  8,
		MemoryLimitBytes: 4096,
	}, nil)
	req := httptest.NewRequest(http.MethodPost, "/v1/logs", strings.NewReader(`{"too":"large"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	pipeline.HandleOTLPHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", rec.Code)
	}
	if pipeline.Snapshot().Dropped == 0 {
		t.Fatal("expected dropped counter to increase")
	}
}

func TestPrometheusScrapeCreatesMetricEvent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("# HELP demo demo\n# TYPE demo counter\ndemo_total{trace_id=\"x\"} 1\nother 2\n"))
	}))
	defer server.Close()

	pipeline := NewPipeline(Config{
		Enabled:              true,
		QueueSize:            4,
		MemoryLimitBytes:     4096,
		PrometheusScrapeURLs: []string{server.URL},
		ScrapeInterval:       time.Hour,
		MaxPayloadBytes:      1024,
	}, nil)
	pipeline.ctx = context.Background()
	pipeline.scrapePrometheusOnce(server.URL)

	event := <-pipeline.queue
	if event.Kind != EventKindMetric || event.Attributes["sample_count"] != "2" {
		t.Fatalf("unexpected scrape event: %+v", event)
	}
	if pipeline.Snapshot().PrometheusScrapes != 1 {
		t.Fatalf("expected scrape counter to increase: %+v", pipeline.Snapshot())
	}
}

func TestEventFromLogLineDropsStructuredSecrets(t *testing.T) {
	line, err := json.Marshal(map[string]any{
		"message":       "request finished token=secret",
		"level":         "info",
		"Authorization": "Bearer secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	event, redacted := EventFromLogLine("unit", string(line))
	if redacted == 0 {
		t.Fatal("expected redaction")
	}
	if _, ok := event.Attributes["Authorization"]; ok {
		t.Fatalf("secret field was retained: %+v", event.Attributes)
	}
	if event.Attributes["level"] != "info" {
		t.Fatalf("expected level attr to remain: %+v", event.Attributes)
	}
	if strings.Contains(event.Message, "secret") {
		t.Fatalf("message was not redacted: %q", event.Message)
	}
}
