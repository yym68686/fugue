package observability

import (
	"bytes"
	"context"
	"encoding/json"
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
