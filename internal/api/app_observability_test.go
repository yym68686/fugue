package api

import (
	"net/http"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/observability"
)

func TestAppObservabilityMetricsSummaryDisabledIsStable(t *testing.T) {
	_, server, apiKey, app := setupAppConfigTestServer(t, appObservabilityTestSpec())

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/observability/metrics/summary?since=30m", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Source  appObservabilitySourceStatus `json:"source"`
		Window  appObservabilityWindow       `json:"window"`
		Metrics []any                        `json:"metrics"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Source.Available || response.Source.Status != "disabled" {
		t.Fatalf("expected disabled source, got %+v", response.Source)
	}
	if response.Source.Reason != "observability is disabled" {
		t.Fatalf("unexpected disabled reason: %+v", response.Source)
	}
	if len(response.Metrics) != 0 {
		t.Fatalf("expected empty metrics, got %+v", response.Metrics)
	}
	if response.Window.Since == "" || response.Window.Until == "" {
		t.Fatalf("expected response window, got %+v", response.Window)
	}
}

func TestAppObservabilityLogsShowsActiveExporterButQueryPending(t *testing.T) {
	_, server, apiKey, app := setupAppConfigTestServer(t, appObservabilityTestSpec())
	server.observabilityConfig = observability.Config{
		Enabled:       true,
		LokiURL:       "https://loki.example.test",
		ClickHouseDSN: "http://clickhouse.example.test:8123?database=fugue_observability",
	}.Normalize()

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/observability/logs/query?since=15m&level=error", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Source appObservabilitySourceStatus `json:"source"`
		Logs   []any                        `json:"logs"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Source.Available {
		t.Fatalf("query backend should not be marked available yet: %+v", response.Source)
	}
	if response.Source.Status != "degraded" || !strings.Contains(response.Source.Reason, "logs query backend is not wired yet") {
		t.Fatalf("unexpected source status: %+v", response.Source)
	}
	if strings.Join(response.Source.ActiveExporters, ",") != "analytics,logs" {
		t.Fatalf("expected active logs and analytics exporters, got %+v", response.Source.ActiveExporters)
	}
	if len(response.Logs) != 0 {
		t.Fatalf("expected empty logs until query backend is wired, got %+v", response.Logs)
	}
}

func TestAppObservabilityTraceReturnsTraceIDAndEmptySpans(t *testing.T) {
	_, server, apiKey, app := setupAppConfigTestServer(t, appObservabilityTestSpec())

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/observability/traces/trace_123", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		TraceID string `json:"trace_id"`
		Spans   []any  `json:"spans"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.TraceID != "trace_123" {
		t.Fatalf("expected trace id in response, got %+v", response)
	}
	if len(response.Spans) != 0 {
		t.Fatalf("expected empty spans until query backend is wired, got %+v", response.Spans)
	}
}

func TestAppObservabilityRejectsOversizedWindow(t *testing.T) {
	_, server, apiKey, app := setupAppConfigTestServer(t, appObservabilityTestSpec())

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/observability/requests?since=25h", apiKey, nil)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", recorder.Code, recorder.Body.String())
	}
}

func appObservabilityTestSpec() model.AppSpec {
	return model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	}
}
