package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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
	if response.Source.ActiveExporters == nil {
		t.Fatalf("active_exporters should be an empty array, got nil")
	}
	if len(response.Metrics) != 0 {
		t.Fatalf("expected empty metrics, got %+v", response.Metrics)
	}
	if response.Window.Since == "" || response.Window.Until == "" {
		t.Fatalf("expected response window, got %+v", response.Window)
	}
}

func TestAppObservabilityMetricsSummaryQueriesPrometheus(t *testing.T) {
	_, server, apiKey, app := setupAppConfigTestServer(t, appObservabilityTestSpec())
	queries := []string{}
	prometheus := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/query" {
			t.Fatalf("expected Prometheus query path, got %q", r.URL.Path)
		}
		query := r.URL.Query().Get("query")
		queries = append(queries, query)
		value := "1"
		switch {
		case strings.Contains(query, "fugue_app_response_status_total"):
			value = "0.05"
		case strings.Contains(query, "fugue_app_requests_total"):
			value = "120"
		case strings.Contains(query, "fugue_app_ttfb_seconds_bucket"):
			value = "250"
		case strings.Contains(query, "fugue_app_duration_seconds_bucket"):
			value = "900"
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"status": "success",
			"data": map[string]any{
				"resultType": "vector",
				"result": []any{
					map[string]any{
						"metric": map[string]string{"app_id": app.ID},
						"value":  []any{float64(1713571200), value},
					},
				},
			},
		}); err != nil {
			t.Errorf("write Prometheus response: %v", err)
		}
	}))
	t.Cleanup(prometheus.Close)
	server.observabilityConfig = observability.Config{
		Enabled:         true,
		MetricsQueryURL: prometheus.URL + "/api/v1/write",
	}.Normalize()

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/observability/metrics/summary?since=5m", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Source  appObservabilitySourceStatus `json:"source"`
		Metrics []map[string]any             `json:"metrics"`
	}
	mustDecodeJSON(t, recorder, &response)
	if !response.Source.Available || response.Source.Status != "available" {
		t.Fatalf("expected available metrics source, got %+v", response.Source)
	}
	if len(response.Metrics) != 4 {
		t.Fatalf("expected four metric samples, got %+v", response.Metrics)
	}
	joined := strings.Join(queries, "\n")
	for _, want := range []string{
		`app_id="` + app.ID + `"`,
		"[300s]",
		"fugue_app_requests_total",
		"fugue_app_response_status_total",
		"fugue_app_ttfb_seconds_bucket",
		"fugue_app_duration_seconds_bucket",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected Prometheus queries to contain %q, got %q", want, joined)
		}
	}
}

func TestAppObservabilityMetricsSummaryFallsBackToClickHouse(t *testing.T) {
	_, server, apiKey, app := setupAppConfigTestServer(t, appObservabilityTestSpec())
	prometheus := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/query" {
			t.Fatalf("expected Prometheus query path, got %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"status": "success",
			"data": map[string]any{
				"resultType": "vector",
				"result":     []any{},
			},
		}); err != nil {
			t.Errorf("write Prometheus response: %v", err)
		}
	}))
	t.Cleanup(prometheus.Close)
	var clickHouseQuery string
	clickHouse := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clickHouseQuery = r.URL.Query().Get("query")
		_, _ = w.Write([]byte(`{"app_request_count":10,"total_request_count":20,"app_error_count":2,"total_error_count":8,"app_p95_ttfb_ms":321,"total_p95_ttfb_ms":999,"app_p95_duration_ms":654,"total_p95_duration_ms":1999}` + "\n"))
	}))
	t.Cleanup(clickHouse.Close)
	server.observabilityConfig = observability.Config{
		Enabled:         true,
		MetricsQueryURL: prometheus.URL,
		ClickHouseDSN:   clickHouse.URL + "?database=fugue_observability",
	}.Normalize()

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/observability/metrics/summary?since=5m", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Source  appObservabilitySourceStatus `json:"source"`
		Metrics []map[string]any             `json:"metrics"`
	}
	mustDecodeJSON(t, recorder, &response)
	if !response.Source.Available || response.Source.Status != "available" {
		t.Fatalf("expected available metrics source, got %+v", response.Source)
	}
	for _, want := range []string{
		"FROM request_facts",
		"countIf(edge_id = '') AS app_request_count",
		"app_id = '" + app.ID + "'",
		"FORMAT JSONEachRow",
	} {
		if !strings.Contains(clickHouseQuery, want) {
			t.Fatalf("expected ClickHouse query to contain %q, got %q", want, clickHouseQuery)
		}
	}
	if len(response.Metrics) != 4 {
		t.Fatalf("expected four fallback metric samples, got %+v", response.Metrics)
	}
	metricsByName := map[string]map[string]any{}
	for _, metric := range response.Metrics {
		metricsByName[fmt.Sprint(metric["name"])] = metric
	}
	if metricsByName["error_rate"]["value"] != 0.2 {
		t.Fatalf("expected app error_rate=0.2, got %+v", metricsByName["error_rate"])
	}
	if metricsByName["p95_ttfb_ms"]["value"] != float64(321) {
		t.Fatalf("expected app p95_ttfb_ms=321, got %+v", metricsByName["p95_ttfb_ms"])
	}
	if metricsByName["p95_duration_ms"]["value"] != float64(654) {
		t.Fatalf("expected app p95_duration_ms=654, got %+v", metricsByName["p95_duration_ms"])
	}
	labels, ok := metricsByName["rpm"]["labels"].(map[string]any)
	if !ok || labels["source"] != "app" {
		t.Fatalf("expected fallback metrics to prefer app rows, got %+v", metricsByName["rpm"])
	}
}

func TestAppObservabilityMetricsQueryUsesSupportedAlias(t *testing.T) {
	_, server, apiKey, app := setupAppConfigTestServer(t, appObservabilityTestSpec())
	var prometheusQuery string
	prometheus := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/query" {
			t.Fatalf("expected Prometheus query path, got %q", r.URL.Path)
		}
		prometheusQuery = r.URL.Query().Get("query")
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"status": "success",
			"data": map[string]any{
				"resultType": "vector",
				"result": []any{
					map[string]any{
						"metric": map[string]string{"app_id": app.ID},
						"value":  []any{float64(1713571200), "900"},
					},
				},
			},
		}); err != nil {
			t.Errorf("write Prometheus response: %v", err)
		}
	}))
	t.Cleanup(prometheus.Close)
	server.observabilityConfig = observability.Config{
		Enabled:         true,
		MetricsQueryURL: prometheus.URL,
	}.Normalize()

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/observability/metrics/query?since=5m&query=p95+latency", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Source  appObservabilitySourceStatus `json:"source"`
		Query   string                       `json:"query"`
		Metrics []map[string]any             `json:"metrics"`
	}
	mustDecodeJSON(t, recorder, &response)
	if !response.Source.Available || response.Source.Status != "available" {
		t.Fatalf("expected available metrics source, got %+v", response.Source)
	}
	if response.Query != "p95 latency" {
		t.Fatalf("expected echoed query, got %+v", response)
	}
	for _, want := range []string{
		`app_id="` + app.ID + `"`,
		"fugue_app_duration_seconds_bucket",
		"[300s]",
	} {
		if !strings.Contains(prometheusQuery, want) {
			t.Fatalf("expected Prometheus query to contain %q, got %q", want, prometheusQuery)
		}
	}
	if len(response.Metrics) != 1 || response.Metrics[0]["name"] != "p95_duration_ms" {
		t.Fatalf("expected p95 duration metric, got %+v", response.Metrics)
	}
}

func TestAppObservabilityRequiresReadScope(t *testing.T) {
	stateStore, server, _, app := setupAppConfigTestServer(t, appObservabilityTestSpec())
	_, apiKey, err := stateStore.CreateAPIKey(app.TenantID, "no-observability", []string{"billing.write"})
	if err != nil {
		t.Fatalf("create restricted api key: %v", err)
	}

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/observability/metrics/summary?since=30m", apiKey, nil)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "app.observability.read") {
		t.Fatalf("expected missing scope message, got %s", recorder.Body.String())
	}
}

func TestAppObservabilityRejectsWindowBeyondAppRetention(t *testing.T) {
	_, server, apiKey, app := setupAppConfigTestServer(t, appObservabilityTestSpec())
	server.observabilityConfig = observability.Config{
		Enabled: true,
		AppRetentionOverrides: map[string]time.Duration{
			app.ID: 30 * time.Minute,
		},
	}.Normalize()

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/observability/metrics/summary?since=45m", apiKey, nil)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "app retention 30m0s") {
		t.Fatalf("expected retention error, got %s", recorder.Body.String())
	}
}

func TestAppObservabilityDefaultWindowHonorsShortAppRetention(t *testing.T) {
	_, server, apiKey, app := setupAppConfigTestServer(t, appObservabilityTestSpec())
	server.observabilityConfig = observability.Config{
		Enabled: true,
		AppRetentionOverrides: map[string]time.Duration{
			app.ID: 30 * time.Minute,
		},
	}.Normalize()

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/observability/metrics/summary", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Source appObservabilitySourceStatus `json:"source"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Source.Retention != "30m0s" {
		t.Fatalf("expected app retention in source, got %+v", response.Source)
	}
}

func TestAppObservabilityLogsQueriesLoki(t *testing.T) {
	_, server, apiKey, app := setupAppConfigTestServer(t, appObservabilityTestSpec())
	var lokiPath string
	var lokiQuery string
	var lokiLimit string
	loki := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lokiPath = r.URL.Path
		lokiQuery = r.URL.Query().Get("query")
		lokiLimit = r.URL.Query().Get("limit")
		if r.URL.Query().Get("start") == "" || r.URL.Query().Get("end") == "" {
			t.Errorf("expected query_range start and end parameters, got %s", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"status": "success",
			"data": map[string]any{
				"result": []any{
					map[string]any{
						"stream": map[string]string{
							"app_id": app.ID,
							"level":  "error",
						},
						"values": [][2]string{
							{
								"1713571200000000000",
								`{"message":"timeout","trace_id":"trace_123","attributes":{"stage":"db"}}`,
							},
						},
					},
				},
			},
		}); err != nil {
			t.Errorf("write Loki response: %v", err)
		}
	}))
	t.Cleanup(loki.Close)
	server.observabilityConfig = observability.Config{
		Enabled: true,
		LokiURL: loki.URL,
	}.Normalize()

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/observability/logs/query?since=15m&level=error&grep=timeout&trace_id=trace_123&limit=10", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Source appObservabilitySourceStatus `json:"source"`
		Logs   []map[string]any             `json:"logs"`
	}
	mustDecodeJSON(t, recorder, &response)
	if !response.Source.Available || response.Source.Status != "available" {
		t.Fatalf("expected available logs source, got %+v", response.Source)
	}
	if strings.Join(response.Source.ActiveExporters, ",") != "logs" {
		t.Fatalf("expected active logs exporter, got %+v", response.Source.ActiveExporters)
	}
	if lokiPath != "/loki/api/v1/query_range" {
		t.Fatalf("expected Loki query_range path, got %q", lokiPath)
	}
	for _, want := range []string{`app_id="` + app.ID + `"`, `level="error"`, `|= "timeout"`, `|= "trace_123"`} {
		if !strings.Contains(lokiQuery, want) {
			t.Fatalf("expected Loki query to contain %q, got %q", want, lokiQuery)
		}
	}
	if lokiLimit != "10" {
		t.Fatalf("expected Loki limit=10, got %q", lokiLimit)
	}
	if len(response.Logs) != 1 {
		t.Fatalf("expected one log, got %+v", response.Logs)
	}
	if response.Logs[0]["message"] != "timeout" || response.Logs[0]["trace_id"] != "trace_123" {
		t.Fatalf("expected parsed log fields, got %+v", response.Logs[0])
	}
	if response.Logs[0]["timestamp"] == "" {
		t.Fatalf("expected timestamp, got %+v", response.Logs[0])
	}
	if _, ok := response.Logs[0]["line"]; ok {
		t.Fatalf("line should not be a top-level observability log field: %+v", response.Logs[0])
	}
	attributes, ok := response.Logs[0]["attributes"].(map[string]any)
	if !ok || attributes["stage"] != "db" || attributes["app_id"] != app.ID {
		t.Fatalf("expected merged log attributes, got %+v", response.Logs[0])
	}
}

func TestAppObservabilityLogsLokiFailureIsNonBlocking(t *testing.T) {
	_, server, apiKey, app := setupAppConfigTestServer(t, appObservabilityTestSpec())
	loki := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
	}))
	t.Cleanup(loki.Close)
	server.observabilityConfig = observability.Config{
		Enabled: true,
		LokiURL: loki.URL,
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
		t.Fatalf("query backend failure should not be marked available: %+v", response.Source)
	}
	if response.Source.Status != "degraded" || !strings.Contains(response.Source.Reason, "query Loki logs returned 503") {
		t.Fatalf("unexpected source status: %+v", response.Source)
	}
	if strings.Join(response.Source.ActiveExporters, ",") != "logs" {
		t.Fatalf("expected active logs exporter, got %+v", response.Source.ActiveExporters)
	}
	if len(response.Logs) != 0 {
		t.Fatalf("expected empty logs on Loki failure, got %+v", response.Logs)
	}
}

func TestAppObservabilityRequestsQueriesClickHouse(t *testing.T) {
	_, server, apiKey, app := setupAppConfigTestServer(t, appObservabilityTestSpec())
	var clickHouseQuery string
	clickHouse := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clickHouseQuery = r.URL.Query().Get("query")
		if r.URL.Query().Get("database") != "fugue_observability" {
			t.Errorf("expected database query parameter, got %s", r.URL.RawQuery)
		}
		_, _ = w.Write([]byte(`{"ts":"2026-06-05 22:00:00.000","trace_id":"trace_123","request_id":"request_123","path_template":"/v1/items","method":"POST","status_code":503,"duration_ms":1200,"ttfb_ms":240,"summary_json":"{\"provider\":\"example\"}"}` + "\n"))
	}))
	t.Cleanup(clickHouse.Close)
	server.observabilityConfig = observability.Config{
		Enabled:       true,
		ClickHouseDSN: clickHouse.URL + "?database=fugue_observability",
	}.Normalize()

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/observability/requests?since=15m&trace_id=trace_123&status_class=5xx&errors=true&slow=true&limit=10", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Source   appObservabilitySourceStatus `json:"source"`
		Requests []map[string]any             `json:"requests"`
	}
	mustDecodeJSON(t, recorder, &response)
	if !response.Source.Available || response.Source.Status != "available" {
		t.Fatalf("expected available analytics source, got %+v", response.Source)
	}
	for _, want := range []string{
		"FROM request_facts",
		"app_id = '" + app.ID + "'",
		"trace_id = 'trace_123'",
		"status_class = '5xx'",
		"(status_code >= 400 OR error_type != '')",
		"duration_ms >= 1000",
		"LIMIT 10",
		"FORMAT JSONEachRow",
	} {
		if !strings.Contains(clickHouseQuery, want) {
			t.Fatalf("expected ClickHouse query to contain %q, got %q", want, clickHouseQuery)
		}
	}
	if len(response.Requests) != 1 {
		t.Fatalf("expected one request, got %+v", response.Requests)
	}
	request := response.Requests[0]
	if request["trace_id"] != "trace_123" || request["request_id"] != "request_123" || request["route"] != "/v1/items" || request["ttft_ms"] == nil {
		t.Fatalf("expected request summary fields, got %+v", request)
	}
	summary, ok := request["summary"].(map[string]any)
	if !ok || summary["provider"] != "example" {
		t.Fatalf("expected parsed request summary, got %+v", request)
	}
}

func TestAppObservabilityRequestsStreamDisabledEnds(t *testing.T) {
	_, server, apiKey, app := setupAppConfigTestServer(t, appObservabilityTestSpec())

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/observability/requests/stream?follow=false", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	for _, want := range []string{
		"event: ready",
		`"status":"disabled"`,
		"event: end",
		"observability is disabled",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("expected stream body to contain %q, got %q", want, body)
		}
	}
}

func TestAppObservabilityRequestsStreamQueriesClickHouse(t *testing.T) {
	_, server, apiKey, app := setupAppConfigTestServer(t, appObservabilityTestSpec())
	var clickHouseQuery string
	clickHouse := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clickHouseQuery = r.URL.Query().Get("query")
		_, _ = w.Write([]byte(`{"ts":"2026-06-05 22:00:01.000","trace_id":"trace_2","request_id":"request_2","path_template":"/v1/items","method":"POST","status_code":200,"duration_ms":80,"ttfb_ms":20,"summary_json":"{\"stage\":\"ok\"}"}` + "\n"))
		_, _ = w.Write([]byte(`{"ts":"2026-06-05 22:00:00.000","trace_id":"trace_1","request_id":"request_1","path_template":"/v1/items","method":"POST","status_code":503,"duration_ms":1200,"ttfb_ms":240,"summary_json":"{\"stage\":\"retry\"}"}` + "\n"))
	}))
	t.Cleanup(clickHouse.Close)
	server.observabilityConfig = observability.Config{
		Enabled:       true,
		ClickHouseDSN: clickHouse.URL + "?database=fugue_observability",
	}.Normalize()

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/observability/requests/stream?follow=false&since=15m&limit=10", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	for _, want := range []string{
		"event: ready",
		`"status":"available"`,
		"event: request",
		`"trace_id":"trace_1"`,
		`"trace_id":"trace_2"`,
		"event: end",
		"snapshot complete",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("expected stream body to contain %q, got %q", want, body)
		}
	}
	for _, want := range []string{
		"FROM request_facts",
		"app_id = '" + app.ID + "'",
		"LIMIT 10",
		"FORMAT JSONEachRow",
	} {
		if !strings.Contains(clickHouseQuery, want) {
			t.Fatalf("expected ClickHouse query to contain %q, got %q", want, clickHouseQuery)
		}
	}
}

func TestAppObservabilityTraceQueriesClickHouse(t *testing.T) {
	_, server, apiKey, app := setupAppConfigTestServer(t, appObservabilityTestSpec())
	var clickHouseQuery string
	clickHouse := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clickHouseQuery = r.URL.Query().Get("query")
		_, _ = w.Write([]byte(`{"ts":"2026-06-05 22:00:00.000","service":"api","trace_id":"trace_123","span_id":"span_1","parent_span_id":"","request_id":"request_123","stage":"db","stage_ms":12,"status_code":200,"error_type":"","attributes_json":"{\"pool\":\"main\"}"}` + "\n"))
		_, _ = w.Write([]byte(`{"ts":"2026-06-05 22:00:00.010","service":"api","trace_id":"trace_123","span_id":"span_2","parent_span_id":"span_1","request_id":"request_123","stage":"stream","stage_ms":34,"status_code":200,"error_type":"","attributes_json":"{}"}` + "\n"))
	}))
	t.Cleanup(clickHouse.Close)
	server.observabilityConfig = observability.Config{
		Enabled:       true,
		ClickHouseDSN: clickHouse.URL + "?database=fugue_observability",
	}.Normalize()

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/observability/traces/trace_123", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Source appObservabilitySourceStatus `json:"source"`
		Spans  []map[string]any             `json:"spans"`
	}
	mustDecodeJSON(t, recorder, &response)
	if !response.Source.Available || response.Source.Status != "available" {
		t.Fatalf("expected available analytics source, got %+v", response.Source)
	}
	for _, want := range []string{
		"FROM request_spans",
		"app_id = '" + app.ID + "'",
		"trace_id = 'trace_123'",
		"ORDER BY ts ASC, stage_ms ASC",
		"LIMIT 1000",
		"FORMAT JSONEachRow",
	} {
		if !strings.Contains(clickHouseQuery, want) {
			t.Fatalf("expected ClickHouse query to contain %q, got %q", want, clickHouseQuery)
		}
	}
	if len(response.Spans) != 2 {
		t.Fatalf("expected two spans, got %+v", response.Spans)
	}
	attributes, ok := response.Spans[0]["attributes"].(map[string]any)
	if response.Spans[0]["stage"] != "db" || !ok || attributes["pool"] != "main" {
		t.Fatalf("expected parsed span fields, got %+v", response.Spans[0])
	}
}

func TestAppObservabilityDiagnosisQueriesClickHouse(t *testing.T) {
	_, server, apiKey, app := setupAppConfigTestServer(t, appObservabilityTestSpec())
	var clickHouseQuery string
	clickHouse := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clickHouseQuery = r.URL.Query().Get("query")
		_, _ = w.Write([]byte(`{"minute":"2026-06-05 23:00:00","rpm":1200,"p95_ttfb_ms":450,"p95_duration_ms":900,"error_rate":0.02,"top_bottleneck_stage":"runtime_pool_wait","top_bottleneck_confidence":0.82}` + "\n"))
	}))
	t.Cleanup(clickHouse.Close)
	server.observabilityConfig = observability.Config{
		Enabled:       true,
		ClickHouseDSN: clickHouse.URL + "?database=fugue_observability",
	}.Normalize()

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/observability/diagnosis?since=15m", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Source    appObservabilitySourceStatus `json:"source"`
		Diagnosis appObservabilityDiagnosis    `json:"diagnosis"`
	}
	mustDecodeJSON(t, recorder, &response)
	if !response.Source.Available || response.Source.Status != "available" {
		t.Fatalf("expected available analytics source, got %+v", response.Source)
	}
	for _, want := range []string{
		"FROM diagnosis_windows_1m",
		"app_id = '" + app.ID + "'",
		"ORDER BY minute DESC",
		"LIMIT 1",
		"FORMAT JSONEachRow",
	} {
		if !strings.Contains(clickHouseQuery, want) {
			t.Fatalf("expected ClickHouse query to contain %q, got %q", want, clickHouseQuery)
		}
	}
	if response.Diagnosis.Bottleneck != "runtime_pool_wait" || response.Diagnosis.Confidence != 0.82 {
		t.Fatalf("unexpected diagnosis: %+v", response.Diagnosis)
	}
	joinedEvidence := strings.Join(response.Diagnosis.Evidence, "\n")
	for _, want := range []string{"rpm=1200", "p95_ttfb_ms=450ms", "p95_duration_ms=900ms", "error_rate=0.02"} {
		if !strings.Contains(joinedEvidence, want) {
			t.Fatalf("expected evidence %q in %+v", want, response.Diagnosis.Evidence)
		}
	}
}

func TestAppObservabilityDiagnosisFallsBackToRuleRows(t *testing.T) {
	_, server, apiKey, app := setupAppConfigTestServer(t, appObservabilityTestSpec())
	queries := []string{}
	clickHouse := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queryText := r.URL.Query().Get("query")
		queries = append(queries, queryText)
		switch {
		case strings.Contains(queryText, "FROM diagnosis_windows_1m"):
			_, _ = w.Write([]byte(""))
		case strings.Contains(queryText, "FROM request_facts"):
			_, _ = w.Write([]byte(`{"request_count":120,"error_5xx_count":0,"error_4xx_count":0,"not_found_count":0,"error_5xx_rate":0,"error_4xx_rate":0,"not_found_rate":0,"p95_ttfb_ms":1600,"p95_duration_ms":2200,"max_duration_ms":4100,"edge_fallback_count":0,"peer_fallback_count":0,"stream_count":12}` + "\n"))
		case strings.Contains(queryText, "FROM request_spans"):
			_, _ = w.Write([]byte(`{"service":"web-api","stage":"db_pool_acquire","span_count":80,"p95_stage_ms":1400,"max_stage_ms":2200,"error_count":0}` + "\n"))
		case strings.Contains(queryText, "FROM app_events"):
			_, _ = w.Write([]byte(""))
		default:
			t.Fatalf("unexpected ClickHouse query: %s", queryText)
		}
	}))
	t.Cleanup(clickHouse.Close)
	server.observabilityConfig = observability.Config{
		Enabled:       true,
		ClickHouseDSN: clickHouse.URL + "?database=fugue_observability",
	}.Normalize()

	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/observability/diagnosis?since=15m", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Diagnosis appObservabilityDiagnosis `json:"diagnosis"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Diagnosis.Bottleneck != "db_lock_or_query_wait" {
		t.Fatalf("expected DB/pool bottleneck, got %+v", response.Diagnosis)
	}
	joinedEvidence := strings.Join(response.Diagnosis.Evidence, "\n")
	for _, want := range []string{"request_count=120", "top_span=web-api.db_pool_acquire", "top_span_p95_stage_ms=1400ms"} {
		if !strings.Contains(joinedEvidence, want) {
			t.Fatalf("expected evidence %q in %+v", want, response.Diagnosis.Evidence)
		}
	}
	for _, want := range []string{"FROM diagnosis_windows_1m", "FROM request_facts", "FROM request_spans", "FROM app_events"} {
		if !strings.Contains(strings.Join(queries, "\n"), want) {
			t.Fatalf("expected fallback query %q in %+v", want, queries)
		}
	}
	joinedQueries := strings.Join(queries, "\n")
	for _, want := range []string{
		"JSONExtractBool(summary_json, 'stream')",
		"JSONExtractBool(summary_json, 'streaming')",
		"NOT (JSONExtractBool(summary_json, 'sse') OR JSONExtractBool(summary_json, 'stream') OR JSONExtractBool(summary_json, 'streaming'))",
	} {
		if !strings.Contains(joinedQueries, want) {
			t.Fatalf("expected fallback query to contain %q, got %+v", want, queries)
		}
	}
}

func TestBuildAppObservabilityRulePeerTTFBQuery(t *testing.T) {
	query, err := buildAppObservabilityRulePeerTTFBQuery("app_123", appObservabilityWindow{
		Since: "2026-06-05T23:00:00Z",
		Until: "2026-06-05T23:05:00Z",
	})
	if err != nil {
		t.Fatalf("build peer TTFB query: %v", err)
	}
	for _, want := range []string{
		"app_id = 'app_123'",
		"app_id != 'app_123'",
		"trace_id IN (SELECT trace_id FROM request_facts",
		"current_ttfb_ms >= 1000",
		"peer_ttfb_ms <= 1000",
		"delta_ms >= 500",
		"peer_delta_p95_ms",
		"FORMAT JSONEachRow",
	} {
		if !strings.Contains(query, want) {
			t.Fatalf("expected peer TTFB query to contain %q, got %s", want, query)
		}
	}
}

func TestAppObservabilityBottleneckFromTopSpanClassifiesCommonStages(t *testing.T) {
	cases := []struct {
		name       string
		service    string
		stage      string
		bottleneck string
	}{
		{name: "db pool", service: "web-api", stage: "db_pool_acquire", bottleneck: "db_lock_or_query_wait"},
		{name: "api key", service: "api", stage: "api_key_checked", bottleneck: "auth_or_api_key_wait"},
		{name: "balance", service: "api", stage: "balance_checked", bottleneck: "billing_or_balance_wait"},
		{name: "channel", service: "api", stage: "channel_selected", bottleneck: "routing_or_provider_selection_wait"},
		{name: "event loop", service: "worker", stage: "event_loop_lag_ms", bottleneck: "event_loop_lag"},
		{name: "retry", service: "proxy", stage: "retry_started", bottleneck: "upstream_retry_or_cooldown"},
		{name: "cooldown", service: "proxy", stage: "provider_cooldown_wait", bottleneck: "upstream_retry_or_cooldown"},
		{name: "token pool", service: "gateway", stage: "token_acquired", bottleneck: "token_pool_wait"},
		{name: "http connect", service: "gateway", stage: "http_connect_done", bottleneck: "upstream_network_connect_wait"},
		{name: "http tls", service: "gateway", stage: "http_tls_done", bottleneck: "upstream_network_connect_wait"},
		{name: "client pool", service: "proxy", stage: "client_pool_acquired", bottleneck: "upstream_connection_pool_wait"},
		{name: "request log", service: "gateway", stage: "request_log_queue_wait_ms", bottleneck: "request_log_write_wait"},
		{name: "usage write", service: "api", stage: "usage_write_ms", bottleneck: "request_log_write_wait"},
		{name: "upstream response", service: "proxy", stage: "upstream_headers_received", bottleneck: "upstream_response_wait"},
		{name: "downstream response", service: "proxy", stage: "downstream_response_start", bottleneck: "downstream_response_wait"},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got, actions := appObservabilityBottleneckFromTopSpan(map[string]any{
				"service": tt.service,
				"stage":   tt.stage,
			})
			if got != tt.bottleneck {
				t.Fatalf("expected bottleneck %q, got %q", tt.bottleneck, got)
			}
			if len(actions) == 0 || strings.TrimSpace(actions[0]) == "" {
				t.Fatalf("expected next actions for %s.%s", tt.service, tt.stage)
			}
		})
	}
}

func TestAppObservabilityRuleDiagnosisDetectsPrePeerServiceWait(t *testing.T) {
	diagnosis := appObservabilityRuleDiagnosisFromRowsWithPeers(
		[]map[string]any{{
			"request_count":                  12,
			"error_5xx_count":                0,
			"error_4xx_count":                0,
			"not_found_count":                0,
			"error_5xx_rate":                 0,
			"error_4xx_rate":                 0,
			"not_found_rate":                 0,
			"p95_ttfb_ms":                    2600,
			"p95_duration_ms":                3100,
			"max_duration_ms":                4600,
			"edge_fallback_count":            0,
			"peer_fallback_count":            0,
			"actionable_edge_fallback_count": 0,
			"actionable_peer_fallback_count": 0,
			"stream_count":                   0,
		}},
		[]map[string]any{{
			"service":      "api",
			"stage":        "upstream_headers_received",
			"span_count":   11,
			"p95_stage_ms": 2400,
			"max_stage_ms": 4100,
			"error_count":  0,
		}},
		[]map[string]any{{
			"peer_correlated_trace_count":    10,
			"peer_delta_trace_count":         8,
			"peer_delta_trace_rate":          0.8,
			"current_peer_delta_p95_ttfb_ms": 2600,
			"peer_p95_ttfb_ms":               350,
			"peer_delta_p95_ms":              2250,
			"peer_delta_max_ms":              3100,
			"peer_sample_app_id":             "peer_app",
		}},
		nil,
	)
	if diagnosis.Bottleneck != "pre_peer_service_wait" {
		t.Fatalf("expected pre-peer service wait, got %+v", diagnosis)
	}
	joinedEvidence := strings.Join(diagnosis.Evidence, "\n")
	for _, want := range []string{"peer_correlated_trace_count=10", "peer_p95_ttfb_ms=350ms", "peer_delta_p95_ms=2250ms", "peer_sample_app_id=peer_app"} {
		if !strings.Contains(joinedEvidence, want) {
			t.Fatalf("expected evidence %q in %+v", want, diagnosis.Evidence)
		}
	}
}

func TestAppObservabilityRuleDiagnosisKeepsSpecificSpanOverPeerDelta(t *testing.T) {
	diagnosis := appObservabilityRuleDiagnosisFromRowsWithPeers(
		[]map[string]any{{
			"request_count":                  12,
			"error_5xx_count":                0,
			"error_4xx_count":                0,
			"not_found_count":                0,
			"error_5xx_rate":                 0,
			"error_4xx_rate":                 0,
			"not_found_rate":                 0,
			"p95_ttfb_ms":                    2600,
			"p95_duration_ms":                3100,
			"max_duration_ms":                4600,
			"edge_fallback_count":            0,
			"peer_fallback_count":            0,
			"actionable_edge_fallback_count": 0,
			"actionable_peer_fallback_count": 0,
			"stream_count":                   0,
		}},
		[]map[string]any{{
			"service":      "api",
			"stage":        "db_pool_acquire",
			"span_count":   11,
			"p95_stage_ms": 2400,
			"max_stage_ms": 4100,
			"error_count":  0,
		}},
		[]map[string]any{{
			"peer_correlated_trace_count":    10,
			"peer_delta_trace_count":         8,
			"peer_delta_trace_rate":          0.8,
			"current_peer_delta_p95_ttfb_ms": 2600,
			"peer_p95_ttfb_ms":               350,
			"peer_delta_p95_ms":              2250,
		}},
		nil,
	)
	if diagnosis.Bottleneck != "db_lock_or_query_wait" {
		t.Fatalf("expected specific DB span to win over peer delta, got %+v", diagnosis)
	}
}

func TestAppObservabilityRuleDiagnosisDetectsTracebackErrorBurst(t *testing.T) {
	diagnosis := appObservabilityRuleDiagnosisFromRows(
		[]map[string]any{{
			"request_count":       50,
			"error_5xx_count":     10,
			"error_4xx_count":     12,
			"not_found_count":     0,
			"error_5xx_rate":      0.2,
			"error_4xx_rate":      0.24,
			"not_found_rate":      0,
			"p95_ttfb_ms":         320,
			"p95_duration_ms":     900,
			"max_duration_ms":     2000,
			"edge_fallback_count": 0,
			"peer_fallback_count": 0,
		}},
		nil,
		[]map[string]any{{
			"event_type": "runtime_event",
			"severity":   "error",
			"message":    "Traceback: connection refused",
		}},
	)
	if diagnosis.Bottleneck != "traceback_error_burst" {
		t.Fatalf("expected traceback bottleneck, got %+v", diagnosis)
	}
	if diagnosis.Confidence < 0.9 {
		t.Fatalf("expected high confidence, got %+v", diagnosis)
	}
}

func TestAppObservabilityRuleDiagnosisIgnoresNonActionableFallback(t *testing.T) {
	diagnosis := appObservabilityRuleDiagnosisFromRows(
		[]map[string]any{{
			"request_count":                  46,
			"error_5xx_count":                0,
			"error_4xx_count":                0,
			"not_found_count":                0,
			"error_5xx_rate":                 0,
			"error_4xx_rate":                 0,
			"not_found_rate":                 0,
			"p95_ttfb_ms":                    5155,
			"p95_duration_ms":                19820,
			"max_duration_ms":                34350,
			"edge_fallback_count":            10,
			"peer_fallback_count":            0,
			"actionable_edge_fallback_count": 0,
			"actionable_peer_fallback_count": 0,
			"stream_count":                   20,
		}},
		nil,
		nil,
	)
	if diagnosis.Bottleneck == "edge_routing_fallback" {
		t.Fatalf("expected non-actionable fallback to be ignored, got %+v", diagnosis)
	}
	if diagnosis.Bottleneck != "app_latency" {
		t.Fatalf("expected latency diagnosis after ignoring static fallback, got %+v", diagnosis)
	}
	joinedEvidence := strings.Join(diagnosis.Evidence, "\n")
	for _, want := range []string{"edge_fallback_count=10", "actionable_edge_fallback_count=0"} {
		if !strings.Contains(joinedEvidence, want) {
			t.Fatalf("expected evidence %q in %+v", want, diagnosis.Evidence)
		}
	}
}

func TestAppObservabilityRuleDiagnosisDoesNotTreatStreamingTailAsReleaseRegression(t *testing.T) {
	diagnosis := appObservabilityRuleDiagnosisFromRows(
		[]map[string]any{{
			"request_count":                  20,
			"error_5xx_count":                0,
			"error_4xx_count":                0,
			"not_found_count":                0,
			"error_5xx_rate":                 0,
			"error_4xx_rate":                 0,
			"not_found_rate":                 0,
			"p95_ttfb_ms":                    900,
			"p95_duration_ms":                40340,
			"max_duration_ms":                129800,
			"edge_fallback_count":            5,
			"peer_fallback_count":            0,
			"actionable_edge_fallback_count": 0,
			"actionable_peer_fallback_count": 0,
			"stream_count":                   20,
		}},
		[]map[string]any{{
			"service":      "oaix",
			"stage":        "response_stream_end",
			"span_count":   20,
			"p95_stage_ms": 129800,
			"max_stage_ms": 129800,
			"error_count":  0,
		}},
		[]map[string]any{{
			"event_type": "deploy_event",
			"severity":   "info",
			"message":    "deploy completed",
		}},
	)
	if diagnosis.Bottleneck == "release_regression_candidate" {
		t.Fatalf("expected streaming tail not to be treated as release regression, got %+v", diagnosis)
	}
	if diagnosis.Bottleneck != "streaming_response_tail" {
		t.Fatalf("expected streaming tail diagnosis, got %+v", diagnosis)
	}
	joinedEvidence := strings.Join(diagnosis.Evidence, "\n")
	for _, want := range []string{"stream_count=20", "top_span=oaix.response_stream_end", "top_span_p95_stage_ms=1.298e+05ms"} {
		if !strings.Contains(joinedEvidence, want) {
			t.Fatalf("expected evidence %q in %+v", want, diagnosis.Evidence)
		}
	}
}

func TestAppObservabilityRuleDiagnosisDetectsNonStreamingResponseCollection(t *testing.T) {
	diagnosis := appObservabilityRuleDiagnosisFromRows(
		[]map[string]any{{
			"request_count":                  40,
			"error_5xx_count":                0,
			"error_4xx_count":                0,
			"not_found_count":                0,
			"error_5xx_rate":                 0,
			"error_4xx_rate":                 0,
			"not_found_rate":                 0,
			"p95_ttfb_ms":                    34000,
			"p95_duration_ms":                35100,
			"max_duration_ms":                52000,
			"edge_fallback_count":            0,
			"peer_fallback_count":            0,
			"actionable_edge_fallback_count": 0,
			"actionable_peer_fallback_count": 0,
			"stream_count":                   0,
		}},
		[]map[string]any{{
			"service":      "api",
			"stage":        "stream_end",
			"span_count":   40,
			"p95_stage_ms": 34200,
			"max_stage_ms": 52000,
			"error_count":  0,
		}},
		nil,
	)
	if diagnosis.Bottleneck != "non_streaming_response_collection" {
		t.Fatalf("expected non-streaming collection diagnosis, got %+v", diagnosis)
	}
	joinedActions := strings.Join(diagnosis.NextActions, "\n")
	if !strings.Contains(joinedActions, "non-streaming") {
		t.Fatalf("expected non-streaming next action, got %+v", diagnosis.NextActions)
	}
}

func TestAppObservabilityRuleDiagnosisPrefersTopSpanOverLatencyOnlyReleaseRegression(t *testing.T) {
	diagnosis := appObservabilityRuleDiagnosisFromRows(
		[]map[string]any{{
			"request_count":                  17,
			"error_5xx_count":                0,
			"error_4xx_count":                0,
			"not_found_count":                0,
			"error_5xx_rate":                 0,
			"error_4xx_rate":                 0,
			"not_found_rate":                 0,
			"p95_ttfb_ms":                    28310,
			"p95_duration_ms":                28320,
			"max_duration_ms":                28320,
			"edge_fallback_count":            0,
			"peer_fallback_count":            0,
			"actionable_edge_fallback_count": 0,
			"actionable_peer_fallback_count": 0,
			"stream_count":                   17,
		}},
		[]map[string]any{{
			"service":      "uni-api-ember",
			"stage":        "upstream_headers_received",
			"span_count":   17,
			"p95_stage_ms": 28040,
			"max_stage_ms": 28040,
			"error_count":  0,
		}},
		[]map[string]any{{
			"event_type": "deploy_event",
			"severity":   "info",
			"message":    "deploy completed",
		}},
	)
	if diagnosis.Bottleneck == "release_regression_candidate" {
		t.Fatalf("expected top span to take precedence over latency-only release regression, got %+v", diagnosis)
	}
	if diagnosis.Bottleneck != "upstream_response_wait" {
		t.Fatalf("expected upstream headers span diagnosis, got %+v", diagnosis)
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
