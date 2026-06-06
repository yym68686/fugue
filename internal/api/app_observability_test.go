package api

import (
	"encoding/json"
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
