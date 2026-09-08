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

func TestRequestExplainQueriesRealIDsAcrossRequestFactsAndPlatformEvents(t *testing.T) {
	t.Parallel()
	for _, source := range []string{"request_facts", "app_events"} {
		t.Run(source, func(t *testing.T) {
			storeState, server, _, adminKey, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
			at := time.Now().UTC().Add(-time.Minute)
			id := "edge_abcd_1234"
			// An aggregate with even the same ID must not supply the request's status.
			if err := storeState.RecordEdgePerformanceSamples([]model.EdgePerformanceSample{{ID: id, Hostname: "wrong.example.test", StatusCode: 200, SampledAt: at, SampleCount: 1}}, time.Time{}); err != nil {
				t.Fatal(err)
			}
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				query := r.URL.Query().Get("query")
				for _, want := range []string{"FROM request_facts", "FROM app_events", "event_type = 'request_fact_incomplete'", "JSONExtractString(summary_json, 'edge_request_id')", "trace_id = '" + id + "'", "LIMIT 2", "max_threads = 1", "max_memory_usage = 134217728"} {
					if !strings.Contains(query, want) {
						t.Errorf("missing %q in %s", want, query)
					}
				}
				summary := `{"edge_request_id":"edge_abcd_1234","request_content_length":40000,"request_body_read_bytes":30000,"request_body_complete":false,"origin_dns_ms":3,"origin_ttfb_ms":300001,"authorization":"Bearer private","request_body_read_error":""}`
				_ = json.NewEncoder(w).Encode(map[string]any{"ts": at.Format(time.RFC3339Nano), "evidence_source": source, "trace_id": "trace_example", "request_id": "", "status_code": 408, "hostname": "api.example.test", "method": "POST", "path_template": "/v1?token=private", "summary_json": summary})
			}))
			defer backend.Close()
			server.observabilityConfig = observability.Config{Enabled: true, ClickHouseDSN: backend.URL}.Normalize()
			recorder := performJSONRequest(t, server, http.MethodGet, "/v1/admin/requests/"+id+"/explain?since=2h", adminKey, nil)
			if recorder.Code != 200 {
				t.Fatalf("%d %s", recorder.Code, recorder.Body.String())
			}
			var result model.RequestExplainResponseEnvelope
			mustDecodeJSON(t, recorder, &result)
			e := result.Explain
			if !e.Found || e.StatusCode != 408 || e.Hostname != "api.example.test" || e.RequestBodyReadBytes != 30000 || e.ErrorClass != "edge.body_incomplete" || e.Evidence["source"] != source || e.PathPrefix != "/v1" {
				t.Fatalf("wrong request attribution: %+v", e)
			}
			if e.OriginFailureClass != "" || e.RuntimeNode != "" {
				t.Fatalf("inferred cause or current topology: %+v", e)
			}
			if strings.Contains(recorder.Body.String(), "private") {
				t.Fatal("raw request material leaked")
			}
		})
	}
}

func TestRequestExplainDistinguishesUnavailableAbsentAmbiguousAndUnauthorized(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, body, status string
		httpStatus         int
	}{
		{"empty", "", "not_found", 200},
		{"failed-backend", "password=private", "backend_unavailable", 500},
		{"ambiguous", "{}\n{}\n", "ambiguous", 200},
		{"invalid-record", "{}\n", "invalid_record", 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, server, apiKey, adminKey, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
			calls := 0
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.WriteHeader(tc.httpStatus)
				_, _ = fmt.Fprint(w, tc.body)
			}))
			defer backend.Close()
			server.observabilityConfig = observability.Config{Enabled: true, ClickHouseDSN: backend.URL}.Normalize()
			path := "/v1/admin/requests/edge_abcd_1234/explain?since=1h"
			denied := performJSONRequest(t, server, http.MethodGet, path, apiKey, nil)
			if denied.Code != 403 || calls != 0 {
				t.Fatalf("unauthorized lookup: %d calls=%d", denied.Code, calls)
			}
			response := performJSONRequest(t, server, http.MethodGet, path, adminKey, nil)
			var result model.RequestExplainResponseEnvelope
			mustDecodeJSON(t, response, &result)
			if result.Explain.Found || result.Explain.Evidence["lookup_status"] != tc.status {
				t.Fatalf("unexpected lookup: %+v", result.Explain)
			}
			if strings.Contains(response.Body.String(), "password") {
				t.Fatal("query error leaked")
			}
			server.observabilityConfig.Enabled = false
			response = performJSONRequest(t, server, http.MethodGet, path, adminKey, nil)
			mustDecodeJSON(t, response, &result)
			if result.Explain.Evidence["lookup_status"] != "not_configured" || calls != 1 {
				t.Fatalf("disabled query: %+v calls=%d", result.Explain, calls)
			}
		})
	}
}

func TestRecordedRequestQueryEscapesIdentifiersAndBoundsWindow(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	query := recordedRequestQuery("req_' OR 1=1 --", at, at.Add(time.Hour))
	for _, want := range []string{`req_\' OR 1=1 --`, "ts >= parseDateTime64BestEffort('2026-01-01T00:00:00Z')", "ts <= parseDateTime64BestEffort('2026-01-01T01:00:00Z')"} {
		if !strings.Contains(query, want) {
			t.Fatalf("missing %s in %s", want, query)
		}
	}
}

func TestRequestExplainTraceAndApplicationIDsDoNotInventMissingMeasurements(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		id     string
		status int
		class  string
	}{
		{"trace_example", 200, "none"},
		{"req_example", 408, "http.request_timeout"},
	} {
		t.Run(tc.id, func(t *testing.T) {
			_, server, _, adminKey, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
			at := time.Now().UTC().Add(-time.Minute)
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"ts": at.Format(time.RFC3339Nano), "evidence_source": "request_facts", "trace_id": "trace_example", "request_id": "req_example", "status_code": tc.status,
					"summary_json": `{"edge_request_id":"edge_abcd_1234","origin_dns_ms":8,"origin_connect_ms":12,"request_content_length":40000}`,
				})
			}))
			defer backend.Close()
			server.observabilityConfig = observability.Config{Enabled: true, ClickHouseDSN: backend.URL}.Normalize()
			response := performJSONRequest(t, server, http.MethodGet, "/v1/admin/requests/"+tc.id+"/explain?since=1h", adminKey, nil)
			var result model.RequestExplainResponseEnvelope
			mustDecodeJSON(t, response, &result)
			e := result.Explain
			if !e.Found || e.ErrorClass != tc.class || e.OriginFailureClass != "" || e.BodyIncompleteCount != 0 || e.BodyReadErrorCount != 0 {
				t.Fatalf("invented missing measurements or failure: %+v", e)
			}
			if _, exists := e.Evidence["request_body_read_complete"]; exists {
				t.Fatal("missing read-byte measurement was treated as zero")
			}
		})
	}
}
