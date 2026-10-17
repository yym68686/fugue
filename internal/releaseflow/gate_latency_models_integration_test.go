package releaseflow

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"fugue/internal/observability"
)

// Uses only an inline VALUES relation, never creates tables or inserts request
// facts. Point FUGUE_TEST_CLICKHOUSE_READONLY_DSN at an accessible test backend.
func TestModelLatencyClickHouseIntegration(t *testing.T) {
	dsn := os.Getenv("FUGUE_TEST_CLICKHOUSE_READONLY_DSN")
	if dsn == "" {
		t.Skip("FUGUE_TEST_CLICKHOUSE_READONLY_DSN is not configured")
	}
	until := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	for _, scenario := range []string{"unique", "legacy_empty_tenant", "different_app", "different_tenant", "different_trace", "different_method", "different_path", "different_stream", "completion_time_skew", "duplicate_model_receipt", "duplicate_edge_other_release", "conflicting_model_alias", "missing_model", "missing_request_id", "missing_trace_id", "boundary_metadata"} {
		t.Run(scenario, func(t *testing.T) {
			edgeTime, appTime := until.Add(-time.Minute), until.Add(-time.Minute+time.Second)
			modelValue, tenant, appID, trace, method, path, streaming := "reasoning", "tenant", "app", "trace", "POST", "/events", true
			extra := map[string]any{}
			switch scenario {
			case "legacy_empty_tenant":
				tenant = ""
			case "different_app":
				appID = "another"
			case "different_tenant":
				tenant = "another"
			case "different_trace":
				trace = "another"
			case "different_method":
				method = "GET"
			case "different_path":
				path = "/other"
			case "different_stream":
				streaming = false
			case "completion_time_skew":
				appTime = edgeTime.Add(6 * time.Second)
			case "conflicting_model_alias":
				extra["model_id"] = "fast"
			case "missing_model":
				modelValue = ""
			case "boundary_metadata":
				edgeTime = until.Add(-10 * time.Minute)
				appTime = edgeTime.Add(-time.Second)
			}
			fixtureRow := func(at time.Time, tenant, app, trace, method, path, release, model string, stream bool, ttfb int, extra map[string]any) string {
				summary := map[string]any{"streaming": stream, "path": path + "?redacted=1", "release_id": release}
				if model != "" {
					summary["model"] = model
				}
				for k, v := range extra {
					summary[k] = v
				}
				raw, _ := json.Marshal(summary)
				values := []string{quoteClickHouseString(at.Format("2006-01-02 15:04:05.000")), quoteClickHouseString(tenant), quoteClickHouseString(app), "'request'", quoteClickHouseString(trace), "'app.example.test'", quoteClickHouseString(method), quoteClickHouseString(path), quoteClickHouseString(string(raw)), strconv.Itoa(ttfb), "90000"}
				return "(" + strings.Join(values, ",") + ")"
			}
			edge := fixtureRow(edgeTime, "tenant", "app", "trace", "POST", "/events", "new", "", true, 40000, nil)
			metadata := fixtureRow(appTime, tenant, appID, trace, method, path, "", modelValue, streaming, 1, extra)
			if scenario == "missing_request_id" {
				edge, metadata = strings.ReplaceAll(edge, "'request'", "''"), strings.ReplaceAll(metadata, "'request'", "''")
			}
			if scenario == "missing_trace_id" {
				edge, metadata = strings.ReplaceAll(edge, "'trace'", "''"), strings.ReplaceAll(metadata, "'trace'", "''")
			}
			rows := []string{edge, metadata}
			if scenario == "duplicate_model_receipt" {
				rows = append(rows, metadata)
			}
			if scenario == "duplicate_edge_other_release" {
				rows = append(rows, fixtureRow(edgeTime, "tenant", "app", "trace", "POST", "/events", "unrelated", "", true, 2, nil))
			}
			values := "values('ts DateTime64(3),tenant_id String,app_id String,request_id String,trace_id String,hostname String,method String,path_template String,summary_json String,ttfb_ms UInt32,duration_ms UInt32'," + strings.Join(rows, ",") + ")"
			query := strings.Replace(releaseGateModelCohortQuery("app", "new", "old", until, 10*time.Minute), "FROM request_facts", "FROM "+values, 1)
			query = strings.Replace(query, "SETTINGS max_execution_time", "SETTINGS readonly=1, max_execution_time", 1)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			result, err := observability.NewClickHouseExporter(dsn, http.DefaultClient).QueryJSONEachRow(ctx, query, 1<<20)
			if err != nil || len(result) != 1 {
				t.Fatalf("model correlation SQL failed: %v rows=%+v", err, result)
			}
			wantModel := ""
			if scenario == "unique" || scenario == "legacy_empty_tenant" || scenario == "boundary_metadata" {
				wantModel = "reasoning"
			}
			if result[0]["model_name"] != wantModel || cohortNumber(result[0], "request_count") != 1 || FloatMetric(result[0], "p95_ttfb_ms") != 40000 {
				t.Fatal("wrong identity correlation or application timing replaced edge timing", result)
			}
		})
	}
}
