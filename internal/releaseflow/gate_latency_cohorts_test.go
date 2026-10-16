package releaseflow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestLatencyCohortsSeparateTrafficMixAndPreserveRegressionChecks(t *testing.T) {
	policy := model.AppReleaseGatePolicy{MinCandidateRequests: 20, MaxP95TTFBMilliseconds: 60000, MaxP99DurationMilliseconds: 30000}
	baseline := []ReleaseGateLatencyCohort{
		{ReleaseID: "new", Hostname: "app.example.test", Path: "/ready", Method: "GET", Requests: 25, P95TTFB: 150, P99Duration: 180},
		{ReleaseID: "old", Hostname: "app.example.test", Path: "/ready", Method: "GET", Requests: 90, P95TTFB: 140, P99Duration: 260},
		{ReleaseID: "new", Hostname: "app.example.test", Path: "/events", Method: "POST", Streaming: true, Requests: 70, P95TTFB: 36000, P99Duration: 80000},
		{ReleaseID: "old", Hostname: "app.example.test", Path: "/events", Method: "POST", Streaming: true, Requests: 60, P95TTFB: 35000, P99Duration: 120000},
		{ReleaseID: "new", Hostname: "app.example.test", Path: "/jobs", Method: "POST", Requests: 8, P95TTFB: 8000, P99Duration: 8000},
	}
	for _, scenario := range []string{"different_mix", "stream_ttfb_regression", "ordinary_duration_regression", "stream_duration_is_not_ordinary_latency", "undersampled_baseline", "different_method", "different_host", "different_stream_mode", "no_matched_samples"} {
		t.Run(scenario, func(t *testing.T) {
			cohorts := append([]ReleaseGateLatencyCohort(nil), baseline...)
			wantFailure := false
			switch scenario {
			case "stream_ttfb_regression":
				cohorts[2].P95TTFB = 62000
				wantFailure = true
			case "ordinary_duration_regression":
				cohorts[0].P99Duration = 2000
				wantFailure = true
			case "stream_duration_is_not_ordinary_latency":
				cohorts[2].P99Duration = 500000
			case "undersampled_baseline":
				cohorts[3].Requests = 1
				cohorts[2].P95TTFB = 50000
			case "different_method":
				cohorts[3].Method = "GET"
				cohorts[2].P95TTFB = 50000
			case "different_host":
				cohorts[3].Hostname = "other.example.test"
				cohorts[2].P95TTFB = 50000
			case "different_stream_mode":
				cohorts[3].Streaming = false
				cohorts[2].P95TTFB = 50000
			case "no_matched_samples":
				cohorts = []ReleaseGateLatencyCohort{baseline[4]}
				wantFailure = true
			}
			m, failures, warnings := ReleaseGateLatencyCohortComparison(cohorts, "new", "old", policy)
			if (len(failures) > 0) != wantFailure {
				t.Fatalf("unexpected failures: %v", failures)
			}
			if len(warnings) == 0 || FloatMetric(m, "unmatched_candidate_requests") < 8 {
				t.Fatal("unmatched requests silently omitted", m, warnings)
			}
		})
	}
	comparison := map[string]any{"candidate_request_count": 100., "stable_request_count": 100., "latency_cohorts_evaluated": true, "candidate_p95_ttfb_ms": 50000., "stable_p95_ttfb_ms": 100., "candidate_error_5xx_rate": 0.1}
	if failures := ReleaseGateComparisonFailures(comparison, policy); len(failures) != 1 || !strings.Contains(failures[0], "5xx") {
		t.Fatal("cohorts bypassed error checks or repeated mixed latency", failures)
	}
	if failures := ReleaseGateMetricFailures(map[string]any{"request_count": 100., "p95_ttfb_ms": 61000.}, policy); !releaseflowStringsContain(failures, "p95 ttfb") {
		t.Fatal("absolute latency guard disappeared", failures)
	}
}

func TestClickHouseLatencyCohortsQueryIsExactBoundedAndComplete(t *testing.T) {
	for _, scenario := range []string{"complete", "truncated", "foreign_release", "query_error"} {
		t.Run(scenario, func(t *testing.T) {
			var query string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				query = r.URL.Query().Get("query")
				if scenario == "query_error" {
					http.Error(w, "unavailable", 503)
					return
				}
				row := map[string]any{"release_id": "new", "hostname": "app.example.test", "request_path": "/events", "method": "POST", "is_stream": 1, "request_count": "30", "cohort_count": "1", "p95_ttfb_ms": 35000, "p99_duration_ms": 90000}
				if scenario == "truncated" {
					row["cohort_count"] = "2"
				}
				if scenario == "foreign_release" {
					row["release_id"] = "unrelated"
				}
				json.NewEncoder(w).Encode(row)
			}))
			defer server.Close()
			q := ClickHouseReleaseGateMetricsQuerier{DSN: server.URL, Now: func() time.Time { return time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC) }}
			rows, err := q.QueryReleaseGateLatencyCohorts(context.Background(), "app", "new", "old", 10*time.Minute)
			if scenario != "complete" {
				if err == nil {
					t.Fatal("invalid evidence accepted")
				}
				return
			}
			if err != nil || len(rows) != 1 || rows[0].Requests != 30 || !rows[0].Streaming {
				t.Fatal(rows, err)
			}
			for _, part := range []string{"IN ('new', 'old')", "2026-07-07 11:50:00.000", "2026-07-07 12:00:00.000", "splitByChar('?'", "count() OVER () AS cohort_count", "GROUP BY release_id, hostname, method, request_path, is_stream", "LIMIT 513"} {
				if !strings.Contains(query, part) {
					t.Fatalf("query lacks %s: %s", part, query)
				}
			}
		})
	}
}
