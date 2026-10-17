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

func TestModelLatencyCohortsNeverCompareDifferentOrUnknownModels(t *testing.T) {
	policy := model.AppReleaseGatePolicy{LatencyCohortModel: true, MinCandidateRequests: 20}
	for _, scenario := range []string{"same_model", "different_model", "missing_candidate_model", "missing_stable_model", "unscoped", "too_few_baseline", "regression"} {
		t.Run(scenario, func(t *testing.T) {
			rows := []ReleaseGateLatencyCohort{
				{ReleaseID: "new", Hostname: "app.example.test", Path: "/events", Method: "POST", Streaming: true, ModelScoped: true, Model: "reasoning", Requests: 40, P95TTFB: 40000},
				{ReleaseID: "old", Hostname: "app.example.test", Path: "/events", Method: "POST", Streaming: true, ModelScoped: true, Model: "reasoning", Requests: 21, P95TTFB: 42000},
			}
			switch scenario {
			case "different_model":
				rows[1].Model, rows[1].P95TTFB = "fast", 5000
			case "missing_candidate_model":
				rows[0].Model = ""
			case "missing_stable_model":
				rows[1].Model = ""
			case "unscoped":
				rows[0].ModelScoped = false
			case "too_few_baseline":
				rows[1].Requests = 19
			case "regression":
				rows[1].P95TTFB = 10000
			}
			metrics, failures, _ := ReleaseGateLatencyCohortComparison(rows, "new", "old", policy)
			if scenario == "same_model" {
				if len(failures) != 0 || FloatMetric(metrics, "matched_candidate_requests") != 40 {
					t.Fatal(metrics, failures)
				}
			} else if scenario == "regression" {
				if !strings.Contains(strings.Join(failures, ";"), "worse than stable") {
					t.Fatal("real same-model regression disappeared", failures)
				}
			} else if len(failures) != 1 || !strings.Contains(failures[0], "below minimum") || FloatMetric(metrics, "matched_candidate_requests") != 0 {
				t.Fatal("invalid model evidence compared", metrics, failures)
			}
		})
	}
	if !((ReleaseGateEvaluator{}).NormalizePolicy(&policy)).LatencyCohortModel {
		t.Fatal("normalization lost explicit model policy")
	}
	if ((ReleaseGateEvaluator{}).NormalizePolicy(nil)).LatencyCohortModel {
		t.Fatal("model evidence became required by default")
	}
}

func TestModelLatencyQueryBoundsAndCorrelationEvidence(t *testing.T) {
	for _, scenario := range []string{"complete", "missing_field", "truncated", "backend_error"} {
		t.Run(scenario, func(t *testing.T) {
			var query string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				query = r.URL.Query().Get("query")
				if scenario == "backend_error" {
					http.Error(w, "bounded query failed", 503)
					return
				}
				row := map[string]any{"release_id": "new", "hostname": "app.example.test", "method": "POST", "request_path": "/events", "is_stream": 1, "model_name": "reasoning", "request_count": 40, "cohort_count": 1, "p95_ttfb_ms": 40000}
				if scenario == "missing_field" {
					delete(row, "model_name")
				}
				if scenario == "truncated" {
					row["cohort_count"] = 2
				}
				_ = json.NewEncoder(w).Encode(row)
			}))
			defer server.Close()
			q := ClickHouseReleaseGateMetricsQuerier{DSN: server.URL, Now: func() time.Time { return time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC) }}
			rows, err := q.QueryReleaseGateModelLatencyCohorts(context.Background(), "app", "new", "old", 10*time.Minute)
			if scenario != "complete" {
				if err == nil {
					t.Fatal("invalid correlation response accepted")
				}
				return
			}
			if err != nil || len(rows) != 1 || !rows[0].ModelScoped || rows[0].Model != "reasoning" || rows[0].P95TTFB != 40000 {
				t.Fatal(rows, err)
			}
			for _, bound := range []string{"app_id = 'app'", "e.trace_id = m.trace_id", "e.request_path = m.request_path", "e.is_stream = m.is_stream", "e.edge_count = 1", "m.receipt_count = 1", "metadata_tenant = e.tenant_id", "<= 5000", "LIMIT 513", "max_execution_time=10", "read_overflow_mode='throw'", "join_overflow_mode='throw'"} {
				if !strings.Contains(query, bound) {
					t.Fatalf("query lacks bound %s", bound)
				}
			}
		})
	}
}
