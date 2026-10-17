package releaseflow

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/observability"
)

type ReleaseGateLatencyCohort struct {
	ReleaseID   string  `json:"release_id"`
	Hostname    string  `json:"hostname"`
	Path        string  `json:"path"`
	Method      string  `json:"method"`
	Streaming   bool    `json:"streaming"`
	ModelScoped bool    `json:"model_scoped,omitempty"`
	Model       string  `json:"model,omitempty"`
	Requests    float64 `json:"request_count"`
	P95TTFB     float64 `json:"p95_ttfb_ms"`
	P99Duration float64 `json:"p99_duration_ms"`
}

type ReleaseGateLatencyCohortsQuerier interface {
	QueryReleaseGateLatencyCohorts(context.Context, string, string, string, time.Duration) ([]ReleaseGateLatencyCohort, error)
}

type ReleaseGateModelLatencyCohortsQuerier interface {
	QueryReleaseGateModelLatencyCohorts(context.Context, string, string, string, time.Duration) ([]ReleaseGateLatencyCohort, error)
}

// One raw query puts both releases in the same time window. The redacted path
// distinguishes operations behind a catch-all route; query strings never form
// part of the key. Missing or excessive evidence is an error, not a bypass.
func (q ClickHouseReleaseGateMetricsQuerier) QueryReleaseGateLatencyCohorts(ctx context.Context, appID, candidateID, stableID string, window time.Duration) ([]ReleaseGateLatencyCohort, error) {
	if strings.TrimSpace(q.DSN) == "" || candidateID == "" || stableID == "" || candidateID == stableID {
		return nil, fmt.Errorf("latency cohorts require a backend and two distinct release identities")
	}
	if window <= 0 {
		window = DefaultAppReleaseGateWindow
	}
	until := q.now()
	query := "SELECT JSONExtractString(summary_json, 'release_id') AS release_id, hostname, method, " +
		"splitByChar('?', if(JSONExtractString(summary_json, 'path') != '', JSONExtractString(summary_json, 'path'), path_template))[1] AS request_path, " +
		clickHouseStreamingPredicate() + " AS is_stream, count() AS request_count, count() OVER () AS cohort_count, " +
		"quantileTDigest(0.95)(toFloat64(ttfb_ms)) AS p95_ttfb_ms, " +
		"quantileTDigest(0.99)(toFloat64(duration_ms)) AS p99_duration_ms " +
		"FROM request_facts WHERE app_id = " + quoteClickHouseString(appID) +
		" AND ts >= " + clickHouseDateTime64Literal(until.Add(-window)) +
		" AND ts <= " + clickHouseDateTime64Literal(until) +
		" AND JSONExtractString(summary_json, 'release_id') IN (" + quoteClickHouseString(candidateID) + ", " + quoteClickHouseString(stableID) + ")" +
		" GROUP BY release_id, hostname, method, request_path, is_stream ORDER BY release_id, hostname, method, request_path, is_stream LIMIT 513 FORMAT JSONEachRow"
	return q.queryLatencyCohorts(ctx, query, candidateID, stableID, false)
}

func (q ClickHouseReleaseGateMetricsQuerier) queryLatencyCohorts(ctx context.Context, query, candidateID, stableID string, modelScoped bool) ([]ReleaseGateLatencyCohort, error) {
	client := q.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	limit := q.MaxPayloadBytes
	if limit <= 0 {
		limit = observability.DefaultClickHouseQueryMaxPayloadBytes
	}
	rows, err := observability.NewClickHouseExporter(q.DSN, client).QueryJSONEachRow(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	if len(rows) > 512 {
		return nil, fmt.Errorf("release latency cohort limit exceeded")
	}
	cohorts := make([]ReleaseGateLatencyCohort, 0, len(rows))
	for _, row := range rows {
		// Detect a backend result-byte limit returning only part of the rows.
		// QueryJSONEachRow is shared with diagnostics which permit truncation.
		if cohortNumber(row, "cohort_count") != float64(len(rows)) {
			return nil, fmt.Errorf("release latency cohort evidence is incomplete")
		}
		id, _ := row["release_id"].(string)
		host, _ := row["hostname"].(string)
		path, _ := row["request_path"].(string)
		method, _ := row["method"].(string)
		modelName, _ := row["model_name"].(string)
		if modelScoped {
			if _, present := row["model_name"].(string); !present {
				return nil, fmt.Errorf("release latency model evidence is missing")
			}
		}
		if (id != candidateID && id != stableID) || host == "" || path == "" || method == "" || cohortNumber(row, "request_count") <= 0 {
			return nil, fmt.Errorf("release latency cohort identity or count is invalid")
		}
		cohorts = append(cohorts, ReleaseGateLatencyCohort{ReleaseID: id, Hostname: host, Path: path, Method: method, Streaming: BoolMetric(row, "is_stream") || cohortNumber(row, "is_stream") != 0, ModelScoped: modelScoped, Model: modelName, Requests: cohortNumber(row, "request_count"), P95TTFB: FloatMetric(row, "p95_ttfb_ms"), P99Duration: FloatMetric(row, "p99_duration_ms")})
	}
	return cohorts, nil
}

func cohortNumber(row map[string]any, key string) float64 {
	if value, ok := row[key].(string); ok {
		number, _ := strconv.ParseFloat(value, 64)
		return number
	}
	return FloatMetric(row, key)
}

// Compare latency only for the same host, operation, method and streaming mode.
// Every compared cohort needs the existing policy's sample minimum on BOTH
// releases. Unmatched traffic remains subject to all absolute and error gates.
func ReleaseGateLatencyCohortComparison(cohorts []ReleaseGateLatencyCohort, candidateID, stableID string, policy model.AppReleaseGatePolicy) (map[string]any, []string, []string) {
	type key struct {
		host, path, method, model string
		streaming                 bool
	}
	stable := map[key]ReleaseGateLatencyCohort{}
	for _, c := range cohorts {
		if c.ReleaseID == stableID {
			if policy.LatencyCohortModel && (!c.ModelScoped || c.Model == "") {
				continue
			}
			stable[key{c.Hostname, c.Path, c.Method, c.Model, c.Streaming}] = c
		}
	}
	minimum := float64(policy.MinCandidateRequests)
	if minimum < 1 {
		minimum = 1
	}
	matched, unmatched, undersampled := 0.0, 0.0, 0.0
	comparisons := []map[string]any{}
	failures, warnings := []string{}, []string{}
	for _, c := range cohorts {
		if c.ReleaseID != candidateID {
			continue
		}
		if policy.LatencyCohortModel && (!c.ModelScoped || c.Model == "") {
			unmatched += c.Requests
			continue
		}
		b, ok := stable[key{c.Hostname, c.Path, c.Method, c.Model, c.Streaming}]
		if !ok {
			unmatched += c.Requests
			continue
		}
		if c.Requests < minimum || b.Requests < minimum {
			undersampled += c.Requests
			continue
		}
		matched += c.Requests
		comparisons = append(comparisons, map[string]any{"hostname": c.Hostname, "path": c.Path, "method": c.Method, "streaming": c.Streaming, "candidate_requests": c.Requests, "stable_requests": b.Requests, "candidate_p95_ttfb_ms": c.P95TTFB, "stable_p95_ttfb_ms": b.P95TTFB, "candidate_p99_duration_ms": c.P99Duration, "stable_p99_duration_ms": b.P99Duration})
		label := fmt.Sprintf("%s %s%s streaming=%t", c.Method, c.Hostname, c.Path, c.Streaming)
		if policy.LatencyCohortModel {
			comparisons[len(comparisons)-1]["model"] = c.Model
			label += " model=" + c.Model
		}
		if b.P95TTFB > 0 && c.P95TTFB > b.P95TTFB*1.75+250 {
			failures = append(failures, fmt.Sprintf("candidate p95 ttfb %.0fms is worse than stable %.0fms in cohort %s", c.P95TTFB, b.P95TTFB, label))
		}
		if !c.Streaming && b.P99Duration > 0 && c.P99Duration > b.P99Duration*1.75+1000 {
			failures = append(failures, fmt.Sprintf("candidate p99 duration %.0fms is worse than stable %.0fms in cohort %s", c.P99Duration, b.P99Duration, label))
		}
	}
	if matched < minimum {
		failures = append(failures, fmt.Sprintf("matched latency cohort request count %.0f is below minimum %.0f", matched, minimum))
	}
	if unmatched > 0 || undersampled > 0 {
		warnings = append(warnings, fmt.Sprintf("comparative latency excludes %.0f unmatched and %.0f undersampled candidate requests; absolute gates still include all requests", unmatched, undersampled))
	}
	metrics := map[string]any{"mode": "matched_latency_cohorts", "matched_candidate_requests": matched, "unmatched_candidate_requests": unmatched, "undersampled_candidate_requests": undersampled, "cohorts": comparisons}
	if policy.LatencyCohortModel {
		metrics["model_dimension"] = true
	}
	return metrics, failures, warnings
}
