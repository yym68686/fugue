package releaseflow

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Model labels are application telemetry, enabled explicitly by the operator.
// They only partition edge timings; they cannot replace latency, release
// identity, error counts or any ordinary gate metric. Never guess a model from
// the path, a partial body or a non-unique request identifier.
func (q ClickHouseReleaseGateMetricsQuerier) QueryReleaseGateModelLatencyCohorts(ctx context.Context, appID, candidateID, stableID string, window time.Duration) ([]ReleaseGateLatencyCohort, error) {
	if strings.TrimSpace(q.DSN) == "" || appID == "" || candidateID == "" || stableID == "" || candidateID == stableID {
		return nil, fmt.Errorf("model latency cohorts require an app, backend and two distinct releases")
	}
	if window <= 0 {
		window = DefaultAppReleaseGateWindow
	}
	query := releaseGateModelCohortQuery(appID, candidateID, stableID, q.now(), window)
	return q.queryLatencyCohorts(ctx, query, candidateID, stableID, true)
}

func releaseGateModelCohortQuery(appID, candidateID, stableID string, until time.Time, window time.Duration) string {
	// Include a small boundary margin for differently timed app/edge completion
	// receipts and count identities across ALL releases in that bounded window.
	// Only target-release edge requests inside the exact gate window are scored.
	since := until.Add(-window)
	return `WITH facts AS (
 SELECT ts, tenant_id, request_id, trace_id, hostname, method, ttfb_ms, duration_ms,
 JSONExtractString(summary_json, 'release_id') AS release_id,
 splitByChar('?', if(JSONExtractString(summary_json, 'path') != '', JSONExtractString(summary_json, 'path'), path_template))[1] AS request_path,
 ` + clickHouseStreamingPredicate() + ` AS is_stream,
 if(JSONExtractString(summary_json, 'model') != '', JSONExtractString(summary_json, 'model'), JSONExtractString(summary_json, 'model_id')) AS model_label,
 (JSONHas(summary_json, 'model') OR JSONHas(summary_json, 'model_id')) AS has_model,
 (JSONExtractString(summary_json, 'model') != '' AND JSONExtractString(summary_json, 'model_id') != ''
  AND JSONExtractString(summary_json, 'model') != JSONExtractString(summary_json, 'model_id')) AS model_conflict
 FROM request_facts WHERE app_id = ` + quoteClickHouseString(appID) + `
 AND ts >= ` + clickHouseDateTime64Literal(since.Add(-5*time.Second)) + `
 AND ts <= ` + clickHouseDateTime64Literal(until.Add(5*time.Second)) + `
), metadata AS (
 SELECT request_id, trace_id, method, request_path, is_stream,
 count() AS receipt_count, any(model_label) AS model_value,
 any(model_conflict) AS conflicting_aliases, any(tenant_id) AS metadata_tenant,
 any(ts) AS metadata_at
 FROM facts WHERE release_id = '' AND has_model AND request_id != '' AND trace_id != ''
 GROUP BY request_id, trace_id, method, request_path, is_stream
), edge_facts AS (
 SELECT *, count() OVER (PARTITION BY request_id, trace_id, method, request_path, is_stream) AS edge_count
 FROM facts WHERE release_id != ''
), labeled AS (
 SELECT e.release_id, e.hostname, e.method, e.request_path, e.is_stream, e.ttfb_ms, e.duration_ms,
 if(e.request_id != '' AND e.trace_id != '' AND e.edge_count = 1 AND m.receipt_count = 1
    AND NOT m.conflicting_aliases AND length(m.model_value) BETWEEN 1 AND 256
    AND NOT match(m.model_value, '[[:cntrl:]]')
    AND (m.metadata_tenant = '' OR m.metadata_tenant = e.tenant_id)
    AND abs(dateDiff('millisecond', e.ts, m.metadata_at)) <= 5000,
    m.model_value, '') AS model_name
 FROM edge_facts e LEFT JOIN metadata m
 ON e.request_id = m.request_id AND e.trace_id = m.trace_id AND e.method = m.method
 AND e.request_path = m.request_path AND e.is_stream = m.is_stream
 WHERE e.ts >= ` + clickHouseDateTime64Literal(since) + ` AND e.ts <= ` + clickHouseDateTime64Literal(until) + `
 AND e.release_id IN (` + quoteClickHouseString(candidateID) + `, ` + quoteClickHouseString(stableID) + `)
)
SELECT release_id, hostname, method, request_path, is_stream, model_name,
 count() AS request_count, count() OVER () AS cohort_count,
 quantileTDigest(0.95)(toFloat64(ttfb_ms)) AS p95_ttfb_ms,
 quantileTDigest(0.99)(toFloat64(duration_ms)) AS p99_duration_ms
FROM labeled GROUP BY release_id, hostname, method, request_path, is_stream, model_name
ORDER BY release_id, hostname, method, request_path, is_stream, model_name
LIMIT 513 SETTINGS max_execution_time=10, max_memory_usage=268435456, max_rows_to_read=1000000,
 read_overflow_mode='throw', join_overflow_mode='throw', max_rows_in_join=100000, join_use_nulls=0
FORMAT JSONEachRow`
}
