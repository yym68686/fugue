package releaseflow

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"fugue/internal/observability"
)

type ClickHouseReleaseGateMetricsQuerier struct {
	DSN             string
	HTTPClient      *http.Client
	MaxPayloadBytes int64
	Now             func() time.Time
}

func (q ClickHouseReleaseGateMetricsQuerier) QueryReleaseGateMetrics(ctx context.Context, appID, releaseID, releaseRole string, window time.Duration) (map[string]any, error) {
	if strings.TrimSpace(q.DSN) == "" {
		return nil, fmt.Errorf("ClickHouse DSN is not configured")
	}
	if window <= 0 {
		window = DefaultAppReleaseGateWindow
	}
	until := q.now()
	since := until.Add(-window)
	rollup, rollupErr := q.queryReleaseGateRollupMetrics(ctx, appID, releaseID, releaseRole, since, until)
	if rollupErr == nil && FloatMetric(rollup, "request_count") > 0 {
		rollup["metrics_source"] = "release_gate_rollups_1m"
		return rollup, nil
	}
	raw, rawErr := q.queryReleaseGateRawFactMetrics(ctx, appID, releaseID, releaseRole, since, until)
	if rawErr != nil {
		if rollupErr != nil {
			return nil, fmt.Errorf("release gate rollups unavailable: %v; raw request facts unavailable: %w", rollupErr, rawErr)
		}
		return nil, rawErr
	}
	raw["metrics_source"] = "request_facts"
	if rollupErr != nil {
		raw["rollup_fallback_reason"] = rollupErr.Error()
	}
	return raw, nil
}

func (q ClickHouseReleaseGateMetricsQuerier) queryReleaseGateRollupMetrics(ctx context.Context, appID, releaseID, releaseRole string, since, until time.Time) (map[string]any, error) {
	queryText := "SELECT " +
		"sum(request_count) AS request_count, " +
		"sum(error_5xx_count) AS error_5xx_count, " +
		"sum(edge_upstream_error_count) AS edge_upstream_error_count, " +
		"max(p95_ttfb_ms) AS p95_ttfb_ms, " +
		"max(p99_duration_ms) AS p99_duration_ms " +
		"FROM release_gate_rollups_1m WHERE app_id = " + quoteClickHouseString(appID) +
		" AND minute >= toStartOfMinute(" + clickHouseDateTime64Literal(since) + ")" +
		" AND minute <= toStartOfMinute(" + clickHouseDateTime64Literal(until) + ")" +
		" AND " + clickHouseReleaseGateRollupCondition(releaseID, releaseRole) +
		" FORMAT JSONEachRow"
	return q.queryReleaseGateMetricRow(ctx, queryText)
}

func (q ClickHouseReleaseGateMetricsQuerier) queryReleaseGateRawFactMetrics(ctx context.Context, appID, releaseID, releaseRole string, since, until time.Time) (map[string]any, error) {
	queryText := "SELECT " +
		"count() AS request_count, " +
		"countIf(status_code >= 500) AS error_5xx_count, " +
		"countIf(error_type = 'upstream_error') AS edge_upstream_error_count, " +
		"quantileTDigest(0.95)(toFloat64(ttfb_ms)) AS p95_ttfb_ms, " +
		"quantileTDigestIf(0.99)(toFloat64(duration_ms), NOT " + clickHouseStreamingPredicate() + ") AS p99_duration_ms " +
		"FROM request_facts WHERE app_id = " + quoteClickHouseString(appID) +
		" AND ts >= " + clickHouseDateTime64Literal(since) +
		" AND ts <= " + clickHouseDateTime64Literal(until) +
		" AND " + clickHouseReleaseGateMetricCondition(releaseID, releaseRole) +
		" FORMAT JSONEachRow"
	return q.queryReleaseGateMetricRow(ctx, queryText)
}

func (q ClickHouseReleaseGateMetricsQuerier) queryReleaseGateMetricRow(ctx context.Context, queryText string) (map[string]any, error) {
	maxPayloadBytes := q.MaxPayloadBytes
	if maxPayloadBytes <= 0 {
		maxPayloadBytes = observability.DefaultClickHouseQueryMaxPayloadBytes
	}
	client := q.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	rows, err := observability.NewClickHouseExporter(q.DSN, client).QueryJSONEachRow(ctx, queryText, maxPayloadBytes)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return map[string]any{}, nil
	}
	row := rows[0]
	requestCount := FloatMetric(row, "request_count")
	error5xxCount := FloatMetric(row, "error_5xx_count")
	upstreamErrorCount := FloatMetric(row, "edge_upstream_error_count")
	error5xxRate := 0.0
	upstreamErrorRate := 0.0
	if requestCount > 0 {
		error5xxRate = error5xxCount / requestCount
		upstreamErrorRate = upstreamErrorCount / requestCount
	}
	return map[string]any{
		"request_count":             requestCount,
		"error_5xx_count":           error5xxCount,
		"edge_upstream_error_count": upstreamErrorCount,
		"error_5xx_rate":            error5xxRate,
		"edge_upstream_error_rate":  upstreamErrorRate,
		"p95_ttfb_ms":               FloatMetric(row, "p95_ttfb_ms"),
		"p99_duration_ms":           FloatMetric(row, "p99_duration_ms"),
	}, nil
}

func (q ClickHouseReleaseGateMetricsQuerier) now() time.Time {
	if q.Now != nil {
		return q.Now().UTC()
	}
	return time.Now().UTC()
}

func clickHouseReleaseGateMetricCondition(releaseID, releaseRole string) string {
	releaseID = strings.TrimSpace(releaseID)
	if releaseID != "" {
		return "JSONExtractString(summary_json, 'release_id') = " + quoteClickHouseString(releaseID)
	}
	releaseRole = strings.TrimSpace(releaseRole)
	if releaseRole != "" {
		return "JSONExtractString(summary_json, 'release_role') = " + quoteClickHouseString(releaseRole)
	}
	return "1 = 1"
}

func clickHouseReleaseGateRollupCondition(releaseID, releaseRole string) string {
	releaseID = strings.TrimSpace(releaseID)
	if releaseID != "" {
		return "release_id = " + quoteClickHouseString(releaseID)
	}
	releaseRole = strings.TrimSpace(releaseRole)
	if releaseRole != "" {
		return "release_role = " + quoteClickHouseString(releaseRole)
	}
	return "1 = 1"
}

func clickHouseStreamingPredicate() string {
	return "(JSONExtractBool(summary_json, 'sse') OR JSONExtractBool(summary_json, 'stream') OR JSONExtractBool(summary_json, 'streaming'))"
}

func clickHouseDateTime64Literal(value time.Time) string {
	if value.IsZero() {
		value = time.Now().UTC()
	}
	return "toDateTime64(" + quoteClickHouseString(value.UTC().Format("2006-01-02 15:04:05.000")) + ", 3, 'UTC')"
}

func quoteClickHouseString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "\\'") + "'"
}
