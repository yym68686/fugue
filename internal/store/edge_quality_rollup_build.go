package store

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

type EdgeQualityPercentileMetric int16

const (
	EdgeQualityPercentileTTFB EdgeQualityPercentileMetric = iota + 1
	EdgeQualityPercentileUpload
	EdgeQualityPercentileMinWindow
	EdgeQualityPercentileMaxReadGap
	EdgeQualityPercentileResponseEgress
	EdgeQualityPercentileResponseWrite
)

type EdgeQualityPercentileValueKey struct {
	BucketKey string
	Metric    EdgeQualityPercentileMetric
	Value     int64
}

type EdgeQualityPercentiles struct {
	P10 float64
	P50 float64
	P95 float64
	P99 float64
}

type EdgeQualityPercentileSet [EdgeQualityPercentileResponseWrite + 1]EdgeQualityPercentiles

type EdgeQualityPercentileWindow struct {
	ID        int64
	Window    string
	StartedAt time.Time
	EndedAt   time.Time
}

// EdgeQualityRollupBuildSession keeps the raw-sample read snapshot, exact
// percentile scratch data, rollup writes, and watermarks isolated from the
// last-known-good rollups until Commit succeeds.
type EdgeQualityRollupBuildSession struct {
	store *Store
	ctx   context.Context

	readTx  *sql.Tx
	writeTx *sql.Tx

	filePercentileWeights map[EdgeQualityPercentileValueKey]int64
	percentileWindows     []EdgeQualityPercentileWindow
	closed                bool
}

func (s *Store) BeginEdgeQualityRollupBuild(ctx context.Context) (*EdgeQualityRollupBuildSession, error) {
	if s == nil {
		return nil, ErrInvalidInput
	}
	if ctx == nil {
		ctx = context.Background()
	}
	session := &EdgeQualityRollupBuildSession{
		store: s,
		ctx:   ctx,
	}
	if !s.usingDatabase() {
		session.filePercentileWeights = map[EdgeQualityPercentileValueKey]int64{}
		return session, nil
	}
	if err := s.ensureDatabaseReady(); err != nil {
		return nil, err
	}
	readTx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin edge quality rollup read transaction: %w", err)
	}
	session.readTx = readTx
	writeTx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		_ = readTx.Rollback()
		return nil, fmt.Errorf("begin edge quality rollup write transaction: %w", err)
	}
	session.writeTx = writeTx
	return session, nil
}

func (b *EdgeQualityRollupBuildSession) SetPercentileWindows(windows []EdgeQualityPercentileWindow) {
	if b == nil || b.closed {
		return
	}
	b.percentileWindows = append(b.percentileWindows[:0], windows...)
}

func (b *EdgeQualityRollupBuildSession) NeedsPercentileWeights() bool {
	return b != nil && !b.closed && b.writeTx == nil
}

func (b *EdgeQualityRollupBuildSession) Rollback() {
	if b == nil || b.closed {
		return
	}
	b.closed = true
	if b.readTx != nil {
		_ = b.readTx.Rollback()
		b.readTx = nil
	}
	if b.writeTx != nil {
		_ = b.writeTx.Rollback()
		b.writeTx = nil
	}
}

func (b *EdgeQualityRollupBuildSession) Watermarks() (map[string]time.Time, error) {
	if b == nil || b.closed {
		return nil, fmt.Errorf("edge quality rollup build session is closed")
	}
	watermarks := map[string]time.Time{}
	if b.writeTx != nil {
		rows, err := b.writeTx.QueryContext(b.ctx, `SELECT window_name, window_ended_at FROM fugue_edge_quality_rollup_watermarks`)
		if err != nil {
			return nil, fmt.Errorf("list edge quality rollup watermarks: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var window string
			var endedAt time.Time
			if err := rows.Scan(&window, &endedAt); err != nil {
				return nil, fmt.Errorf("scan edge quality rollup watermark: %w", err)
			}
			watermarks[strings.TrimSpace(strings.ToLower(window))] = endedAt.UTC()
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterate edge quality rollup watermarks: %w", err)
		}
		return watermarks, nil
	}
	err := b.store.withLockedState(false, func(state *model.State) error {
		for window, endedAt := range state.EdgeQualityWatermarks {
			watermarks[strings.TrimSpace(strings.ToLower(window))] = endedAt.UTC()
		}
		for _, rollup := range state.EdgeQualityRollups {
			window := strings.TrimSpace(strings.ToLower(rollup.Window))
			if window != "" && watermarks[window].Before(rollup.WindowEndedAt) {
				watermarks[window] = rollup.WindowEndedAt.UTC()
			}
		}
		return nil
	})
	return watermarks, err
}

func (b *EdgeQualityRollupBuildSession) ForEachSampleChunk(since, before time.Time, chunkSize int, fn func([]model.EdgePerformanceSample) error) error {
	if b == nil || b.closed {
		return fmt.Errorf("edge quality rollup build session is closed")
	}
	if !before.After(since) {
		return nil
	}
	if chunkSize <= 0 {
		chunkSize = 128
	}
	if fn == nil {
		fn = func([]model.EdgePerformanceSample) error { return nil }
	}
	if b.readTx == nil {
		return b.store.withLockedState(false, func(state *model.State) error {
			chunk := make([]model.EdgePerformanceSample, 0, chunkSize)
			for _, sample := range state.EdgePerformanceSamples {
				if sample.SampledAt.Before(since) || !sample.SampledAt.Before(before) {
					continue
				}
				chunk = append(chunk, sample)
				if len(chunk) == chunkSize {
					if err := fn(chunk); err != nil {
						return err
					}
					chunk = chunk[:0]
				}
			}
			if len(chunk) > 0 {
				return fn(chunk)
			}
			return nil
		})
	}
	rows, err := b.readTx.QueryContext(b.ctx, pgEdgeQualityRollupSampleQuery, since.UTC(), before.UTC())
	if err != nil {
		return fmt.Errorf("query edge performance rollup samples: %w", err)
	}
	chunk := make([]model.EdgePerformanceSample, 0, chunkSize)
	for rows.Next() {
		sample, err := scanEdgeQualityRollupSample(rows)
		if err != nil {
			_ = rows.Close()
			return err
		}
		chunk = append(chunk, sample)
		if len(chunk) == chunkSize {
			if err := fn(chunk); err != nil {
				_ = rows.Close()
				return err
			}
			chunk = chunk[:0]
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("iterate edge performance rollup samples: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close edge performance rollup sample cursor: %w", err)
	}
	if len(chunk) > 0 {
		if err := fn(chunk); err != nil {
			return err
		}
	}
	return nil
}

func (b *EdgeQualityRollupBuildSession) finishRead() error {
	if b == nil || b.readTx == nil {
		return nil
	}
	if err := b.readTx.Commit(); err != nil {
		return fmt.Errorf("commit edge quality rollup read transaction: %w", err)
	}
	b.readTx = nil
	return nil
}

func (b *EdgeQualityRollupBuildSession) AddPercentileWeights(weights map[EdgeQualityPercentileValueKey]int) error {
	if b == nil || b.closed {
		return fmt.Errorf("edge quality rollup build session is closed")
	}
	if len(weights) == 0 {
		return nil
	}
	if b.writeTx == nil {
		for key, weight := range weights {
			if strings.TrimSpace(key.BucketKey) == "" || key.Metric <= 0 || key.Value <= 0 || weight <= 0 {
				continue
			}
			b.filePercentileWeights[key] += int64(weight)
		}
		return nil
	}
	return nil
}

func (b *EdgeQualityRollupBuildSession) Percentiles() (map[string]EdgeQualityPercentileSet, error) {
	if b == nil || b.closed {
		return nil, fmt.Errorf("edge quality rollup build session is closed")
	}
	if b.writeTx == nil {
		return groupedEdgeQualityPercentiles(b.filePercentileWeights), nil
	}
	if len(b.percentileWindows) == 0 {
		return nil, nil
	}
	if b.readTx == nil {
		return nil, fmt.Errorf("edge quality rollup read transaction is closed")
	}
	query, args := pgEdgeQualityPercentilesQueryForWindows(b.percentileWindows)
	rows, err := b.readTx.QueryContext(b.ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("calculate edge quality percentiles: %w", err)
	}
	defer rows.Close()
	results := map[string]EdgeQualityPercentileSet{}
	for rows.Next() {
		var targetID int64
		var hostname, trafficClass, method, pathPrefixBucket string
		var clientScopeKind, clientScopeValue, edgeGroupID, edgeID string
		var metric int16
		var values EdgeQualityPercentiles
		if err := rows.Scan(
			&targetID,
			&hostname,
			&trafficClass,
			&method,
			&pathPrefixBucket,
			&clientScopeKind,
			&clientScopeValue,
			&edgeGroupID,
			&edgeID,
			&metric,
			&values.P10,
			&values.P50,
			&values.P95,
			&values.P99,
		); err != nil {
			return nil, fmt.Errorf("scan edge quality percentiles: %w", err)
		}
		bucketKey := fmt.Sprintf("%d\x00%s", targetID, strings.Join([]string{
			hostname,
			trafficClass,
			method,
			pathPrefixBucket,
			clientScopeKind,
			clientScopeValue,
			edgeGroupID,
			edgeID,
		}, "\x00"))
		byMetric := results[bucketKey]
		byMetric[EdgeQualityPercentileMetric(metric)] = values
		results[bucketKey] = byMetric
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate edge quality percentiles: %w", err)
	}
	return results, nil
}

func (b *EdgeQualityRollupBuildSession) Commit(rollups []model.EdgeQualityRollup, retentionBefore, watermarks map[string]time.Time) error {
	if b == nil || b.closed {
		return fmt.Errorf("edge quality rollup build session is closed")
	}
	if err := b.finishRead(); err != nil {
		return err
	}
	now := time.Now().UTC()
	if b.writeTx == nil {
		err := b.store.withLockedState(true, func(state *model.State) error {
			applyEdgeQualityRollupsToState(state, rollups, retentionBefore, now)
			for window, endedAt := range watermarks {
				window = strings.TrimSpace(strings.ToLower(window))
				if window == "" || endedAt.IsZero() {
					continue
				}
				if current := state.EdgeQualityWatermarks[window]; current.Before(endedAt) {
					state.EdgeQualityWatermarks[window] = endedAt.UTC()
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		b.closed = true
		return nil
	}
	if err := pgApplyEdgeQualityRollups(b.ctx, b.writeTx, rollups, retentionBefore, now); err != nil {
		return err
	}
	for window, endedAt := range watermarks {
		window = strings.TrimSpace(strings.ToLower(window))
		if window == "" || endedAt.IsZero() {
			continue
		}
		if _, err := b.writeTx.ExecContext(b.ctx, `
INSERT INTO fugue_edge_quality_rollup_watermarks (window_name, window_ended_at, updated_at)
VALUES ($1, $2, $3)
ON CONFLICT (window_name) DO UPDATE SET
	window_ended_at = EXCLUDED.window_ended_at,
	updated_at = EXCLUDED.updated_at
WHERE fugue_edge_quality_rollup_watermarks.window_ended_at < EXCLUDED.window_ended_at`, window, endedAt.UTC(), now); err != nil {
			return fmt.Errorf("upsert edge quality rollup watermark: %w", err)
		}
	}
	if err := b.writeTx.Commit(); err != nil {
		return fmt.Errorf("commit edge quality rollups and watermarks: %w", err)
	}
	b.writeTx = nil
	b.closed = true
	return nil
}

const pgEdgeQualityRollupSampleQuery = `
SELECT edge_id, edge_group_id, hostname, client_country, client_region, client_asn,
	path_prefix, method, traffic_class, ttfb_ms, upstream_ms, total_ms,
	sample_count, cache_hit_count, cache_observation_count, error_count,
	body_read_block_ms, upload_effective_bps, min_window_bps, max_read_gap_ms,
	body_incomplete_count, body_read_error_count, response_write_ms, response_egress_bps,
	origin_dns_ms, origin_connect_ms, origin_request_write_ms, origin_response_wait_ms, origin_ttfb_ms, origin_total_ms,
	client_cancel_count, active_requests, active_body_buffers, goroutine_count, memory_alloc_bytes,
	client_tcp_rtt_ms, client_tcp_min_rtt_ms, client_tcp_rttvar_ms,
	client_tcp_retrans_rate, client_tcp_bytes_retrans_rate, client_tcp_rto_rate, client_tcp_delivery_rate_bps,
	sampled_at
FROM fugue_edge_performance_samples
WHERE sampled_at >= $1 AND sampled_at < $2
ORDER BY sampled_at ASC, hostname ASC, edge_group_id ASC, id ASC`

func scanEdgeQualityRollupSample(rows *sql.Rows) (model.EdgePerformanceSample, error) {
	var sample model.EdgePerformanceSample
	var ttfb, upstream, total sql.NullInt64
	var bodyReadBlock, uploadEffective, minWindow, maxReadGap sql.NullInt64
	var responseWrite, responseEgress sql.NullInt64
	var originDNS, originConnect, originWrite, originWait, originTTFB, originTotal sql.NullInt64
	var memoryAlloc, clientTCPDeliveryBPS sql.NullInt64
	var sampleCount, cacheHitCount, cacheObservationCount, errorCount sql.NullInt64
	var bodyIncompleteCount, bodyReadErrorCount, clientCancelCount sql.NullInt64
	var activeRequests, activeBodyBuffers, goroutineCount sql.NullInt64
	var clientTCPRTT, clientTCPMinRTT, clientTCPRTTVar sql.NullFloat64
	var clientTCPRetransRate, clientTCPBytesRetransRate, clientTCPRTORate sql.NullFloat64
	if err := rows.Scan(
		&sample.EdgeID,
		&sample.EdgeGroupID,
		&sample.Hostname,
		&sample.ClientCountry,
		&sample.ClientRegion,
		&sample.ClientASN,
		&sample.PathPrefix,
		&sample.Method,
		&sample.TrafficClass,
		&ttfb,
		&upstream,
		&total,
		&sampleCount,
		&cacheHitCount,
		&cacheObservationCount,
		&errorCount,
		&bodyReadBlock,
		&uploadEffective,
		&minWindow,
		&maxReadGap,
		&bodyIncompleteCount,
		&bodyReadErrorCount,
		&responseWrite,
		&responseEgress,
		&originDNS,
		&originConnect,
		&originWrite,
		&originWait,
		&originTTFB,
		&originTotal,
		&clientCancelCount,
		&activeRequests,
		&activeBodyBuffers,
		&goroutineCount,
		&memoryAlloc,
		&clientTCPRTT,
		&clientTCPMinRTT,
		&clientTCPRTTVar,
		&clientTCPRetransRate,
		&clientTCPBytesRetransRate,
		&clientTCPRTORate,
		&clientTCPDeliveryBPS,
		&sample.SampledAt,
	); err != nil {
		return model.EdgePerformanceSample{}, fmt.Errorf("scan edge performance rollup sample: %w", err)
	}
	sample.TTFBMS = edgePerformanceInt64FromNull(ttfb)
	sample.UpstreamMS = edgePerformanceInt64FromNull(upstream)
	sample.TotalMS = edgePerformanceInt64FromNull(total)
	sample.SampleCount = int(sampleCount.Int64)
	sample.CacheHitCount = int(cacheHitCount.Int64)
	sample.CacheObservationCount = int(cacheObservationCount.Int64)
	sample.ErrorCount = int(errorCount.Int64)
	sample.BodyReadBlockMS = edgePerformanceInt64FromNull(bodyReadBlock)
	sample.UploadEffectiveBPS = edgePerformanceInt64FromNull(uploadEffective)
	sample.MinWindowBPS = edgePerformanceInt64FromNull(minWindow)
	sample.MaxReadGapMS = edgePerformanceInt64FromNull(maxReadGap)
	sample.BodyIncompleteCount = int(bodyIncompleteCount.Int64)
	sample.BodyReadErrorCount = int(bodyReadErrorCount.Int64)
	sample.ResponseWriteMS = edgePerformanceInt64FromNull(responseWrite)
	sample.ResponseEgressBPS = edgePerformanceInt64FromNull(responseEgress)
	sample.OriginDNSMS = edgePerformanceInt64FromNull(originDNS)
	sample.OriginConnectMS = edgePerformanceInt64FromNull(originConnect)
	sample.OriginRequestWriteMS = edgePerformanceInt64FromNull(originWrite)
	sample.OriginResponseWaitMS = edgePerformanceInt64FromNull(originWait)
	sample.OriginTTFBMS = edgePerformanceInt64FromNull(originTTFB)
	sample.OriginTotalMS = edgePerformanceInt64FromNull(originTotal)
	sample.ClientCancelCount = int(clientCancelCount.Int64)
	sample.ActiveRequests = int(activeRequests.Int64)
	sample.ActiveBodyBuffers = int(activeBodyBuffers.Int64)
	sample.GoroutineCount = int(goroutineCount.Int64)
	sample.MemoryAllocBytes = edgePerformanceInt64FromNull(memoryAlloc)
	sample.ClientTCPRTTMS = edgePerformanceFloat64FromNull(clientTCPRTT)
	sample.ClientTCPMinRTTMS = edgePerformanceFloat64FromNull(clientTCPMinRTT)
	sample.ClientTCPRTTVarMS = edgePerformanceFloat64FromNull(clientTCPRTTVar)
	sample.ClientTCPRetransRate = edgePerformanceFloat64FromNull(clientTCPRetransRate)
	sample.ClientTCPBytesRetransRate = edgePerformanceFloat64FromNull(clientTCPBytesRetransRate)
	sample.ClientTCPRTORate = edgePerformanceFloat64FromNull(clientTCPRTORate)
	sample.ClientTCPDeliveryBPS = edgePerformanceInt64FromNull(clientTCPDeliveryBPS)
	return sample, nil
}

func pgEdgeQualityPercentilesQueryForWindows(windows []EdgeQualityPercentileWindow) (string, []any) {
	var windowValues strings.Builder
	args := make([]any, 0, len(windows)*4)
	for index, window := range windows {
		if index > 0 {
			windowValues.WriteByte(',')
		}
		base := index*4 + 1
		fmt.Fprintf(&windowValues, "($%d::bigint,$%d::text,$%d::timestamptz,$%d::timestamptz)", base, base+1, base+2, base+3)
		args = append(args, window.ID, window.Window, window.StartedAt.UTC(), window.EndedAt.UTC())
	}
	query := fmt.Sprintf(`
WITH target_windows(target_id, window_name, started_at, ended_at) AS (VALUES %s),
raw_samples AS MATERIALIZED (
	SELECT hostname, traffic_class, method, path_prefix, client_country, client_region, client_asn,
		edge_group_id, edge_id, sample_count, ttfb_ms, upload_effective_bps, min_window_bps,
		max_read_gap_ms, response_egress_bps, response_write_ms, sampled_at
	FROM fugue_edge_performance_samples
	WHERE sampled_at >= (SELECT MIN(started_at) FROM target_windows)
		AND sampled_at < (SELECT MAX(ended_at) FROM target_windows)
),
base_samples AS (
	SELECT w.target_id, TRIM(BOTH '.' FROM LOWER(BTRIM(s.hostname))) AS hostname,
		CASE WHEN LOWER(BTRIM(s.traffic_class)) IN ('large_body_api','small_api','dynamic_api','static_cacheable','streaming','sse','websocket','html_dynamic') THEN LOWER(BTRIM(s.traffic_class)) ELSE '' END AS traffic_class,
		UPPER(BTRIM(s.method)) AS method,
		CASE
			WHEN s.path_prefix = '' OR s.path_prefix = '/' THEN ''
			WHEN s.path_prefix LIKE '/_next/static%%' THEN '/_next/static/*'
			WHEN s.path_prefix LIKE '/assets%%' THEN '/assets/*'
			WHEN s.path_prefix LIKE '/api%%' THEN '/api/*'
			WHEN s.path_prefix LIKE '/upload%%' THEN '/upload/*'
			WHEN s.path_prefix LIKE '/stream%%' THEN '/stream/*'
			ELSE s.path_prefix
		END AS path_prefix_bucket,
		LOWER(BTRIM(s.client_country)) AS client_country,
		LOWER(BTRIM(s.client_region)) AS client_region,
		LOWER(BTRIM(s.client_asn)) AS client_asn,
		BTRIM(s.edge_group_id) AS edge_group_id,
		BTRIM(s.edge_id) AS edge_id,
		GREATEST(s.sample_count, 1)::bigint AS weight,
		s.ttfb_ms,
		CASE WHEN s.min_window_bps > 0 AND (s.upload_effective_bps <= 0 OR s.min_window_bps < s.upload_effective_bps) THEN s.min_window_bps ELSE s.upload_effective_bps END AS upload_bps,
		s.min_window_bps, s.max_read_gap_ms, s.response_egress_bps, s.response_write_ms
	FROM raw_samples s
	JOIN target_windows w ON s.sampled_at >= w.started_at AND s.sampled_at < w.ended_at
	WHERE BTRIM(s.hostname) <> '' AND BTRIM(s.edge_group_id) <> ''
),
expanded AS (
	SELECT b.target_id, b.hostname, b.traffic_class, b.method, b.path_prefix_bucket,
		sc.kind AS client_scope_kind, sc.value AS client_scope_value, b.edge_group_id, b.edge_id,
		v.metric, v.value, b.weight
	FROM base_samples b
	CROSS JOIN LATERAL (
		VALUES ('global', 'global'),
			('country', NULLIF(b.client_country, '')),
			('region', CASE WHEN b.client_country <> '' AND b.client_region <> '' THEN b.client_country || ':' || b.client_region END),
			('asn', NULLIF(b.client_asn, ''))
	) sc(kind, value)
	CROSS JOIN LATERAL (VALUES
		(%d::smallint, b.ttfb_ms),
		(%d::smallint, b.upload_bps),
		(%d::smallint, b.min_window_bps),
		(%d::smallint, b.max_read_gap_ms),
		(%d::smallint, b.response_egress_bps),
		(%d::smallint, b.response_write_ms)
	) v(metric, value)
	WHERE sc.value IS NOT NULL AND v.value > 0
),
grouped_sets AS (
	SELECT target_id,
		CASE WHEN GROUPING(hostname) = 1 THEN '__platform__' ELSE hostname END AS hostname,
		traffic_class, method, path_prefix_bucket, client_scope_kind, client_scope_value, edge_group_id,
		CASE WHEN GROUPING(edge_id) = 1 THEN '' ELSE edge_id END AS edge_id,
		GROUPING(edge_id) AS edge_grouped,
		metric, value, SUM(weight)::bigint AS weight
	FROM expanded
	GROUP BY GROUPING SETS (
		(target_id, hostname, traffic_class, method, path_prefix_bucket, client_scope_kind, client_scope_value, edge_group_id, edge_id, metric, value),
		(target_id, hostname, traffic_class, method, path_prefix_bucket, client_scope_kind, client_scope_value, edge_group_id, metric, value),
		(target_id, traffic_class, method, path_prefix_bucket, client_scope_kind, client_scope_value, edge_group_id, edge_id, metric, value),
		(target_id, traffic_class, method, path_prefix_bucket, client_scope_kind, client_scope_value, edge_group_id, metric, value)
	)
),
grouped AS (
	SELECT target_id, hostname, traffic_class, method, path_prefix_bucket,
		client_scope_kind, client_scope_value, edge_group_id, edge_id, metric, value,
		SUM(weight)::bigint AS weight
	FROM grouped_sets
	WHERE edge_grouped = 1 OR edge_id <> ''
	GROUP BY target_id, hostname, traffic_class, method, path_prefix_bucket,
		client_scope_kind, client_scope_value, edge_group_id, edge_id, metric, value
),
weighted AS (
	SELECT target_id, hostname, traffic_class, method, path_prefix_bucket,
		client_scope_kind, client_scope_value, edge_group_id, edge_id, metric, value,
		SUM(weight) OVER (
			PARTITION BY target_id, hostname, traffic_class, method, path_prefix_bucket,
				client_scope_kind, client_scope_value, edge_group_id, edge_id, metric
			ORDER BY value ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
		) AS cumulative_weight,
		SUM(weight) OVER (
			PARTITION BY target_id, hostname, traffic_class, method, path_prefix_bucket,
				client_scope_kind, client_scope_value, edge_group_id, edge_id, metric
		) AS total_weight
	FROM grouped
)
SELECT target_id, hostname, traffic_class, method, path_prefix_bucket,
	client_scope_kind, client_scope_value, edge_group_id, edge_id, metric,
	COALESCE(MIN(value) FILTER (WHERE cumulative_weight >= GREATEST(1, CEIL(0.10::double precision * total_weight::double precision))), 0)::double precision,
	COALESCE(MIN(value) FILTER (WHERE cumulative_weight >= GREATEST(1, CEIL(0.50::double precision * total_weight::double precision))), 0)::double precision,
	COALESCE(MIN(value) FILTER (WHERE cumulative_weight >= GREATEST(1, CEIL(0.95::double precision * total_weight::double precision))), 0)::double precision,
	COALESCE(MIN(value) FILTER (WHERE cumulative_weight >= GREATEST(1, CEIL(0.99::double precision * total_weight::double precision))), 0)::double precision
FROM weighted
GROUP BY target_id, hostname, traffic_class, method, path_prefix_bucket,
	client_scope_kind, client_scope_value, edge_group_id, edge_id, metric
ORDER BY target_id, hostname, traffic_class, method, path_prefix_bucket,
	client_scope_kind, client_scope_value, edge_group_id, edge_id, metric`, windowValues.String(),
		EdgeQualityPercentileTTFB,
		EdgeQualityPercentileUpload,
		EdgeQualityPercentileMinWindow,
		EdgeQualityPercentileMaxReadGap,
		EdgeQualityPercentileResponseEgress,
		EdgeQualityPercentileResponseWrite,
	)
	return query, args
}

func groupedEdgeQualityPercentiles(weights map[EdgeQualityPercentileValueKey]int64) map[string]EdgeQualityPercentileSet {
	type groupKey struct {
		BucketKey string
		Metric    EdgeQualityPercentileMetric
	}
	grouped := map[groupKey]map[int64]int64{}
	for key, weight := range weights {
		if key.Value <= 0 || weight <= 0 {
			continue
		}
		group := groupKey{BucketKey: key.BucketKey, Metric: key.Metric}
		values := grouped[group]
		if values == nil {
			values = map[int64]int64{}
			grouped[group] = values
		}
		values[key.Value] += weight
	}
	results := map[string]EdgeQualityPercentileSet{}
	for group, values := range grouped {
		byMetric := results[group.BucketKey]
		byMetric[group.Metric] = EdgeQualityPercentiles{
			P10: groupedWeightedQuantile(values, 0.10),
			P50: groupedWeightedQuantile(values, 0.50),
			P95: groupedWeightedQuantile(values, 0.95),
			P99: groupedWeightedQuantile(values, 0.99),
		}
		results[group.BucketKey] = byMetric
	}
	return results
}

func groupedWeightedQuantile(weights map[int64]int64, q float64) float64 {
	values := make([]int64, 0, len(weights))
	var total int64
	for value, weight := range weights {
		if value > 0 && weight > 0 {
			values = append(values, value)
			total += weight
		}
	}
	if total <= 0 {
		return 0
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	target := int64(math.Ceil(q * float64(total)))
	if target <= 0 {
		target = 1
	}
	var seen int64
	for _, value := range values {
		seen += weights[value]
		if seen >= target {
			return float64(value)
		}
	}
	return float64(values[len(values)-1])
}
