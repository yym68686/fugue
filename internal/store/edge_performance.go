package store

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) RecordEdgePerformanceSamples(samples []model.EdgePerformanceSample, pruneBefore time.Time) error {
	if len(samples) == 0 && pruneBefore.IsZero() {
		return nil
	}
	if s.usingDatabase() {
		return s.pgRecordEdgePerformanceSamples(samples, pruneBefore)
	}
	return s.withLockedState(true, func(state *model.State) error {
		if !pruneBefore.IsZero() {
			filtered := state.EdgePerformanceSamples[:0]
			for _, sample := range state.EdgePerformanceSamples {
				if !sample.SampledAt.Before(pruneBefore) {
					filtered = append(filtered, sample)
				}
			}
			state.EdgePerformanceSamples = filtered
		}
		now := time.Now().UTC()
		byID := make(map[string]int, len(state.EdgePerformanceSamples))
		for index, sample := range state.EdgePerformanceSamples {
			if strings.TrimSpace(sample.ID) == "" {
				continue
			}
			byID[sample.ID] = index
		}
		for _, sample := range samples {
			normalized, err := normalizeEdgePerformanceSampleForStore(sample, now)
			if err != nil {
				continue
			}
			if normalized.ID == "" {
				normalized.ID = model.NewID("edge_perf")
			}
			if index, ok := byID[normalized.ID]; ok {
				state.EdgePerformanceSamples[index] = normalized
				continue
			}
			state.EdgePerformanceSamples = append(state.EdgePerformanceSamples, normalized)
			byID[normalized.ID] = len(state.EdgePerformanceSamples) - 1
		}
		sortEdgePerformanceSamples(state.EdgePerformanceSamples)
		return nil
	})
}

func (s *Store) ListEdgePerformanceSamples(hostname string, since time.Time) ([]model.EdgePerformanceSample, error) {
	hostname = normalizeEdgePerformanceHostname(hostname)
	if s.usingDatabase() {
		return s.pgListEdgePerformanceSamples(hostname, since)
	}
	var samples []model.EdgePerformanceSample
	err := s.withLockedState(false, func(state *model.State) error {
		for _, sample := range state.EdgePerformanceSamples {
			if hostname != "" && !strings.EqualFold(normalizeEdgePerformanceHostname(sample.Hostname), hostname) {
				continue
			}
			if !since.IsZero() && sample.SampledAt.Before(since) {
				continue
			}
			samples = append(samples, sample)
		}
		return nil
	})
	sortEdgePerformanceSamples(samples)
	return samples, err
}

func (s *Store) pgRecordEdgePerformanceSamples(samples []model.EdgePerformanceSample, pruneBefore time.Time) error {
	if err := s.ensureDatabaseReady(); err != nil {
		return err
	}
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin edge performance sample transaction: %w", err)
	}
	defer tx.Rollback()

	if !pruneBefore.IsZero() {
		if _, err := tx.ExecContext(ctx, `DELETE FROM fugue_edge_performance_samples WHERE sampled_at < $1`, pruneBefore); err != nil {
			return fmt.Errorf("prune edge performance samples: %w", err)
		}
	}
	for _, sample := range samples {
		normalized, err := normalizeEdgePerformanceSampleForStore(sample, time.Now().UTC())
		if err != nil {
			continue
		}
		if normalized.ID == "" {
			normalized.ID = model.NewID("edge_perf")
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_edge_performance_samples (
	id, edge_id, edge_group_id, hostname, path_prefix, client_country, client_region, client_asn, runtime_region,
	route_generation, cache_status, dns_policy, tls_handshake_ms, ttfb_ms, upstream_ms,
	total_ms, status_code, sample_count, cache_hit_count, cache_observation_count,
	error_count, sampled_at
) VALUES (
	$1, $2, $3, $4, $5, $6, $7, $8,
	$9, $10, $11, $12, $13, $14,
	$15, $16, $17, $18, $19,
	$20, $21, $22
)
ON CONFLICT (id) DO UPDATE SET
	edge_id = EXCLUDED.edge_id,
	edge_group_id = EXCLUDED.edge_group_id,
	hostname = EXCLUDED.hostname,
	path_prefix = EXCLUDED.path_prefix,
	client_country = EXCLUDED.client_country,
	client_region = EXCLUDED.client_region,
	client_asn = EXCLUDED.client_asn,
	runtime_region = EXCLUDED.runtime_region,
	route_generation = EXCLUDED.route_generation,
	cache_status = EXCLUDED.cache_status,
	dns_policy = EXCLUDED.dns_policy,
	tls_handshake_ms = EXCLUDED.tls_handshake_ms,
	ttfb_ms = EXCLUDED.ttfb_ms,
	upstream_ms = EXCLUDED.upstream_ms,
	total_ms = EXCLUDED.total_ms,
	status_code = EXCLUDED.status_code,
	sample_count = EXCLUDED.sample_count,
	cache_hit_count = EXCLUDED.cache_hit_count,
	cache_observation_count = EXCLUDED.cache_observation_count,
	error_count = EXCLUDED.error_count,
	sampled_at = EXCLUDED.sampled_at
`,
			normalized.ID,
			normalized.EdgeID,
			normalized.EdgeGroupID,
			normalized.Hostname,
			normalized.PathPrefix,
			normalized.ClientCountry,
			normalized.ClientRegion,
			normalized.ClientASN,
			normalized.RuntimeRegion,
			normalized.RouteGeneration,
			normalized.CacheStatus,
			normalized.DNSPolicy,
			normalized.TLSHandshakeMS,
			normalized.TTFBMS,
			normalized.UpstreamMS,
			normalized.TotalMS,
			normalized.StatusCode,
			normalized.SampleCount,
			normalized.CacheHitCount,
			normalized.CacheObservationCount,
			normalized.ErrorCount,
			normalized.SampledAt,
		); err != nil {
			return fmt.Errorf("insert edge performance sample: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit edge performance sample transaction: %w", err)
	}
	return nil
}

func (s *Store) pgListEdgePerformanceSamples(hostname string, since time.Time) ([]model.EdgePerformanceSample, error) {
	if err := s.ensureDatabaseReady(); err != nil {
		return nil, err
	}
	query := `
SELECT id, edge_id, edge_group_id, hostname, client_country, client_region, client_asn, runtime_region,
	path_prefix,
	route_generation, cache_status, dns_policy, tls_handshake_ms, ttfb_ms, upstream_ms,
	total_ms, status_code, sample_count, cache_hit_count, cache_observation_count,
	error_count, sampled_at
FROM fugue_edge_performance_samples
WHERE 1=1
`
	args := []any{}
	if hostname != "" {
		args = append(args, hostname)
		query += fmt.Sprintf(" AND hostname = $%d", len(args))
	}
	if !since.IsZero() {
		args = append(args, since)
		query += fmt.Sprintf(" AND sampled_at >= $%d", len(args))
	}
	query += " ORDER BY sampled_at ASC, hostname ASC, edge_group_id ASC, id ASC"

	rows, err := s.db.QueryContext(context.Background(), query, args...)
	if err != nil {
		return nil, fmt.Errorf("list edge performance samples: %w", err)
	}
	defer rows.Close()

	var samples []model.EdgePerformanceSample
	for rows.Next() {
		var sample model.EdgePerformanceSample
		var tlsHandshake, ttfb, upstream, total sql.NullInt64
		var sampleCount, cacheHitCount, cacheObservationCount, errorCount sql.NullInt64
		if err := rows.Scan(
			&sample.ID,
			&sample.EdgeID,
			&sample.EdgeGroupID,
			&sample.Hostname,
			&sample.ClientCountry,
			&sample.ClientRegion,
			&sample.ClientASN,
			&sample.RuntimeRegion,
			&sample.PathPrefix,
			&sample.RouteGeneration,
			&sample.CacheStatus,
			&sample.DNSPolicy,
			&tlsHandshake,
			&ttfb,
			&upstream,
			&total,
			&sample.StatusCode,
			&sampleCount,
			&cacheHitCount,
			&cacheObservationCount,
			&errorCount,
			&sample.SampledAt,
		); err != nil {
			return nil, fmt.Errorf("scan edge performance sample: %w", err)
		}
		sample.TLSHandshakeMS = edgePerformanceInt64FromNull(tlsHandshake)
		sample.TTFBMS = edgePerformanceInt64FromNull(ttfb)
		sample.UpstreamMS = edgePerformanceInt64FromNull(upstream)
		sample.TotalMS = edgePerformanceInt64FromNull(total)
		if sampleCount.Valid {
			sample.SampleCount = int(sampleCount.Int64)
		}
		if cacheHitCount.Valid {
			sample.CacheHitCount = int(cacheHitCount.Int64)
		}
		if cacheObservationCount.Valid {
			sample.CacheObservationCount = int(cacheObservationCount.Int64)
		}
		if errorCount.Valid {
			sample.ErrorCount = int(errorCount.Int64)
		}
		samples = append(samples, sample)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate edge performance samples: %w", err)
	}
	return samples, nil
}

func normalizeEdgePerformanceSampleForStore(sample model.EdgePerformanceSample, now time.Time) (model.EdgePerformanceSample, error) {
	sample.ID = strings.TrimSpace(sample.ID)
	sample.EdgeID = normalizeEdgeID(sample.EdgeID)
	sample.EdgeGroupID = normalizeEdgeGroupID(sample.EdgeGroupID)
	sample.Hostname = normalizeEdgePerformanceHostname(sample.Hostname)
	sample.PathPrefix = model.NormalizeAppRoutePathPrefix(sample.PathPrefix)
	sample.ClientCountry = normalizeEdgeMetadataValue(sample.ClientCountry)
	sample.ClientRegion = normalizeEdgeMetadataValue(sample.ClientRegion)
	sample.ClientASN = normalizeEdgeMetadataValue(sample.ClientASN)
	sample.RuntimeRegion = normalizeEdgeMetadataValue(sample.RuntimeRegion)
	sample.RouteGeneration = strings.TrimSpace(sample.RouteGeneration)
	sample.CacheStatus = strings.TrimSpace(strings.ToLower(sample.CacheStatus))
	sample.DNSPolicy = strings.TrimSpace(strings.ToLower(sample.DNSPolicy))
	if sample.SampledAt.IsZero() {
		sample.SampledAt = now
	}
	if sample.EdgeGroupID == "" || sample.Hostname == "" {
		return model.EdgePerformanceSample{}, ErrInvalidInput
	}
	if sample.SampleCount <= 0 {
		sample.SampleCount = 1
	}
	if sample.CacheHitCount < 0 {
		sample.CacheHitCount = 0
	}
	if sample.CacheObservationCount < sample.CacheHitCount {
		sample.CacheObservationCount = sample.CacheHitCount
	}
	if sample.ErrorCount < 0 {
		sample.ErrorCount = 0
	}
	if sample.TLSHandshakeMS < 0 {
		sample.TLSHandshakeMS = 0
	}
	if sample.TTFBMS < 0 {
		sample.TTFBMS = 0
	}
	if sample.UpstreamMS < 0 {
		sample.UpstreamMS = 0
	}
	if sample.TotalMS < 0 {
		sample.TotalMS = 0
	}
	if sample.StatusCode < 0 {
		sample.StatusCode = 0
	}
	return sample, nil
}

func sortEdgePerformanceSamples(samples []model.EdgePerformanceSample) {
	sort.Slice(samples, func(i, j int) bool {
		if !samples[i].SampledAt.Equal(samples[j].SampledAt) {
			return samples[i].SampledAt.Before(samples[j].SampledAt)
		}
		if samples[i].Hostname != samples[j].Hostname {
			return samples[i].Hostname < samples[j].Hostname
		}
		if samples[i].EdgeGroupID != samples[j].EdgeGroupID {
			return samples[i].EdgeGroupID < samples[j].EdgeGroupID
		}
		return samples[i].ID < samples[j].ID
	})
}

func normalizeEdgePerformanceHostname(hostname string) string {
	return strings.Trim(strings.TrimSpace(strings.ToLower(hostname)), ".")
}

func edgePerformanceInt64FromNull(value sql.NullInt64) int64 {
	if !value.Valid {
		return 0
	}
	return value.Int64
}
