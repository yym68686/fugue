package api

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Server) appResourceTimeseries(app model.App, window appObservabilityWindow) ([]map[string]any, error) {
	since, until, err := parseAppObservabilityWindowTimes(window)
	if err != nil {
		return nil, err
	}
	samples, err := s.store.ListResourceUsageSamples(app.TenantID, rightSizingSampleTargetKind(model.ClusterNodeWorkloadKindApp), app.ID, since)
	if err != nil {
		return nil, err
	}
	if len(samples) > 1440 {
		samples = samples[len(samples)-1440:]
	}
	points := make([][]map[string]any, 3)
	for i := range points {
		points[i] = []map[string]any{}
	}
	for _, sample := range samples {
		if sample.ObservedAt.After(until) || sample.ObservedAt.Before(since) {
			continue
		}
		for i, value := range []*int64{sample.CPUMilliCores, sample.MemoryBytes, sample.EphemeralStorageBytes} {
			var number any
			if value != nil {
				number = *value
			}
			points[i] = append(points[i], map[string]any{"observed_at": sample.ObservedAt.UTC().Format(time.RFC3339), "value": number})
		}
	}
	names := []string{"cpu_history", "memory_history", "ephemeral_storage_history"}
	units := []string{"mCPU", "bytes", "bytes"}
	out := make([]map[string]any, 3)
	for i, name := range names {
		out[i] = map[string]any{"name": name, "unit": units[i], "source": "resource sampler: busiest replica", "interval_seconds": int(resourceUsageSampleInterval.Seconds()), "points": points[i]}
	}
	return out, nil
}

func (s *Server) queryAppObservabilityTimeseriesFromClickHouse(ctx context.Context, appID string, window appObservabilityWindow, step int) ([]map[string]any, error) {
	if strings.TrimSpace(s.observabilityConfig.ClickHouseDSN) == "" {
		return nil, fmt.Errorf("historical metrics backend is not configured")
	}
	since, until, err := parseAppObservabilityWindowTimes(window)
	if err != nil {
		return nil, err
	}
	step = max(step, 10)
	columns := []string{"toUnixTimestamp(toStartOfInterval(ts, INTERVAL " + strconv.Itoa(step) + " SECOND)) AS sampled_at", "countIf(edge_id = '') AS app_count", "count() AS total_count", "countIf(edge_id = '' AND status_code >= 400) AS app_errors", "countIf(status_code >= 400) AS total_errors"}
	for _, q := range []string{"50", "95", "99"} {
		columns = append(columns, "quantileTDigestIf(0."+q+")(toFloat64(duration_ms), edge_id = '') AS app_p"+q, "quantileTDigest(0."+q+")(toFloat64(duration_ms)) AS total_p"+q)
	}
	columns = append(columns, "quantileTDigestIf(0.95)(toFloat64(ttfb_ms), edge_id = '') AS app_ttfb", "quantileTDigest(0.95)(toFloat64(ttfb_ms)) AS total_ttfb")
	query := "SELECT " + strings.Join(columns, ", ") + " FROM request_facts WHERE app_id = " + quoteClickHouseString(appID) + " AND ts >= " + clickHouseDateTime64Literal(since) + " AND ts <= " + clickHouseDateTime64Literal(until) + " GROUP BY sampled_at ORDER BY sampled_at ASC LIMIT 1440 FORMAT JSONEachRow"
	rows, err := s.queryAppObservabilityClickHouse(ctx, query)
	if err != nil {
		return nil, err
	}
	names := []string{"rpm", "error_rate", "p50_duration_ms", "p95_duration_ms", "p99_duration_ms", "p95_ttfb_ms"}
	units := []string{"rpm", "ratio", "ms", "ms", "ms", "ms"}
	points := make([][]map[string]any, len(names))
	for i := range points {
		points[i] = []map[string]any{}
	}
	for _, row := range rows {
		ts := finiteFloatField(row, "sampled_at")
		at := time.Unix(int64(ts), 0).UTC()
		if at.Before(since) || at.After(until) {
			continue
		}
		prefix := "total_"
		if finiteFloatField(row, "app_count") > 0 {
			prefix = "app_"
		}
		count := finiteFloatField(row, prefix+"count")
		if count <= 0 {
			continue
		}
		values := []float64{count * 60 / float64(step), finiteFloatField(row, prefix+"errors") / count, finiteFloatField(row, prefix+"p50"), finiteFloatField(row, prefix+"p95"), finiteFloatField(row, prefix+"p99"), finiteFloatField(row, prefix+"ttfb")}
		for i, value := range values {
			var sample any = value
			if math.IsNaN(value) || math.IsInf(value, 0) {
				sample = nil
			}
			points[i] = append(points[i], map[string]any{"observed_at": at.Format(time.RFC3339), "value": sample})
		}
	}
	out := make([]map[string]any, len(names))
	for i, name := range names {
		out[i] = map[string]any{"name": name, "unit": units[i], "source": "clickhouse request_facts", "interval_seconds": step, "points": points[i]}
	}
	return out, nil
}

// Live samples retain the kubelet observation time. Repeated cached reads do
// not manufacture new points, and lack of timestamp makes the source absent.
func (s *Server) appLiveResourceTimeseries(ctx context.Context, app model.App) []map[string]any {
	snapshots, err := s.loadClusterNodeInventory(ctx)
	if err != nil {
		return nil
	}
	var at *time.Time
	resolver := newClusterWorkloadResolver([]model.App{app}, nil)
	for _, snapshot := range snapshots {
		for _, pod := range snapshot.pods {
			workload, ok := resolver.resolvePod(pod)
			if ok && workload.ID == app.ID && workload.Kind == model.ClusterNodeWorkloadKindApp {
				if snapshot.node.ObservedAt == nil {
					return nil
				}
				if at == nil || snapshot.node.ObservedAt.Before(*at) {
					value := *snapshot.node.ObservedAt
					at = &value
				}
			}
		}
	}
	if at == nil {
		return nil
	}
	overlay := buildCurrentResourceUsageOverlayWithPolicies(snapshots, []model.App{app}, nil, persistentVolumeUsagePolicies{strict: true, byClaim: map[string]persistentVolumeUsagePolicy{}})
	usage, ok := overlay.rightSizingApps[app.ID]
	if !ok {
		return nil
	}
	out := []map[string]any{}
	for i, value := range []*int64{usage.CPUMilliCores, usage.MemoryBytes, usage.EphemeralStorageBytes} {
		var number any
		if value != nil {
			number = *value
		}
		out = append(out, map[string]any{"name": []string{"cpu", "memory", "ephemeral_storage"}[i], "unit": []string{"mCPU", "bytes", "bytes"}[i], "source": "kubelet busiest replica", "interval_seconds": int(defaultClusterNodeInventoryCacheTTL.Seconds()), "points": []map[string]any{{"observed_at": at.UTC().Format(time.RFC3339), "value": number}}})
	}
	return out
}
