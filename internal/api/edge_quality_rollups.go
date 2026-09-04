package api

import (
	"context"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/observability"
	"fugue/internal/store"
)

const edgeQualityRollupBuilderInterval = 5 * time.Minute
const edgeQualityPlatformRollupHostname = "__platform__"
const edgeQualityRollupSampleChunkSize = 128

var edgeQualityRollupWindows = []struct {
	Name      string
	Duration  time.Duration
	Retention time.Duration
}{
	{Name: "5m", Duration: 5 * time.Minute, Retention: 48 * time.Hour},
	{Name: "30m", Duration: 30 * time.Minute, Retention: 14 * 24 * time.Hour},
	{Name: "6h", Duration: 6 * time.Hour, Retention: 45 * 24 * time.Hour},
	{Name: "24h", Duration: 24 * time.Hour, Retention: 180 * 24 * time.Hour},
}

func (s *Server) StartBackgroundEdgeQualityRollups(ctx context.Context) {
	if s == nil || s.store == nil {
		return
	}
	s.runEdgeQualityRollupBuilder(ctx, time.Now().UTC())
	timer := time.NewTicker(edgeQualityRollupBuilderInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-timer.C:
			s.runEdgeQualityRollupBuilder(ctx, now.UTC())
		}
	}
}

func (s *Server) runEdgeQualityRollupBuilder(ctx context.Context, now time.Time) {
	started := time.Now().UTC()
	count := 0
	acquired := true
	var err error
	if s.store != nil {
		acquired, err = s.store.WithAdvisoryLock(ctx, "edge-quality-rollup-builder", func() error {
			var buildErr error
			count, buildErr = s.rebuildEdgeQualityRollups(ctx, now)
			return buildErr
		})
	} else {
		count, err = s.rebuildEdgeQualityRollups(ctx, now)
	}
	duration := time.Since(started)
	if !acquired {
		if s.log != nil {
			s.log.Printf("edge quality rollup builder skipped: another writer holds lock")
		}
		return
	}
	s.edgeQualityRollupMu.Lock()
	s.edgeQualityRollupLastRun = started
	s.edgeQualityRollupLastDuration = duration
	s.edgeQualityRollupLastCount = count
	s.edgeQualityRollupRunCount++
	if err != nil {
		s.edgeQualityRollupLastError = err.Error()
		s.edgeQualityRollupErrorCount++
	} else {
		s.edgeQualityRollupLastError = ""
		s.edgeQualityRollupLastSuccess = time.Now().UTC()
	}
	s.edgeQualityRollupMu.Unlock()
	if err != nil {
		if s.log != nil {
			s.log.Printf("edge quality rollup builder failed: duration=%s err=%v", duration, err)
		}
		return
	}
	if s.log != nil {
		s.log.Printf("edge quality rollup builder complete: rollups=%d duration=%s", count, duration)
	}
}

func (s *Server) rebuildEdgeQualityRollups(ctx context.Context, now time.Time) (int, error) {
	if s == nil || s.store == nil {
		return 0, nil
	}
	now = now.UTC()
	session, err := s.store.BeginEdgeQualityRollupBuild(ctx)
	if err != nil {
		return 0, err
	}
	defer session.Rollback()
	watermarks, err := session.Watermarks()
	if err != nil {
		return 0, err
	}
	plans, retention, pendingWatermarks, earliest, latest := planEdgeQualityRollupWindows(now, watermarks)
	if len(plans) == 0 {
		if len(pendingWatermarks) == 0 {
			return 0, nil
		}
		if err := session.Commit(nil, retention, pendingWatermarks); err != nil {
			return 0, err
		}
		return 0, nil
	}
	builder := newEdgeQualityRollupBuildState(plans)
	session.SetPercentileWindows(builder.percentileWindows())
	var percentileWeights map[store.EdgeQualityPercentileValueKey]int
	if session.NeedsPercentileWeights() {
		percentileWeights = map[store.EdgeQualityPercentileValueKey]int{}
	}
	if err := session.ForEachSampleChunk(earliest, latest, edgeQualityRollupSampleChunkSize, func(samples []model.EdgePerformanceSample) error {
		if percentileWeights != nil {
			clear(percentileWeights)
		}
		builder.addSamples(samples, percentileWeights)
		return session.AddPercentileWeights(percentileWeights)
	}); err != nil {
		return 0, err
	}
	percentiles, err := session.Percentiles()
	if err != nil {
		return 0, err
	}
	rollups := builder.rollups(percentiles, now)
	if err := session.Commit(rollups, retention, pendingWatermarks); err != nil {
		return 0, err
	}
	return len(rollups), nil
}

func (s *Server) writeEdgeQualityRollupMetrics(w io.Writer) {
	s.edgeQualityRollupMu.Lock()
	lastRun := s.edgeQualityRollupLastRun
	lastSuccess := s.edgeQualityRollupLastSuccess
	lastDuration := s.edgeQualityRollupLastDuration
	lastCount := s.edgeQualityRollupLastCount
	runCount := s.edgeQualityRollupRunCount
	errorCount := s.edgeQualityRollupErrorCount
	lastError := s.edgeQualityRollupLastError
	s.edgeQualityRollupMu.Unlock()

	labels := map[string]string{"mode": strings.TrimSpace(s.edgeQualityRankingMode)}
	observability.WriteGaugeMetric(w, "fugue_edge_quality_ranking_active", "Whether scoped edge quality ranking actively changes DNS answers.", labels, boolMetric(s.edgeQualityRankingActive()))
	observability.WriteGaugeMetric(w, "fugue_edge_quality_ranking_shadow", "Whether scoped edge quality ranking only records shadow decisions.", labels, boolMetric(s.edgeQualityRankingShadow()))
	observability.WriteCounterMetric(w, "fugue_edge_quality_rollup_runs_total", "Total edge quality rollup builder runs.", nil, float64(runCount))
	observability.WriteCounterMetric(w, "fugue_edge_quality_rollup_errors_total", "Total edge quality rollup builder errors.", nil, float64(errorCount))
	observability.WriteGaugeMetric(w, "fugue_edge_quality_rollup_last_duration_seconds", "Duration of the last edge quality rollup builder run.", nil, lastDuration.Seconds())
	observability.WriteGaugeMetric(w, "fugue_edge_quality_rollup_last_count", "Number of rollups written by the last builder run.", nil, float64(lastCount))
	if !lastRun.IsZero() {
		observability.WriteGaugeMetric(w, "fugue_edge_quality_rollup_last_run_timestamp_seconds", "Unix timestamp of the last edge quality rollup builder run.", nil, float64(lastRun.Unix()))
	}
	if !lastSuccess.IsZero() {
		observability.WriteGaugeMetric(w, "fugue_edge_quality_rollup_last_success_timestamp_seconds", "Unix timestamp of the last successful edge quality rollup builder run.", nil, float64(lastSuccess.Unix()))
	}
	observability.WriteGaugeMetric(w, "fugue_edge_quality_rollup_last_error", "Whether the last edge quality rollup builder run failed.", map[string]string{"error": truncateMetricLabel(lastError, 160)}, boolMetric(lastError != ""))
}

type edgeQualityRollupKey struct {
	Hostname         string
	TrafficClass     string
	Method           string
	PathPrefixBucket string
	ClientScopeKind  string
	ClientScopeValue string
	EdgeGroupID      string
	EdgeID           string
}

type edgeQualityRollupAccumulator struct {
	Target                       *edgeQualityRollupWindowTarget
	Key                          edgeQualityRollupKey
	PercentileBucketKey          string
	SampleRecords                int
	RequestCount                 int
	ErrorCount                   int
	CacheHitCount                int
	CacheObservationCount        int
	BodyIncompleteCount          int
	BodyReadErrorCount           int
	ClientCancelCount            int
	UpstreamWeightedMS           float64
	TotalWeightedMS              float64
	OriginDNSWeightedMS          float64
	OriginDNSSampleCount         int
	OriginConnectWeightedMS      float64
	OriginConnectSampleCount     int
	OriginWriteWeightedMS        float64
	OriginWriteSampleCount       int
	OriginWaitWeightedMS         float64
	OriginWaitSampleCount        int
	OriginTTFBWeightedMS         float64
	OriginTTFBSampleCount        int
	OriginTotalWeightedMS        float64
	OriginTotalSampleCount       int
	UploadWeightedBPS            float64
	UploadSampleCount            int
	MinWindowWeightedBPS         float64
	MinWindowSampleCount         int
	MaxReadGapSampleCount        int
	BodyReadBlockWeightedMS      float64
	BodyReadBlockSampleCount     int
	ResponseEgressWeightedBPS    float64
	ResponseEgressSampleCount    int
	ResponseWriteSampleCount     int
	ClientTCPRTTWeighted         float64
	ClientTCPMinRTTWeighted      float64
	ClientTCPRTTVarWeighted      float64
	ClientTCPMetricSampleCount   int
	ClientTCPRetransRateWeighted float64
	ClientTCPBytesRateWeighted   float64
	ClientTCPRTORateWeighted     float64
	ClientTCPRateSampleCount     int
	ClientTCPDeliveryWeighted    float64
	ClientTCPDeliverySampleCount int
	ActiveRequestsWeighted       float64
	ActiveBodyBuffersWeighted    float64
	GoroutineCountWeighted       float64
	MemoryAllocWeighted          float64
	SaturationSampleCount        int
}

type edgeQualityRollupWindowTarget struct {
	ID        int64
	Window    string
	Duration  time.Duration
	StartedAt time.Time
	EndedAt   time.Time
}

type edgeQualityRollupWindowPlan struct {
	Duration time.Duration
	Targets  map[int64]*edgeQualityRollupWindowTarget
}

type edgeQualityRollupAccumulatorKey struct {
	TargetID int64
	Rollup   edgeQualityRollupKey
}

type edgeQualityRollupBuildState struct {
	plans        []edgeQualityRollupWindowPlan
	accumulators map[edgeQualityRollupAccumulatorKey]*edgeQualityRollupAccumulator
}

func planEdgeQualityRollupWindows(now time.Time, watermarks map[string]time.Time) ([]edgeQualityRollupWindowPlan, map[string]time.Time, map[string]time.Time, time.Time, time.Time) {
	now = now.UTC()
	plans := make([]edgeQualityRollupWindowPlan, 0, len(edgeQualityRollupWindows))
	retention := make(map[string]time.Time, len(edgeQualityRollupWindows))
	pendingWatermarks := make(map[string]time.Time, len(edgeQualityRollupWindows))
	var earliest time.Time
	var latest time.Time
	var nextTargetID int64
	for _, window := range edgeQualityRollupWindows {
		retentionBefore := now.Add(-window.Retention)
		retention[window.Name] = retentionBefore
		currentEnd := now.Truncate(window.Duration)
		if currentEnd.IsZero() {
			currentEnd = now
		}
		watermark := watermarks[window.Name].UTC()
		if !watermark.IsZero() {
			watermark = watermark.Truncate(window.Duration)
		}
		if !watermark.Before(currentEnd) {
			continue
		}
		pendingWatermarks[window.Name] = currentEnd
		nextEnd := currentEnd
		if !watermark.IsZero() {
			nextEnd = watermark.Add(window.Duration)
			oldestRetainedEnd := retentionBefore.Truncate(window.Duration)
			if oldestRetainedEnd.Before(retentionBefore) {
				oldestRetainedEnd = oldestRetainedEnd.Add(window.Duration)
			}
			if nextEnd.Before(oldestRetainedEnd) {
				nextEnd = oldestRetainedEnd
			}
		}
		plan := edgeQualityRollupWindowPlan{
			Duration: window.Duration,
			Targets:  map[int64]*edgeQualityRollupWindowTarget{},
		}
		for endedAt := nextEnd; !endedAt.After(currentEnd); endedAt = endedAt.Add(window.Duration) {
			target := &edgeQualityRollupWindowTarget{
				ID:        nextTargetID,
				Window:    window.Name,
				Duration:  window.Duration,
				StartedAt: endedAt.Add(-window.Duration),
				EndedAt:   endedAt,
			}
			nextTargetID++
			plan.Targets[endedAt.UnixNano()] = target
			if earliest.IsZero() || target.StartedAt.Before(earliest) {
				earliest = target.StartedAt
			}
			if latest.IsZero() || target.EndedAt.After(latest) {
				latest = target.EndedAt
			}
		}
		if len(plan.Targets) > 0 {
			plans = append(plans, plan)
		}
	}
	return plans, retention, pendingWatermarks, earliest, latest
}

func newEdgeQualityRollupBuildState(plans []edgeQualityRollupWindowPlan) *edgeQualityRollupBuildState {
	return &edgeQualityRollupBuildState{
		plans:        plans,
		accumulators: map[edgeQualityRollupAccumulatorKey]*edgeQualityRollupAccumulator{},
	}
}

func (b *edgeQualityRollupBuildState) percentileWindows() []store.EdgeQualityPercentileWindow {
	windows := make([]store.EdgeQualityPercentileWindow, 0)
	for _, plan := range b.plans {
		for _, target := range plan.Targets {
			windows = append(windows, store.EdgeQualityPercentileWindow{
				ID:        target.ID,
				Window:    target.Window,
				StartedAt: target.StartedAt,
				EndedAt:   target.EndedAt,
			})
		}
	}
	sort.Slice(windows, func(i, j int) bool { return windows[i].ID < windows[j].ID })
	return windows
}

func (b *edgeQualityRollupBuildState) addSamples(samples []model.EdgePerformanceSample, percentileWeights map[store.EdgeQualityPercentileValueKey]int) {
	for _, sample := range samples {
		hostname := normalizeExternalAppDomain(sample.Hostname)
		edgeGroupID := strings.TrimSpace(sample.EdgeGroupID)
		if hostname == "" || edgeGroupID == "" {
			continue
		}
		trafficClass := normalizeEdgeTrafficClass(sample.TrafficClass)
		method := strings.ToUpper(strings.TrimSpace(sample.Method))
		pathPrefixBucket := edgeQualityPathPrefixBucket(sample.PathPrefix)
		scopes := edgeQualityRollupScopesForSample(sample)
		hostnames := []string{hostname, edgeQualityPlatformRollupHostname}
		edgeIDs := []string{""}
		if edgeID := strings.TrimSpace(sample.EdgeID); edgeID != "" {
			edgeIDs = append(edgeIDs, edgeID)
		}
		for _, plan := range b.plans {
			endedAt := sample.SampledAt.UTC().Truncate(plan.Duration).Add(plan.Duration)
			target := plan.Targets[endedAt.UnixNano()]
			if target == nil || sample.SampledAt.Before(target.StartedAt) || !sample.SampledAt.Before(target.EndedAt) {
				continue
			}
			for _, scope := range scopes {
				for _, rollupHostname := range hostnames {
					for _, edgeID := range edgeIDs {
						key := edgeQualityRollupAccumulatorKey{
							TargetID: target.ID,
							Rollup: edgeQualityRollupKey{
								Hostname:         rollupHostname,
								TrafficClass:     trafficClass,
								Method:           method,
								PathPrefixBucket: pathPrefixBucket,
								ClientScopeKind:  scope.Kind,
								ClientScopeValue: scope.Value,
								EdgeGroupID:      edgeGroupID,
								EdgeID:           edgeID,
							},
						}
						accumulator := b.accumulators[key]
						if accumulator == nil {
							accumulator = &edgeQualityRollupAccumulator{
								Target:              target,
								Key:                 key.Rollup,
								PercentileBucketKey: edgeQualityPercentileBucketKey(target.ID, key.Rollup),
							}
							b.accumulators[key] = accumulator
						}
						accumulateEdgeQualityRollup(accumulator, sample, percentileWeights)
					}
				}
			}
		}
	}
}

func edgeQualityPercentileBucketKey(targetID int64, key edgeQualityRollupKey) string {
	return fmt.Sprintf("%d\x00%s", targetID, strings.Join([]string{
		key.Hostname,
		key.TrafficClass,
		key.Method,
		key.PathPrefixBucket,
		key.ClientScopeKind,
		key.ClientScopeValue,
		key.EdgeGroupID,
		key.EdgeID,
	}, "\x00"))
}

func (b *edgeQualityRollupBuildState) rollups(percentiles map[string]store.EdgeQualityPercentileSet, now time.Time) []model.EdgeQualityRollup {
	out := make([]model.EdgeQualityRollup, 0, len(b.accumulators))
	for _, accumulator := range b.accumulators {
		out = append(out, accumulator.rollup(percentiles[accumulator.PercentileBucketKey], now))
	}
	sortEdgeQualityRollupOutput(out)
	return out
}

func buildEdgeQualityRollupsForWindow(samples []model.EdgePerformanceSample, window string, startedAt, endedAt, now time.Time) []model.EdgeQualityRollup {
	target := &edgeQualityRollupWindowTarget{ID: 0, Window: window, Duration: endedAt.Sub(startedAt), StartedAt: startedAt, EndedAt: endedAt}
	builder := newEdgeQualityRollupBuildState([]edgeQualityRollupWindowPlan{{
		Duration: target.Duration,
		Targets:  map[int64]*edgeQualityRollupWindowTarget{endedAt.UnixNano(): target},
	}})
	weights := map[store.EdgeQualityPercentileValueKey]int{}
	builder.addSamples(samples, weights)
	return builder.rollups(edgeQualityPercentilesFromWeights(weights), now)
}

func edgeQualityPercentilesFromWeights(weights map[store.EdgeQualityPercentileValueKey]int) map[string]store.EdgeQualityPercentileSet {
	grouped := map[string]map[store.EdgeQualityPercentileMetric]map[int64]int{}
	for key, weight := range weights {
		byMetric := grouped[key.BucketKey]
		if byMetric == nil {
			byMetric = map[store.EdgeQualityPercentileMetric]map[int64]int{}
			grouped[key.BucketKey] = byMetric
		}
		values := byMetric[key.Metric]
		if values == nil {
			values = map[int64]int{}
			byMetric[key.Metric] = values
		}
		values[key.Value] += weight
	}
	results := map[string]store.EdgeQualityPercentileSet{}
	for bucketKey, byMetric := range grouped {
		percentileSet := store.EdgeQualityPercentileSet{}
		for metric, values := range byMetric {
			percentileSet[metric] = store.EdgeQualityPercentiles{
				P10: groupedEdgeQualityWeightedQuantile(values, 0.10),
				P50: groupedEdgeQualityWeightedQuantile(values, 0.50),
				P95: groupedEdgeQualityWeightedQuantile(values, 0.95),
				P99: groupedEdgeQualityWeightedQuantile(values, 0.99),
			}
		}
		results[bucketKey] = percentileSet
	}
	return results
}

func groupedEdgeQualityWeightedQuantile(weights map[int64]int, q float64) float64 {
	values := make([]int64, 0, len(weights))
	total := 0
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
	target := int(math.Ceil(q * float64(total)))
	if target <= 0 {
		target = 1
	}
	seen := 0
	for _, value := range values {
		seen += weights[value]
		if seen >= target {
			return float64(value)
		}
	}
	return float64(values[len(values)-1])
}

func sortEdgeQualityRollupOutput(out []model.EdgeQualityRollup) {
	sort.Slice(out, func(i, j int) bool {
		if !out[i].WindowStartedAt.Equal(out[j].WindowStartedAt) {
			return out[i].WindowStartedAt.Before(out[j].WindowStartedAt)
		}
		if out[i].Window != out[j].Window {
			return out[i].Window < out[j].Window
		}
		if out[i].Hostname != out[j].Hostname {
			return out[i].Hostname < out[j].Hostname
		}
		if out[i].TrafficClass != out[j].TrafficClass {
			return out[i].TrafficClass < out[j].TrafficClass
		}
		if out[i].Method != out[j].Method {
			return out[i].Method < out[j].Method
		}
		if out[i].PathPrefixBucket != out[j].PathPrefixBucket {
			return out[i].PathPrefixBucket < out[j].PathPrefixBucket
		}
		if out[i].ClientScopeKind != out[j].ClientScopeKind {
			return out[i].ClientScopeKind < out[j].ClientScopeKind
		}
		if out[i].ClientScopeValue != out[j].ClientScopeValue {
			return out[i].ClientScopeValue < out[j].ClientScopeValue
		}
		if out[i].EdgeGroupID != out[j].EdgeGroupID {
			return out[i].EdgeGroupID < out[j].EdgeGroupID
		}
		return out[i].EdgeID < out[j].EdgeID
	})
}

func accumulateEdgeQualityRollup(accumulator *edgeQualityRollupAccumulator, sample model.EdgePerformanceSample, percentileWeights map[store.EdgeQualityPercentileValueKey]int) {
	requestCount, uploadBPS := accumulateEdgeQualityRollupScalars(accumulator, sample)
	addEdgeQualityPercentileWeight(percentileWeights, accumulator.PercentileBucketKey, store.EdgeQualityPercentileTTFB, sample.TTFBMS, requestCount)
	addEdgeQualityPercentileWeight(percentileWeights, accumulator.PercentileBucketKey, store.EdgeQualityPercentileUpload, uploadBPS, requestCount)
	addEdgeQualityPercentileWeight(percentileWeights, accumulator.PercentileBucketKey, store.EdgeQualityPercentileMinWindow, sample.MinWindowBPS, requestCount)
	addEdgeQualityPercentileWeight(percentileWeights, accumulator.PercentileBucketKey, store.EdgeQualityPercentileMaxReadGap, sample.MaxReadGapMS, requestCount)
	addEdgeQualityPercentileWeight(percentileWeights, accumulator.PercentileBucketKey, store.EdgeQualityPercentileResponseEgress, sample.ResponseEgressBPS, requestCount)
	addEdgeQualityPercentileWeight(percentileWeights, accumulator.PercentileBucketKey, store.EdgeQualityPercentileResponseWrite, sample.ResponseWriteMS, requestCount)
}

func accumulateEdgeQualityRollupScalars(accumulator *edgeQualityRollupAccumulator, sample model.EdgePerformanceSample) (int, int64) {
	requestCount := sample.SampleCount
	if requestCount <= 0 {
		requestCount = 1
	}
	accumulator.SampleRecords++
	accumulator.RequestCount += requestCount
	accumulator.ErrorCount += sample.ErrorCount
	accumulator.CacheHitCount += sample.CacheHitCount
	accumulator.CacheObservationCount += sample.CacheObservationCount
	accumulator.BodyIncompleteCount += sample.BodyIncompleteCount
	accumulator.BodyReadErrorCount += sample.BodyReadErrorCount
	accumulator.ClientCancelCount += sample.ClientCancelCount
	accumulator.UpstreamWeightedMS += float64(sample.UpstreamMS) * float64(requestCount)
	accumulator.TotalWeightedMS += float64(sample.TotalMS) * float64(requestCount)
	accumulator.addAverage(sample.OriginDNSMS, requestCount, &accumulator.OriginDNSWeightedMS, &accumulator.OriginDNSSampleCount)
	accumulator.addAverage(sample.OriginConnectMS, requestCount, &accumulator.OriginConnectWeightedMS, &accumulator.OriginConnectSampleCount)
	accumulator.addAverage(sample.OriginRequestWriteMS, requestCount, &accumulator.OriginWriteWeightedMS, &accumulator.OriginWriteSampleCount)
	accumulator.addAverage(sample.OriginResponseWaitMS, requestCount, &accumulator.OriginWaitWeightedMS, &accumulator.OriginWaitSampleCount)
	accumulator.addAverage(sample.OriginTTFBMS, requestCount, &accumulator.OriginTTFBWeightedMS, &accumulator.OriginTTFBSampleCount)
	accumulator.addAverage(sample.OriginTotalMS, requestCount, &accumulator.OriginTotalWeightedMS, &accumulator.OriginTotalSampleCount)
	uploadBPS := edgeDNSPerformanceUploadBPS(sample)
	if uploadBPS > 0 {
		accumulator.UploadWeightedBPS += float64(uploadBPS) * float64(requestCount)
		accumulator.UploadSampleCount += requestCount
	}
	if sample.MinWindowBPS > 0 {
		accumulator.MinWindowWeightedBPS += float64(sample.MinWindowBPS) * float64(requestCount)
		accumulator.MinWindowSampleCount += requestCount
	}
	if sample.MaxReadGapMS > 0 {
		accumulator.MaxReadGapSampleCount += requestCount
	}
	accumulator.addAverage(sample.BodyReadBlockMS, requestCount, &accumulator.BodyReadBlockWeightedMS, &accumulator.BodyReadBlockSampleCount)
	if sample.ResponseEgressBPS > 0 {
		accumulator.ResponseEgressWeightedBPS += float64(sample.ResponseEgressBPS) * float64(requestCount)
		accumulator.ResponseEgressSampleCount += requestCount
	}
	if sample.ResponseWriteMS > 0 {
		accumulator.ResponseWriteSampleCount += requestCount
	}
	if sample.ClientTCPRTTMS > 0 || sample.ClientTCPMinRTTMS > 0 || sample.ClientTCPRTTVarMS > 0 {
		accumulator.ClientTCPRTTWeighted += sample.ClientTCPRTTMS * float64(requestCount)
		accumulator.ClientTCPMinRTTWeighted += sample.ClientTCPMinRTTMS * float64(requestCount)
		accumulator.ClientTCPRTTVarWeighted += sample.ClientTCPRTTVarMS * float64(requestCount)
		accumulator.ClientTCPMetricSampleCount += requestCount
	}
	if sample.ClientTCPRetransRate > 0 || sample.ClientTCPBytesRetransRate > 0 || sample.ClientTCPRTORate > 0 {
		accumulator.ClientTCPRetransRateWeighted += sample.ClientTCPRetransRate * float64(requestCount)
		accumulator.ClientTCPBytesRateWeighted += sample.ClientTCPBytesRetransRate * float64(requestCount)
		accumulator.ClientTCPRTORateWeighted += sample.ClientTCPRTORate * float64(requestCount)
		accumulator.ClientTCPRateSampleCount += requestCount
	}
	if sample.ClientTCPDeliveryBPS > 0 {
		accumulator.ClientTCPDeliveryWeighted += float64(sample.ClientTCPDeliveryBPS) * float64(requestCount)
		accumulator.ClientTCPDeliverySampleCount += requestCount
	}
	if sample.ActiveRequests > 0 || sample.ActiveBodyBuffers > 0 || sample.GoroutineCount > 0 || sample.MemoryAllocBytes > 0 {
		accumulator.ActiveRequestsWeighted += float64(sample.ActiveRequests) * float64(requestCount)
		accumulator.ActiveBodyBuffersWeighted += float64(sample.ActiveBodyBuffers) * float64(requestCount)
		accumulator.GoroutineCountWeighted += float64(sample.GoroutineCount) * float64(requestCount)
		accumulator.MemoryAllocWeighted += float64(sample.MemoryAllocBytes) * float64(requestCount)
		accumulator.SaturationSampleCount += requestCount
	}
	return requestCount, uploadBPS
}

func (a *edgeQualityRollupAccumulator) addAverage(value int64, weight int, weighted *float64, count *int) {
	if value <= 0 || weight <= 0 {
		return
	}
	*weighted += float64(value) * float64(weight)
	*count += weight
}

func addEdgeQualityPercentileWeight(weights map[store.EdgeQualityPercentileValueKey]int, bucketKey string, metric store.EdgeQualityPercentileMetric, value int64, weight int) {
	if weights == nil || value <= 0 || weight <= 0 {
		return
	}
	weights[store.EdgeQualityPercentileValueKey{BucketKey: bucketKey, Metric: metric, Value: value}] += weight
}

func (a *edgeQualityRollupAccumulator) rollup(percentiles store.EdgeQualityPercentileSet, now time.Time) model.EdgeQualityRollup {
	requestCount := a.RequestCount
	if requestCount <= 0 {
		requestCount = a.SampleRecords
	}
	rollup := model.EdgeQualityRollup{
		Window:                    a.Target.Window,
		WindowStartedAt:           a.Target.StartedAt,
		WindowEndedAt:             a.Target.EndedAt,
		Hostname:                  a.Key.Hostname,
		TrafficClass:              a.Key.TrafficClass,
		Method:                    a.Key.Method,
		PathPrefixBucket:          a.Key.PathPrefixBucket,
		ClientScopeKind:           a.Key.ClientScopeKind,
		ClientScopeValue:          a.Key.ClientScopeValue,
		EdgeGroupID:               a.Key.EdgeGroupID,
		EdgeID:                    a.Key.EdgeID,
		SampleCount:               a.SampleRecords,
		RequestCount:              requestCount,
		ErrorCount:                a.ErrorCount,
		CacheHitCount:             a.CacheHitCount,
		CacheObservationCount:     a.CacheObservationCount,
		P50TTFBMS:                 percentiles[store.EdgeQualityPercentileTTFB].P50,
		P95TTFBMS:                 percentiles[store.EdgeQualityPercentileTTFB].P95,
		P99TTFBMS:                 percentiles[store.EdgeQualityPercentileTTFB].P99,
		AvgUpstreamMS:             divideWeighted(a.UpstreamWeightedMS, requestCount),
		AvgTotalMS:                divideWeighted(a.TotalWeightedMS, requestCount),
		AvgOriginDNSMS:            divideWeighted(a.OriginDNSWeightedMS, a.OriginDNSSampleCount),
		AvgOriginConnectMS:        divideWeighted(a.OriginConnectWeightedMS, a.OriginConnectSampleCount),
		AvgOriginRequestWriteMS:   divideWeighted(a.OriginWriteWeightedMS, a.OriginWriteSampleCount),
		AvgOriginResponseWaitMS:   divideWeighted(a.OriginWaitWeightedMS, a.OriginWaitSampleCount),
		AvgOriginTTFBMS:           divideWeighted(a.OriginTTFBWeightedMS, a.OriginTTFBSampleCount),
		AvgOriginTotalMS:          divideWeighted(a.OriginTotalWeightedMS, a.OriginTotalSampleCount),
		AvgUploadEffectiveBPS:     divideWeighted(a.UploadWeightedBPS, a.UploadSampleCount),
		P10UploadEffectiveBPS:     percentiles[store.EdgeQualityPercentileUpload].P10,
		AvgMinWindowBPS:           divideWeighted(a.MinWindowWeightedBPS, a.MinWindowSampleCount),
		P10MinWindowBPS:           percentiles[store.EdgeQualityPercentileMinWindow].P10,
		P95MaxReadGapMS:           percentiles[store.EdgeQualityPercentileMaxReadGap].P95,
		AvgBodyReadBlockMS:        divideWeighted(a.BodyReadBlockWeightedMS, a.BodyReadBlockSampleCount),
		AvgResponseEgressBPS:      divideWeighted(a.ResponseEgressWeightedBPS, a.ResponseEgressSampleCount),
		P10ResponseEgressBPS:      percentiles[store.EdgeQualityPercentileResponseEgress].P10,
		P95ResponseWriteMS:        percentiles[store.EdgeQualityPercentileResponseWrite].P95,
		AvgClientTCPRTTMS:         divideWeighted(a.ClientTCPRTTWeighted, a.ClientTCPMetricSampleCount),
		AvgClientTCPMinRTTMS:      divideWeighted(a.ClientTCPMinRTTWeighted, a.ClientTCPMetricSampleCount),
		AvgClientTCPRTTVarMS:      divideWeighted(a.ClientTCPRTTVarWeighted, a.ClientTCPMetricSampleCount),
		ClientTCPRetransRate:      divideWeighted(a.ClientTCPRetransRateWeighted, a.ClientTCPRateSampleCount),
		ClientTCPBytesRetransRate: divideWeighted(a.ClientTCPBytesRateWeighted, a.ClientTCPRateSampleCount),
		ClientTCPRTORate:          divideWeighted(a.ClientTCPRTORateWeighted, a.ClientTCPRateSampleCount),
		AvgClientTCPDeliveryBPS:   divideWeighted(a.ClientTCPDeliveryWeighted, a.ClientTCPDeliverySampleCount),
		AvgActiveRequests:         divideWeighted(a.ActiveRequestsWeighted, a.SaturationSampleCount),
		AvgActiveBodyBuffers:      divideWeighted(a.ActiveBodyBuffersWeighted, a.SaturationSampleCount),
		AvgGoroutineCount:         divideWeighted(a.GoroutineCountWeighted, a.SaturationSampleCount),
		AvgMemoryAllocBytes:       divideWeighted(a.MemoryAllocWeighted, a.SaturationSampleCount),
		UpdatedAt:                 now,
	}
	if requestCount > 0 {
		rollup.ErrorRate = float64(a.ErrorCount) / float64(requestCount)
		rollup.BodyIncompleteRate = float64(a.BodyIncompleteCount) / float64(requestCount)
		rollup.BodyReadErrorRate = float64(a.BodyReadErrorCount) / float64(requestCount)
		rollup.ClientCancelRate = float64(a.ClientCancelCount) / float64(requestCount)
	}
	if a.CacheObservationCount > 0 {
		rollup.CacheHitRate = float64(a.CacheHitCount) / float64(a.CacheObservationCount)
	}
	rollup.Confidence = edgeQualityRollupConfidence(rollup)
	rollup.Score, rollup.ScoreBreakdown = scoreEdgeQualityRollup(rollup)
	return rollup
}

func scoreEdgeQualityRollup(rollup model.EdgeQualityRollup) (float64, map[string]float64) {
	profile := edgeDNSLatencyCandidateProfile{
		EdgeGroupID:               rollup.EdgeGroupID,
		EdgeID:                    rollup.EdgeID,
		ScoreBreakdown:            map[string]float64{},
		TrafficClass:              rollup.TrafficClass,
		TTFBMS:                    firstPositiveFloat(rollup.P95TTFBMS, rollup.P50TTFBMS),
		UpstreamMS:                rollup.AvgUpstreamMS,
		TotalMS:                   rollup.AvgTotalMS,
		HitRatio:                  rollup.CacheHitRate,
		ErrorRate:                 rollup.ErrorRate,
		UploadBPS:                 firstPositiveFloat(rollup.P10UploadEffectiveBPS, rollup.AvgUploadEffectiveBPS),
		BodyReadMS:                rollup.AvgBodyReadBlockMS,
		MaxReadGapMS:              rollup.P95MaxReadGapMS,
		BodyIncompleteRate:        rollup.BodyIncompleteRate,
		BodyReadErrorRate:         rollup.BodyReadErrorRate,
		ResponseEgressBPS:         firstPositiveFloat(rollup.P10ResponseEgressBPS, rollup.AvgResponseEgressBPS),
		ResponseWriteMS:           rollup.P95ResponseWriteMS,
		OriginConnectMS:           rollup.AvgOriginConnectMS,
		OriginWriteMS:             rollup.AvgOriginRequestWriteMS,
		OriginWaitMS:              rollup.AvgOriginResponseWaitMS,
		OriginTTFBMS:              rollup.AvgOriginTTFBMS,
		OriginTotalMS:             rollup.AvgOriginTotalMS,
		ActiveRequests:            rollup.AvgActiveRequests,
		ActiveBodyBuffers:         rollup.AvgActiveBodyBuffers,
		ClientTCPRTTMS:            rollup.AvgClientTCPRTTMS,
		ClientTCPMinRTTMS:         rollup.AvgClientTCPMinRTTMS,
		ClientTCPRTTVarMS:         rollup.AvgClientTCPRTTVarMS,
		ClientTCPRetransRate:      rollup.ClientTCPRetransRate,
		ClientTCPBytesRetransRate: rollup.ClientTCPBytesRetransRate,
		ClientTCPRTORate:          rollup.ClientTCPRTORate,
		ClientTCPDeliveryBPS:      rollup.AvgClientTCPDeliveryBPS,
		Confidence:                rollup.Confidence,
		ConfidencePenalty:         edgeDNSLatencyConfidencePenalty(rollup.Confidence),
		SampleCount:               rollup.RequestCount,
		BodySampleCount:           rollup.RequestCount,
	}
	score := edgeDNSLatencyScore(profile)
	breakdown := cloneFloat64Map(profile.ScoreBreakdown)
	if penalty, _ := edgeQualitySevereDegradePenalty(rollup); penalty > 0 {
		breakdown["severe_degrade"] = penalty
		score += penalty
	}
	return score, breakdown
}

func edgeQualityRollupConfidence(rollup model.EdgeQualityRollup) float64 {
	requestConfidence := math.Min(1, float64(rollup.RequestCount)/50)
	recordConfidence := math.Min(1, float64(rollup.SampleCount)/10)
	metricCompleteness := 0.45
	if rollup.P50TTFBMS > 0 || rollup.P95TTFBMS > 0 || rollup.AvgTotalMS > 0 {
		metricCompleteness += 0.15
	}
	if rollup.AvgUploadEffectiveBPS > 0 || rollup.AvgResponseEgressBPS > 0 {
		metricCompleteness += 0.15
	}
	if rollup.AvgClientTCPRTTMS > 0 || rollup.ClientTCPRetransRate > 0 || rollup.ClientTCPRTORate > 0 {
		metricCompleteness += 0.15
	}
	if rollup.CacheObservationCount > 0 {
		metricCompleteness += 0.05
	}
	if rollup.AvgActiveRequests > 0 || rollup.AvgActiveBodyBuffers > 0 {
		metricCompleteness += 0.05
	}
	if metricCompleteness > 1 {
		metricCompleteness = 1
	}
	confidence := requestConfidence * (0.7 + 0.3*recordConfidence) * metricCompleteness
	if confidence < 0 {
		return 0
	}
	if confidence > 1 {
		return 1
	}
	return confidence
}

func edgeQualitySevereDegradePenalty(rollup model.EdgeQualityRollup) (float64, string) {
	if strings.TrimSpace(rollup.Window) != "5m" {
		return 0, ""
	}
	switch {
	case rollup.RequestCount >= 10 && rollup.ErrorRate >= 0.20:
		return 1200, "5m_error_rate"
	case rollup.RequestCount >= 10 && rollup.BodyReadErrorRate+rollup.BodyIncompleteRate >= 0.08:
		return 1100, "5m_body_read_failures"
	case rollup.RequestCount >= 5 && firstPositiveFloat(rollup.P10UploadEffectiveBPS, rollup.AvgUploadEffectiveBPS) > 0 && firstPositiveFloat(rollup.P10UploadEffectiveBPS, rollup.AvgUploadEffectiveBPS) < 32*1024:
		return 900, "5m_upload_collapse"
	case rollup.RequestCount >= 10 && (rollup.ClientTCPRetransRate >= 0.12 || rollup.ClientTCPRTORate >= 0.08):
		return 900, "5m_tcp_loss"
	case rollup.AvgActiveRequests >= 250 || rollup.AvgActiveBodyBuffers >= 100:
		return 700, "5m_saturation"
	default:
		return 0, ""
	}
}

type edgeQualityRollupScope struct {
	Kind  string
	Value string
}

func edgeQualityRollupScopesForSample(sample model.EdgePerformanceSample) []edgeQualityRollupScope {
	scopes := []edgeQualityRollupScope{{Kind: "global", Value: "global"}}
	if !edgeDNSPerformanceSampleHasClientScope(sample) {
		return scopes
	}
	country := strings.ToLower(strings.TrimSpace(sample.ClientCountry))
	region := strings.ToLower(strings.TrimSpace(sample.ClientRegion))
	asn := strings.ToLower(strings.TrimSpace(sample.ClientASN))
	if country != "" {
		scopes = append(scopes, edgeQualityRollupScope{Kind: "country", Value: country})
	}
	if country != "" && region != "" {
		scopes = append(scopes, edgeQualityRollupScope{Kind: "region", Value: country + ":" + region})
	}
	if asn != "" {
		scopes = append(scopes, edgeQualityRollupScope{Kind: "asn", Value: asn})
	}
	return scopes
}

func divideWeighted(sum float64, count int) float64 {
	if count <= 0 {
		return 0
	}
	return sum / float64(count)
}

func firstPositiveFloat(values ...float64) float64 {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

func truncateMetricLabel(value string, max int) string {
	value = strings.TrimSpace(value)
	if max <= 0 || len(value) <= max {
		return value
	}
	return value[:max]
}

func formatEdgeQualityRollupReason(rollup model.EdgeQualityRollup) string {
	if penalty, reason := edgeQualitySevereDegradePenalty(rollup); penalty > 0 {
		return fmt.Sprintf("scoped_quality_rollup_%s_penalty_%.0f_confidence_%d_pct", reason, penalty, int(rollup.Confidence*100+0.5))
	}
	return fmt.Sprintf("scoped_quality_rollup_confidence_%d_pct", int(rollup.Confidence*100+0.5))
}
