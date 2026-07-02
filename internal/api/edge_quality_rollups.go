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
)

const edgeQualityRollupBuilderInterval = 5 * time.Minute
const edgeQualityPlatformRollupHostname = "__platform__"

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
			count, buildErr = s.rebuildEdgeQualityRollups(now)
			return buildErr
		})
	} else {
		count, err = s.rebuildEdgeQualityRollups(now)
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

func (s *Server) rebuildEdgeQualityRollups(now time.Time) (int, error) {
	if s == nil || s.store == nil {
		return 0, nil
	}
	now = now.UTC()
	earliest := now.Add(-24 * time.Hour)
	samples, err := s.store.ListEdgePerformanceSamples("", earliest)
	if err != nil {
		return 0, err
	}
	retention := make(map[string]time.Time, len(edgeQualityRollupWindows))
	var rollups []model.EdgeQualityRollup
	for _, window := range edgeQualityRollupWindows {
		retention[window.Name] = now.Add(-window.Retention)
		endedAt := now.Truncate(window.Duration)
		if endedAt.IsZero() {
			endedAt = now
		}
		startedAt := endedAt.Add(-window.Duration)
		rollups = append(rollups, buildEdgeQualityRollupsForWindow(samples, window.Name, startedAt, endedAt, now)...)
	}
	if err := s.store.UpsertEdgeQualityRollups(rollups, retention); err != nil {
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

type edgeQualityWeightedValue struct {
	Value  float64
	Weight int
}

type edgeQualityRollupAccumulator struct {
	Key                          edgeQualityRollupKey
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
	TTFBValues                   []edgeQualityWeightedValue
	UploadValues                 []edgeQualityWeightedValue
	MinWindowValues              []edgeQualityWeightedValue
	MaxReadGapValues             []edgeQualityWeightedValue
	ResponseEgressValues         []edgeQualityWeightedValue
	ResponseWriteValues          []edgeQualityWeightedValue
}

func buildEdgeQualityRollupsForWindow(samples []model.EdgePerformanceSample, window string, startedAt, endedAt, now time.Time) []model.EdgeQualityRollup {
	accumulators := map[string]*edgeQualityRollupAccumulator{}
	for _, sample := range samples {
		if sample.SampledAt.Before(startedAt) || !sample.SampledAt.Before(endedAt) {
			continue
		}
		hostname := normalizeExternalAppDomain(sample.Hostname)
		if hostname == "" || strings.TrimSpace(sample.EdgeGroupID) == "" {
			continue
		}
		for _, scope := range edgeQualityRollupScopesForSample(sample) {
			for _, rollupHostname := range []string{hostname, edgeQualityPlatformRollupHostname} {
				edgeIDs := []string{""}
				if edgeID := strings.TrimSpace(sample.EdgeID); edgeID != "" {
					edgeIDs = append(edgeIDs, edgeID)
				}
				for _, edgeID := range edgeIDs {
					key := edgeQualityRollupKey{
						Hostname:         rollupHostname,
						TrafficClass:     normalizeEdgeTrafficClass(sample.TrafficClass),
						Method:           strings.ToUpper(strings.TrimSpace(sample.Method)),
						PathPrefixBucket: edgeQualityPathPrefixBucket(sample.PathPrefix),
						ClientScopeKind:  scope.Kind,
						ClientScopeValue: scope.Value,
						EdgeGroupID:      strings.TrimSpace(sample.EdgeGroupID),
						EdgeID:           edgeID,
					}
					accumulateEdgeQualityRollup(accumulators, key, sample)
				}
			}
		}
	}
	out := make([]model.EdgeQualityRollup, 0, len(accumulators))
	for _, accumulator := range accumulators {
		out = append(out, accumulator.rollup(window, startedAt, endedAt, now))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Hostname != out[j].Hostname {
			return out[i].Hostname < out[j].Hostname
		}
		if out[i].TrafficClass != out[j].TrafficClass {
			return out[i].TrafficClass < out[j].TrafficClass
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
	return out
}

func accumulateEdgeQualityRollup(accumulators map[string]*edgeQualityRollupAccumulator, key edgeQualityRollupKey, sample model.EdgePerformanceSample) {
	keyString := strings.Join([]string{key.Hostname, key.TrafficClass, key.Method, key.PathPrefixBucket, key.ClientScopeKind, key.ClientScopeValue, key.EdgeGroupID, key.EdgeID}, "\x00")
	accumulator := accumulators[keyString]
	if accumulator == nil {
		accumulator = &edgeQualityRollupAccumulator{Key: key}
		accumulators[keyString] = accumulator
	}
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
	accumulator.addWeighted(&accumulator.TTFBValues, sample.TTFBMS, requestCount)
	accumulator.addAverage(sample.OriginDNSMS, requestCount, &accumulator.OriginDNSWeightedMS, &accumulator.OriginDNSSampleCount)
	accumulator.addAverage(sample.OriginConnectMS, requestCount, &accumulator.OriginConnectWeightedMS, &accumulator.OriginConnectSampleCount)
	accumulator.addAverage(sample.OriginRequestWriteMS, requestCount, &accumulator.OriginWriteWeightedMS, &accumulator.OriginWriteSampleCount)
	accumulator.addAverage(sample.OriginResponseWaitMS, requestCount, &accumulator.OriginWaitWeightedMS, &accumulator.OriginWaitSampleCount)
	accumulator.addAverage(sample.OriginTTFBMS, requestCount, &accumulator.OriginTTFBWeightedMS, &accumulator.OriginTTFBSampleCount)
	accumulator.addAverage(sample.OriginTotalMS, requestCount, &accumulator.OriginTotalWeightedMS, &accumulator.OriginTotalSampleCount)
	if uploadBPS := edgeDNSPerformanceUploadBPS(sample); uploadBPS > 0 {
		accumulator.UploadWeightedBPS += float64(uploadBPS) * float64(requestCount)
		accumulator.UploadSampleCount += requestCount
		accumulator.addWeighted(&accumulator.UploadValues, uploadBPS, requestCount)
	}
	if sample.MinWindowBPS > 0 {
		accumulator.MinWindowWeightedBPS += float64(sample.MinWindowBPS) * float64(requestCount)
		accumulator.MinWindowSampleCount += requestCount
		accumulator.addWeighted(&accumulator.MinWindowValues, sample.MinWindowBPS, requestCount)
	}
	if sample.MaxReadGapMS > 0 {
		accumulator.MaxReadGapSampleCount += requestCount
		accumulator.addWeighted(&accumulator.MaxReadGapValues, sample.MaxReadGapMS, requestCount)
	}
	accumulator.addAverage(sample.BodyReadBlockMS, requestCount, &accumulator.BodyReadBlockWeightedMS, &accumulator.BodyReadBlockSampleCount)
	if sample.ResponseEgressBPS > 0 {
		accumulator.ResponseEgressWeightedBPS += float64(sample.ResponseEgressBPS) * float64(requestCount)
		accumulator.ResponseEgressSampleCount += requestCount
		accumulator.addWeighted(&accumulator.ResponseEgressValues, sample.ResponseEgressBPS, requestCount)
	}
	if sample.ResponseWriteMS > 0 {
		accumulator.ResponseWriteSampleCount += requestCount
		accumulator.addWeighted(&accumulator.ResponseWriteValues, sample.ResponseWriteMS, requestCount)
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
}

func (a *edgeQualityRollupAccumulator) addAverage(value int64, weight int, weighted *float64, count *int) {
	if value <= 0 || weight <= 0 {
		return
	}
	*weighted += float64(value) * float64(weight)
	*count += weight
}

func (a *edgeQualityRollupAccumulator) addWeighted(values *[]edgeQualityWeightedValue, value int64, weight int) {
	if value <= 0 || weight <= 0 {
		return
	}
	*values = append(*values, edgeQualityWeightedValue{Value: float64(value), Weight: weight})
}

func (a *edgeQualityRollupAccumulator) rollup(window string, startedAt, endedAt, now time.Time) model.EdgeQualityRollup {
	requestCount := a.RequestCount
	if requestCount <= 0 {
		requestCount = a.SampleRecords
	}
	rollup := model.EdgeQualityRollup{
		Window:                    window,
		WindowStartedAt:           startedAt,
		WindowEndedAt:             endedAt,
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
		P50TTFBMS:                 weightedQuantile(a.TTFBValues, 0.50),
		P95TTFBMS:                 weightedQuantile(a.TTFBValues, 0.95),
		P99TTFBMS:                 weightedQuantile(a.TTFBValues, 0.99),
		AvgUpstreamMS:             divideWeighted(a.UpstreamWeightedMS, requestCount),
		AvgTotalMS:                divideWeighted(a.TotalWeightedMS, requestCount),
		AvgOriginDNSMS:            divideWeighted(a.OriginDNSWeightedMS, a.OriginDNSSampleCount),
		AvgOriginConnectMS:        divideWeighted(a.OriginConnectWeightedMS, a.OriginConnectSampleCount),
		AvgOriginRequestWriteMS:   divideWeighted(a.OriginWriteWeightedMS, a.OriginWriteSampleCount),
		AvgOriginResponseWaitMS:   divideWeighted(a.OriginWaitWeightedMS, a.OriginWaitSampleCount),
		AvgOriginTTFBMS:           divideWeighted(a.OriginTTFBWeightedMS, a.OriginTTFBSampleCount),
		AvgOriginTotalMS:          divideWeighted(a.OriginTotalWeightedMS, a.OriginTotalSampleCount),
		AvgUploadEffectiveBPS:     divideWeighted(a.UploadWeightedBPS, a.UploadSampleCount),
		P10UploadEffectiveBPS:     weightedQuantile(a.UploadValues, 0.10),
		AvgMinWindowBPS:           divideWeighted(a.MinWindowWeightedBPS, a.MinWindowSampleCount),
		P10MinWindowBPS:           weightedQuantile(a.MinWindowValues, 0.10),
		P95MaxReadGapMS:           weightedQuantile(a.MaxReadGapValues, 0.95),
		AvgBodyReadBlockMS:        divideWeighted(a.BodyReadBlockWeightedMS, a.BodyReadBlockSampleCount),
		AvgResponseEgressBPS:      divideWeighted(a.ResponseEgressWeightedBPS, a.ResponseEgressSampleCount),
		P10ResponseEgressBPS:      weightedQuantile(a.ResponseEgressValues, 0.10),
		P95ResponseWriteMS:        weightedQuantile(a.ResponseWriteValues, 0.95),
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

func weightedQuantile(values []edgeQualityWeightedValue, q float64) float64 {
	if len(values) == 0 {
		return 0
	}
	if q <= 0 {
		q = 0
	}
	if q > 1 {
		q = 1
	}
	sort.Slice(values, func(i, j int) bool {
		return values[i].Value < values[j].Value
	})
	total := 0
	for _, value := range values {
		if value.Weight > 0 {
			total += value.Weight
		}
	}
	if total <= 0 {
		return 0
	}
	target := int(math.Ceil(q * float64(total)))
	if target <= 0 {
		target = 1
	}
	seen := 0
	for _, value := range values {
		if value.Weight <= 0 {
			continue
		}
		seen += value.Weight
		if seen >= target {
			return value.Value
		}
	}
	return values[len(values)-1].Value
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
