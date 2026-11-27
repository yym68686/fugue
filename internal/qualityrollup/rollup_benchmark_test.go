package qualityrollup

import (
	"context"
	"math"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/qualityrollup/rolluptest"
	"fugue/internal/store"
)

type legacyEdgeQualityWeightedValue struct {
	Value  float64
	Weight int
}

type legacyEdgeQualityRollupAccumulator struct {
	rollup         *edgeQualityRollupAccumulator
	ttfb           []legacyEdgeQualityWeightedValue
	upload         []legacyEdgeQualityWeightedValue
	minWindow      []legacyEdgeQualityWeightedValue
	maxReadGap     []legacyEdgeQualityWeightedValue
	responseEgress []legacyEdgeQualityWeightedValue
	responseWrite  []legacyEdgeQualityWeightedValue
}

func TestChunkedEdgeQualityRollupsExactlyMatchLegacyEveryField(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	samples := rolluptest.Samples(2048, now, 8)
	want := buildLegacyCurrentEdgeQualityRollups(samples, now)
	for _, chunkSize := range []int{1, 17, edgeQualityRollupSampleChunkSize, len(samples)} {
		got := buildChunkedCurrentEdgeQualityRollups(samples, now, chunkSize)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("chunk size %d changed one or more rollup fields:\nwant=%#v\ngot=%#v", chunkSize, want, got)
		}
	}
}

func TestPlanEdgeQualityRollupWindowsUsesOnlyNewClosedWindows(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 5, 12, 3, 0, 0, time.UTC)
	plans, _, pending, earliest, latest := planEdgeQualityRollupWindows(now, nil)
	if len(plans) != len(edgeQualityRollupWindows) || edgeQualityRollupTargetCount(plans) != len(edgeQualityRollupWindows) {
		t.Fatalf("first run should plan only the latest closed window for each duration: plans=%+v", plans)
	}
	if earliest != now.Truncate(24*time.Hour).Add(-24*time.Hour) || latest != now.Truncate(5*time.Minute) {
		t.Fatalf("unexpected first-run scan bounds: earliest=%s latest=%s", earliest, latest)
	}
	watermarks := make(map[string]time.Time, len(pending))
	for window, endedAt := range pending {
		watermarks[window] = endedAt
	}
	plans, _, pending, _, _ = planEdgeQualityRollupWindows(now, watermarks)
	if len(plans) != 0 || len(pending) != 0 {
		t.Fatalf("unchanged closed windows must be skipped: plans=%+v pending=%+v", plans, pending)
	}

	watermarks["5m"] = now.Truncate(5 * time.Minute).Add(-15 * time.Minute)
	plans, _, pending, _, _ = planEdgeQualityRollupWindows(now, watermarks)
	if edgeQualityRollupTargetCount(plans) != 3 || pending["5m"] != now.Truncate(5*time.Minute) {
		t.Fatalf("expected exactly three newly closed 5m windows: plans=%+v pending=%+v", plans, pending)
	}
}

func TestRebuildEdgeQualityRollupsPersistsAndUsesWatermarks(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "store.json")
	stateStore := store.New(path)
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	now := time.Date(2026, 9, 5, 12, 3, 0, 0, time.UTC)
	if err := stateStore.RecordEdgePerformanceSamples([]model.EdgePerformanceSample{{
		ID:          "first",
		EdgeID:      "edge-a",
		EdgeGroupID: "edge-group-a",
		Hostname:    "service.example.test",
		TTFBMS:      100,
		SampleCount: 3,
		SampledAt:   now.Add(-4 * time.Minute),
	}}, time.Time{}); err != nil {
		t.Fatalf("record initial sample: %v", err)
	}
	builder := New(stateStore, nil)
	firstCount, err := builder.rebuild(context.Background(), now)
	if err != nil || firstCount == 0 {
		t.Fatalf("first rebuild count=%d err=%v", firstCount, err)
	}
	restarted := store.New(path)
	if err := restarted.Init(); err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	secondCount, err := New(restarted, nil).rebuild(context.Background(), now)
	if err != nil || secondCount != 0 {
		t.Fatalf("unchanged closed windows must not rebuild: count=%d err=%v", secondCount, err)
	}

	nextNow := now.Add(5 * time.Minute)
	if err := stateStore.RecordEdgePerformanceSamples([]model.EdgePerformanceSample{{
		ID:          "second",
		EdgeID:      "edge-a",
		EdgeGroupID: "edge-group-a",
		Hostname:    "service.example.test",
		TTFBMS:      200,
		SampleCount: 2,
		SampledAt:   now.Add(time.Minute),
	}}, time.Time{}); err != nil {
		t.Fatalf("record next-window sample: %v", err)
	}
	thirdCount, err := builder.rebuild(context.Background(), nextNow)
	if err != nil || thirdCount == 0 {
		t.Fatalf("newly closed 5m window was not built: count=%d err=%v", thirdCount, err)
	}
	rollups, err := stateStore.ListEdgeQualityRollups("service.example.test", "5m", now.Add(-10*time.Minute))
	if err != nil {
		t.Fatalf("list 5m rollups: %v", err)
	}
	if len(rollups) != 4 {
		t.Fatalf("expected group and node rollups for two 5m windows, got %d", len(rollups))
	}
}

func BenchmarkEdgeQualityRollupsLegacy(b *testing.B) {
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	samples := rolluptest.Samples(8192, now, 8)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		benchmarkEdgeQualityRollupSink = buildLegacyCurrentEdgeQualityRollups(samples, now)
	}
}

func BenchmarkEdgeQualityRollupsChunkedGrouped(b *testing.B) {
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	samples := rolluptest.Samples(8192, now, 8)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		benchmarkEdgeQualityRollupSink = buildChunkedCurrentEdgeQualityRollups(samples, now, edgeQualityRollupSampleChunkSize)
	}
}

func BenchmarkEdgeQualityRollupsPostgresLegacy(b *testing.B) {
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	samples := rolluptest.Samples(8192, now, 1)
	stateStore := rolluptest.PostgresStore(b, samples)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		loaded, err := stateStore.ListEdgePerformanceSamples("", now.Add(-24*time.Hour))
		if err != nil {
			b.Fatal(err)
		}
		benchmarkEdgeQualityRollupSink = buildLegacyCurrentEdgeQualityRollups(loaded, now)
	}
}

func BenchmarkEdgeQualityRollupsPostgresChunked(b *testing.B) {
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	samples := rolluptest.Samples(8192, now, 1)
	stateStore := rolluptest.PostgresStore(b, samples)
	wantLoaded, err := stateStore.ListEdgePerformanceSamples("", now.Add(-24*time.Hour))
	if err != nil {
		b.Fatal(err)
	}
	want := buildLegacyCurrentEdgeQualityRollups(wantLoaded, now)
	got := buildChunkedEdgeQualityRollupsFromStore(b, stateStore, now)
	if !reflect.DeepEqual(got, want) {
		b.Fatal("Postgres chunked rollups differ from legacy output")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		benchmarkEdgeQualityRollupSink = buildChunkedEdgeQualityRollupsFromStore(b, stateStore, now)
	}
}

var benchmarkEdgeQualityRollupSink []model.EdgeQualityRollup

func buildChunkedEdgeQualityRollupsFromStore(b *testing.B, stateStore *store.Store, now time.Time) []model.EdgeQualityRollup {
	b.Helper()
	session, err := stateStore.BeginEdgeQualityRollupBuild(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	defer session.Rollback()
	plans, _, _, earliest, latest := planEdgeQualityRollupWindows(now, nil)
	builder := newEdgeQualityRollupBuildState(plans)
	session.SetPercentileWindows(builder.percentileWindows())
	var batch map[store.EdgeQualityPercentileValueKey]int
	if session.NeedsPercentileWeights() {
		batch = map[store.EdgeQualityPercentileValueKey]int{}
	}
	if err := session.ForEachSampleChunk(earliest, latest, edgeQualityRollupSampleChunkSize, func(samples []model.EdgePerformanceSample) error {
		if batch != nil {
			clear(batch)
		}
		builder.addSamples(samples, batch)
		return session.AddPercentileWeights(batch)
	}); err != nil {
		b.Fatal(err)
	}
	percentiles, err := session.Percentiles()
	if err != nil {
		b.Fatal(err)
	}
	return builder.rollups(percentiles, now)
}

func buildChunkedCurrentEdgeQualityRollups(samples []model.EdgePerformanceSample, now time.Time, chunkSize int) []model.EdgeQualityRollup {
	plans, _, _, _, _ := planEdgeQualityRollupWindows(now, nil)
	builder := newEdgeQualityRollupBuildState(plans)
	weights := map[store.EdgeQualityPercentileValueKey]int{}
	batch := map[store.EdgeQualityPercentileValueKey]int{}
	for start := 0; start < len(samples); start += chunkSize {
		end := min(start+chunkSize, len(samples))
		clear(batch)
		builder.addSamples(samples[start:end], batch)
		for key, weight := range batch {
			weights[key] += weight
		}
	}
	return builder.rollups(edgeQualityPercentilesFromWeights(weights), now)
}

func buildLegacyCurrentEdgeQualityRollups(samples []model.EdgePerformanceSample, now time.Time) []model.EdgeQualityRollup {
	var rollups []model.EdgeQualityRollup
	for _, window := range edgeQualityRollupWindows {
		endedAt := now.Truncate(window.Duration)
		rollups = append(rollups, buildLegacyEdgeQualityRollupsForWindow(samples, window.Name, endedAt.Add(-window.Duration), endedAt, now)...)
	}
	sortEdgeQualityRollupOutput(rollups)
	return rollups
}

func buildLegacyEdgeQualityRollupsForWindow(samples []model.EdgePerformanceSample, window string, startedAt, endedAt, now time.Time) []model.EdgeQualityRollup {
	target := &edgeQualityRollupWindowTarget{Window: window, StartedAt: startedAt, EndedAt: endedAt}
	accumulators := map[string]*legacyEdgeQualityRollupAccumulator{}
	for _, sample := range samples {
		if sample.SampledAt.Before(startedAt) || !sample.SampledAt.Before(endedAt) {
			continue
		}
		hostname := normalizedHostname(sample.Hostname)
		if hostname == "" || strings.TrimSpace(sample.EdgeGroupID) == "" {
			continue
		}
		for _, scope := range edgeQualityRollupScopesForSample(sample) {
			for _, rollupHostname := range []string{hostname, PlatformHostname} {
				edgeIDs := []string{""}
				if edgeID := strings.TrimSpace(sample.EdgeID); edgeID != "" {
					edgeIDs = append(edgeIDs, edgeID)
				}
				for _, edgeID := range edgeIDs {
					key := edgeQualityRollupKey{
						Hostname:         rollupHostname,
						TrafficClass:     NormalizeTrafficClass(sample.TrafficClass),
						Method:           strings.ToUpper(strings.TrimSpace(sample.Method)),
						PathPrefixBucket: PathPrefixBucket(sample.PathPrefix),
						ClientScopeKind:  scope.Kind,
						ClientScopeValue: scope.Value,
						EdgeGroupID:      strings.TrimSpace(sample.EdgeGroupID),
						EdgeID:           edgeID,
					}
					keyString := strings.Join([]string{key.Hostname, key.TrafficClass, key.Method, key.PathPrefixBucket, key.ClientScopeKind, key.ClientScopeValue, key.EdgeGroupID, key.EdgeID}, "\x00")
					accumulator := accumulators[keyString]
					if accumulator == nil {
						accumulator = &legacyEdgeQualityRollupAccumulator{rollup: &edgeQualityRollupAccumulator{Target: target, Key: key}}
						accumulators[keyString] = accumulator
					}
					requestCount, uploadBPS := accumulateEdgeQualityRollupScalars(accumulator.rollup, sample)
					legacyAppendWeighted(&accumulator.ttfb, sample.TTFBMS, requestCount)
					legacyAppendWeighted(&accumulator.upload, uploadBPS, requestCount)
					legacyAppendWeighted(&accumulator.minWindow, sample.MinWindowBPS, requestCount)
					legacyAppendWeighted(&accumulator.maxReadGap, sample.MaxReadGapMS, requestCount)
					legacyAppendWeighted(&accumulator.responseEgress, sample.ResponseEgressBPS, requestCount)
					legacyAppendWeighted(&accumulator.responseWrite, sample.ResponseWriteMS, requestCount)
				}
			}
		}
	}
	out := make([]model.EdgeQualityRollup, 0, len(accumulators))
	for _, accumulator := range accumulators {
		percentiles := store.EdgeQualityPercentileSet{}
		percentiles[store.EdgeQualityPercentileTTFB] = store.EdgeQualityPercentiles{
			P50: legacyWeightedQuantile(accumulator.ttfb, 0.50),
			P95: legacyWeightedQuantile(accumulator.ttfb, 0.95),
			P99: legacyWeightedQuantile(accumulator.ttfb, 0.99),
		}
		percentiles[store.EdgeQualityPercentileUpload] = store.EdgeQualityPercentiles{P10: legacyWeightedQuantile(accumulator.upload, 0.10)}
		percentiles[store.EdgeQualityPercentileMinWindow] = store.EdgeQualityPercentiles{P10: legacyWeightedQuantile(accumulator.minWindow, 0.10)}
		percentiles[store.EdgeQualityPercentileMaxReadGap] = store.EdgeQualityPercentiles{P95: legacyWeightedQuantile(accumulator.maxReadGap, 0.95)}
		percentiles[store.EdgeQualityPercentileResponseEgress] = store.EdgeQualityPercentiles{P10: legacyWeightedQuantile(accumulator.responseEgress, 0.10)}
		percentiles[store.EdgeQualityPercentileResponseWrite] = store.EdgeQualityPercentiles{P95: legacyWeightedQuantile(accumulator.responseWrite, 0.95)}
		out = append(out, accumulator.rollup.rollup(percentiles, now))
	}
	sortEdgeQualityRollupOutput(out)
	return out
}

func legacyAppendWeighted(values *[]legacyEdgeQualityWeightedValue, value int64, weight int) {
	if value > 0 && weight > 0 {
		*values = append(*values, legacyEdgeQualityWeightedValue{Value: float64(value), Weight: weight})
	}
}

func legacyWeightedQuantile(values []legacyEdgeQualityWeightedValue, q float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sort.Slice(values, func(i, j int) bool { return values[i].Value < values[j].Value })
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

func edgeQualityRollupTargetCount(plans []edgeQualityRollupWindowPlan) int {
	count := 0
	for _, plan := range plans {
		count += len(plan.Targets)
	}
	return count
}
