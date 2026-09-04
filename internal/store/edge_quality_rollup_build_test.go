package store

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestEdgeQualityRollupBuildSessionChunksAndCommitsWatermark(t *testing.T) {
	t.Parallel()

	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	var samples []model.EdgePerformanceSample
	for index := 0; index < 5; index++ {
		samples = append(samples, model.EdgePerformanceSample{
			ID:          model.NewID("sample"),
			EdgeGroupID: "edge-group-a",
			Hostname:    "service.example.test",
			TTFBMS:      int64(100 + index*100),
			SampleCount: index + 1,
			SampledAt:   now.Add(time.Duration(index-5) * time.Minute),
		})
	}
	if err := stateStore.RecordEdgePerformanceSamples(samples, time.Time{}); err != nil {
		t.Fatalf("record samples: %v", err)
	}

	session, err := stateStore.BeginEdgeQualityRollupBuild(context.Background())
	if err != nil {
		t.Fatalf("begin build: %v", err)
	}
	defer session.Rollback()
	var chunkLengths []int
	if err := session.ForEachSampleChunk(now.Add(-10*time.Minute), now, 2, func(chunk []model.EdgePerformanceSample) error {
		chunkLengths = append(chunkLengths, len(chunk))
		weights := map[EdgeQualityPercentileValueKey]int{}
		for _, sample := range chunk {
			weights[EdgeQualityPercentileValueKey{BucketKey: "bucket-a", Metric: EdgeQualityPercentileTTFB, Value: sample.TTFBMS}] += sample.SampleCount
		}
		return session.AddPercentileWeights(weights)
	}); err != nil {
		t.Fatalf("iterate chunks: %v", err)
	}
	if !reflect.DeepEqual(chunkLengths, []int{2, 2, 1}) {
		t.Fatalf("chunk lengths = %v, want [2 2 1]", chunkLengths)
	}
	percentiles, err := session.Percentiles()
	if err != nil {
		t.Fatalf("calculate percentiles: %v", err)
	}
	if got := percentiles["bucket-a"][EdgeQualityPercentileTTFB]; got != (EdgeQualityPercentiles{P10: 200, P50: 400, P95: 500, P99: 500}) {
		t.Fatalf("unexpected exact weighted percentiles: %+v", got)
	}
	rollup := model.EdgeQualityRollup{
		Window:           "5m",
		WindowStartedAt:  now.Add(-5 * time.Minute),
		WindowEndedAt:    now,
		Hostname:         "service.example.test",
		ClientScopeKind:  "global",
		ClientScopeValue: "global",
		EdgeGroupID:      "edge-group-a",
		P50TTFBMS:        400,
		UpdatedAt:        now,
	}
	if err := session.Commit([]model.EdgeQualityRollup{rollup}, nil, map[string]time.Time{"5m": now}); err != nil {
		t.Fatalf("commit rollup and watermark: %v", err)
	}

	check, err := stateStore.BeginEdgeQualityRollupBuild(context.Background())
	if err != nil {
		t.Fatalf("begin watermark check: %v", err)
	}
	defer check.Rollback()
	watermarks, err := check.Watermarks()
	if err != nil {
		t.Fatalf("read watermarks: %v", err)
	}
	if !watermarks["5m"].Equal(now) {
		t.Fatalf("5m watermark = %s, want %s", watermarks["5m"], now)
	}
}

func TestEdgeQualityRollupBuildFailurePreservesRollupLKGAndWatermark(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "store.json")
	stateStore := New(path)
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	old := model.EdgeQualityRollup{
		Window:           "5m",
		WindowStartedAt:  now.Add(-5 * time.Minute),
		WindowEndedAt:    now,
		Hostname:         "service.example.test",
		ClientScopeKind:  "global",
		ClientScopeValue: "global",
		EdgeGroupID:      "edge-group-a",
		Score:            1,
	}
	if err := stateStore.UpsertEdgeQualityRollups([]model.EdgeQualityRollup{old}, nil); err != nil {
		t.Fatalf("seed rollup LKG: %v", err)
	}
	session, err := stateStore.BeginEdgeQualityRollupBuild(context.Background())
	if err != nil {
		t.Fatalf("begin failed build: %v", err)
	}
	newRollup := old
	newRollup.Score = 999
	newRollup.WindowStartedAt = now
	newRollup.WindowEndedAt = now.Add(5 * time.Minute)
	badPath := t.TempDir()
	stateStore.path = badPath
	err = session.Commit([]model.EdgeQualityRollup{newRollup}, nil, map[string]time.Time{"5m": now.Add(5 * time.Minute)})
	stateStore.path = path
	if err == nil {
		t.Fatal("expected failed durable commit")
	}
	session.Rollback()

	rollups, err := stateStore.ListEdgeQualityRollups("service.example.test", "5m", time.Time{})
	if err != nil {
		t.Fatalf("list rollup LKG: %v", err)
	}
	if len(rollups) != 1 || rollups[0].Score != 1 || !rollups[0].WindowEndedAt.Equal(now) {
		t.Fatalf("failed build changed rollup LKG: %+v", rollups)
	}
	check, err := stateStore.BeginEdgeQualityRollupBuild(context.Background())
	if err != nil {
		t.Fatalf("begin watermark check: %v", err)
	}
	defer check.Rollback()
	watermarks, err := check.Watermarks()
	if err != nil {
		t.Fatalf("read watermarks: %v", err)
	}
	if !watermarks["5m"].Equal(now) {
		t.Fatalf("failed build advanced watermark beyond rollup LKG: %+v", watermarks)
	}
}

func TestEdgeQualityPercentilesPostgresMatchGroupedReference(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("FUGUE_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set FUGUE_TEST_DATABASE_URL to run edge-quality percentile Postgres integration test")
	}
	if !strings.Contains(databaseURL, "fugue-pgtest") && !strings.Contains(databaseURL, "fugue_test") {
		t.Fatalf("refusing non-disposable Postgres integration target %q", databaseURL)
	}
	schemaName := model.NewID("edge_quality_percentile")
	quotedSchema := quotePostgresIntegrationIdentifier(t, schemaName)
	schemaURL := postgresIntegrationURLWithSearchPath(t, databaseURL, schemaName)
	baseDB, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatalf("open disposable Postgres database: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := baseDB.ExecContext(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		_ = baseDB.Close()
		t.Fatalf("create disposable Postgres schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = baseDB.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+quotedSchema+" CASCADE")
		_ = baseDB.Close()
	})
	schemaDB, err := sql.Open("pgx", schemaURL)
	if err != nil {
		t.Fatalf("open disposable Postgres schema: %v", err)
	}
	t.Cleanup(func() { _ = schemaDB.Close() })
	for _, table := range []string{
		"CREATE TABLE IF NOT EXISTS fugue_edge_performance_samples",
		"CREATE TABLE IF NOT EXISTS fugue_edge_quality_rollups",
		"CREATE TABLE IF NOT EXISTS fugue_edge_quality_rollup_watermarks",
	} {
		execPostgresSchemaStatementContaining(t, ctx, schemaDB, table)
	}
	stateStore := &Store{databaseURL: schemaURL, db: schemaDB, dbReady: true}
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	for index, ttfb := range []int64{100, 300, 200} {
		if _, err := schemaDB.ExecContext(ctx, `
INSERT INTO fugue_edge_performance_samples (id, edge_id, edge_group_id, hostname, ttfb_ms, sample_count, sampled_at)
VALUES ($1, 'edge-a', 'edge-group-a', 'service.example.test', $2, $3, $4)`,
			model.NewID("sample"), ttfb, index+1, now.Add(time.Duration(index-3)*time.Minute)); err != nil {
			t.Fatalf("insert sample: %v", err)
		}
	}
	if _, err := schemaDB.ExecContext(ctx, `
INSERT INTO fugue_edge_performance_samples (
	id, edge_id, edge_group_id, hostname, path_prefix, method, traffic_class,
	client_country, client_region, client_asn, ttfb_ms, sample_count, sampled_at
) VALUES (
	'exact-dimensions', '', 'edge-group-b', 'Mixed.Example.Test.', '/assets/app.js', 'get', 'STATIC_CACHEABLE',
	'US', 'CA', 'AS64500', 444, 2, $1
)`, now.Add(-time.Minute)); err != nil {
		t.Fatalf("insert exact-dimension sample: %v", err)
	}
	session, err := stateStore.BeginEdgeQualityRollupBuild(ctx)
	if err != nil {
		t.Fatalf("begin Postgres build: %v", err)
	}
	defer session.Rollback()
	var chunkLengths []int
	if err := session.ForEachSampleChunk(now.Add(-10*time.Minute), now, 2, func(samples []model.EdgePerformanceSample) error {
		chunkLengths = append(chunkLengths, len(samples))
		return nil
	}); err != nil {
		t.Fatalf("iterate Postgres sample cursor: %v", err)
	}
	if !reflect.DeepEqual(chunkLengths, []int{2, 2}) {
		t.Fatalf("Postgres sample cursor chunks = %v, want [2 2]", chunkLengths)
	}
	session.SetPercentileWindows([]EdgeQualityPercentileWindow{{ID: 0, Window: "5m", StartedAt: now.Add(-5 * time.Minute), EndedAt: now}})
	got, err := session.Percentiles()
	if err != nil {
		t.Fatalf("calculate Postgres percentiles: %v", err)
	}
	bucketKey := "0\x00" + strings.Join([]string{"service.example.test", "", "", "", "global", "global", "edge-group-a", ""}, "\x00")
	want := EdgeQualityPercentiles{P10: 100, P50: 200, P95: 300, P99: 300}
	if got := got[bucketKey][EdgeQualityPercentileTTFB]; got != want {
		t.Fatalf("Postgres weighted percentiles differ from reference: want=%+v got=%+v", want, got)
	}
	dimensionBucket := "0\x00" + strings.Join([]string{"mixed.example.test", "static_cacheable", "GET", "/assets/*", "region", "us:ca", "edge-group-b", ""}, "\x00")
	if got := got[dimensionBucket][EdgeQualityPercentileTTFB]; got != (EdgeQualityPercentiles{P10: 444, P50: 444, P95: 444, P99: 444}) {
		t.Fatalf("Postgres dimension expansion changed rollup key or percentile: %+v", got)
	}

	oldRollup := model.EdgeQualityRollup{
		Window:           "5m",
		WindowStartedAt:  now.Add(-5 * time.Minute),
		WindowEndedAt:    now,
		Hostname:         "service.example.test",
		ClientScopeKind:  "global",
		ClientScopeValue: "global",
		EdgeGroupID:      "edge-group-a",
		Score:            1,
		UpdatedAt:        now,
	}
	if err := session.Commit([]model.EdgeQualityRollup{oldRollup}, nil, map[string]time.Time{"5m": now}); err != nil {
		t.Fatalf("seed Postgres rollup LKG: %v", err)
	}

	failed, err := stateStore.BeginEdgeQualityRollupBuild(ctx)
	if err != nil {
		t.Fatalf("begin failing Postgres build: %v", err)
	}
	defer failed.Rollback()
	fallbackWatermarks, err := failed.Watermarks()
	if err != nil {
		t.Fatalf("read Postgres fallback watermarks: %v", err)
	}
	if !fallbackWatermarks["5m"].Equal(now) {
		t.Fatalf("Postgres watermark did not fall back to rollup LKG: %+v", fallbackWatermarks)
	}
	if _, err := failed.writeTx.ExecContext(ctx, `DROP TABLE fugue_edge_quality_rollup_watermarks`); err != nil {
		t.Fatalf("stage failing schema change: %v", err)
	}
	newRollup := oldRollup
	newRollup.Score = 999
	if err := failed.Commit([]model.EdgeQualityRollup{newRollup}, nil, map[string]time.Time{"5m": now.Add(5 * time.Minute)}); err == nil {
		t.Fatal("expected Postgres watermark failure")
	}
	failed.Rollback()
	rollups, err := stateStore.ListEdgeQualityRollups("service.example.test", "5m", time.Time{})
	if err != nil {
		t.Fatalf("list Postgres rollup LKG: %v", err)
	}
	if len(rollups) != 1 || rollups[0].Score != 1 {
		t.Fatalf("failed Postgres build changed rollup LKG: %+v", rollups)
	}
}

func execPostgresSchemaStatementContaining(t *testing.T, ctx context.Context, db *sql.DB, marker string) {
	t.Helper()
	for _, statement := range postgresSchemaStatements {
		if strings.Contains(statement, marker) {
			if _, err := db.ExecContext(ctx, statement); err != nil {
				t.Fatalf("execute schema statement %q: %v", marker, err)
			}
			return
		}
	}
	t.Fatalf("schema statement %q not found", marker)
}

func TestPostgresSchemaSeedsWatermarksFromExistingRollupLKG(t *testing.T) {
	t.Parallel()

	watermarkTable := -1
	watermarkSeed := -1
	for index, statement := range postgresSchemaStatements {
		switch {
		case strings.Contains(statement, "CREATE TABLE IF NOT EXISTS fugue_edge_quality_rollup_watermarks"):
			watermarkTable = index
		case strings.Contains(statement, "INSERT INTO fugue_edge_quality_rollup_watermarks"):
			watermarkSeed = index
			if !strings.Contains(statement, "MAX(window_ended_at)") ||
				!strings.Contains(statement, "WHERE NOT EXISTS (SELECT 1 FROM fugue_edge_quality_rollup_watermarks)") ||
				!strings.Contains(statement, "ON CONFLICT (window_name) DO NOTHING") {
				t.Fatalf("watermark seed must preserve existing values and initialize from rollup LKG: %s", statement)
			}
		}
	}
	if watermarkTable < 0 || watermarkSeed <= watermarkTable {
		t.Fatalf("watermark schema ordering invalid: table=%d seed=%d", watermarkTable, watermarkSeed)
	}
}
