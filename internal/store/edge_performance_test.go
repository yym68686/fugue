package store

import (
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestRecordEdgePerformanceSamplesPrunesAndListsByHostname(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}

	now := time.Date(2026, 5, 18, 10, 0, 0, 0, time.UTC)
	if err := s.RecordEdgePerformanceSamples([]model.EdgePerformanceSample{
		{
			ID:                    "old-us",
			EdgeID:                "edge-us-1",
			EdgeGroupID:           "edge-group-country-us",
			Hostname:              "Demo.Fugue.Pro.",
			ClientRegion:          "na",
			RuntimeRegion:         "us",
			TTFBMS:                400,
			UpstreamMS:            300,
			TotalMS:               450,
			StatusCode:            200,
			SampleCount:           2,
			CacheHitCount:         1,
			CacheObservationCount: 2,
			SampledAt:             now.Add(-3 * time.Hour),
		},
		{
			ID:                        "recent-hk",
			EdgeID:                    "edge-hk-1",
			EdgeGroupID:               "edge-group-country-hk",
			Hostname:                  "demo.fugue.pro",
			Method:                    "post",
			TrafficClass:              "large_body_api",
			ClientRegion:              "apac",
			RuntimeRegion:             "us",
			TTFBMS:                    120,
			UpstreamMS:                80,
			TotalMS:                   150,
			StatusCode:                200,
			SampleCount:               5,
			CacheHitCount:             5,
			CacheObservationCount:     5,
			UploadRequestCount:        5,
			BodyReadBlockMS:           250,
			UploadEffectiveBPS:        128 * 1024,
			MaxReadGapMS:              900,
			RequestBodyBytes:          2048,
			RequestBodyReadBytes:      1024,
			BodyIncompleteCount:       1,
			ResponseEgressBPS:         512 * 1024,
			OriginTTFBMS:              90,
			ActiveRequests:            3,
			ActiveBodyBuffers:         1,
			ClientTCPRTTMS:            88.5,
			ClientTCPMinRTTMS:         70.0,
			ClientTCPRTTVarMS:         12.5,
			ClientTCPTotalRetrans:     3,
			ClientTCPRetransRate:      0.02,
			ClientTCPBytesRetrans:     4096,
			ClientTCPBytesRetransRate: 0.03,
			ClientTCPTotalRTO:         1,
			ClientTCPRTORate:          0.01,
			ClientTCPDeliveryBPS:      2 * 1024 * 1024,
			SampledAt:                 now.Add(-30 * time.Minute),
		},
		{
			ID:          "other-host",
			EdgeGroupID: "edge-group-country-hk",
			Hostname:    "other.fugue.pro",
			TTFBMS:      90,
			SampledAt:   now.Add(-20 * time.Minute),
		},
	}, time.Time{}); err != nil {
		t.Fatalf("record samples: %v", err)
	}
	if err := s.RecordEdgePerformanceSamples(nil, now.Add(-2*time.Hour)); err != nil {
		t.Fatalf("prune samples: %v", err)
	}

	samples, err := s.ListEdgePerformanceSamples("demo.fugue.pro", time.Time{})
	if err != nil {
		t.Fatalf("list samples: %v", err)
	}
	if len(samples) != 1 {
		t.Fatalf("expected one recent demo sample, got %+v", samples)
	}
	if samples[0].ID != "recent-hk" ||
		samples[0].Hostname != "demo.fugue.pro" ||
		samples[0].EdgeGroupID != "edge-group-country-hk" ||
		samples[0].Method != "POST" ||
		samples[0].TrafficClass != "large_body_api" ||
		samples[0].SampleCount != 5 ||
		samples[0].CacheHitCount != 5 ||
		samples[0].UploadRequestCount != 5 ||
		samples[0].UploadEffectiveBPS != 128*1024 ||
		samples[0].MaxReadGapMS != 900 ||
		samples[0].RequestBodyReadBytes != 1024 ||
		samples[0].BodyIncompleteCount != 1 ||
		samples[0].OriginTTFBMS != 90 ||
		samples[0].ActiveBodyBuffers != 1 ||
		samples[0].ClientTCPRTTMS != 88.5 ||
		samples[0].ClientTCPTotalRetrans != 3 ||
		samples[0].ClientTCPBytesRetrans != 4096 ||
		samples[0].ClientTCPRTORate != 0.01 ||
		samples[0].ClientTCPDeliveryBPS != 2*1024*1024 {
		t.Fatalf("unexpected normalized sample: %+v", samples[0])
	}
}

func TestUpsertEdgeQualityRollupsPrunesAndListsByHostname(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	now := time.Date(2026, 6, 23, 12, 0, 0, 0, time.UTC)
	if err := s.UpsertEdgeQualityRollups([]model.EdgeQualityRollup{
		{
			Window:                "5m",
			WindowStartedAt:       now.Add(-10 * time.Minute),
			WindowEndedAt:         now.Add(-5 * time.Minute),
			Hostname:              "Old.Fugue.Pro.",
			ClientScopeKind:       "global",
			ClientScopeValue:      "global",
			EdgeGroupID:           "edge-group-country-us",
			RequestCount:          10,
			CacheObservationCount: 10,
			CacheHitRate:          0.5,
			ScoreBreakdown:        map[string]float64{"latency": 10},
		},
		{
			Window:                "5m",
			WindowStartedAt:       now.Add(-5 * time.Minute),
			WindowEndedAt:         now,
			Hostname:              "Demo.Fugue.Pro.",
			TrafficClass:          "STATIC_CACHEABLE",
			Method:                "get",
			PathPrefixBucket:      "/assets/*",
			ClientScopeKind:       "ASN",
			ClientScopeValue:      "AS4134",
			EdgeGroupID:           "edge-group-country-us",
			EdgeID:                "edge-us-1",
			RequestCount:          20,
			CacheObservationCount: 20,
			CacheHitRate:          0.9,
			Score:                 42,
			ScoreBreakdown:        map[string]float64{"cache": 24},
		},
	}, nil); err != nil {
		t.Fatalf("upsert rollups: %v", err)
	}
	if err := s.UpsertEdgeQualityRollups(nil, map[string]time.Time{"5m": now.Add(-2 * time.Minute)}); err != nil {
		t.Fatalf("prune rollups: %v", err)
	}

	rollups, err := s.ListEdgeQualityRollups("demo.fugue.pro", "5m", now.Add(-time.Minute))
	if err != nil {
		t.Fatalf("list rollups: %v", err)
	}
	if len(rollups) != 1 {
		t.Fatalf("expected one retained rollup, got %+v", rollups)
	}
	rollup := rollups[0]
	if rollup.Hostname != "demo.fugue.pro" ||
		rollup.TrafficClass != "static_cacheable" ||
		rollup.Method != "GET" ||
		rollup.ClientScopeKind != "asn" ||
		rollup.ClientScopeValue != "as4134" ||
		rollup.EdgeID != "edge-us-1" ||
		rollup.ScoreBreakdown["cache"] != 24 {
		t.Fatalf("unexpected normalized rollup: %+v", rollup)
	}
}

func TestPGRecordEdgePerformanceSampleInsertSQLColumnPlaceholderParity(t *testing.T) {
	t.Parallel()

	columns := splitSQLCSVSection(t, pgRecordEdgePerformanceSampleInsertSQL, "INSERT INTO fugue_edge_performance_samples (", ") VALUES (")
	placeholderMatches := regexp.MustCompile(`\$(\d+)`).FindAllStringSubmatch(
		sqlSection(t, pgRecordEdgePerformanceSampleInsertSQL, ") VALUES (", ")\nON CONFLICT"),
		-1,
	)
	if len(columns) != len(placeholderMatches) {
		t.Fatalf("insert column count %d does not match placeholder count %d", len(columns), len(placeholderMatches))
	}
	seen := make(map[int]bool, len(placeholderMatches))
	for _, match := range placeholderMatches {
		value, err := strconv.Atoi(match[1])
		if err != nil {
			t.Fatalf("parse placeholder %q: %v", match[0], err)
		}
		seen[value] = true
	}
	for index := 1; index <= len(columns); index++ {
		if !seen[index] {
			t.Fatalf("insert SQL missing placeholder $%d for %d columns", index, len(columns))
		}
	}
}

func TestPGUpsertEdgeQualityRollupSQLColumnPlaceholderParity(t *testing.T) {
	t.Parallel()

	columns := splitSQLCSVSection(t, pgUpsertEdgeQualityRollupSQL, "INSERT INTO fugue_edge_quality_rollups (", ") VALUES (")
	placeholderMatches := regexp.MustCompile(`\$(\d+)`).FindAllStringSubmatch(
		sqlSection(t, pgUpsertEdgeQualityRollupSQL, ") VALUES (", ")\nON CONFLICT"),
		-1,
	)
	if len(columns) != len(placeholderMatches) {
		t.Fatalf("rollup insert column count %d does not match placeholder count %d", len(columns), len(placeholderMatches))
	}
	seen := make(map[int]bool, len(placeholderMatches))
	for _, match := range placeholderMatches {
		value, err := strconv.Atoi(match[1])
		if err != nil {
			t.Fatalf("parse placeholder %q: %v", match[0], err)
		}
		seen[value] = true
	}
	for index := 1; index <= len(columns); index++ {
		if !seen[index] {
			t.Fatalf("rollup insert SQL missing placeholder $%d for %d columns", index, len(columns))
		}
	}
}

func sqlSection(t *testing.T, sql, startMarker, endMarker string) string {
	t.Helper()

	start := strings.Index(sql, startMarker)
	if start < 0 {
		t.Fatalf("SQL section start marker %q not found", startMarker)
	}
	start += len(startMarker)
	end := strings.Index(sql[start:], endMarker)
	if end < 0 {
		t.Fatalf("SQL section end marker %q not found", endMarker)
	}
	return sql[start : start+end]
}

func splitSQLCSVSection(t *testing.T, sql, startMarker, endMarker string) []string {
	t.Helper()

	section := sqlSection(t, sql, startMarker, endMarker)
	fields := strings.Split(section, ",")
	out := make([]string, 0, len(fields))
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field != "" {
			out = append(out, field)
		}
	}
	return out
}
