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
			ID:                    "recent-hk",
			EdgeID:                "edge-hk-1",
			EdgeGroupID:           "edge-group-country-hk",
			Hostname:              "demo.fugue.pro",
			Method:                "post",
			TrafficClass:          "large_body_api",
			ClientRegion:          "apac",
			RuntimeRegion:         "us",
			TTFBMS:                120,
			UpstreamMS:            80,
			TotalMS:               150,
			StatusCode:            200,
			SampleCount:           5,
			CacheHitCount:         5,
			CacheObservationCount: 5,
			UploadRequestCount:    5,
			BodyReadBlockMS:       250,
			UploadEffectiveBPS:    128 * 1024,
			MaxReadGapMS:          900,
			RequestBodyBytes:      2048,
			RequestBodyReadBytes:  1024,
			BodyIncompleteCount:   1,
			ResponseEgressBPS:     512 * 1024,
			OriginTTFBMS:          90,
			ActiveRequests:        3,
			ActiveBodyBuffers:     1,
			SampledAt:             now.Add(-30 * time.Minute),
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
		samples[0].ActiveBodyBuffers != 1 {
		t.Fatalf("unexpected normalized sample: %+v", samples[0])
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
