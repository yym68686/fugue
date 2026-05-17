package store

import (
	"path/filepath"
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
			ClientRegion:          "apac",
			RuntimeRegion:         "us",
			TTFBMS:                120,
			UpstreamMS:            80,
			TotalMS:               150,
			StatusCode:            200,
			SampleCount:           5,
			CacheHitCount:         5,
			CacheObservationCount: 5,
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
		samples[0].SampleCount != 5 ||
		samples[0].CacheHitCount != 5 {
		t.Fatalf("unexpected normalized sample: %+v", samples[0])
	}
}
