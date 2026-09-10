package store

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestWalkEdgePerformanceSamplesPreservesRowsAndStopsOnError(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "state.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	samples := []model.EdgePerformanceSample{
		{ID: "b", Hostname: "one.example.test", EdgeID: "a", EdgeGroupID: "a", SampledAt: now, SampleCount: 3, TTFBMS: 42, ClientTCPRTTMS: 7.5},
		{ID: "a", Hostname: "one.example.test", EdgeID: "b", EdgeGroupID: "a", SampledAt: now, SampleCount: 2},
		{ID: "old", Hostname: "one.example.test", EdgeGroupID: "a", SampledAt: now.Add(-time.Hour)},
		{ID: "future", Hostname: "two.example.test", EdgeGroupID: "a", SampledAt: now.Add(time.Hour)},
	}
	if err := s.RecordEdgePerformanceSamples(samples, time.Time{}); err != nil {
		t.Fatal(err)
	}
	for _, host := range []string{"", "One.Example.Test."} {
		want, err := s.ListEdgePerformanceSamples(host, now)
		if err != nil {
			t.Fatal(err)
		}
		var got []model.EdgePerformanceSample
		err = s.WalkEdgePerformanceSamples(context.Background(), host, now, func(sample model.EdgePerformanceSample) error { got = append(got, sample); return nil })
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("cursor changed ordered values: got=%+v want=%+v err=%v", got, want, err)
		}
	}
	sentinel := errors.New("consumer stopped")
	calls := 0
	err := s.WalkEdgePerformanceSamples(context.Background(), "", now, func(model.EdgePerformanceSample) error { calls++; return sentinel })
	if !errors.Is(err, sentinel) || calls != 1 {
		t.Fatalf("callback failure ignored: calls=%d err=%v", calls, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = s.WalkEdgePerformanceSamples(ctx, "", now, func(model.EdgePerformanceSample) error { t.Fatal("callback after cancellation"); return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
}
