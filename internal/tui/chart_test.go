package tui

import (
	"strings"
	"testing"
	"time"
)

func chartSample(at time.Time, value float64) Point { return Point{At: at, Value: &value} }

func TestChartRangeGrowsWithObservedHistory(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name       string
		ages       []time.Duration
		clock      time.Duration
		start, end time.Duration
	}{
		{"startup", []time.Duration{-time.Minute, -30 * time.Second, 0}, 0, -time.Minute, 0},
		{"between polls", []time.Duration{-time.Minute, -30 * time.Second, 0}, 20 * time.Second, -time.Minute, 0},
		{"full window", []time.Duration{-20 * time.Minute, -30 * time.Second, 0}, 0, -15 * time.Minute, 0},
		{"stale tail", []time.Duration{-time.Minute, -30 * time.Second, 0}, 2 * time.Minute, -time.Minute, 2 * time.Minute},
		{"future excluded", []time.Duration{-time.Minute, 0, time.Hour}, 0, -time.Minute, 0},
		{"single sample", []time.Duration{0}, 0, 0, 0},
		{"empty", nil, 0, -15 * time.Minute, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			series := Series{Interval: 30 * time.Second}
			for _, age := range tc.ages {
				series.Points = append(series.Points, chartSample(now.Add(age), 42))
			}
			start, end := chartRange(series, now.Add(tc.clock), 15*time.Minute)
			if !start.Equal(now.Add(tc.start)) || !end.Equal(now.Add(tc.end)) {
				t.Fatalf("range %s..%s, want %s..%s", start, end, now.Add(tc.start), now.Add(tc.end))
			}
		})
	}
}

func TestChartJoinsNormalSamplesAtAnyWidth(t *testing.T) {
	now := time.Now()
	series := Series{Interval: 30 * time.Second, Points: []Point{
		chartSample(now.Add(-time.Minute), 10), chartSample(now.Add(-30*time.Second), 90), chartSample(now, 30),
	}}
	for _, width := range []int{1, 2, 20, 48, 200} {
		buckets := plotBuckets(series, now.Add(-time.Minute), now, width)
		for i, bucket := range buckets {
			if bucket.Last == nil {
				t.Fatalf("normal sampling left column %d/%d empty", i, width)
			}
			if *bucket.Low < 10 || *bucket.High > 90 {
				t.Fatal("interpolation overshot observed extrema")
			}
		}
		if *buckets[width-1].Last != 30 || !buckets[width-1].LastAt.Equal(now) || buckets[width-1].Interpolated {
			t.Fatal("latest observation was replaced by an estimate")
		}
		for _, mode := range []string{"braille", "block", "line", "ascii"} {
			plot := drawPlot(buckets, width, 6, 90, mode, nil)
			for x := 0; x < width; x++ {
				filled := false
				for _, line := range plot {
					filled = filled || []rune(line)[x] != ' '
				}
				if !filled {
					t.Fatalf("%s plot left column %d/%d blank", mode, x, width)
				}
			}
		}
	}
	if len(series.Points) != 3 || *series.Points[1].Value != 90 {
		t.Fatal("display interpolation changed stored samples")
	}
}

func TestChartPreservesMissingData(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name     string
		interval time.Duration
		points   []Point
	}{
		{"missed interval", 30 * time.Second, []Point{chartSample(now.Add(-time.Minute), 10), chartSample(now, 90)}},
		{"unknown cadence", 0, []Point{chartSample(now.Add(-time.Minute), 10), chartSample(now, 90)}},
		{"explicit null", time.Minute, []Point{chartSample(now.Add(-time.Minute), 10), {At: now.Add(-30 * time.Second)}, chartSample(now, 90)}},
		{"future sample", time.Minute, []Point{chartSample(now.Add(-time.Minute), 10), chartSample(now.Add(time.Minute), 90)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buckets := plotBuckets(Series{Interval: tc.interval, Points: tc.points}, now.Add(-time.Minute), now, 40)
			if buckets[20].Last != nil {
				t.Fatal("missing data was connected")
			}
		})
	}
	series := Series{Interval: 30 * time.Second, Points: []Point{chartSample(now.Add(-time.Minute), 10), chartSample(now.Add(-30*time.Second), 90)}}
	buckets := plotBuckets(series, now.Add(-2*time.Minute), now, 80)
	if buckets[10].Last != nil || buckets[75].Last != nil {
		t.Fatal("chart extrapolated outside observations")
	}
}

func TestChartInterpolationKeepsSpikesAndWindowBoundary(t *testing.T) {
	now := time.Now()
	series := Series{Interval: time.Second, Points: []Point{
		chartSample(now.Add(-time.Second), 0), chartSample(now, 100), chartSample(now.Add(time.Second), 0),
	}}
	b := plotBuckets(series, now.Add(-time.Second), now.Add(time.Second), 1)[0]
	if *b.Low != 0 || *b.High != 100 || *b.First != 0 || *b.Last != 0 {
		t.Fatal("downsampling hid a spike")
	}
	b = plotBuckets(series, now.Add(-500*time.Millisecond), now.Add(time.Second), 30)[0]
	if !b.Interpolated || *b.Low != 50 {
		t.Fatal("visible segment from an observation before the window was lost")
	}
}

func TestChartShowsActualSpanAndHonestCursor(t *testing.T) {
	m := testModel()
	m.prefs.Graph = "braille"
	series := Series{Label: "CPU", Unit: "%", State: "available", Interval: 30 * time.Second, Points: []Point{
		chartSample(m.now.Add(-30*time.Second), 10), chartSample(m.now, 90),
	}}
	m.graphIndex, m.graphCursor = 0, 20
	view := m.renderChart(series, 60, 10, 0)
	if !strings.Contains(view, "30s / 15m") || !strings.Contains(view, "(interpolated)") {
		t.Fatalf("startup axis/cursor missing: %s", view)
	}
	m.graphCursor = 57
	view = m.renderChart(series, 60, 10, 0)
	if !strings.Contains(view, m.now.Local().Format("15:04:05")+" · 90.0%") || strings.Contains(view, "(interpolated)") {
		t.Fatalf("latest cursor lost the real observation: %s", view)
	}
	m.graphCursor = -1
	series.Points = series.Points[1:]
	view = m.renderChart(series, 60, 10, 0)
	if !strings.Contains(view, "collecting · 1 sample") || strings.ContainsAny(view, "⣿⣀⣤") {
		t.Fatalf("single observation fabricated a curve: %s", view)
	}
}
