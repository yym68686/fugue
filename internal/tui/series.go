package tui

import (
	"math"
	"sort"
	"time"
)

const maxSeriesPoints = 1440

// MergeSeries uses observed timestamps, not UI ticks, so cached observations
// never create artificial new samples. A source reset starts a new series.
func MergeSeries(old Series, next Series, now time.Time) Series {
	points := next.Points
	if old.Source == next.Source && old.Unit == next.Unit {
		points = append(append([]Point(nil), old.Points...), points...)
	}
	sort.SliceStable(points, func(i, j int) bool { return points[i].At.Before(points[j].At) })
	out := make([]Point, 0, min(len(points), maxSeriesPoints))
	cutoff := now.Add(-time.Hour)
	for _, p := range points {
		if p.At.IsZero() || p.At.Before(cutoff) || p.At.After(now.Add(time.Minute)) {
			continue
		}
		if p.Value != nil && (math.IsNaN(*p.Value) || math.IsInf(*p.Value, 0)) {
			p.Value = nil
		}
		if len(out) > 0 && out[len(out)-1].At.Equal(p.At) {
			out[len(out)-1] = p
		} else {
			out = append(out, p)
		}
	}
	if len(out) > maxSeriesPoints {
		out = out[len(out)-maxSeriesPoints:]
	}
	next.Points = out
	return next
}

type Bucket struct {
	First, Last, Low, High *float64
	At                     time.Time
}

// Buckets preserve extrema so narrow charts do not hide short load spikes.
// Empty time buckets stay empty, including gaps caused by disconnection.
func Buckets(series Series, start, end time.Time, width int) []Bucket {
	if width < 1 || !end.After(start) {
		return nil
	}
	out := make([]Bucket, width)
	step := end.Sub(start) / time.Duration(width)
	for i := range out {
		out[i].At = start.Add(time.Duration(i) * step)
	}
	for _, p := range series.Points {
		if p.At.Before(start) || p.At.After(end) || p.Value == nil {
			continue
		}
		i := min(width-1, int(float64(p.At.Sub(start))/float64(end.Sub(start))*float64(width)))
		b := &out[i]
		v := *p.Value
		if b.First == nil {
			b.First = &v
			b.Low = &v
			b.High = &v
		}
		b.Last = &v
		if v < *b.Low {
			b.Low = &v
		}
		if v > *b.High {
			b.High = &v
		}
	}
	return out
}
