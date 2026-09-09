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
	At, LastAt             time.Time
	Interpolated           bool
}

func staleAfter(series Series) time.Duration {
	return max(30*time.Second, 2*series.Interval)
}

// Grow into the requested window as observations accumulate. Fresh charts end
// at the latest observation, so polling between samples does not shrink or
// shift them. Once stale, include wall time to expose the missing tail.
func chartRange(series Series, now time.Time, window time.Duration) (start, end time.Time) {
	end = now
	var first, last time.Time
	for _, point := range series.Points {
		if point.At.IsZero() || point.At.After(now) {
			continue
		}
		if first.IsZero() || point.At.Before(first) {
			first = point.At
		}
		if point.At.After(last) {
			last = point.At
		}
	}
	if !last.IsZero() && now.Sub(last) <= staleAfter(series) {
		end = last
	}
	start = end.Add(-window)
	if first.After(start) {
		start = first
	}
	return start, end
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
		b.LastAt = p.At
		if v < *b.Low {
			b.Low = &v
		}
		if v > *b.High {
			b.High = &v
		}
	}
	return out
}

// Connect only adjacent observations within the declared sampling cadence
// (with 50% jitter tolerance). Interpolation is display-only: null observations,
// missed intervals, unknown cadence and time outside observations stay empty.
// The raw extrema remain intact even when many samples share a raster column.
func plotBuckets(series Series, start, end time.Time, width int) []Bucket {
	out := Buckets(series, start, end, width)
	if len(out) == 0 || series.Interval <= 0 {
		return out
	}
	span := end.Sub(start)
	column := func(at time.Time) int {
		return min(width-1, max(0, int(float64(at.Sub(start))/float64(span)*float64(width))))
	}
	for i := 1; i < len(series.Points); i++ {
		a, b := series.Points[i-1], series.Points[i]
		delta := b.At.Sub(a.At)
		if a.Value == nil || b.Value == nil || delta <= 0 || delta > series.Interval+series.Interval/2 || b.At.Before(start) || b.At.After(end) {
			continue
		}
		valueAt := func(at time.Time) float64 {
			return *a.Value + (*b.Value-*a.Value)*float64(at.Sub(a.At))/float64(delta)
		}
		for x := column(a.At); x <= column(b.At); x++ {
			left := out[x].At
			right := end
			if x+1 < len(out) {
				right = out[x+1].At
			}
			if left.Before(a.At) {
				left = a.At
			}
			if right.After(b.At) {
				right = b.At
			}
			lo, hi := valueAt(left), valueAt(right)
			bucket := &out[x]
			if bucket.First == nil {
				bucket.First, bucket.Last = &lo, &hi
				bucket.LastAt, bucket.Interpolated = right, true
			}
			low, high := math.Min(lo, hi), math.Max(lo, hi)
			if bucket.Low == nil || low < *bucket.Low {
				bucket.Low = &low
			}
			if bucket.High == nil || high > *bucket.High {
				bucket.High = &high
			}
		}
	}
	return out
}
