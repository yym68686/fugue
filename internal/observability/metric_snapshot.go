package observability

import (
	"bytes"
	"context"
	"io"
	"sync"
	"time"
)

// MetricSnapshot never performs collection on a scrape goroutine. Failed
// refreshes preserve the last successful bytes and their original timestamp.
type MetricSnapshot struct {
	mu        sync.RWMutex
	collectMu sync.Mutex
	body      []byte
	observed  time.Time
	attempted time.Time
	failed    bool
}

func (s *MetricSnapshot) Refresh(ctx context.Context, collect func(context.Context, io.Writer) error) error {
	s.collectMu.Lock()
	defer s.collectMu.Unlock()
	started := time.Now().UTC()
	var b bytes.Buffer
	err := collect(ctx, &b)
	if err == nil {
		err = ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempted = started
	s.failed = err != nil
	if err == nil {
		s.body = bytes.Clone(b.Bytes())
		s.observed = started
	}
	return err
}

func (s *MetricSnapshot) Run(ctx context.Context, interval time.Duration, collect func(context.Context, io.Writer) error) {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		refreshCtx, cancel := context.WithTimeout(ctx, 2*interval)
		_ = s.Refresh(refreshCtx, collect)
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *MetricSnapshot) Write(w io.Writer, component string) {
	s.mu.RLock()
	body, observed, attempted, failed := s.body, s.observed, s.attempted, s.failed
	s.mu.RUnlock()
	_, _ = w.Write(body)
	labels := map[string]string{"component": component}
	WriteGaugeMetric(w, "fugue_metrics_snapshot_ready", "Whether a complete metrics snapshot has been collected.", labels, metricBool(!observed.IsZero()))
	WriteGaugeMetric(w, "fugue_metrics_snapshot_refresh_failed", "Whether the last background metrics refresh failed.", labels, metricBool(failed))
	if !observed.IsZero() {
		WriteGaugeMetric(w, "fugue_metrics_snapshot_observed_timestamp_seconds", "Original observation time of the last complete metrics snapshot.", labels, float64(observed.Unix()))
	}
	if !attempted.IsZero() {
		WriteGaugeMetric(w, "fugue_metrics_snapshot_attempt_timestamp_seconds", "Start time of the last background collection attempt.", labels, float64(attempted.Unix()))
	}
}
func metricBool(v bool) float64 {
	if v {
		return 1
	}
	return 0
}
