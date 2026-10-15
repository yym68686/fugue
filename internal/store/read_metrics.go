package store

import (
	"io"
	"sync/atomic"
	"time"

	"fugue/internal/observability"
)

type storeReadCounters struct{ calls, rows, errors, nanoseconds, bodyBytes, bodyElisions atomic.Uint64 }

const (
	artifactReadMetric = iota
	journaldReadMetric
)

func (s *Store) recordRead(family int, started time.Time, rows int, err error) {
	m := &s.readMetrics[family]
	m.calls.Add(1)
	m.rows.Add(uint64(rows))
	m.nanoseconds.Add(uint64(time.Since(started)))
	if err != nil {
		m.errors.Add(1)
	}
}

// Fixed family labels only: no SQL, IDs, content, credentials or tenant data.
func (s *Store) WriteReadMetrics(w io.Writer) {
	if s == nil {
		return
	}
	for _, metric := range []struct {
		name, help string
		value      func(*storeReadCounters) float64
	}{
		{"fugue_store_reads_total", "Store reads by bounded query family.", func(m *storeReadCounters) float64 { return float64(m.calls.Load()) }},
		{"fugue_store_read_rows_total", "Rows returned by bounded query family.", func(m *storeReadCounters) float64 { return float64(m.rows.Load()) }},
		{"fugue_store_read_errors_total", "Failed reads by bounded query family.", func(m *storeReadCounters) float64 { return float64(m.errors.Load()) }},
		{"fugue_store_read_duration_seconds_total", "Cumulative store read wall time.", func(m *storeReadCounters) float64 { return float64(m.nanoseconds.Load()) / float64(time.Second) }},
	} {
		observability.WriteMetricHeader(w, metric.name, metric.help, "counter")
		for family, name := range []string{"platform_artifact", "host_journald_state"} {
			observability.WriteMetricSample(w, metric.name, map[string]string{"family": name}, metric.value(&s.readMetrics[family]))
		}
	}

	observability.WriteMetricHeader(w, "fugue_store_artifact_body_bytes_total", "Actual content body bytes received from PostgreSQL; excludes metadata and framing.", "counter")
	observability.WriteMetricSample(w, "fugue_store_artifact_body_bytes_total", nil, float64(s.readMetrics[artifactReadMetric].bodyBytes.Load()))
	observability.WriteMetricHeader(w, "fugue_store_artifact_body_elisions_total", "Content bodies omitted after a fresh database digest match.", "counter")
	observability.WriteMetricSample(w, "fugue_store_artifact_body_elisions_total", nil, float64(s.readMetrics[artifactReadMetric].bodyElisions.Load()))
}
