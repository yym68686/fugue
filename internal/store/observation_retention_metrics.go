package store

import (
	"io"
	"time"

	"fugue/internal/observability"
)

// WriteObservationRetentionMetrics reports the API's remaining retention work.
func (s *Store) WriteObservationRetentionMetrics(w io.Writer) {
	if s == nil {
		return
	}
	flag := func(b bool) float64 {
		if b {
			return 1
		}
		return 0
	}
	gauge := func(name, help string, value float64) {
		observability.WriteGaugeMetric(w, name, help, nil, value)
	}
	// Observe the EXISTING closed observation cleaner; do not broaden its scope.
	s.retention.mu.Lock()
	at, failed, enabled := s.retention.observedAt, s.retention.lastError, s.retention.policy.Enabled
	s.retention.mu.Unlock()
	gauge("fugue_observation_retention_enabled", "Whether the existing observation retention policy is enabled.", flag(enabled))
	gauge("fugue_observation_retention_last_attempt_timestamp_seconds", "Last existing observation cleanup attempt, not necessarily successful.", retentionUnix(at))
	gauge("fugue_observation_retention_last_run_failed", "Whether existing observation cleanup last failed.", flag(failed))
}

func retentionUnix(at time.Time) float64 {
	if at.IsZero() {
		return 0
	}
	return float64(at.Unix())
}
