package imagetracking

import (
	"context"
	"errors"
	"fugue/internal/model"
	"fugue/internal/observability"
	"io"
	"strings"
	"sync"
	"time"
)

// Metrics owns the process-local tracking counters and sync lifecycle.
// The zero value is ready to use; callers receive only detached snapshots.
type Metrics struct {
	mu                                     sync.RWMutex
	decisions                              map[string]int64
	lastCheckAt, lastQueuedAt, lastErrorAt time.Time
	lastError                              string
	running                                bool
	startedAt, finishedAt, syncErrorAt     time.Time
	duration                               time.Duration
	syncError                              string
}

func (s *Metrics) observeImageTrackingDecision(check model.AppImageTrackingCheck) {
	if s == nil {
		return
	}
	decision := strings.TrimSpace(check.Decision)
	if decision == "" {
		decision = "unknown"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.decisions == nil {
		s.decisions = map[string]int64{}
	}
	s.decisions[decision]++
	if !check.CheckedAt.IsZero() && (s.lastCheckAt.IsZero() || check.CheckedAt.After(s.lastCheckAt)) {
		s.lastCheckAt = check.CheckedAt
	}
	if decision == model.AppImageTrackingDecisionQueued && !check.CheckedAt.IsZero() {
		s.lastQueuedAt = check.CheckedAt
	}
	if strings.TrimSpace(check.ResolverError) != "" || decision == model.AppImageTrackingDecisionQueueError || decision == model.AppImageTrackingDecisionResolverError {
		if !check.CheckedAt.IsZero() {
			s.lastErrorAt = check.CheckedAt
		}
		s.lastError = firstNonEmptyImageTrackingMetric(check.ResolverError, check.SkipReason, decision)
	}
}

func (s *Metrics) markImageTrackingSyncStarted() {
	if s == nil {
		return
	}
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.running = true
	s.startedAt = now
}

func (s *Metrics) markImageTrackingSyncFinished(startedAt time.Time, syncErr error) {
	if s == nil {
		return
	}
	finishedAt := time.Now().UTC()
	duration := finishedAt.Sub(startedAt)
	if duration < 0 {
		duration = 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.running = false
	s.finishedAt = finishedAt
	s.duration = duration
	if syncErr != nil && !errors.Is(syncErr, context.Canceled) {
		s.syncErrorAt = finishedAt
		s.syncError = syncErr.Error()
	}
}

func (s *Metrics) imageTrackingMetricsSnapshot() (map[string]int64, time.Time, time.Time, time.Time, string) {
	if s == nil {
		return map[string]int64{}, time.Time{}, time.Time{}, time.Time{}, ""
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	counts := make(map[string]int64, len(s.decisions))
	for key, value := range s.decisions {
		counts[key] = value
	}
	return counts, s.lastCheckAt, s.lastQueuedAt, s.lastErrorAt, strings.TrimSpace(s.lastError)
}

func (s *Metrics) SyncSnapshot() (bool, time.Time, time.Time, time.Duration, time.Time, string) {
	if s == nil {
		return false, time.Time{}, time.Time{}, 0, time.Time{}, ""
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.running,
		s.startedAt,
		s.finishedAt,
		s.duration,
		s.syncErrorAt,
		strings.TrimSpace(s.syncError)
}

func (s *Metrics) Write(w io.Writer) {
	counts, lastCheckAt, lastQueuedAt, lastErrorAt, _ := s.imageTrackingMetricsSnapshot()
	syncRunning, syncStartedAt, syncFinishedAt, syncDuration, syncErrorAt, _ := s.SyncSnapshot()
	observability.WriteMetricHeader(w, "fugue_image_tracking_decisions_total", "Image tracking check decisions made by this controller process.", "counter")
	for _, decision := range knownImageTrackingMetricDecisions(counts) {
		observability.WriteMetricSample(w, "fugue_image_tracking_decisions_total", map[string]string{"decision": decision}, float64(counts[decision]))
	}
	lastCheck := float64(0)
	if !lastCheckAt.IsZero() {
		lastCheck = float64(lastCheckAt.Unix())
	}
	lastQueued := float64(0)
	if !lastQueuedAt.IsZero() {
		lastQueued = float64(lastQueuedAt.Unix())
	}
	lastErrorTs := float64(0)
	if !lastErrorAt.IsZero() {
		lastErrorTs = float64(lastErrorAt.Unix())
	}
	observability.WriteGaugeMetric(w, "fugue_image_tracking_last_check_timestamp_seconds", "Unix timestamp for the latest image tracking check handled by this controller process.", nil, lastCheck)
	observability.WriteGaugeMetric(w, "fugue_image_tracking_last_queued_timestamp_seconds", "Unix timestamp for the latest image tracking queued import handled by this controller process.", nil, lastQueued)
	observability.WriteGaugeMetric(w, "fugue_image_tracking_last_error_timestamp_seconds", "Unix timestamp for the latest image tracking resolver or queue error handled by this controller process.", nil, lastErrorTs)
	syncStarted := float64(0)
	if !syncStartedAt.IsZero() {
		syncStarted = float64(syncStartedAt.Unix())
	}
	syncFinished := float64(0)
	if !syncFinishedAt.IsZero() {
		syncFinished = float64(syncFinishedAt.Unix())
	}
	syncError := float64(0)
	if !syncErrorAt.IsZero() {
		syncError = float64(syncErrorAt.Unix())
	}
	observability.WriteGaugeMetric(w, "fugue_controller_image_tracking_sync_running", "Whether this controller process is currently running an image tracking sync.", nil, boolGauge(syncRunning))
	observability.WriteGaugeMetric(w, "fugue_controller_image_tracking_sync_started_timestamp_seconds", "Unix timestamp for the latest image tracking sync start.", nil, syncStarted)
	observability.WriteGaugeMetric(w, "fugue_controller_last_image_tracking_sync_timestamp_seconds", "Unix timestamp for the latest completed image tracking sync.", nil, syncFinished)
	observability.WriteGaugeMetric(w, "fugue_controller_image_tracking_sync_duration_seconds", "Duration of the latest completed image tracking sync.", nil, syncDuration.Seconds())
	observability.WriteGaugeMetric(w, "fugue_controller_image_tracking_sync_last_error_timestamp_seconds", "Unix timestamp for the latest image tracking sync-level error.", nil, syncError)
}

func knownImageTrackingMetricDecisions(counts map[string]int64) []string {
	decisions := []string{
		model.AppImageTrackingDecisionQueued,
		model.AppImageTrackingDecisionAlreadyDeployed,
		model.AppImageTrackingDecisionNoChange,
		model.AppImageTrackingDecisionReplicasZero,
		model.AppImageTrackingDecisionActiveOperation,
		model.AppImageTrackingDecisionRetrySuppressed,
		model.AppImageTrackingDecisionResolverError,
		model.AppImageTrackingDecisionQueueConflict,
		model.AppImageTrackingDecisionQueueError,
	}
	seen := map[string]struct{}{}
	for _, decision := range decisions {
		seen[decision] = struct{}{}
	}
	for decision := range counts {
		if _, ok := seen[decision]; !ok {
			decisions = append(decisions, decision)
		}
	}
	return decisions
}

func firstNonEmptyImageTrackingMetric(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func boolGauge(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
