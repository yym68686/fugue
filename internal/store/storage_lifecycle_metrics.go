package store

import (
	"io"
	"time"

	"fugue/internal/observability"
)

// No queries on scrape; fixed relation/class labels only. Each API replica
// reports its own observations. Aggregate allocations with max, NOT sum.
func (s *Store) WriteStorageLifecycleMetrics(w io.Writer) {
	if s == nil {
		return
	}
	o := s.StorageLifecycleObservation()
	flag := func(b bool) float64 {
		if b {
			return 1
		}
		return 0
	}
	gauge := func(name, help string, value float64) {
		observability.WriteGaugeMetric(w, name, help, nil, value)
	}
	gauge("fugue_storage_lifecycle_audit_enabled", "Whether a valid, effective audit policy is active; never authorizes deletion.", flag(o.Policy.Mode == "audit" && !time.Now().Before(o.Policy.EffectiveFrom)))
	gauge("fugue_storage_lifecycle_policy_read_failed", "Whether lifecycle policy parsing or reading failed.", flag(o.PolicyReadFailed))
	gauge("fugue_storage_lifecycle_last_run_failed", "Whether the last bounded lifecycle read failed.", flag(o.LastRunFailed))
	gauge("fugue_storage_lifecycle_last_attempt_timestamp_seconds", "Last audit attempt; zero before first attempt.", lifecycleUnix(o.LastAttemptAt))
	gauge("fugue_storage_lifecycle_interval_seconds", "Configured minimum seconds between bounded audit samples.", float64(o.Policy.IntervalSeconds))
	gauge("fugue_storage_lifecycle_sample_current_policy", "Whether the last sample belongs to the currently configured policy.", flag(o.Sample != nil && o.Sample.PolicySHA256 == o.PolicySHA256 && o.PolicySHA256 != ""))
	gauge("fugue_storage_lifecycle_last_duration_seconds", "Wall time of the last bounded audit read.", o.DurationSeconds)
	observability.WriteCounterMetric(w, "fugue_storage_lifecycle_read_failures_total", "Failed bounded lifecycle reads in this process.", nil, float64(o.Failures))
	lastSuccess := float64(0)
	if o.Sample != nil {
		lastSuccess = lifecycleUnix(o.Sample.ObservedAt)
	}
	gauge("fugue_storage_lifecycle_last_success_timestamp_seconds", "Last complete bounded sample; NOT a complete global reference inventory.", lastSuccess)
	observability.WriteMetricHeader(w, "fugue_storage_lifecycle_free_percent_threshold", "Proposed free-space alert thresholds; audit does not throttle or delete.", "gauge")
	for _, item := range []struct {
		level string
		value int
	}{{"warning", o.Policy.WarningFreePercent}, {"pressure", o.Policy.PressureFreePercent}, {"critical", o.Policy.CriticalFreePercent}} {
		observability.WriteMetricSample(w, "fugue_storage_lifecycle_free_percent_threshold", map[string]string{"level": item.level}, float64(item.value))
	}
	if o.Sample != nil {
		gauge("fugue_storage_database_allocated_bytes", "Observed whole database allocation; max across replicas, not sum.", float64(o.Sample.DatabaseBytes))
		observability.WriteMetricHeader(w, "fugue_storage_relation_present", "Whether a fixed audit relation exists; missing is not zero allocation.", "gauge")
		observability.WriteMetricHeader(w, "fugue_storage_relation_allocated_bytes", "Allocation by fixed relation and size kind; table includes TOAST, total includes indexes; do not sum kinds or replicas.", "gauge")
		for _, r := range o.Sample.Relations {
			observability.WriteMetricSample(w, "fugue_storage_relation_present", map[string]string{"relation": r.Relation}, flag(r.Present))
			if !r.Present {
				continue
			}
			for _, size := range []struct {
				kind  string
				bytes int64
			}{{"heap", r.HeapBytes}, {"table", r.TableBytes}, {"index", r.IndexBytes}, {"total", r.TotalBytes}} {
				observability.WriteMetricSample(w, "fugue_storage_relation_allocated_bytes", map[string]string{"relation": r.Relation, "kind": size.kind}, float64(size.bytes))
			}
		}
		page := o.Sample.SourcePage
		observability.WriteMetricHeader(w, "fugue_storage_source_page_rows", "Classification of ONE bounded page; not global totals or deletion authority.", "gauge")
		observability.WriteMetricHeader(w, "fugue_storage_source_page_declared_bytes", "Declared archive bytes in ONE page, not physically reclaimable bytes.", "gauge")
		for _, c := range []struct {
			class string
			rows  int
			bytes int64
		}{{"observed_referenced", page.ReferencedRows, page.ReferencedBytes}, {"unknown", page.UnknownRows, page.UnknownBytes}} {
			observability.WriteMetricSample(w, "fugue_storage_source_page_rows", map[string]string{"class": c.class}, float64(c.rows))
			observability.WriteMetricSample(w, "fugue_storage_source_page_declared_bytes", map[string]string{"class": c.class}, float64(c.bytes))
		}
	}
	// Observe the EXISTING closed observation cleaner; do not broaden its scope.
	s.retention.mu.Lock()
	at, failed, enabled := s.retention.observedAt, s.retention.lastError, s.retention.policy.Enabled
	s.retention.mu.Unlock()
	gauge("fugue_observation_retention_enabled", "Whether the existing observation retention policy is enabled.", flag(enabled))
	gauge("fugue_observation_retention_last_attempt_timestamp_seconds", "Last existing observation cleanup attempt, not necessarily successful.", lifecycleUnix(at))
	gauge("fugue_observation_retention_last_run_failed", "Whether existing observation cleanup last failed.", flag(failed))
}

func lifecycleUnix(at time.Time) float64 {
	if at.IsZero() {
		return 0
	}
	return float64(at.Unix())
}
