package storageaudit

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
)

// WriteLifecycleMetrics maintains the existing alert and dashboard series
// across executor handoff. Scrapes never query the database.
func WriteLifecycleMetrics(w io.Writer, o Observation, now time.Time) {
	header := func(name, help, typ string) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
	}
	sample := func(name string, labels map[string]string, value float64) {
		labelSet := ""
		if len(labels) > 0 {
			parts := make([]string, 0, len(labels))
			for k, v := range labels {
				parts = append(parts, fmt.Sprintf("%s=%q", k, v))
			}
			sort.Strings(parts)
			labelSet = "{" + strings.Join(parts, ",") + "}"
		}
		fmt.Fprintf(w, "%s%s %.6f\n", name, labelSet, value)
	}
	flag := func(b bool) float64 {
		if b {
			return 1
		}
		return 0
	}
	gauge := func(name, help string, value float64) {
		header(name, help, "gauge")
		sample(name, nil, value)
	}
	gauge("fugue_storage_lifecycle_audit_enabled", "Whether a valid, effective audit policy is active; never authorizes deletion.", flag(o.Policy.Mode == "audit" && !now.Before(o.Policy.EffectiveFrom)))
	gauge("fugue_storage_lifecycle_policy_read_failed", "Whether lifecycle policy parsing or reading failed.", flag(o.PolicyReadFailed))
	gauge("fugue_storage_lifecycle_last_run_failed", "Whether the last bounded lifecycle read failed.", flag(o.LastRunFailed))
	gauge("fugue_storage_lifecycle_last_attempt_timestamp_seconds", "Last audit attempt; zero before first attempt.", lifecycleUnix(o.LastAttemptAt))
	gauge("fugue_storage_lifecycle_interval_seconds", "Configured minimum seconds between bounded audit samples.", float64(o.Policy.IntervalSeconds))
	gauge("fugue_storage_lifecycle_sample_current_policy", "Whether the last sample belongs to the currently configured policy.", flag(o.Sample != nil && o.Sample.PolicySHA256 == o.PolicySHA256 && o.PolicySHA256 != ""))
	gauge("fugue_storage_lifecycle_last_duration_seconds", "Wall time of the last bounded audit read.", o.DurationSeconds)
	header("fugue_storage_lifecycle_read_failures_total", "Failed bounded lifecycle reads recorded by the executor.", "counter")
	fmt.Fprintf(w, "fugue_storage_lifecycle_read_failures_total %d\n", o.Failures)
	lastSuccess := float64(0)
	if o.Sample != nil {
		lastSuccess = lifecycleUnix(o.Sample.ObservedAt)
	}
	gauge("fugue_storage_lifecycle_last_success_timestamp_seconds", "Last complete bounded sample; NOT a complete global reference inventory.", lastSuccess)
	header("fugue_storage_lifecycle_free_percent_threshold", "Proposed free-space alert thresholds; audit does not throttle or delete.", "gauge")
	for _, item := range []struct {
		level string
		value int
	}{{"warning", o.Policy.WarningFreePercent}, {"pressure", o.Policy.PressureFreePercent}, {"critical", o.Policy.CriticalFreePercent}} {
		sample("fugue_storage_lifecycle_free_percent_threshold", map[string]string{"level": item.level}, float64(item.value))
	}
	if o.Sample != nil {
		gauge("fugue_storage_database_allocated_bytes", "Observed whole database allocation; max across replicas, not sum.", float64(o.Sample.DatabaseBytes))
		header("fugue_storage_relation_present", "Whether a fixed audit relation exists; missing is not zero allocation.", "gauge")
		header("fugue_storage_relation_allocated_bytes", "Allocation by fixed relation and size kind; table includes TOAST, total includes indexes; do not sum kinds or replicas.", "gauge")
		for _, r := range o.Sample.Relations {
			if !isLifecycleRelation(r.Relation) {
				continue
			}
			sample("fugue_storage_relation_present", map[string]string{"relation": r.Relation}, flag(r.Present))
			if !r.Present {
				continue
			}
			for _, size := range []struct {
				kind  string
				bytes int64
			}{{"heap", r.HeapBytes}, {"table", r.TableBytes}, {"index", r.IndexBytes}, {"total", r.TotalBytes}} {
				sample("fugue_storage_relation_allocated_bytes", map[string]string{"relation": r.Relation, "kind": size.kind}, float64(size.bytes))
			}
		}
		page := o.Sample.SourcePage
		header("fugue_storage_source_page_rows", "Classification of ONE bounded page; not global totals or deletion authority.", "gauge")
		header("fugue_storage_source_page_declared_bytes", "Declared archive bytes in ONE page, not physically reclaimable bytes.", "gauge")
		for _, c := range []struct {
			class string
			rows  int
			bytes int64
		}{{"observed_referenced", page.ReferencedRows, page.ReferencedBytes}, {"unknown", page.UnknownRows, page.UnknownBytes}} {
			sample("fugue_storage_source_page_rows", map[string]string{"class": c.class}, float64(c.rows))
			sample("fugue_storage_source_page_declared_bytes", map[string]string{"class": c.class}, float64(c.bytes))
		}
	}
}

func lifecycleUnix(at time.Time) float64 {
	if at.IsZero() {
		return 0
	}
	return float64(at.Unix())
}
