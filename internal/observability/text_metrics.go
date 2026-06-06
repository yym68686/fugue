package observability

import (
	"fmt"
	"io"
	"runtime"
	"sort"
	"strings"
	"time"
)

func WriteComponentRuntimeMetrics(w io.Writer, component string, startedAt time.Time) {
	component = strings.TrimSpace(component)
	if component == "" {
		component = "unknown"
	}
	now := time.Now().UTC()
	if startedAt.IsZero() {
		startedAt = now
	}
	labels := map[string]string{"component": component}
	WriteGaugeMetric(w, "fugue_component_info", "Static Fugue component identity labels.", labels, 1)
	WriteGaugeMetric(w, "fugue_process_uptime_seconds", "Seconds since this Fugue process started.", labels, now.Sub(startedAt).Seconds())
	WriteGaugeMetric(w, "fugue_go_goroutines", "Current number of goroutines in this Fugue process.", labels, float64(runtime.NumGoroutine()))

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	WriteGaugeMetric(w, "fugue_go_alloc_bytes", "Bytes of allocated heap objects.", labels, float64(mem.Alloc))
	WriteGaugeMetric(w, "fugue_go_heap_alloc_bytes", "Bytes of allocated heap memory.", labels, float64(mem.HeapAlloc))
	WriteGaugeMetric(w, "fugue_go_heap_sys_bytes", "Bytes of heap memory obtained from the OS.", labels, float64(mem.HeapSys))
	WriteCounterMetric(w, "fugue_go_gc_cycles_total", "Completed GC cycles.", labels, float64(mem.NumGC))
}

func WriteGaugeMetric(w io.Writer, name, help string, labels map[string]string, value float64) {
	WriteMetricHeader(w, name, help, "gauge")
	WriteMetricSample(w, name, labels, value)
}

func WriteCounterMetric(w io.Writer, name, help string, labels map[string]string, value float64) {
	WriteMetricHeader(w, name, help, "counter")
	WriteMetricSample(w, name, labels, value)
}

func WriteMetricHeader(w io.Writer, name, help, metricType string) {
	name = sanitizeMetricName(name)
	if name == "" {
		return
	}
	help = strings.TrimSpace(help)
	if help != "" {
		fmt.Fprintf(w, "# HELP %s %s\n", name, escapePrometheusHelp(help))
	}
	metricType = strings.TrimSpace(metricType)
	if metricType != "" {
		fmt.Fprintf(w, "# TYPE %s %s\n", name, metricType)
	}
}

func WriteMetricSample(w io.Writer, name string, labels map[string]string, value float64) {
	name = sanitizeMetricName(name)
	if name == "" {
		return
	}
	fmt.Fprintf(w, "%s%s %.6f\n", name, formatPrometheusLabels(labels), value)
}

func formatPrometheusLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for key, value := range labels {
		key = strings.TrimSpace(key)
		if key == "" || strings.TrimSpace(value) == "" {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) == 0 {
		return ""
	}
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf(`%s="%s"`, sanitizeLabelName(key), EscapePrometheusLabelValue(labels[key])))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func EscapePrometheusLabelValue(value string) string {
	value = strings.TrimSpace(value)
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return value
}

func escapePrometheusHelp(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	return value
}

func sanitizeMetricName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	var builder strings.Builder
	for index, r := range name {
		if r == '_' || r == ':' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || index > 0 && r >= '0' && r <= '9' {
			builder.WriteRune(r)
			continue
		}
		builder.WriteByte('_')
	}
	return builder.String()
}

func sanitizeLabelName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "label"
	}
	var builder strings.Builder
	for index, r := range name {
		if r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || index > 0 && r >= '0' && r <= '9' {
			builder.WriteRune(r)
			continue
		}
		builder.WriteByte('_')
	}
	return builder.String()
}
