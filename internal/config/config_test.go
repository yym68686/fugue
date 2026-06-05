package config

import (
	"testing"
	"time"
)

func TestDNSFromEnvDefaultsEdgeHealthProbeEnabled(t *testing.T) {
	t.Setenv("FUGUE_DNS_EDGE_HEALTH_PROBE_ENABLED", "")

	cfg := DNSFromEnv()
	if !cfg.EdgeHealthProbeEnabled {
		t.Fatal("expected DNS edge health probe to default enabled")
	}
}

func TestDNSFromEnvAllowsDisablingEdgeHealthProbe(t *testing.T) {
	t.Setenv("FUGUE_DNS_EDGE_HEALTH_PROBE_ENABLED", "false")

	cfg := DNSFromEnv()
	if cfg.EdgeHealthProbeEnabled {
		t.Fatal("expected explicit env value to disable DNS edge health probe")
	}
}

func TestObservabilityFromEnvDefaultsToDisabledTwentyFourHourRetention(t *testing.T) {
	for _, key := range []string{
		"FUGUE_OBSERVABILITY_ENABLED",
		"FUGUE_OBSERVABILITY_RETENTION",
		"FUGUE_OBSERVABILITY_METRICS_REMOTE_WRITE_URL",
		"FUGUE_OBSERVABILITY_METRICS_QUERY_URL",
		"FUGUE_OBSERVABILITY_LOKI_URL",
		"FUGUE_OBSERVABILITY_CLICKHOUSE_DSN",
		"FUGUE_OBSERVABILITY_OTLP_ENDPOINT",
		"FUGUE_OBSERVABILITY_RUNTIME_LOG_PATHS",
		"FUGUE_OBSERVABILITY_PROMETHEUS_SCRAPE_URLS",
		"FUGUE_OBSERVABILITY_COMPONENT",
	} {
		t.Setenv(key, "")
	}

	cfg := ObservabilityFromEnv()
	if cfg.Enabled {
		t.Fatal("expected observability to be disabled by default")
	}
	if cfg.Retention != 24*time.Hour {
		t.Fatalf("expected 24h retention, got %s", cfg.Retention)
	}
	if cfg.HasExporters() {
		t.Fatalf("expected no exporters by default, got %+v", cfg.Exporters())
	}
}

func TestObservabilityFromEnvReadsExporterConfiguration(t *testing.T) {
	t.Setenv("FUGUE_OBSERVABILITY_ENABLED", "true")
	t.Setenv("FUGUE_OBSERVABILITY_RETENTION", "2h")
	t.Setenv("FUGUE_OBSERVABILITY_METRICS_REMOTE_WRITE_URL", "https://metrics.example.test/api/v1/write")
	t.Setenv("FUGUE_OBSERVABILITY_METRICS_QUERY_URL", "https://metrics.example.test/api/v1/query")
	t.Setenv("FUGUE_OBSERVABILITY_LOKI_URL", "https://loki.example.test")
	t.Setenv("FUGUE_OBSERVABILITY_CLICKHOUSE_DSN", "clickhouse://user:secret@example.test/fugue")
	t.Setenv("FUGUE_OBSERVABILITY_OTLP_ENDPOINT", "otel.example.test:4317")
	t.Setenv("FUGUE_OBSERVABILITY_RUNTIME_LOG_PATHS", "/var/log/pods/app.log,/var/log/pods/app.log")
	t.Setenv("FUGUE_OBSERVABILITY_PROMETHEUS_SCRAPE_URLS", "http://127.0.0.1:9100/metrics")
	t.Setenv("FUGUE_OBSERVABILITY_TENANT_ID", "tenant_123")
	t.Setenv("FUGUE_OBSERVABILITY_PROJECT_ID", "project_123")
	t.Setenv("FUGUE_OBSERVABILITY_APP_ID", "app_123")
	t.Setenv("FUGUE_OBSERVABILITY_RUNTIME_ID", "runtime_123")
	t.Setenv("FUGUE_OBSERVABILITY_COMPONENT", "runtime")

	cfg := ObservabilityFromEnv()
	if !cfg.Enabled {
		t.Fatal("expected observability to be enabled")
	}
	if cfg.Retention != 2*time.Hour {
		t.Fatalf("expected configured retention, got %s", cfg.Retention)
	}
	status := cfg.Status()
	if !status.MetricsConfigured || !status.MetricsQueryConfigured || !status.LogsConfigured || !status.AnalyticsConfigured || !status.OTLPConfigured {
		t.Fatalf("expected all exporters configured, got %+v", status)
	}
	if !status.RuntimeLogPipelineConfigured || !status.PrometheusScrapeConfigured || !status.IdentityConfigured {
		t.Fatalf("expected pipeline inputs and identity configured, got %+v", status)
	}
	if len(cfg.RuntimeLogPaths) != 1 || cfg.RuntimeLogPaths[0] != "/var/log/pods/app.log" {
		t.Fatalf("expected runtime log paths to be normalized, got %+v", cfg.RuntimeLogPaths)
	}
}
