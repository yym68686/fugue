package observability

import (
	"testing"
	"time"
)

func TestConfigNormalizeKeepsObservabilityDisabledByDefault(t *testing.T) {
	cfg := (Config{}).Normalize()
	if cfg.Enabled {
		t.Fatal("expected observability to be disabled by default")
	}
	if cfg.Retention != 24*time.Hour {
		t.Fatalf("expected default retention to be 24h, got %s", cfg.Retention)
	}
	if cfg.Mode() != "disabled" {
		t.Fatalf("expected disabled mode, got %s", cfg.Mode())
	}
	if cfg.QueueSize != DefaultQueueSize || cfg.BatchSize != DefaultBatchSize || cfg.MaxPayloadBytes != DefaultMaxPayloadBytes {
		t.Fatalf("expected telemetry pipeline defaults, got %+v", cfg.Status())
	}
}

func TestConfigStatusDoesNotExposeBackendSecrets(t *testing.T) {
	cfg := Config{
		Enabled:                        true,
		MetricsRemoteWriteURL:          "https://metrics.example.test/api/v1/write",
		MetricsQueryURL:                "https://metrics.example.test/api/v1/query",
		LokiURL:                        "https://loki.example.test",
		ClickHouseDSN:                  "clickhouse://user:secret@example.test/fugue",
		OTLPEndpoint:                   "otel.example.test:4317",
		RuntimeLogPaths:                []string{"/var/log/pods/app.log"},
		PrometheusScrapeURLs:           []string{"http://127.0.0.1:9100/metrics"},
		KubernetesLogsEnabled:          true,
		KubernetesLogNamespaces:        []string{"fugue-system"},
		KubernetesLogNamespacePrefixes: []string{"fg-"},
		Identity:                       Identity{TenantID: "tenant_123", Component: "runtime"},
	}.Normalize()
	status := cfg.Status()
	if !status.Enabled || !status.MetricsConfigured || !status.MetricsQueryConfigured || !status.LogsConfigured || !status.AnalyticsConfigured || !status.OTLPConfigured {
		t.Fatalf("expected all exporters to be marked configured, got %+v", status)
	}
	if !status.RuntimeLogPipelineConfigured || !status.PrometheusScrapeConfigured || !status.IdentityConfigured {
		t.Fatalf("expected input pipelines and identity to be marked configured, got %+v", status)
	}
	if !status.KubernetesLogsConfigured {
		t.Fatalf("expected Kubernetes log pipeline to be marked configured, got %+v", status)
	}
	if status.Retention != "24h0m0s" {
		t.Fatalf("expected normalized retention, got %s", status.Retention)
	}
	if len(status.Exporters) != 3 {
		t.Fatalf("expected implemented exporter names only, got %+v", status.Exporters)
	}
	if status.Exporters[0] != "analytics" || status.Exporters[1] != "logs" || status.Exporters[2] != "metrics" {
		t.Fatalf("unexpected implemented exporters: %+v", status.Exporters)
	}
	backends := cfg.Backends()
	if len(backends) != 4 || backends[0] != "analytics" || backends[1] != "logs" || backends[2] != "metrics" || backends[3] != "otlp" {
		t.Fatalf("unexpected configured backends: %+v", backends)
	}
}

func TestConfigBackendsIncludeMetricsQueryWithoutExporter(t *testing.T) {
	cfg := Config{
		Enabled:         true,
		MetricsQueryURL: "https://metrics.example.test/api/v1/query",
	}.Normalize()
	if cfg.HasExporters() {
		t.Fatalf("metrics query URL should not be treated as a write exporter: %+v", cfg.Exporters())
	}
	backends := cfg.Backends()
	if len(backends) != 1 || backends[0] != "metrics" {
		t.Fatalf("expected metrics backend from query URL, got %+v", backends)
	}
}

func TestConfigModeTreatsMetricsAsBaselineExporter(t *testing.T) {
	cfg := Config{
		Enabled:               true,
		MetricsRemoteWriteURL: "https://metrics.example.test/api/v1/write",
		OTLPEndpoint:          "otel.example.test:4317",
	}.Normalize()
	if !cfg.HasExporters() {
		t.Fatalf("metrics exporter should be active: %+v", cfg.Exporters())
	}
	if got := cfg.Mode(); got != "baseline" {
		t.Fatalf("expected metrics-only mode to be baseline, got %s", got)
	}

	cfg.LokiURL = "https://loki.example.test"
	if got := cfg.Mode(); got != "baseline" {
		t.Fatalf("expected Loki-only mode to be baseline, got %s", got)
	}

	cfg.ClickHouseDSN = "http://clickhouse.example.test:8123?database=fugue_observability"
	if got := cfg.Mode(); got != "instrumented" {
		t.Fatalf("expected ClickHouse mode to be instrumented, got %s", got)
	}
}

func TestConfigValidateRejectsBadURLs(t *testing.T) {
	for _, cfg := range []Config{
		{Enabled: true, MetricsRemoteWriteURL: "ftp://metrics.example.test"},
		{Enabled: true, MetricsQueryURL: "ftp://metrics.example.test"},
		{Enabled: true, LokiURL: "://bad"},
		{Enabled: true, ClickHouseDSN: "postgres://clickhouse.example.test"},
		{Enabled: true, OTLPEndpoint: "missing-port"},
		{Enabled: true, PrometheusScrapeURLs: []string{"ftp://127.0.0.1/metrics"}},
	} {
		if err := cfg.Validate(); err == nil {
			t.Fatalf("expected validation error for %+v", cfg)
		}
	}
}
