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
}

func TestConfigStatusDoesNotExposeBackendSecrets(t *testing.T) {
	cfg := Config{
		Enabled:               true,
		MetricsRemoteWriteURL: "https://metrics.example.test/api/v1/write",
		LokiURL:               "https://loki.example.test",
		ClickHouseDSN:         "clickhouse://user:secret@example.test/fugue",
		OTLPEndpoint:          "otel.example.test:4317",
	}.Normalize()
	status := cfg.Status()
	if !status.Enabled || !status.MetricsConfigured || !status.LogsConfigured || !status.AnalyticsConfigured || !status.OTLPConfigured {
		t.Fatalf("expected all exporters to be marked configured, got %+v", status)
	}
	if status.Retention != "24h0m0s" {
		t.Fatalf("expected normalized retention, got %s", status.Retention)
	}
	if len(status.Exporters) != 4 {
		t.Fatalf("expected four exporter names, got %+v", status.Exporters)
	}
}

func TestConfigValidateRejectsBadURLs(t *testing.T) {
	for _, cfg := range []Config{
		{Enabled: true, MetricsRemoteWriteURL: "ftp://metrics.example.test"},
		{Enabled: true, LokiURL: "://bad"},
		{Enabled: true, OTLPEndpoint: "missing-port"},
	} {
		if err := cfg.Validate(); err == nil {
			t.Fatalf("expected validation error for %+v", cfg)
		}
	}
}
