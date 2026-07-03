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
		"FUGUE_OBSERVABILITY_KUBERNETES_LOGS_ENABLED",
		"FUGUE_OBSERVABILITY_KUBERNETES_LOG_NAMESPACES",
		"FUGUE_OBSERVABILITY_KUBERNETES_LOG_NAMESPACE_PREFIXES",
		"FUGUE_OBSERVABILITY_KUBERNETES_LOG_LABEL_SELECTOR",
		"FUGUE_OBSERVABILITY_CLICKHOUSE_QUERY_MAX_PAYLOAD_BYTES",
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
	t.Setenv("FUGUE_OBSERVABILITY_KUBERNETES_LOGS_ENABLED", "true")
	t.Setenv("FUGUE_OBSERVABILITY_KUBERNETES_LOG_NAMESPACES", "fugue-system,fg-tenant")
	t.Setenv("FUGUE_OBSERVABILITY_KUBERNETES_LOG_NAMESPACE_PREFIXES", "fg-")
	t.Setenv("FUGUE_OBSERVABILITY_KUBERNETES_LOG_LABEL_SELECTOR", "app.kubernetes.io/managed-by=fugue")
	t.Setenv("FUGUE_OBSERVABILITY_KUBERNETES_LOG_POLL_INTERVAL", "7s")
	t.Setenv("FUGUE_OBSERVABILITY_KUBERNETES_LOG_TAIL_LINES", "33")
	t.Setenv("FUGUE_OBSERVABILITY_KUBERNETES_LOG_MAX_PODS", "44")
	t.Setenv("FUGUE_OBSERVABILITY_KUBERNETES_LOG_MAX_LINES_PER_CYCLE", "55")
	t.Setenv("FUGUE_OBSERVABILITY_QUEUE_SIZE", "66")
	t.Setenv("FUGUE_OBSERVABILITY_BATCH_SIZE", "11")
	t.Setenv("FUGUE_OBSERVABILITY_CLICKHOUSE_QUERY_MAX_PAYLOAD_BYTES", "888")
	t.Setenv("FUGUE_OBSERVABILITY_MEMORY_LIMIT_BYTES", "777")
	t.Setenv("FUGUE_OBSERVABILITY_TENANT_EVENT_QUOTA_PER_MINUTE", "1000")
	t.Setenv("FUGUE_OBSERVABILITY_APP_EVENT_QUOTA_PER_MINUTE", "100")
	t.Setenv("FUGUE_OBSERVABILITY_TENANT_EVENT_QUOTA_OVERRIDES", "tenant_hot=2000,tenant_bad=0,bad")
	t.Setenv("FUGUE_OBSERVABILITY_APP_RETENTION_OVERRIDES", "app_hot=6h,app_bad=0s,bad")
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
	if !status.KubernetesLogsConfigured {
		t.Fatalf("expected Kubernetes logs configured, got %+v", status)
	}
	if len(cfg.RuntimeLogPaths) != 1 || cfg.RuntimeLogPaths[0] != "/var/log/pods/app.log" {
		t.Fatalf("expected runtime log paths to be normalized, got %+v", cfg.RuntimeLogPaths)
	}
	if len(cfg.KubernetesLogNamespaces) != 2 || cfg.KubernetesLogNamespaces[0] != "fugue-system" || cfg.KubernetesLogNamespaces[1] != "fg-tenant" {
		t.Fatalf("expected Kubernetes log namespaces to be parsed, got %+v", cfg.KubernetesLogNamespaces)
	}
	if cfg.KubernetesLogLabelSelector != "app.kubernetes.io/managed-by=fugue" || cfg.KubernetesLogPollInterval != 7*time.Second || cfg.KubernetesLogTailLines != 33 || cfg.KubernetesLogMaxPods != 44 || cfg.KubernetesLogMaxLinesPerCycle != 55 {
		t.Fatalf("expected Kubernetes log settings from env, got %+v", cfg)
	}
	if cfg.QueueSize != 66 || cfg.BatchSize != 11 || cfg.ClickHouseQueryMaxPayloadBytes != 888 || cfg.MemoryLimitBytes != 777 {
		t.Fatalf("expected telemetry pipeline sizing from env, got %+v", cfg)
	}
	if cfg.TenantEventQuotaPerMinute != 1000 || cfg.AppEventQuotaPerMinute != 100 {
		t.Fatalf("expected quota settings from env, got %+v", cfg)
	}
	if cfg.TenantEventQuotaFor("tenant_hot") != 2000 || cfg.TenantEventQuotaFor("tenant_123") != 1000 {
		t.Fatalf("expected tenant quota overrides from env, got %+v", cfg.TenantEventQuotaOverrides)
	}
	if cfg.RetentionForApp("app_hot") != 6*time.Hour || cfg.RetentionForApp("app_123") != 2*time.Hour {
		t.Fatalf("expected app retention overrides from env, got %+v", cfg.AppRetentionOverrides)
	}
}

func TestControlPlaneMetricsBindAddrFromEnv(t *testing.T) {
	t.Setenv("FUGUE_API_METRICS_BIND_ADDR", ":19090")
	t.Setenv("FUGUE_CONTROLLER_METRICS_BIND_ADDR", ":19091")

	apiCfg := APIFromEnv()
	if apiCfg.MetricsBindAddr != ":19090" {
		t.Fatalf("expected API metrics bind addr from env, got %q", apiCfg.MetricsBindAddr)
	}
	controllerCfg := ControllerFromEnv()
	if controllerCfg.MetricsBindAddr != ":19091" {
		t.Fatalf("expected controller metrics bind addr from env, got %q", controllerCfg.MetricsBindAddr)
	}
}

func TestAPIFromEnvReadsDefaultManagedPostgresStorageClass(t *testing.T) {
	t.Setenv("FUGUE_DEFAULT_MANAGED_POSTGRES_STORAGE_CLASS_NAME", "fugue-postgres-rwo")

	cfg := APIFromEnv()
	if cfg.ManagedPostgresStorageClass != "fugue-postgres-rwo" {
		t.Fatalf("expected managed postgres storage class from env, got %q", cfg.ManagedPostgresStorageClass)
	}
}

func TestControllerFromEnvReadsAppObservabilityEndpoint(t *testing.T) {
	t.Setenv("FUGUE_APP_OBSERVABILITY_ENDPOINT", "http://telemetry-agent.fugue-system.svc.cluster.local:7834")

	cfg := ControllerFromEnv()
	if cfg.AppObservabilityEndpoint != "http://telemetry-agent.fugue-system.svc.cluster.local:7834" {
		t.Fatalf("expected app observability endpoint from env, got %q", cfg.AppObservabilityEndpoint)
	}
}

func TestControllerFromEnvDefaultsImageCacheOrphanPruneToLimitedDelete(t *testing.T) {
	t.Setenv("FUGUE_IMAGE_STORE_ORPHAN_PRUNE_MODE", "")
	t.Setenv("FUGUE_IMAGE_STORE_ORPHAN_PRUNE_MAX_DELETE_BYTES_PER_NODE", "")

	cfg := ControllerFromEnv()
	if cfg.ImageStoreOrphanPruneMode != "delete" {
		t.Fatalf("expected image-cache orphan prune to default to delete, got %q", cfg.ImageStoreOrphanPruneMode)
	}
	if cfg.ImageStoreOrphanPruneMaxDeleteBytesPerNode != "104857600" {
		t.Fatalf("expected image-cache orphan prune budget to default to 100MiB, got %q", cfg.ImageStoreOrphanPruneMaxDeleteBytesPerNode)
	}
}

func TestControllerFromEnvManagedAppRolloutTimeout(t *testing.T) {
	t.Setenv("FUGUE_CONTROLLER_MANAGED_APP_ROLLOUT_TIMEOUT", "")

	cfg := ControllerFromEnv()
	if cfg.ManagedAppRolloutTimeout != time.Hour {
		t.Fatalf("expected default managed app rollout timeout 1h, got %s", cfg.ManagedAppRolloutTimeout)
	}

	t.Setenv("FUGUE_CONTROLLER_MANAGED_APP_ROLLOUT_TIMEOUT", "45m")
	cfg = ControllerFromEnv()
	if cfg.ManagedAppRolloutTimeout != 45*time.Minute {
		t.Fatalf("expected configured managed app rollout timeout 45m, got %s", cfg.ManagedAppRolloutTimeout)
	}
}

func TestControllerFromEnvReadsRegistryMaintenanceNames(t *testing.T) {
	t.Setenv("FUGUE_CONTROLLER_REGISTRY_GC_LEASE_NAME", "registry-gc-state")
	t.Setenv("FUGUE_CONTROLLER_REGISTRY_JANITOR_CRONJOB_NAME", "registry-retention")
	t.Setenv("FUGUE_CONTROLLER_REGISTRY_GC_CRONJOB_NAME", "registry-gc")

	cfg := ControllerFromEnv()
	if cfg.RegistryGCLeaseName != "registry-gc-state" ||
		cfg.RegistryJanitorCronJobName != "registry-retention" ||
		cfg.RegistryGCCronJobName != "registry-gc" {
		t.Fatalf("unexpected registry maintenance names: %+v", cfg)
	}
}

func TestAPIFromEnvReadsRegistryGCLeaseName(t *testing.T) {
	t.Setenv("FUGUE_REGISTRY_GC_LEASE_NAME", "registry-gc-state")

	cfg := APIFromEnv()
	if cfg.RegistryGCLeaseName != "registry-gc-state" {
		t.Fatalf("expected registry GC lease name from env, got %q", cfg.RegistryGCLeaseName)
	}
}
