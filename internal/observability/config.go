package observability

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"
)

const (
	DefaultRetention       = 24 * time.Hour
	DefaultExportTimeout   = 5 * time.Second
	DefaultQueueSize       = 4096
	DefaultSampleRate      = 1.0
	DefaultScrapeInterval  = 30 * time.Second
	DefaultBatchSize       = 128
	DefaultMaxPayloadBytes = 1 << 20
	DefaultMemoryLimit     = 64 << 20
	DefaultRetryAttempts   = 3
)

type Config struct {
	Enabled               bool
	Retention             time.Duration
	MetricsRemoteWriteURL string
	LokiURL               string
	ClickHouseDSN         string
	OTLPEndpoint          string
	ExportTimeout         time.Duration
	QueueSize             int
	SampleRate            float64
	RuntimeLogPaths       []string
	PrometheusScrapeURLs  []string
	ScrapeInterval        time.Duration
	BatchSize             int
	MaxPayloadBytes       int64
	MemoryLimitBytes      int64
	RetryMaxAttempts      int
	Identity              Identity
}

type Identity struct {
	TenantID  string
	ProjectID string
	AppID     string
	RuntimeID string
	Component string
}

type Status struct {
	Enabled                      bool     `json:"enabled"`
	Mode                         string   `json:"mode"`
	Retention                    string   `json:"retention"`
	MetricsConfigured            bool     `json:"metrics_configured"`
	LogsConfigured               bool     `json:"logs_configured"`
	AnalyticsConfigured          bool     `json:"analytics_configured"`
	OTLPConfigured               bool     `json:"otlp_configured"`
	RuntimeLogPipelineConfigured bool     `json:"runtime_log_pipeline_configured"`
	PrometheusScrapeConfigured   bool     `json:"prometheus_scrape_configured"`
	IdentityConfigured           bool     `json:"identity_configured"`
	QueueSize                    int      `json:"queue_size"`
	BatchSize                    int      `json:"batch_size"`
	MaxPayloadBytes              int64    `json:"max_payload_bytes"`
	MemoryLimitBytes             int64    `json:"memory_limit_bytes"`
	RetryMaxAttempts             int      `json:"retry_max_attempts"`
	Exporters                    []string `json:"exporters,omitempty"`
}

func (c Config) Normalize() Config {
	c.MetricsRemoteWriteURL = strings.TrimSpace(c.MetricsRemoteWriteURL)
	c.LokiURL = strings.TrimSpace(c.LokiURL)
	c.ClickHouseDSN = strings.TrimSpace(c.ClickHouseDSN)
	c.OTLPEndpoint = strings.TrimSpace(c.OTLPEndpoint)
	c.RuntimeLogPaths = normalizeStringList(c.RuntimeLogPaths)
	c.PrometheusScrapeURLs = normalizeStringList(c.PrometheusScrapeURLs)
	c.Identity = c.Identity.Normalize()
	if c.Retention <= 0 {
		c.Retention = DefaultRetention
	}
	if c.ExportTimeout <= 0 {
		c.ExportTimeout = DefaultExportTimeout
	}
	if c.QueueSize <= 0 {
		c.QueueSize = DefaultQueueSize
	}
	if c.SampleRate <= 0 || c.SampleRate > 1 {
		c.SampleRate = DefaultSampleRate
	}
	if c.ScrapeInterval <= 0 {
		c.ScrapeInterval = DefaultScrapeInterval
	}
	if c.BatchSize <= 0 {
		c.BatchSize = DefaultBatchSize
	}
	if c.BatchSize > c.QueueSize {
		c.BatchSize = c.QueueSize
	}
	if c.MaxPayloadBytes <= 0 {
		c.MaxPayloadBytes = DefaultMaxPayloadBytes
	}
	if c.MemoryLimitBytes <= 0 {
		c.MemoryLimitBytes = DefaultMemoryLimit
	}
	if c.RetryMaxAttempts <= 0 {
		c.RetryMaxAttempts = DefaultRetryAttempts
	}
	return c
}

func (c Config) Exporters() []string {
	c = c.Normalize()
	exporters := []string{}
	if c.LokiURL != "" {
		exporters = append(exporters, "logs")
	}
	if c.ClickHouseDSN != "" {
		exporters = append(exporters, "analytics")
	}
	sort.Strings(exporters)
	return exporters
}

func (c Config) HasExporters() bool {
	return len(c.Exporters()) > 0
}

func (c Config) Status() Status {
	c = c.Normalize()
	return Status{
		Enabled:                      c.Enabled,
		Mode:                         c.Mode(),
		Retention:                    c.Retention.String(),
		MetricsConfigured:            c.MetricsRemoteWriteURL != "",
		LogsConfigured:               c.LokiURL != "",
		AnalyticsConfigured:          c.ClickHouseDSN != "",
		OTLPConfigured:               c.OTLPEndpoint != "",
		RuntimeLogPipelineConfigured: len(c.RuntimeLogPaths) > 0,
		PrometheusScrapeConfigured:   len(c.PrometheusScrapeURLs) > 0,
		IdentityConfigured:           c.Identity.HasResourceIdentity(),
		QueueSize:                    c.QueueSize,
		BatchSize:                    c.BatchSize,
		MaxPayloadBytes:              c.MaxPayloadBytes,
		MemoryLimitBytes:             c.MemoryLimitBytes,
		RetryMaxAttempts:             c.RetryMaxAttempts,
		Exporters:                    c.Exporters(),
	}
}

func (c Config) Mode() string {
	c = c.Normalize()
	if !c.Enabled {
		return "disabled"
	}
	if !c.HasExporters() {
		return "enabled_without_exporters"
	}
	if c.ClickHouseDSN != "" {
		return "instrumented"
	}
	return "baseline"
}

func (c Config) Validate() error {
	c = c.Normalize()
	if err := validateOptionalHTTPURL("metrics remote write URL", c.MetricsRemoteWriteURL); err != nil {
		return err
	}
	if err := validateOptionalHTTPURL("Loki URL", c.LokiURL); err != nil {
		return err
	}
	if err := validateOptionalClickHouseDSN("ClickHouse DSN", c.ClickHouseDSN); err != nil {
		return err
	}
	if err := validateOptionalEndpoint("OTLP endpoint", c.OTLPEndpoint); err != nil {
		return err
	}
	for _, raw := range c.PrometheusScrapeURLs {
		if err := validateOptionalHTTPURL("Prometheus scrape URL", raw); err != nil {
			return err
		}
	}
	return nil
}

func (i Identity) Normalize() Identity {
	return Identity{
		TenantID:  strings.TrimSpace(i.TenantID),
		ProjectID: strings.TrimSpace(i.ProjectID),
		AppID:     strings.TrimSpace(i.AppID),
		RuntimeID: strings.TrimSpace(i.RuntimeID),
		Component: strings.TrimSpace(i.Component),
	}
}

func (i Identity) HasResourceIdentity() bool {
	i = i.Normalize()
	return i.TenantID != "" || i.ProjectID != "" || i.AppID != "" || i.RuntimeID != "" || i.Component != ""
}

func (i Identity) Attributes() map[string]string {
	i = i.Normalize()
	attrs := map[string]string{}
	if i.TenantID != "" {
		attrs["tenant_id"] = i.TenantID
	}
	if i.ProjectID != "" {
		attrs["project_id"] = i.ProjectID
	}
	if i.AppID != "" {
		attrs["app_id"] = i.AppID
	}
	if i.RuntimeID != "" {
		attrs["runtime_id"] = i.RuntimeID
	}
	if i.Component != "" {
		attrs["component"] = i.Component
	}
	return attrs
}

func validateOptionalHTTPURL(name, raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return fmt.Errorf("invalid %s", name)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("%s must use http or https", name)
	}
	return nil
}

func validateOptionalClickHouseDSN(name, raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return fmt.Errorf("invalid %s", name)
	}
	switch parsed.Scheme {
	case "http", "https", "clickhouse":
		return nil
	default:
		return fmt.Errorf("%s must use http, https, or clickhouse", name)
	}
}

func validateOptionalEndpoint(name, raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	if strings.Contains(raw, "://") {
		return validateOptionalHTTPURL(name, raw)
	}
	if strings.Contains(raw, " ") || !strings.Contains(raw, ":") {
		return fmt.Errorf("invalid %s", name)
	}
	return nil
}

func normalizeStringList(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := []string{}
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
