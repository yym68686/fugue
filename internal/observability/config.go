package observability

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"
)

const (
	DefaultRetention     = 24 * time.Hour
	DefaultExportTimeout = 5 * time.Second
	DefaultQueueSize     = 4096
	DefaultSampleRate    = 1.0
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
}

type Status struct {
	Enabled             bool     `json:"enabled"`
	Mode                string   `json:"mode"`
	Retention           string   `json:"retention"`
	MetricsConfigured   bool     `json:"metrics_configured"`
	LogsConfigured      bool     `json:"logs_configured"`
	AnalyticsConfigured bool     `json:"analytics_configured"`
	OTLPConfigured      bool     `json:"otlp_configured"`
	Exporters           []string `json:"exporters,omitempty"`
}

func (c Config) Normalize() Config {
	c.MetricsRemoteWriteURL = strings.TrimSpace(c.MetricsRemoteWriteURL)
	c.LokiURL = strings.TrimSpace(c.LokiURL)
	c.ClickHouseDSN = strings.TrimSpace(c.ClickHouseDSN)
	c.OTLPEndpoint = strings.TrimSpace(c.OTLPEndpoint)
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
	return c
}

func (c Config) Exporters() []string {
	c = c.Normalize()
	exporters := []string{}
	if c.MetricsRemoteWriteURL != "" {
		exporters = append(exporters, "metrics")
	}
	if c.LokiURL != "" {
		exporters = append(exporters, "logs")
	}
	if c.ClickHouseDSN != "" {
		exporters = append(exporters, "analytics")
	}
	if c.OTLPEndpoint != "" {
		exporters = append(exporters, "otlp")
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
		Enabled:             c.Enabled,
		Mode:                c.Mode(),
		Retention:           c.Retention.String(),
		MetricsConfigured:   c.MetricsRemoteWriteURL != "",
		LogsConfigured:      c.LokiURL != "",
		AnalyticsConfigured: c.ClickHouseDSN != "",
		OTLPConfigured:      c.OTLPEndpoint != "",
		Exporters:           c.Exporters(),
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
	if c.OTLPEndpoint != "" || c.ClickHouseDSN != "" {
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
	if err := validateOptionalEndpoint("OTLP endpoint", c.OTLPEndpoint); err != nil {
		return err
	}
	return nil
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
