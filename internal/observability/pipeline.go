package observability

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type EventKind string

const (
	EventKindLog    EventKind = "log"
	EventKindMetric EventKind = "metric"
	EventKindSpan   EventKind = "span"
)

type Event struct {
	Timestamp  time.Time         `json:"timestamp"`
	Kind       EventKind         `json:"kind"`
	Source     string            `json:"source"`
	Message    string            `json:"message,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

type PipelineSnapshot struct {
	Enabled                 bool   `json:"enabled"`
	Running                 bool   `json:"running"`
	QueueDepth              int64  `json:"queue_depth"`
	QueuedBytes             int64  `json:"queued_bytes"`
	Received                uint64 `json:"received"`
	Exported                uint64 `json:"exported"`
	Dropped                 uint64 `json:"dropped"`
	Redacted                uint64 `json:"redacted"`
	Batches                 uint64 `json:"batches"`
	ExportErrors            uint64 `json:"export_errors"`
	RuntimeLogLines         uint64 `json:"runtime_log_lines"`
	RuntimeLogErrors        uint64 `json:"runtime_log_errors"`
	KubernetesLogLines      uint64 `json:"kubernetes_log_lines"`
	KubernetesLogErrors     uint64 `json:"kubernetes_log_errors"`
	KubernetesLogPods       int64  `json:"kubernetes_log_pods"`
	PrometheusScrapes       uint64 `json:"prometheus_scrapes"`
	PrometheusScrapeErrors  uint64 `json:"prometheus_scrape_errors"`
	OTLPRequests            uint64 `json:"otlp_requests"`
	OTLPBytes               uint64 `json:"otlp_bytes"`
	QuotaDropped            uint64 `json:"quota_dropped"`
	LastError               string `json:"last_error,omitempty"`
	RuntimeLogPipelineCount int    `json:"runtime_log_pipeline_count"`
	KubernetesLogPipeline   bool   `json:"kubernetes_log_pipeline"`
	PrometheusScrapeCount   int    `json:"prometheus_scrape_count"`
}

type Exporter interface {
	Name() string
	Export(context.Context, []Event) error
}

type NoopExporter struct{}

func (NoopExporter) Name() string {
	return "noop"
}

func (NoopExporter) Export(context.Context, []Event) error {
	return nil
}

type telemetryQuotaBucket struct {
	WindowStart time.Time
	Count       int
}

type telemetryTenantMeter struct {
	Received     uint64
	Dropped      uint64
	QuotaDropped uint64
}

type telemetryTenantMeterSnapshot struct {
	TenantID     string
	Received     uint64
	Dropped      uint64
	QuotaDropped uint64
}

type Pipeline struct {
	cfg        Config
	logger     *log.Logger
	queue      chan Event
	exporter   Exporter
	httpClient *http.Client

	mu      sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	running bool

	quotaMu            sync.Mutex
	tenantQuotaBuckets map[string]telemetryQuotaBucket
	appQuotaBuckets    map[string]telemetryQuotaBucket
	meterMu            sync.Mutex
	tenantMeters       map[string]telemetryTenantMeter

	queueDepth             atomic.Int64
	queuedBytes            atomic.Int64
	received               atomic.Uint64
	exported               atomic.Uint64
	dropped                atomic.Uint64
	redacted               atomic.Uint64
	quotaDropped           atomic.Uint64
	batches                atomic.Uint64
	exportErrors           atomic.Uint64
	runtimeLogLines        atomic.Uint64
	runtimeLogErrors       atomic.Uint64
	kubernetesLogLines     atomic.Uint64
	kubernetesLogErrors    atomic.Uint64
	kubernetesLogPods      atomic.Int64
	prometheusScrapes      atomic.Uint64
	prometheusScrapeErrors atomic.Uint64
	otlpRequests           atomic.Uint64
	otlpBytes              atomic.Uint64
	lastError              atomic.Value
}

func NewPipeline(cfg Config, logger *log.Logger) *Pipeline {
	cfg = cfg.Normalize()
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	httpClient := &http.Client{
		Timeout: cfg.ExportTimeout,
	}
	return &Pipeline{
		cfg:                cfg,
		logger:             logger,
		queue:              make(chan Event, cfg.QueueSize),
		exporter:           NewConfiguredExporter(cfg, httpClient),
		httpClient:         httpClient,
		tenantQuotaBuckets: map[string]telemetryQuotaBucket{},
		appQuotaBuckets:    map[string]telemetryQuotaBucket{},
		tenantMeters:       map[string]telemetryTenantMeter{},
	}
}

func (p *Pipeline) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.running {
		return nil
	}
	if !p.cfg.Enabled {
		return nil
	}
	if err := p.cfg.Validate(); err != nil {
		p.recordError(err)
		return err
	}
	p.ctx, p.cancel = context.WithCancel(ctx)
	p.running = true

	p.wg.Add(1)
	go p.runBatchExporter()

	for _, path := range p.cfg.RuntimeLogPaths {
		path := path
		p.wg.Add(1)
		go p.runRuntimeLogTail(path)
	}
	if p.cfg.KubernetesLogsEnabled {
		p.wg.Add(1)
		go p.runKubernetesLogCollection()
	}
	for _, scrapeURL := range p.cfg.PrometheusScrapeURLs {
		scrapeURL := scrapeURL
		p.wg.Add(1)
		go p.runPrometheusScrape(scrapeURL)
	}
	return nil
}

func (p *Pipeline) Stop(ctx context.Context) error {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return nil
	}
	p.cancel()
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	p.mu.Unlock()

	select {
	case <-done:
		p.mu.Lock()
		p.running = false
		p.mu.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Pipeline) Snapshot() PipelineSnapshot {
	p.mu.Lock()
	running := p.running
	p.mu.Unlock()
	return PipelineSnapshot{
		Enabled:                 p.cfg.Enabled,
		Running:                 running,
		QueueDepth:              p.queueDepth.Load(),
		QueuedBytes:             p.queuedBytes.Load(),
		Received:                p.received.Load(),
		Exported:                p.exported.Load(),
		Dropped:                 p.dropped.Load(),
		Redacted:                p.redacted.Load(),
		Batches:                 p.batches.Load(),
		ExportErrors:            p.exportErrors.Load(),
		RuntimeLogLines:         p.runtimeLogLines.Load(),
		RuntimeLogErrors:        p.runtimeLogErrors.Load(),
		KubernetesLogLines:      p.kubernetesLogLines.Load(),
		KubernetesLogErrors:     p.kubernetesLogErrors.Load(),
		KubernetesLogPods:       p.kubernetesLogPods.Load(),
		PrometheusScrapes:       p.prometheusScrapes.Load(),
		PrometheusScrapeErrors:  p.prometheusScrapeErrors.Load(),
		OTLPRequests:            p.otlpRequests.Load(),
		OTLPBytes:               p.otlpBytes.Load(),
		QuotaDropped:            p.quotaDropped.Load(),
		LastError:               p.lastErrorString(),
		RuntimeLogPipelineCount: len(p.cfg.RuntimeLogPaths),
		KubernetesLogPipeline:   p.cfg.KubernetesLogsEnabled,
		PrometheusScrapeCount:   len(p.cfg.PrometheusScrapeURLs),
	}
}

func (p *Pipeline) Ingest(ctx context.Context, event Event) bool {
	if !p.cfg.Enabled {
		p.dropped.Add(1)
		return false
	}
	if p.cfg.SampleRate < 1 && rand.Float64() > p.cfg.SampleRate {
		p.dropped.Add(1)
		return false
	}

	event, redacted := p.prepareEvent(event)
	p.redacted.Add(uint64(redacted))
	if !p.allowWithinTelemetryQuota(event) {
		p.dropped.Add(1)
		p.quotaDropped.Add(1)
		p.recordTenantMeter(event, "dropped")
		p.recordTenantMeter(event, "quota_dropped")
		return false
	}
	size := eventSize(event)
	if p.cfg.MemoryLimitBytes > 0 {
		nextSize := p.queuedBytes.Add(size)
		if nextSize > p.cfg.MemoryLimitBytes {
			p.queuedBytes.Add(-size)
			p.dropped.Add(1)
			p.recordTenantMeter(event, "dropped")
			p.recordError(errors.New("telemetry memory limiter dropped event"))
			return false
		}
	}

	select {
	case p.queue <- event:
		p.received.Add(1)
		p.queueDepth.Add(1)
		p.recordTenantMeter(event, "received")
		return true
	case <-ctx.Done():
		if p.cfg.MemoryLimitBytes > 0 {
			p.queuedBytes.Add(-size)
		}
		p.dropped.Add(1)
		p.recordTenantMeter(event, "dropped")
		p.recordError(ctx.Err())
		return false
	default:
		if p.cfg.MemoryLimitBytes > 0 {
			p.queuedBytes.Add(-size)
		}
		p.dropped.Add(1)
		p.recordTenantMeter(event, "dropped")
		p.recordError(errors.New("telemetry queue full"))
		return false
	}
}

func (p *Pipeline) HandleOTLPHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	kind := otlpPathKind(r.URL.Path)
	if kind == "" {
		http.NotFound(w, r)
		return
	}
	p.otlpRequests.Add(1)
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, p.cfg.MaxPayloadBytes))
	if err != nil {
		p.dropped.Add(1)
		p.recordError(fmt.Errorf("read OTLP payload: %w", err))
		writePipelineJSON(w, http.StatusRequestEntityTooLarge, map[string]string{
			"status": "rejected",
			"reason": "payload too large",
		})
		return
	}
	p.otlpBytes.Add(uint64(len(body)))
	if !json.Valid(body) && strings.Contains(strings.ToLower(r.Header.Get("Content-Type")), "json") {
		p.dropped.Add(1)
		p.recordError(errors.New("invalid OTLP JSON payload"))
		writePipelineJSON(w, http.StatusBadRequest, map[string]string{
			"status": "rejected",
			"reason": "invalid JSON payload",
		})
		return
	}

	events, redacted := eventsFromOTLPJSON(kind, r.URL.Path, r.Header.Get("Content-Type"), body, time.Now().UTC())
	if len(events) == 0 {
		var structuredEvents []Event
		var structuredRedacted int
		structuredEvents, structuredRedacted = eventsFromStructuredTelemetryJSON(kind, r.URL.Path, r.Header.Get("Content-Type"), body, time.Now().UTC())
		if len(structuredEvents) > 0 {
			events = structuredEvents
			redacted += structuredRedacted
		}
	}
	p.redacted.Add(uint64(redacted))
	if len(events) == 0 {
		events = []Event{{
			Timestamp: time.Now().UTC(),
			Kind:      kind,
			Source:    "otlp_http",
			Message:   "otlp payload accepted",
			Attributes: map[string]string{
				"path":          r.URL.Path,
				"content_type":  r.Header.Get("Content-Type"),
				"payload_bytes": strconv.Itoa(len(body)),
			},
		}}
	}
	for _, event := range events {
		p.Ingest(r.Context(), event)
	}
	writePipelineJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}

func (p *Pipeline) PrometheusMetrics() string {
	snap := p.Snapshot()
	var b strings.Builder
	writeMetric := func(name string, value uint64) {
		_, _ = fmt.Fprintf(&b, "%s %d\n", name, value)
	}
	_, _ = fmt.Fprintln(&b, "# HELP fugue_telemetry_pipeline_queue_depth Current telemetry queue depth.")
	_, _ = fmt.Fprintln(&b, "# TYPE fugue_telemetry_pipeline_queue_depth gauge")
	_, _ = fmt.Fprintf(&b, "fugue_telemetry_pipeline_queue_depth %d\n", snap.QueueDepth)
	_, _ = fmt.Fprintln(&b, "# HELP fugue_telemetry_pipeline_queued_bytes Approximate bytes held in the telemetry queue.")
	_, _ = fmt.Fprintln(&b, "# TYPE fugue_telemetry_pipeline_queued_bytes gauge")
	_, _ = fmt.Fprintf(&b, "fugue_telemetry_pipeline_queued_bytes %d\n", snap.QueuedBytes)
	_, _ = fmt.Fprintln(&b, "# HELP fugue_telemetry_pipeline_events_total Telemetry events observed by pipeline outcome.")
	_, _ = fmt.Fprintln(&b, "# TYPE fugue_telemetry_pipeline_events_total counter")
	_, _ = fmt.Fprintf(&b, "fugue_telemetry_pipeline_events_total{outcome=\"received\"} %d\n", snap.Received)
	_, _ = fmt.Fprintf(&b, "fugue_telemetry_pipeline_events_total{outcome=\"exported\"} %d\n", snap.Exported)
	_, _ = fmt.Fprintf(&b, "fugue_telemetry_pipeline_events_total{outcome=\"dropped\"} %d\n", snap.Dropped)
	_, _ = fmt.Fprintf(&b, "fugue_telemetry_pipeline_events_total{outcome=\"quota_dropped\"} %d\n", snap.QuotaDropped)
	_, _ = fmt.Fprintf(&b, "fugue_telemetry_pipeline_events_total{outcome=\"redacted\"} %d\n", snap.Redacted)
	_, _ = fmt.Fprintln(&b, "# HELP fugue_telemetry_tenant_events_total Telemetry events observed by tenant for platform observability metering.")
	_, _ = fmt.Fprintln(&b, "# TYPE fugue_telemetry_tenant_events_total counter")
	for _, meter := range p.tenantMeterSnapshot() {
		tenantID := EscapePrometheusLabelValue(meter.TenantID)
		_, _ = fmt.Fprintf(&b, "fugue_telemetry_tenant_events_total{tenant_id=\"%s\",outcome=\"received\"} %d\n", tenantID, meter.Received)
		_, _ = fmt.Fprintf(&b, "fugue_telemetry_tenant_events_total{tenant_id=\"%s\",outcome=\"dropped\"} %d\n", tenantID, meter.Dropped)
		_, _ = fmt.Fprintf(&b, "fugue_telemetry_tenant_events_total{tenant_id=\"%s\",outcome=\"quota_dropped\"} %d\n", tenantID, meter.QuotaDropped)
	}
	_, _ = fmt.Fprintln(&b, "# HELP fugue_telemetry_pipeline_batches_total Export batches attempted by the telemetry pipeline.")
	_, _ = fmt.Fprintln(&b, "# TYPE fugue_telemetry_pipeline_batches_total counter")
	writeMetric("fugue_telemetry_pipeline_batches_total", snap.Batches)
	_, _ = fmt.Fprintln(&b, "# HELP fugue_telemetry_pipeline_errors_total Pipeline errors by source.")
	_, _ = fmt.Fprintln(&b, "# TYPE fugue_telemetry_pipeline_errors_total counter")
	_, _ = fmt.Fprintf(&b, "fugue_telemetry_pipeline_errors_total{source=\"export\"} %d\n", snap.ExportErrors)
	_, _ = fmt.Fprintf(&b, "fugue_telemetry_pipeline_errors_total{source=\"runtime_log\"} %d\n", snap.RuntimeLogErrors)
	_, _ = fmt.Fprintf(&b, "fugue_telemetry_pipeline_errors_total{source=\"kubernetes_log\"} %d\n", snap.KubernetesLogErrors)
	_, _ = fmt.Fprintf(&b, "fugue_telemetry_pipeline_errors_total{source=\"prometheus_scrape\"} %d\n", snap.PrometheusScrapeErrors)
	_, _ = fmt.Fprintln(&b, "# HELP fugue_telemetry_pipeline_kubernetes_log_lines_total Kubernetes pod log lines ingested by the telemetry pipeline.")
	_, _ = fmt.Fprintln(&b, "# TYPE fugue_telemetry_pipeline_kubernetes_log_lines_total counter")
	writeMetric("fugue_telemetry_pipeline_kubernetes_log_lines_total", snap.KubernetesLogLines)
	_, _ = fmt.Fprintln(&b, "# HELP fugue_telemetry_pipeline_kubernetes_log_pods Last Kubernetes pod count considered by the log collector.")
	_, _ = fmt.Fprintln(&b, "# TYPE fugue_telemetry_pipeline_kubernetes_log_pods gauge")
	_, _ = fmt.Fprintf(&b, "fugue_telemetry_pipeline_kubernetes_log_pods %d\n", snap.KubernetesLogPods)
	_, _ = fmt.Fprintln(&b, "# HELP fugue_telemetry_pipeline_otlp_requests_total OTLP HTTP requests received by the telemetry pipeline.")
	_, _ = fmt.Fprintln(&b, "# TYPE fugue_telemetry_pipeline_otlp_requests_total counter")
	writeMetric("fugue_telemetry_pipeline_otlp_requests_total", snap.OTLPRequests)
	return b.String()
}

func (p *Pipeline) IngestLogLine(ctx context.Context, source string, line string) bool {
	return p.IngestLogLineWithAttributes(ctx, source, line, nil, time.Time{})
}

func (p *Pipeline) IngestLogLineWithAttributes(ctx context.Context, source string, line string, attrs map[string]string, timestamp time.Time) bool {
	event, redacted := EventFromLogLine(source, line)
	p.redacted.Add(uint64(redacted))
	if !timestamp.IsZero() {
		event.Timestamp = timestamp.UTC()
	}
	if len(attrs) > 0 {
		if event.Attributes == nil {
			event.Attributes = map[string]string{}
		}
		for key, value := range attrs {
			if strings.TrimSpace(key) == "" {
				continue
			}
			event.Attributes[key] = value
		}
	}
	return p.Ingest(ctx, event)
}

func EventFromLogLine(source string, line string) (Event, int) {
	line = strings.TrimRight(line, "\r\n")
	attrs := map[string]string{}
	message := line
	redacted := 0

	var raw map[string]any
	kind := EventKindLog
	if err := json.Unmarshal([]byte(line), &raw); err == nil {
		kind = eventKindFromStructuredLog(raw)
		for key, value := range raw {
			if IsSecretField(key) {
				redacted++
				continue
			}
			switch v := value.(type) {
			case string:
				if key == "message" || key == "msg" {
					if clean, changed := RedactText(v); changed {
						redacted++
						message = clean
					} else {
						message = v
					}
					continue
				}
				attrs[key] = v
			case float64, bool:
				attrs[key] = fmt.Sprint(v)
			}
		}
	} else if clean, changed := RedactText(line); changed {
		redacted++
		message = clean
	}

	return Event{
		Timestamp:  time.Now().UTC(),
		Kind:       kind,
		Source:     source,
		Message:    message,
		Attributes: attrs,
	}, redacted
}

func eventKindFromStructuredLog(raw map[string]any) EventKind {
	for _, key := range []string{"kind", "event_kind"} {
		value, ok := raw[key].(string)
		if !ok {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(value)) {
		case string(EventKindLog):
			return EventKindLog
		case string(EventKindMetric), "metrics":
			return EventKindMetric
		case string(EventKindSpan), "trace":
			return EventKindSpan
		}
	}
	if value, ok := raw["event_type"].(string); ok {
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "request_span", "span", "trace_span":
			return EventKindSpan
		case "metric", "metrics", "metric_scrape":
			return EventKindMetric
		}
	}
	return EventKindLog
}

func (p *Pipeline) runBatchExporter() {
	defer p.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	batch := make([]Event, 0, p.cfg.BatchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		p.exportBatch(batch)
		batch = make([]Event, 0, p.cfg.BatchSize)
	}
	for {
		select {
		case <-p.ctx.Done():
			flush()
			return
		case event := <-p.queue:
			p.queueDepth.Add(-1)
			p.queuedBytes.Add(-eventSize(event))
			batch = append(batch, event)
			if len(batch) >= p.cfg.BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (p *Pipeline) exportBatch(batch []Event) {
	p.batches.Add(1)
	ctx, cancel := context.WithTimeout(p.ctx, p.cfg.ExportTimeout)
	defer cancel()

	attempts := p.cfg.RetryMaxAttempts
	if attempts < 1 {
		attempts = 1
	}
	var err error
	for attempt := 1; attempt <= attempts; attempt++ {
		err = p.exporter.Export(ctx, batch)
		if err == nil {
			p.exported.Add(uint64(len(batch)))
			return
		}
		if attempt < attempts {
			select {
			case <-ctx.Done():
				break
			case <-time.After(time.Duration(attempt) * 100 * time.Millisecond):
			}
		}
	}
	p.exportErrors.Add(1)
	p.recordError(fmt.Errorf("export via %s failed: %w", p.exporter.Name(), err))
}

func (p *Pipeline) runRuntimeLogTail(path string) {
	defer p.wg.Done()
	file, err := os.Open(path)
	if err != nil {
		p.runtimeLogErrors.Add(1)
		p.recordError(fmt.Errorf("open runtime log %s: %w", path, err))
		return
	}
	defer file.Close()
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		p.runtimeLogErrors.Add(1)
		p.recordError(fmt.Errorf("seek runtime log %s: %w", path, err))
		return
	}
	reader := bufio.NewReader(file)
	for {
		select {
		case <-p.ctx.Done():
			return
		default:
		}
		line, err := reader.ReadString('\n')
		if err == nil {
			p.runtimeLogLines.Add(1)
			p.IngestLogLine(p.ctx, path, line)
			continue
		}
		if errors.Is(err, io.EOF) {
			time.Sleep(250 * time.Millisecond)
			continue
		}
		p.runtimeLogErrors.Add(1)
		p.recordError(fmt.Errorf("read runtime log %s: %w", path, err))
		return
	}
}

func (p *Pipeline) runPrometheusScrape(scrapeURL string) {
	defer p.wg.Done()
	p.scrapePrometheusOnce(scrapeURL)
	ticker := time.NewTicker(p.cfg.ScrapeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.scrapePrometheusOnce(scrapeURL)
		}
	}
}

func (p *Pipeline) scrapePrometheusOnce(scrapeURL string) {
	req, err := http.NewRequestWithContext(p.ctx, http.MethodGet, scrapeURL, nil)
	if err != nil {
		p.prometheusScrapeErrors.Add(1)
		p.recordError(err)
		return
	}
	resp, err := p.httpClient.Do(req)
	if err != nil {
		p.prometheusScrapeErrors.Add(1)
		p.recordError(fmt.Errorf("scrape %s: %w", scrapeURL, err))
		return
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, p.cfg.MaxPayloadBytes))
	if err != nil {
		p.prometheusScrapeErrors.Add(1)
		p.recordError(fmt.Errorf("read scrape %s: %w", scrapeURL, err))
		return
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		p.prometheusScrapeErrors.Add(1)
		p.recordError(fmt.Errorf("scrape %s returned %s", scrapeURL, resp.Status))
		return
	}
	samples := countPrometheusSamples(body)
	p.prometheusScrapes.Add(1)
	p.Ingest(p.ctx, Event{
		Timestamp: time.Now().UTC(),
		Kind:      EventKindMetric,
		Source:    scrapeURL,
		Message:   "prometheus scrape accepted",
		Attributes: map[string]string{
			"sample_count": strconv.Itoa(samples),
		},
	})
}

func (p *Pipeline) prepareEvent(event Event) (Event, int) {
	redacted := 0
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	if event.Kind == "" {
		event.Kind = EventKindLog
	}
	event.Source = strings.TrimSpace(event.Source)
	if clean, changed := RedactText(event.Message); changed {
		redacted++
		event.Message = clean
	}
	attrs := map[string]string{}
	for key, value := range event.Attributes {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if IsSecretField(key) {
			redacted++
			continue
		}
		if event.Kind == EventKindMetric && IsDeniedMetricLabel(key) {
			redacted++
			continue
		}
		if clean, changed := RedactText(strings.TrimSpace(value)); changed {
			redacted++
			attrs[key] = clean
		} else {
			attrs[key] = strings.TrimSpace(value)
		}
	}
	for key, value := range p.cfg.Identity.Attributes() {
		if attrs[key] == "" {
			attrs[key] = value
		}
	}
	event.Attributes = attrs
	return event, redacted
}

func (p *Pipeline) allowWithinTelemetryQuota(event Event) bool {
	tenantID := strings.TrimSpace(event.Attributes["tenant_id"])
	appID := strings.TrimSpace(event.Attributes["app_id"])
	tenantLimit := 0
	if tenantID != "" {
		tenantLimit = p.cfg.TenantEventQuotaPerMinute
		if quota := p.cfg.TenantEventQuotaOverrides[tenantID]; quota > 0 {
			tenantLimit = quota
		}
	}
	appLimit := 0
	if appID != "" {
		appLimit = p.cfg.AppEventQuotaPerMinute
	}
	if tenantLimit <= 0 && appLimit <= 0 {
		return true
	}

	windowStart := time.Now().UTC().Truncate(time.Minute)
	p.quotaMu.Lock()
	defer p.quotaMu.Unlock()
	p.pruneTelemetryQuotaBuckets(windowStart)

	if tenantLimit > 0 && quotaBucketCount(p.tenantQuotaBuckets, tenantID, windowStart) >= tenantLimit {
		return false
	}
	if appLimit > 0 && quotaBucketCount(p.appQuotaBuckets, appID, windowStart) >= appLimit {
		return false
	}
	if tenantLimit > 0 {
		incrementQuotaBucket(p.tenantQuotaBuckets, tenantID, windowStart)
	}
	if appLimit > 0 {
		incrementQuotaBucket(p.appQuotaBuckets, appID, windowStart)
	}
	return true
}

func (p *Pipeline) pruneTelemetryQuotaBuckets(windowStart time.Time) {
	if len(p.tenantQuotaBuckets)+len(p.appQuotaBuckets) < 2048 {
		return
	}
	cutoff := windowStart.Add(-2 * time.Minute)
	for key, bucket := range p.tenantQuotaBuckets {
		if bucket.WindowStart.Before(cutoff) {
			delete(p.tenantQuotaBuckets, key)
		}
	}
	for key, bucket := range p.appQuotaBuckets {
		if bucket.WindowStart.Before(cutoff) {
			delete(p.appQuotaBuckets, key)
		}
	}
}

func quotaBucketCount(buckets map[string]telemetryQuotaBucket, key string, windowStart time.Time) int {
	bucket := buckets[key]
	if !bucket.WindowStart.Equal(windowStart) {
		return 0
	}
	return bucket.Count
}

func incrementQuotaBucket(buckets map[string]telemetryQuotaBucket, key string, windowStart time.Time) {
	bucket := buckets[key]
	if !bucket.WindowStart.Equal(windowStart) {
		bucket = telemetryQuotaBucket{WindowStart: windowStart}
	}
	bucket.Count++
	buckets[key] = bucket
}

func (p *Pipeline) recordTenantMeter(event Event, outcome string) {
	tenantID := strings.TrimSpace(event.Attributes["tenant_id"])
	if tenantID == "" {
		return
	}
	p.meterMu.Lock()
	defer p.meterMu.Unlock()
	if _, ok := p.tenantMeters[tenantID]; !ok && len(p.tenantMeters) >= 4096 {
		return
	}
	meter := p.tenantMeters[tenantID]
	switch outcome {
	case "received":
		meter.Received++
	case "dropped":
		meter.Dropped++
	case "quota_dropped":
		meter.QuotaDropped++
	}
	p.tenantMeters[tenantID] = meter
}

func (p *Pipeline) tenantMeterSnapshot() []telemetryTenantMeterSnapshot {
	p.meterMu.Lock()
	defer p.meterMu.Unlock()
	items := make([]telemetryTenantMeterSnapshot, 0, len(p.tenantMeters))
	for tenantID, meter := range p.tenantMeters {
		items = append(items, telemetryTenantMeterSnapshot{
			TenantID:     tenantID,
			Received:     meter.Received,
			Dropped:      meter.Dropped,
			QuotaDropped: meter.QuotaDropped,
		})
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].TenantID < items[j].TenantID
	})
	return items
}

func (p *Pipeline) recordError(err error) {
	if err == nil {
		return
	}
	p.lastError.Store(err.Error())
	p.logger.Printf("telemetry pipeline warning: %v", err)
}

func (p *Pipeline) lastErrorString() string {
	value := p.lastError.Load()
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	return ""
}

func eventSize(event Event) int64 {
	size := len(event.Source) + len(event.Message) + 64
	for key, value := range event.Attributes {
		size += len(key) + len(value) + 8
	}
	return int64(size)
}

func otlpPathKind(path string) EventKind {
	switch path {
	case "/v1/logs":
		return EventKindLog
	case "/v1/metrics":
		return EventKindMetric
	case "/v1/traces":
		return EventKindSpan
	default:
		return ""
	}
}

func countPrometheusSamples(body []byte) int {
	count := 0
	scanner := bufio.NewScanner(bytes.NewReader(body))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		count++
	}
	return count
}

func writePipelineJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
