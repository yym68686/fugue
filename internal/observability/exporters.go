package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
)

const defaultClickHouseDatabase = "fugue_observability"

type MultiExporter struct {
	exporters []Exporter
}

func NewConfiguredExporter(cfg Config, client *http.Client) Exporter {
	cfg = cfg.Normalize()
	if client == nil {
		client = &http.Client{Timeout: cfg.ExportTimeout}
	}
	exporters := []Exporter{}
	if cfg.LokiURL != "" {
		exporters = append(exporters, LokiExporter{
			Client:  client,
			PushURL: normalizeLokiPushURL(cfg.LokiURL),
		})
	}
	if cfg.ClickHouseDSN != "" {
		exporters = append(exporters, NewClickHouseExporter(cfg.ClickHouseDSN, client))
	}
	switch len(exporters) {
	case 0:
		return NoopExporter{}
	case 1:
		return exporters[0]
	default:
		return MultiExporter{exporters: exporters}
	}
}

func (e MultiExporter) Name() string {
	names := make([]string, 0, len(e.exporters))
	for _, exporter := range e.exporters {
		names = append(names, exporter.Name())
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}

func (e MultiExporter) Export(ctx context.Context, events []Event) error {
	var errs []error
	for _, exporter := range e.exporters {
		if err := exporter.Export(ctx, events); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

type LokiExporter struct {
	Client  *http.Client
	PushURL string
}

type lokiPushRequest struct {
	Streams []lokiStream `json:"streams"`
}

type lokiStream struct {
	Stream map[string]string `json:"stream"`
	Values [][2]string       `json:"values"`
}

func (e LokiExporter) Name() string {
	return "logs"
}

func (e LokiExporter) Export(ctx context.Context, events []Event) error {
	if strings.TrimSpace(e.PushURL) == "" {
		return nil
	}
	streamsByKey := map[string]*lokiStream{}
	for _, event := range events {
		if event.Kind != EventKindLog {
			continue
		}
		labels := lokiLabels(event)
		key := stableLabelKey(labels)
		stream := streamsByKey[key]
		if stream == nil {
			stream = &lokiStream{Stream: labels}
			streamsByKey[key] = stream
		}
		line, err := json.Marshal(lokiLogLine(event))
		if err != nil {
			return fmt.Errorf("marshal Loki log line: %w", err)
		}
		stream.Values = append(stream.Values, [2]string{
			strconv.FormatInt(event.Timestamp.UTC().UnixNano(), 10),
			string(line),
		})
	}
	if len(streamsByKey) == 0 {
		return nil
	}
	payload := lokiPushRequest{Streams: make([]lokiStream, 0, len(streamsByKey))}
	for _, stream := range streamsByKey {
		payload.Streams = append(payload.Streams, *stream)
	}
	sort.Slice(payload.Streams, func(i, j int) bool {
		return stableLabelKey(payload.Streams[i].Stream) < stableLabelKey(payload.Streams[j].Stream)
	})
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal Loki payload: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.PushURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create Loki request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.Client.Do(req)
	if err != nil {
		return fmt.Errorf("push Loki payload: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		responseBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("push Loki payload returned %s: %s", resp.Status, strings.TrimSpace(string(responseBody)))
	}
	return nil
}

type lokiLine struct {
	Timestamp  string            `json:"timestamp"`
	Kind       EventKind         `json:"kind"`
	Source     string            `json:"source,omitempty"`
	Message    string            `json:"message,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

func lokiLogLine(event Event) lokiLine {
	return lokiLine{
		Timestamp:  event.Timestamp.UTC().Format(time.RFC3339Nano),
		Kind:       event.Kind,
		Source:     event.Source,
		Message:    event.Message,
		Attributes: event.Attributes,
	}
}

func lokiLabels(event Event) map[string]string {
	labels := map[string]string{}
	for _, key := range []string{"tenant_id", "project_id", "app_id", "runtime_id", "pod", "container", "component", "level"} {
		if value := strings.TrimSpace(event.Attributes[key]); value != "" {
			labels[key] = sanitizeLokiLabelValue(value)
		}
	}
	if labels["component"] == "" {
		labels["component"] = "telemetry"
	}
	return labels
}

func sanitizeLokiLabelValue(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 128 {
		return value[:128]
	}
	return value
}

func stableLabelKey(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+labels[key])
	}
	return strings.Join(parts, "\xff")
}

func normalizeLokiPushURL(raw string) string {
	raw = strings.TrimSpace(raw)
	parsed, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	cleanPath := strings.TrimRight(parsed.Path, "/")
	if !strings.HasSuffix(cleanPath, "/loki/api/v1/push") {
		cleanPath = path.Join(cleanPath, "/loki/api/v1/push")
	}
	parsed.Path = cleanPath
	return parsed.String()
}

type ClickHouseExporter struct {
	Client   *http.Client
	Target   clickHouseTarget
	parseErr error
}

type clickHouseTarget struct {
	URL      *url.URL
	Database string
	Username string
	Password string
}

func NewClickHouseExporter(rawDSN string, client *http.Client) ClickHouseExporter {
	target, err := parseClickHouseTarget(rawDSN)
	return ClickHouseExporter{
		Client:   client,
		Target:   target,
		parseErr: err,
	}
}

func (e ClickHouseExporter) Name() string {
	return "analytics"
}

func (e ClickHouseExporter) Export(ctx context.Context, events []Event) error {
	if e.parseErr != nil {
		return e.parseErr
	}
	rowsByTable := map[string][][]byte{}
	for _, event := range events {
		table, row, err := clickHouseRowForEvent(event)
		if err != nil {
			return err
		}
		if table == "" {
			continue
		}
		raw, err := json.Marshal(row)
		if err != nil {
			return fmt.Errorf("marshal ClickHouse row for %s: %w", table, err)
		}
		rowsByTable[table] = append(rowsByTable[table], raw)
	}
	if len(rowsByTable) == 0 {
		return nil
	}
	var errs []error
	for table, rows := range rowsByTable {
		if err := e.insertRows(ctx, table, rows); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (e ClickHouseExporter) insertRows(ctx context.Context, table string, rows [][]byte) error {
	if len(rows) == 0 {
		return nil
	}
	body := bytes.Join(rows, []byte("\n"))
	body = append(body, '\n')

	endpoint := *e.Target.URL
	query := endpoint.Query()
	if e.Target.Database != "" {
		query.Set("database", e.Target.Database)
	}
	query.Set("query", "INSERT INTO "+clickHouseQualifiedTable(e.Target.Database, table)+" FORMAT JSONEachRow")
	endpoint.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create ClickHouse request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if e.Target.Username != "" {
		req.SetBasicAuth(e.Target.Username, e.Target.Password)
	}
	resp, err := e.Client.Do(req)
	if err != nil {
		return fmt.Errorf("insert ClickHouse rows into %s: %w", table, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		responseBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("insert ClickHouse rows into %s returned %s: %s", table, resp.Status, strings.TrimSpace(string(responseBody)))
	}
	return nil
}

func clickHouseQualifiedTable(database string, table string) string {
	database = sanitizeClickHouseIdentifier(database)
	if database == "" {
		database = defaultClickHouseDatabase
	}
	return database + "." + sanitizeClickHouseIdentifier(table)
}

func parseClickHouseTarget(raw string) (clickHouseTarget, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return clickHouseTarget{}, errors.New("empty ClickHouse DSN")
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return clickHouseTarget{}, errors.New("invalid ClickHouse DSN")
	}
	target := clickHouseTarget{Database: defaultClickHouseDatabase}
	if parsed.User != nil {
		target.Username = parsed.User.Username()
		target.Password, _ = parsed.User.Password()
		parsed.User = nil
	}
	switch parsed.Scheme {
	case "http", "https":
		if database := strings.Trim(parsed.Query().Get("database"), " /"); database != "" {
			target.Database = database
		}
		target.URL = parsed
	case "clickhouse":
		database := strings.Trim(parsed.EscapedPath(), " /")
		if database != "" {
			target.Database = database
		}
		scheme := "http"
		if parsed.Query().Get("secure") == "true" || parsed.Query().Get("protocol") == "https" {
			scheme = "https"
		}
		host := parsed.Host
		if parsed.Port() == "" {
			host = net.JoinHostPort(parsed.Hostname(), "8123")
		}
		target.URL = &url.URL{Scheme: scheme, Host: host}
	default:
		return clickHouseTarget{}, errors.New("ClickHouse DSN must use http, https, or clickhouse scheme")
	}
	return target, nil
}

func clickHouseRowForEvent(event Event) (string, any, error) {
	switch clickHouseTableForEvent(event) {
	case "request_facts":
		return "request_facts", clickHouseRequestFactRow(event), nil
	case "request_spans":
		return "request_spans", clickHouseRequestSpanRow(event), nil
	case "app_events":
		return "app_events", clickHouseAppEventRow(event), nil
	case "":
		return "", nil, nil
	default:
		return "", nil, errors.New("unsupported ClickHouse observability table")
	}
}

func clickHouseTableForEvent(event Event) string {
	for _, key := range []string{"fugue_table", "table"} {
		switch strings.ToLower(strings.TrimSpace(event.Attributes[key])) {
		case "request_facts", "request_fact":
			return "request_facts"
		case "request_spans", "request_span":
			return "request_spans"
		case "app_events", "app_event":
			return "app_events"
		}
	}
	switch event.Kind {
	case EventKindSpan:
		return "request_spans"
	default:
		switch strings.ToLower(strings.TrimSpace(event.Attributes["event_type"])) {
		case "request_fact", "request_summary":
			return "request_facts"
		case "request_span", "trace_span":
			return "request_spans"
		case "app_event", "operation_event", "deploy_event", "runtime_event", "platform_event":
			return "app_events"
		default:
			return ""
		}
	}
}

type requestFactRow struct {
	Timestamp    string `json:"ts"`
	TenantID     string `json:"tenant_id"`
	ProjectID    string `json:"project_id"`
	AppID        string `json:"app_id"`
	RuntimeID    string `json:"runtime_id"`
	EdgeID       string `json:"edge_id"`
	Pod          string `json:"pod"`
	TraceID      string `json:"trace_id"`
	RequestID    string `json:"request_id"`
	RouteID      string `json:"route_id"`
	Hostname     string `json:"hostname"`
	PathTemplate string `json:"path_template"`
	Method       string `json:"method"`
	StatusCode   uint16 `json:"status_code"`
	StatusClass  string `json:"status_class"`
	DurationMS   uint32 `json:"duration_ms"`
	TTFBMS       uint32 `json:"ttfb_ms"`
	UpstreamMS   uint32 `json:"upstream_ms"`
	BytesIn      uint64 `json:"bytes_in"`
	BytesOut     uint64 `json:"bytes_out"`
	Streaming    bool   `json:"streaming"`
	ErrorType    string `json:"error_type"`
	DeploymentID string `json:"deployment_id"`
	OperationID  string `json:"operation_id"`
	SummaryJSON  string `json:"summary_json"`
}

type requestSpanRow struct {
	Timestamp      string `json:"ts"`
	TenantID       string `json:"tenant_id"`
	ProjectID      string `json:"project_id"`
	AppID          string `json:"app_id"`
	RuntimeID      string `json:"runtime_id"`
	Service        string `json:"service"`
	TraceID        string `json:"trace_id"`
	SpanID         string `json:"span_id"`
	ParentSpanID   string `json:"parent_span_id"`
	RequestID      string `json:"request_id"`
	Stage          string `json:"stage"`
	StageMS        uint32 `json:"stage_ms"`
	StatusCode     uint16 `json:"status_code"`
	ErrorType      string `json:"error_type"`
	AttributesJSON string `json:"attributes_json"`
}

type appEventRow struct {
	Timestamp      string `json:"ts"`
	TenantID       string `json:"tenant_id"`
	ProjectID      string `json:"project_id"`
	AppID          string `json:"app_id"`
	EventType      string `json:"event_type"`
	Severity       string `json:"severity"`
	OperationID    string `json:"operation_id"`
	DeploymentID   string `json:"deployment_id"`
	RuntimeID      string `json:"runtime_id"`
	Pod            string `json:"pod"`
	Message        string `json:"message"`
	AttributesJSON string `json:"attributes_json"`
}

func clickHouseRequestFactRow(event Event) requestFactRow {
	return requestFactRow{
		Timestamp:    clickHouseTime(event.Timestamp),
		TenantID:     eventAttr(event, "tenant_id"),
		ProjectID:    eventAttr(event, "project_id"),
		AppID:        eventAttr(event, "app_id"),
		RuntimeID:    eventAttr(event, "runtime_id"),
		EdgeID:       eventAttr(event, "edge_id"),
		Pod:          eventAttr(event, "pod"),
		TraceID:      eventAttr(event, "trace_id"),
		RequestID:    eventAttr(event, "request_id"),
		RouteID:      eventAttr(event, "route_id"),
		Hostname:     eventAttr(event, "hostname"),
		PathTemplate: eventAttr(event, "path_template"),
		Method:       eventAttr(event, "method"),
		StatusCode:   uint16Attr(event, "status_code"),
		StatusClass:  firstAttr(event, "status_class", "status"),
		DurationMS:   uint32Attr(event, "duration_ms", "total_ms"),
		TTFBMS:       uint32Attr(event, "ttfb_ms"),
		UpstreamMS:   uint32Attr(event, "upstream_ms"),
		BytesIn:      uint64Attr(event, "bytes_in"),
		BytesOut:     uint64Attr(event, "bytes_out"),
		Streaming:    boolAttr(event, "streaming", "stream"),
		ErrorType:    eventAttr(event, "error_type"),
		DeploymentID: eventAttr(event, "deployment_id"),
		OperationID:  eventAttr(event, "operation_id"),
		SummaryJSON:  summaryJSON(event),
	}
}

func clickHouseRequestSpanRow(event Event) requestSpanRow {
	return requestSpanRow{
		Timestamp:      clickHouseTime(event.Timestamp),
		TenantID:       eventAttr(event, "tenant_id"),
		ProjectID:      eventAttr(event, "project_id"),
		AppID:          eventAttr(event, "app_id"),
		RuntimeID:      eventAttr(event, "runtime_id"),
		Service:        firstAttr(event, "service", "component"),
		TraceID:        eventAttr(event, "trace_id"),
		SpanID:         eventAttr(event, "span_id"),
		ParentSpanID:   eventAttr(event, "parent_span_id"),
		RequestID:      eventAttr(event, "request_id"),
		Stage:          firstAttr(event, "stage", "name"),
		StageMS:        uint32Attr(event, "stage_ms", "duration_ms"),
		StatusCode:     uint16Attr(event, "status_code"),
		ErrorType:      eventAttr(event, "error_type"),
		AttributesJSON: attributesJSON(event),
	}
}

func clickHouseAppEventRow(event Event) appEventRow {
	eventType := firstAttr(event, "event_type", "kind")
	if eventType == "" {
		eventType = string(event.Kind)
	}
	severity := firstAttr(event, "severity", "level")
	if severity == "" {
		severity = "info"
	}
	return appEventRow{
		Timestamp:      clickHouseTime(event.Timestamp),
		TenantID:       eventAttr(event, "tenant_id"),
		ProjectID:      eventAttr(event, "project_id"),
		AppID:          eventAttr(event, "app_id"),
		EventType:      eventType,
		Severity:       severity,
		OperationID:    eventAttr(event, "operation_id"),
		DeploymentID:   eventAttr(event, "deployment_id"),
		RuntimeID:      eventAttr(event, "runtime_id"),
		Pod:            eventAttr(event, "pod"),
		Message:        event.Message,
		AttributesJSON: attributesJSON(event),
	}
}

func eventAttr(event Event, key string) string {
	return strings.TrimSpace(event.Attributes[key])
}

func firstAttr(event Event, keys ...string) string {
	for _, key := range keys {
		if value := eventAttr(event, key); value != "" {
			return value
		}
	}
	return ""
}

func uint16Attr(event Event, keys ...string) uint16 {
	value := uint64Attr(event, keys...)
	if value > 65535 {
		return 65535
	}
	return uint16(value)
}

func uint32Attr(event Event, keys ...string) uint32 {
	value := uint64Attr(event, keys...)
	if value > 4294967295 {
		return 4294967295
	}
	return uint32(value)
}

func uint64Attr(event Event, keys ...string) uint64 {
	for _, key := range keys {
		raw := eventAttr(event, key)
		if raw == "" {
			continue
		}
		if value, err := strconv.ParseUint(raw, 10, 64); err == nil {
			return value
		}
		if value, err := strconv.ParseFloat(raw, 64); err == nil && value > 0 {
			return uint64(value)
		}
	}
	return 0
}

func boolAttr(event Event, keys ...string) bool {
	for _, key := range keys {
		raw := strings.ToLower(eventAttr(event, key))
		switch raw {
		case "true", "1", "yes":
			return true
		case "false", "0", "no":
			return false
		}
	}
	return false
}

func attributesJSON(event Event) string {
	if len(event.Attributes) == 0 {
		return "{}"
	}
	body, err := json.Marshal(event.Attributes)
	if err != nil {
		return "{}"
	}
	return string(body)
}

func summaryJSON(event Event) string {
	if raw := eventAttr(event, "summary_json"); raw != "" && json.Valid([]byte(raw)) {
		return raw
	}
	summary := map[string]string{}
	for key, value := range event.Attributes {
		if strings.HasPrefix(key, "summary.") {
			summary[strings.TrimPrefix(key, "summary.")] = value
		}
	}
	if len(summary) == 0 {
		return "{}"
	}
	body, err := json.Marshal(summary)
	if err != nil {
		return "{}"
	}
	return string(body)
}

func clickHouseTime(ts time.Time) string {
	if ts.IsZero() {
		ts = time.Now()
	}
	return ts.UTC().Format("2006-01-02 15:04:05.000")
}

func sanitizeClickHouseIdentifier(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		}
	}
	return b.String()
}
