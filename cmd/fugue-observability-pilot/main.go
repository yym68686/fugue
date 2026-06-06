package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

type pilotOptions struct {
	Endpoint  string
	TenantID  string
	ProjectID string
	AppID     string
	RuntimeID string
	Service   string
	Scenario  string
	Count     int
	DryRun    bool
}

type pilotEvent struct {
	Timestamp  string         `json:"timestamp"`
	Kind       string         `json:"kind,omitempty"`
	Source     string         `json:"source,omitempty"`
	Message    string         `json:"message,omitempty"`
	EventType  string         `json:"event_type,omitempty"`
	Attributes map[string]any `json:"attributes,omitempty"`
	Summary    map[string]any `json:"summary,omitempty"`
	Metric     string         `json:"metric,omitempty"`
	Value      float64        `json:"value,omitempty"`
}

func main() {
	opts := pilotOptions{}
	flag.StringVar(&opts.Endpoint, "endpoint", "", "Telemetry agent HTTP endpoint, for example http://fugue-fugue-telemetry-agent:7834")
	flag.StringVar(&opts.TenantID, "tenant-id", "tenant_observability_pilot", "Synthetic tenant id")
	flag.StringVar(&opts.ProjectID, "project-id", "project_observability_pilot", "Synthetic project id")
	flag.StringVar(&opts.AppID, "app-id", "app_observability_pilot", "Synthetic app id or selected internal sample app id")
	flag.StringVar(&opts.RuntimeID, "runtime-id", "runtime_observability_pilot", "Synthetic runtime id")
	flag.StringVar(&opts.Service, "service", "observability-pilot", "Synthetic service name")
	flag.StringVar(&opts.Scenario, "scenario", "pool-wait", "Pilot scenario: normal, pool-wait, error-burst")
	flag.IntVar(&opts.Count, "count", 12, "Number of synthetic request summaries to emit")
	flag.BoolVar(&opts.DryRun, "dry-run", false, "Print payloads without sending")
	flag.Parse()

	if opts.Count < 1 {
		opts.Count = 1
	}
	if strings.TrimSpace(opts.Endpoint) == "" && !opts.DryRun {
		fmt.Fprintln(os.Stderr, "--endpoint is required unless --dry-run is set")
		os.Exit(2)
	}
	report, err := runPilot(context.Background(), opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "observability pilot failed: %v\n", err)
		os.Exit(1)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(report)
}

func runPilot(ctx context.Context, opts pilotOptions) (map[string]any, error) {
	now := time.Now().UTC()
	traceID := randomHex(16)
	if traceID == "" {
		traceID = fmt.Sprintf("%032x", now.UnixNano())
	}
	logPayload := map[string]any{"events": pilotLogEvents(opts, traceID, now)}
	metricPayload := map[string]any{"events": pilotMetricEvents(opts, now)}
	tracePayload := pilotTracePayload(opts, traceID, now)
	if opts.DryRun {
		return map[string]any{
			"dry_run":        true,
			"app_id":         opts.AppID,
			"trace_id":       traceID,
			"log_payload":    logPayload,
			"metric_payload": metricPayload,
			"trace_payload":  tracePayload,
		}, nil
	}
	endpoint := strings.TrimRight(strings.TrimSpace(opts.Endpoint), "/")
	client := &http.Client{Timeout: 10 * time.Second}
	if err := postJSON(ctx, client, endpoint+"/v1/logs", logPayload); err != nil {
		return nil, err
	}
	if err := postJSON(ctx, client, endpoint+"/v1/metrics", metricPayload); err != nil {
		return nil, err
	}
	if err := postJSON(ctx, client, endpoint+"/v1/traces", tracePayload); err != nil {
		return nil, err
	}
	return map[string]any{
		"status":          "sent",
		"app_id":          opts.AppID,
		"trace_id":        traceID,
		"request_events":  opts.Count,
		"metric_events":   len(pilotMetricEvents(opts, now)),
		"span_events":     5,
		"verify_requests": fmt.Sprintf("fugue app requests %s --trace %s --since 15m", opts.AppID, traceID),
		"verify_trace":    fmt.Sprintf("fugue app traces %s %s", opts.AppID, traceID),
		"verify_report":   fmt.Sprintf("fugue app diagnose %s --since 15m", opts.AppID),
	}, nil
}

func pilotLogEvents(opts pilotOptions, traceID string, now time.Time) []pilotEvent {
	events := make([]pilotEvent, 0, opts.Count+1)
	for i := 0; i < opts.Count; i++ {
		duration := 80 + i*7
		ttfb := 24 + i
		status := 200
		statusClass := "2xx"
		if opts.Scenario == "error-burst" && i%3 == 0 {
			status = 503
			statusClass = "5xx"
			duration = 900
			ttfb = 400
		}
		if opts.Scenario == "pool-wait" && i >= opts.Count/2 {
			duration = 1200 + i*25
			ttfb = 850 + i*10
		}
		streaming := i%2 == 0
		events = append(events, pilotEvent{
			Timestamp: now.Add(time.Duration(i) * time.Millisecond).Format(time.RFC3339Nano),
			Kind:      "log",
			Source:    opts.Service,
			Message:   "pilot request summary",
			EventType: "request_summary",
			Attributes: baseAttributes(opts, map[string]any{
				"trace_id":      traceID,
				"request_id":    fmt.Sprintf("pilot_req_%03d", i+1),
				"route_id":      "pilot-route",
				"path_template": "/pilot/{mode}",
				"method":        "GET",
				"status_code":   status,
				"status_class":  statusClass,
				"duration_ms":   duration,
				"ttfb_ms":       ttfb,
				"upstream_ms":   duration - ttfb,
				"streaming":     streaming,
			}),
			Summary: map[string]any{
				"scenario":   opts.Scenario,
				"request":    "http",
				"dependency": "managed-service",
				"mode":       map[bool]string{true: "streaming", false: "http"}[streaming],
			},
		})
	}
	events = append(events, pilotEvent{
		Timestamp:  now.Format(time.RFC3339Nano),
		Kind:       "log",
		Source:     opts.Service,
		Message:    "pilot background worker checkpoint",
		EventType:  "app_event",
		Attributes: baseAttributes(opts, map[string]any{"severity": "info", "event_type": "app_event", "worker": "pilot-worker"}),
	})
	return events
}

func pilotMetricEvents(opts pilotOptions, now time.Time) []pilotEvent {
	return []pilotEvent{
		metricEvent(opts, now, "app_inflight_requests", 3, map[string]any{"component": opts.Service}),
		metricEvent(opts, now, "app_upstream_pool_wait_ms", scenarioValue(opts.Scenario, 18, 620, 90), map[string]any{"component": opts.Service}),
		metricEvent(opts, now, "app_db_pool_wait_ms", scenarioValue(opts.Scenario, 4, 740, 7), map[string]any{"component": opts.Service}),
		metricEvent(opts, now, "app_event_loop_lag_ms", scenarioValue(opts.Scenario, 2, 3, 2), map[string]any{"component": opts.Service}),
	}
}

func metricEvent(opts pilotOptions, now time.Time, name string, value float64, attrs map[string]any) pilotEvent {
	return pilotEvent{
		Timestamp:  now.Format(time.RFC3339Nano),
		Kind:       "metric",
		Source:     opts.Service,
		Message:    name,
		Metric:     name,
		Value:      value,
		Attributes: baseAttributes(opts, attrs),
	}
}

func scenarioValue(scenario string, normal float64, poolWait float64, errorBurst float64) float64 {
	switch scenario {
	case "pool-wait":
		return poolWait
	case "error-burst":
		return errorBurst
	default:
		return normal
	}
}

func pilotTracePayload(opts pilotOptions, traceID string, now time.Time) map[string]any {
	spans := []map[string]any{}
	parent := ""
	stages := []struct {
		Name string
		MS   int
	}{
		{Name: "request_received", MS: 2},
		{Name: "http_handler", MS: 18},
		{Name: "stream_first_chunk", MS: 35},
		{Name: "background_worker", MS: 47},
		{Name: "managed_db_query", MS: 24},
	}
	if opts.Scenario == "pool-wait" {
		stages = append(stages, struct {
			Name string
			MS   int
		}{Name: "db_pool_wait", MS: 840})
	}
	for index, stage := range stages {
		spanID := randomHex(8)
		start := now.Add(time.Duration(index*10) * time.Millisecond)
		span := map[string]any{
			"traceId":           traceID,
			"spanId":            spanID,
			"name":              stage.Name,
			"startTimeUnixNano": fmt.Sprintf("%d", start.UnixNano()),
			"endTimeUnixNano":   fmt.Sprintf("%d", start.Add(time.Duration(stage.MS)*time.Millisecond).UnixNano()),
			"attributes": []map[string]any{
				otlpStringAttr("fugue.request_id", "pilot_req_001"),
				otlpStringAttr("fugue.stage", stage.Name),
				otlpStringAttr("scenario", opts.Scenario),
				otlpIntAttr("http.response.status_code", 200),
			},
		}
		if parent != "" {
			span["parentSpanId"] = parent
		}
		parent = spanID
		spans = append(spans, span)
	}
	return map[string]any{
		"resourceSpans": []map[string]any{{
			"resource": map[string]any{"attributes": []map[string]any{
				otlpStringAttr("service.name", opts.Service),
				otlpStringAttr("fugue.tenant_id", opts.TenantID),
				otlpStringAttr("fugue.project_id", opts.ProjectID),
				otlpStringAttr("fugue.app_id", opts.AppID),
				otlpStringAttr("fugue.runtime_id", opts.RuntimeID),
			}},
			"scopeSpans": []map[string]any{{"spans": spans}},
		}},
	}
}

func baseAttributes(opts pilotOptions, attrs map[string]any) map[string]any {
	out := map[string]any{
		"tenant_id":  opts.TenantID,
		"project_id": opts.ProjectID,
		"app_id":     opts.AppID,
		"runtime_id": opts.RuntimeID,
		"component":  opts.Service,
	}
	for key, value := range attrs {
		out[key] = value
	}
	return out
}

func otlpStringAttr(key, value string) map[string]any {
	return map[string]any{"key": key, "value": map[string]any{"stringValue": value}}
}

func otlpIntAttr(key string, value int) map[string]any {
	return map[string]any{"key": key, "value": map[string]any{"intValue": fmt.Sprintf("%d", value)}}
}

func postJSON(ctx context.Context, client *http.Client, url string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		responseBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("POST %s returned %s: %s", url, resp.Status, strings.TrimSpace(string(responseBody)))
	}
	return nil
}

func randomHex(bytesLen int) string {
	buf := make([]byte, bytesLen)
	if _, err := rand.Read(buf); err != nil {
		return ""
	}
	return hex.EncodeToString(buf)
}
