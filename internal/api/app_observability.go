package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/observability"
)

const (
	appObservabilityDefaultWindow = time.Hour
	appObservabilityMaxWindow     = 24 * time.Hour
)

type appObservabilitySourceStatus struct {
	Available       bool     `json:"available"`
	Status          string   `json:"status"`
	Mode            string   `json:"mode"`
	Retention       string   `json:"retention"`
	ActiveExporters []string `json:"active_exporters"`
	Reason          string   `json:"reason"`
	Freshness       string   `json:"freshness,omitempty"`
}

type appObservabilityWindow struct {
	Since string `json:"since"`
	Until string `json:"until"`
}

type appObservabilityDiagnosis struct {
	Bottleneck  string   `json:"bottleneck"`
	Confidence  float64  `json:"confidence"`
	Evidence    []string `json:"evidence"`
	NextActions []string `json:"next_actions"`
}

func (s *Server) handleGetAppObservabilityMetricsSummary(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}
	window, ok := readAppObservabilityWindow(w, r)
	if !ok {
		return
	}
	source := s.appObservabilitySourceStatus("metrics", "metrics query backend is not wired yet")
	s.appendAudit(principal, "app.observability.metrics.read", "app", app.ID, app.TenantID, appObservabilityAuditMetadata(window))
	if source.Status != "disabled" {
		metrics, err := s.queryAppObservabilityMetricsSummary(r.Context(), app.ID, window)
		if err != nil {
			source.Status = "degraded"
			source.Available = false
			source.Reason = err.Error()
			httpx.WriteJSON(w, http.StatusOK, map[string]any{
				"source":  source,
				"window":  window,
				"metrics": []any{},
			})
			return
		}
		source.Status = "available"
		source.Available = true
		source.Reason = "metrics query backend returned data"
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"source":  source,
			"window":  window,
			"metrics": metrics,
		})
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"source":  source,
		"window":  window,
		"metrics": []any{},
	})
}

func (s *Server) handleQueryAppObservabilityLogs(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}
	window, ok := readAppObservabilityWindow(w, r)
	if !ok {
		return
	}
	source := s.appObservabilitySourceStatus("logs", "logs query backend is not wired yet")
	s.appendAudit(principal, "app.observability.logs.query", "app", app.ID, app.TenantID, appObservabilityAuditMetadata(window))
	if source.Status != "disabled" && observabilityExporterActive(source.ActiveExporters, "logs") {
		logs, err := s.queryAppObservabilityLogs(r.Context(), app.ID, window, r.URL.Query())
		if err != nil {
			source.Status = "degraded"
			source.Available = false
			source.Reason = err.Error()
			httpx.WriteJSON(w, http.StatusOK, map[string]any{
				"source": source,
				"window": window,
				"logs":   []any{},
			})
			return
		}
		source.Status = "available"
		source.Available = true
		source.Reason = "logs query backend returned data"
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"source": source,
			"window": window,
			"logs":   logs,
		})
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"source": source,
		"window": window,
		"logs":   []any{},
	})
}

func (s *Server) handleListAppObservabilityRequests(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}
	window, ok := readAppObservabilityWindow(w, r)
	if !ok {
		return
	}
	source := s.appObservabilitySourceStatus("analytics", "request analytics query backend is not wired yet")
	s.appendAudit(principal, "app.observability.requests.list", "app", app.ID, app.TenantID, appObservabilityAuditMetadata(window))
	if source.Status != "disabled" && observabilityExporterActive(source.ActiveExporters, "analytics") {
		requests, err := s.queryAppObservabilityRequests(r.Context(), app.ID, window, r.URL.Query())
		if err != nil {
			source.Status = "degraded"
			source.Available = false
			source.Reason = err.Error()
			httpx.WriteJSON(w, http.StatusOK, map[string]any{
				"source":   source,
				"window":   window,
				"requests": []any{},
			})
			return
		}
		source.Status = "available"
		source.Available = true
		source.Reason = "request analytics query backend returned data"
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"source":   source,
			"window":   window,
			"requests": requests,
		})
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"source":   source,
		"window":   window,
		"requests": []any{},
	})
}

func (s *Server) handleGetAppObservabilityTrace(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}
	traceID := strings.TrimSpace(r.PathValue("trace_id"))
	if traceID == "" {
		httpx.WriteError(w, http.StatusBadRequest, "trace_id is required")
		return
	}
	source := s.appObservabilitySourceStatus("analytics", "trace query backend is not wired yet")
	s.appendAudit(principal, "app.observability.trace.read", "app", app.ID, app.TenantID, map[string]string{"trace_id_present": "true"})
	if source.Status != "disabled" && observabilityExporterActive(source.ActiveExporters, "analytics") {
		spans, err := s.queryAppObservabilityTrace(r.Context(), app.ID, traceID)
		if err != nil {
			source.Status = "degraded"
			source.Available = false
			source.Reason = err.Error()
			httpx.WriteJSON(w, http.StatusOK, map[string]any{
				"source":   source,
				"trace_id": traceID,
				"spans":    []any{},
			})
			return
		}
		source.Status = "available"
		source.Available = true
		source.Reason = "trace query backend returned data"
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"source":   source,
			"trace_id": traceID,
			"spans":    spans,
		})
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"source":   source,
		"trace_id": traceID,
		"spans":    []any{},
	})
}

func (s *Server) handleGetAppObservabilityDiagnosis(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}
	window, ok := readAppObservabilityWindow(w, r)
	if !ok {
		return
	}
	source := s.appObservabilitySourceStatus("analytics", "diagnosis query backend is not wired yet")
	diagnosis := appObservabilityUnavailableDiagnosis(source.Reason)
	s.appendAudit(principal, "app.observability.diagnosis.read", "app", app.ID, app.TenantID, appObservabilityAuditMetadata(window))
	if source.Status != "disabled" && observabilityExporterActive(source.ActiveExporters, "analytics") {
		resolvedDiagnosis, err := s.queryAppObservabilityDiagnosis(r.Context(), app.ID, window)
		if err != nil {
			source.Status = "degraded"
			source.Available = false
			source.Reason = err.Error()
			diagnosis = appObservabilityUnavailableDiagnosis(source.Reason)
		} else {
			source.Status = "available"
			source.Available = true
			source.Reason = "diagnosis query backend returned data"
			diagnosis = resolvedDiagnosis
		}
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"source":    source,
		"window":    window,
		"diagnosis": diagnosis,
	})
}

func (s *Server) appObservabilitySourceStatus(requiredExporter string, queryPendingReason string) appObservabilitySourceStatus {
	cfg := s.observabilityConfig.Normalize()
	status := cfg.Status()
	activeExporters := append([]string{}, status.Exporters...)
	source := appObservabilitySourceStatus{
		Available:       false,
		Status:          "disabled",
		Mode:            status.Mode,
		Retention:       status.Retention,
		ActiveExporters: activeExporters,
		Reason:          "observability is disabled",
	}
	if !cfg.Enabled {
		return source
	}
	source.Status = "degraded"
	if err := cfg.Validate(); err != nil {
		source.Reason = err.Error()
		return source
	}
	if requiredExporter != "" && !observabilityExporterActive(status.Exporters, requiredExporter) {
		source.Reason = fmt.Sprintf("%s exporter is not active", requiredExporter)
		return source
	}
	source.Reason = queryPendingReason
	return source
}

func observabilityExporterActive(exporters []string, want string) bool {
	for _, exporter := range exporters {
		if strings.EqualFold(strings.TrimSpace(exporter), strings.TrimSpace(want)) {
			return true
		}
	}
	return false
}

func readAppObservabilityWindow(w http.ResponseWriter, r *http.Request) (appObservabilityWindow, bool) {
	until := time.Now().UTC()
	if rawUntil := strings.TrimSpace(r.URL.Query().Get("until")); rawUntil != "" {
		parsed, err := time.Parse(time.RFC3339, rawUntil)
		if err != nil {
			httpx.WriteError(w, http.StatusBadRequest, "until must be an RFC3339 timestamp")
			return appObservabilityWindow{}, false
		}
		until = parsed.UTC()
	}

	since := until.Add(-appObservabilityDefaultWindow)
	if rawSince := strings.TrimSpace(r.URL.Query().Get("since")); rawSince != "" {
		parsed, err := parseAppObservabilitySince(rawSince, until)
		if err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return appObservabilityWindow{}, false
		}
		since = parsed.UTC()
	}
	if since.After(until) {
		httpx.WriteError(w, http.StatusBadRequest, "since must be before until")
		return appObservabilityWindow{}, false
	}
	if until.Sub(since) > appObservabilityMaxWindow {
		httpx.WriteError(w, http.StatusBadRequest, "observability query window cannot exceed 24h")
		return appObservabilityWindow{}, false
	}
	return appObservabilityWindow{
		Since: since.Format(time.RFC3339),
		Until: until.Format(time.RFC3339),
	}, true
}

func parseAppObservabilitySince(raw string, until time.Time) (time.Time, error) {
	if duration, err := time.ParseDuration(raw); err == nil {
		if duration < 0 {
			return time.Time{}, fmt.Errorf("since duration must be positive")
		}
		return until.Add(-duration), nil
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("since must be a duration or RFC3339 timestamp")
	}
	return parsed, nil
}

func appObservabilityAuditMetadata(window appObservabilityWindow) map[string]string {
	return map[string]string{
		"since": window.Since,
		"until": window.Until,
	}
}

func appObservabilityUnavailableDiagnosis(reason string) appObservabilityDiagnosis {
	return appObservabilityDiagnosis{
		Bottleneck: "unavailable",
		Confidence: 0,
		Evidence: []string{
			firstNonEmpty(reason, "observability diagnosis data is unavailable"),
		},
		NextActions: []string{
			"enable and verify Fugue Observability query backends before relying on automated bottleneck diagnosis",
		},
	}
}

type lokiQueryRangeResponse struct {
	Status string `json:"status"`
	Data   struct {
		Result []struct {
			Stream map[string]string `json:"stream"`
			Values [][2]string       `json:"values"`
		} `json:"result"`
	} `json:"data"`
	Error string `json:"error,omitempty"`
}

type prometheusQueryResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Value  []any             `json:"value"`
		} `json:"result"`
	} `json:"data"`
	Error string `json:"error,omitempty"`
}

type appObservabilityMetricQuery struct {
	Name  string
	Unit  string
	Query string
}

func (s *Server) queryAppObservabilityMetricsSummary(ctx context.Context, appID string, window appObservabilityWindow) ([]map[string]any, error) {
	cfg := s.observabilityConfig.Normalize()
	endpoint, err := normalizePrometheusQueryURL(cfg.MetricsQueryURL)
	if err != nil {
		return nil, err
	}
	_, until, err := parseAppObservabilityWindowTimes(window)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: cfg.ExportTimeout}
	metrics := []map[string]any{}
	for _, item := range buildAppObservabilityMetricQueries(appID, window) {
		samples, err := queryPrometheusInstant(ctx, client, endpoint, item, until, cfg.MaxPayloadBytes)
		if err != nil {
			return nil, err
		}
		metrics = append(metrics, samples...)
	}
	return metrics, nil
}

func normalizePrometheusQueryURL(raw string) (*url.URL, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("metrics query URL is not configured")
	}
	cleanPath := strings.TrimRight(parsed.Path, "/")
	switch {
	case strings.HasSuffix(cleanPath, "/api/v1/query"):
	case strings.HasSuffix(cleanPath, "/api/v1/query_range"):
		cleanPath = strings.TrimSuffix(cleanPath, "/query_range") + "/query"
	case strings.HasSuffix(cleanPath, "/api/v1/write"):
		cleanPath = strings.TrimSuffix(cleanPath, "/write") + "/query"
	default:
		cleanPath = strings.TrimRight(cleanPath, "/") + "/api/v1/query"
	}
	parsed.Path = cleanPath
	parsed.RawQuery = ""
	return parsed, nil
}

func buildAppObservabilityMetricQueries(appID string, window appObservabilityWindow) []appObservabilityMetricQuery {
	selector := `{app_id="` + escapePromQLString(appID) + `"}`
	errorSelector := `{app_id="` + escapePromQLString(appID) + `",status_class=~"4xx|5xx"}`
	rangeSelector := prometheusRangeSelector(window)
	return []appObservabilityMetricQuery{
		{
			Name:  "rpm",
			Unit:  "rpm",
			Query: "sum(rate(fugue_app_requests_total" + selector + "[" + rangeSelector + "])) * 60",
		},
		{
			Name:  "error_rate",
			Unit:  "ratio",
			Query: "sum(rate(fugue_app_response_status_total" + errorSelector + "[" + rangeSelector + "])) / sum(rate(fugue_app_requests_total" + selector + "[" + rangeSelector + "]))",
		},
		{
			Name:  "p95_ttfb_ms",
			Unit:  "ms",
			Query: "histogram_quantile(0.95, sum(rate(fugue_app_ttfb_seconds_bucket" + selector + "[" + rangeSelector + "])) by (le)) * 1000",
		},
		{
			Name:  "p95_duration_ms",
			Unit:  "ms",
			Query: "histogram_quantile(0.95, sum(rate(fugue_app_duration_seconds_bucket" + selector + "[" + rangeSelector + "])) by (le)) * 1000",
		},
	}
}

func prometheusRangeSelector(window appObservabilityWindow) string {
	since, until, err := parseAppObservabilityWindowTimes(window)
	if err != nil {
		return "300s"
	}
	seconds := int(until.Sub(since).Seconds())
	if seconds < 60 {
		seconds = 60
	}
	if seconds > int(appObservabilityMaxWindow.Seconds()) {
		seconds = int(appObservabilityMaxWindow.Seconds())
	}
	return fmt.Sprintf("%ds", seconds)
}

func queryPrometheusInstant(ctx context.Context, client *http.Client, endpoint *url.URL, metricQuery appObservabilityMetricQuery, at time.Time, maxPayloadBytes int64) ([]map[string]any, error) {
	queryURL := *endpoint
	values := queryURL.Query()
	values.Set("query", metricQuery.Query)
	values.Set("time", strconv.FormatInt(at.UTC().Unix(), 10))
	queryURL.RawQuery = values.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, queryURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build Prometheus query request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("query Prometheus metrics: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxPayloadBytes))
	if err != nil {
		return nil, fmt.Errorf("read Prometheus query response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("query Prometheus metrics returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	var payload prometheusQueryResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("decode Prometheus query response: %w", err)
	}
	if payload.Status != "" && payload.Status != "success" {
		return nil, fmt.Errorf("query Prometheus metrics failed: %s", firstNonEmpty(payload.Error, payload.Status))
	}
	return prometheusMetricSamples(metricQuery, payload), nil
}

func prometheusMetricSamples(metricQuery appObservabilityMetricQuery, payload prometheusQueryResponse) []map[string]any {
	samples := []map[string]any{}
	for _, result := range payload.Data.Result {
		if len(result.Value) < 2 {
			continue
		}
		rawValue, ok := result.Value[1].(string)
		if !ok {
			continue
		}
		value, err := strconv.ParseFloat(rawValue, 64)
		if err != nil {
			continue
		}
		labels := map[string]string{}
		for key, label := range result.Metric {
			if key == "__name__" || key == "app_id" {
				continue
			}
			labels[key] = label
		}
		sample := map[string]any{
			"name":  metricQuery.Name,
			"value": value,
			"unit":  metricQuery.Unit,
		}
		if len(labels) > 0 {
			sample["labels"] = labels
		}
		samples = append(samples, sample)
	}
	return samples
}

func escapePromQLString(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	return value
}

func (s *Server) queryAppObservabilityLogs(ctx context.Context, appID string, window appObservabilityWindow, query url.Values) ([]map[string]any, error) {
	cfg := s.observabilityConfig.Normalize()
	endpoint, err := normalizeLokiQueryRangeURL(cfg.LokiURL)
	if err != nil {
		return nil, err
	}
	since, until, err := parseAppObservabilityWindowTimes(window)
	if err != nil {
		return nil, err
	}
	limit := boundedAppObservabilityLimit(query.Get("limit"), 200, 1000)
	values := url.Values{}
	values.Set("query", buildAppObservabilityLokiQuery(appID, query))
	values.Set("start", strconv.FormatInt(since.UnixNano(), 10))
	values.Set("end", strconv.FormatInt(until.UnixNano(), 10))
	values.Set("limit", strconv.Itoa(limit))
	endpoint.RawQuery = values.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build Loki query request: %w", err)
	}
	client := &http.Client{Timeout: cfg.ExportTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("query Loki logs: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, cfg.MaxPayloadBytes))
	if err != nil {
		return nil, fmt.Errorf("read Loki logs response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("query Loki logs returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	var payload lokiQueryRangeResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("decode Loki logs response: %w", err)
	}
	if payload.Status != "" && payload.Status != "success" {
		return nil, fmt.Errorf("query Loki logs failed: %s", firstNonEmpty(payload.Error, payload.Status))
	}
	logs := make([]map[string]any, 0, limit)
	for _, stream := range payload.Data.Result {
		for _, value := range stream.Values {
			entry := lokiValueToAppObservabilityLog(stream.Stream, value)
			logs = append(logs, entry)
			if len(logs) >= limit {
				return logs, nil
			}
		}
	}
	return logs, nil
}

func parseAppObservabilityWindowTimes(window appObservabilityWindow) (time.Time, time.Time, error) {
	since, err := time.Parse(time.RFC3339, strings.TrimSpace(window.Since))
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid observability window since: %w", err)
	}
	until, err := time.Parse(time.RFC3339, strings.TrimSpace(window.Until))
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid observability window until: %w", err)
	}
	return since.UTC(), until.UTC(), nil
}

func boundedAppObservabilityLimit(raw string, defaultLimit, maxLimit int) int {
	limit := defaultLimit
	if parsed, err := strconv.Atoi(strings.TrimSpace(raw)); err == nil && parsed > 0 {
		limit = parsed
	}
	if limit > maxLimit {
		return maxLimit
	}
	return limit
}

func normalizeLokiQueryRangeURL(raw string) (*url.URL, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("invalid Loki URL")
	}
	cleanPath := strings.TrimRight(parsed.Path, "/")
	if !strings.HasSuffix(cleanPath, "/loki/api/v1/query_range") {
		if strings.HasSuffix(cleanPath, "/loki/api/v1/push") {
			cleanPath = strings.TrimSuffix(cleanPath, "/push") + "/query_range"
		} else {
			cleanPath = strings.TrimRight(cleanPath, "/") + "/loki/api/v1/query_range"
		}
	}
	parsed.Path = cleanPath
	parsed.RawQuery = ""
	return parsed, nil
}

func buildAppObservabilityLokiQuery(appID string, query url.Values) string {
	labels := []string{`app_id="` + escapeLogQLString(appID) + `"`}
	if level := strings.TrimSpace(query.Get("level")); level != "" {
		labels = append(labels, `level="`+escapeLogQLString(level)+`"`)
	}
	parts := []string{"{" + strings.Join(labels, ",") + "}"}
	for _, key := range []string{"grep", "trace_id"} {
		if value := strings.TrimSpace(query.Get(key)); value != "" {
			parts = append(parts, `|= "`+escapeLogQLString(value)+`"`)
		}
	}
	return strings.Join(parts, " ")
}

func escapeLogQLString(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return value
}

func lokiValueToAppObservabilityLog(labels map[string]string, value [2]string) map[string]any {
	entry := map[string]any{
		"timestamp": formatLokiTimestamp(value[0]),
		"message":   value[1],
	}
	attributes := map[string]any{}
	for key, label := range labels {
		switch key {
		case "pod", "container", "level":
			entry[key] = label
		default:
			attributes[key] = label
		}
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(value[1]), &parsed); err == nil {
		for key, parsedValue := range parsed {
			switch key {
			case "timestamp", "pod", "container", "level", "trace_id", "message":
				entry[key] = parsedValue
			case "attributes":
				if parsedAttributes, ok := parsedValue.(map[string]any); ok {
					for attrKey, attrValue := range parsedAttributes {
						attributes[attrKey] = attrValue
					}
				} else {
					attributes[key] = parsedValue
				}
			default:
				attributes[key] = parsedValue
			}
		}
	}
	if len(attributes) > 0 {
		entry["attributes"] = attributes
	}
	return entry
}

func formatLokiTimestamp(raw string) string {
	ns, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return strings.TrimSpace(raw)
	}
	return time.Unix(0, ns).UTC().Format(time.RFC3339Nano)
}

func (s *Server) queryAppObservabilityRequests(ctx context.Context, appID string, window appObservabilityWindow, query url.Values) ([]map[string]any, error) {
	queryText, err := buildAppObservabilityRequestsQuery(appID, window, query)
	if err != nil {
		return nil, err
	}
	rows, err := s.queryAppObservabilityClickHouse(ctx, queryText)
	if err != nil {
		return nil, err
	}
	requests := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		requests = append(requests, appObservabilityRequestFromClickHouseRow(row))
	}
	return requests, nil
}

func (s *Server) queryAppObservabilityTrace(ctx context.Context, appID string, traceID string) ([]map[string]any, error) {
	until := time.Now().UTC()
	window := appObservabilityWindow{
		Since: until.Add(-appObservabilityMaxWindow).Format(time.RFC3339),
		Until: until.Format(time.RFC3339),
	}
	queryText, err := buildAppObservabilityTraceQuery(appID, traceID, window)
	if err != nil {
		return nil, err
	}
	rows, err := s.queryAppObservabilityClickHouse(ctx, queryText)
	if err != nil {
		return nil, err
	}
	spans := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		spans = append(spans, appObservabilitySpanFromClickHouseRow(row))
	}
	return spans, nil
}

func (s *Server) queryAppObservabilityClickHouse(ctx context.Context, queryText string) ([]map[string]any, error) {
	cfg := s.observabilityConfig.Normalize()
	client := &http.Client{Timeout: cfg.ExportTimeout}
	exporter := observability.NewClickHouseExporter(cfg.ClickHouseDSN, client)
	return exporter.QueryJSONEachRow(ctx, queryText, cfg.MaxPayloadBytes)
}

func (s *Server) queryAppObservabilityDiagnosis(ctx context.Context, appID string, window appObservabilityWindow) (appObservabilityDiagnosis, error) {
	queryText, err := buildAppObservabilityDiagnosisQuery(appID, window)
	if err != nil {
		return appObservabilityDiagnosis{}, err
	}
	rows, err := s.queryAppObservabilityClickHouse(ctx, queryText)
	if err != nil {
		return appObservabilityDiagnosis{}, err
	}
	if len(rows) == 0 {
		return appObservabilityUnavailableDiagnosis("no diagnosis window rows in the selected range"), nil
	}
	return appObservabilityDiagnosisFromClickHouseRow(rows[0]), nil
}

func buildAppObservabilityRequestsQuery(appID string, window appObservabilityWindow, query url.Values) (string, error) {
	since, until, err := parseAppObservabilityWindowTimes(window)
	if err != nil {
		return "", err
	}
	conditions := []string{
		"app_id = " + quoteClickHouseString(appID),
		"ts >= " + clickHouseDateTime64Literal(since),
		"ts <= " + clickHouseDateTime64Literal(until),
	}
	if traceID := strings.TrimSpace(query.Get("trace_id")); traceID != "" {
		conditions = append(conditions, "trace_id = "+quoteClickHouseString(traceID))
	}
	if requestID := strings.TrimSpace(query.Get("request_id")); requestID != "" {
		conditions = append(conditions, "request_id = "+quoteClickHouseString(requestID))
	}
	if statusClass := strings.TrimSpace(query.Get("status_class")); statusClass != "" {
		if !validStatusClass(statusClass) {
			return "", fmt.Errorf("status_class must look like 2xx, 4xx, or 5xx")
		}
		conditions = append(conditions, "status_class = "+quoteClickHouseString(statusClass))
	}
	if statusCode := strings.TrimSpace(query.Get("status_code")); statusCode != "" {
		parsed, err := strconv.Atoi(statusCode)
		if err != nil || parsed < 100 || parsed > 599 {
			return "", fmt.Errorf("status_code must be between 100 and 599")
		}
		conditions = append(conditions, fmt.Sprintf("status_code = %d", parsed))
	}
	if strings.EqualFold(strings.TrimSpace(query.Get("errors")), "true") {
		conditions = append(conditions, "(status_code >= 400 OR error_type != '')")
	}
	if strings.EqualFold(strings.TrimSpace(query.Get("slow")), "true") {
		conditions = append(conditions, "duration_ms >= 1000")
	}
	limit := boundedAppObservabilityLimit(query.Get("limit"), 200, 1000)
	return "SELECT ts, trace_id, request_id, route_id, hostname, path_template, method, status_code, status_class, duration_ms, ttfb_ms, summary_json " +
		"FROM request_facts WHERE " + strings.Join(conditions, " AND ") +
		fmt.Sprintf(" ORDER BY ts DESC LIMIT %d FORMAT JSONEachRow", limit), nil
}

func buildAppObservabilityTraceQuery(appID string, traceID string, window appObservabilityWindow) (string, error) {
	traceID = strings.TrimSpace(traceID)
	if traceID == "" {
		return "", fmt.Errorf("trace_id is required")
	}
	since, until, err := parseAppObservabilityWindowTimes(window)
	if err != nil {
		return "", err
	}
	conditions := []string{
		"app_id = " + quoteClickHouseString(appID),
		"trace_id = " + quoteClickHouseString(traceID),
		"ts >= " + clickHouseDateTime64Literal(since),
		"ts <= " + clickHouseDateTime64Literal(until),
	}
	return "SELECT ts, service, trace_id, span_id, parent_span_id, request_id, stage, stage_ms, status_code, error_type, attributes_json " +
		"FROM request_spans WHERE " + strings.Join(conditions, " AND ") +
		" ORDER BY ts ASC, stage_ms ASC LIMIT 1000 FORMAT JSONEachRow", nil
}

func buildAppObservabilityDiagnosisQuery(appID string, window appObservabilityWindow) (string, error) {
	since, until, err := parseAppObservabilityWindowTimes(window)
	if err != nil {
		return "", err
	}
	conditions := []string{
		"app_id = " + quoteClickHouseString(appID),
		"minute >= " + clickHouseDateTimeLiteral(since),
		"minute <= " + clickHouseDateTimeLiteral(until),
	}
	return "SELECT minute, rpm, p50_ttfb_ms, p95_ttfb_ms, p99_ttfb_ms, p50_duration_ms, p95_duration_ms, p99_duration_ms, error_rate, top_bottleneck_stage, top_bottleneck_confidence " +
		"FROM diagnosis_windows_1m WHERE " + strings.Join(conditions, " AND ") +
		" ORDER BY minute DESC LIMIT 1 FORMAT JSONEachRow", nil
}

func validStatusClass(value string) bool {
	if len(value) != 3 || value[1:] != "xx" {
		return false
	}
	return value[0] >= '1' && value[0] <= '5'
}

func quoteClickHouseString(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `'`, `\'`)
	return "'" + value + "'"
}

func clickHouseDateTime64Literal(value time.Time) string {
	return "parseDateTime64BestEffort(" + quoteClickHouseString(value.UTC().Format(time.RFC3339Nano)) + ")"
}

func clickHouseDateTimeLiteral(value time.Time) string {
	return "parseDateTimeBestEffort(" + quoteClickHouseString(value.UTC().Format(time.RFC3339Nano)) + ")"
}

func appObservabilityRequestFromClickHouseRow(row map[string]any) map[string]any {
	return map[string]any{
		"timestamp":   stringField(row, "ts"),
		"trace_id":    stringField(row, "trace_id"),
		"request_id":  stringField(row, "request_id"),
		"route":       firstNonEmpty(stringField(row, "path_template"), stringField(row, "route_id"), stringField(row, "hostname")),
		"method":      stringField(row, "method"),
		"status_code": row["status_code"],
		"duration_ms": row["duration_ms"],
		"ttft_ms":     row["ttfb_ms"],
		"summary":     parseJSONMapField(row["summary_json"]),
	}
}

func appObservabilitySpanFromClickHouseRow(row map[string]any) map[string]any {
	return map[string]any{
		"timestamp":      stringField(row, "ts"),
		"service":        stringField(row, "service"),
		"trace_id":       stringField(row, "trace_id"),
		"span_id":        stringField(row, "span_id"),
		"parent_span_id": stringField(row, "parent_span_id"),
		"request_id":     stringField(row, "request_id"),
		"stage":          stringField(row, "stage"),
		"stage_ms":       row["stage_ms"],
		"status_code":    row["status_code"],
		"error_type":     stringField(row, "error_type"),
		"attributes":     parseJSONMapField(row["attributes_json"]),
	}
}

func appObservabilityDiagnosisFromClickHouseRow(row map[string]any) appObservabilityDiagnosis {
	bottleneck := firstNonEmpty(stringField(row, "top_bottleneck_stage"), "unavailable")
	confidence := floatField(row, "top_bottleneck_confidence")
	evidence := []string{}
	for _, item := range []struct {
		key   string
		label string
		unit  string
	}{
		{key: "rpm", label: "rpm"},
		{key: "p95_ttfb_ms", label: "p95_ttfb_ms", unit: "ms"},
		{key: "p95_duration_ms", label: "p95_duration_ms", unit: "ms"},
		{key: "error_rate", label: "error_rate"},
	} {
		if value, ok := optionalFloatField(row, item.key); ok {
			evidence = append(evidence, formatAppObservabilityEvidence(item.label, value, item.unit))
		}
	}
	if len(evidence) == 0 {
		evidence = append(evidence, "diagnosis window row did not contain aggregate evidence")
	}
	nextActions := []string{
		"inspect app requests, traces, logs, and runtime metrics for the same window before changing capacity",
	}
	if bottleneck == "unavailable" {
		nextActions = []string{
			"verify diagnosis window aggregation and app telemetry ingestion before changing capacity",
		}
	}
	return appObservabilityDiagnosis{
		Bottleneck:  bottleneck,
		Confidence:  confidence,
		Evidence:    evidence,
		NextActions: nextActions,
	}
}

func stringField(row map[string]any, key string) string {
	value, ok := row[key]
	if !ok || value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	default:
		return fmt.Sprint(typed)
	}
}

func floatField(row map[string]any, key string) float64 {
	value, _ := optionalFloatField(row, key)
	return value
}

func optionalFloatField(row map[string]any, key string) (float64, bool) {
	value, ok := row[key]
	if !ok || value == nil {
		return 0, false
	}
	switch typed := value.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case json.Number:
		parsed, err := typed.Float64()
		return parsed, err == nil
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}

func formatAppObservabilityEvidence(label string, value float64, unit string) string {
	if unit == "" {
		return fmt.Sprintf("%s=%.4g", label, value)
	}
	return fmt.Sprintf("%s=%.4g%s", label, value, unit)
}

func parseJSONMapField(value any) map[string]any {
	raw, ok := value.(string)
	if !ok || strings.TrimSpace(raw) == "" {
		return map[string]any{}
	}
	parsed := map[string]any{}
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return map[string]any{}
	}
	return parsed
}
