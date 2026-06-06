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
	"fugue/internal/model"
	"fugue/internal/observability"
)

const (
	appObservabilityDefaultWindow              = time.Hour
	appObservabilityRequestStreamDefaultWindow = time.Minute
	appObservabilityRequestStreamPollInterval  = 2 * time.Second
	appObservabilityRequestStreamRetryMS       = 3000
	appObservabilityMaxWindow                  = 24 * time.Hour
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

type appObservabilityRequestStreamReadyEvent struct {
	Cursor string                       `json:"cursor"`
	Source appObservabilitySourceStatus `json:"source"`
	Window appObservabilityWindow       `json:"window"`
	Follow bool                         `json:"follow"`
}

type appObservabilityRequestStreamRequestEvent struct {
	Cursor  string         `json:"cursor"`
	Request map[string]any `json:"request"`
}

type appObservabilityRequestStreamWarningEvent struct {
	Cursor  string `json:"cursor"`
	Message string `json:"message"`
}

type appObservabilityRequestStreamEndEvent struct {
	Cursor string `json:"cursor"`
	Reason string `json:"reason"`
}

func (s *Server) handleGetAppObservabilityMetricsSummary(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principalCanReadAppObservability(principal) {
		httpx.WriteError(w, http.StatusForbidden, "missing app.observability.read scope")
		return
	}
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}
	window, ok := s.readAppObservabilityWindow(w, r, app.ID)
	if !ok {
		return
	}
	source := s.appObservabilitySourceStatus(app.ID, "metrics", "metrics query backend is not wired yet")
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

func (s *Server) handleQueryAppObservabilityMetrics(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principalCanReadAppObservability(principal) {
		httpx.WriteError(w, http.StatusForbidden, "missing app.observability.read scope")
		return
	}
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}
	window, ok := s.readAppObservabilityWindow(w, r, app.ID)
	if !ok {
		return
	}
	query := strings.TrimSpace(r.URL.Query().Get("query"))
	if query == "" {
		httpx.WriteError(w, http.StatusBadRequest, "query is required")
		return
	}
	source := s.appObservabilitySourceStatus(app.ID, "metrics", "metrics query backend is not wired yet")
	auditMetadata := appObservabilityAuditMetadata(window)
	auditMetadata["query_present"] = "true"
	s.appendAudit(principal, "app.observability.metrics.query", "app", app.ID, app.TenantID, auditMetadata)
	if source.Status != "disabled" {
		metrics, err := s.queryAppObservabilityMetrics(r.Context(), app.ID, window, query)
		if err != nil {
			source.Status = "degraded"
			source.Available = false
			source.Reason = err.Error()
			httpx.WriteJSON(w, http.StatusOK, map[string]any{
				"source":  source,
				"window":  window,
				"query":   query,
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
			"query":   query,
			"metrics": metrics,
		})
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"source":  source,
		"window":  window,
		"query":   query,
		"metrics": []any{},
	})
}

func (s *Server) handleQueryAppObservabilityLogs(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principalCanReadAppObservability(principal) {
		httpx.WriteError(w, http.StatusForbidden, "missing app.observability.read scope")
		return
	}
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}
	window, ok := s.readAppObservabilityWindow(w, r, app.ID)
	if !ok {
		return
	}
	source := s.appObservabilitySourceStatus(app.ID, "logs", "logs query backend is not wired yet")
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
	if !principalCanReadAppObservability(principal) {
		httpx.WriteError(w, http.StatusForbidden, "missing app.observability.read scope")
		return
	}
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}
	window, ok := s.readAppObservabilityWindow(w, r, app.ID)
	if !ok {
		return
	}
	source := s.appObservabilitySourceStatus(app.ID, "analytics", "request analytics query backend is not wired yet")
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

func (s *Server) handleStreamAppObservabilityRequests(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principalCanReadAppObservability(principal) {
		httpx.WriteError(w, http.StatusForbidden, "missing app.observability.read scope")
		return
	}
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}
	follow, err := parseFollowQuery(r.URL.Query().Get("follow"), true)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	window, ok := s.readAppObservabilityStreamWindow(w, r, follow, app.ID)
	if !ok {
		return
	}
	const queryPendingReason = "request analytics query backend is not wired yet"
	source := s.appObservabilitySourceStatus(app.ID, "analytics", queryPendingReason)
	if source.Status != "disabled" && source.Reason == queryPendingReason && observabilityExporterActive(source.ActiveExporters, "analytics") {
		source.Status = "available"
		source.Available = true
		source.Reason = "request analytics stream is available"
	}
	auditMetadata := appObservabilityAuditMetadata(window)
	auditMetadata["follow"] = strconv.FormatBool(follow)
	s.appendAudit(principal, "app.observability.requests.stream", "app", app.ID, app.TenantID, auditMetadata)

	stream, err := newSSEWriter(w)
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := stream.writeRetry(appObservabilityRequestStreamRetryMS); err != nil {
		return
	}
	cursor := appObservabilityStreamCursorFromWindow(window)
	if err := stream.writeEvent("ready", cursor, appObservabilityRequestStreamReadyEvent{
		Cursor: cursor,
		Source: source,
		Window: window,
		Follow: follow,
	}); err != nil {
		return
	}
	if source.Status == "disabled" || !observabilityExporterActive(source.ActiveExporters, "analytics") {
		_ = stream.writeEvent("end", cursor, appObservabilityRequestStreamEndEvent{
			Cursor: cursor,
			Reason: source.Reason,
		})
		return
	}

	currentSince, _, err := parseAppObservabilityWindowTimes(window)
	if err != nil {
		_ = stream.writeEvent("warning", cursor, appObservabilityRequestStreamWarningEvent{Cursor: cursor, Message: err.Error()})
		_ = stream.writeEvent("end", cursor, appObservabilityRequestStreamEndEvent{Cursor: cursor, Reason: err.Error()})
		return
	}
	if resume, ok := parseAppObservabilityRequestTimestamp(r.Header.Get("Last-Event-ID")); ok && resume.After(currentSince) {
		currentSince = resume.Add(time.Nanosecond)
	}

	query := cloneURLValues(r.URL.Query())
	query.Del("follow")
	ticker := time.NewTicker(appObservabilityRequestStreamPollInterval)
	defer ticker.Stop()
	for {
		pollUntil := time.Now().UTC()
		pollWindow := appObservabilityWindow{
			Since: currentSince.UTC().Format(time.RFC3339Nano),
			Until: pollUntil.Format(time.RFC3339Nano),
		}
		requests, err := s.queryAppObservabilityRequests(r.Context(), app.ID, pollWindow, query)
		if err != nil {
			source.Status = "degraded"
			source.Available = false
			source.Reason = err.Error()
			_ = stream.writeEvent("warning", cursor, appObservabilityRequestStreamWarningEvent{Cursor: cursor, Message: err.Error()})
			_ = stream.writeEvent("end", cursor, appObservabilityRequestStreamEndEvent{Cursor: cursor, Reason: err.Error()})
			return
		}
		for index := len(requests) - 1; index >= 0; index-- {
			item := requests[index]
			if ts, ok := parseAppObservabilityRequestTimestamp(stringField(item, "timestamp")); ok {
				if !ts.Before(currentSince) {
					currentSince = ts.Add(time.Nanosecond)
				}
				cursor = ts.UTC().Format(time.RFC3339Nano)
			} else {
				cursor = pollUntil.Format(time.RFC3339Nano)
			}
			if err := stream.writeEvent("request", cursor, appObservabilityRequestStreamRequestEvent{
				Cursor:  cursor,
				Request: item,
			}); err != nil {
				return
			}
		}
		if !follow {
			_ = stream.writeEvent("end", cursor, appObservabilityRequestStreamEndEvent{Cursor: cursor, Reason: "snapshot complete"})
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Server) handleGetAppObservabilityTrace(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principalCanReadAppObservability(principal) {
		httpx.WriteError(w, http.StatusForbidden, "missing app.observability.read scope")
		return
	}
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}
	traceID := strings.TrimSpace(r.PathValue("trace_id"))
	if traceID == "" {
		httpx.WriteError(w, http.StatusBadRequest, "trace_id is required")
		return
	}
	source := s.appObservabilitySourceStatus(app.ID, "analytics", "trace query backend is not wired yet")
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
	if !principalCanReadAppObservability(principal) {
		httpx.WriteError(w, http.StatusForbidden, "missing app.observability.read scope")
		return
	}
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}
	window, ok := s.readAppObservabilityWindow(w, r, app.ID)
	if !ok {
		return
	}
	source := s.appObservabilitySourceStatus(app.ID, "analytics", "diagnosis query backend is not wired yet")
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

func principalCanReadAppObservability(principal model.Principal) bool {
	return principal.IsPlatformAdmin() ||
		principal.HasScope("app.observability.read") ||
		principal.HasScope("app.read") ||
		principal.HasScope("app.write") ||
		principal.HasScope("app.deploy")
}

func (s *Server) appObservabilitySourceStatus(appID string, requiredExporter string, queryPendingReason string) appObservabilitySourceStatus {
	cfg := s.observabilityConfig.Normalize()
	status := cfg.Status()
	retention := cfg.RetentionForApp(appID)
	activeExporters := append([]string{}, status.Exporters...)
	source := appObservabilitySourceStatus{
		Available:       false,
		Status:          "disabled",
		Mode:            status.Mode,
		Retention:       retention.String(),
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

func (s *Server) readAppObservabilityWindow(w http.ResponseWriter, r *http.Request, appID string) (appObservabilityWindow, bool) {
	return readAppObservabilityWindow(w, r, s.observabilityConfig.RetentionForApp(appID))
}

func readAppObservabilityWindow(w http.ResponseWriter, r *http.Request, retention time.Duration) (appObservabilityWindow, bool) {
	now := time.Now().UTC()
	until := now
	if rawUntil := strings.TrimSpace(r.URL.Query().Get("until")); rawUntil != "" {
		parsed, err := parseAppObservabilityTimestamp(rawUntil)
		if err != nil {
			httpx.WriteError(w, http.StatusBadRequest, "until must be an RFC3339 timestamp")
			return appObservabilityWindow{}, false
		}
		until = parsed.UTC()
	}

	if retention <= 0 {
		retention = observability.DefaultRetention
	}
	defaultWindow := appObservabilityDefaultWindow
	if retention < defaultWindow {
		defaultWindow = retention
	}
	since := until.Add(-defaultWindow)
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
	if until.Sub(since) > retention {
		httpx.WriteError(w, http.StatusBadRequest, fmt.Sprintf("observability query window cannot exceed app retention %s", retention))
		return appObservabilityWindow{}, false
	}
	if since.Before(now.Add(-retention)) {
		httpx.WriteError(w, http.StatusBadRequest, fmt.Sprintf("observability query starts before app retention %s", retention))
		return appObservabilityWindow{}, false
	}
	return appObservabilityWindow{
		Since: since.Format(time.RFC3339),
		Until: until.Format(time.RFC3339),
	}, true
}

func (s *Server) readAppObservabilityStreamWindow(w http.ResponseWriter, r *http.Request, follow bool, appID string) (appObservabilityWindow, bool) {
	window, ok := s.readAppObservabilityWindow(w, r, appID)
	if !ok {
		return appObservabilityWindow{}, false
	}
	if !follow || strings.TrimSpace(r.URL.Query().Get("since")) != "" {
		return window, true
	}
	_, until, err := parseAppObservabilityWindowTimes(window)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return appObservabilityWindow{}, false
	}
	streamWindow := appObservabilityRequestStreamDefaultWindow
	if retention := s.observabilityConfig.RetentionForApp(appID); retention > 0 && retention < streamWindow {
		streamWindow = retention
	}
	window.Since = until.Add(-streamWindow).UTC().Format(time.RFC3339)
	return window, true
}

func parseAppObservabilitySince(raw string, until time.Time) (time.Time, error) {
	if duration, err := time.ParseDuration(raw); err == nil {
		if duration < 0 {
			return time.Time{}, fmt.Errorf("since duration must be positive")
		}
		return until.Add(-duration), nil
	}
	parsed, err := parseAppObservabilityTimestamp(raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("since must be a duration or RFC3339 timestamp")
	}
	return parsed, nil
}

func parseAppObservabilityTimestamp(raw string) (time.Time, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return time.Time{}, fmt.Errorf("timestamp is required")
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05.999",
		"2006-01-02 15:04:05",
	} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid timestamp %q", value)
}

func parseAppObservabilityRequestTimestamp(raw string) (time.Time, bool) {
	parsed, err := parseAppObservabilityTimestamp(raw)
	return parsed, err == nil
}

func appObservabilityStreamCursorFromWindow(window appObservabilityWindow) string {
	since, _, err := parseAppObservabilityWindowTimes(window)
	if err != nil {
		return time.Now().UTC().Format(time.RFC3339Nano)
	}
	return since.UTC().Format(time.RFC3339Nano)
}

func cloneURLValues(values url.Values) url.Values {
	out := make(url.Values, len(values))
	for key, items := range values {
		copied := append([]string(nil), items...)
		out[key] = copied
	}
	return out
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

func (s *Server) queryAppObservabilityMetrics(ctx context.Context, appID string, window appObservabilityWindow, rawQuery string) ([]map[string]any, error) {
	cfg := s.observabilityConfig.Normalize()
	endpoint, err := normalizePrometheusQueryURL(cfg.MetricsQueryURL)
	if err != nil {
		return nil, err
	}
	_, until, err := parseAppObservabilityWindowTimes(window)
	if err != nil {
		return nil, err
	}
	metricQuery, err := buildAppObservabilityAdhocMetricQuery(appID, window, rawQuery)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: cfg.ExportTimeout}
	return queryPrometheusInstant(ctx, client, endpoint, metricQuery, until, cfg.MaxPayloadBytes)
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

func buildAppObservabilityAdhocMetricQuery(appID string, window appObservabilityWindow, rawQuery string) (appObservabilityMetricQuery, error) {
	normalized := strings.ToLower(strings.TrimSpace(rawQuery))
	normalized = strings.ReplaceAll(normalized, "_", " ")
	normalized = strings.Join(strings.Fields(normalized), " ")
	if normalized == "" {
		return appObservabilityMetricQuery{}, fmt.Errorf("query is required")
	}
	queries := buildAppObservabilityMetricQueries(appID, window)
	for _, query := range queries {
		name := strings.ReplaceAll(strings.ToLower(query.Name), "_", " ")
		if normalized == name {
			return query, nil
		}
	}
	aliases := map[string]string{
		"requests":     "rpm",
		"request rate": "rpm",
		"rpm":          "rpm",
		"errors":       "error_rate",
		"error rate":   "error_rate",
		"p95 latency":  "p95_duration_ms",
		"latency":      "p95_duration_ms",
		"duration":     "p95_duration_ms",
		"p95 duration": "p95_duration_ms",
		"ttfb":         "p95_ttfb_ms",
		"p95 ttfb":     "p95_ttfb_ms",
	}
	if target, ok := aliases[normalized]; ok {
		for _, query := range queries {
			if query.Name == target {
				return query, nil
			}
		}
	}
	return appObservabilityMetricQuery{}, fmt.Errorf("unsupported metrics query %q; use rpm, error_rate, p95_ttfb_ms, p95_duration_ms, or a supported alias", rawQuery)
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
	since, err := parseAppObservabilityTimestamp(window.Since)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid observability window since: %w", err)
	}
	until, err := parseAppObservabilityTimestamp(window.Until)
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
		return s.queryAppObservabilityRuleDiagnosis(ctx, appID, window)
	}
	return appObservabilityDiagnosisFromClickHouseRow(rows[0]), nil
}

func (s *Server) queryAppObservabilityRuleDiagnosis(ctx context.Context, appID string, window appObservabilityWindow) (appObservabilityDiagnosis, error) {
	requestQuery, err := buildAppObservabilityRuleRequestStatsQuery(appID, window)
	if err != nil {
		return appObservabilityDiagnosis{}, err
	}
	requestRows, err := s.queryAppObservabilityClickHouse(ctx, requestQuery)
	if err != nil {
		return appObservabilityDiagnosis{}, err
	}

	spanQuery, err := buildAppObservabilityRuleSpanStatsQuery(appID, window)
	if err != nil {
		return appObservabilityDiagnosis{}, err
	}
	spanRows, err := s.queryAppObservabilityClickHouse(ctx, spanQuery)
	if err != nil {
		return appObservabilityDiagnosis{}, err
	}

	eventQuery, err := buildAppObservabilityRuleEventQuery(appID, window)
	if err != nil {
		return appObservabilityDiagnosis{}, err
	}
	eventRows, err := s.queryAppObservabilityClickHouse(ctx, eventQuery)
	if err != nil {
		return appObservabilityDiagnosis{}, err
	}

	return appObservabilityRuleDiagnosisFromRows(requestRows, spanRows, eventRows), nil
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

func buildAppObservabilityRuleRequestStatsQuery(appID string, window appObservabilityWindow) (string, error) {
	since, until, err := parseAppObservabilityWindowTimes(window)
	if err != nil {
		return "", err
	}
	conditions := []string{
		"app_id = " + quoteClickHouseString(appID),
		"ts >= " + clickHouseDateTime64Literal(since),
		"ts <= " + clickHouseDateTime64Literal(until),
	}
	return "SELECT " +
		"count() AS request_count, " +
		"countIf(status_code >= 500) AS error_5xx_count, " +
		"countIf(status_code >= 400) AS error_4xx_count, " +
		"countIf(status_code = 404) AS not_found_count, " +
		"if(count() = 0, 0, countIf(status_code >= 500) / count()) AS error_5xx_rate, " +
		"if(count() = 0, 0, countIf(status_code >= 400) / count()) AS error_4xx_rate, " +
		"if(count() = 0, 0, countIf(status_code = 404) / count()) AS not_found_rate, " +
		"quantileTDigest(0.95)(ttfb_ms) AS p95_ttfb_ms, " +
		"quantileTDigest(0.95)(duration_ms) AS p95_duration_ms, " +
		"max(duration_ms) AS max_duration_ms, " +
		"countIf(JSONExtractBool(summary_json, 'fallback_hit')) AS edge_fallback_count, " +
		"countIf(JSONExtractBool(summary_json, 'peer_fallback')) AS peer_fallback_count, " +
		"countIf(JSONExtractBool(summary_json, 'fallback_hit') AND " + appObservabilityActionableFallbackPredicate() + ") AS actionable_edge_fallback_count, " +
		"countIf(JSONExtractBool(summary_json, 'peer_fallback') AND " + appObservabilityActionableFallbackPredicate() + ") AS actionable_peer_fallback_count, " +
		"countIf(JSONExtractBool(summary_json, 'sse')) AS stream_count " +
		"FROM request_facts WHERE " + strings.Join(conditions, " AND ") +
		" FORMAT JSONEachRow", nil
}

func appObservabilityActionableFallbackPredicate() string {
	path := "JSONExtractString(summary_json, 'path')"
	return "NOT JSONExtractBool(summary_json, 'sse') AND " +
		"NOT startsWith(" + path + ", '/assets/') AND " +
		path + " NOT IN ('/', '/favicon.ico', '/healthz', '/livez')"
}

func buildAppObservabilityRuleSpanStatsQuery(appID string, window appObservabilityWindow) (string, error) {
	since, until, err := parseAppObservabilityWindowTimes(window)
	if err != nil {
		return "", err
	}
	conditions := []string{
		"app_id = " + quoteClickHouseString(appID),
		"ts >= " + clickHouseDateTime64Literal(since),
		"ts <= " + clickHouseDateTime64Literal(until),
	}
	return "SELECT service, stage, count() AS span_count, " +
		"quantileTDigest(0.95)(stage_ms) AS p95_stage_ms, " +
		"max(stage_ms) AS max_stage_ms, " +
		"countIf(status_code >= 500 OR error_type != '') AS error_count " +
		"FROM request_spans WHERE " + strings.Join(conditions, " AND ") +
		" GROUP BY service, stage ORDER BY p95_stage_ms DESC LIMIT 10 FORMAT JSONEachRow", nil
}

func buildAppObservabilityRuleEventQuery(appID string, window appObservabilityWindow) (string, error) {
	since, until, err := parseAppObservabilityWindowTimes(window)
	if err != nil {
		return "", err
	}
	conditions := []string{
		"app_id = " + quoteClickHouseString(appID),
		"ts >= " + clickHouseDateTime64Literal(since),
		"ts <= " + clickHouseDateTime64Literal(until),
	}
	return "SELECT ts, event_type, severity, operation_id, runtime_id, message, attributes_json " +
		"FROM app_events WHERE " + strings.Join(conditions, " AND ") +
		" ORDER BY ts DESC LIMIT 20 FORMAT JSONEachRow", nil
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

func appObservabilityRuleDiagnosisFromRows(requestRows []map[string]any, spanRows []map[string]any, eventRows []map[string]any) appObservabilityDiagnosis {
	requestStats := map[string]any{}
	if len(requestRows) > 0 {
		requestStats = requestRows[0]
	}
	requestCount := floatField(requestStats, "request_count")
	error5xxCount := floatField(requestStats, "error_5xx_count")
	error4xxCount := floatField(requestStats, "error_4xx_count")
	notFoundCount := floatField(requestStats, "not_found_count")
	error5xxRate := floatField(requestStats, "error_5xx_rate")
	error4xxRate := floatField(requestStats, "error_4xx_rate")
	notFoundRate := floatField(requestStats, "not_found_rate")
	p95TTFB := floatField(requestStats, "p95_ttfb_ms")
	p95Duration := floatField(requestStats, "p95_duration_ms")
	maxDuration := floatField(requestStats, "max_duration_ms")
	edgeFallbackCount := floatField(requestStats, "edge_fallback_count")
	peerFallbackCount := floatField(requestStats, "peer_fallback_count")
	actionableEdgeFallbackCount := edgeFallbackCount
	if value, ok := optionalFloatField(requestStats, "actionable_edge_fallback_count"); ok {
		actionableEdgeFallbackCount = value
	}
	actionablePeerFallbackCount := peerFallbackCount
	if value, ok := optionalFloatField(requestStats, "actionable_peer_fallback_count"); ok {
		actionablePeerFallbackCount = value
	}

	evidence := []string{
		formatAppObservabilityEvidence("request_count", requestCount, ""),
		formatAppObservabilityEvidence("error_5xx_count", error5xxCount, ""),
		formatAppObservabilityEvidence("p95_ttfb_ms", p95TTFB, "ms"),
		formatAppObservabilityEvidence("p95_duration_ms", p95Duration, "ms"),
	}
	if edgeFallbackCount > 0 || peerFallbackCount > 0 {
		evidence = append(evidence,
			formatAppObservabilityEvidence("edge_fallback_count", edgeFallbackCount, ""),
			formatAppObservabilityEvidence("peer_fallback_count", peerFallbackCount, ""),
		)
		if actionableEdgeFallbackCount != edgeFallbackCount || actionablePeerFallbackCount != peerFallbackCount {
			evidence = append(evidence,
				formatAppObservabilityEvidence("actionable_edge_fallback_count", actionableEdgeFallbackCount, ""),
				formatAppObservabilityEvidence("actionable_peer_fallback_count", actionablePeerFallbackCount, ""),
			)
		}
	}

	topSpan := appObservabilityTopSpanRuleRow(spanRows)
	if topSpan != nil {
		evidence = append(evidence,
			fmt.Sprintf("top_span=%s.%s", stringField(topSpan, "service"), stringField(topSpan, "stage")),
			formatAppObservabilityEvidence("top_span_p95_stage_ms", floatField(topSpan, "p95_stage_ms"), "ms"),
		)
	}
	if event := appObservabilityFirstEventMatching(eventRows, appObservabilityEventLooksLikeError); event != nil {
		evidence = append(evidence,
			"recent_error_event="+firstNonEmpty(stringField(event, "event_type"), "app_event"),
			"recent_error_message="+truncateAppObservabilityEvidence(stringField(event, "message"), 160),
		)
	}

	switch {
	case requestCount == 0:
		return appObservabilityDiagnosis{
			Bottleneck: "no_recent_requests",
			Confidence: 0.35,
			Evidence:   evidence,
			NextActions: []string{
				"send a small canary request or widen the diagnosis window before changing capacity",
				"verify edge request_facts ingestion if traffic was expected in this window",
			},
		}
	case appObservabilityHasTracebackEvent(eventRows):
		return appObservabilityDiagnosis{
			Bottleneck: "traceback_error_burst",
			Confidence: appObservabilityConfidence(0.8+error5xxRate, 0.95),
			Evidence:   evidence,
			NextActions: []string{
				"inspect logs around the traceback burst before scaling workers",
				"compare the latest release events against the first error timestamp",
			},
		}
	case error5xxCount >= 3 && error5xxRate >= 0.05:
		return appObservabilityDiagnosis{
			Bottleneck: "error_burst",
			Confidence: appObservabilityConfidence(0.72+error5xxRate, 0.95),
			Evidence: append(evidence,
				formatAppObservabilityEvidence("error_5xx_rate", error5xxRate, ""),
			),
			NextActions: []string{
				"open request samples and trace waterfall for the 5xx window",
				"inspect recent app events and runtime logs before changing capacity",
			},
		}
	case notFoundCount >= 5 && notFoundRate >= 0.2:
		return appObservabilityDiagnosis{
			Bottleneck: "edge_routing_not_found",
			Confidence: appObservabilityConfidence(0.62+notFoundRate, 0.9),
			Evidence: append(evidence,
				formatAppObservabilityEvidence("not_found_count", notFoundCount, ""),
				formatAppObservabilityEvidence("not_found_rate", notFoundRate, ""),
			),
			NextActions: []string{
				"inspect route table, custom domains, and path prefixes for this app",
				"compare affected hostnames and path templates in request_facts",
			},
		}
	case requestCount > 0 && (actionableEdgeFallbackCount/requestCount >= 0.05 || actionablePeerFallbackCount/requestCount >= 0.05):
		return appObservabilityDiagnosis{
			Bottleneck: "edge_routing_fallback",
			Confidence: appObservabilityConfidence(0.62+(actionableEdgeFallbackCount+actionablePeerFallbackCount)/requestCount, 0.9),
			Evidence:   evidence,
			NextActions: []string{
				"inspect edge group placement and runtime availability for fallback traffic",
				"verify the active edge worker slot and route generation before scaling the app",
			},
		}
	case appObservabilityRecentDeployEvent(eventRows) != nil && (error4xxRate >= 0.15 || error5xxRate >= 0.03 || p95Duration >= 3000):
		return appObservabilityDiagnosis{
			Bottleneck: "release_regression_candidate",
			Confidence: 0.72,
			Evidence: append(evidence,
				formatAppObservabilityEvidence("error_4xx_count", error4xxCount, ""),
				formatAppObservabilityEvidence("max_duration_ms", maxDuration, "ms"),
			),
			NextActions: []string{
				"compare request failures before and after the latest deploy_event",
				"inspect the latest operation logs and roll back only if the regression aligns with the deploy window",
			},
		}
	case topSpan != nil:
		bottleneck, nextActions := appObservabilityBottleneckFromTopSpan(topSpan)
		return appObservabilityDiagnosis{
			Bottleneck:  bottleneck,
			Confidence:  appObservabilitySpanConfidence(topSpan, p95Duration),
			Evidence:    evidence,
			NextActions: nextActions,
		}
	case p95TTFB >= 1000 || p95Duration >= 3000:
		return appObservabilityDiagnosis{
			Bottleneck: "app_latency",
			Confidence: 0.58,
			Evidence: append(evidence,
				formatAppObservabilityEvidence("max_duration_ms", maxDuration, "ms"),
			),
			NextActions: []string{
				"add or verify request_spans to split runtime, pool, database, and upstream wait",
				"inspect requests and runtime metrics in the same window before scaling",
			},
		}
	default:
		return appObservabilityDiagnosis{
			Bottleneck: "no_clear_bottleneck",
			Confidence: 0.45,
			Evidence:   evidence,
			NextActions: []string{
				"keep the current capacity unchanged until one signal dominates",
				"run a short stepped load test and watch requests, spans, logs, and metrics together",
			},
		}
	}
}

func appObservabilityTopSpanRuleRow(rows []map[string]any) map[string]any {
	var selected map[string]any
	var selectedP95 float64
	for _, row := range rows {
		p95 := floatField(row, "p95_stage_ms")
		if selected == nil || p95 > selectedP95 {
			selected = row
			selectedP95 = p95
		}
	}
	return selected
}

func appObservabilityBottleneckFromTopSpan(row map[string]any) (string, []string) {
	service := strings.ToLower(stringField(row, "service"))
	stage := strings.ToLower(stringField(row, "stage"))
	key := strings.Trim(strings.Join([]string{service, stage}, "."), ".")

	switch {
	case strings.Contains(stage, "db") ||
		strings.Contains(stage, "postgres") ||
		strings.Contains(stage, "sql") ||
		strings.Contains(stage, "lock"):
		return "db_lock_or_query_wait", []string{
			"inspect database lock waiters, slow queries, and transaction scope",
			"do not scale app workers until database wait is separated from runtime wait",
		}
	case strings.Contains(stage, "pool") ||
		strings.Contains(stage, "acquire") ||
		strings.Contains(stage, "connection"):
		return "connection_pool_wait", []string{
			"inspect client pool size, pending acquire count, and upstream concurrency",
			"increase pool or replicas only after confirming event loop lag is low",
		}
	case strings.Contains(service, "edge") ||
		strings.Contains(stage, "route") ||
		strings.Contains(stage, "routing"):
		return "edge_routing_wait", []string{
			"inspect edge route generation, fallback count, and active edge slot",
			"verify runtime readiness before scaling application workers",
		}
	case strings.Contains(stage, "cpu") ||
		strings.Contains(stage, "memory") ||
		strings.Contains(stage, "event_loop") ||
		strings.Contains(stage, "worker"):
		return "runtime_resource_wait", []string{
			"inspect runtime CPU, memory, event loop lag, and worker saturation",
			"scale runtime capacity only if resource gauges align with this span evidence",
		}
	default:
		if key == "" {
			key = "app_span_wait"
		}
		return key, []string{
			"inspect the dominant span stage before changing capacity",
			"add lower-level spans if this stage still hides multiple waits",
		}
	}
}

func appObservabilitySpanConfidence(row map[string]any, p95Duration float64) float64 {
	p95Stage := floatField(row, "p95_stage_ms")
	if p95Stage <= 0 {
		return 0.55
	}
	if p95Duration <= 0 {
		return 0.68
	}
	return appObservabilityConfidence(0.55+(p95Stage/p95Duration)*0.45, 0.92)
}

func appObservabilityFirstEventMatching(rows []map[string]any, match func(map[string]any) bool) map[string]any {
	for _, row := range rows {
		if match(row) {
			return row
		}
	}
	return nil
}

func appObservabilityHasTracebackEvent(rows []map[string]any) bool {
	return appObservabilityFirstEventMatching(rows, func(row map[string]any) bool {
		text := strings.ToLower(strings.Join([]string{
			stringField(row, "message"),
			stringField(row, "event_type"),
			stringField(row, "severity"),
			stringField(row, "attributes_json"),
		}, " "))
		return strings.Contains(text, "traceback") ||
			strings.Contains(text, "panic") ||
			strings.Contains(text, "exception")
	}) != nil
}

func appObservabilityEventLooksLikeError(row map[string]any) bool {
	text := strings.ToLower(strings.Join([]string{
		stringField(row, "message"),
		stringField(row, "event_type"),
		stringField(row, "severity"),
		stringField(row, "attributes_json"),
	}, " "))
	return strings.Contains(text, "error") ||
		strings.Contains(text, "failed") ||
		strings.Contains(text, "traceback") ||
		strings.Contains(text, "panic") ||
		strings.Contains(text, "exception")
}

func appObservabilityRecentDeployEvent(rows []map[string]any) map[string]any {
	return appObservabilityFirstEventMatching(rows, func(row map[string]any) bool {
		eventType := strings.ToLower(stringField(row, "event_type"))
		message := strings.ToLower(stringField(row, "message"))
		return strings.Contains(eventType, "deploy") || strings.Contains(message, "deploy")
	})
}

func appObservabilityConfidence(value float64, maxValue float64) float64 {
	if value < 0 {
		return 0
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

func truncateAppObservabilityEvidence(value string, maxLength int) string {
	value = strings.TrimSpace(value)
	if maxLength <= 0 || len(value) <= maxLength {
		return value
	}
	return value[:maxLength] + "..."
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
