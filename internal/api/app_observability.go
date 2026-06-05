package api

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"fugue/internal/httpx"
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
	diagnosis := appObservabilityDiagnosis{
		Bottleneck: "unavailable",
		Confidence: 0,
		Evidence: []string{
			source.Reason,
		},
		NextActions: []string{
			"enable and verify Fugue Observability query backends before relying on automated bottleneck diagnosis",
		},
	}
	s.appendAudit(principal, "app.observability.diagnosis.read", "app", app.ID, app.TenantID, appObservabilityAuditMetadata(window))
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"source":    source,
		"window":    window,
		"diagnosis": diagnosis,
	})
}

func (s *Server) appObservabilitySourceStatus(requiredExporter string, queryPendingReason string) appObservabilitySourceStatus {
	cfg := s.observabilityConfig.Normalize()
	status := cfg.Status()
	source := appObservabilitySourceStatus{
		Available:       false,
		Status:          "disabled",
		Mode:            status.Mode,
		Retention:       status.Retention,
		ActiveExporters: append([]string(nil), status.Exporters...),
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
