package api

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/observability"
)

const (
	edgeRouteDecisionQueryDefaultLimit = 200
	edgeRouteDecisionQueryMaxLimit     = 500
)

func (s *Server) handleListAppEdgeRouteDecisions(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principalCanReadAppObservability(principal) {
		httpx.WriteError(w, http.StatusForbidden, "missing app.observability.read scope")
		return
	}
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}
	hostname := normalizeExternalAppDomain(r.URL.Query().Get("domain"))
	if hostname == "" {
		httpx.WriteError(w, http.StatusBadRequest, "domain is required")
		return
	}
	if !s.edgeRouteDecisionDomainBelongsToApp(app, hostname) {
		httpx.WriteError(w, http.StatusForbidden, "domain is not visible for this app")
		return
	}
	window, ok := readAppObservabilityWindow(w, r, observability.EdgeRouteDecisionRetention)
	if !ok {
		return
	}
	limit, err := parseEdgeRouteDecisionQueryLimit(r.URL.Query().Get("limit"))
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	source := s.appObservabilitySourceStatus(app.ID, "analytics", "edge route decision evidence query backend is not wired yet")
	source.Retention = observability.EdgeRouteDecisionRetention.String()
	audit := appObservabilityAuditMetadata(window)
	audit["domain"] = hostname
	audit["limit"] = strconv.Itoa(limit)
	s.appendAudit(principal, "app.observability.edge_route_decisions.list", "app", app.ID, app.TenantID, audit)
	decisions := []map[string]any{}
	missingLinks := []map[string]any{}
	if source.Status != "disabled" && observabilityExporterActive(source.ActiveExporters, "analytics") {
		decisions, missingLinks, err = s.queryAppEdgeRouteDecisionEvidence(r.Context(), app.TenantID, app.ID, hostname, window, limit)
		if err != nil {
			source.Status = "degraded"
			source.Available = false
			source.Reason = err.Error()
		} else {
			source.Status = "available"
			source.Available = true
			source.Reason = "edge route decision evidence query backend returned data"
		}
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"source":        source,
		"app_id":        app.ID,
		"domain":        hostname,
		"window":        window,
		"decisions":     decisions,
		"missing_links": missingLinks,
	})
}

func (s *Server) edgeRouteDecisionDomainBelongsToApp(app model.App, hostname string) bool {
	hostname = normalizeExternalAppDomain(hostname)
	if hostname == "" {
		return false
	}
	if app.Route != nil && normalizeExternalAppDomain(app.Route.Hostname) == hostname {
		return true
	}
	domains, err := s.store.ListVerifiedAppDomains()
	if err != nil {
		return false
	}
	for _, domain := range domains {
		if strings.TrimSpace(domain.AppID) == app.ID && strings.TrimSpace(domain.TenantID) == app.TenantID && normalizeExternalAppDomain(domain.Hostname) == hostname {
			return true
		}
	}
	return false
}

func parseEdgeRouteDecisionQueryLimit(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return edgeRouteDecisionQueryDefaultLimit, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 || value > edgeRouteDecisionQueryMaxLimit {
		return 0, fmt.Errorf("limit must be between 1 and %d", edgeRouteDecisionQueryMaxLimit)
	}
	return value, nil
}

func (s *Server) queryAppEdgeRouteDecisionEvidence(ctx context.Context, tenantID, appID, hostname string, window appObservabilityWindow, limit int) ([]map[string]any, []map[string]any, error) {
	decisionQuery, missingQuery, err := buildAppEdgeRouteDecisionQueries(tenantID, appID, hostname, window, limit)
	if err != nil {
		return nil, nil, err
	}
	decisions, err := s.queryAppObservabilityClickHouse(ctx, decisionQuery)
	if err != nil {
		return nil, nil, err
	}
	missingLinks, err := s.queryAppObservabilityClickHouse(ctx, missingQuery)
	if err != nil {
		return nil, nil, err
	}
	for _, decision := range decisions {
		decision["material"] = parseJSONMapField(decision["material_json"])
		delete(decision, "material_json")
	}
	for _, link := range missingLinks {
		link["material"] = parseJSONMapField(link["material_json"])
		delete(link, "material_json")
	}
	return decisions, missingLinks, nil
}

func buildAppEdgeRouteDecisionQueries(tenantID, appID, hostname string, window appObservabilityWindow, limit int) (string, string, error) {
	since, until, err := parseAppObservabilityWindowTimes(window)
	if err != nil {
		return "", "", err
	}
	if limit < 1 || limit > edgeRouteDecisionQueryMaxLimit {
		return "", "", fmt.Errorf("invalid edge route decision query limit")
	}
	conditions := strings.Join([]string{
		"tenant_id = " + quoteClickHouseString(strings.TrimSpace(tenantID)),
		"app_id = " + quoteClickHouseString(strings.TrimSpace(appID)),
		"hostname = " + quoteClickHouseString(normalizeExternalAppDomain(hostname)),
		"ts >= " + clickHouseDateTime64Literal(since),
		"ts <= " + clickHouseDateTime64Literal(until),
	}, " AND ")
	decisionQuery := fmt.Sprintf(`SELECT ts, tenant_id, project_id, app_id, hostname, path_prefix, edge_group_id, decision_id, bundle_version, route_generation, correlation_key, final_status, final_reason, invariant_violations_json, material_json
FROM fugue_observability.edge_route_decisions FINAL
WHERE %s
ORDER BY ts ASC, decision_id ASC
LIMIT %d
FORMAT JSONEachRow`, conditions, limit)
	missingQuery := fmt.Sprintf(`SELECT ts, tenant_id, app_id, hostname, request_id, edge_id, decision_id, bundle_version, route_generation, correlation_key, reason, material_json
FROM fugue_observability.edge_route_decision_missing_links FINAL
WHERE %s
ORDER BY ts ASC, request_id ASC
LIMIT %d
FORMAT JSONEachRow`, conditions, limit)
	return decisionQuery, missingQuery, nil
}
