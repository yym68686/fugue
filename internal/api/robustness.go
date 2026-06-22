package api

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

func (s *Server) handleGetRobustnessStatus(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	status, err := s.buildRobustnessStatus(r, principal, r.URL.Query().Get("subject"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, model.RobustnessStatusResponse{Status: status})
}

func (s *Server) handleCheckRobustnessSubject(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	status, err := s.buildRobustnessStatus(r, principal, r.PathValue("subject"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, model.RobustnessStatusResponse{Status: status})
}

func (s *Server) handleListRobustnessIncidents(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	status, err := s.buildRobustnessStatus(r, principal, r.URL.Query().Get("subject"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, model.RobustnessIncidentListResponse{
		Incidents:   status.Incidents,
		GeneratedAt: status.GeneratedAt,
	})
}

func (s *Server) handleGetRobustnessIncident(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	status, err := s.buildRobustnessStatus(r, principal, r.URL.Query().Get("subject"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	incident, ok := robustnessIncidentByID(status.Incidents, r.PathValue("id"))
	if !ok {
		httpx.WriteError(w, http.StatusNotFound, "robustness incident not found in current status")
		return
	}
	httpx.WriteJSON(w, http.StatusOK, model.RobustnessIncidentResponse{
		Incident: incident,
		Status:   status,
	})
}

func (s *Server) handlePlanRobustnessRepair(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	status, err := s.buildRobustnessStatus(r, principal, r.URL.Query().Get("subject"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	incident, ok := robustnessIncidentByID(status.Incidents, r.PathValue("id"))
	if !ok {
		httpx.WriteError(w, http.StatusNotFound, "robustness incident not found in current status")
		return
	}
	plan := robustnessRepairPlanForIncident(incident, true)
	httpx.WriteJSON(w, http.StatusOK, model.RobustnessRepairPlanResponse{Plan: plan})
}

func (s *Server) handleRunRobustnessRepair(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	req := model.RobustnessRepairRequest{DryRun: true}
	if r.Body != nil && r.ContentLength != 0 {
		if err := httpx.DecodeJSON(r, &req); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	status, err := s.buildRobustnessStatus(r, principal, r.URL.Query().Get("subject"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	incident, ok := robustnessIncidentByID(status.Incidents, r.PathValue("id"))
	if !ok {
		httpx.WriteError(w, http.StatusNotFound, "robustness incident not found in current status")
		return
	}
	plan := robustnessRepairPlanForIncident(incident, req.DryRun)
	if !req.DryRun {
		httpx.WriteError(w, http.StatusConflict, "automatic robustness repair is not enabled for this incident class; inspect the repair plan and run the recommended command")
		return
	}
	httpx.WriteJSON(w, http.StatusOK, model.RobustnessRepairPlanResponse{Plan: plan})
}

func (s *Server) buildRobustnessStatus(r *http.Request, principal model.Principal, subject string) (model.RobustnessStatus, error) {
	generatedAt := time.Now().UTC()
	subject = normalizeRobustnessSubject(subject)
	autonomy, err := s.platformAutonomyStatus(r)
	if err != nil {
		return model.RobustnessStatus{}, err
	}
	dnsOpts := s.dnsDelegationPreflightOptionsFromRequest(r)
	if dnsOpts.Zone == "" {
		dnsOpts.Zone = normalizeExternalAppDomain(s.appBaseDomain)
	}
	dns := s.buildDNSDelegationPreflight(r.Context(), principal, dnsOpts)
	checks := []model.RobustnessCheck{}
	checks = append(checks, robustnessChecksFromStore("control-plane-store", autonomy.ControlPlaneStore.Invariants)...)
	checks = append(checks, robustnessChecksFromStore("platform-autonomy", autonomy.Checks)...)
	checks = append(checks, robustnessChecksFromDNSPreflight(dns)...)
	checks = append(checks, robustnessChecksFromDNSNodes(dns.Nodes)...)
	var routeExplain *model.RouteExplainResponse
	if subject != "" {
		explain, err := s.explainRouteForRobustness(r, subject)
		if err != nil {
			return model.RobustnessStatus{}, err
		}
		routeExplain = &explain
		checks = append(checks, robustnessChecksFromRouteExplain(explain)...)
	}
	checks = dedupeRobustnessChecks(checks)
	incidents := robustnessIncidentsFromChecks(checks, generatedAt)
	pass := len(incidents) == 0
	blockRollout := false
	for _, check := range checks {
		if !check.Pass && check.Severity == model.RobustnessSeverityBlockPublish {
			blockRollout = true
			break
		}
	}
	summary := map[string]string{
		"checks":    fmt.Sprintf("%d", len(checks)),
		"incidents": fmt.Sprintf("%d", len(incidents)),
		"dns_zone":  dns.Zone,
	}
	if autonomy.BlockRollout {
		blockRollout = true
	}
	if subject != "" {
		summary["subject"] = subject
	}
	return model.RobustnessStatus{
		GeneratedAt:  generatedAt,
		Pass:         pass,
		BlockRollout: blockRollout,
		Subject:      subject,
		Summary:      summary,
		Checks:       checks,
		Incidents:    incidents,
		Autonomy:     &autonomy,
		DNS:          &dns,
		RouteExplain: routeExplain,
		GeneratedSources: []string{
			"platform_autonomy",
			"dns_delegation_preflight",
			"route_explain",
		},
	}, nil
}

func (s *Server) explainRouteForRobustness(r *http.Request, hostname string) (model.RouteExplainResponse, error) {
	hostname = normalizeExternalAppDomain(hostname)
	if hostname == "" {
		return model.RouteExplainResponse{}, nil
	}
	bundle, err := s.deriveEdgeRouteBundle(r, edgeRouteBundleOptions{})
	if err != nil {
		return model.RouteExplainResponse{}, err
	}
	healthyEdgeGroups, err := s.edgeRouteHealthyEdgeGroups()
	if err != nil {
		return model.RouteExplainResponse{}, err
	}
	response := model.RouteExplainResponse{
		Hostname:          hostname,
		ServingMode:       "unrouted",
		HealthyEdgeGroups: healthyEdgeGroups,
		GeneratedAt:       time.Now().UTC(),
	}
	for _, route := range bundle.Routes {
		if !strings.EqualFold(normalizeExternalAppDomain(route.Hostname), hostname) {
			continue
		}
		routeCopy := route
		response.Routes = append(response.Routes, routeCopy)
		if response.Route == nil {
			response.Route = &routeCopy
			response.ServingMode = routeServingMode(route)
			response.FallbackChain = routeFallbackChain(route)
			response.Reasons = routeExplainReasons(route)
		}
	}
	return response, nil
}

func robustnessChecksFromStore(subject string, checks []model.StoreInvariantCheck) []model.RobustnessCheck {
	out := make([]model.RobustnessCheck, 0, len(checks))
	for _, check := range checks {
		name := strings.TrimSpace(check.Name)
		if name == "" {
			continue
		}
		out = append(out, model.RobustnessCheck{
			Name:       name,
			Pass:       check.Pass,
			Severity:   robustnessSeverityForCheck(name),
			Subject:    subject,
			Expected:   "pass=true",
			Observed:   fmt.Sprintf("pass=%t count=%d", check.Pass, check.Count),
			Message:    check.Message,
			RepairHint: robustnessRepairHint(name),
		})
	}
	return out
}

func robustnessChecksFromDNSPreflight(preflight model.DNSDelegationPreflightResponse) []model.RobustnessCheck {
	out := make([]model.RobustnessCheck, 0, len(preflight.Checks)+1)
	out = append(out, model.RobustnessCheck{
		Name:       "dns_preflight",
		Pass:       preflight.Pass,
		Severity:   model.RobustnessSeverityBlockPublish,
		Subject:    "dns:" + preflight.Zone,
		Expected:   "pass=true",
		Observed:   fmt.Sprintf("pass=%t healthy_nodes=%d min_healthy_nodes=%d", preflight.Pass, preflight.HealthyNodeCount, preflight.MinHealthyNodes),
		Message:    preflight.DNSBundleVersion,
		RepairHint: "inspect failing DNS checks and keep serving last known good DNS bundles until all hard checks pass",
	})
	for _, check := range preflight.Checks {
		name := strings.TrimSpace(check.Name)
		if name == "" {
			continue
		}
		out = append(out, model.RobustnessCheck{
			Name:       name,
			Pass:       check.Pass,
			Severity:   robustnessSeverityForCheck(name),
			Subject:    "dns:" + preflight.Zone,
			Expected:   "pass=true",
			Observed:   fmt.Sprintf("pass=%t", check.Pass),
			Message:    check.Message,
			RepairHint: robustnessRepairHint(name),
		})
	}
	return out
}

func robustnessChecksFromDNSNodes(nodes []model.DNSDelegationNodeCheck) []model.RobustnessCheck {
	out := make([]model.RobustnessCheck, 0, len(nodes))
	for _, node := range nodes {
		subject := "dns-node:" + strings.TrimSpace(node.DNSNodeID)
		evidence := map[string]string{
			"edge_group_id":      node.EdgeGroupID,
			"public_ip":          node.PublicIP,
			"dns_bundle_version": node.DNSBundleVersion,
			"record_count":       fmt.Sprintf("%d", node.RecordCount),
		}
		out = append(out, model.RobustnessCheck{
			Name:       "dns_node_health",
			Pass:       node.Pass,
			Severity:   model.RobustnessSeverityDegraded,
			Subject:    subject,
			Expected:   "healthy node answers probe over UDP/TCP 53",
			Observed:   fmt.Sprintf("pass=%t healthy=%t udp=%t tcp=%t", node.Pass, node.Healthy, node.UDP53Reachable, node.TCP53Reachable),
			Evidence:   evidence,
			Message:    node.Message,
			RepairHint: "trigger DNS node resync or quarantine the DNS role for this node if it remains unhealthy",
		})
	}
	return out
}

func robustnessChecksFromRouteExplain(explain model.RouteExplainResponse) []model.RobustnessCheck {
	routeCount := len(explain.Routes)
	check := model.RobustnessCheck{
		Name:       "route_explain",
		Pass:       routeCount > 0,
		Severity:   model.RobustnessSeverityWarning,
		Subject:    "hostname:" + explain.Hostname,
		Expected:   "at least one route for hostname",
		Observed:   fmt.Sprintf("serving_mode=%s routes=%d", explain.ServingMode, routeCount),
		RepairHint: "create or repair the app route before publishing DNS answers for this hostname",
	}
	if routeCount == 0 {
		check.Message = "hostname is unrouted"
	}
	out := []model.RobustnessCheck{check}
	for _, route := range explain.Routes {
		name := "route_active"
		subject := "hostname:" + route.Hostname + ":" + model.NormalizeAppRoutePathPrefix(route.PathPrefix)
		pass := strings.EqualFold(strings.TrimSpace(route.Status), model.EdgeRouteStatusActive)
		out = append(out, model.RobustnessCheck{
			Name:       name,
			Pass:       pass,
			Severity:   model.RobustnessSeverityBlockPublish,
			Subject:    subject,
			Expected:   "active route with selected edge group",
			Observed:   fmt.Sprintf("status=%s edge_group=%s policy=%s excluded_edges=%s", route.Status, route.EdgeGroupID, route.RoutePolicy, strings.Join(route.ExcludedEdgeIDs, ",")),
			RepairHint: "repair the route source or hold DNS publication for this hostname",
		})
	}
	return out
}

func robustnessIncidentsFromChecks(checks []model.RobustnessCheck, now time.Time) []model.RobustnessIncident {
	incidents := []model.RobustnessIncident{}
	for _, check := range checks {
		if check.Pass {
			continue
		}
		id := robustnessIncidentID(check)
		incidents = append(incidents, model.RobustnessIncident{
			ID:         id,
			Status:     model.RobustnessIncidentStatusDetected,
			Severity:   check.Severity,
			Subject:    check.Subject,
			CheckName:  check.Name,
			Title:      check.Name + " failed",
			Message:    check.Message,
			Expected:   check.Expected,
			Observed:   check.Observed,
			Evidence:   check.Evidence,
			RepairHint: check.RepairHint,
			CreatedAt:  now,
			UpdatedAt:  now,
		})
	}
	sort.Slice(incidents, func(i, j int) bool {
		if incidents[i].Severity != incidents[j].Severity {
			return robustnessSeverityRank(incidents[i].Severity) < robustnessSeverityRank(incidents[j].Severity)
		}
		return incidents[i].ID < incidents[j].ID
	})
	return incidents
}

func robustnessIncidentByID(incidents []model.RobustnessIncident, id string) (model.RobustnessIncident, bool) {
	id = strings.TrimSpace(id)
	for _, incident := range incidents {
		if incident.ID == id {
			return incident, true
		}
	}
	return model.RobustnessIncident{}, false
}

func robustnessRepairPlanForIncident(incident model.RobustnessIncident, dryRun bool) model.RobustnessRepairPlan {
	actions := []model.RobustnessRepairAction{{
		Kind:        "manual_action_required",
		Subject:     incident.Subject,
		Description: firstNonEmpty(strings.TrimSpace(incident.RepairHint), "inspect the incident and repair the failing subsystem"),
		Automatic:   false,
		Risk:        "manual approval required",
	}}
	if strings.Contains(incident.CheckName, "dns") || strings.Contains(incident.Subject, "dns") {
		actions = append(actions, model.RobustnessRepairAction{
			Kind:        "diagnose_dns",
			Subject:     incident.Subject,
			Description: "run DNS status and answer checks before changing delegation or node state",
			Command:     "fugue admin dns status",
			Automatic:   false,
			Risk:        "read-only",
		})
	}
	return model.RobustnessRepairPlan{
		IncidentID:  incident.ID,
		Status:      model.RobustnessRepairPlanStatusManualActionRequired,
		Safe:        false,
		DryRun:      dryRun,
		Message:     "no automatic repair is enabled for this incident class yet",
		Actions:     actions,
		GeneratedAt: time.Now().UTC(),
	}
}

func robustnessIncidentID(check model.RobustnessCheck) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		check.Name,
		check.Subject,
		check.Severity,
		check.Expected,
		check.Observed,
	}, "\x00")))
	return "robust_" + hex.EncodeToString(sum[:])[:16]
}

func robustnessSeverityForCheck(name string) string {
	name = strings.TrimSpace(strings.ToLower(name))
	switch name {
	case "route_dns_invariant", "edge_tls_ready", "dns_preflight", "route_active", "restore_readiness", "permission_verification":
		return model.RobustnessSeverityBlockPublish
	case "dns_bundle_version_stable", "dns_reachability_and_probe", "dns_node_health", "healthy_dns_nodes", "edge", "dns", "node_policy":
		return model.RobustnessSeverityDegraded
	case "cache_errors_zero", "kubernetes_health_gate", "registry", "headscale":
		return model.RobustnessSeverityWarning
	default:
		return model.RobustnessSeverityInfo
	}
}

func robustnessSeverityRank(severity string) int {
	switch severity {
	case model.RobustnessSeverityBlockPublish:
		return 0
	case model.RobustnessSeverityDegraded:
		return 1
	case model.RobustnessSeverityWarning:
		return 2
	default:
		return 3
	}
}

func robustnessRepairHint(name string) string {
	name = strings.TrimSpace(strings.ToLower(name))
	switch name {
	case "route_dns_invariant":
		return "reject the new DNS bundle and keep the last known good DNS answer set until route-ready edges match DNS answers"
	case "edge_tls_ready":
		return "quarantine affected hostname-edge pairs from DNS answers until SNI/TLS probes pass"
	case "dns_bundle_version_stable":
		return "request stale DNS nodes to resync and keep serving LKG on nodes that cannot converge"
	case "dns_reachability_and_probe":
		return "verify DNS node UDP/TCP 53 reachability and keep the node out of delegation candidates if probes fail"
	case "restore_readiness":
		return "block rollout until control-plane store invariants and restore readiness pass"
	case "registry", "headscale":
		return "repair the dependency or hold rollout until platform autonomy passes"
	default:
		return ""
	}
}

func normalizeRobustnessSubject(subject string) string {
	return strings.TrimSpace(subject)
}

func dedupeRobustnessChecks(checks []model.RobustnessCheck) []model.RobustnessCheck {
	seen := map[string]struct{}{}
	out := make([]model.RobustnessCheck, 0, len(checks))
	for _, check := range checks {
		check.Name = strings.TrimSpace(check.Name)
		check.Subject = strings.TrimSpace(check.Subject)
		if check.Name == "" {
			continue
		}
		if check.Severity == "" {
			check.Severity = model.RobustnessSeverityInfo
		}
		key := check.Name + "\x00" + check.Subject + "\x00" + check.Observed
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, check)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Severity != out[j].Severity {
			return robustnessSeverityRank(out[i].Severity) < robustnessSeverityRank(out[j].Severity)
		}
		if out[i].Subject != out[j].Subject {
			return out[i].Subject < out[j].Subject
		}
		return out[i].Name < out[j].Name
	})
	return out
}
