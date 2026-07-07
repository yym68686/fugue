package api

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"

	"k8s.io/apimachinery/pkg/api/resource"
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
	if robustnessRepairDisabled() {
		s.appendRobustnessRepairAudit(principal, incident, plan, "disabled")
		httpx.WriteError(w, http.StatusConflict, "automatic robustness repair is disabled by FUGUE_ROBUSTNESS_REPAIR_DISABLED")
		return
	}
	if !req.DryRun {
		s.appendRobustnessRepairAudit(principal, incident, plan, "blocked")
		httpx.WriteError(w, http.StatusConflict, "automatic robustness repair is not enabled for this incident class; inspect the repair plan and run the recommended command")
		return
	}
	s.appendRobustnessRepairAudit(principal, incident, plan, "dry_run")
	httpx.WriteJSON(w, http.StatusOK, model.RobustnessRepairPlanResponse{Plan: plan})
}

func robustnessRepairDisabled() bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv("FUGUE_ROBUSTNESS_REPAIR_DISABLED")))
	switch value {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func (s *Server) appendRobustnessRepairAudit(principal model.Principal, incident model.RobustnessIncident, plan model.RobustnessRepairPlan, outcome string) {
	if s == nil || s.store == nil {
		return
	}
	outcome = strings.TrimSpace(outcome)
	if outcome == "" {
		outcome = "unknown"
	}
	s.appendAudit(principal, "robustness.repair."+outcome, "robustness_incident", incident.ID, principal.TenantID, map[string]string{
		"check_name": incident.CheckName,
		"severity":   incident.Severity,
		"subject":    incident.Subject,
		"dry_run":    fmt.Sprintf("%t", plan.DryRun),
		"safe":       fmt.Sprintf("%t", plan.Safe),
		"status":     plan.Status,
	})
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
	artifactChecks, err := s.robustnessGeneratedArtifactChecks(r, dnsOpts)
	if err != nil {
		return model.RobustnessStatus{}, err
	}
	checks = append(checks, artifactChecks...)
	nodeChecks, err := s.robustnessNodeStateChecks(r, principal)
	if err != nil {
		return model.RobustnessStatus{}, err
	}
	checks = append(checks, nodeChecks...)
	nodeDeepHealthChecks, err := s.robustnessNodeDeepHealthChecks()
	if err != nil {
		return model.RobustnessStatus{}, err
	}
	checks = append(checks, nodeDeepHealthChecks...)
	backupChecks, err := s.robustnessBackupChecks()
	if err != nil {
		return model.RobustnessStatus{}, err
	}
	checks = append(checks, backupChecks...)
	backingServiceChecks, err := s.robustnessBackingServiceChecks()
	if err != nil {
		return model.RobustnessStatus{}, err
	}
	checks = append(checks, backingServiceChecks...)
	operationChecks, err := s.robustnessOperationChecks()
	if err != nil {
		return model.RobustnessStatus{}, err
	}
	checks = append(checks, operationChecks...)
	consumerChecks, err := s.robustnessPlatformConsumerChecks()
	if err != nil {
		return model.RobustnessStatus{}, err
	}
	checks = append(checks, consumerChecks...)
	runtimeContinuity, err := s.buildRuntimeContinuityStatuses()
	if err != nil {
		return model.RobustnessStatus{}, err
	}
	checks = append(checks, robustnessChecksFromRuntimeContinuity(runtimeContinuity)...)
	var routeExplain *model.RouteExplainResponse
	if subject != "" {
		explain, err := s.explainRouteForRobustness(r, subject)
		if err != nil {
			return model.RobustnessStatus{}, err
		}
		routeExplain = &explain
		checks = append(checks, robustnessChecksFromRouteExplain(explain)...)
	}
	failureContracts := subsystemFailureContracts()
	checks = append(checks, model.RobustnessCheck{
		Name:     "subsystem_failure_contracts",
		Pass:     len(failureContracts) >= 16,
		Severity: model.RobustnessSeverityInfo,
		Subject:  "platform",
		Observed: fmt.Sprintf("%d", len(failureContracts)),
		Message:  "production critical subsystems must have explicit failure contracts",
		Evidence: map[string]string{"contracts": fmt.Sprintf("%d", len(failureContracts))},
	})
	releaseSignals := []model.ReleaseSignal{}
	releaseSignalPolicy, err := s.activeReleaseSignalPolicy()
	if err != nil {
		checks = append(checks, model.RobustnessCheck{
			Name:       "release_signal_policy",
			Pass:       false,
			Severity:   model.RobustnessSeverityBlockPublish,
			Subject:    "release_guard_policy",
			Expected:   "active release guard policy can be read and parsed",
			Observed:   err.Error(),
			Message:    "release guard policy is invalid",
			RepairHint: "publish a valid release_guard_policy artifact or roll back to the last known good policy",
			Evidence:   map[string]string{"guardian": "release-guard", "artifact_kind": model.PlatformArtifactKindReleaseGuardPolicy},
		})
	} else {
		releaseSignals = releaseSignalPolicy.Signals
		checks = applyReleaseSignalsToRobustnessChecks(checks, releaseSignals)
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
		"checks":          fmt.Sprintf("%d", len(checks)),
		"incidents":       fmt.Sprintf("%d", len(incidents)),
		"dns_zone":        dns.Zone,
		"release_signals": fmt.Sprintf("%d", len(releaseSignals)),
	}
	if autonomy.BlockRollout {
		blockRollout = true
	}
	if subject != "" {
		summary["subject"] = subject
	}
	return model.RobustnessStatus{
		GeneratedAt:       generatedAt,
		Pass:              pass,
		BlockRollout:      blockRollout,
		Subject:           subject,
		Summary:           summary,
		Checks:            checks,
		Incidents:         incidents,
		Invariants:        resilienceInvariantRegistry(),
		Inventory:         s.buildResilienceInventory(subject),
		Gaps:              resilienceGapReport(),
		Dashboards:        resilienceDashboards(),
		AlertRules:        resilienceAlertRules(),
		RuntimeContinuity: runtimeContinuity,
		ChaosDrills:       resilienceChaosDrills(),
		Runbooks:          resilienceRunbooks(),
		Autonomy:          &autonomy,
		DNS:               &dns,
		RouteExplain:      routeExplain,
		FailureContracts:  failureContracts,
		ReleaseSignals:    releaseSignals,
		GeneratedSources: []string{
			"platform_autonomy",
			"dns_delegation_preflight",
			"generated_artifact_inventory",
			"node_generation_inventory",
			"subsystem_failure_contracts",
			"backup_restore_inventory",
			"backing_service_runtime",
			"operation_inventory",
			"route_explain",
			"node_deep_health",
			"platform_consumer_generation",
			"runtime_continuity",
			"release_guard_policy",
			"resilience_invariant_registry",
			"resilience_inventory",
			"resilience_gap_report",
			"resilience_dashboards",
			"resilience_alert_rules",
			"chaos_drills",
			"runbooks",
		},
	}, nil
}

func (s *Server) robustnessGeneratedArtifactChecks(r *http.Request, dnsOpts dnsDelegationPreflightOptions) ([]model.RobustnessCheck, error) {
	checks := []model.RobustnessCheck{}
	routeBundle, err := s.deriveEdgeRouteBundle(r, edgeRouteBundleOptions{})
	if err != nil {
		if failures := bundleInvariantChecks(err); len(failures) > 0 {
			checks = append(checks, robustnessChecksWithGuardian(failures, "bundle-rollout")...)
			return checks, nil
		}
		checks = append(checks, model.RobustnessCheck{
			Name:       "generated_artifact_edge_route_bundle",
			Pass:       false,
			Severity:   model.RobustnessSeverityWarning,
			Subject:    "edge_route_bundle",
			Expected:   "route bundle can be derived and validated",
			Observed:   err.Error(),
			Message:    "edge route bundle derivation failed",
			RepairHint: "inspect app route, runtime, and edge node inventory before publishing route bundles",
			Evidence:   map[string]string{"guardian": "bundle-rollout", "validation_path": "validateEdgeRouteBundleForPublish"},
		})
	} else {
		checks = append(checks, model.RobustnessCheck{
			Name:     "generated_artifact_edge_route_bundle",
			Pass:     true,
			Severity: model.RobustnessSeverityInfo,
			Subject:  "edge_route_bundle",
			Expected: "route bundle can be derived and validated",
			Observed: fmt.Sprintf("version=%s routes=%d tls_allowlist=%d", routeBundle.Version, len(routeBundle.Routes), len(routeBundle.TLSAllowlist)),
			Evidence: map[string]string{
				"guardian":        "bundle-rollout",
				"artifact_kind":   "edge_route_bundle",
				"validation_path": "validateEdgeRouteBundleForPublish",
				"generation":      routeBundle.Generation,
			},
		})
	}

	zone := normalizeExternalAppDomain(dnsOpts.Zone)
	if zone == "" {
		zone = normalizeExternalAppDomain(s.appBaseDomain)
	}
	edgeAnswerIPsByGroup, err := s.edgeDNSAnswerIPsByGroup(r.Context(), edgeDNSBundleOptions{Zone: zone})
	if err != nil {
		return nil, err
	}
	answerIPs := edgeDNSAllHealthyAnswerIPs("", edgeAnswerIPsByGroup)
	if len(answerIPs) == 0 {
		checks = append(checks, model.RobustnessCheck{
			Name:     "generated_artifact_edge_dns_bundle",
			Pass:     true,
			Severity: model.RobustnessSeverityInfo,
			Subject:  "edge_dns_bundle",
			Expected: "DNS bundle validation is skipped when no route-publishable edge IP exists",
			Observed: "answer_ips=0",
			Evidence: map[string]string{
				"guardian":        "bundle-rollout",
				"artifact_kind":   "edge_dns_bundle",
				"validation_path": "validateEdgeDNSBundleForPublish",
			},
		})
		return checks, nil
	}
	dnsBundle, err := s.deriveEdgeDNSBundle(r, edgeDNSBundleOptions{
		Zone:      zone,
		AnswerIPs: answerIPs,
		TTL:       defaultEdgeDNSTTL,
	})
	if err != nil {
		if failures := bundleInvariantChecks(err); len(failures) > 0 {
			checks = append(checks, robustnessChecksWithGuardian(failures, "bundle-rollout")...)
			return checks, nil
		}
		checks = append(checks, model.RobustnessCheck{
			Name:       "generated_artifact_edge_dns_bundle",
			Pass:       false,
			Severity:   model.RobustnessSeverityWarning,
			Subject:    "edge_dns_bundle",
			Expected:   "DNS bundle can be derived and validated",
			Observed:   err.Error(),
			Message:    "edge DNS bundle derivation failed",
			RepairHint: "inspect route/DNS invariant inputs before publishing DNS bundles",
			Evidence:   map[string]string{"guardian": "bundle-rollout", "validation_path": "validateEdgeDNSBundleForPublish"},
		})
	} else {
		checks = append(checks, model.RobustnessCheck{
			Name:     "generated_artifact_edge_dns_bundle",
			Pass:     true,
			Severity: model.RobustnessSeverityInfo,
			Subject:  "edge_dns_bundle",
			Expected: "DNS bundle can be derived and validated",
			Observed: fmt.Sprintf("version=%s records=%d answer_ips=%d", dnsBundle.Version, len(dnsBundle.Records), len(answerIPs)),
			Evidence: map[string]string{
				"guardian":        "bundle-rollout",
				"artifact_kind":   "edge_dns_bundle",
				"validation_path": "validateEdgeDNSBundleForPublish",
				"generation":      dnsBundle.Generation,
			},
		})
	}
	return checks, nil
}

func robustnessChecksWithGuardian(checks []model.RobustnessCheck, guardian string) []model.RobustnessCheck {
	out := make([]model.RobustnessCheck, 0, len(checks))
	for _, check := range checks {
		if check.Evidence == nil {
			check.Evidence = map[string]string{}
		}
		check.Evidence["guardian"] = strings.TrimSpace(guardian)
		out = append(out, check)
	}
	return out
}

func (s *Server) robustnessNodeStateChecks(r *http.Request, principal model.Principal) ([]model.RobustnessCheck, error) {
	now := time.Now().UTC()
	checks := []model.RobustnessCheck{}

	edgeNodes, _, err := s.store.ListEdgeNodes("")
	if err != nil {
		return nil, err
	}
	if nodePolicies, policyErr := s.loadClusterNodePolicyStatuses(r.Context(), principal); policyErr == nil {
		edgeNodes = activeEdgeNodesForPolicy(edgeNodes, nodePolicies)
	}
	edgeNodes = freshEdgeNodes(edgeNodes, now)
	expectedRouteGenerationByGroup := mostCommonNonEmptyEdgeRouteGenerationByGroup(edgeNodes)
	checks = append(checks, model.RobustnessCheck{
		Name:     "edge_route_generation_inventory",
		Pass:     true,
		Severity: model.RobustnessSeverityInfo,
		Subject:  "edge_nodes",
		Expected: "edge nodes report route bundle and LKG generations",
		Observed: fmt.Sprintf("nodes=%d expected_generations=%s", len(edgeNodes), formatStringStringMap(expectedRouteGenerationByGroup)),
		Evidence: map[string]string{"guardian": "node-health", "desired_generation_by_group": formatStringStringMap(expectedRouteGenerationByGroup)},
	})
	for _, node := range edgeNodes {
		subject := "edge-node:" + strings.TrimSpace(node.ID)
		if strings.TrimSpace(node.ID) == "" {
			continue
		}
		expectedRouteGeneration := expectedRouteGenerationByGroup[strings.TrimSpace(node.EdgeGroupID)]
		routeGeneration := firstNonEmpty(strings.TrimSpace(node.RouteBundleVersion), strings.TrimSpace(node.ServingGeneration))
		generationPass := expectedRouteGeneration == "" || routeGeneration == "" || routeGeneration == expectedRouteGeneration
		checks = append(checks, model.RobustnessCheck{
			Name:     "edge_route_generation_drift",
			Pass:     generationPass,
			Severity: model.RobustnessSeverityDegraded,
			Subject:  subject,
			Expected: firstNonEmpty(expectedRouteGeneration, "reported route generation exists"),
			Observed: fmt.Sprintf("route_bundle=%s serving=%s lkg=%s", node.RouteBundleVersion, node.ServingGeneration, node.LKGGeneration),
			Message:  robustnessGenerationDriftMessage("edge", node.ID, expectedRouteGeneration, routeGeneration),
			Evidence: map[string]string{
				"guardian":           "node-health",
				"edge_group_id":      node.EdgeGroupID,
				"desired_generation": expectedRouteGeneration,
				"serving_generation": node.ServingGeneration,
				"lkg_generation":     node.LKGGeneration,
			},
			RepairHint: "request edge route resync or keep this edge out of DNS answer candidates until it converges",
		})
		caddyPass := strings.TrimSpace(node.CaddyLastError) == ""
		checks = append(checks, model.RobustnessCheck{
			Name:     "edge_caddy_reload",
			Pass:     caddyPass,
			Severity: model.RobustnessSeverityDegraded,
			Subject:  subject,
			Expected: "caddy_last_error is empty",
			Observed: firstNonEmpty(strings.TrimSpace(node.CaddyLastError), "caddy_last_error="),
			Message:  strings.TrimSpace(node.CaddyLastError),
			Evidence: map[string]string{
				"guardian":              "edge-tls",
				"caddy_applied_version": node.CaddyAppliedVersion,
				"caddy_route_count":     fmt.Sprintf("%d", node.CaddyRouteCount),
			},
			RepairHint: "keep the previous Caddy config active and inspect the rendered route config before retrying reload",
		})
		if edgeNodeHasRouteState(node) {
			tlsPass := edgeNodeTLSReadyForDNS(node)
			checks = append(checks, model.RobustnessCheck{
				Name:     "edge_tls_ready",
				Pass:     tlsPass,
				Severity: model.RobustnessSeverityBlockPublish,
				Subject:  subject,
				Expected: "route-publishable edge node has TLS-ready status",
				Observed: fmt.Sprintf("tls_status=%s caddy_last_error=%s", firstNonEmpty(strings.TrimSpace(node.TLSStatus), "unknown"), strings.TrimSpace(node.CaddyLastError)),
				Message:  strings.TrimSpace(node.TLSLastMessage),
				Evidence: map[string]string{
					"guardian":      "edge-tls",
					"edge_group_id": node.EdgeGroupID,
				},
				RepairHint: "quarantine the hostname-edge pair from DNS answers until SNI/TLS probes pass",
			})
		}
	}

	dnsNodes, err := s.store.ListDNSNodes("")
	if err != nil {
		return nil, err
	}
	if nodePolicies, policyErr := s.loadClusterNodePolicyStatuses(r.Context(), principal); policyErr == nil {
		dnsNodes = activeDNSNodesForPolicy(dnsNodes, nodePolicies)
	}
	dnsNodes = freshDNSNodes(dnsNodes, now)
	expectedDNSGenerationByGroup := mostCommonNonEmptyDNSGenerationByGroup(dnsNodes)
	checks = append(checks, model.RobustnessCheck{
		Name:     "dns_generation_inventory",
		Pass:     true,
		Severity: model.RobustnessSeverityInfo,
		Subject:  "dns_nodes",
		Expected: "DNS nodes report active and LKG generations",
		Observed: fmt.Sprintf("nodes=%d expected_generations=%s", len(dnsNodes), formatStringStringMap(expectedDNSGenerationByGroup)),
		Evidence: map[string]string{"guardian": "node-health", "desired_generation_by_group": formatStringStringMap(expectedDNSGenerationByGroup)},
	})
	for _, node := range dnsNodes {
		subject := "dns-node:" + strings.TrimSpace(node.ID)
		if strings.TrimSpace(node.ID) == "" {
			continue
		}
		expectedDNSGeneration := expectedDNSGenerationByGroup[strings.TrimSpace(node.EdgeGroupID)]
		dnsGeneration := firstNonEmpty(strings.TrimSpace(node.DNSBundleVersion), strings.TrimSpace(node.ServingGeneration))
		generationPass := expectedDNSGeneration == "" || dnsGeneration == "" || dnsGeneration == expectedDNSGeneration
		checks = append(checks, model.RobustnessCheck{
			Name:     "dns_generation_drift",
			Pass:     generationPass,
			Severity: model.RobustnessSeverityDegraded,
			Subject:  subject,
			Expected: firstNonEmpty(expectedDNSGeneration, "reported DNS generation exists"),
			Observed: fmt.Sprintf("dns_bundle=%s serving=%s lkg=%s", node.DNSBundleVersion, node.ServingGeneration, node.LKGGeneration),
			Message:  robustnessGenerationDriftMessage("dns", node.ID, expectedDNSGeneration, dnsGeneration),
			Evidence: map[string]string{
				"guardian":           "node-health",
				"edge_group_id":      node.EdgeGroupID,
				"desired_generation": expectedDNSGeneration,
				"serving_generation": node.ServingGeneration,
				"lkg_generation":     node.LKGGeneration,
			},
			RepairHint: "request DNS node resync and keep serving LKG while the node converges",
		})
		lkgReportPass := strings.TrimSpace(node.ServingGeneration) != "" || strings.TrimSpace(node.LKGGeneration) != ""
		checks = append(checks, model.RobustnessCheck{
			Name:     "dns_node_lkg_reporting",
			Pass:     lkgReportPass,
			Severity: model.RobustnessSeverityWarning,
			Subject:  subject,
			Expected: "serving_generation or lkg_generation is reported",
			Observed: fmt.Sprintf("serving=%s lkg=%s cache_status=%s", node.ServingGeneration, node.LKGGeneration, node.CacheStatus),
			Evidence: map[string]string{
				"guardian":       "node-health",
				"cache_status":   node.CacheStatus,
				"bundle_version": node.DNSBundleVersion,
			},
			RepairHint: "upgrade or resync this DNS node so operators can distinguish active and LKG serving states",
		})
	}
	return checks, nil
}

func robustnessGenerationDriftMessage(kind, id, expected, observed string) string {
	expected = strings.TrimSpace(expected)
	observed = strings.TrimSpace(observed)
	if expected == "" || observed == "" || expected == observed {
		return ""
	}
	return fmt.Sprintf("%s node %s reports generation %s but the current majority generation is %s", kind, strings.TrimSpace(id), observed, expected)
}

func mostCommonNonEmptyEdgeRouteGenerationByGroup(nodes []model.EdgeNode) map[string]string {
	countsByGroup := map[string]map[string]int{}
	for _, node := range nodes {
		groupID := strings.TrimSpace(node.EdgeGroupID)
		if groupID == "" {
			groupID = "default"
		}
		generation := firstNonEmpty(strings.TrimSpace(node.RouteBundleVersion), strings.TrimSpace(node.ServingGeneration))
		if generation != "" {
			if countsByGroup[groupID] == nil {
				countsByGroup[groupID] = map[string]int{}
			}
			countsByGroup[groupID][generation]++
		}
	}
	return mostCommonGenerationByGroup(countsByGroup)
}

func mostCommonNonEmptyDNSGenerationByGroup(nodes []model.DNSNode) map[string]string {
	countsByGroup := map[string]map[string]int{}
	for _, node := range nodes {
		groupID := strings.TrimSpace(node.EdgeGroupID)
		if groupID == "" {
			groupID = "default"
		}
		generation := firstNonEmpty(strings.TrimSpace(node.DNSBundleVersion), strings.TrimSpace(node.ServingGeneration))
		if generation != "" {
			if countsByGroup[groupID] == nil {
				countsByGroup[groupID] = map[string]int{}
			}
			countsByGroup[groupID][generation]++
		}
	}
	return mostCommonGenerationByGroup(countsByGroup)
}

func mostCommonGenerationByGroup(countsByGroup map[string]map[string]int) map[string]string {
	out := map[string]string{}
	for groupID, counts := range countsByGroup {
		if generation := mostCommonGeneration(counts); generation != "" {
			out[groupID] = generation
		}
	}
	return out
}

func mostCommonGeneration(counts map[string]int) string {
	best := ""
	bestCount := 0
	for generation, count := range counts {
		if count > bestCount || (count == bestCount && generation < best) {
			best = generation
			bestCount = count
		}
	}
	return best
}

func (s *Server) robustnessBackupChecks() ([]model.RobustnessCheck, error) {
	policies, err := s.store.ListBackupPolicies(store.BackupPolicyFilter{IncludeDisabled: true, PlatformAdmin: true, Limit: 500})
	if err != nil {
		return nil, err
	}
	usage, err := s.store.BackupUsage("", true)
	if err != nil {
		return nil, err
	}
	posture := s.platformBackupPosture(policies, usage)
	checks := make([]model.RobustnessCheck, 0, len(posture)+2)
	for _, item := range posture {
		target := strings.TrimSpace(item.Target.Type)
		if target == "" {
			target = strings.TrimSpace(item.Target.Component)
		}
		subject := "backup:" + firstNonEmpty(target, "unknown")
		status := strings.TrimSpace(item.Status)
		pass := !strings.EqualFold(status, "blocked") && !strings.EqualFold(status, model.BackupPolicyStatusBlockedNoBackend)
		checks = append(checks, model.RobustnessCheck{
			Name:     "backup_backend_readiness",
			Pass:     pass,
			Severity: model.RobustnessSeverityDegraded,
			Subject:  subject,
			Expected: "backup posture is not blocked",
			Observed: fmt.Sprintf("status=%s policy=%s last_success=%s", status, item.PolicyID, robustnessTimeValue(item.LastSuccessfulAt)),
			Message:  item.Message,
			Evidence: map[string]string{
				"guardian":             "bundle-rollout",
				"policy_id":            item.PolicyID,
				"restore_drill_status": item.RestoreDrillStatus,
			},
			RepairHint: "configure or repair the backup backend before relying on automated recovery",
		})
		if item.PolicyID != "" {
			fresh := item.LastSuccessfulAt != nil && time.Since(item.LastSuccessfulAt.UTC()) <= 25*time.Hour
			checks = append(checks, model.RobustnessCheck{
				Name:     "scheduled_backup_freshness",
				Pass:     fresh,
				Severity: model.RobustnessSeverityWarning,
				Subject:  subject,
				Expected: "last successful scheduled backup is within 25h",
				Observed: "last_successful_at=" + robustnessTimeValue(item.LastSuccessfulAt),
				Evidence: map[string]string{
					"guardian":  "bundle-rollout",
					"policy_id": item.PolicyID,
				},
				RepairHint: "run a manual backup or repair the scheduled backup worker before a risky rollout",
			})
		}
		if item.RestoreDrillStatus != "" {
			checks = append(checks, model.RobustnessCheck{
				Name:     "restore_dry_run_plan",
				Pass:     strings.TrimSpace(item.RestoreDrillStatus) != "",
				Severity: model.RobustnessSeverityInfo,
				Subject:  subject,
				Expected: "restore drill or plan status is recorded",
				Observed: "restore_drill_status=" + item.RestoreDrillStatus,
				Evidence: map[string]string{"guardian": "bundle-rollout", "policy_id": item.PolicyID},
			})
		}
	}
	artifacts, err := s.store.ListBackupArtifacts(store.BackupArtifactFilter{PlatformAdmin: true, ActiveOnly: true, Limit: 500})
	if err != nil {
		return nil, err
	}
	missingIntegrity := 0
	for _, artifact := range artifacts {
		if strings.TrimSpace(artifact.SHA256) == "" || strings.TrimSpace(artifact.ObjectKey) == "" {
			missingIntegrity++
		}
	}
	checks = append(checks, model.RobustnessCheck{
		Name:       "backup_artifact_integrity",
		Pass:       missingIntegrity == 0,
		Severity:   model.RobustnessSeverityWarning,
		Subject:    "backup_artifacts",
		Expected:   "active artifacts include object key and sha256",
		Observed:   fmt.Sprintf("active=%d missing_integrity=%d", len(artifacts), missingIntegrity),
		Evidence:   map[string]string{"guardian": "bundle-rollout"},
		RepairHint: "verify or replace artifacts that lack immutable object and digest metadata",
	})
	return checks, nil
}

func robustnessTimeValue(value *time.Time) string {
	if value == nil || value.IsZero() {
		return "never"
	}
	return value.UTC().Format(time.RFC3339)
}

func (s *Server) robustnessBackingServiceChecks() ([]model.RobustnessCheck, error) {
	services, err := s.store.ListBackingServices("", true)
	if err != nil {
		return nil, err
	}
	checks := []model.RobustnessCheck{{
		Name:     "managed_postgres_inventory",
		Pass:     true,
		Severity: model.RobustnessSeverityInfo,
		Subject:  "backing_services",
		Expected: "managed postgres backing service runtime status is visible",
		Observed: fmt.Sprintf("managed_postgres=%d", countManagedPostgresBackingServices(services)),
		Evidence: map[string]string{"guardian": "node-health"},
	}}
	for _, service := range services {
		if !robustnessManagedPostgresBackingService(service) {
			continue
		}
		postgres := service.Spec.Postgres
		subject := "backing-service:" + strings.TrimSpace(service.ID)
		started := robustnessTimeValue(service.CurrentRuntimeStartedAt)
		ready := robustnessTimeValue(service.CurrentRuntimeReadyAt)
		pass := service.CurrentRuntimeStartedAt == nil || service.CurrentRuntimeReadyAt != nil
		ownerAppID := strings.TrimSpace(service.OwnerAppID)
		check := model.RobustnessCheck{
			Name:       "managed_postgres_runtime_ready",
			Pass:       pass,
			Severity:   model.RobustnessSeverityDegraded,
			Subject:    subject,
			Expected:   "current_runtime_ready_at is present after managed postgres runtime start",
			Observed:   fmt.Sprintf("status=%s runtime_id=%s started_at=%s ready_at=%s storage_size=%s primary_node=%s", service.Status, strings.TrimSpace(postgres.RuntimeID), started, ready, strings.TrimSpace(postgres.StorageSize), strings.TrimSpace(postgres.PrimaryNodeName)),
			Evidence:   map[string]string{"guardian": "node-health", "service_id": strings.TrimSpace(service.ID), "owner_app_id": ownerAppID, "runtime_id": strings.TrimSpace(postgres.RuntimeID), "storage_size": strings.TrimSpace(postgres.StorageSize), "primary_node_name": strings.TrimSpace(postgres.PrimaryNodeName)},
			RepairHint: "inspect managed Postgres logs and storage state before app rollouts; if WAL or data storage is exhausted, expand or localize the app database through Fugue",
		}
		if !pass {
			check.Message = "managed Postgres runtime has started but is not ready"
		}
		checks = append(checks, check)

		storageSize := strings.TrimSpace(postgres.StorageSize)
		storageGiB, storageOK := robustnessStorageGiB(storageSize)
		storageFloorGiB := model.DefaultManagedPostgresStorageGibibytes
		storagePass := storageOK && storageGiB >= storageFloorGiB
		storageCheck := model.RobustnessCheck{
			Name:       "managed_postgres_storage_floor",
			Pass:       storagePass,
			Severity:   model.RobustnessSeverityWarning,
			Subject:    subject,
			Expected:   fmt.Sprintf("managed postgres storage_size is at least %dGi", storageFloorGiB),
			Observed:   fmt.Sprintf("storage_size=%s parsed_gib=%d", firstNonEmpty(storageSize, "unset"), storageGiB),
			Evidence:   map[string]string{"guardian": "node-health", "service_id": strings.TrimSpace(service.ID), "owner_app_id": ownerAppID, "runtime_id": strings.TrimSpace(postgres.RuntimeID), "storage_size": storageSize, "storage_floor_gib": fmt.Sprintf("%d", storageFloorGiB)},
			RepairHint: "expand small managed Postgres volumes through Fugue before heavy write traffic or long WAL bursts",
		}
		if !storageOK {
			storageCheck.Message = "managed Postgres storage size is missing or could not be parsed"
		} else if !storagePass {
			storageCheck.Message = "managed Postgres storage size is below the default safety floor"
		}
		checks = append(checks, storageCheck)
	}
	return checks, nil
}

func robustnessStorageGiB(value string) (int64, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	quantity, err := resource.ParseQuantity(value)
	if err != nil {
		return 0, false
	}
	bytes := quantity.Value()
	if bytes <= 0 {
		return 0, false
	}
	const bytesPerGiB = int64(1 << 30)
	return (bytes + bytesPerGiB - 1) / bytesPerGiB, true
}

func countManagedPostgresBackingServices(services []model.BackingService) int {
	count := 0
	for _, service := range services {
		if robustnessManagedPostgresBackingService(service) {
			count++
		}
	}
	return count
}

func robustnessManagedPostgresBackingService(service model.BackingService) bool {
	if !strings.EqualFold(strings.TrimSpace(service.Type), model.BackingServiceTypePostgres) {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(service.Status), model.BackingServiceStatusDeleted) {
		return false
	}
	provisioner := strings.TrimSpace(strings.ToLower(service.Provisioner))
	if provisioner != "" && provisioner != model.BackingServiceProvisionerManaged {
		return false
	}
	return service.Spec.Postgres != nil
}

func (s *Server) robustnessOperationChecks() ([]model.RobustnessCheck, error) {
	ops, err := s.store.ListOperations("", true)
	if err != nil {
		return nil, err
	}
	activeByType := map[string]int{}
	stuck := 0
	now := time.Now().UTC()
	for _, op := range ops {
		if !robustnessOperationActive(op.Status) {
			continue
		}
		activeByType[op.Type]++
		started := op.CreatedAt
		if op.StartedAt != nil {
			started = op.StartedAt.UTC()
		}
		if !started.IsZero() && now.Sub(started) > 6*time.Hour {
			stuck++
		}
	}
	return []model.RobustnessCheck{
		{
			Name:     "operation_inventory",
			Pass:     true,
			Severity: model.RobustnessSeverityInfo,
			Subject:  "operations",
			Expected: "long-running operation types and active counts are visible",
			Observed: fmt.Sprintf("active=%d types=%s", sumIntMap(activeByType), formatStringIntMap(activeByType)),
			Evidence: map[string]string{"guardian": "node-health"},
		},
		{
			Name:       "operation_stuck_detection",
			Pass:       stuck == 0,
			Severity:   model.RobustnessSeverityWarning,
			Subject:    "operations",
			Expected:   "no active operation has been running longer than 6h",
			Observed:   fmt.Sprintf("stuck=%d", stuck),
			Evidence:   map[string]string{"guardian": "node-health", "threshold": "6h"},
			RepairHint: "inspect operation diagnosis and blocking dependencies before retrying, failing, or canceling work",
		},
	}, nil
}

func robustnessOperationActive(status string) bool {
	switch strings.TrimSpace(status) {
	case model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent:
		return true
	default:
		return false
	}
}

func sumIntMap(values map[string]int) int {
	total := 0
	for _, value := range values {
		total += value
	}
	return total
}

func formatStringIntMap(values map[string]int) string {
	if len(values) == 0 {
		return "none"
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", key, values[key]))
	}
	return strings.Join(parts, ",")
}

func formatStringStringMap(values map[string]string) string {
	if len(values) == 0 {
		return "none"
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%s", key, values[key]))
	}
	return strings.Join(parts, ",")
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
	if incident.CheckName == "managed_postgres_runtime_ready" || incident.CheckName == "managed_postgres_storage_floor" {
		ownerAppID := strings.TrimSpace(incident.Evidence["owner_app_id"])
		serviceID := strings.TrimSpace(incident.Evidence["service_id"])
		if ownerAppID != "" {
			actions = append(actions,
				model.RobustnessRepairAction{
					Kind:        "diagnose_managed_postgres",
					Subject:     incident.Subject,
					Description: "inspect the app-owned managed Postgres status",
					Command:     "fugue app db show " + ownerAppID,
					Automatic:   false,
					Risk:        "read-only",
				},
				model.RobustnessRepairAction{
					Kind:        "inspect_managed_postgres_logs",
					Subject:     incident.Subject,
					Description: "inspect recent and previous managed Postgres logs for disk, WAL, crashloop, or bootstrap errors",
					Command:     "fugue app logs runtime " + ownerAppID + " --component postgres --tail 200 --previous",
					Automatic:   false,
					Risk:        "read-only",
				},
				model.RobustnessRepairAction{
					Kind:        "expand_managed_postgres_storage",
					Subject:     incident.Subject,
					Description: "expand storage through the database localization path when the current size is below the safety floor or logs confirm WAL/data exhaustion",
					Command:     "fugue app db configure " + ownerAppID + " --storage-size <larger-than-current> --wait",
					Automatic:   false,
					Risk:        "operator must choose a safe target size",
				},
			)
		} else if serviceID != "" {
			actions = append(actions, model.RobustnessRepairAction{
				Kind:        "diagnose_managed_postgres",
				Subject:     incident.Subject,
				Description: "inspect the managed Postgres backing service status",
				Command:     "fugue service show " + serviceID,
				Automatic:   false,
				Risk:        "read-only",
			})
		}
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
