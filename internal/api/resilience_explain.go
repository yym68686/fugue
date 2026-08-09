package api

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

func (s *Server) handleGetReleaseGuardStatus(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	baseline, err := s.buildRobustnessStatus(r, principal, r.URL.Query().Get("subject"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	artifacts, err := s.store.ListPlatformArtifacts(model.PlatformArtifactFilter{Limit: 500})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	artifactKinds := platformArtifactKinds(artifacts)
	if len(artifactKinds) == 0 {
		artifactKinds = platformArtifactKindList()
	}
	artifactFailures, err := s.releaseGuardArtifactValidationFailures(artifacts)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	drift := 0
	for _, artifact := range artifacts {
		consumers, err := s.store.ListPlatformConsumers(artifact.ArtifactKind, artifact.ScopeKey)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		for _, consumer := range consumers {
			if consumer.DesiredGeneration != "" && consumer.ActualGeneration != "" && consumer.DesiredGeneration != consumer.ActualGeneration {
				drift++
			}
		}
	}
	blockedReasons := []string{}
	for _, incident := range baseline.Incidents {
		if incident.Severity == model.RobustnessSeverityBlockPublish {
			blockedReasons = append(blockedReasons, incident.CheckName+": "+firstNonEmpty(incident.Message, incident.Observed, incident.Title))
		}
	}
	blockedReasons = append(blockedReasons, releaseGuardAutonomyBlockedReasons(baseline.Autonomy)...)
	if drift > 0 {
		blockedReasons = append(blockedReasons, fmt.Sprintf("platform consumer generation drift: %d", drift))
	}
	if artifactFailures > 0 {
		blockedReasons = append(blockedReasons, fmt.Sprintf("platform artifact validation failed: %d", artifactFailures))
	}
	gatePolicies := s.gatePolicyRegistry()
	gateViolations := releaseGuardGatePolicyViolations(gatePolicies)
	enforcedGateCount := 0
	for _, policy := range gatePolicies {
		if policy.Mode == model.GatePolicyModeEnforced {
			enforcedGateCount++
		}
	}
	if len(gateViolations) > 0 {
		blockedReasons = append(blockedReasons, fmt.Sprintf("gate policy validation failed: %d", len(gateViolations)))
	}
	blockRollout := baseline.BlockRollout || drift > 0 || artifactFailures > 0 || len(gateViolations) > 0
	blockedReasons = dedupeStrings(blockedReasons)
	if blockRollout && len(blockedReasons) == 0 {
		blockedReasons = []string{"release guard blocked without a classified blocking source"}
	}
	status := model.ReleaseGuardStatus{
		GeneratedAt:              time.Now().UTC(),
		Pass:                     !blockRollout,
		BlockRollout:             blockRollout,
		Mode:                     "enforced",
		RobustnessBaseline:       baseline,
		FailureContractCount:     len(baseline.FailureContracts),
		PlatformArtifactKinds:    artifactKinds,
		PlatformArtifactFailures: artifactFailures,
		PlatformConsumerDrift:    drift,
		GatePolicyCount:          len(gatePolicies),
		EnforcedGateCount:        enforcedGateCount,
		GatePolicyViolations:     gateViolations,
		GatePolicies:             gatePolicies,
		ReleaseSignals:           baseline.ReleaseSignals,
		BlockedReasons:           blockedReasons,
		RecommendedOperatorSteps: releaseGuardRecommendedSteps(blockRollout, blockedReasons),
	}
	httpx.WriteJSON(w, http.StatusOK, model.ReleaseGuardStatusResponse{Status: status})
}

func (s *Server) handleExplainTrafficSafety(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	hostname := normalizeExternalAppDomain(r.PathValue("hostname"))
	if hostname == "" {
		httpx.WriteError(w, http.StatusBadRequest, "hostname is required")
		return
	}
	explain, err := s.explainRouteForRobustness(r, hostname)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	minHealthy := queryIntDefault(r, "min_healthy_edges", 1)
	eligible, gated := trafficSafetyEdgeGroups(explain)
	healthyEdgeCount := trafficSafetyHealthyEdgeNodeCount(explain)
	if healthyEdgeCount == 0 {
		healthyEdgeCount = len(eligible)
	}
	gateReasons := trafficSafetyHardGateReasons(explain)
	blockers := []string{}
	if healthyEdgeCount < minHealthy {
		blockers = append(blockers, fmt.Sprintf("healthy eligible edge nodes %d below minimum %d", healthyEdgeCount, minHealthy))
	}
	if explain.Route == nil {
		blockers = append(blockers, "hostname has no generated edge route")
	}
	if routeBlockers := trafficSafetyRouteBlockers(explain); len(routeBlockers) > 0 {
		blockers = append(blockers, routeBlockers...)
	}
	grayScope := s.trafficSafetyGrayReleaseScope(hostname)
	strictProtection := healthyEdgeCount <= minHealthy
	explorationPaused := len(blockers) > 0 || strictProtection
	if strictProtection {
		blockers = append(blockers, "service is at or below minimum healthy edge count; exploration and exclusion expansion require strict protection")
	}
	state := model.ServiceTrafficSafetyState{
		Hostname:            hostname,
		Pass:                len(blockers) == 0,
		MinHealthyEdgeCount: minHealthy,
		HealthyEdgeCount:    healthyEdgeCount,
		EligibleEdgeGroups:  eligible,
		HardGatedEdgeGroups: gated,
		HardGateReasons:     gateReasons,
		Blockers:            blockers,
		FailureContracts:    []string{"edge_front", "edge_worker", "dns_server", "dns_answer_policy", "caddy_route_bundle", "runtime_scheduler"},
		GrayReleaseScope:    grayScope,
		StrictProtection:    strictProtection,
		ExplorationPaused:   explorationPaused,
		RouteExplain:        explain,
		GeneratedAt:         time.Now().UTC(),
	}
	httpx.WriteJSON(w, http.StatusOK, model.TrafficSafetyExplainResponse{State: state})
}

func (s *Server) handleExplainRequest(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	requestID := strings.TrimSpace(r.PathValue("request_id"))
	if requestID == "" {
		httpx.WriteError(w, http.StatusBadRequest, "request_id is required")
		return
	}
	now := time.Now().UTC()
	since, err := parseEdgeNodeQualitySince(r.URL.Query().Get("since"), now)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	samples, err := s.store.ListEdgePerformanceSamples("", since)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	for _, sample := range samples {
		if strings.TrimSpace(sample.ID) != requestID {
			continue
		}
		httpx.WriteJSON(w, http.StatusOK, model.RequestExplainResponseEnvelope{Explain: s.requestExplainFromSample(r, requestID, sample, now)})
		return
	}
	httpx.WriteJSON(w, http.StatusOK, model.RequestExplainResponseEnvelope{Explain: model.RequestExplainResponse{
		RequestID:    requestID,
		Found:        false,
		ErrorClass:   "not_observed",
		FailurePlane: "control_plane_observability",
		SecretSafe:   true,
		GeneratedAt:  now,
		Evidence: map[string]string{
			"since": since.Format(time.RFC3339),
		},
	}})
}

func platformArtifactKinds(artifacts []model.PlatformArtifact) []string {
	set := map[string]struct{}{}
	for _, artifact := range artifacts {
		if artifact.ArtifactKind != "" {
			set[artifact.ArtifactKind] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for kind := range set {
		out = append(out, kind)
	}
	sort.Strings(out)
	return out
}

func (s *Server) releaseGuardArtifactValidationFailures(artifacts []model.PlatformArtifact) (int, error) {
	failures := 0
	for _, artifact := range artifacts {
		needsValidation, err := s.releaseGuardArtifactNeedsValidation(artifact)
		if err != nil {
			return 0, err
		}
		if !needsValidation {
			continue
		}
		if !platformArtifactValidationPass(validatePlatformArtifactDraft(artifact)) {
			failures++
		}
	}
	return failures, nil
}

func (s *Server) releaseGuardArtifactNeedsValidation(artifact model.PlatformArtifact) (bool, error) {
	if artifact.Status == model.PlatformArtifactStatusValidated {
		return true, nil
	}
	for _, channel := range []string{
		model.PlatformArtifactReleaseChannelShadow,
		model.PlatformArtifactReleaseChannelGray,
		model.PlatformArtifactReleaseChannelFull,
	} {
		active, _, found, err := s.store.GetActivePlatformArtifact(artifact.ArtifactKind, artifact.ScopeKey, channel)
		if err != nil {
			return false, err
		}
		if found && active.ID == artifact.ID {
			return true, nil
		}
	}
	return false, nil
}

func releaseGuardAutonomyBlockedReasons(autonomy *model.PlatformAutonomyStatus) []string {
	if autonomy == nil || !autonomy.BlockRollout {
		return nil
	}
	reasons := []string{}
	if autonomy.ControlPlaneStore.BlockRollout {
		reason := "control plane store blocked rollout"
		failedInvariants := []string{}
		for _, invariant := range autonomy.ControlPlaneStore.Invariants {
			if !invariant.Pass && strings.TrimSpace(invariant.Name) != "" {
				failedInvariants = append(failedInvariants, strings.TrimSpace(invariant.Name))
			}
		}
		failedInvariants = dedupeStrings(failedInvariants)
		if len(failedInvariants) > 0 {
			reason += ": failed_invariants=" + strings.Join(failedInvariants, ",")
		}
		reasons = append(reasons, reason)
	}
	for _, check := range platformAutonomyBlockingChecks(autonomy.Checks) {
		name := strings.TrimSpace(check.Name)
		if name == "restore_readiness" && autonomy.ControlPlaneStore.BlockRollout {
			continue
		}
		reason := "platform autonomy " + name + " failed"
		if errorClass := releaseGuardCheckErrorClass(check.Message); errorClass != "check_failed" {
			reason += ": error_class=" + errorClass
		}
		if check.Count != 0 {
			reason += fmt.Sprintf(" count=%d", check.Count)
		}
		reasons = append(reasons, reason)
	}
	reasons = dedupeStrings(reasons)
	if len(reasons) == 0 {
		return []string{"platform autonomy reported block_rollout=true without a failing blocking check"}
	}
	return reasons
}

func releaseGuardCheckErrorClass(message string) string {
	message = strings.ToLower(strings.TrimSpace(message))
	switch {
	case strings.Contains(message, "context deadline exceeded"), strings.Contains(message, "timeout"), strings.Contains(message, "timed out"):
		return "timeout"
	case strings.Contains(message, "connection refused"):
		return "connection_refused"
	case strings.Contains(message, "connection reset"):
		return "connection_reset"
	case strings.Contains(message, "no route to host"):
		return "no_route_to_host"
	case strings.Contains(message, "not configured"):
		return "not_configured"
	case strings.Contains(message, "invalid"):
		return "invalid"
	case strings.Contains(message, "missing"):
		return "missing"
	case strings.Contains(message, "unavailable"):
		return "unavailable"
	default:
		return "check_failed"
	}
}

func releaseGuardRecommendedSteps(blockRollout bool, blockedReasons []string) []string {
	if !blockRollout {
		return []string{"release guard passed; continue normal rollout"}
	}
	return []string{
		"run fugue admin robustness status --json for full evidence",
		"inspect the named platform autonomy checks when an autonomy blocker is present",
		"run fugue admin artifact consumers <artifact> when generation drift is present",
		"run fugue admin artifact validate <artifact> before releasing invalid platform state",
		"hold rollout until every blocked reason clears",
	}
}

func trafficSafetyEdgeGroups(explain model.RouteExplainResponse) ([]string, []string) {
	eligibleSet := map[string]struct{}{}
	gatedSet := map[string]struct{}{}
	for groupID, healthy := range explain.HealthyEdgeGroups {
		if healthy {
			eligibleSet[groupID] = struct{}{}
		} else {
			gatedSet[groupID] = struct{}{}
		}
	}
	for _, route := range explain.Routes {
		groupID := strings.TrimSpace(firstNonEmpty(route.SelectedEdgeGroup, route.EdgeGroupID))
		if groupID == "" {
			continue
		}
		if route.Status != "" && !trafficSafetyRouteStatusReady(route.Status) {
			delete(eligibleSet, groupID)
			gatedSet[groupID] = struct{}{}
		}
		if route.TLSPolicy != "" && strings.Contains(strings.ToLower(route.TLSPolicy), "blocked") {
			delete(eligibleSet, groupID)
			gatedSet[groupID] = struct{}{}
		}
	}
	return sortedKeys(eligibleSet), sortedKeys(gatedSet)
}

func trafficSafetyHealthyEdgeNodeCount(explain model.RouteExplainResponse) int {
	healthy := 0
	for _, route := range explain.Routes {
		if route.HealthyEdgeNodeCount > healthy {
			healthy = route.HealthyEdgeNodeCount
		}
	}
	if explain.Route != nil && explain.Route.HealthyEdgeNodeCount > healthy {
		healthy = explain.Route.HealthyEdgeNodeCount
	}
	return healthy
}

func trafficSafetyHardGateReasons(explain model.RouteExplainResponse) map[string]string {
	reasons := map[string]string{}
	for groupID, healthy := range explain.HealthyEdgeGroups {
		if !healthy {
			reasons[groupID] = "edge group is not healthy, route-ready, TLS-ready, non-draining, and non-quarantined"
		}
	}
	for _, route := range explain.Routes {
		groupID := strings.TrimSpace(firstNonEmpty(route.SelectedEdgeGroup, route.EdgeGroupID))
		if groupID == "" {
			continue
		}
		if route.Status != "" && !trafficSafetyRouteStatusReady(route.Status) {
			reasons[groupID] = "route generation is not ready: " + route.Status
		}
		if route.TLSPolicy != "" && strings.Contains(strings.ToLower(route.TLSPolicy), "blocked") {
			reasons[groupID] = "TLS policy blocks route publication"
		}
		if len(route.ExcludedEdgeIDs) > 0 || len(route.ExcludedEdgeGroupIDs) > 0 {
			if route.ExclusionReason != "" {
				reasons[groupID] = "service edge exclusion active: " + route.ExclusionReason
			} else {
				reasons[groupID] = "service edge exclusion active"
			}
		}
	}
	return reasons
}

func trafficSafetyRouteBlockers(explain model.RouteExplainResponse) []string {
	blockers := []string{}
	for _, route := range explain.Routes {
		routeID := firstNonEmpty(route.Hostname, explain.Hostname)
		if route.PathPrefix != "" {
			routeID += route.PathPrefix
		}
		if route.Status != "" && !trafficSafetyRouteStatusReady(route.Status) {
			blockers = append(blockers, fmt.Sprintf("route %s status is %s", routeID, route.Status))
		}
		if route.RouteGeneration == "" {
			blockers = append(blockers, fmt.Sprintf("route %s has no route generation", routeID))
		}
	}
	return blockers
}

func trafficSafetyRouteStatusReady(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "", "ready", model.EdgeRouteStatusActive:
		return true
	default:
		return false
	}
}

func (s *Server) trafficSafetyGrayReleaseScope(hostname string) string {
	artifacts, err := s.store.ListPlatformArtifacts(model.PlatformArtifactFilter{Limit: 500})
	if err != nil {
		return ""
	}
	hostname = normalizeExternalAppDomain(hostname)
	for _, artifact := range artifacts {
		if !platformArtifactTrafficSafetyRelevant(artifact.ArtifactKind) {
			continue
		}
		scopeHostname := normalizeExternalAppDomain(artifact.Scope.Hostname)
		if scopeHostname != "" && scopeHostname != hostname {
			continue
		}
		active, release, found, err := s.store.GetActivePlatformArtifact(artifact.ArtifactKind, artifact.ScopeKey, model.PlatformArtifactReleaseChannelGray)
		if err != nil || !found || active.ID != artifact.ID {
			continue
		}
		return fmt.Sprintf("artifact=%s kind=%s scope=%s generation=%s canary=%s", active.ID, active.ArtifactKind, active.ScopeKey, active.Generation, release.CanaryRuleRef)
	}
	return ""
}

func platformArtifactTrafficSafetyRelevant(kind string) bool {
	switch strings.TrimSpace(kind) {
	case model.PlatformArtifactKindTrafficSafetyPolicy,
		model.PlatformArtifactKindEdgeRankingPolicy,
		model.PlatformArtifactKindEdgeRouteBundle,
		model.PlatformArtifactKindDNSAnswerBundle,
		model.PlatformArtifactKindCaddyRouteConfig:
		return true
	default:
		return false
	}
}

func sortedKeys(in map[string]struct{}) []string {
	out := make([]string, 0, len(in))
	for key := range in {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func (s *Server) requestExplainFromSample(r *http.Request, requestID string, sample model.EdgePerformanceSample, generatedAt time.Time) model.RequestExplainResponse {
	attribution := requestAttributionFromSample(sample)
	errorClass := requestErrorClassFromSample(sample)
	explain := model.RequestExplainResponse{
		RequestID:               requestID,
		Found:                   true,
		ErrorClass:              errorClass,
		FailurePlane:            requestFailurePlane(errorClass, sample),
		EdgeID:                  sample.EdgeID,
		EdgeGroupID:             sample.EdgeGroupID,
		Hostname:                sample.Hostname,
		PathPrefix:              sample.PathPrefix,
		Method:                  sample.Method,
		TrafficClass:            sample.TrafficClass,
		RouteGeneration:         sample.RouteGeneration,
		StatusCode:              sample.StatusCode,
		BodyReadBlockMS:         sample.BodyReadBlockMS,
		UploadEffectiveBPS:      sample.UploadEffectiveBPS,
		MinWindowBPS:            sample.MinWindowBPS,
		MaxReadGapMS:            sample.MaxReadGapMS,
		RequestBodyBytes:        sample.RequestBodyBytes,
		RequestBodyReadBytes:    sample.RequestBodyReadBytes,
		BodyIncompleteCount:     sample.BodyIncompleteCount,
		BodyReadErrorCount:      sample.BodyReadErrorCount,
		OriginDNSMS:             sample.OriginDNSMS,
		OriginConnectMS:         sample.OriginConnectMS,
		OriginEndpointConnectMS: sample.OriginEndpointConnectMS,
		OriginRequestWriteMS:    sample.OriginRequestWriteMS,
		OriginResponseWaitMS:    sample.OriginResponseWaitMS,
		OriginTTFBMS:            sample.OriginTTFBMS,
		OriginTotalMS:           sample.OriginTotalMS,
		OriginFailureClass:      firstNonEmpty(sample.OriginFailureClass, requestOriginFailureClassFromSample(sample)),
		ClientTCPRTTMS:          sample.ClientTCPRTTMS,
		ClientTCPRetransRate:    sample.ClientTCPRetransRate,
		ClientTCPRTORate:        sample.ClientTCPRTORate,
		ClientTCPDeliveryBPS:    sample.ClientTCPDeliveryBPS,
		Attribution:             attribution,
		FailureContracts:        requestFailureContractsFromAttribution(attribution),
		Evidence:                requestExplainEvidence(sample),
		SecretSafe:              true,
		SampledAt:               sample.SampledAt,
		GeneratedAt:             generatedAt,
	}
	if routeExplain, err := s.explainRouteForRobustness(r, sample.Hostname); err == nil {
		if route := requestRouteForSample(routeExplain.Routes, sample); route != nil {
			explain.RuntimeNode = route.RuntimeClusterNode
			if explain.Evidence == nil {
				explain.Evidence = map[string]string{}
			}
			explain.Evidence["runtime_node"] = route.RuntimeClusterNode
			explain.Evidence["runtime_id"] = route.RuntimeID
			explain.Evidence["route_generation_explain"] = route.RouteGeneration
		}
	}
	return explain
}

func requestErrorClassFromSample(sample model.EdgePerformanceSample) string {
	switch {
	case sample.BodyReadErrorCount > 0:
		return "edge.body_read_error"
	case sample.BodyIncompleteCount > 0:
		return "edge.body_incomplete"
	case sample.StatusCode == http.StatusServiceUnavailable:
		return requestUpstreamUnavailableClass(sample)
	case sample.StatusCode == http.StatusTooManyRequests:
		return "quota"
	case sample.StatusCode >= 500:
		return "origin.5xx"
	case sample.StatusCode == http.StatusUnauthorized || sample.StatusCode == http.StatusForbidden:
		return "auth"
	case sample.StatusCode >= 400:
		return "business.4xx"
	case sample.ErrorCount > 0:
		return "edge.error"
	default:
		return "none"
	}
}

func requestFailurePlane(errorClass string, sample model.EdgePerformanceSample) string {
	errorClass = strings.TrimSpace(strings.ToLower(errorClass))
	switch {
	case errorClass == "", errorClass == "none":
		return "none"
	case strings.HasPrefix(errorClass, "edge."), strings.HasPrefix(errorClass, "origin."):
		return "data_plane"
	case sample.OriginDNSMS > 0 || sample.OriginConnectMS > 0 || sample.OriginEndpointConnectMS > 0 || sample.OriginRequestWriteMS > 0 || sample.OriginResponseWaitMS > 0 || sample.OriginTTFBMS > 0 || strings.TrimSpace(sample.OriginFailureClass) != "":
		return "data_plane"
	case sample.BodyReadErrorCount > 0 || sample.BodyIncompleteCount > 0 || sample.MaxReadGapMS > 0 || sample.ClientTCPRetransRate > 0 || sample.ClientTCPRTORate > 0:
		return "data_plane"
	case errorClass == "auth":
		return "auth_control"
	case errorClass == "quota":
		return "quota_control"
	case strings.HasPrefix(errorClass, "business."):
		return "application"
	default:
		return "unknown"
	}
}

func requestUpstreamUnavailableClass(sample model.EdgePerformanceSample) string {
	switch requestOriginFailureClassFromSample(sample) {
	case "origin_dns_failed":
		return "edge.upstream_unavailable.origin_dns"
	case "origin_connect_failed":
		return "edge.upstream_unavailable.origin_connect"
	case "origin_endpoint_connect_failed":
		return "edge.upstream_unavailable.origin_endpoint_connect"
	case "origin_request_write_failed":
		return "edge.upstream_unavailable.origin_request_write"
	case "origin_ttfb_timeout":
		return "edge.upstream_unavailable.timeout"
	case "origin_5xx_or_slow":
		return "edge.upstream_unavailable.origin_slow"
	case "origin_unavailable":
		return "edge.upstream_unavailable.origin_unavailable"
	default:
		return "edge.upstream_unavailable.origin_unavailable"
	}
}

func requestOriginFailureClassFromSample(sample model.EdgePerformanceSample) string {
	if class := strings.TrimSpace(sample.OriginFailureClass); class != "" {
		return class
	}
	switch {
	case sample.OriginDNSMS > 0 && sample.OriginConnectMS == 0 && sample.OriginEndpointConnectMS == 0 && sample.OriginTTFBMS == 0:
		return "origin_dns_failed"
	case sample.OriginEndpointConnectMS > 0 && sample.OriginTTFBMS == 0:
		return "origin_endpoint_connect_failed"
	case sample.OriginConnectMS > 0 && sample.OriginTTFBMS == 0:
		return "origin_connect_failed"
	case sample.OriginRequestWriteMS > 0 && sample.OriginResponseWaitMS == 0 && sample.OriginTTFBMS == 0:
		return "origin_request_write_failed"
	case sample.OriginResponseWaitMS > 0 && sample.OriginTTFBMS == 0:
		return "origin_ttfb_timeout"
	case sample.OriginTTFBMS > 0:
		return "origin_5xx_or_slow"
	default:
		return ""
	}
}

func requestAttributionFromSample(sample model.EdgePerformanceSample) []string {
	out := []string{}
	if sample.BodyReadErrorCount > 0 || sample.BodyIncompleteCount > 0 || sample.UploadEffectiveBPS > 0 || sample.MaxReadGapMS > 0 {
		out = append(out, "client_to_edge_body_read")
	}
	if sample.ClientTCPRetransRate > 0 || sample.ClientTCPRTORate > 0 {
		out = append(out, "client_to_edge_tcp")
	}
	if sample.OriginDNSMS > 0 {
		out = append(out, "edge_to_origin_dns")
	}
	if sample.OriginConnectMS > 0 {
		out = append(out, "edge_to_origin_connect")
	}
	if sample.OriginEndpointConnectMS > 0 {
		out = append(out, "edge_to_origin_endpoint_connect")
	}
	if sample.OriginTTFBMS > 0 || sample.OriginResponseWaitMS > 0 {
		out = append(out, "origin_response_wait")
	}
	if len(out) == 0 {
		out = append(out, "edge_observed_request")
	}
	return out
}

func requestFailureContractsFromAttribution(attribution []string) []string {
	set := map[string]struct{}{}
	for _, item := range attribution {
		switch strings.TrimSpace(item) {
		case "client_to_edge_body_read", "client_to_edge_tcp":
			set["edge_front"] = struct{}{}
			set["edge_worker"] = struct{}{}
		case "edge_to_origin_dns", "edge_to_origin_connect", "edge_to_origin_endpoint_connect":
			set["edge_worker"] = struct{}{}
			set["kubernetes_cni_dns"] = struct{}{}
			set["runtime_scheduler"] = struct{}{}
		case "origin_response_wait":
			set["app_runtime"] = struct{}{}
			set["runtime_scheduler"] = struct{}{}
		default:
			set["observability_metrics"] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for item := range set {
		out = append(out, item)
	}
	sort.Strings(out)
	return out
}

func requestRouteForSample(routes []model.EdgeRouteBinding, sample model.EdgePerformanceSample) *model.EdgeRouteBinding {
	hostname := normalizeExternalAppDomain(sample.Hostname)
	pathPrefix := strings.TrimSpace(sample.PathPrefix)
	var best *model.EdgeRouteBinding
	for idx := range routes {
		route := &routes[idx]
		if !strings.EqualFold(normalizeExternalAppDomain(route.Hostname), hostname) {
			continue
		}
		if pathPrefix != "" && route.PathPrefix != "" && !strings.HasPrefix(pathPrefix, route.PathPrefix) {
			continue
		}
		if strings.TrimSpace(sample.EdgeGroupID) != "" && route.EdgeGroupID != "" && !strings.EqualFold(sample.EdgeGroupID, route.EdgeGroupID) {
			continue
		}
		if best == nil || len(route.PathPrefix) > len(best.PathPrefix) {
			best = route
		}
	}
	return best
}

func requestExplainEvidence(sample model.EdgePerformanceSample) map[string]string {
	return map[string]string{
		"sample_count":               fmt.Sprintf("%d", sample.SampleCount),
		"error_count":                fmt.Sprintf("%d", sample.ErrorCount),
		"route_generation":           sample.RouteGeneration,
		"dns_policy":                 sample.DNSPolicy,
		"cache_status":               sample.CacheStatus,
		"request_body_read_complete": fmt.Sprintf("%t", sample.RequestBodyBytes == 0 || sample.RequestBodyReadBytes >= sample.RequestBodyBytes),
	}
}

func platformArtifactKindList() []string {
	return []string{
		model.PlatformArtifactKindEdgeRouteBundle,
		model.PlatformArtifactKindDNSAnswerBundle,
		model.PlatformArtifactKindCaddyRouteConfig,
		model.PlatformArtifactKindDiscoveryBundle,
		model.PlatformArtifactKindNodeDesiredState,
		model.PlatformArtifactKindRuntimePlacementPlan,
		model.PlatformArtifactKindRuntimeContinuityPlan,
		model.PlatformArtifactKindNodeGuardianPolicy,
		model.PlatformArtifactKindReleaseGuardPolicy,
		model.PlatformArtifactKindEdgeRankingPolicy,
		model.PlatformArtifactKindTrafficSafetyPolicy,
		model.PlatformArtifactKindSubsystemFailureContracts,
		model.PlatformArtifactKindGatePolicyRegistry,
		model.PlatformArtifactKindAutomaticActionContracts,
	}
}
