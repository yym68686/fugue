package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

func (s *Server) handleListGatePolicies(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	policies := s.gatePolicyRegistry()
	httpx.WriteJSON(w, http.StatusOK, model.GatePolicyListResponse{Policies: policies, GeneratedAt: time.Now().UTC()})
}

func (s *Server) handleGetGatePolicy(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	policy, ok := gatePolicyByID(s.gatePolicyRegistry(), r.PathValue("gate_id"))
	if !ok {
		httpx.WriteError(w, http.StatusNotFound, "gate policy not found")
		return
	}
	httpx.WriteJSON(w, http.StatusOK, model.GatePolicyResponse{Policy: policy})
}

func (s *Server) handlePromoteGatePolicy(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	if !principal.IsPlatformAdmin() {
		httpx.WriteError(w, http.StatusForbidden, "platform admin required")
		return
	}
	var req model.GatePolicyPromoteRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	mode := normalizeGatePolicyMode(req.Mode)
	if mode == "" {
		httpx.WriteError(w, http.StatusBadRequest, "mode must be one of shadow, canary, enforced, disabled")
		return
	}
	policies := s.gatePolicyRegistry()
	index := gatePolicyIndex(policies, r.PathValue("gate_id"))
	if index < 0 {
		httpx.WriteError(w, http.StatusNotFound, "gate policy not found")
		return
	}
	if mode == model.GatePolicyModeEnforced && strings.TrimSpace(req.Reason) == "" {
		httpx.WriteError(w, http.StatusBadRequest, "reason is required when promoting a gate to enforced")
		return
	}
	now := time.Now().UTC()
	policies[index].Mode = mode
	policies[index].UpdatedAt = now
	policies[index].UpdatedBy = principal.ActorID
	policies[index].PromotionReason = strings.TrimSpace(req.Reason)
	if strings.TrimSpace(req.IntroducedByRelease) != "" {
		policies[index].IntroducedByRelease = strings.TrimSpace(req.IntroducedByRelease)
	}
	if len(req.CanaryScopes) > 0 {
		policies[index].CanaryFailureDomains = uniqueSortedStrings(req.CanaryScopes)
	}
	if policies[index].SoakStartedAt == nil && mode != model.GatePolicyModeShadow {
		start := now
		policies[index].SoakStartedAt = &start
	}
	content := map[string]any{
		"version":    "v1",
		"policies":   policies,
		"updated_at": now,
	}
	artifact, err := s.store.CreatePlatformArtifact(model.PlatformArtifact{
		ArtifactKind: model.PlatformArtifactKindGatePolicyRegistry,
		Scope:        model.PlatformArtifactScope{ScopeType: "global"},
		Generation:   fmt.Sprintf("gate_policy_registry_%d", now.Unix()),
		Content:      content,
		Metadata: map[string]string{
			"gate_id": policies[index].ID,
			"mode":    mode,
			"reason":  strings.TrimSpace(req.Reason),
		},
		CreatedByType: principal.ActorType,
		CreatedByID:   principal.ActorID,
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	results := validatePlatformArtifactDraft(artifact)
	if !platformArtifactValidationPass(results) {
		artifact, _ = s.store.ValidatePlatformArtifact(artifact.ID, results)
		httpx.WriteJSON(w, http.StatusConflict, model.PlatformArtifactValidationResponse{Artifact: artifact, Results: results, Pass: false, DryRun: false})
		return
	}
	artifact, err = s.store.ValidatePlatformArtifact(artifact.ID, results)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	releaseChannel := model.PlatformArtifactReleaseChannelFull
	if currentLKG, lkgErr := s.store.GetPlatformLKG(model.PlatformArtifactKindGatePolicyRegistry, "global"); lkgErr != nil {
		s.writeStoreError(w, lkgErr)
		return
	} else if currentLKG == nil {
		releaseChannel = model.PlatformArtifactReleaseChannelShadow
	}
	artifact, release, message, lkg, err := s.store.ReleasePlatformArtifact(artifact.ID, model.PlatformArtifactReleaseRequest{
		ReleaseChannel: releaseChannel,
		Reason:         firstNonEmpty(strings.TrimSpace(req.Reason), "gate policy promotion"),
	}, principal)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	s.appendAudit(principal, "gate_policy.promote", "gate_policy", policies[index].ID, "", map[string]string{
		"mode":       mode,
		"reason":     strings.TrimSpace(req.Reason),
		"generation": artifact.Generation,
	})
	httpx.WriteJSON(w, http.StatusOK, model.GatePolicyPromotionResponse{
		Policy:   policies[index],
		Artifact: artifact,
		Release:  release,
		Message:  message,
		LKG:      lkg,
	})
}

func (s *Server) gatePolicyRegistry() []model.GatePolicy {
	policies := defaultGatePolicies()
	if artifact, ok, err := s.verifiedPlatformArtifactForScope(model.PlatformArtifactKindGatePolicyRegistry, "global"); err == nil && ok {
		if overrides, valid := gatePoliciesFromArtifact(artifact); valid && len(overrides) > 0 {
			policies = mergeGatePolicies(policies, overrides)
		}
	}
	for index := range policies {
		policies[index].Mode = effectiveGatePolicyMode(policies[index])
	}
	sort.SliceStable(policies, func(i, j int) bool { return policies[i].ID < policies[j].ID })
	return policies
}

func defaultGatePolicies() []model.GatePolicy {
	now := time.Date(2026, 7, 9, 0, 0, 0, 0, time.UTC)
	rollbackSignals := []string{"public_synthetic_503_no_healthy_edge_groups", "release_guard_block_rollout"}
	return []model.GatePolicy{
		{ID: "node.kubernetes_service_dns", Description: "pod-network Kubernetes Service DNS deep health", Mode: model.GatePolicyModeShadow, DefaultMode: model.GatePolicyModeShadow, Scope: model.GatePolicyScopeNode, IntroducedAt: now, SoakMinDuration: "24h", MinimumSamples: 3, MinimumFailureDomains: 2, BlastRadius: model.GateBlastRadiusPolicy{MaxNodes: 1}, RollbackOn: rollbackSignals, KillSwitchEnv: "FUGUE_GATE_NODE_KUBERNETES_SERVICE_DNS_MODE", RunbookRef: "docs/runbooks/node-dns-failure.md"},
		{ID: "node.kube_proxy_rules", Description: "kube-proxy and managed iptables deep health", Mode: model.GatePolicyModeShadow, DefaultMode: model.GatePolicyModeShadow, Scope: model.GatePolicyScopeNode, IntroducedAt: now, SoakMinDuration: "24h", MinimumSamples: 3, MinimumFailureDomains: 2, BlastRadius: model.GateBlastRadiusPolicy{MaxNodes: 1}, RollbackOn: rollbackSignals, KillSwitchEnv: "FUGUE_GATE_NODE_KUBE_PROXY_RULES_MODE", RunbookRef: "docs/runbooks/stale-iptables-managed-rule.md"},
		{ID: "node.cni_bridge", Description: "node CNI bridge health", Mode: model.GatePolicyModeShadow, DefaultMode: model.GatePolicyModeShadow, Scope: model.GatePolicyScopeNode, IntroducedAt: now, SoakMinDuration: "24h", MinimumSamples: 3, MinimumFailureDomains: 2, BlastRadius: model.GateBlastRadiusPolicy{MaxNodes: 1}, RollbackOn: rollbackSignals, KillSwitchEnv: "FUGUE_GATE_NODE_CNI_BRIDGE_MODE", RunbookRef: "docs/runbooks/node-dns-failure.md"},
		{ID: "node.conntrack_saturation", Description: "node conntrack saturation health", Mode: model.GatePolicyModeShadow, DefaultMode: model.GatePolicyModeShadow, Scope: model.GatePolicyScopeNode, IntroducedAt: now, SoakMinDuration: "24h", MinimumSamples: 3, MinimumFailureDomains: 2, BlastRadius: model.GateBlastRadiusPolicy{MaxNodes: 1}, RollbackOn: rollbackSignals, KillSwitchEnv: "FUGUE_GATE_NODE_CONNTRACK_SATURATION_MODE", RunbookRef: "docs/runbooks/node-dns-failure.md"},
		{ID: "edge.route_inventory_quarantine", Description: "edge route inventory quarantine filtering", Mode: model.GatePolicyModeShadow, DefaultMode: model.GatePolicyModeShadow, Scope: model.GatePolicyScopeEdgeGroup, IntroducedAt: now, SoakMinDuration: "24h", MinimumSamples: 3, MinimumFailureDomains: 2, BlastRadius: model.GateBlastRadiusPolicy{MaxEdgesPerGroup: 1, PreserveMinHealthyEdgeGroups: 1}, RollbackOn: rollbackSignals, KillSwitchEnv: "FUGUE_GATE_EDGE_ROUTE_INVENTORY_QUARANTINE_MODE", RunbookRef: "docs/runbooks/edge-route-all-unhealthy.md"},
		{ID: "dns.answer_route_ready", Description: "DNS answer route-ready filtering", Mode: model.GatePolicyModeShadow, DefaultMode: model.GatePolicyModeShadow, Scope: model.GatePolicyScopeHostname, IntroducedAt: now, SoakMinDuration: "24h", MinimumSamples: 3, MinimumFailureDomains: 2, BlastRadius: model.GateBlastRadiusPolicy{PreserveMinEligibleEdgesPerHost: 1}, RollbackOn: rollbackSignals, KillSwitchEnv: "FUGUE_GATE_DNS_ANSWER_ROUTE_READY_MODE", RunbookRef: "docs/runbooks/edge-route-all-unhealthy.md"},
		{ID: "scheduler.node_quarantine", Description: "scheduler exclusion of quarantined runtime nodes", Mode: model.GatePolicyModeShadow, DefaultMode: model.GatePolicyModeShadow, Scope: model.GatePolicyScopeRuntime, IntroducedAt: now, SoakMinDuration: "24h", MinimumSamples: 3, MinimumFailureDomains: 2, BlastRadius: model.GateBlastRadiusPolicy{MaxNodes: 1}, RollbackOn: rollbackSignals, KillSwitchEnv: "FUGUE_GATE_SCHEDULER_NODE_QUARANTINE_MODE", RunbookRef: "docs/runbooks/release-guard-blocked.md"},
		{ID: "public.synthetic_503", Description: "public synthetic probes detecting Fugue 503 classes", Mode: model.GatePolicyModeEnforced, DefaultMode: model.GatePolicyModeEnforced, Scope: model.GatePolicyScopeCluster, IntroducedAt: now, SoakMinDuration: "0s", MinimumSamples: 1, BlastRadius: model.GateBlastRadiusPolicy{PreserveMinHealthyEdgeGroups: 1}, RollbackOn: rollbackSignals, KillSwitchEnv: "FUGUE_GATE_PUBLIC_SYNTHETIC_503_MODE", RunbookRef: "docs/runbooks/bad-control-plane-release-rollback.md"},
		{ID: "release_guard.block_rollout", Description: "post-deploy release guard hard rollback gate", Mode: model.GatePolicyModeEnforced, DefaultMode: model.GatePolicyModeEnforced, Scope: model.GatePolicyScopeCluster, IntroducedAt: now, SoakMinDuration: "0s", MinimumSamples: 1, BlastRadius: model.GateBlastRadiusPolicy{PreserveMinHealthyEdgeGroups: 1}, RollbackOn: rollbackSignals, KillSwitchEnv: "FUGUE_GATE_RELEASE_GUARD_BLOCK_ROLLOUT_MODE", RunbookRef: "docs/runbooks/release-guard-blocked.md"},
		{ID: "node_updater.generation_rollout", Description: "node-updater desired generation rollout", Mode: model.GatePolicyModeCanary, DefaultMode: model.GatePolicyModeCanary, Scope: model.GatePolicyScopeNode, IntroducedAt: now, SoakMinDuration: "1h", MinimumSamples: 1, MinimumFailureDomains: 1, CanaryFailureDomains: []string{"single-node"}, BlastRadius: model.GateBlastRadiusPolicy{MaxNodes: 1}, RollbackOn: rollbackSignals, KillSwitchEnv: "FUGUE_GATE_NODE_UPDATER_GENERATION_ROLLOUT_MODE", RunbookRef: "docs/runbooks/node-updater-canary-restore.md"},
	}
}

func gatePoliciesFromArtifact(artifact model.PlatformArtifact) ([]model.GatePolicy, bool) {
	raw, ok := artifact.Content["policies"]
	if !ok {
		return nil, false
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return nil, false
	}
	var policies []model.GatePolicy
	if err := json.Unmarshal(data, &policies); err != nil {
		return nil, false
	}
	return policies, true
}

func mergeGatePolicies(defaults, overrides []model.GatePolicy) []model.GatePolicy {
	out := append([]model.GatePolicy(nil), defaults...)
	for _, override := range overrides {
		override.ID = strings.TrimSpace(override.ID)
		if override.ID == "" {
			continue
		}
		if index := gatePolicyIndex(out, override.ID); index >= 0 {
			out[index] = mergeGatePolicy(out[index], override)
			continue
		}
		out = append(out, override)
	}
	return out
}

func mergeGatePolicy(base, override model.GatePolicy) model.GatePolicy {
	if strings.TrimSpace(override.Mode) != "" {
		base.Mode = normalizeGatePolicyMode(override.Mode)
	}
	if strings.TrimSpace(override.Scope) != "" {
		base.Scope = normalizeGatePolicyScope(override.Scope)
	}
	if strings.TrimSpace(override.IntroducedByRelease) != "" {
		base.IntroducedByRelease = strings.TrimSpace(override.IntroducedByRelease)
	}
	if override.SoakStartedAt != nil {
		base.SoakStartedAt = override.SoakStartedAt
	}
	if strings.TrimSpace(override.SoakMinDuration) != "" {
		base.SoakMinDuration = strings.TrimSpace(override.SoakMinDuration)
	}
	if override.MinimumSamples > 0 {
		base.MinimumSamples = override.MinimumSamples
	}
	if override.MinimumFailureDomains > 0 {
		base.MinimumFailureDomains = override.MinimumFailureDomains
	}
	if len(override.CanaryFailureDomains) > 0 {
		base.CanaryFailureDomains = uniqueSortedStrings(override.CanaryFailureDomains)
	}
	if override.BlastRadius != (model.GateBlastRadiusPolicy{}) {
		base.BlastRadius = override.BlastRadius
	}
	if len(override.RollbackOn) > 0 {
		base.RollbackOn = uniqueSortedStrings(override.RollbackOn)
	}
	if strings.TrimSpace(override.KillSwitchEnv) != "" {
		base.KillSwitchEnv = strings.TrimSpace(override.KillSwitchEnv)
	}
	if strings.TrimSpace(override.RunbookRef) != "" {
		base.RunbookRef = strings.TrimSpace(override.RunbookRef)
	}
	if !override.UpdatedAt.IsZero() {
		base.UpdatedAt = override.UpdatedAt
	}
	base.UpdatedBy = strings.TrimSpace(override.UpdatedBy)
	base.PromotionReason = strings.TrimSpace(override.PromotionReason)
	return base
}

func effectiveGatePolicyMode(policy model.GatePolicy) string {
	if strings.TrimSpace(policy.KillSwitchEnv) != "" {
		if override := normalizeGatePolicyMode(os.Getenv(strings.TrimSpace(policy.KillSwitchEnv))); override != "" {
			return override
		}
	}
	if mode := normalizeGatePolicyMode(policy.Mode); mode != "" {
		return mode
	}
	if mode := normalizeGatePolicyMode(policy.DefaultMode); mode != "" {
		return mode
	}
	return model.GatePolicyModeShadow
}

func gatePolicyValidationResult(artifact model.PlatformArtifact) model.PlatformArtifactValidationResult {
	policies, ok := gatePoliciesFromArtifact(artifact)
	if !ok || len(policies) == 0 {
		return model.PlatformArtifactValidationResult{Name: "invariant.gate_policy_registry", Pass: false, Severity: model.RobustnessSeverityBlockPublish, Message: "gate_policy_registry artifacts must include a non-empty policies array"}
	}
	ids := map[string]struct{}{}
	for _, policy := range policies {
		id := strings.TrimSpace(policy.ID)
		if id == "" {
			return model.PlatformArtifactValidationResult{Name: "invariant.gate_policy_registry", Pass: false, Severity: model.RobustnessSeverityBlockPublish, Message: "gate policy id is required"}
		}
		if _, exists := ids[id]; exists {
			return model.PlatformArtifactValidationResult{Name: "invariant.gate_policy_registry", Pass: false, Severity: model.RobustnessSeverityBlockPublish, Message: "gate policy ids must be unique"}
		}
		ids[id] = struct{}{}
		if normalizeGatePolicyMode(policy.Mode) == "" {
			return model.PlatformArtifactValidationResult{Name: "invariant.gate_policy_registry", Pass: false, Severity: model.RobustnessSeverityBlockPublish, Message: "gate policy mode must be shadow, canary, enforced, or disabled", Evidence: map[string]string{"gate_id": id, "mode": policy.Mode}}
		}
		if normalizeGatePolicyScope(policy.Scope) == "" {
			return model.PlatformArtifactValidationResult{Name: "invariant.gate_policy_registry", Pass: false, Severity: model.RobustnessSeverityBlockPublish, Message: "gate policy scope is invalid", Evidence: map[string]string{"gate_id": id, "scope": policy.Scope}}
		}
		if normalizeGatePolicyMode(policy.Mode) == model.GatePolicyModeEnforced && strings.TrimSpace(policy.KillSwitchEnv) == "" {
			return model.PlatformArtifactValidationResult{Name: "invariant.gate_policy_registry", Pass: false, Severity: model.RobustnessSeverityBlockPublish, Message: "enforced gate policies must declare kill_switch_env", Evidence: map[string]string{"gate_id": id}}
		}
	}
	return model.PlatformArtifactValidationResult{Name: "invariant.gate_policy_registry", Pass: true, Severity: model.RobustnessSeverityBlockPublish, Message: "gate policies are valid"}
}

func releaseGuardGatePolicyViolations(policies []model.GatePolicy) []string {
	violations := []string{}
	for _, policy := range policies {
		mode := normalizeGatePolicyMode(policy.Mode)
		if mode == "" {
			violations = append(violations, fmt.Sprintf("gate %s has invalid mode %q", policy.ID, policy.Mode))
			continue
		}
		if normalizeGatePolicyScope(policy.Scope) == "" {
			violations = append(violations, fmt.Sprintf("gate %s has invalid scope %q", policy.ID, policy.Scope))
		}
		if (mode == model.GatePolicyModeCanary || mode == model.GatePolicyModeEnforced) && strings.TrimSpace(policy.KillSwitchEnv) == "" {
			violations = append(violations, fmt.Sprintf("gate %s is %s without kill_switch_env", policy.ID, mode))
		}
		if mode == model.GatePolicyModeCanary && len(policy.CanaryFailureDomains) == 0 && policy.Scope != model.GatePolicyScopeCluster {
			violations = append(violations, fmt.Sprintf("gate %s is canary without canary_failure_domains", policy.ID))
		}
		if mode == model.GatePolicyModeEnforced && strings.TrimSpace(policy.RunbookRef) == "" {
			violations = append(violations, fmt.Sprintf("gate %s is enforced without runbook_ref", policy.ID))
		}
	}
	return violations
}

func gatePolicyByID(policies []model.GatePolicy, id string) (model.GatePolicy, bool) {
	index := gatePolicyIndex(policies, id)
	if index < 0 {
		return model.GatePolicy{}, false
	}
	return policies[index], true
}

func gatePolicyIndex(policies []model.GatePolicy, id string) int {
	id = strings.TrimSpace(id)
	for index, policy := range policies {
		if strings.TrimSpace(policy.ID) == id {
			return index
		}
	}
	return -1
}

func normalizeGatePolicyMode(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case model.GatePolicyModeShadow:
		return model.GatePolicyModeShadow
	case model.GatePolicyModeCanary:
		return model.GatePolicyModeCanary
	case model.GatePolicyModeEnforced:
		return model.GatePolicyModeEnforced
	case model.GatePolicyModeDisabled:
		return model.GatePolicyModeDisabled
	default:
		return ""
	}
}

func normalizeGatePolicyScope(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case model.GatePolicyScopeCluster:
		return model.GatePolicyScopeCluster
	case model.GatePolicyScopeNode:
		return model.GatePolicyScopeNode
	case model.GatePolicyScopeEdgeNode:
		return model.GatePolicyScopeEdgeNode
	case model.GatePolicyScopeEdgeGroup:
		return model.GatePolicyScopeEdgeGroup
	case model.GatePolicyScopeHostname:
		return model.GatePolicyScopeHostname
	case model.GatePolicyScopeService:
		return model.GatePolicyScopeService
	case model.GatePolicyScopeRuntime:
		return model.GatePolicyScopeRuntime
	default:
		return ""
	}
}
