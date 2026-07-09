package store

import (
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) RecordNodeDeepHealthResult(result model.NodeDeepHealthResult) (model.NodeDeepHealthResult, error) {
	result.NodeUpdaterID = strings.TrimSpace(result.NodeUpdaterID)
	if result.NodeUpdaterID == "" {
		return model.NodeDeepHealthResult{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgRecordNodeDeepHealthResult(result)
	}
	result = normalizeNodeDeepHealthResult(result, time.Now().UTC())
	var saved model.NodeDeepHealthResult
	err := s.withLockedState(true, func(state *model.State) error {
		for idx := range state.NodeDeepHealthResults {
			if strings.TrimSpace(state.NodeDeepHealthResults[idx].NodeUpdaterID) == result.NodeUpdaterID {
				state.NodeDeepHealthResults[idx] = result
				saved = result
				return nil
			}
		}
		state.NodeDeepHealthResults = append(state.NodeDeepHealthResults, result)
		saved = result
		return nil
	})
	if err != nil {
		return model.NodeDeepHealthResult{}, err
	}
	return saved, nil
}

func (s *Store) ListNodeDeepHealthResults() ([]model.NodeDeepHealthResult, error) {
	if s.usingDatabase() {
		return s.pgListNodeDeepHealthResults()
	}
	results := []model.NodeDeepHealthResult{}
	err := s.withLockedState(false, func(state *model.State) error {
		results = append(results, state.NodeDeepHealthResults...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sortNodeDeepHealthResults(results)
	return results, nil
}

func (s *Store) GetNodeDeepHealthResult(nodeUpdaterID string) (model.NodeDeepHealthResult, error) {
	nodeUpdaterID = strings.TrimSpace(nodeUpdaterID)
	if nodeUpdaterID == "" {
		return model.NodeDeepHealthResult{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetNodeDeepHealthResult(nodeUpdaterID)
	}
	var result model.NodeDeepHealthResult
	err := s.withLockedState(false, func(state *model.State) error {
		for _, candidate := range state.NodeDeepHealthResults {
			if strings.TrimSpace(candidate.NodeUpdaterID) == nodeUpdaterID {
				result = candidate
				return nil
			}
		}
		return ErrNotFound
	})
	if err != nil {
		return model.NodeDeepHealthResult{}, err
	}
	return result, nil
}

func normalizeNodeDeepHealthResult(result model.NodeDeepHealthResult, now time.Time) model.NodeDeepHealthResult {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	result.NodeUpdaterID = strings.TrimSpace(result.NodeUpdaterID)
	result.ClusterNodeName = strings.TrimSpace(result.ClusterNodeName)
	result.RuntimeID = strings.TrimSpace(result.RuntimeID)
	result.MachineID = strings.TrimSpace(result.MachineID)
	result.ObservedOnly = true
	if result.ReportedAt.IsZero() {
		result.ReportedAt = now
	}
	result.UpdatedAt = now
	result.Checks = normalizeNodeDeepHealthChecks(result.Checks, result.ReportedAt)
	result.OverallStatus, result.QuarantineState, result.QuarantineReason = nodeDeepHealthDecision(result.Checks, result.ObservedOnly)
	if result.ObservedOnly {
		result.QuarantineExpiresAt = nil
	} else if result.QuarantineState == model.NodeQuarantineStateQuarantined && result.QuarantineExpiresAt == nil {
		expires := now.Add(15 * time.Minute)
		result.QuarantineExpiresAt = &expires
	}
	if result.QuarantineState == model.NodeQuarantineStateClear {
		result.QuarantineExpiresAt = nil
		result.RecoveryConditions = nil
	} else if len(result.RecoveryConditions) == 0 {
		result.RecoveryConditions = []string{"all hard-fail checks must pass on a subsequent deep health report"}
	}
	return result
}

func normalizeNodeDeepHealthChecks(checks []model.NodeDeepHealthCheck, fallback time.Time) []model.NodeDeepHealthCheck {
	out := make([]model.NodeDeepHealthCheck, 0, len(checks))
	for _, check := range checks {
		check.Name = strings.TrimSpace(check.Name)
		if check.Name == "" {
			continue
		}
		check.Category = strings.TrimSpace(check.Category)
		check.Status = normalizeNodeDeepHealthStatus(check.Status)
		check.GateID = normalizeNodeDeepHealthGateID(check)
		check.GateMode = normalizeNodeDeepHealthGateMode(check.GateMode)
		if check.GateMode == "" {
			check.GateMode = model.GatePolicyModeShadow
		}
		if check.GateMode == model.GatePolicyModeEnforced && !knownNodeDeepHealthGateID(check.GateID) {
			check.GateMode = model.GatePolicyModeShadow
			check.HardFail = false
			if check.Evidence == nil {
				check.Evidence = map[string]string{}
			}
			check.Evidence["gate_policy"] = "unknown_gate_downgraded_to_shadow"
		}
		if check.GateMode == model.GatePolicyModeShadow || check.GateMode == model.GatePolicyModeDisabled {
			check.HardFail = false
		}
		check.Expected = strings.TrimSpace(check.Expected)
		check.Observed = strings.TrimSpace(check.Observed)
		check.Message = strings.TrimSpace(check.Message)
		check.RepairAction = strings.TrimSpace(check.RepairAction)
		if check.CheckedAt.IsZero() {
			check.CheckedAt = fallback
		}
		out = append(out, check)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out
}

func normalizeNodeDeepHealthGateMode(raw string) string {
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

func normalizeNodeDeepHealthGateID(check model.NodeDeepHealthCheck) string {
	if gateID := strings.TrimSpace(check.GateID); gateID != "" {
		return gateID
	}
	switch check.Name {
	case model.NodeDeepHealthCheckPodDNSToKubeDNSService,
		model.NodeDeepHealthCheckPodDNSToCoreDNSPod,
		model.NodeDeepHealthCheckKubernetesDefaultDNS,
		model.NodeDeepHealthCheckNamespaceServiceDNS,
		model.NodeDeepHealthCheckNamespaceServiceTCP,
		model.NodeDeepHealthCheckExternalDNS:
		return "node.kubernetes_service_dns"
	case model.NodeDeepHealthCheckManagedIptablesStale,
		model.NodeDeepHealthCheckKubeProxyRules:
		return "node.kube_proxy_rules"
	case model.NodeDeepHealthCheckCNIBridge:
		return "node.cni_bridge"
	case model.NodeDeepHealthCheckConntrackSaturation:
		return "node.conntrack_saturation"
	case model.NodeDeepHealthCheckUpdaterGenerationDrift:
		return "node_updater.generation_rollout"
	default:
		name := strings.TrimSpace(check.Name)
		if name == "" {
			return ""
		}
		return "node." + name
	}
}

func knownNodeDeepHealthGateID(gateID string) bool {
	switch strings.TrimSpace(gateID) {
	case "node.kubernetes_service_dns",
		"node.kube_proxy_rules",
		"node.cni_bridge",
		"node.conntrack_saturation",
		"node_updater.generation_rollout":
		return true
	default:
		return false
	}
}

func normalizeNodeDeepHealthStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case model.NodeDeepHealthStatusPass:
		return model.NodeDeepHealthStatusPass
	case model.NodeDeepHealthStatusWarning:
		return model.NodeDeepHealthStatusWarning
	case model.NodeDeepHealthStatusFail, "failed", "error":
		return model.NodeDeepHealthStatusFail
	default:
		return model.NodeDeepHealthStatusWarning
	}
}

func nodeDeepHealthDecision(checks []model.NodeDeepHealthCheck, observedOnly bool) (string, string, string) {
	overall := model.NodeDeepHealthStatusPass
	for _, check := range checks {
		if check.Status == model.NodeDeepHealthStatusWarning && overall == model.NodeDeepHealthStatusPass {
			overall = model.NodeDeepHealthStatusWarning
		}
		if check.Status != model.NodeDeepHealthStatusFail {
			continue
		}
		overall = model.NodeDeepHealthStatusFail
		if check.HardFail {
			if observedOnly {
				continue
			}
			return overall, model.NodeQuarantineStateQuarantined, nodeQuarantineReasonForCheck(check)
		}
	}
	if overall == model.NodeDeepHealthStatusPass {
		return overall, model.NodeQuarantineStateClear, ""
	}
	return overall, model.NodeQuarantineStateDegraded, "warning_or_soft_fail"
}

func nodeQuarantineReasonForCheck(check model.NodeDeepHealthCheck) string {
	switch check.Name {
	case model.NodeDeepHealthCheckPodDNSToKubeDNSService,
		model.NodeDeepHealthCheckPodDNSToCoreDNSPod,
		model.NodeDeepHealthCheckKubernetesDefaultDNS,
		model.NodeDeepHealthCheckNamespaceServiceDNS,
		model.NodeDeepHealthCheckExternalDNS:
		return model.NodeQuarantineReasonDNSHardFail
	case model.NodeDeepHealthCheckManagedIptablesStale:
		return model.NodeQuarantineReasonIptablesHardFail
	case model.NodeDeepHealthCheckPodCIDRDrift:
		return model.NodeQuarantineReasonPodCIDRDrift
	case model.NodeDeepHealthCheckK3SAgentProcess,
		model.NodeDeepHealthCheckKubeletProcess,
		model.NodeDeepHealthCheckLocalAPIServer,
		model.NodeDeepHealthCheckRemoteDialer,
		model.NodeDeepHealthCheckNodeLeaseFreshness,
		model.NodeDeepHealthCheckPodSandboxCreation:
		return model.NodeQuarantineReasonK3SAgentHardFail
	case model.NodeDeepHealthCheckCNIBridge,
		model.NodeDeepHealthCheckKubeProxyRules:
		return model.NodeQuarantineReasonCNIHardFail
	case model.NodeDeepHealthCheckDiskInodePressure,
		model.NodeDeepHealthCheckMemoryPressure,
		model.NodeDeepHealthCheckCPULoadPressure:
		return model.NodeQuarantineReasonResourcePressure
	case model.NodeDeepHealthCheckConntrackSaturation:
		return model.NodeQuarantineReasonConntrackSaturated
	default:
		return "hard_fail"
	}
}

func sortNodeDeepHealthResults(results []model.NodeDeepHealthResult) {
	sort.SliceStable(results, func(i, j int) bool {
		if !results[i].UpdatedAt.Equal(results[j].UpdatedAt) {
			return results[i].UpdatedAt.After(results[j].UpdatedAt)
		}
		return strings.TrimSpace(results[i].NodeUpdaterID) < strings.TrimSpace(results[j].NodeUpdaterID)
	})
}
