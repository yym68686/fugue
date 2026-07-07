package model

import "strings"

const (
	ReleaseSignalOwnerScopePlatform          = "platform"
	ReleaseSignalOwnerScopeFirstPartyService = "first_party_service"
	ReleaseSignalOwnerScopeTenantWorkload    = "tenant_workload"

	ReleaseSignalGateScopeControlPlane   = "control_plane"
	ReleaseSignalGateScopeEdgeRollout    = "edge_rollout"
	ReleaseSignalGateScopeRuntimeRollout = "runtime_rollout"
	ReleaseSignalGateScopeTenantTraffic  = "tenant_traffic"
	ReleaseSignalGateScopeReportOnly     = "report_only"

	ReleaseSignalModeReportOnly   = "report_only"
	ReleaseSignalModeSoftGate     = "soft_gate"
	ReleaseSignalModeCanaryGate   = "canary_gate"
	ReleaseSignalModeRollbackGate = "rollback_gate"
	ReleaseSignalModeHardGate     = "hard_gate"
)

type ReleaseSignalPolicy struct {
	Version string          `json:"version"`
	Signals []ReleaseSignal `json:"signals,omitempty"`
}

type ReleaseSignal struct {
	ID         string `json:"id"`
	Name       string `json:"name,omitempty"`
	Enabled    bool   `json:"enabled"`
	OwnerScope string `json:"owner_scope"`
	GateScope  string `json:"gate_scope"`
	Mode       string `json:"mode"`
	Subject    string `json:"subject"`
	CheckName  string `json:"check_name,omitempty"`
	Reason     string `json:"reason,omitempty"`
	CreatedAt  string `json:"created_at,omitempty"`
	UpdatedAt  string `json:"updated_at,omitempty"`
}

func NormalizeReleaseSignalOwnerScope(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case ReleaseSignalOwnerScopePlatform:
		return ReleaseSignalOwnerScopePlatform
	case ReleaseSignalOwnerScopeFirstPartyService:
		return ReleaseSignalOwnerScopeFirstPartyService
	case ReleaseSignalOwnerScopeTenantWorkload:
		return ReleaseSignalOwnerScopeTenantWorkload
	default:
		return ""
	}
}

func NormalizeReleaseSignalGateScope(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case ReleaseSignalGateScopeControlPlane:
		return ReleaseSignalGateScopeControlPlane
	case ReleaseSignalGateScopeEdgeRollout:
		return ReleaseSignalGateScopeEdgeRollout
	case ReleaseSignalGateScopeRuntimeRollout:
		return ReleaseSignalGateScopeRuntimeRollout
	case ReleaseSignalGateScopeTenantTraffic:
		return ReleaseSignalGateScopeTenantTraffic
	case ReleaseSignalGateScopeReportOnly:
		return ReleaseSignalGateScopeReportOnly
	default:
		return ""
	}
}

func NormalizeReleaseSignalMode(raw string) string {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case ReleaseSignalModeReportOnly:
		return ReleaseSignalModeReportOnly
	case ReleaseSignalModeSoftGate:
		return ReleaseSignalModeSoftGate
	case ReleaseSignalModeCanaryGate:
		return ReleaseSignalModeCanaryGate
	case ReleaseSignalModeRollbackGate:
		return ReleaseSignalModeRollbackGate
	case ReleaseSignalModeHardGate:
		return ReleaseSignalModeHardGate
	default:
		return ""
	}
}

func ReleaseSignalBlocksControlPlane(signal ReleaseSignal) bool {
	return NormalizeReleaseSignalGateScope(signal.GateScope) == ReleaseSignalGateScopeControlPlane &&
		NormalizeReleaseSignalMode(signal.Mode) == ReleaseSignalModeHardGate
}
