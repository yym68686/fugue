package model

import "time"

const (
	NodeDeepHealthStatusPass    = "pass"
	NodeDeepHealthStatusWarning = "warning"
	NodeDeepHealthStatusFail    = "fail"

	NodeDeepHealthCheckPodDNSToKubeDNSService = "pod_dns_to_kube_dns_service"
	NodeDeepHealthCheckPodDNSToCoreDNSPod     = "pod_dns_to_coredns_pod"
	NodeDeepHealthCheckKubernetesDefaultDNS   = "kubernetes_default_svc_dns"
	NodeDeepHealthCheckNamespaceServiceDNS    = "same_namespace_service_dns"
	NodeDeepHealthCheckNamespaceServiceTCP    = "same_namespace_service_tcp"
	NodeDeepHealthCheckExternalDNS            = "external_dns"
	NodeDeepHealthCheckManagedIptablesStale   = "managed_iptables_stale_rule"
	NodeDeepHealthCheckPodCIDRDrift           = "pod_cidr_drift"
	NodeDeepHealthCheckConntrackSaturation    = "conntrack_saturation"
	NodeDeepHealthCheckUpdaterGenerationDrift = "node_updater_generation_drift"

	NodeQuarantineStateClear       = "clear"
	NodeQuarantineStateDegraded    = "degraded"
	NodeQuarantineStateQuarantined = "quarantined"

	NodeQuarantineReasonDNSHardFail        = "dns_hard_fail"
	NodeQuarantineReasonCNIHardFail        = "cni_hard_fail"
	NodeQuarantineReasonIptablesHardFail   = "iptables_hard_fail"
	NodeQuarantineReasonPodCIDRDrift       = "pod_cidr_drift"
	NodeQuarantineReasonConntrackSaturated = "conntrack_saturated"

	NodeRepairSafetyObserveOnly    = "observe_only"
	NodeRepairSafetyDryRun         = "dry_run"
	NodeRepairSafetyAutomaticSafe  = "automatic_safe"
	NodeRepairSafetyManualApproval = "manual_approval"
	NodeRepairSafetyNeverAutomatic = "never_automatic"
)

type NodeDeepHealthCheck struct {
	Name         string            `json:"name"`
	Category     string            `json:"category,omitempty"`
	Status       string            `json:"status"`
	Expected     string            `json:"expected,omitempty"`
	Observed     string            `json:"observed,omitempty"`
	Message      string            `json:"message,omitempty"`
	HardFail     bool              `json:"hard_fail,omitempty"`
	RepairAction string            `json:"repair_action,omitempty"`
	Evidence     map[string]string `json:"evidence,omitempty"`
	CheckedAt    time.Time         `json:"checked_at,omitempty"`
}

type NodeDeepHealthResult struct {
	NodeUpdaterID       string                `json:"node_updater_id,omitempty"`
	ClusterNodeName     string                `json:"cluster_node_name,omitempty"`
	RuntimeID           string                `json:"runtime_id,omitempty"`
	MachineID           string                `json:"machine_id,omitempty"`
	ObservedOnly        bool                  `json:"observed_only"`
	OverallStatus       string                `json:"overall_status"`
	QuarantineState     string                `json:"quarantine_state"`
	QuarantineReason    string                `json:"quarantine_reason,omitempty"`
	QuarantineExpiresAt *time.Time            `json:"quarantine_expires_at,omitempty"`
	RecoveryConditions  []string              `json:"recovery_conditions,omitempty"`
	Checks              []NodeDeepHealthCheck `json:"checks"`
	ReportedAt          time.Time             `json:"reported_at"`
	UpdatedAt           time.Time             `json:"updated_at"`
}

type NodeDeepHealthListResponse struct {
	Results     []NodeDeepHealthResult `json:"results"`
	GeneratedAt time.Time              `json:"generated_at"`
}

type NodeDeepHealthResponse struct {
	Result NodeDeepHealthResult `json:"result"`
}
