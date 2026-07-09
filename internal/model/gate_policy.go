package model

import "time"

const (
	GatePolicyModeShadow   = "shadow"
	GatePolicyModeCanary   = "canary"
	GatePolicyModeEnforced = "enforced"
	GatePolicyModeDisabled = "disabled"

	GatePolicyScopeCluster   = "cluster"
	GatePolicyScopeNode      = "node"
	GatePolicyScopeEdgeNode  = "edge-node"
	GatePolicyScopeEdgeGroup = "edge-group"
	GatePolicyScopeHostname  = "hostname"
	GatePolicyScopeService   = "service"
	GatePolicyScopeRuntime   = "runtime"
)

type GateBlastRadiusPolicy struct {
	MaxNodes                        int `json:"max_nodes,omitempty"`
	MaxEdgesPerGroup                int `json:"max_edges_per_group,omitempty"`
	PreserveMinHealthyEdgeGroups    int `json:"preserve_min_healthy_edge_groups,omitempty"`
	PreserveMinEligibleEdgesPerHost int `json:"preserve_min_eligible_edges_per_host,omitempty"`
}

type GatePolicy struct {
	ID                    string                `json:"id"`
	Description           string                `json:"description,omitempty"`
	Mode                  string                `json:"mode"`
	Scope                 string                `json:"scope"`
	DefaultMode           string                `json:"default_mode,omitempty"`
	IntroducedAt          time.Time             `json:"introduced_at,omitempty"`
	IntroducedByRelease   string                `json:"introduced_by_release,omitempty"`
	SoakStartedAt         *time.Time            `json:"soak_started_at,omitempty"`
	SoakMinDuration       string                `json:"soak_min_duration,omitempty"`
	MinimumSamples        int                   `json:"minimum_samples,omitempty"`
	MinimumFailureDomains int                   `json:"minimum_failure_domains,omitempty"`
	CanaryFailureDomains  []string              `json:"canary_failure_domains,omitempty"`
	BlastRadius           GateBlastRadiusPolicy `json:"blast_radius,omitempty"`
	RollbackOn            []string              `json:"rollback_on,omitempty"`
	KillSwitchEnv         string                `json:"kill_switch_env,omitempty"`
	RunbookRef            string                `json:"runbook_ref,omitempty"`
	UpdatedAt             time.Time             `json:"updated_at,omitempty"`
	UpdatedBy             string                `json:"updated_by,omitempty"`
	PromotionReason       string                `json:"promotion_reason,omitempty"`
}

type GatePolicyListResponse struct {
	Policies    []GatePolicy `json:"policies"`
	GeneratedAt time.Time    `json:"generated_at"`
}

type GatePolicyResponse struct {
	Policy GatePolicy `json:"policy"`
}

type GatePolicyPromoteRequest struct {
	Mode                string   `json:"mode"`
	Reason              string   `json:"reason,omitempty"`
	CanaryScopes        []string `json:"canary_scopes,omitempty"`
	IntroducedByRelease string   `json:"introduced_by_release,omitempty"`
}

type GatePolicyPromotionResponse struct {
	Policy   GatePolicy              `json:"policy"`
	Artifact PlatformArtifact        `json:"artifact"`
	Release  PlatformArtifactRelease `json:"release"`
	Message  PlatformReleaseMessage  `json:"message"`
	LKG      *PlatformLKGSnapshot    `json:"lkg,omitempty"`
}

type AutomaticActionContract struct {
	ID                    string            `json:"id"`
	Scope                 string            `json:"scope"`
	TriggerInvariant      string            `json:"trigger_invariant"`
	EvidenceSource        string            `json:"evidence_source"`
	MaxBlastRadius        string            `json:"max_blast_radius"`
	TTL                   string            `json:"ttl"`
	RecoveryCondition     string            `json:"recovery_condition"`
	RollbackAction        string            `json:"rollback_action"`
	DryRunOutput          string            `json:"dry_run_output"`
	AuditLogLocation      string            `json:"audit_log_location"`
	KillSwitchEnv         string            `json:"kill_switch_env"`
	HumanApprovalBoundary string            `json:"human_approval_boundary"`
	Metadata              map[string]string `json:"metadata,omitempty"`
}
