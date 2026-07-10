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
	ID                     string                `json:"id"`
	ActionType             string                `json:"action_type,omitempty"`
	Scope                  string                `json:"scope"`
	TriggerInvariant       string                `json:"trigger_invariant"`
	EvidenceSource         string                `json:"evidence_source"`
	RequiredEvidence       []string              `json:"required_evidence,omitempty"`
	GatePolicyID           string                `json:"gate_policy_id,omitempty"`
	MaxBlastRadius         string                `json:"max_blast_radius"`
	BlastRadius            GateBlastRadiusPolicy `json:"blast_radius,omitempty"`
	TTL                    string                `json:"ttl"`
	MinimumSamples         int                   `json:"minimum_samples,omitempty"`
	MinimumFailureDomains  int                   `json:"minimum_failure_domains,omitempty"`
	SoakMinDuration        string                `json:"soak_min_duration,omitempty"`
	RecoveryCondition      string                `json:"recovery_condition"`
	RollbackAction         string                `json:"rollback_action"`
	DryRunOutput           string                `json:"dry_run_output"`
	AuditLogLocation       string                `json:"audit_log_location"`
	EnableEnv              string                `json:"enable_env"`
	KillSwitchEnv          string                `json:"kill_switch_env"`
	HumanApprovalBoundary  string                `json:"human_approval_boundary"`
	RunbookRef             string                `json:"runbook_ref,omitempty"`
	AllowedModes           []string              `json:"allowed_modes,omitempty"`
	RequiresRollbackTarget bool                  `json:"requires_rollback_target,omitempty"`
	RequiresAudit          bool                  `json:"requires_audit,omitempty"`
	RequiresWAL            bool                  `json:"requires_wal,omitempty"`
	RequiresIdempotencyKey bool                  `json:"requires_idempotency_key,omitempty"`
	RequiresFencingToken   bool                  `json:"requires_fencing_token,omitempty"`
	Metadata               map[string]string     `json:"metadata,omitempty"`
}

type AutomaticActionContractListResponse struct {
	Contracts   []AutomaticActionContract `json:"contracts"`
	GeneratedAt time.Time                 `json:"generated_at"`
}

type AutomaticActionContractResponse struct {
	Contract AutomaticActionContract `json:"contract"`
}

type ActionSafetyEvidence struct {
	ID            string            `json:"id"`
	State         string            `json:"state"`
	Source        string            `json:"source"`
	FailureDomain string            `json:"failure_domain,omitempty"`
	ObservedAt    time.Time         `json:"observed_at,omitempty"`
	ExpiresAt     *time.Time        `json:"expires_at,omitempty"`
	Hash          string            `json:"hash,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

type ActionSafetyRequest struct {
	ActionType             string                 `json:"action_type"`
	ContractID             string                 `json:"contract_id"`
	TriggerInvariant       string                 `json:"trigger_invariant"`
	Scope                  string                 `json:"scope"`
	Subject                string                 `json:"subject"`
	Evidence               []ActionSafetyEvidence `json:"evidence,omitempty"`
	CurrentMode            string                 `json:"current_mode,omitempty"`
	CurrentCounts          map[string]int         `json:"current_counts,omitempty"`
	CandidateCounts        map[string]int         `json:"candidate_counts,omitempty"`
	FailureDomains         []string               `json:"failure_domains,omitempty"`
	SampleCount            int                    `json:"sample_count,omitempty"`
	SoakStartedAt          *time.Time             `json:"soak_started_at,omitempty"`
	TTL                    string                 `json:"ttl,omitempty"`
	RollbackTarget         string                 `json:"rollback_target,omitempty"`
	RequestedBy            string                 `json:"requested_by,omitempty"`
	IdempotencyKey         string                 `json:"idempotency_key,omitempty"`
	FencingToken           int64                  `json:"fencing_token,omitempty"`
	HumanApproved          bool                   `json:"human_approved,omitempty"`
	AuditReady             bool                   `json:"audit_ready,omitempty"`
	WALReady               bool                   `json:"wal_ready,omitempty"`
	CanaryScopeMatch       bool                   `json:"canary_scope_match,omitempty"`
	ClockUncertaintyMillis int64                  `json:"clock_uncertainty_millis,omitempty"`
}

type ActionSafetyViolation struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type ActionSafetyDecision struct {
	Pass                      bool                    `json:"pass"`
	Allowed                   bool                    `json:"allowed"`
	WouldAction               bool                    `json:"would_action"`
	ProductionMutationAllowed bool                    `json:"production_mutation_allowed"`
	EffectiveMode             string                  `json:"effective_mode"`
	ContractID                string                  `json:"contract_id"`
	GatePolicyID              string                  `json:"gate_policy_id,omitempty"`
	Subject                   string                  `json:"subject,omitempty"`
	ExpiresAt                 *time.Time              `json:"expires_at,omitempty"`
	Violations                []ActionSafetyViolation `json:"violations,omitempty"`
	EvidenceStates            map[string]string       `json:"evidence_states,omitempty"`
	BlastRadius               BlastRadiusEvaluation   `json:"blast_radius"`
	GeneratedAt               time.Time               `json:"generated_at"`
}

type ActionSafetyDecisionResponse struct {
	Decision ActionSafetyDecision `json:"decision"`
}
