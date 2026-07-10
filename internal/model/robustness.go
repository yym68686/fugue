package model

import "time"

const (
	RobustnessSeverityBlockPublish = "block_publish"
	RobustnessSeverityDegraded     = "degraded"
	RobustnessSeverityWarning      = "warning"
	RobustnessSeverityInfo         = "info"

	InvariantEvidenceStatePass    = "pass"
	InvariantEvidenceStateFail    = "fail"
	InvariantEvidenceStateUnknown = "unknown"
	InvariantEvidenceStateStale   = "stale"

	InvariantEvidenceBehaviorPass        = "pass"
	InvariantEvidenceBehaviorHold        = "hold"
	InvariantEvidenceBehaviorFailClosed  = "fail_closed"
	InvariantEvidenceBehaviorPreserveLKG = "preserve_lkg"

	PlatformControlMechanismDesigned = "designed"
	PlatformControlMechanismShadow   = "shadow"
	PlatformControlMechanismCanary   = "canary"
	PlatformControlMechanismEnforced = "enforced"

	RobustnessIncidentStatusDetected             = "detected"
	RobustnessIncidentStatusManualActionRequired = "manual_action_required"

	RobustnessRepairPlanStatusPlanned              = "planned"
	RobustnessRepairPlanStatusManualActionRequired = "manual_action_required"
)

type RobustnessCheck struct {
	Name       string            `json:"name"`
	Pass       bool              `json:"pass"`
	Severity   string            `json:"severity"`
	Subject    string            `json:"subject,omitempty"`
	Expected   string            `json:"expected,omitempty"`
	Observed   string            `json:"observed,omitempty"`
	Evidence   map[string]string `json:"evidence,omitempty"`
	RepairHint string            `json:"repair_hint,omitempty"`
	Message    string            `json:"message,omitempty"`
}

type RobustnessIncident struct {
	ID         string            `json:"id"`
	Status     string            `json:"status"`
	Severity   string            `json:"severity"`
	Subject    string            `json:"subject"`
	CheckName  string            `json:"check_name"`
	Title      string            `json:"title"`
	Message    string            `json:"message,omitempty"`
	Expected   string            `json:"expected,omitempty"`
	Observed   string            `json:"observed,omitempty"`
	Evidence   map[string]string `json:"evidence,omitempty"`
	RepairHint string            `json:"repair_hint,omitempty"`
	CreatedAt  time.Time         `json:"created_at"`
	UpdatedAt  time.Time         `json:"updated_at"`
}

type RobustnessStatus struct {
	GeneratedAt       time.Time                       `json:"generated_at"`
	Pass              bool                            `json:"pass"`
	BlockRollout      bool                            `json:"block_rollout"`
	Subject           string                          `json:"subject,omitempty"`
	Summary           map[string]string               `json:"summary,omitempty"`
	Checks            []RobustnessCheck               `json:"checks"`
	Incidents         []RobustnessIncident            `json:"incidents"`
	Invariants        []InvariantDefinition           `json:"invariants,omitempty"`
	Inventory         []ResilienceInventoryItem       `json:"inventory,omitempty"`
	Gaps              []ResilienceGap                 `json:"gaps,omitempty"`
	Dashboards        []ResilienceDashboard           `json:"dashboards,omitempty"`
	AlertRules        []ResilienceAlertRule           `json:"alert_rules,omitempty"`
	RuntimeContinuity []RuntimeContinuityStatus       `json:"runtime_continuity,omitempty"`
	ChaosDrills       []ResilienceChaosDrill          `json:"chaos_drills,omitempty"`
	Runbooks          []RunbookReference              `json:"runbooks,omitempty"`
	Autonomy          *PlatformAutonomyStatus         `json:"autonomy,omitempty"`
	DNS               *DNSDelegationPreflightResponse `json:"dns,omitempty"`
	RouteExplain      *RouteExplainResponse           `json:"route_explain,omitempty"`
	FailureContracts  []SubsystemFailureContract      `json:"failure_contracts,omitempty"`
	ReleaseSignals    []ReleaseSignal                 `json:"release_signals,omitempty"`
	PlatformConsumers []PlatformConsumerInstance      `json:"platform_consumers,omitempty"`
	GeneratedSources  []string                        `json:"generated_sources,omitempty"`
}

type RobustnessStatusResponse struct {
	Status RobustnessStatus `json:"status"`
}

type RobustnessIncidentListResponse struct {
	Incidents   []RobustnessIncident `json:"incidents"`
	GeneratedAt time.Time            `json:"generated_at"`
}

type RobustnessIncidentResponse struct {
	Incident RobustnessIncident `json:"incident"`
	Status   RobustnessStatus   `json:"status"`
}

type RobustnessRepairAction struct {
	Kind        string `json:"kind"`
	Subject     string `json:"subject,omitempty"`
	Description string `json:"description"`
	Command     string `json:"command,omitempty"`
	Automatic   bool   `json:"automatic"`
	Risk        string `json:"risk,omitempty"`
}

type RobustnessRepairRequest struct {
	DryRun bool `json:"dry_run"`
}

type RobustnessRepairPlan struct {
	IncidentID  string                   `json:"incident_id"`
	Status      string                   `json:"status"`
	Safe        bool                     `json:"safe"`
	DryRun      bool                     `json:"dry_run"`
	Message     string                   `json:"message,omitempty"`
	Actions     []RobustnessRepairAction `json:"actions"`
	GeneratedAt time.Time                `json:"generated_at"`
}

type RobustnessRepairPlanResponse struct {
	Plan RobustnessRepairPlan `json:"plan"`
}

type InvariantEvidencePolicy struct {
	MaxAge                string `json:"max_age,omitempty"`
	MinimumSources        int    `json:"minimum_sources,omitempty"`
	MinimumFailureDomains int    `json:"minimum_failure_domains,omitempty"`
	AllowLKGEvidence      bool   `json:"allow_lkg_evidence,omitempty"`
}

type InvariantDefinition struct {
	ID                        string                  `json:"id"`
	Category                  string                  `json:"category"`
	Scope                     string                  `json:"scope"`
	Subject                   string                  `json:"subject,omitempty"`
	Owner                     string                  `json:"owner"`
	Description               string                  `json:"description"`
	Severity                  string                  `json:"severity"`
	DefaultMode               string                  `json:"default_mode"`
	HardGate                  bool                    `json:"hard_gate"`
	EvidenceSource            string                  `json:"evidence_source,omitempty"`
	EvidenceSources           []string                `json:"evidence_sources,omitempty"`
	GatePolicyID              string                  `json:"gate_policy_id,omitempty"`
	AutomaticActionContractID string                  `json:"automatic_action_contract_id,omitempty"`
	BlastRadius               GateBlastRadiusPolicy   `json:"blast_radius_policy,omitempty"`
	RollbackSignal            string                  `json:"rollback_signal,omitempty"`
	KillSwitchEnv             string                  `json:"kill_switch_env,omitempty"`
	RunbookRef                string                  `json:"runbook_ref,omitempty"`
	EvidenceFreshnessPolicy   InvariantEvidencePolicy `json:"evidence_freshness_policy,omitempty"`
	UnknownBehavior           string                  `json:"unknown_behavior"`
	StaleBehavior             string                  `json:"stale_behavior"`
	NonBypassable             bool                    `json:"non_bypassable"`
	ExpectedConsumerSetRef    string                  `json:"expected_consumer_set_ref,omitempty"`
	CompatibilityPolicyRef    string                  `json:"compatibility_policy_ref,omitempty"`
	ClockUncertaintyBudget    string                  `json:"clock_uncertainty_budget,omitempty"`
}

// ResilienceInvariant remains a source-compatible alias for callers that used
// the pre-registry name.
type ResilienceInvariant = InvariantDefinition

type InvariantDefinitionListResponse struct {
	Invariants  []InvariantDefinition `json:"invariants"`
	GeneratedAt time.Time             `json:"generated_at"`
}

type InvariantDefinitionResponse struct {
	Invariant InvariantDefinition `json:"invariant"`
}

type PlatformConsumerContractDefinition struct {
	Component              string   `json:"component"`
	ArtifactKinds          []string `json:"artifact_kinds"`
	Scope                  string   `json:"scope"`
	IdentityKind           string   `json:"identity_kind"`
	ProtocolVersion        string   `json:"protocol_version"`
	SchemaVersion          string   `json:"schema_version"`
	Required               bool     `json:"required"`
	LoadLKGFirst           bool     `json:"load_lkg_first"`
	AtomicApply            bool     `json:"atomic_apply"`
	LocalProbe             bool     `json:"local_probe"`
	HeartbeatGeneration    bool     `json:"heartbeat_generation"`
	HeartbeatFreshness     string   `json:"heartbeat_freshness"`
	CompatibilityFloor     string   `json:"compatibility_floor,omitempty"`
	ExpectedConsumerSource string   `json:"expected_consumer_source"`
}

type PlatformSyntheticProbeDefinition struct {
	ID          string `json:"id"`
	Scope       string `json:"scope"`
	Description string `json:"description"`
	HardGate    bool   `json:"hard_gate"`
	Timeout     string `json:"timeout"`
}

type PlatformLKGPolicyDefinition struct {
	ArtifactKind       string `json:"artifact_kind"`
	StorageLocation    string `json:"storage_location"`
	CachePathEnv       string `json:"cache_path_env,omitempty"`
	MaxAge             string `json:"max_age"`
	MaxStale           string `json:"max_stale"`
	MinimumGenerations int    `json:"minimum_generations"`
	ArchiveLimit       int    `json:"archive_limit,omitempty"`
	ExpiryBehavior     string `json:"expiry_behavior"`
}

type PlatformControlMechanism struct {
	ID                string `json:"id"`
	Category          string `json:"category"`
	Status            string `json:"status"`
	Mode              string `json:"mode,omitempty"`
	ImplementationRef string `json:"implementation_ref,omitempty"`
	Summary           string `json:"summary,omitempty"`
}

type PlatformControlInventory struct {
	GeneratedAt      time.Time                            `json:"generated_at"`
	ArtifactKinds    []string                             `json:"artifact_kinds"`
	Consumers        []PlatformConsumerContractDefinition `json:"consumers"`
	GatePolicies     []GatePolicy                         `json:"gate_policies"`
	AutomaticActions []AutomaticActionContract            `json:"automatic_actions"`
	AutonomyControls AutonomyControls                     `json:"autonomy_controls"`
	ReleaseSignals   []ReleaseSignal                      `json:"release_signals"`
	SyntheticProbes  []PlatformSyntheticProbeDefinition   `json:"synthetic_probes"`
	LKGPolicies      []PlatformLKGPolicyDefinition        `json:"lkg_policies"`
	Mechanisms       []PlatformControlMechanism           `json:"mechanisms"`
}

type PlatformControlInventoryResponse struct {
	Inventory PlatformControlInventory `json:"inventory"`
}

type ResilienceInventoryItem struct {
	Category  string            `json:"category"`
	Subject   string            `json:"subject"`
	Status    string            `json:"status"`
	Summary   string            `json:"summary,omitempty"`
	Evidence  map[string]string `json:"evidence,omitempty"`
	UpdatedAt time.Time         `json:"updated_at,omitempty"`
}

type ResilienceGap struct {
	ID                 string `json:"id"`
	Category           string `json:"category"`
	Severity           string `json:"severity"`
	Description        string `json:"description"`
	ImplementationPath string `json:"implementation_path,omitempty"`
}

type ResilienceDashboard struct {
	Name        string   `json:"name"`
	Scope       string   `json:"scope"`
	Metrics     []string `json:"metrics,omitempty"`
	Command     string   `json:"command,omitempty"`
	Description string   `json:"description,omitempty"`
}

type ResilienceAlertRule struct {
	Name           string `json:"name"`
	Severity       string `json:"severity"`
	Expression     string `json:"expression"`
	IncidentClass  string `json:"incident_class"`
	ExplainCommand string `json:"explain_command"`
	RunbookRef     string `json:"runbook_ref,omitempty"`
}

type RuntimeContinuityStatus struct {
	AppID             string            `json:"app_id,omitempty"`
	AppName           string            `json:"app_name,omitempty"`
	Hostname          string            `json:"hostname,omitempty"`
	State             string            `json:"state"`
	Strategy          string            `json:"strategy"`
	DesiredReplicas   int               `json:"desired_replicas"`
	ReadyReplicas     int               `json:"ready_replicas"`
	RuntimeID         string            `json:"runtime_id,omitempty"`
	RuntimeNode       string            `json:"runtime_node,omitempty"`
	NodeQuarantine    string            `json:"node_quarantine,omitempty"`
	Blockers          []string          `json:"blockers,omitempty"`
	ReplacementPlan   string            `json:"replacement_plan,omitempty"`
	StatefulPreflight []string          `json:"stateful_preflight,omitempty"`
	Attribution       []string          `json:"attribution,omitempty"`
	Evidence          map[string]string `json:"evidence,omitempty"`
}

type ResilienceChaosDrill struct {
	ID               string `json:"id"`
	FailureMode      string `json:"failure_mode"`
	Detection        string `json:"detection"`
	Quarantine       string `json:"quarantine"`
	RepairOrRollback string `json:"repair_or_rollback"`
	ExplainCommand   string `json:"explain_command"`
	ReleaseReadiness bool   `json:"release_readiness"`
	RunbookRef       string `json:"runbook_ref,omitempty"`
}

type RunbookReference struct {
	Name          string `json:"name"`
	Path          string `json:"path"`
	IncidentClass string `json:"incident_class,omitempty"`
}
