package model

import "time"

const (
	PlatformArtifactKindEdgeRouteBundle           = "edge_route_bundle"
	PlatformArtifactKindDNSAnswerBundle           = "dns_answer_bundle"
	PlatformArtifactKindCaddyRouteConfig          = "caddy_route_config"
	PlatformArtifactKindDiscoveryBundle           = "discovery_bundle"
	PlatformArtifactKindNodeDesiredState          = "node_desired_state"
	PlatformArtifactKindRuntimePlacementPlan      = "runtime_placement_plan"
	PlatformArtifactKindRuntimeContinuityPlan     = "runtime_continuity_plan"
	PlatformArtifactKindNodeGuardianPolicy        = "node_guardian_policy"
	PlatformArtifactKindReleaseGuardPolicy        = "release_guard_policy"
	PlatformArtifactKindEdgeRankingPolicy         = "edge_ranking_policy"
	PlatformArtifactKindTrafficSafetyPolicy       = "traffic_safety_policy"
	PlatformArtifactKindSubsystemFailureContracts = "subsystem_failure_contracts"

	PlatformArtifactStatusDraft     = "draft"
	PlatformArtifactStatusValidated = "validated"
	PlatformArtifactStatusRejected  = "rejected"

	PlatformArtifactReleaseChannelShadow = "shadow"
	PlatformArtifactReleaseChannelGray   = "gray"
	PlatformArtifactReleaseChannelFull   = "full"

	PlatformArtifactReleaseStatusActive     = "active"
	PlatformArtifactReleaseStatusSuperseded = "superseded"
	PlatformArtifactReleaseStatusRolledBack = "rolled_back"

	PlatformReleaseMessageTypeRelease  = "release"
	PlatformReleaseMessageTypeRollback = "rollback"
)

type PlatformArtifactScope struct {
	ScopeType    string `json:"scope_type,omitempty"`
	Key          string `json:"key,omitempty"`
	TenantID     string `json:"tenant_id,omitempty"`
	ProjectID    string `json:"project_id,omitempty"`
	AppID        string `json:"app_id,omitempty"`
	Hostname     string `json:"hostname,omitempty"`
	EdgeGroupID  string `json:"edge_group_id,omitempty"`
	EdgeID       string `json:"edge_id,omitempty"`
	NodeID       string `json:"node_id,omitempty"`
	Region       string `json:"region,omitempty"`
	Country      string `json:"country,omitempty"`
	TrafficClass string `json:"traffic_class,omitempty"`
}

type PlatformArtifactValidationResult struct {
	Name     string            `json:"name"`
	Pass     bool              `json:"pass"`
	Severity string            `json:"severity"`
	Message  string            `json:"message,omitempty"`
	Evidence map[string]string `json:"evidence,omitempty"`
}

type PlatformArtifact struct {
	ID                 string                             `json:"id"`
	ArtifactKind       string                             `json:"artifact_kind"`
	Scope              PlatformArtifactScope              `json:"scope"`
	ScopeKey           string                             `json:"scope_key"`
	Generation         string                             `json:"generation"`
	Status             string                             `json:"status"`
	ContentHash        string                             `json:"content_hash"`
	Content            map[string]any                     `json:"content,omitempty"`
	ValidationResults  []PlatformArtifactValidationResult `json:"validation_results,omitempty"`
	CompatibilityFloor string                             `json:"compatibility_floor,omitempty"`
	Metadata           map[string]string                  `json:"metadata,omitempty"`
	CreatedByType      string                             `json:"created_by_type,omitempty"`
	CreatedByID        string                             `json:"created_by_id,omitempty"`
	CreatedAt          time.Time                          `json:"created_at"`
	UpdatedAt          time.Time                          `json:"updated_at"`
}

type PlatformArtifactContent struct {
	ContentHash string         `json:"content_hash"`
	Content     map[string]any `json:"content,omitempty"`
	SizeBytes   int64          `json:"size_bytes"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
}

type PlatformArtifactRelease struct {
	ID                       string                `json:"id"`
	ArtifactID               string                `json:"artifact_id"`
	ArtifactKind             string                `json:"artifact_kind"`
	Scope                    PlatformArtifactScope `json:"scope"`
	ScopeKey                 string                `json:"scope_key"`
	Generation               string                `json:"generation"`
	ReleaseChannel           string                `json:"release_channel"`
	Status                   string                `json:"status"`
	RollbackTargetGeneration string                `json:"rollback_target_generation,omitempty"`
	CanaryRuleRef            string                `json:"canary_rule_ref,omitempty"`
	Reason                   string                `json:"reason,omitempty"`
	ReleasedByType           string                `json:"released_by_type,omitempty"`
	ReleasedByID             string                `json:"released_by_id,omitempty"`
	ReleasedAt               time.Time             `json:"released_at"`
	CreatedAt                time.Time             `json:"created_at"`
	UpdatedAt                time.Time             `json:"updated_at"`
}

type PlatformReleaseMessage struct {
	ID             string                `json:"id"`
	ReleaseID      string                `json:"release_id"`
	ArtifactID     string                `json:"artifact_id"`
	ArtifactKind   string                `json:"artifact_kind"`
	Scope          PlatformArtifactScope `json:"scope"`
	ScopeKey       string                `json:"scope_key"`
	Generation     string                `json:"generation"`
	ReleaseChannel string                `json:"release_channel"`
	MessageType    string                `json:"message_type"`
	CreatedAt      time.Time             `json:"created_at"`
	ExpiresAt      *time.Time            `json:"expires_at,omitempty"`
	AckCount       int                   `json:"ack_count"`
}

type PlatformConsumerInstance struct {
	ID                string    `json:"id"`
	ConsumerID        string    `json:"consumer_id"`
	Component         string    `json:"component,omitempty"`
	NodeID            string    `json:"node_id,omitempty"`
	ArtifactKind      string    `json:"artifact_kind"`
	ScopeKey          string    `json:"scope_key"`
	SupportedKinds    []string  `json:"supported_artifact_kinds,omitempty"`
	DesiredGeneration string    `json:"desired_generation,omitempty"`
	ActualGeneration  string    `json:"actual_generation,omitempty"`
	LKGGeneration     string    `json:"lkg_generation,omitempty"`
	ApplyStatus       string    `json:"apply_status,omitempty"`
	ProbeStatus       string    `json:"probe_status,omitempty"`
	ServingLKG        bool      `json:"serving_lkg,omitempty"`
	LKGExpired        bool      `json:"lkg_expired,omitempty"`
	LastError         string    `json:"last_error,omitempty"`
	LastHeartbeatAt   time.Time `json:"last_heartbeat_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

type PlatformLKGSnapshot struct {
	ID           string                `json:"id"`
	ArtifactID   string                `json:"artifact_id"`
	ArtifactKind string                `json:"artifact_kind"`
	Scope        PlatformArtifactScope `json:"scope"`
	ScopeKey     string                `json:"scope_key"`
	Generation   string                `json:"generation"`
	ContentHash  string                `json:"content_hash"`
	ExpiresAt    time.Time             `json:"expires_at"`
	CreatedAt    time.Time             `json:"created_at"`
	UpdatedAt    time.Time             `json:"updated_at"`
}

type PlatformArtifactFilter struct {
	ArtifactKind string
	ScopeKey     string
	Status       string
	Limit        int
}

type PlatformArtifactCreateRequest struct {
	ArtifactKind       string                `json:"artifact_kind"`
	Scope              PlatformArtifactScope `json:"scope,omitempty"`
	Generation         string                `json:"generation,omitempty"`
	Content            map[string]any        `json:"content"`
	CompatibilityFloor string                `json:"compatibility_floor,omitempty"`
	Metadata           map[string]string     `json:"metadata,omitempty"`
}

type PlatformArtifactValidateRequest struct {
	DryRun bool `json:"dry_run"`
}

type PlatformArtifactReleaseRequest struct {
	ReleaseChannel string `json:"release_channel"`
	CanaryRuleRef  string `json:"canary_rule_ref,omitempty"`
	ForcePublish   bool   `json:"force_publish,omitempty"`
	Reason         string `json:"reason,omitempty"`
}

type PlatformArtifactRollbackRequest struct {
	ReleaseChannel string `json:"release_channel,omitempty"`
	ToGeneration   string `json:"to_generation"`
	Reason         string `json:"reason"`
	ForcePublish   bool   `json:"force_publish,omitempty"`
	CanaryRuleRef  string `json:"canary_rule_ref,omitempty"`
}

type PlatformConsumerHeartbeatRequest struct {
	ConsumerID        string   `json:"consumer_id"`
	Component         string   `json:"component,omitempty"`
	NodeID            string   `json:"node_id,omitempty"`
	ArtifactKind      string   `json:"artifact_kind"`
	ScopeKey          string   `json:"scope_key,omitempty"`
	SupportedKinds    []string `json:"supported_artifact_kinds,omitempty"`
	DesiredGeneration string   `json:"desired_generation,omitempty"`
	ActualGeneration  string   `json:"actual_generation,omitempty"`
	LKGGeneration     string   `json:"lkg_generation,omitempty"`
	ApplyStatus       string   `json:"apply_status,omitempty"`
	ProbeStatus       string   `json:"probe_status,omitempty"`
	ServingLKG        bool     `json:"serving_lkg,omitempty"`
	LKGExpired        bool     `json:"lkg_expired,omitempty"`
	LastError         string   `json:"last_error,omitempty"`
}

type PlatformArtifactListResponse struct {
	Artifacts   []PlatformArtifact `json:"artifacts"`
	GeneratedAt time.Time          `json:"generated_at"`
}

type PlatformArtifactResponse struct {
	Artifact PlatformArtifact `json:"artifact"`
}

type PlatformArtifactValidationResponse struct {
	Artifact PlatformArtifact                   `json:"artifact"`
	Results  []PlatformArtifactValidationResult `json:"results"`
	Pass     bool                               `json:"pass"`
	DryRun   bool                               `json:"dry_run"`
}

type PlatformArtifactReleaseResponse struct {
	Artifact PlatformArtifact        `json:"artifact"`
	Release  PlatformArtifactRelease `json:"release"`
	Message  PlatformReleaseMessage  `json:"message"`
	LKG      *PlatformLKGSnapshot    `json:"lkg,omitempty"`
}

type PlatformArtifactConsumersResponse struct {
	Consumers   []PlatformConsumerInstance `json:"consumers"`
	GeneratedAt time.Time                  `json:"generated_at"`
}

type PlatformArtifactLKGResponse struct {
	LKG *PlatformLKGSnapshot `json:"lkg,omitempty"`
}

type PlatformStateArtifactResponse struct {
	Artifact   *PlatformArtifact        `json:"artifact,omitempty"`
	Release    *PlatformArtifactRelease `json:"release,omitempty"`
	Messages   []PlatformReleaseMessage `json:"messages,omitempty"`
	LKG        *PlatformLKGSnapshot     `json:"lkg,omitempty"`
	Generation string                   `json:"generation,omitempty"`
	Waited     bool                     `json:"waited"`
}

type PlatformConsumerHeartbeatResponse struct {
	Consumer PlatformConsumerInstance `json:"consumer"`
	Drift    bool                     `json:"drift"`
}

type FailureMode struct {
	ID          string `json:"id"`
	Description string `json:"description"`
	Severity    string `json:"severity,omitempty"`
}

type DetectionSignal struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Required    bool   `json:"required,omitempty"`
}

type IsolationAction struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Automatic   bool   `json:"automatic,omitempty"`
}

type FallbackBehavior struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

type RepairAction struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	SafetyClass string `json:"safety_class,omitempty"`
	Automatic   bool   `json:"automatic,omitempty"`
}

type RollbackPath struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

type HumanApprovalBoundary struct {
	Action      string `json:"action"`
	Description string `json:"description"`
	Required    bool   `json:"required"`
}

type SubsystemFailureContract struct {
	Subsystem                  string                  `json:"subsystem"`
	Owner                      string                  `json:"owner,omitempty"`
	Summary                    string                  `json:"summary,omitempty"`
	FailureModes               []FailureMode           `json:"failure_modes"`
	DetectionSignals           []DetectionSignal       `json:"detection_signals"`
	IsolationActions           []IsolationAction       `json:"isolation_actions"`
	FallbackBehaviors          []FallbackBehavior      `json:"fallback_behaviors"`
	RepairActions              []RepairAction          `json:"repair_actions"`
	RollbackPaths              []RollbackPath          `json:"rollback_paths"`
	AttributionClasses         []string                `json:"attribution_classes"`
	HumanApprovalBoundaries    []HumanApprovalBoundary `json:"human_approval_boundaries"`
	ObserveOnlyAllowed         bool                    `json:"observe_only_allowed"`
	AutomaticQuarantineAllowed bool                    `json:"automatic_quarantine_allowed"`
	AutomaticRepairAllowed     bool                    `json:"automatic_repair_allowed"`
	HumanApprovalRequired      bool                    `json:"human_approval_required"`
	RunbookRef                 string                  `json:"runbook_ref,omitempty"`
	UpdatedAt                  time.Time               `json:"updated_at"`
}

type SubsystemFailureContractListResponse struct {
	Contracts   []SubsystemFailureContract `json:"contracts"`
	GeneratedAt time.Time                  `json:"generated_at"`
}

type SubsystemFailureContractResponse struct {
	Contract SubsystemFailureContract `json:"contract"`
}
