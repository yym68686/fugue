package model

import "time"

const (
	AutomationOwnerSystem = "system"
	AutomationOwnerUser   = "user"

	AutomationPolicyKindManagedSystem = "managed_system"
	AutomationPolicyKindAppRecovery   = "app_recovery"

	AutomationTriggerInvariant      = "invariant"
	AutomationTriggerRequestMetric  = "request_metric"
	AutomationTriggerSyntheticProbe = "synthetic_probe"
	AutomationTriggerEvent          = "event"
	AutomationTriggerSchedule       = "schedule"
)

// AutomationScope identifies the resource boundary within which an automation
// may observe evidence and request actions. An empty ID is only valid for
// singleton scopes such as cluster.
type AutomationScope struct {
	Type string `json:"type"`
	ID   string `json:"id,omitempty"`
}

// AutomationTrigger is the common trigger envelope shared by managed system
// automations and user-owned policies. Domain-specific configuration is added
// through typed optional fields instead of arbitrary executable expressions.
type AutomationTrigger struct {
	Type                  string   `json:"type"`
	Source                string   `json:"source"`
	InvariantID           string   `json:"invariant_id,omitempty"`
	RequiredEvidence      []string `json:"required_evidence,omitempty"`
	MinimumSamples        int      `json:"minimum_samples,omitempty"`
	MinimumFailureDomains int      `json:"minimum_failure_domains,omitempty"`
}

// AutomationAction describes a typed Fugue action. Parameters are declarative
// string values that must be interpreted and validated by the owning domain
// executor; they are not executable payloads.
type AutomationAction struct {
	Type       string            `json:"type"`
	Parameters map[string]string `json:"parameters,omitempty"`
}

// AutomationSafetyPolicy records the non-user-bypassable contract that guards
// an action. User policies may tighten these values but may not relax them.
type AutomationSafetyPolicy struct {
	ActionContractID       string                `json:"action_contract_id"`
	GatePolicyID           string                `json:"gate_policy_id"`
	TTL                    string                `json:"ttl"`
	BlastRadius            GateBlastRadiusPolicy `json:"blast_radius,omitempty"`
	RecoveryCondition      string                `json:"recovery_condition"`
	RollbackAction         string                `json:"rollback_action"`
	RequiresRollbackTarget bool                  `json:"requires_rollback_target,omitempty"`
	RequiresAudit          bool                  `json:"requires_audit,omitempty"`
	RequiresWAL            bool                  `json:"requires_wal,omitempty"`
	RequiresIdempotencyKey bool                  `json:"requires_idempotency_key,omitempty"`
	RequiresFencingToken   bool                  `json:"requires_fencing_token,omitempty"`
}

type AutomationRule struct {
	ID          string                 `json:"id"`
	Description string                 `json:"description,omitempty"`
	Trigger     AutomationTrigger      `json:"trigger"`
	Action      AutomationAction       `json:"action"`
	Safety      AutomationSafetyPolicy `json:"safety"`
}

// AutomationPolicy is the stable control-plane representation for both
// platform-managed and user-managed rule/action pairs.
type AutomationPolicy struct {
	ID          string            `json:"id"`
	TenantID    string            `json:"tenant_id,omitempty"`
	ProjectID   string            `json:"project_id,omitempty"`
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	Kind        string            `json:"kind"`
	OwnerType   string            `json:"owner_type"`
	Scope       AutomationScope   `json:"scope"`
	Mode        string            `json:"mode"`
	Priority    int               `json:"priority,omitempty"`
	Managed     bool              `json:"managed"`
	SourceRef   string            `json:"source_ref,omitempty"`
	Rules       []AutomationRule  `json:"rules"`
	Generation  int64             `json:"generation"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

type AutomationPolicyListResponse struct {
	Policies    []AutomationPolicy `json:"policies"`
	GeneratedAt time.Time          `json:"generated_at"`
}

type AutomationPolicyResponse struct {
	Policy AutomationPolicy `json:"policy"`
}
