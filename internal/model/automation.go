package model

import "time"

const (
	AutomationOwnerSystem = "system"
	AutomationOwnerUser   = "user"

	AutomationPolicyKindManagedSystem = "managed_system"
	AutomationPolicyKindAppRecovery   = "app_recovery"

	AutomationScopeApp = "app"

	AutomationTriggerInvariant      = "invariant"
	AutomationTriggerRequestMetric  = "request_metric"
	AutomationTriggerSyntheticProbe = "synthetic_probe"
	AutomationTriggerEvent          = "event"
	AutomationTriggerSchedule       = "schedule"

	AutomationIntentSourceAdminReplay = "admin_replay"
	AutomationIntentSourceControlLoop = "control_loop"
	AutomationIntentStatusObserved    = "observed"
)

// AutomationScope identifies the resource boundary within which an automation
// may observe evidence and request actions. An empty ID is only valid for
// singleton scopes such as cluster.
type AutomationScope struct {
	Type string `json:"type"`
	ID   string `json:"id,omitempty"`
}

// AutomationRequestMetricSelector is the bounded, typed selector used by
// request-metric triggers. It deliberately supports only observed metric
// dimensions; it is not an expression language.
type AutomationRequestMetricSelector struct {
	Metric       string   `json:"metric"`
	Window       string   `json:"window"`
	StatusCodes  []int    `json:"status_codes,omitempty"`
	ErrorClasses []string `json:"error_classes,omitempty"`
}

// AutomationTrigger is the common trigger envelope shared by managed system
// automations and user-owned policies. Domain-specific configuration is added
// through typed optional fields instead of arbitrary executable expressions.
type AutomationTrigger struct {
	Type                  string                           `json:"type"`
	Source                string                           `json:"source"`
	InvariantID           string                           `json:"invariant_id,omitempty"`
	RequestMetric         *AutomationRequestMetricSelector `json:"request_metric,omitempty"`
	RequiredEvidence      []string                         `json:"required_evidence,omitempty"`
	MinimumSamples        int                              `json:"minimum_samples,omitempty"`
	MinimumFailureDomains int                              `json:"minimum_failure_domains,omitempty"`
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

// AutomationRequestOutcomeAggregate is a bounded, already-aggregated view of
// request outcomes. The control loop will eventually populate it from a
// trusted observability adapter; the initial replay API accepts the same typed
// shape so evaluation remains deterministic and cannot execute arbitrary
// expressions.
type AutomationRequestOutcomeAggregate struct {
	StatusCode    int    `json:"status_code"`
	Count         int64  `json:"count"`
	FailureDomain string `json:"failure_domain,omitempty"`
}

// AutomationEvaluationEvidence is the immutable evidence snapshot captured
// alongside an observe-only intent. App revision and readiness are populated
// by the control plane from its app store; callers only provide the bounded
// request-outcome aggregates.
type AutomationEvaluationEvidence struct {
	CollectedBy            string                              `json:"collected_by"`
	Trusted                bool                                `json:"trusted"`
	WindowStartedAt        time.Time                           `json:"window_started_at"`
	WindowEndedAt          time.Time                           `json:"window_ended_at"`
	RequestOutcomes        []AutomationRequestOutcomeAggregate `json:"request_outcomes"`
	AppRevision            string                              `json:"app_revision"`
	AppReadiness           string                              `json:"app_readiness"`
	AppReadinessObservedAt time.Time                           `json:"app_readiness_observed_at"`
}

// AutomationEvaluationDecision is the pure trigger result. In this initial
// atom production mutation is unconditionally false; later execution atoms
// may consume only explicitly eligible intent records after a separate safety
// evaluation.
type AutomationEvaluationDecision struct {
	PolicyID                  string          `json:"policy_id"`
	PolicyGeneration          int64           `json:"policy_generation"`
	RuleID                    string          `json:"rule_id"`
	Scope                     AutomationScope `json:"scope"`
	Mode                      string          `json:"mode"`
	Matched                   bool            `json:"matched"`
	WouldAction               bool            `json:"would_action"`
	ProductionMutationAllowed bool            `json:"production_mutation_allowed"`
	MatchingSamples           int64           `json:"matching_samples"`
	FailureDomains            []string        `json:"failure_domains"`
	EvidenceHash              string          `json:"evidence_hash"`
	ReasonCodes               []string        `json:"reason_codes"`
	EvaluatedAt               time.Time       `json:"evaluated_at"`
}

// AutomationActionIntent is append-only. The initial status is deliberately
// observe-only and production_mutation_allowed is permanently false. Keeping
// the policy rule and evidence snapshots here makes a later executor
// auditable even if the source policy is edited or deleted.
type AutomationActionIntent struct {
	ID                        string                       `json:"id"`
	TenantID                  string                       `json:"tenant_id"`
	ProjectID                 string                       `json:"project_id"`
	PolicyID                  string                       `json:"policy_id"`
	PolicyGeneration          int64                        `json:"policy_generation"`
	RuleID                    string                       `json:"rule_id"`
	Scope                     AutomationScope              `json:"scope"`
	Mode                      string                       `json:"mode"`
	Source                    string                       `json:"source"`
	Status                    string                       `json:"status"`
	RuleSnapshot              AutomationRule               `json:"rule_snapshot"`
	Evidence                  AutomationEvaluationEvidence `json:"evidence"`
	Decision                  AutomationEvaluationDecision `json:"decision"`
	EvidenceHash              string                       `json:"evidence_hash"`
	IdempotencyKey            string                       `json:"idempotency_key"`
	RollbackTarget            string                       `json:"rollback_target"`
	ProductionMutationAllowed bool                         `json:"production_mutation_allowed"`
	ExpiresAt                 time.Time                    `json:"expires_at"`
	CreatedAt                 time.Time                    `json:"created_at"`
	UpdatedAt                 time.Time                    `json:"updated_at"`
}

type AutomationActionIntentListResponse struct {
	Intents     []AutomationActionIntent `json:"intents"`
	GeneratedAt time.Time                `json:"generated_at"`
}

type AutomationActionIntentResponse struct {
	Intent AutomationActionIntent `json:"intent"`
}

type EvaluateAutomationPolicyRequest struct {
	PolicyID           string                              `json:"policy_id"`
	ExpectedGeneration int64                               `json:"expected_generation"`
	RuleID             string                              `json:"rule_id"`
	WindowStartedAt    time.Time                           `json:"window_started_at"`
	WindowEndedAt      time.Time                           `json:"window_ended_at"`
	RequestOutcomes    []AutomationRequestOutcomeAggregate `json:"request_outcomes"`
}

type AutomationEvaluationResponse struct {
	Decision      AutomationEvaluationDecision `json:"decision"`
	Intent        *AutomationActionIntent      `json:"intent,omitempty"`
	IntentCreated bool                         `json:"intent_created"`
}

// AutomationTriggerInput excludes server-owned invariant bindings. The API
// resolves those bindings from the registered action contract.
type AutomationTriggerInput struct {
	Type                  string                           `json:"type"`
	Source                string                           `json:"source"`
	RequestMetric         *AutomationRequestMetricSelector `json:"request_metric,omitempty"`
	RequiredEvidence      []string                         `json:"required_evidence,omitempty"`
	MinimumSamples        int                              `json:"minimum_samples,omitempty"`
	MinimumFailureDomains int                              `json:"minimum_failure_domains,omitempty"`
}

type AutomationActionInput struct {
	Type       string            `json:"type"`
	Parameters map[string]string `json:"parameters,omitempty"`
}

type AutomationRuleInput struct {
	ID          string                 `json:"id"`
	Description string                 `json:"description,omitempty"`
	Trigger     AutomationTriggerInput `json:"trigger"`
	Action      AutomationActionInput  `json:"action"`
}

type CreateAutomationPolicyRequest struct {
	TenantID    string                `json:"tenant_id,omitempty"`
	ProjectID   string                `json:"project_id,omitempty"`
	Name        string                `json:"name"`
	Description string                `json:"description,omitempty"`
	Kind        string                `json:"kind"`
	Scope       AutomationScope       `json:"scope"`
	Mode        string                `json:"mode"`
	Priority    int                   `json:"priority,omitempty"`
	SourceRef   string                `json:"source_ref,omitempty"`
	Rules       []AutomationRuleInput `json:"rules"`
	Metadata    map[string]string     `json:"metadata,omitempty"`
}

type UpdateAutomationPolicyRequest struct {
	ExpectedGeneration int64                 `json:"expected_generation"`
	Name               string                `json:"name"`
	Description        string                `json:"description,omitempty"`
	Mode               string                `json:"mode"`
	Priority           int                   `json:"priority,omitempty"`
	SourceRef          string                `json:"source_ref,omitempty"`
	Rules              []AutomationRuleInput `json:"rules"`
	Metadata           map[string]string     `json:"metadata,omitempty"`
}

type DeleteAutomationPolicyResponse struct {
	Deleted bool             `json:"deleted"`
	Policy  AutomationPolicy `json:"policy"`
}
