package model

import "time"

// AutomationActionDispatchStatus is the durable state of the action WAL
// entry. A dispatch is only claimable in ready state; held entries are
// intentionally inert until a later, independently evaluated pass makes them
// eligible.
const (
	AutomationActionDispatchStatusHeld       = "held"
	AutomationActionDispatchStatusReady      = "ready"
	AutomationActionDispatchStatusClaimed    = "claimed"
	AutomationActionDispatchStatusExecuting  = "executing"
	AutomationActionDispatchStatusSucceeded  = "succeeded"
	AutomationActionDispatchStatusFailed     = "failed"
	AutomationActionDispatchStatusRolledBack = "rolled_back"
	AutomationActionDispatchStatusExpired    = "expired"
	AutomationActionDispatchStatusCancelled  = "cancelled"
)

// AutomationActionDispatch records the immutable action request and the
// mutable, compare-and-swap guarded execution envelope. The record is the
// durable WAL boundary between evaluation and a future executor; creating it
// never mutates an application.
type AutomationActionDispatch struct {
	ID               string               `json:"id"`
	IntentID         string               `json:"intent_id"`
	TenantID         string               `json:"tenant_id"`
	ProjectID        string               `json:"project_id"`
	PolicyID         string               `json:"policy_id"`
	PolicyGeneration int64                `json:"policy_generation"`
	RuleID           string               `json:"rule_id"`
	Scope            AutomationScope      `json:"scope"`
	ActionType       string               `json:"action_type"`
	ContractID       string               `json:"contract_id"`
	TriggerInvariant string               `json:"trigger_invariant"`
	Subject          string               `json:"subject"`
	SourceGeneration string               `json:"source_generation"`
	RollbackTarget   string               `json:"rollback_target"`
	IdempotencyKey   string               `json:"idempotency_key"`
	WALHash          string               `json:"wal_hash"`
	SafetyDecision   ActionSafetyDecision `json:"safety_decision"`
	Status           string               `json:"status"`
	FencingToken     int64                `json:"fencing_token"`
	Version          int64                `json:"version"`
	ExpiresAt        time.Time            `json:"expires_at"`
	LeaseOwner       string               `json:"lease_owner,omitempty"`
	LeaseExpiresAt   *time.Time           `json:"lease_expires_at,omitempty"`
	CooldownUntil    *time.Time           `json:"cooldown_until,omitempty"`
	LastError        string               `json:"last_error,omitempty"`
	CreatedAt        time.Time            `json:"created_at"`
	UpdatedAt        time.Time            `json:"updated_at"`
	ClaimedAt        *time.Time           `json:"claimed_at,omitempty"`
	CompletedAt      *time.Time           `json:"completed_at,omitempty"`
}

// AutomationActionFencing is a monotonic per-subject token source. A token is
// allocated transactionally when a dispatch is written, so a newer dispatch
// invalidates any stale executor that still holds an older token.
type AutomationActionFencing struct {
	SubjectKey string    `json:"subject_key"`
	LastToken  int64     `json:"last_token"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type AutomationActionDispatchListResponse struct {
	Dispatches  []AutomationActionDispatch `json:"dispatches"`
	GeneratedAt time.Time                  `json:"generated_at"`
}

type AutomationActionDispatchResponse struct {
	Dispatch AutomationActionDispatch `json:"dispatch"`
}
