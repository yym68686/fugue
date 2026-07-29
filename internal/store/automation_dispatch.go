package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	defaultAutomationActionDispatchLimit = 200
	maxAutomationActionDispatchLimit     = 1000
	minAutomationActionDispatchLease     = time.Second
	maxAutomationActionDispatchLease     = 15 * time.Minute
)

var automationActionDispatchStatuses = map[string]struct{}{
	model.AutomationActionDispatchStatusHeld:       {},
	model.AutomationActionDispatchStatusReady:      {},
	model.AutomationActionDispatchStatusClaimed:    {},
	model.AutomationActionDispatchStatusExecuting:  {},
	model.AutomationActionDispatchStatusSucceeded:  {},
	model.AutomationActionDispatchStatusFailed:     {},
	model.AutomationActionDispatchStatusRolledBack: {},
	model.AutomationActionDispatchStatusExpired:    {},
	model.AutomationActionDispatchStatusCancelled:  {},
}

type AutomationActionDispatchFilter struct {
	TenantID      string
	ProjectID     string
	PolicyID      string
	AppID         string
	Status        string
	PlatformAdmin bool
	Limit         int
}

// CreateAutomationActionDispatch persists the action WAL entry exactly once
// for an immutable intent. A fencing token is allocated transactionally per
// subject. This method has no application/runtime side effects.
func (s *Store) CreateAutomationActionDispatch(
	dispatch model.AutomationActionDispatch,
) (model.AutomationActionDispatch, bool, error) {
	requestedStatus := strings.TrimSpace(strings.ToLower(dispatch.Status))
	normalized, err := normalizeAutomationActionDispatchForCreate(dispatch)
	if err != nil {
		return model.AutomationActionDispatch{}, false, err
	}
	expectedStatus := automationActionDispatchInitialStatus(normalized.SafetyDecision)
	if requestedStatus != "" && requestedStatus != expectedStatus {
		return model.AutomationActionDispatch{}, false, fmt.Errorf(
			"%w: new automation dispatch status must match the safety decision",
			ErrInvalidInput,
		)
	}
	normalized.Status = expectedStatus
	if s.usingDatabase() {
		return s.pgCreateAutomationActionDispatch(normalized)
	}

	var (
		out     model.AutomationActionDispatch
		created bool
	)
	err = s.withLockedState(true, func(state *model.State) error {
		intent, err := findAutomationActionIntentInState(state, normalized.IntentID)
		if err != nil {
			return err
		}
		if err := validateAutomationActionDispatchIntent(intent, normalized); err != nil {
			return err
		}

		for _, existing := range state.AutomationActionDispatches {
			if existing.IntentID != normalized.IntentID &&
				existing.IdempotencyKey != normalized.IdempotencyKey {
				continue
			}
			existing, err = normalizePersistedAutomationActionDispatch(existing)
			if err != nil {
				return err
			}
			// The immutable intent is the exactly-once boundary. A replay can
			// evaluate the same intent at a slightly different wall-clock
			// instant (the safety decision carries generated/expiry times);
			// the first durable decision therefore wins.
			if existing.IntentID == normalized.IntentID {
				out = cloneAutomationActionDispatch(existing)
				return nil
			}
			if !automationActionDispatchEquivalent(existing, normalized) {
				return ErrIdempotencyMismatch
			}
			out = cloneAutomationActionDispatch(existing)
			return nil
		}

		now := time.Now().UTC().Truncate(time.Microsecond)
		normalized.ID = strings.TrimSpace(normalized.ID)
		if normalized.ID == "" {
			normalized.ID = model.NewID("automation_dispatch")
		}
		for _, existing := range state.AutomationActionDispatches {
			if existing.ID == normalized.ID {
				return ErrConflict
			}
		}
		token, err := nextAutomationActionFencingTokenLocked(
			state,
			automationActionDispatchSubjectKey(normalized),
			now,
		)
		if err != nil {
			return err
		}
		normalized.FencingToken = token
		normalized.Version = 1
		normalized.CreatedAt = now
		normalized.UpdatedAt = now
		if normalized.Status == "" {
			normalized.Status = automationActionDispatchInitialStatus(normalized.SafetyDecision)
		}
		normalized.WALHash = automationActionDispatchWALHash(normalized)
		normalized, err = normalizePersistedAutomationActionDispatch(normalized)
		if err != nil {
			return err
		}
		state.AutomationActionDispatches = append(
			state.AutomationActionDispatches,
			cloneAutomationActionDispatch(normalized),
		)
		out = cloneAutomationActionDispatch(normalized)
		created = true
		return nil
	})
	return out, created, err
}

func (s *Store) ListAutomationActionDispatches(
	filter AutomationActionDispatchFilter,
) ([]model.AutomationActionDispatch, error) {
	filter, err := normalizeAutomationActionDispatchFilter(filter)
	if err != nil {
		return nil, err
	}
	if !filter.PlatformAdmin && filter.TenantID == "" {
		return nil, fmt.Errorf("%w: tenant ID is required", ErrInvalidInput)
	}
	if s.usingDatabase() {
		return s.pgListAutomationActionDispatches(filter)
	}

	out := make([]model.AutomationActionDispatch, 0)
	err = s.withLockedState(false, func(state *model.State) error {
		for _, dispatch := range state.AutomationActionDispatches {
			if !automationActionDispatchVisible(dispatch, filter) {
				continue
			}
			normalized, err := normalizePersistedAutomationActionDispatch(dispatch)
			if err != nil {
				return err
			}
			out = append(out, cloneAutomationActionDispatch(normalized))
		}
		sortAutomationActionDispatches(out)
		if len(out) > filter.Limit {
			out = out[:filter.Limit]
		}
		return nil
	})
	return out, err
}

func (s *Store) GetAutomationActionDispatch(
	id,
	tenantID string,
	platformAdmin bool,
) (model.AutomationActionDispatch, error) {
	id = strings.TrimSpace(id)
	tenantID = strings.TrimSpace(tenantID)
	if id == "" || (!platformAdmin && tenantID == "") {
		return model.AutomationActionDispatch{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetAutomationActionDispatch(id, tenantID, platformAdmin)
	}

	var out model.AutomationActionDispatch
	err := s.withLockedState(false, func(state *model.State) error {
		for _, dispatch := range state.AutomationActionDispatches {
			if dispatch.ID != id ||
				(!platformAdmin && dispatch.TenantID != tenantID) {
				continue
			}
			normalized, err := normalizePersistedAutomationActionDispatch(dispatch)
			if err != nil {
				return err
			}
			out = cloneAutomationActionDispatch(normalized)
			return nil
		}
		return ErrNotFound
	})
	return out, err
}

// GetAutomationActionDispatchByIntent returns the single WAL entry bound to an
// immutable intent. It is intentionally not tenant-scoped because callers use
// it only after resolving the intent through the control-loop store boundary.
func (s *Store) GetAutomationActionDispatchByIntent(
	intentID string,
) (model.AutomationActionDispatch, error) {
	intentID = strings.TrimSpace(intentID)
	if intentID == "" {
		return model.AutomationActionDispatch{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetAutomationActionDispatchByIntent(intentID)
	}
	var out model.AutomationActionDispatch
	err := s.withLockedState(false, func(state *model.State) error {
		for _, dispatch := range state.AutomationActionDispatches {
			if dispatch.IntentID != intentID {
				continue
			}
			normalized, err := normalizePersistedAutomationActionDispatch(dispatch)
			if err != nil {
				return err
			}
			out = cloneAutomationActionDispatch(normalized)
			return nil
		}
		return ErrNotFound
	})
	return out, err
}

// ClaimAutomationActionDispatch obtains a short lease and advances the
// subject fencing token. An active lease, an expired dispatch, or a stale
// token returns claimed=false. Reclaiming an expired lease is safe because the
// token is advanced before the new owner receives the record.
func (s *Store) ClaimAutomationActionDispatch(
	id,
	owner string,
	now time.Time,
	lease time.Duration,
) (model.AutomationActionDispatch, bool, error) {
	id = strings.TrimSpace(id)
	owner = strings.TrimSpace(owner)
	if id == "" || owner == "" {
		return model.AutomationActionDispatch{}, false, ErrInvalidInput
	}
	lease = normalizeAutomationActionDispatchLease(lease)
	if lease <= 0 {
		return model.AutomationActionDispatch{}, false, ErrInvalidInput
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC().Truncate(time.Microsecond)
	if s.usingDatabase() {
		return s.pgClaimAutomationActionDispatch(id, owner, now, lease)
	}

	var (
		out     model.AutomationActionDispatch
		claimed bool
	)
	err := s.withLockedState(true, func(state *model.State) error {
		index := -1
		for i := range state.AutomationActionDispatches {
			if state.AutomationActionDispatches[i].ID == id {
				index = i
				break
			}
		}
		if index < 0 {
			return ErrNotFound
		}
		dispatch, err := normalizePersistedAutomationActionDispatch(state.AutomationActionDispatches[index])
		if err != nil {
			return err
		}
		if !dispatch.ExpiresAt.After(now) {
			dispatch.Status = model.AutomationActionDispatchStatusExpired
			dispatch.Version++
			dispatch.UpdatedAt = now
			dispatch.LastError = "dispatch ttl expired before claim"
			dispatch.LeaseOwner = ""
			dispatch.LeaseExpiresAt = nil
			dispatch.CompletedAt = timePtr(now)
			dispatch.WALHash = automationActionDispatchWALHash(dispatch)
			state.AutomationActionDispatches[index] = dispatch
			return nil
		}
		if dispatch.Status != model.AutomationActionDispatchStatusReady &&
			!(dispatch.Status == model.AutomationActionDispatchStatusClaimed ||
				dispatch.Status == model.AutomationActionDispatchStatusExecuting) {
			return nil
		}
		if dispatch.CooldownUntil != nil && dispatch.CooldownUntil.After(now) {
			return nil
		}
		if dispatch.Status == model.AutomationActionDispatchStatusClaimed ||
			dispatch.Status == model.AutomationActionDispatchStatusExecuting {
			if dispatch.LeaseExpiresAt != nil && dispatch.LeaseExpiresAt.After(now) {
				return nil
			}
		}

		subjectKey := automationActionDispatchSubjectKey(dispatch)
		currentToken, found := automationActionFencingTokenLocked(state, subjectKey)
		if !found || currentToken != dispatch.FencingToken {
			dispatch.Status = model.AutomationActionDispatchStatusHeld
			dispatch.Version++
			dispatch.UpdatedAt = now
			dispatch.LastError = "dispatch fencing token is stale"
			dispatch.LeaseOwner = ""
			dispatch.LeaseExpiresAt = nil
			dispatch.WALHash = automationActionDispatchWALHash(dispatch)
			state.AutomationActionDispatches[index] = dispatch
			return nil
		}
		token, err := nextAutomationActionFencingTokenLocked(state, subjectKey, now)
		if err != nil {
			return err
		}
		leaseExpires := now.Add(lease)
		dispatch.FencingToken = token
		dispatch.Version++
		dispatch.Status = model.AutomationActionDispatchStatusClaimed
		dispatch.LeaseOwner = owner
		dispatch.LeaseExpiresAt = &leaseExpires
		dispatch.ClaimedAt = timePtr(now)
		dispatch.CompletedAt = nil
		dispatch.UpdatedAt = now
		dispatch.LastError = ""
		dispatch.WALHash = automationActionDispatchWALHash(dispatch)
		state.AutomationActionDispatches[index] = dispatch
		out = cloneAutomationActionDispatch(dispatch)
		claimed = true
		return nil
	})
	return out, claimed, err
}

// ValidateAutomationActionDispatchFence is the executor-side stale-writer
// guard. It checks both the dispatch row and the current subject token.
func (s *Store) ValidateAutomationActionDispatchFence(
	id string,
	fencingToken,
	version int64,
	now time.Time,
) error {
	id = strings.TrimSpace(id)
	if id == "" || fencingToken <= 0 || version <= 0 {
		return ErrInvalidInput
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	if s.usingDatabase() {
		return s.pgValidateAutomationActionDispatchFence(id, fencingToken, version, now)
	}
	return s.withLockedState(false, func(state *model.State) error {
		for _, candidate := range state.AutomationActionDispatches {
			if candidate.ID != id {
				continue
			}
			dispatch, err := normalizePersistedAutomationActionDispatch(candidate)
			if err != nil {
				return err
			}
			if dispatch.FencingToken != fencingToken ||
				dispatch.Version != version ||
				(dispatch.Status != model.AutomationActionDispatchStatusClaimed &&
					dispatch.Status != model.AutomationActionDispatchStatusExecuting) {
				return ErrConflict
			}
			if !dispatch.ExpiresAt.After(now) ||
				dispatch.LeaseExpiresAt == nil ||
				!dispatch.LeaseExpiresAt.After(now) {
				return ErrConflict
			}
			for _, fencing := range state.AutomationActionFencing {
				if fencing.SubjectKey == automationActionDispatchSubjectKey(dispatch) &&
					fencing.LastToken == fencingToken {
					return nil
				}
			}
			return ErrConflict
		}
		return ErrNotFound
	})
}

func normalizeAutomationActionDispatchFilter(
	filter AutomationActionDispatchFilter,
) (AutomationActionDispatchFilter, error) {
	filter.TenantID = strings.TrimSpace(filter.TenantID)
	filter.ProjectID = strings.TrimSpace(filter.ProjectID)
	filter.PolicyID = strings.TrimSpace(filter.PolicyID)
	filter.AppID = strings.TrimSpace(filter.AppID)
	filter.Status = strings.TrimSpace(strings.ToLower(filter.Status))
	if filter.Status != "" {
		if _, ok := automationActionDispatchStatuses[filter.Status]; !ok {
			return AutomationActionDispatchFilter{}, fmt.Errorf("%w: unsupported automation dispatch status", ErrInvalidInput)
		}
	}
	if filter.Limit == 0 {
		filter.Limit = defaultAutomationActionDispatchLimit
	}
	if filter.Limit < 1 || filter.Limit > maxAutomationActionDispatchLimit {
		return AutomationActionDispatchFilter{}, fmt.Errorf(
			"%w: automation dispatch limit must be between 1 and %d",
			ErrInvalidInput,
			maxAutomationActionDispatchLimit,
		)
	}
	return filter, nil
}

func normalizeAutomationActionDispatchLease(lease time.Duration) time.Duration {
	if lease <= 0 {
		lease = 30 * time.Second
	}
	if lease < minAutomationActionDispatchLease || lease > maxAutomationActionDispatchLease {
		return 0
	}
	return lease
}

func normalizeAutomationActionDispatchForCreate(
	dispatch model.AutomationActionDispatch,
) (model.AutomationActionDispatch, error) {
	dispatch.ID = strings.TrimSpace(dispatch.ID)
	dispatch.IntentID = strings.TrimSpace(dispatch.IntentID)
	dispatch.TenantID = strings.TrimSpace(dispatch.TenantID)
	dispatch.ProjectID = strings.TrimSpace(dispatch.ProjectID)
	dispatch.PolicyID = strings.TrimSpace(dispatch.PolicyID)
	dispatch.RuleID = strings.TrimSpace(dispatch.RuleID)
	dispatch.Scope.Type = strings.TrimSpace(strings.ToLower(dispatch.Scope.Type))
	dispatch.Scope.ID = strings.TrimSpace(dispatch.Scope.ID)
	dispatch.ActionType = strings.TrimSpace(dispatch.ActionType)
	dispatch.ContractID = strings.TrimSpace(dispatch.ContractID)
	dispatch.TriggerInvariant = strings.TrimSpace(dispatch.TriggerInvariant)
	dispatch.Subject = strings.TrimSpace(dispatch.Subject)
	dispatch.SourceGeneration = strings.TrimSpace(dispatch.SourceGeneration)
	dispatch.RollbackTarget = strings.TrimSpace(dispatch.RollbackTarget)
	dispatch.IdempotencyKey = strings.TrimSpace(strings.ToLower(dispatch.IdempotencyKey))
	dispatch.WALHash = strings.TrimSpace(strings.ToLower(dispatch.WALHash))
	dispatch.Status = strings.TrimSpace(strings.ToLower(dispatch.Status))
	dispatch.LastError = strings.TrimSpace(dispatch.LastError)
	dispatch.LeaseOwner = strings.TrimSpace(dispatch.LeaseOwner)
	dispatch.SafetyDecision = cloneActionSafetyDecision(dispatch.SafetyDecision)
	dispatch.SafetyDecision.ContractID = strings.TrimSpace(dispatch.SafetyDecision.ContractID)
	dispatch.SafetyDecision.GatePolicyID = strings.TrimSpace(dispatch.SafetyDecision.GatePolicyID)
	dispatch.SafetyDecision.Subject = strings.TrimSpace(dispatch.SafetyDecision.Subject)
	dispatch.SafetyDecision.EffectiveMode = strings.TrimSpace(strings.ToLower(dispatch.SafetyDecision.EffectiveMode))
	dispatch.SafetyDecision.GeneratedAt = dispatch.SafetyDecision.GeneratedAt.UTC()
	if dispatch.SafetyDecision.ExpiresAt != nil {
		value := dispatch.SafetyDecision.ExpiresAt.UTC()
		dispatch.SafetyDecision.ExpiresAt = &value
	}
	if dispatch.IntentID == "" ||
		dispatch.TenantID == "" ||
		dispatch.ProjectID == "" ||
		dispatch.PolicyID == "" ||
		dispatch.PolicyGeneration <= 0 ||
		dispatch.RuleID == "" ||
		dispatch.Scope.Type == "" ||
		dispatch.Scope.ID == "" ||
		dispatch.ActionType == "" ||
		dispatch.ContractID == "" ||
		dispatch.TriggerInvariant == "" ||
		dispatch.Subject == "" ||
		dispatch.SourceGeneration == "" ||
		dispatch.RollbackTarget == "" ||
		dispatch.IdempotencyKey == "" {
		return model.AutomationActionDispatch{}, fmt.Errorf("%w: automation dispatch immutable identity is incomplete", ErrInvalidInput)
	}
	if !validAutomationDispatchHash(dispatch.IdempotencyKey) {
		return model.AutomationActionDispatch{}, fmt.Errorf("%w: automation dispatch idempotency key is invalid", ErrInvalidInput)
	}
	if dispatch.Status != "" {
		if _, ok := automationActionDispatchStatuses[dispatch.Status]; !ok {
			return model.AutomationActionDispatch{}, fmt.Errorf(
				"%w: unsupported automation dispatch status %q",
				ErrInvalidInput,
				dispatch.Status,
			)
		}
	}
	// An already-expired record is valid durable history. Claiming it is
	// rejected by the state machine; persistence must not erase that audit
	// evidence.
	if strings.TrimSpace(dispatch.SafetyDecision.ContractID) == "" ||
		strings.TrimSpace(dispatch.SafetyDecision.ContractID) != dispatch.ContractID {
		return model.AutomationActionDispatch{}, fmt.Errorf("%w: safety decision contract does not match dispatch", ErrInvalidInput)
	}
	if dispatch.SafetyDecision.GeneratedAt.IsZero() ||
		!validAutomationActionSafetyMode(dispatch.SafetyDecision.EffectiveMode) ||
		(dispatch.SafetyDecision.Allowed && dispatch.SafetyDecision.WouldAction) ||
		(!dispatch.SafetyDecision.Pass &&
			(dispatch.SafetyDecision.Allowed ||
				dispatch.SafetyDecision.WouldAction ||
				dispatch.SafetyDecision.ProductionMutationAllowed)) ||
		(dispatch.SafetyDecision.ProductionMutationAllowed &&
			(!dispatch.SafetyDecision.Pass ||
				!dispatch.SafetyDecision.Allowed ||
				(dispatch.SafetyDecision.EffectiveMode != model.GatePolicyModeCanary &&
					dispatch.SafetyDecision.EffectiveMode != model.GatePolicyModeEnforced))) ||
		(dispatch.SafetyDecision.WouldAction &&
			(!dispatch.SafetyDecision.Pass ||
				dispatch.SafetyDecision.Allowed ||
				dispatch.SafetyDecision.ProductionMutationAllowed ||
				dispatch.SafetyDecision.EffectiveMode != model.GatePolicyModeShadow)) {
		return model.AutomationActionDispatch{}, fmt.Errorf("%w: automation dispatch safety decision is inconsistent", ErrInvalidInput)
	}
	if dispatch.Status == "" {
		dispatch.Status = automationActionDispatchInitialStatus(dispatch.SafetyDecision)
	}
	return dispatch, nil
}

func normalizePersistedAutomationActionDispatch(
	dispatch model.AutomationActionDispatch,
) (model.AutomationActionDispatch, error) {
	normalized, err := normalizeAutomationActionDispatchForCreate(dispatch)
	if err != nil {
		return model.AutomationActionDispatch{}, fmt.Errorf(
			"invalid persisted automation dispatch %q: %w",
			dispatch.ID,
			err,
		)
	}
	if normalized.ID == "" ||
		normalized.FencingToken <= 0 ||
		normalized.Version <= 0 ||
		normalized.CreatedAt.IsZero() ||
		normalized.UpdatedAt.IsZero() ||
		normalized.ExpiresAt.IsZero() {
		return model.AutomationActionDispatch{}, fmt.Errorf(
			"%w: persisted automation dispatch %q has invalid identity, token, version, or timestamps",
			ErrInvalidInput,
			dispatch.ID,
		)
	}
	if normalized.UpdatedAt.Before(normalized.CreatedAt) {
		return model.AutomationActionDispatch{}, fmt.Errorf(
			"%w: persisted automation dispatch %q has an updated_at before created_at",
			ErrInvalidInput,
			dispatch.ID,
		)
	}
	if (normalized.Status == model.AutomationActionDispatchStatusClaimed ||
		normalized.Status == model.AutomationActionDispatchStatusExecuting) &&
		(normalized.LeaseOwner == "" || normalized.LeaseExpiresAt == nil) {
		return model.AutomationActionDispatch{}, fmt.Errorf(
			"%w: persisted automation dispatch %q has an incomplete execution lease",
			ErrInvalidInput,
			dispatch.ID,
		)
	}
	if normalized.Status != model.AutomationActionDispatchStatusClaimed &&
		normalized.Status != model.AutomationActionDispatchStatusExecuting &&
		(normalized.LeaseOwner != "" || normalized.LeaseExpiresAt != nil) {
		return model.AutomationActionDispatch{}, fmt.Errorf(
			"%w: persisted automation dispatch %q has a lease in non-executable state",
			ErrInvalidInput,
			dispatch.ID,
		)
	}
	if !validAutomationDispatchHash(normalized.WALHash) ||
		normalized.WALHash != automationActionDispatchWALHash(normalized) {
		return model.AutomationActionDispatch{}, fmt.Errorf(
			"%w: persisted automation dispatch %q WAL hash is invalid",
			ErrInvalidInput,
			dispatch.ID,
		)
	}
	normalized.CreatedAt = normalized.CreatedAt.UTC()
	normalized.UpdatedAt = normalized.UpdatedAt.UTC()
	normalized.ExpiresAt = normalized.ExpiresAt.UTC()
	if normalized.LeaseExpiresAt != nil {
		value := normalized.LeaseExpiresAt.UTC()
		normalized.LeaseExpiresAt = &value
	}
	if normalized.CooldownUntil != nil {
		value := normalized.CooldownUntil.UTC()
		normalized.CooldownUntil = &value
	}
	if normalized.ClaimedAt != nil {
		value := normalized.ClaimedAt.UTC()
		normalized.ClaimedAt = &value
	}
	if normalized.CompletedAt != nil {
		value := normalized.CompletedAt.UTC()
		normalized.CompletedAt = &value
	}
	if normalized.SafetyDecision.ExpiresAt != nil &&
		normalized.ExpiresAt.After(normalized.SafetyDecision.ExpiresAt.UTC()) &&
		(normalized.Status == model.AutomationActionDispatchStatusReady ||
			normalized.SafetyDecision.ProductionMutationAllowed) {
		return model.AutomationActionDispatch{}, fmt.Errorf(
			"%w: persisted automation dispatch %q outlives its safety decision",
			ErrInvalidInput,
			dispatch.ID,
		)
	}
	return cloneAutomationActionDispatch(normalized), nil
}

func validAutomationActionSafetyMode(mode string) bool {
	switch strings.TrimSpace(strings.ToLower(mode)) {
	case model.GatePolicyModeDisabled,
		model.GatePolicyModeShadow,
		model.GatePolicyModeCanary,
		model.GatePolicyModeEnforced:
		return true
	default:
		return false
	}
}

func validateAutomationActionDispatchIntent(
	intent model.AutomationActionIntent,
	dispatch model.AutomationActionDispatch,
) error {
	if intent.TenantID != dispatch.TenantID ||
		intent.ProjectID != dispatch.ProjectID ||
		intent.PolicyID != dispatch.PolicyID ||
		intent.PolicyGeneration != dispatch.PolicyGeneration ||
		intent.RuleID != dispatch.RuleID ||
		intent.Scope != dispatch.Scope ||
		intent.IdempotencyKey != dispatch.IdempotencyKey ||
		intent.RollbackTarget != dispatch.RollbackTarget {
		return ErrConflict
	}
	rule := intent.RuleSnapshot
	if dispatch.ActionType != rule.Action.Type ||
		dispatch.ContractID != rule.Safety.ActionContractID ||
		dispatch.TriggerInvariant != rule.Trigger.InvariantID ||
		dispatch.Subject != intent.Scope.ID {
		return fmt.Errorf("%w: dispatch action contract does not match the immutable rule", ErrInvalidInput)
	}
	if dispatch.SafetyDecision.Subject != "" &&
		dispatch.SafetyDecision.Subject != dispatch.Subject {
		return fmt.Errorf("%w: safety decision subject does not match dispatch", ErrInvalidInput)
	}
	if rule.Safety.GatePolicyID != "" &&
		dispatch.SafetyDecision.GatePolicyID != "" &&
		dispatch.SafetyDecision.GatePolicyID != rule.Safety.GatePolicyID {
		return fmt.Errorf("%w: safety decision gate policy does not match the immutable rule", ErrInvalidInput)
	}
	if dispatch.SourceGeneration != intent.Evidence.AppRevision {
		return fmt.Errorf("%w: dispatch source generation does not match intent evidence", ErrInvalidInput)
	}
	return nil
}

// ValidateAutomationActionDispatchIntentForReplay verifies that a previously
// persisted dispatch still belongs to the immutable intent supplied by a
// trusted control-loop replay. It does not read or mutate store state.
func ValidateAutomationActionDispatchIntentForReplay(
	intent model.AutomationActionIntent,
	dispatch model.AutomationActionDispatch,
) error {
	return validateAutomationActionDispatchIntent(intent, dispatch)
}

func findAutomationActionIntentInState(
	state *model.State,
	id string,
) (model.AutomationActionIntent, error) {
	for _, intent := range state.AutomationActionIntents {
		if intent.ID != id {
			continue
		}
		return normalizePersistedAutomationActionIntent(intent)
	}
	return model.AutomationActionIntent{}, ErrNotFound
}

func automationActionDispatchInitialStatus(
	decision model.ActionSafetyDecision,
) string {
	if decision.Allowed && decision.ProductionMutationAllowed {
		return model.AutomationActionDispatchStatusReady
	}
	return model.AutomationActionDispatchStatusHeld
}

func automationActionDispatchSubjectKey(
	dispatch model.AutomationActionDispatch,
) string {
	return strings.Join([]string{
		dispatch.TenantID,
		dispatch.Scope.Type,
		dispatch.Scope.ID,
		dispatch.ActionType,
		dispatch.Subject,
	}, "\n")
}

func nextAutomationActionFencingTokenLocked(
	state *model.State,
	subjectKey string,
	now time.Time,
) (int64, error) {
	if state == nil || strings.TrimSpace(subjectKey) == "" {
		return 0, ErrInvalidInput
	}
	for index := range state.AutomationActionFencing {
		if state.AutomationActionFencing[index].SubjectKey != subjectKey {
			continue
		}
		if state.AutomationActionFencing[index].LastToken == math.MaxInt64 {
			return 0, fmt.Errorf("%w: automation fencing token exhausted", ErrConflict)
		}
		state.AutomationActionFencing[index].LastToken++
		state.AutomationActionFencing[index].UpdatedAt = now
		return state.AutomationActionFencing[index].LastToken, nil
	}
	state.AutomationActionFencing = append(state.AutomationActionFencing, model.AutomationActionFencing{
		SubjectKey: subjectKey,
		LastToken:  1,
		UpdatedAt:  now,
	})
	return 1, nil
}

func automationActionFencingTokenLocked(
	state *model.State,
	subjectKey string,
) (int64, bool) {
	if state == nil {
		return 0, false
	}
	for _, fencing := range state.AutomationActionFencing {
		if fencing.SubjectKey == subjectKey {
			return fencing.LastToken, true
		}
	}
	return 0, false
}

func automationActionDispatchWALHash(
	dispatch model.AutomationActionDispatch,
) string {
	payload := struct {
		IntentID         string                     `json:"intent_id"`
		TenantID         string                     `json:"tenant_id"`
		ProjectID        string                     `json:"project_id"`
		PolicyID         string                     `json:"policy_id"`
		PolicyGeneration int64                      `json:"policy_generation"`
		RuleID           string                     `json:"rule_id"`
		Scope            model.AutomationScope      `json:"scope"`
		ActionType       string                     `json:"action_type"`
		ContractID       string                     `json:"contract_id"`
		TriggerInvariant string                     `json:"trigger_invariant"`
		Subject          string                     `json:"subject"`
		SourceGeneration string                     `json:"source_generation"`
		RollbackTarget   string                     `json:"rollback_target"`
		IdempotencyKey   string                     `json:"idempotency_key"`
		SafetyDecision   model.ActionSafetyDecision `json:"safety_decision"`
		Status           string                     `json:"status"`
		FencingToken     int64                      `json:"fencing_token"`
		Version          int64                      `json:"version"`
		ExpiresAt        time.Time                  `json:"expires_at"`
	}{
		IntentID:         dispatch.IntentID,
		TenantID:         dispatch.TenantID,
		ProjectID:        dispatch.ProjectID,
		PolicyID:         dispatch.PolicyID,
		PolicyGeneration: dispatch.PolicyGeneration,
		RuleID:           dispatch.RuleID,
		Scope:            dispatch.Scope,
		ActionType:       dispatch.ActionType,
		ContractID:       dispatch.ContractID,
		TriggerInvariant: dispatch.TriggerInvariant,
		Subject:          dispatch.Subject,
		SourceGeneration: dispatch.SourceGeneration,
		RollbackTarget:   dispatch.RollbackTarget,
		IdempotencyKey:   dispatch.IdempotencyKey,
		SafetyDecision:   cloneActionSafetyDecision(dispatch.SafetyDecision),
		Status:           dispatch.Status,
		FencingToken:     dispatch.FencingToken,
		Version:          dispatch.Version,
		ExpiresAt:        dispatch.ExpiresAt.UTC(),
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		// All fields are JSON primitives; this is defensive and keeps the
		// hash format total if the model later gains an unsupported field.
		return ""
	}
	sum := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func validAutomationDispatchHash(value string) bool {
	value = strings.TrimSpace(strings.ToLower(value))
	if len(value) != len("sha256:")+64 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	for _, char := range value[len("sha256:"):] {
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f')) {
			return false
		}
	}
	return true
}

func automationActionDispatchEquivalent(
	existing,
	candidate model.AutomationActionDispatch,
) bool {
	existing.ID = ""
	candidate.ID = ""
	existing.Status = ""
	candidate.Status = ""
	existing.Version = 0
	candidate.Version = 0
	existing.FencingToken = 0
	candidate.FencingToken = 0
	existing.WALHash = ""
	candidate.WALHash = ""
	existing.CreatedAt = time.Time{}
	candidate.CreatedAt = time.Time{}
	existing.UpdatedAt = time.Time{}
	candidate.UpdatedAt = time.Time{}
	existing.LeaseOwner = ""
	candidate.LeaseOwner = ""
	existing.LeaseExpiresAt = nil
	candidate.LeaseExpiresAt = nil
	existing.CooldownUntil = nil
	candidate.CooldownUntil = nil
	existing.LastError = ""
	candidate.LastError = ""
	existing.ClaimedAt = nil
	candidate.ClaimedAt = nil
	existing.CompletedAt = nil
	candidate.CompletedAt = nil
	return reflect.DeepEqual(existing, candidate)
}

func automationActionDispatchVisible(
	dispatch model.AutomationActionDispatch,
	filter AutomationActionDispatchFilter,
) bool {
	if filter.PlatformAdmin {
		if filter.TenantID != "" && dispatch.TenantID != filter.TenantID {
			return false
		}
	} else if dispatch.TenantID != filter.TenantID {
		return false
	}
	return (filter.ProjectID == "" || dispatch.ProjectID == filter.ProjectID) &&
		(filter.PolicyID == "" || dispatch.PolicyID == filter.PolicyID) &&
		(filter.AppID == "" || (dispatch.Scope.Type == model.AutomationScopeApp && dispatch.Scope.ID == filter.AppID)) &&
		(filter.Status == "" || dispatch.Status == filter.Status)
}

func sortAutomationActionDispatches(dispatches []model.AutomationActionDispatch) {
	sort.Slice(dispatches, func(i, j int) bool {
		if !dispatches[i].CreatedAt.Equal(dispatches[j].CreatedAt) {
			return dispatches[i].CreatedAt.After(dispatches[j].CreatedAt)
		}
		return dispatches[i].ID > dispatches[j].ID
	})
}

func cloneAutomationActionDispatch(
	dispatch model.AutomationActionDispatch,
) model.AutomationActionDispatch {
	dispatch.SafetyDecision = cloneActionSafetyDecision(dispatch.SafetyDecision)
	if dispatch.LeaseExpiresAt != nil {
		value := dispatch.LeaseExpiresAt.UTC()
		dispatch.LeaseExpiresAt = &value
	}
	if dispatch.CooldownUntil != nil {
		value := dispatch.CooldownUntil.UTC()
		dispatch.CooldownUntil = &value
	}
	if dispatch.ClaimedAt != nil {
		value := dispatch.ClaimedAt.UTC()
		dispatch.ClaimedAt = &value
	}
	if dispatch.CompletedAt != nil {
		value := dispatch.CompletedAt.UTC()
		dispatch.CompletedAt = &value
	}
	return dispatch
}

func cloneActionSafetyDecision(
	decision model.ActionSafetyDecision,
) model.ActionSafetyDecision {
	if decision.ExpiresAt != nil {
		value := decision.ExpiresAt.UTC()
		decision.ExpiresAt = &value
	}
	if decision.Violations != nil {
		decision.Violations = append([]model.ActionSafetyViolation(nil), decision.Violations...)
	}
	if decision.EvidenceStates != nil {
		decision.EvidenceStates = make(map[string]string, len(decision.EvidenceStates))
		for key, value := range decision.EvidenceStates {
			decision.EvidenceStates[key] = value
		}
	}
	decision.BlastRadius.Before = cloneIntMap(decision.BlastRadius.Before)
	decision.BlastRadius.After = cloneIntMap(decision.BlastRadius.After)
	if decision.BlastRadius.Violations != nil {
		decision.BlastRadius.Violations = make(map[string]string, len(decision.BlastRadius.Violations))
		for key, value := range decision.BlastRadius.Violations {
			decision.BlastRadius.Violations[key] = value
		}
	}
	return decision
}

func cloneIntMap(input map[string]int) map[string]int {
	if input == nil {
		return nil
	}
	output := make(map[string]int, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func deleteAutomationActionDispatchesByTenant(
	dispatches []model.AutomationActionDispatch,
	tenantID string,
) []model.AutomationActionDispatch {
	filtered := dispatches[:0]
	for _, dispatch := range dispatches {
		if dispatch.TenantID != tenantID {
			filtered = append(filtered, dispatch)
		}
	}
	return filtered
}

func deleteAutomationActionFencingByTenant(
	fencing []model.AutomationActionFencing,
	tenantID string,
) []model.AutomationActionFencing {
	prefix := strings.TrimSpace(tenantID) + "\n"
	filtered := fencing[:0]
	for _, record := range fencing {
		if !strings.HasPrefix(record.SubjectKey, prefix) {
			filtered = append(filtered, record)
		}
	}
	return filtered
}

func timePtr(value time.Time) *time.Time {
	value = value.UTC()
	return &value
}
