package store

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	automationdomain "fugue/internal/automation"
	"fugue/internal/model"
)

const (
	defaultAutomationActionIntentLimit = 200
	maxAutomationActionIntentLimit     = 1000
)

type AutomationActionIntentFilter struct {
	TenantID      string
	ProjectID     string
	PolicyID      string
	AppID         string
	Source        string
	Status        string
	PlatformAdmin bool
	Limit         int
}

// CreateAutomationActionIntent appends a permanently observe-only intent. A
// repeated immutable evaluation reuses the existing idempotency key; a key
// collision with different content fails closed.
func (s *Store) CreateAutomationActionIntent(
	intent model.AutomationActionIntent,
) (model.AutomationActionIntent, bool, error) {
	intent, err := normalizeAutomationActionIntentForStore(intent)
	if err != nil {
		return model.AutomationActionIntent{}, false, err
	}
	return s.pgCreateAutomationActionIntent(intent)
}

func (s *Store) ListAutomationActionIntents(
	filter AutomationActionIntentFilter,
) ([]model.AutomationActionIntent, error) {
	filter, err := normalizeAutomationActionIntentFilter(filter)
	if err != nil {
		return nil, err
	}
	if !filter.PlatformAdmin && filter.TenantID == "" {
		return nil, fmt.Errorf("%w: tenant ID is required", ErrInvalidInput)
	}
	return s.pgListAutomationActionIntents(filter)
}

func (s *Store) GetAutomationActionIntent(
	id,
	tenantID string,
	platformAdmin bool,
) (model.AutomationActionIntent, error) {
	id = strings.TrimSpace(id)
	tenantID = strings.TrimSpace(tenantID)
	if id == "" || (!platformAdmin && tenantID == "") {
		return model.AutomationActionIntent{}, ErrInvalidInput
	}
	return s.pgGetAutomationActionIntent(id, tenantID, platformAdmin)
}

func normalizeAutomationActionIntentFilter(
	filter AutomationActionIntentFilter,
) (AutomationActionIntentFilter, error) {
	filter.TenantID = strings.TrimSpace(filter.TenantID)
	filter.ProjectID = strings.TrimSpace(filter.ProjectID)
	filter.PolicyID = strings.TrimSpace(filter.PolicyID)
	filter.AppID = strings.TrimSpace(filter.AppID)
	filter.Source = strings.TrimSpace(strings.ToLower(filter.Source))
	filter.Status = strings.TrimSpace(strings.ToLower(filter.Status))
	switch filter.Source {
	case "", model.AutomationIntentSourceAdminReplay, model.AutomationIntentSourceControlLoop:
	default:
		return AutomationActionIntentFilter{}, fmt.Errorf("%w: unsupported automation intent source", ErrInvalidInput)
	}
	switch filter.Status {
	case "", model.AutomationIntentStatusObserved:
	default:
		return AutomationActionIntentFilter{}, fmt.Errorf("%w: unsupported automation intent status", ErrInvalidInput)
	}
	if filter.Limit == 0 {
		filter.Limit = defaultAutomationActionIntentLimit
	}
	if filter.Limit < 1 || filter.Limit > maxAutomationActionIntentLimit {
		return AutomationActionIntentFilter{}, fmt.Errorf(
			"%w: automation intent limit must be between 1 and %d",
			ErrInvalidInput,
			maxAutomationActionIntentLimit,
		)
	}
	return filter, nil
}

func normalizeAutomationActionIntentForStore(
	intent model.AutomationActionIntent,
) (model.AutomationActionIntent, error) {
	normalized, err := automationdomain.NormalizeObservedIntent(intent)
	if err != nil {
		return model.AutomationActionIntent{}, fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	return cloneAutomationActionIntent(normalized), nil
}

func normalizePersistedAutomationActionIntent(
	intent model.AutomationActionIntent,
) (model.AutomationActionIntent, error) {
	normalized, err := normalizeAutomationActionIntentForStore(intent)
	if err != nil {
		return model.AutomationActionIntent{}, fmt.Errorf("invalid persisted automation intent %q: %w", intent.ID, err)
	}
	if normalized.ID == "" || normalized.CreatedAt.IsZero() || normalized.UpdatedAt.IsZero() {
		return model.AutomationActionIntent{}, fmt.Errorf(
			"%w: persisted automation intent %q has an invalid ID or timestamp",
			ErrInvalidInput,
			intent.ID,
		)
	}
	return normalized, nil
}

func validateAutomationActionIntentPolicy(
	policy model.AutomationPolicy,
	intent model.AutomationActionIntent,
) error {
	if policy.TenantID != intent.TenantID ||
		policy.ProjectID != intent.ProjectID ||
		policy.Generation != intent.PolicyGeneration ||
		policy.Scope != intent.Scope ||
		policy.Mode != intent.Mode {
		return ErrConflict
	}
	for _, rule := range policy.Rules {
		if rule.ID == intent.RuleID {
			if !reflect.DeepEqual(rule, intent.RuleSnapshot) {
				return fmt.Errorf("%w: automation intent rule snapshot does not match current policy generation", ErrInvalidInput)
			}
			return nil
		}
	}
	return ErrNotFound
}

func automationActionIntentsEquivalent(
	existing,
	candidate model.AutomationActionIntent,
) bool {
	existing.ID = ""
	existing.CreatedAt = time.Time{}
	existing.UpdatedAt = time.Time{}
	existing.Decision.EvaluatedAt = time.Time{}
	existing.ExpiresAt = time.Time{}
	candidate.ID = ""
	candidate.CreatedAt = time.Time{}
	candidate.UpdatedAt = time.Time{}
	candidate.Decision.EvaluatedAt = time.Time{}
	candidate.ExpiresAt = time.Time{}
	return reflect.DeepEqual(existing, candidate)
}

func cloneAutomationActionIntent(intent model.AutomationActionIntent) model.AutomationActionIntent {
	intent.RuleSnapshot = cloneAutomationPolicy(model.AutomationPolicy{
		Rules: []model.AutomationRule{intent.RuleSnapshot},
	}).Rules[0]
	intent.Evidence.RequestOutcomes = append(
		[]model.AutomationRequestOutcomeAggregate(nil),
		intent.Evidence.RequestOutcomes...,
	)
	intent.Decision.FailureDomains = append([]string{}, intent.Decision.FailureDomains...)
	intent.Decision.ReasonCodes = append([]string{}, intent.Decision.ReasonCodes...)
	return intent
}
