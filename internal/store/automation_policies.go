package store

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

// AutomationPolicyFilter describes the authorization boundary for a policy
// read. PlatformAdmin is deliberately part of the store contract so a future
// API handler cannot accidentally omit tenant isolation when switching
// between the JSON and Postgres backends.
type AutomationPolicyFilter struct {
	TenantID      string
	ProjectID     string
	PlatformAdmin bool
}

// CreateAutomationPolicy persists a user-owned policy. Managed system
// policies are compiled from the registry and must never be written here.
func (s *Store) CreateAutomationPolicy(policy model.AutomationPolicy) (model.AutomationPolicy, error) {
	policy, err := normalizeAutomationPolicyForStore(policy)
	if err != nil {
		return model.AutomationPolicy{}, err
	}
	if s.usingDatabase() {
		return s.pgCreateAutomationPolicy(policy)
	}

	var out model.AutomationPolicy
	err = s.withLockedState(true, func(state *model.State) error {
		if err := validateAutomationPolicyParentsState(state, policy); err != nil {
			return err
		}
		if policy.ID == "" {
			policy.ID = model.NewID("automation_policy")
		}
		for _, existing := range state.AutomationPolicies {
			if existing.ID == policy.ID {
				return ErrConflict
			}
			if automationPolicyNameConflict(existing, policy) {
				return ErrConflict
			}
		}
		now := time.Now().UTC()
		// A caller cannot choose the initial version or timestamps. This keeps
		// the generation boundary authoritative in the store.
		policy.Generation = 1
		policy.CreatedAt = now
		policy.UpdatedAt = now
		policy = cloneAutomationPolicy(policy)
		state.AutomationPolicies = append(state.AutomationPolicies, policy)
		out = cloneAutomationPolicy(policy)
		return nil
	})
	return out, err
}

// ListAutomationPolicies returns only user-owned persisted policies. Managed
// policies are projected by internal/automation and are intentionally absent.
func (s *Store) ListAutomationPolicies(filter AutomationPolicyFilter) ([]model.AutomationPolicy, error) {
	filter = normalizeAutomationPolicyFilter(filter)
	if !filter.PlatformAdmin && filter.TenantID == "" {
		return nil, fmt.Errorf("%w: tenant ID is required", ErrInvalidInput)
	}
	if s.usingDatabase() {
		return s.pgListAutomationPolicies(filter)
	}

	out := make([]model.AutomationPolicy, 0)
	err := s.withLockedState(false, func(state *model.State) error {
		for _, policy := range state.AutomationPolicies {
			if !automationPolicyVisible(policy, filter) {
				continue
			}
			normalized, err := normalizePersistedAutomationPolicy(policy)
			if err != nil {
				return err
			}
			out = append(out, cloneAutomationPolicy(normalized))
		}
		sortAutomationPolicies(out)
		return nil
	})
	return out, err
}

func (s *Store) GetAutomationPolicy(id, tenantID string, platformAdmin bool) (model.AutomationPolicy, error) {
	id = strings.TrimSpace(id)
	tenantID = strings.TrimSpace(tenantID)
	if id == "" || (!platformAdmin && tenantID == "") {
		return model.AutomationPolicy{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetAutomationPolicy(id, tenantID, platformAdmin)
	}

	var out model.AutomationPolicy
	err := s.withLockedState(false, func(state *model.State) error {
		for _, policy := range state.AutomationPolicies {
			if policy.ID != id || !automationPolicyAccessible(policy, tenantID, platformAdmin) {
				continue
			}
			normalized, err := normalizePersistedAutomationPolicy(policy)
			if err != nil {
				return err
			}
			out = cloneAutomationPolicy(normalized)
			return nil
		}
		return ErrNotFound
	})
	return out, err
}

// UpdateAutomationPolicy performs a compare-and-swap update. The parent
// tenant/project and ownership fields are immutable in this operation.
func (s *Store) UpdateAutomationPolicy(policy model.AutomationPolicy, tenantID string, platformAdmin bool, expectedGeneration int64) (model.AutomationPolicy, error) {
	id := strings.TrimSpace(policy.ID)
	tenantID = strings.TrimSpace(tenantID)
	if id == "" || expectedGeneration <= 0 || (!platformAdmin && tenantID == "") {
		return model.AutomationPolicy{}, ErrInvalidInput
	}
	policy.ID = id
	if s.usingDatabase() {
		return s.pgUpdateAutomationPolicy(policy, tenantID, platformAdmin, expectedGeneration)
	}

	var out model.AutomationPolicy
	err := s.withLockedState(true, func(state *model.State) error {
		index := findAutomationPolicyByID(state.AutomationPolicies, id)
		if index < 0 || !automationPolicyAccessible(state.AutomationPolicies[index], tenantID, platformAdmin) {
			return ErrNotFound
		}
		existing, err := normalizePersistedAutomationPolicy(state.AutomationPolicies[index])
		if err != nil {
			return err
		}
		if existing.Generation != expectedGeneration {
			return ErrConflict
		}
		if strings.TrimSpace(policy.TenantID) != existing.TenantID ||
			strings.TrimSpace(policy.ProjectID) != existing.ProjectID ||
			strings.TrimSpace(policy.OwnerType) != existing.OwnerType ||
			policy.Managed != existing.Managed ||
			strings.TrimSpace(strings.ToLower(policy.Kind)) != existing.Kind {
			return fmt.Errorf("%w: policy identity and ownership fields are immutable", ErrInvalidInput)
		}
		normalized, err := normalizeAutomationPolicyForStore(policy)
		if err != nil {
			return err
		}
		if err := validateAutomationPolicyParentsState(state, normalized); err != nil {
			return err
		}
		for idx, candidate := range state.AutomationPolicies {
			if idx != index && automationPolicyNameConflict(candidate, normalized) {
				return ErrConflict
			}
		}
		normalized.ID = existing.ID
		normalized.TenantID = existing.TenantID
		normalized.ProjectID = existing.ProjectID
		normalized.OwnerType = existing.OwnerType
		normalized.Managed = existing.Managed
		normalized.CreatedAt = existing.CreatedAt
		normalized.Generation = existing.Generation + 1
		normalized.UpdatedAt = time.Now().UTC()
		normalized = cloneAutomationPolicy(normalized)
		state.AutomationPolicies[index] = normalized
		out = cloneAutomationPolicy(normalized)
		return nil
	})
	return out, err
}

func (s *Store) DeleteAutomationPolicy(id, tenantID string, platformAdmin bool, expectedGeneration int64) (model.AutomationPolicy, error) {
	id = strings.TrimSpace(id)
	tenantID = strings.TrimSpace(tenantID)
	if id == "" || expectedGeneration <= 0 || (!platformAdmin && tenantID == "") {
		return model.AutomationPolicy{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgDeleteAutomationPolicy(id, tenantID, platformAdmin, expectedGeneration)
	}

	var removed model.AutomationPolicy
	err := s.withLockedState(true, func(state *model.State) error {
		index := findAutomationPolicyByID(state.AutomationPolicies, id)
		if index < 0 || !automationPolicyAccessible(state.AutomationPolicies[index], tenantID, platformAdmin) {
			return ErrNotFound
		}
		existing, err := normalizePersistedAutomationPolicy(state.AutomationPolicies[index])
		if err != nil {
			return err
		}
		if existing.Generation != expectedGeneration {
			return ErrConflict
		}
		removed = cloneAutomationPolicy(existing)
		state.AutomationPolicies = append(state.AutomationPolicies[:index], state.AutomationPolicies[index+1:]...)
		return nil
	})
	return removed, err
}

func normalizeAutomationPolicyFilter(filter AutomationPolicyFilter) AutomationPolicyFilter {
	filter.TenantID = strings.TrimSpace(filter.TenantID)
	filter.ProjectID = strings.TrimSpace(filter.ProjectID)
	return filter
}

func normalizeAutomationPolicyForStore(policy model.AutomationPolicy) (model.AutomationPolicy, error) {
	policy.ID = strings.TrimSpace(policy.ID)
	policy.TenantID = strings.TrimSpace(policy.TenantID)
	policy.ProjectID = strings.TrimSpace(policy.ProjectID)
	policy.Name = strings.TrimSpace(policy.Name)
	policy.Description = strings.TrimSpace(policy.Description)
	policy.Kind = strings.TrimSpace(strings.ToLower(policy.Kind))
	policy.OwnerType = strings.TrimSpace(strings.ToLower(policy.OwnerType))
	policy.Scope.Type = strings.TrimSpace(strings.ToLower(policy.Scope.Type))
	policy.Scope.ID = strings.TrimSpace(policy.Scope.ID)
	policy.Mode = strings.TrimSpace(strings.ToLower(policy.Mode))
	policy.SourceRef = strings.TrimSpace(policy.SourceRef)

	if policy.TenantID == "" || policy.Name == "" || policy.Kind == "" ||
		policy.OwnerType == "" || policy.Scope.Type == "" {
		return model.AutomationPolicy{}, fmt.Errorf("%w: tenant, name, kind, owner type, and scope type are required", ErrInvalidInput)
	}
	if policy.OwnerType != model.AutomationOwnerUser || policy.Managed ||
		policy.Kind == model.AutomationPolicyKindManagedSystem {
		return model.AutomationPolicy{}, fmt.Errorf("%w: only unmanaged user policies may be persisted", ErrInvalidInput)
	}
	switch policy.Mode {
	case model.GatePolicyModeDisabled, model.GatePolicyModeShadow:
	default:
		return model.AutomationPolicy{}, fmt.Errorf("%w: user automation mode must be disabled or shadow", ErrInvalidInput)
	}
	if policy.Priority < 0 || int64(policy.Priority) > 2_147_483_647 {
		return model.AutomationPolicy{}, fmt.Errorf("%w: priority must fit a non-negative Postgres integer", ErrInvalidInput)
	}
	if policy.Scope.ID == "" && policy.Scope.Type != model.GatePolicyScopeCluster {
		return model.AutomationPolicy{}, fmt.Errorf("%w: non-cluster automation scopes require an ID", ErrInvalidInput)
	}
	if len(policy.Rules) == 0 {
		return model.AutomationPolicy{}, fmt.Errorf("%w: at least one automation rule is required", ErrInvalidInput)
	}

	rules := make([]model.AutomationRule, 0, len(policy.Rules))
	seenRuleIDs := make(map[string]struct{}, len(policy.Rules))
	for _, rule := range policy.Rules {
		rule.ID = strings.TrimSpace(rule.ID)
		rule.Description = strings.TrimSpace(rule.Description)
		rule.Trigger.Type = strings.TrimSpace(strings.ToLower(rule.Trigger.Type))
		rule.Trigger.Source = strings.TrimSpace(rule.Trigger.Source)
		rule.Trigger.InvariantID = strings.TrimSpace(rule.Trigger.InvariantID)
		rule.Trigger.RequiredEvidence = normalizeAutomationStrings(rule.Trigger.RequiredEvidence)
		rule.Action.Type = strings.TrimSpace(rule.Action.Type)
		rule.Safety.ActionContractID = strings.TrimSpace(rule.Safety.ActionContractID)
		rule.Safety.GatePolicyID = strings.TrimSpace(rule.Safety.GatePolicyID)
		rule.Safety.TTL = strings.TrimSpace(rule.Safety.TTL)
		rule.Safety.RecoveryCondition = strings.TrimSpace(rule.Safety.RecoveryCondition)
		rule.Safety.RollbackAction = strings.TrimSpace(rule.Safety.RollbackAction)
		if rule.ID == "" || rule.Trigger.Type == "" || rule.Trigger.Source == "" ||
			rule.Action.Type == "" || rule.Safety.ActionContractID == "" ||
			rule.Safety.GatePolicyID == "" || rule.Safety.TTL == "" ||
			rule.Safety.RecoveryCondition == "" || rule.Safety.RollbackAction == "" {
			return model.AutomationPolicy{}, fmt.Errorf("%w: rule ID, trigger, action, and safety contract fields are required", ErrInvalidInput)
		}
		if !validPersistedAutomationTriggerType(rule.Trigger.Type) {
			return model.AutomationPolicy{}, fmt.Errorf("%w: unsupported automation trigger type %q", ErrInvalidInput, rule.Trigger.Type)
		}
		if rule.Trigger.Type == model.AutomationTriggerInvariant && rule.Trigger.InvariantID == "" {
			return model.AutomationPolicy{}, fmt.Errorf("%w: invariant triggers require an invariant ID", ErrInvalidInput)
		}
		if _, exists := seenRuleIDs[rule.ID]; exists {
			return model.AutomationPolicy{}, fmt.Errorf("%w: duplicate rule ID %q", ErrInvalidInput, rule.ID)
		}
		seenRuleIDs[rule.ID] = struct{}{}
		if rule.Trigger.MinimumSamples < 0 || rule.Trigger.MinimumFailureDomains < 0 {
			return model.AutomationPolicy{}, fmt.Errorf("%w: trigger sample and failure-domain limits must not be negative", ErrInvalidInput)
		}
		if rule.Safety.BlastRadius.MaxNodes < 0 ||
			rule.Safety.BlastRadius.MaxEdgesPerGroup < 0 ||
			rule.Safety.BlastRadius.PreserveMinHealthyEdgeGroups < 0 ||
			rule.Safety.BlastRadius.PreserveMinEligibleEdgesPerHost < 0 {
			return model.AutomationPolicy{}, fmt.Errorf("%w: blast-radius limits must not be negative", ErrInvalidInput)
		}
		var err error
		rule.Action.Parameters, err = normalizeAutomationMap(rule.Action.Parameters)
		if err != nil {
			return model.AutomationPolicy{}, err
		}
		rules = append(rules, rule)
	}
	policy.Rules = rules
	var err error
	policy.Metadata, err = normalizeAutomationMap(policy.Metadata)
	if err != nil {
		return model.AutomationPolicy{}, err
	}
	return policy, nil
}

func normalizePersistedAutomationPolicy(policy model.AutomationPolicy) (model.AutomationPolicy, error) {
	normalized, err := normalizeAutomationPolicyForStore(policy)
	if err != nil {
		return model.AutomationPolicy{}, fmt.Errorf("invalid persisted automation policy %q: %w", policy.ID, err)
	}
	if normalized.ID == "" || normalized.Generation <= 0 ||
		normalized.CreatedAt.IsZero() || normalized.UpdatedAt.IsZero() {
		return model.AutomationPolicy{}, fmt.Errorf(
			"%w: persisted automation policy %q has an invalid ID, generation, or timestamp",
			ErrInvalidInput,
			policy.ID,
		)
	}
	normalized.CreatedAt = normalized.CreatedAt.UTC()
	normalized.UpdatedAt = normalized.UpdatedAt.UTC()
	return normalized, nil
}

func validateAutomationPolicyParentsState(state *model.State, policy model.AutomationPolicy) error {
	if state == nil || findTenant(state, policy.TenantID) < 0 {
		return ErrNotFound
	}
	if policy.ProjectID == "" {
		if policy.Scope.Type == model.AutomationScopeApp {
			return fmt.Errorf("%w: app-scoped automation policies require a project", ErrInvalidInput)
		}
		return nil
	}
	projectIndex := findProject(state, policy.ProjectID)
	if projectIndex < 0 || state.Projects[projectIndex].TenantID != policy.TenantID {
		return ErrNotFound
	}
	if projectDeleteRequested(state, policy.ProjectID) {
		return ErrConflict
	}
	if policy.Scope.Type == model.AutomationScopeApp {
		appIndex := findApp(state, policy.Scope.ID)
		if appIndex < 0 ||
			isDeletedApp(state.Apps[appIndex]) ||
			strings.EqualFold(strings.TrimSpace(state.Apps[appIndex].Status.Phase), "deleting") ||
			state.Apps[appIndex].TenantID != policy.TenantID ||
			state.Apps[appIndex].ProjectID != policy.ProjectID {
			return ErrNotFound
		}
	}
	return nil
}

func automationPolicyVisible(policy model.AutomationPolicy, filter AutomationPolicyFilter) bool {
	if policy.OwnerType != model.AutomationOwnerUser || policy.Managed {
		return false
	}
	if filter.PlatformAdmin {
		if filter.TenantID != "" && policy.TenantID != filter.TenantID {
			return false
		}
	} else if policy.TenantID != filter.TenantID {
		return false
	}
	return filter.ProjectID == "" || policy.ProjectID == filter.ProjectID
}

func automationPolicyAccessible(policy model.AutomationPolicy, tenantID string, platformAdmin bool) bool {
	if policy.OwnerType != model.AutomationOwnerUser || policy.Managed {
		return false
	}
	return platformAdmin || policy.TenantID == tenantID
}

func automationPolicyNameConflict(a, b model.AutomationPolicy) bool {
	return a.TenantID == b.TenantID &&
		a.ProjectID == b.ProjectID &&
		strings.EqualFold(strings.TrimSpace(a.Name), strings.TrimSpace(b.Name))
}

func findAutomationPolicyByID(policies []model.AutomationPolicy, id string) int {
	for index, policy := range policies {
		if policy.ID == id {
			return index
		}
	}
	return -1
}

func sortAutomationPolicies(policies []model.AutomationPolicy) {
	sort.Slice(policies, func(i, j int) bool {
		if policies[i].TenantID != policies[j].TenantID {
			return policies[i].TenantID < policies[j].TenantID
		}
		if policies[i].ProjectID != policies[j].ProjectID {
			return policies[i].ProjectID < policies[j].ProjectID
		}
		if policies[i].Priority != policies[j].Priority {
			return policies[i].Priority > policies[j].Priority
		}
		if strings.ToLower(policies[i].Name) != strings.ToLower(policies[j].Name) {
			return strings.ToLower(policies[i].Name) < strings.ToLower(policies[j].Name)
		}
		return policies[i].ID < policies[j].ID
	})
}

func normalizeAutomationStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func normalizeAutomationMap(values map[string]string) (map[string]string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, fmt.Errorf("%w: automation map keys must not be empty", ErrInvalidInput)
		}
		if _, exists := out[key]; exists {
			return nil, fmt.Errorf("%w: duplicate automation map key %q after normalization", ErrInvalidInput, key)
		}
		out[key] = strings.TrimSpace(value)
	}
	return out, nil
}

func validPersistedAutomationTriggerType(triggerType string) bool {
	switch triggerType {
	case model.AutomationTriggerInvariant,
		model.AutomationTriggerRequestMetric,
		model.AutomationTriggerSyntheticProbe,
		model.AutomationTriggerEvent,
		model.AutomationTriggerSchedule:
		return true
	default:
		return false
	}
}

func cloneAutomationPolicy(policy model.AutomationPolicy) model.AutomationPolicy {
	policy.Rules = append([]model.AutomationRule(nil), policy.Rules...)
	for index := range policy.Rules {
		policy.Rules[index].Trigger.RequiredEvidence = append([]string(nil), policy.Rules[index].Trigger.RequiredEvidence...)
		policy.Rules[index].Action.Parameters = cloneAutomationMap(policy.Rules[index].Action.Parameters)
	}
	policy.Metadata = cloneAutomationMap(policy.Metadata)
	return policy
}

func cloneAutomationMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

func deleteAutomationPoliciesByTenant(policies []model.AutomationPolicy, tenantID string) []model.AutomationPolicy {
	filtered := policies[:0]
	for _, policy := range policies {
		if policy.TenantID != tenantID {
			filtered = append(filtered, policy)
		}
	}
	return filtered
}

func deleteAutomationPoliciesByProject(policies []model.AutomationPolicy, projectID string) []model.AutomationPolicy {
	filtered := policies[:0]
	for _, policy := range policies {
		if policy.ProjectID != projectID {
			filtered = append(filtered, policy)
		}
	}
	return filtered
}

func deleteAutomationPoliciesByAppScope(policies []model.AutomationPolicy, appID string) []model.AutomationPolicy {
	appID = strings.TrimSpace(appID)
	filtered := policies[:0]
	for _, policy := range policies {
		if strings.EqualFold(strings.TrimSpace(policy.Scope.Type), model.AutomationScopeApp) &&
			strings.TrimSpace(policy.Scope.ID) == appID {
			continue
		}
		filtered = append(filtered, policy)
	}
	return filtered
}
