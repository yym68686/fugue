package automation

import (
	"fmt"
	"sort"
	"strings"

	"fugue/internal/model"
)

const managedPolicyPrefix = "system."

// BuildManagedPolicies projects the existing compiled action contracts and
// effective gate policies into the unified AutomationPolicy representation.
// It deliberately has no execution side effects.
func BuildManagedPolicies(
	contracts []model.AutomaticActionContract,
	gatePolicies []model.GatePolicy,
) ([]model.AutomationPolicy, error) {
	gates := make(map[string]model.GatePolicy, len(gatePolicies))
	for _, gate := range gatePolicies {
		id := strings.TrimSpace(gate.ID)
		if id == "" {
			return nil, fmt.Errorf("automation registry: gate policy ID is required")
		}
		if _, exists := gates[id]; exists {
			return nil, fmt.Errorf("automation registry: duplicate gate policy %q", id)
		}
		gate.ID = id
		gates[id] = gate
	}

	seenContracts := make(map[string]struct{}, len(contracts))
	policies := make([]model.AutomationPolicy, 0, len(contracts))
	for _, contract := range contracts {
		contractID := strings.TrimSpace(contract.ID)
		if contractID == "" {
			return nil, fmt.Errorf("automation registry: action contract ID is required")
		}
		if _, exists := seenContracts[contractID]; exists {
			return nil, fmt.Errorf("automation registry: duplicate action contract %q", contractID)
		}
		seenContracts[contractID] = struct{}{}

		actionType := strings.TrimSpace(contract.ActionType)
		if actionType == "" {
			return nil, fmt.Errorf("automation registry: action contract %q has no action type", contractID)
		}
		scope := strings.TrimSpace(contract.Scope)
		if scope == "" {
			return nil, fmt.Errorf("automation registry: action contract %q has no scope", contractID)
		}
		triggerInvariant := strings.TrimSpace(contract.TriggerInvariant)
		if triggerInvariant == "" {
			return nil, fmt.Errorf("automation registry: action contract %q has no trigger invariant", contractID)
		}
		evidenceSource := strings.TrimSpace(contract.EvidenceSource)
		if evidenceSource == "" {
			return nil, fmt.Errorf("automation registry: action contract %q has no evidence source", contractID)
		}
		gatePolicyID := strings.TrimSpace(contract.GatePolicyID)
		gate, ok := gates[gatePolicyID]
		if !ok {
			return nil, fmt.Errorf(
				"automation registry: action contract %q references unknown gate policy %q",
				contractID,
				gatePolicyID,
			)
		}
		mode := strings.TrimSpace(gate.Mode)
		if !validAutomationMode(mode) {
			return nil, fmt.Errorf(
				"automation registry: gate policy %q has invalid mode %q",
				gatePolicyID,
				gate.Mode,
			)
		}
		gateScope := strings.TrimSpace(gate.Scope)
		if gateScope == "" {
			return nil, fmt.Errorf("automation registry: gate policy %q has no scope", gatePolicyID)
		}
		ttl := strings.TrimSpace(contract.TTL)
		if ttl == "" {
			return nil, fmt.Errorf("automation registry: action contract %q has no TTL", contractID)
		}
		recoveryCondition := strings.TrimSpace(contract.RecoveryCondition)
		if recoveryCondition == "" {
			return nil, fmt.Errorf("automation registry: action contract %q has no recovery condition", contractID)
		}
		rollbackAction := strings.TrimSpace(contract.RollbackAction)
		if rollbackAction == "" {
			return nil, fmt.Errorf("automation registry: action contract %q has no rollback action", contractID)
		}
		createdAt := gate.IntroducedAt
		updatedAt := gate.UpdatedAt
		// A managed policy has no separate persistence record.  For a policy
		// that has never been promoted, the introduction timestamp is also the
		// last-known update timestamp; emitting Go's zero time would violate
		// the API contract and make an otherwise stable policy look corrupt.
		if updatedAt.IsZero() {
			updatedAt = createdAt
		}

		policies = append(policies, model.AutomationPolicy{
			ID:          managedPolicyPrefix + contractID,
			Name:        contractID,
			Description: strings.TrimSpace(gate.Description),
			Kind:        model.AutomationPolicyKindManagedSystem,
			OwnerType:   model.AutomationOwnerSystem,
			Scope:       model.AutomationScope{Type: scope},
			Mode:        mode,
			Managed:     true,
			SourceRef:   "automatic-action-contract:" + contractID,
			Rules: []model.AutomationRule{{
				ID:          "default",
				Description: strings.TrimSpace(gate.Description),
				Trigger: model.AutomationTrigger{
					Type:                  model.AutomationTriggerInvariant,
					Source:                evidenceSource,
					InvariantID:           triggerInvariant,
					RequiredEvidence:      cloneStrings(contract.RequiredEvidence),
					MinimumSamples:        contract.MinimumSamples,
					MinimumFailureDomains: contract.MinimumFailureDomains,
				},
				Action: model.AutomationAction{
					Type:       actionType,
					Parameters: cloneStringMap(contract.Metadata),
				},
				Safety: model.AutomationSafetyPolicy{
					ActionContractID:       contractID,
					GatePolicyID:           gatePolicyID,
					TTL:                    ttl,
					BlastRadius:            contract.BlastRadius,
					RecoveryCondition:      recoveryCondition,
					RollbackAction:         rollbackAction,
					RequiresRollbackTarget: contract.RequiresRollbackTarget,
					RequiresAudit:          contract.RequiresAudit,
					RequiresWAL:            contract.RequiresWAL,
					RequiresIdempotencyKey: contract.RequiresIdempotencyKey,
					RequiresFencingToken:   contract.RequiresFencingToken,
				},
			}},
			Generation: 1,
			Metadata: map[string]string{
				"gate_policy_scope": gateScope,
				"runbook_ref":       strings.TrimSpace(contract.RunbookRef),
			},
			CreatedAt: createdAt,
			UpdatedAt: updatedAt,
		})
	}

	sort.SliceStable(policies, func(i, j int) bool {
		return policies[i].ID < policies[j].ID
	})
	return policies, nil
}

func validAutomationMode(mode string) bool {
	switch strings.TrimSpace(mode) {
	case model.GatePolicyModeDisabled,
		model.GatePolicyModeShadow,
		model.GatePolicyModeCanary,
		model.GatePolicyModeEnforced:
		return true
	default:
		return false
	}
}

func cloneStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	return append([]string(nil), values...)
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}
