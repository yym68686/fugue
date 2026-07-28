package automation

import (
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

func TestBuildManagedPoliciesProjectsEveryCompiledActionContract(t *testing.T) {
	contracts := platformcontrol.AutomaticActionContracts()
	gates := gatePoliciesForContracts(contracts)

	policies, err := BuildManagedPolicies(contracts, gates)
	if err != nil {
		t.Fatalf("build managed policies: %v", err)
	}
	if len(policies) != len(contracts) {
		t.Fatalf("expected %d policies, got %d", len(contracts), len(policies))
	}

	byContract := make(map[string]model.AutomationPolicy, len(policies))
	for index, policy := range policies {
		if index > 0 && policies[index-1].ID >= policy.ID {
			t.Fatalf("policies are not sorted: %q then %q", policies[index-1].ID, policy.ID)
		}
		if !policy.Managed || policy.OwnerType != model.AutomationOwnerSystem {
			t.Fatalf("compiled policy must be system managed: %+v", policy)
		}
		if policy.Kind != model.AutomationPolicyKindManagedSystem || policy.Generation != 1 {
			t.Fatalf("compiled policy has invalid identity: %+v", policy)
		}
		if len(policy.Rules) != 1 {
			t.Fatalf("compiled policy %q must contain one compatibility rule", policy.ID)
		}
		rule := policy.Rules[0]
		if rule.Safety.ActionContractID == "" || rule.Safety.GatePolicyID == "" {
			t.Fatalf("compiled policy %q lost safety references", policy.ID)
		}
		byContract[rule.Safety.ActionContractID] = policy
	}

	for _, contract := range contracts {
		policy, ok := byContract[contract.ID]
		if !ok {
			t.Fatalf("missing policy for action contract %q", contract.ID)
		}
		rule := policy.Rules[0]
		if rule.Action.Type != contract.ActionType {
			t.Fatalf("contract %q action changed from %q to %q", contract.ID, contract.ActionType, rule.Action.Type)
		}
		if rule.Trigger.InvariantID != contract.TriggerInvariant {
			t.Fatalf(
				"contract %q trigger changed from %q to %q",
				contract.ID,
				contract.TriggerInvariant,
				rule.Trigger.InvariantID,
			)
		}
	}
}

func TestBuildManagedPoliciesReturnsDetachedCollections(t *testing.T) {
	contract := managedContractFixture()
	contract.RequiredEvidence = []string{"safe"}
	contract.Metadata = map[string]string{"executor": "domain"}
	gate := managedGateFixture()

	policies, err := BuildManagedPolicies(
		[]model.AutomaticActionContract{contract},
		[]model.GatePolicy{gate},
	)
	if err != nil {
		t.Fatalf("build managed policies: %v", err)
	}
	contract.RequiredEvidence[0] = "mutated"
	contract.Metadata["executor"] = "mutated"

	rule := policies[0].Rules[0]
	if rule.Trigger.RequiredEvidence[0] != "safe" {
		t.Fatalf("required evidence aliases caller input: %+v", rule.Trigger.RequiredEvidence)
	}
	if rule.Action.Parameters["executor"] != "domain" {
		t.Fatalf("action parameters alias caller input: %+v", rule.Action.Parameters)
	}
}

func TestBuildManagedPoliciesFailsClosedForInvalidRegistry(t *testing.T) {
	tests := []struct {
		name      string
		contracts []model.AutomaticActionContract
		gates     []model.GatePolicy
		want      string
	}{
		{
			name:      "duplicate contract",
			contracts: []model.AutomaticActionContract{managedContractFixture(), managedContractFixture()},
			gates:     []model.GatePolicy{managedGateFixture()},
			want:      "duplicate action contract",
		},
		{
			name:      "missing gate",
			contracts: []model.AutomaticActionContract{managedContractFixture()},
			want:      "unknown gate policy",
		},
		{
			name:      "duplicate gate",
			contracts: []model.AutomaticActionContract{managedContractFixture()},
			gates:     []model.GatePolicy{managedGateFixture(), managedGateFixture()},
			want:      "duplicate gate policy",
		},
		{
			name:      "invalid mode",
			contracts: []model.AutomaticActionContract{managedContractFixture()},
			gates: []model.GatePolicy{func() model.GatePolicy {
				gate := managedGateFixture()
				gate.Mode = "unsafe"
				return gate
			}()},
			want: "invalid mode",
		},
		{
			name: "missing action type",
			contracts: []model.AutomaticActionContract{func() model.AutomaticActionContract {
				contract := managedContractFixture()
				contract.ActionType = ""
				return contract
			}()},
			gates: []model.GatePolicy{managedGateFixture()},
			want:  "no action type",
		},
		{
			name: "missing trigger",
			contracts: []model.AutomaticActionContract{func() model.AutomaticActionContract {
				contract := managedContractFixture()
				contract.TriggerInvariant = ""
				return contract
			}()},
			gates: []model.GatePolicy{managedGateFixture()},
			want:  "no trigger invariant",
		},
		{
			name: "missing TTL",
			contracts: []model.AutomaticActionContract{func() model.AutomaticActionContract {
				contract := managedContractFixture()
				contract.TTL = ""
				return contract
			}()},
			gates: []model.GatePolicy{managedGateFixture()},
			want:  "no TTL",
		},
		{
			name: "missing recovery condition",
			contracts: []model.AutomaticActionContract{func() model.AutomaticActionContract {
				contract := managedContractFixture()
				contract.RecoveryCondition = ""
				return contract
			}()},
			gates: []model.GatePolicy{managedGateFixture()},
			want:  "no recovery condition",
		},
		{
			name: "missing rollback action",
			contracts: []model.AutomaticActionContract{func() model.AutomaticActionContract {
				contract := managedContractFixture()
				contract.RollbackAction = ""
				return contract
			}()},
			gates: []model.GatePolicy{managedGateFixture()},
			want:  "no rollback action",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := BuildManagedPolicies(test.contracts, test.gates)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("expected error containing %q, got %v", test.want, err)
			}
		})
	}
}

func gatePoliciesForContracts(contracts []model.AutomaticActionContract) []model.GatePolicy {
	seen := map[string]bool{}
	gates := make([]model.GatePolicy, 0, len(contracts))
	for _, contract := range contracts {
		if seen[contract.GatePolicyID] {
			continue
		}
		seen[contract.GatePolicyID] = true
		gates = append(gates, model.GatePolicy{
			ID:           contract.GatePolicyID,
			Description:  contract.ID,
			Mode:         model.GatePolicyModeShadow,
			Scope:        contract.Scope,
			IntroducedAt: time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC),
		})
	}
	return gates
}

func managedContractFixture() model.AutomaticActionContract {
	return model.AutomaticActionContract{
		ID:                     "test.restart",
		ActionType:             "restart_test",
		Scope:                  model.GatePolicyScopeNode,
		TriggerInvariant:       "test.safe",
		EvidenceSource:         "test.probe",
		RequiredEvidence:       []string{"test.safe"},
		GatePolicyID:           "test.restart_gate",
		TTL:                    "10m",
		MinimumSamples:         3,
		MinimumFailureDomains:  1,
		RecoveryCondition:      "test probe passes",
		RollbackAction:         "stop restart loop",
		RequiresAudit:          true,
		RequiresWAL:            true,
		RequiresIdempotencyKey: true,
	}
}

func managedGateFixture() model.GatePolicy {
	return model.GatePolicy{
		ID:           "test.restart_gate",
		Description:  "test restart gate",
		Mode:         model.GatePolicyModeShadow,
		Scope:        model.GatePolicyScopeNode,
		IntroducedAt: time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC),
	}
}
