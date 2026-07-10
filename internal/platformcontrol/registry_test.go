package platformcontrol

import (
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformsafety"
)

func TestInvariantRegistryCoversSafetyKernelAndActionBindings(t *testing.T) {
	t.Parallel()

	definitions := InvariantDefinitions()
	if len(definitions) < 30 {
		t.Fatalf("expected a broad invariant registry, got %d definitions", len(definitions))
	}
	byID := map[string]model.InvariantDefinition{}
	contracts := map[string]struct{}{}
	for _, contract := range AutomaticActionContracts() {
		contracts[contract.ID] = struct{}{}
	}
	for _, definition := range definitions {
		if strings.TrimSpace(definition.ID) == "" ||
			strings.TrimSpace(definition.Category) == "" ||
			strings.TrimSpace(definition.Scope) == "" ||
			strings.TrimSpace(definition.Owner) == "" ||
			strings.TrimSpace(definition.Description) == "" ||
			strings.TrimSpace(definition.Severity) == "" ||
			strings.TrimSpace(definition.DefaultMode) == "" ||
			strings.TrimSpace(definition.RunbookRef) == "" ||
			strings.TrimSpace(definition.UnknownBehavior) == "" ||
			strings.TrimSpace(definition.StaleBehavior) == "" {
			t.Fatalf("invariant definition is incomplete: %+v", definition)
		}
		if _, duplicate := byID[definition.ID]; duplicate {
			t.Fatalf("duplicate invariant id %q", definition.ID)
		}
		byID[definition.ID] = definition
		if definition.AutomaticActionContractID != "" {
			if _, ok := contracts[definition.AutomaticActionContractID]; !ok {
				t.Fatalf("invariant %s references unknown action contract %s", definition.ID, definition.AutomaticActionContractID)
			}
		}
	}
	for _, invariantID := range platformsafety.ImmutableInvariantIDs() {
		definition, ok := byID[invariantID]
		if !ok {
			t.Fatalf("platform safety invariant %s is missing from the registry", invariantID)
		}
		if definition.DefaultMode != model.GatePolicyModeEnforced {
			t.Fatalf("platform safety invariant %s must be enforced, got %s", invariantID, definition.DefaultMode)
		}
	}
	for _, invariantID := range []string{
		"node.pod_dns_to_kube_dns_service",
		"node.pod_dns_to_coredns_pod",
		"node.kubernetes_default_dns",
		"node.same_namespace_service_dns_tcp",
		"node.managed_iptables_provenance",
		"node.podcidr_matches_kubernetes",
		"node.conntrack_safe",
		"runtime.no_quarantined_node_placement",
		"runtime.stateless_replacement_ready_before_route",
		"runtime.stateful_fence_before_failover",
		"dns.answer_policy_generation_visible",
		"release.no_block_rollout_incident",
		"release.artifact_shadow_validated",
		"request.error_class_attributed",
		"platform_state.lkg_hash_and_expiry_checked",
	} {
		if _, ok := byID[invariantID]; !ok {
			t.Fatalf("compatibility invariant %s is missing from the registry", invariantID)
		}
	}
}

func TestAutomaticActionContractsAreBoundedAndAuditable(t *testing.T) {
	t.Parallel()

	contracts := AutomaticActionContracts()
	if len(contracts) < 10 {
		t.Fatalf("expected all platform automatic actions to have contracts, got %d", len(contracts))
	}
	seen := map[string]struct{}{}
	for _, contract := range contracts {
		if _, duplicate := seen[contract.ID]; duplicate {
			t.Fatalf("duplicate action contract id %q", contract.ID)
		}
		seen[contract.ID] = struct{}{}
		if strings.TrimSpace(contract.ActionType) == "" ||
			strings.TrimSpace(contract.Scope) == "" ||
			strings.TrimSpace(contract.TriggerInvariant) == "" ||
			strings.TrimSpace(contract.GatePolicyID) == "" ||
			strings.TrimSpace(contract.TTL) == "" ||
			strings.TrimSpace(contract.RecoveryCondition) == "" ||
			strings.TrimSpace(contract.RollbackAction) == "" ||
			strings.TrimSpace(contract.EnableEnv) == "" ||
			strings.TrimSpace(contract.KillSwitchEnv) == "" ||
			strings.TrimSpace(contract.RunbookRef) == "" {
			t.Fatalf("action contract is incomplete: %+v", contract)
		}
		if !contract.RequiresAudit || !contract.RequiresIdempotencyKey {
			t.Fatalf("action contract %s must require audit and idempotency", contract.ID)
		}
		if len(contract.AllowedModes) == 0 || stringInSlice(model.GatePolicyModeDisabled, contract.AllowedModes) {
			t.Fatalf("action contract %s has unsafe allowed modes: %+v", contract.ID, contract.AllowedModes)
		}
	}
}

func TestConsumerAndLKGInventoriesCoverPlatformArtifactKinds(t *testing.T) {
	t.Parallel()

	consumers := ConsumerContracts()
	if len(consumers) != 5 {
		t.Fatalf("expected five platform consumer contracts, got %d", len(consumers))
	}
	for _, contract := range consumers {
		if !contract.Required ||
			!contract.LoadLKGFirst ||
			!contract.AtomicApply ||
			!contract.LocalProbe ||
			!contract.HeartbeatGeneration ||
			strings.TrimSpace(contract.IdentityKind) == "" ||
			strings.TrimSpace(contract.ExpectedConsumerSource) == "" {
			t.Fatalf("consumer contract is incomplete: %+v", contract)
		}
	}
	policyKinds := map[string]struct{}{}
	for _, policy := range LKGPolicies() {
		policyKinds[policy.ArtifactKind] = struct{}{}
		if strings.TrimSpace(policy.StorageLocation) == "" {
			t.Fatalf("LKG policy %s must declare its storage location", policy.ArtifactKind)
		}
		if policy.MinimumGenerations < 3 {
			t.Fatalf("LKG policy %s must retain at least three generations", policy.ArtifactKind)
		}
	}
	for _, kind := range []string{
		model.PlatformArtifactKindEdgeRouteBundle,
		model.PlatformArtifactKindDNSAnswerBundle,
		model.PlatformArtifactKindCaddyRouteConfig,
		model.PlatformArtifactKindDiscoveryBundle,
		model.PlatformArtifactKindNodeDesiredState,
		model.PlatformArtifactKindRuntimePlacementPlan,
		model.PlatformArtifactKindRuntimeContinuityPlan,
		model.PlatformArtifactKindNodeGuardianPolicy,
		model.PlatformArtifactKindReleaseGuardPolicy,
		model.PlatformArtifactKindEdgeRankingPolicy,
		model.PlatformArtifactKindTrafficSafetyPolicy,
		model.PlatformArtifactKindSubsystemFailureContracts,
		model.PlatformArtifactKindGatePolicyRegistry,
		model.PlatformArtifactKindAutomaticActionContracts,
	} {
		if _, ok := policyKinds[kind]; !ok {
			t.Fatalf("artifact kind %s has no LKG policy", kind)
		}
	}
}
