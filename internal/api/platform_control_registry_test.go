package api

import (
	"net/http"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestPlatformControlRegistryRequiresPlatformAdmin(t *testing.T) {
	t.Parallel()

	_, server, tenantKey, _, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	for _, test := range []struct {
		method string
		path   string
		body   any
	}{
		{method: http.MethodGet, path: "/v1/admin/invariants"},
		{method: http.MethodGet, path: "/v1/admin/invariants/inventory"},
		{method: http.MethodGet, path: "/v1/admin/action-contracts"},
		{method: http.MethodPost, path: "/v1/admin/action-safety/evaluate", body: model.ActionSafetyRequest{}},
	} {
		recorder := performJSONRequest(t, server, test.method, test.path, tenantKey, test.body)
		if recorder.Code != http.StatusForbidden {
			t.Fatalf("%s %s: expected status %d, got %d body=%s", test.method, test.path, http.StatusForbidden, recorder.Code, recorder.Body.String())
		}
	}
}

func TestPlatformControlRegistryExposesInvariantActionAndInventory(t *testing.T) {
	t.Parallel()

	_, server, _, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")

	list := performJSONRequest(t, server, http.MethodGet, "/v1/admin/invariants", platformAdminKey, nil)
	if list.Code != http.StatusOK {
		t.Fatalf("list invariants: expected status %d, got %d body=%s", http.StatusOK, list.Code, list.Body.String())
	}
	var invariantList model.InvariantDefinitionListResponse
	mustDecodeJSON(t, list, &invariantList)
	if len(invariantList.Invariants) < 30 || invariantList.GeneratedAt.IsZero() {
		t.Fatalf("unexpected invariant registry response: %+v", invariantList)
	}

	show := performJSONRequest(t, server, http.MethodGet, "/v1/admin/invariants/release.no_new_block_publish", platformAdminKey, nil)
	if show.Code != http.StatusOK {
		t.Fatalf("show invariant: expected status %d, got %d body=%s", http.StatusOK, show.Code, show.Body.String())
	}
	var invariantResponse model.InvariantDefinitionResponse
	mustDecodeJSON(t, show, &invariantResponse)
	if invariantResponse.Invariant.ID != "release.no_new_block_publish" ||
		invariantResponse.Invariant.GatePolicyID == "" ||
		invariantResponse.Invariant.RunbookRef == "" {
		t.Fatalf("invariant binding is incomplete: %+v", invariantResponse.Invariant)
	}

	contracts := performJSONRequest(t, server, http.MethodGet, "/v1/admin/action-contracts", platformAdminKey, nil)
	if contracts.Code != http.StatusOK {
		t.Fatalf("list action contracts: expected status %d, got %d body=%s", http.StatusOK, contracts.Code, contracts.Body.String())
	}
	var contractList model.AutomaticActionContractListResponse
	mustDecodeJSON(t, contracts, &contractList)
	if len(contractList.Contracts) < 10 || contractList.GeneratedAt.IsZero() {
		t.Fatalf("unexpected action contract response: %+v", contractList)
	}

	inventory := performJSONRequest(t, server, http.MethodGet, "/v1/admin/invariants/inventory", platformAdminKey, nil)
	if inventory.Code != http.StatusOK {
		t.Fatalf("get inventory: expected status %d, got %d body=%s", http.StatusOK, inventory.Code, inventory.Body.String())
	}
	var inventoryResponse model.PlatformControlInventoryResponse
	mustDecodeJSON(t, inventory, &inventoryResponse)
	got := inventoryResponse.Inventory
	if got.GeneratedAt.IsZero() ||
		len(got.ArtifactKinds) == 0 ||
		len(got.Consumers) == 0 ||
		len(got.GatePolicies) == 0 ||
		len(got.AutomaticActions) == 0 ||
		len(got.SyntheticProbes) == 0 ||
		len(got.LKGPolicies) == 0 ||
		len(got.Mechanisms) == 0 {
		t.Fatalf("platform control inventory is incomplete: %+v", got)
	}
	if !hasPlatformControlMechanism(got.Mechanisms, "action_safety_evaluator", model.PlatformControlMechanismShadow) {
		t.Fatalf("action safety evaluator must be explicitly reported as shadow: %+v", got.Mechanisms)
	}
}

func TestActionSafetyEndpointIsEvaluationOnlyAndFailsClosed(t *testing.T) {
	t.Parallel()

	_, server, _, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	now := time.Now().UTC()
	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/admin/action-safety/evaluate", platformAdminKey, model.ActionSafetyRequest{
		ActionType:       "dns_answer_filter",
		ContractID:       "dns.answer_filter",
		TriggerInvariant: "dns.answer_route_ready",
		Scope:            model.GatePolicyScopeHostname,
		Subject:          "api.example.test",
		Evidence: []model.ActionSafetyEvidence{{
			ID:            "dns.answer_route_ready",
			State:         model.InvariantEvidenceStatePass,
			Source:        "synthetic-a",
			FailureDomain: "provider-a",
			ObservedAt:    now,
		}},
		CurrentCounts:    map[string]int{"api.example.test": 2},
		CandidateCounts:  map[string]int{"api.example.test": 1},
		FailureDomains:   []string{"provider-a", "provider-b"},
		SampleCount:      3,
		TTL:              "1m",
		IdempotencyKey:   "test-evaluation",
		AuditReady:       true,
		CanaryScopeMatch: true,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("evaluate action safety: expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response model.ActionSafetyDecisionResponse
	mustDecodeJSON(t, recorder, &response)
	if response.Decision.ProductionMutationAllowed || response.Decision.Allowed {
		t.Fatalf("read-only evaluation must not allow a disabled action: %+v", response.Decision)
	}
	if response.Decision.EffectiveMode != model.GatePolicyModeDisabled {
		t.Fatalf("missing action enable switch must fail closed, got %+v", response.Decision)
	}
}

func hasPlatformControlMechanism(mechanisms []model.PlatformControlMechanism, id, status string) bool {
	for _, mechanism := range mechanisms {
		if mechanism.ID == id && mechanism.Status == status {
			return true
		}
	}
	return false
}
