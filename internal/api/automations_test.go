package api

import (
	"net/http"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

func TestAutomationInventoryRequiresPlatformAdmin(t *testing.T) {
	t.Parallel()

	_, server, tenantKey, _, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	for _, path := range []string{
		"/v1/admin/automations",
		"/v1/admin/automations/system.dns.answer_filter",
	} {
		recorder := performJSONRequest(t, server, http.MethodGet, path, tenantKey, nil)
		if recorder.Code != http.StatusForbidden {
			t.Fatalf("GET %s: expected status %d, got %d body=%s", path, http.StatusForbidden, recorder.Code, recorder.Body.String())
		}
	}
}

func TestAutomationInventoryProjectsManagedPolicies(t *testing.T) {
	t.Parallel()

	_, server, _, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/admin/automations", platformAdminKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("list automations: expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response model.AutomationPolicyListResponse
	mustDecodeJSON(t, recorder, &response)
	contracts := platformcontrol.AutomaticActionContracts()
	if len(response.Policies) != len(contracts) || response.GeneratedAt.IsZero() {
		t.Fatalf("unexpected automation inventory: policies=%d contracts=%d generated_at=%v", len(response.Policies), len(contracts), response.GeneratedAt)
	}
	for index, policy := range response.Policies {
		if index > 0 && response.Policies[index-1].ID >= policy.ID {
			t.Fatalf("automation inventory is not sorted: %q before %q", response.Policies[index-1].ID, policy.ID)
		}
		if !strings.HasPrefix(policy.ID, "system.") ||
			policy.OwnerType != model.AutomationOwnerSystem ||
			policy.Kind != model.AutomationPolicyKindManagedSystem ||
			!policy.Managed ||
			policy.Generation != 1 ||
			len(policy.Rules) != 1 {
			t.Fatalf("incomplete managed automation policy: %+v", policy)
		}
		if policy.Rules[0].Safety.ActionContractID == "" ||
			policy.Rules[0].Safety.GatePolicyID == "" ||
			policy.Rules[0].Trigger.Source == "" ||
			policy.Rules[0].Action.Type == "" {
			t.Fatalf("incomplete managed automation rule: %+v", policy.Rules[0])
		}
	}

	policyID := "system." + platformcontrol.ActionContractDNSAnswerFilter
	show := performJSONRequest(t, server, http.MethodGet, "/v1/admin/automations/"+policyID, platformAdminKey, nil)
	if show.Code != http.StatusOK {
		t.Fatalf("show automation: expected status %d, got %d body=%s", http.StatusOK, show.Code, show.Body.String())
	}
	var policyResponse model.AutomationPolicyResponse
	mustDecodeJSON(t, show, &policyResponse)
	if policyResponse.Policy.ID != policyID ||
		policyResponse.Policy.Mode == "" ||
		policyResponse.Policy.Rules[0].Action.Type != "dns_answer_filter" {
		t.Fatalf("unexpected automation policy response: %+v", policyResponse.Policy)
	}

	missing := performJSONRequest(t, server, http.MethodGet, "/v1/admin/automations/system.missing", platformAdminKey, nil)
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing automation: expected status %d, got %d body=%s", http.StatusNotFound, missing.Code, missing.Body.String())
	}
}
