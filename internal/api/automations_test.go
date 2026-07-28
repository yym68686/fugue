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
		if policy.CreatedAt.IsZero() || policy.UpdatedAt.IsZero() || policy.UpdatedAt.Before(policy.CreatedAt) {
			t.Fatalf("managed automation policy has invalid timestamps: %+v", policy)
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

func TestUserAutomationPolicyLifecycleDerivesSafetyAndAuditsMutations(t *testing.T) {
	t.Parallel()

	fixture := setupAutomationAPITestServer(t)
	create := performJSONRequest(
		t,
		fixture.server,
		http.MethodPost,
		"/v1/automations",
		fixture.writeKey,
		automationCreateRequest(fixture.app),
	)
	if create.Code != http.StatusCreated {
		t.Fatalf("create automation: expected status %d, got %d body=%s", http.StatusCreated, create.Code, create.Body.String())
	}
	var createdResponse model.AutomationPolicyResponse
	mustDecodeJSON(t, create, &createdResponse)
	created := createdResponse.Policy
	if created.TenantID != fixture.app.TenantID ||
		created.ProjectID != fixture.app.ProjectID ||
		created.OwnerType != model.AutomationOwnerUser ||
		created.Managed ||
		created.Kind != model.AutomationPolicyKindAppRecovery ||
		created.Scope.Type != model.AutomationScopeApp ||
		created.Scope.ID != fixture.app.ID ||
		created.Generation != 1 ||
		len(created.Rules) != 1 {
		t.Fatalf("unexpected created automation policy: %+v", created)
	}
	rule := created.Rules[0]
	if rule.Trigger.InvariantID != "app.request_unavailability" ||
		rule.Trigger.MinimumSamples != 3 ||
		rule.Trigger.MinimumFailureDomains != 1 ||
		rule.Trigger.RequestMetric == nil ||
		len(rule.Trigger.RequestMetric.StatusCodes) != 2 ||
		rule.Trigger.RequestMetric.StatusCodes[0] != 503 ||
		rule.Trigger.RequestMetric.StatusCodes[1] != 504 {
		t.Fatalf("server did not normalize and floor trigger safety: %+v", rule.Trigger)
	}
	if rule.Safety.ActionContractID != platformcontrol.ActionContractAppRestart ||
		rule.Safety.GatePolicyID != "automation.app-restart" ||
		rule.Safety.BlastRadius.MaxApps != 1 ||
		!rule.Safety.RequiresRollbackTarget ||
		!rule.Safety.RequiresAudit ||
		!rule.Safety.RequiresWAL ||
		!rule.Safety.RequiresIdempotencyKey ||
		!rule.Safety.RequiresFencingToken {
		t.Fatalf("server did not derive the registered action safety contract: %+v", rule.Safety)
	}

	for _, key := range []string{fixture.readKey, fixture.writeKey} {
		list := performJSONRequest(t, fixture.server, http.MethodGet, "/v1/automations", key, nil)
		if list.Code != http.StatusOK {
			t.Fatalf("list automation with authorized key: status=%d body=%s", list.Code, list.Body.String())
		}
		var response model.AutomationPolicyListResponse
		mustDecodeJSON(t, list, &response)
		if len(response.Policies) != 1 || response.Policies[0].ID != created.ID {
			t.Fatalf("unexpected tenant automation list: %+v", response.Policies)
		}
	}
	show := performJSONRequest(t, fixture.server, http.MethodGet, "/v1/automations/"+created.ID, fixture.readKey, nil)
	if show.Code != http.StatusOK {
		t.Fatalf("show automation: status=%d body=%s", show.Code, show.Body.String())
	}

	adminUsers := performJSONRequest(t, fixture.server, http.MethodGet, "/v1/admin/automations?owner_type=user", fixture.platformAdminKey, nil)
	if adminUsers.Code != http.StatusOK {
		t.Fatalf("admin user inventory: status=%d body=%s", adminUsers.Code, adminUsers.Body.String())
	}
	var adminUserResponse model.AutomationPolicyListResponse
	mustDecodeJSON(t, adminUsers, &adminUserResponse)
	if len(adminUserResponse.Policies) != 1 || adminUserResponse.Policies[0].ID != created.ID {
		t.Fatalf("admin inventory did not include persisted user policy: %+v", adminUserResponse.Policies)
	}
	adminShow := performJSONRequest(t, fixture.server, http.MethodGet, "/v1/admin/automations/"+created.ID, fixture.platformAdminKey, nil)
	if adminShow.Code != http.StatusOK {
		t.Fatalf("admin show user automation: status=%d body=%s", adminShow.Code, adminShow.Body.String())
	}

	updateRequest := automationUpdateRequest(created.Generation)
	update := performJSONRequest(
		t,
		fixture.server,
		http.MethodPut,
		"/v1/automations/"+created.ID,
		fixture.writeKey,
		updateRequest,
	)
	if update.Code != http.StatusOK {
		t.Fatalf("update automation: expected status %d, got %d body=%s", http.StatusOK, update.Code, update.Body.String())
	}
	var updatedResponse model.AutomationPolicyResponse
	mustDecodeJSON(t, update, &updatedResponse)
	updated := updatedResponse.Policy
	if updated.Generation != 2 || updated.Mode != model.GatePolicyModeShadow || updated.Name != "API recovery shadow" {
		t.Fatalf("unexpected updated automation policy: %+v", updated)
	}

	staleUpdate := performJSONRequest(
		t,
		fixture.server,
		http.MethodPut,
		"/v1/automations/"+created.ID,
		fixture.writeKey,
		updateRequest,
	)
	if staleUpdate.Code != http.StatusConflict {
		t.Fatalf("stale update: expected status %d, got %d body=%s", http.StatusConflict, staleUpdate.Code, staleUpdate.Body.String())
	}
	staleDelete := performJSONRequest(
		t,
		fixture.server,
		http.MethodDelete,
		"/v1/automations/"+created.ID+"?expected_generation=1",
		fixture.writeKey,
		nil,
	)
	if staleDelete.Code != http.StatusConflict {
		t.Fatalf("stale delete: expected status %d, got %d body=%s", http.StatusConflict, staleDelete.Code, staleDelete.Body.String())
	}
	remove := performJSONRequest(
		t,
		fixture.server,
		http.MethodDelete,
		"/v1/automations/"+created.ID+"?expected_generation=2",
		fixture.writeKey,
		nil,
	)
	if remove.Code != http.StatusOK {
		t.Fatalf("delete automation: expected status %d, got %d body=%s", http.StatusOK, remove.Code, remove.Body.String())
	}
	var removed model.DeleteAutomationPolicyResponse
	mustDecodeJSON(t, remove, &removed)
	if !removed.Deleted || removed.Policy.ID != created.ID || removed.Policy.Generation != 2 {
		t.Fatalf("unexpected delete response: %+v", removed)
	}
	missing := performJSONRequest(t, fixture.server, http.MethodGet, "/v1/automations/"+created.ID, fixture.readKey, nil)
	if missing.Code != http.StatusNotFound {
		t.Fatalf("deleted automation: expected status %d, got %d body=%s", http.StatusNotFound, missing.Code, missing.Body.String())
	}

	events, err := fixture.store.ListAuditEvents(fixture.app.TenantID, false, 100)
	if err != nil {
		t.Fatalf("list automation audit events: %v", err)
	}
	actions := map[string]bool{}
	for _, event := range events {
		if event.TargetType == "automation_policy" && event.TargetID == created.ID {
			actions[event.Action] = true
			if event.Metadata["app_id"] != fixture.app.ID ||
				event.Metadata["project_id"] != fixture.app.ProjectID ||
				event.Metadata["generation"] == "" {
				t.Fatalf("automation audit event lost policy identity: %+v", event)
			}
		}
	}
	for _, action := range []string{
		"automation.policy.create",
		"automation.policy.update",
		"automation.policy.delete",
	} {
		if !actions[action] {
			t.Fatalf("missing automation mutation audit %q in %+v", action, actions)
		}
	}
}

func TestUserAutomationPolicyAuthorizationIsolationAndInputBoundary(t *testing.T) {
	t.Parallel()

	fixture := setupAutomationAPITestServer(t)
	validRequest := automationCreateRequest(fixture.app)
	create := performJSONRequest(t, fixture.server, http.MethodPost, "/v1/automations", fixture.writeKey, validRequest)
	if create.Code != http.StatusCreated {
		t.Fatalf("create fixture automation: status=%d body=%s", create.Code, create.Body.String())
	}
	var createdResponse model.AutomationPolicyResponse
	mustDecodeJSON(t, create, &createdResponse)
	policyID := createdResponse.Policy.ID

	for _, test := range []struct {
		name   string
		method string
		target string
		key    string
		body   any
		status int
	}{
		{name: "missing read scope", method: http.MethodGet, target: "/v1/automations", key: fixture.noScopeKey, status: http.StatusForbidden},
		{name: "read key cannot create", method: http.MethodPost, target: "/v1/automations", key: fixture.readKey, body: automationCreateRequest(fixture.app), status: http.StatusForbidden},
		{name: "other tenant cannot get", method: http.MethodGet, target: "/v1/automations/" + policyID, key: fixture.otherReadKey, status: http.StatusNotFound},
		{name: "delete requires generation", method: http.MethodDelete, target: "/v1/automations/" + policyID, key: fixture.writeKey, status: http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			recorder := performJSONRequest(t, fixture.server, test.method, test.target, test.key, test.body)
			if recorder.Code != test.status {
				t.Fatalf("expected status %d, got %d body=%s", test.status, recorder.Code, recorder.Body.String())
			}
		})
	}

	otherList := performJSONRequest(t, fixture.server, http.MethodGet, "/v1/automations", fixture.otherReadKey, nil)
	if otherList.Code != http.StatusOK {
		t.Fatalf("other tenant list: status=%d body=%s", otherList.Code, otherList.Body.String())
	}
	var otherListResponse model.AutomationPolicyListResponse
	mustDecodeJSON(t, otherList, &otherListResponse)
	if len(otherListResponse.Policies) != 0 {
		t.Fatalf("cross-tenant automation list leaked policies: %+v", otherListResponse.Policies)
	}

	forgedSafety := map[string]any{
		"name":  "forged safety",
		"kind":  model.AutomationPolicyKindAppRecovery,
		"scope": map[string]any{"type": model.AutomationScopeApp, "id": fixture.app.ID},
		"mode":  model.GatePolicyModeDisabled,
		"rules": []any{map[string]any{
			"id":      "restart",
			"trigger": automationRuleInput(1).Trigger,
			"action":  automationRuleInput(1).Action,
			"safety":  map[string]any{"requires_audit": false},
		}},
	}
	assertAutomationCreateStatus(t, fixture, forgedSafety, http.StatusBadRequest)

	enforced := automationCreateRequest(fixture.app)
	enforced.Name = "enforced"
	enforced.Mode = model.GatePolicyModeEnforced
	assertAutomationCreateStatus(t, fixture, enforced, http.StatusBadRequest)

	crossTenantApp := automationCreateRequest(fixture.otherApp)
	crossTenantApp.Name = "cross tenant"
	assertAutomationCreateStatus(t, fixture, crossTenantApp, http.StatusNotFound)

	unknownAction := automationCreateRequest(fixture.app)
	unknownAction.Name = "unknown action"
	unknownAction.Rules[0].Action.Type = "shell"
	assertAutomationCreateStatus(t, fixture, unknownAction, http.StatusBadRequest)

	clientError := automationCreateRequest(fixture.app)
	clientError.Name = "client errors"
	clientError.Rules[0].Trigger.RequestMetric.StatusCodes = []int{404}
	assertAutomationCreateStatus(t, fixture, clientError, http.StatusBadRequest)

	missingSelector := automationCreateRequest(fixture.app)
	missingSelector.Name = "missing selector"
	missingSelector.Rules[0].Trigger.RequestMetric = nil
	assertAutomationCreateStatus(t, fixture, missingSelector, http.StatusBadRequest)

	shortWindow := automationCreateRequest(fixture.app)
	shortWindow.Name = "short window"
	shortWindow.Rules[0].Trigger.RequestMetric.Window = "999ms"
	assertAutomationCreateStatus(t, fixture, shortWindow, http.StatusBadRequest)

	errorClass := automationCreateRequest(fixture.app)
	errorClass.Name = "mixed selector"
	errorClass.Rules[0].Trigger.RequestMetric.ErrorClasses = []string{"timeout"}
	assertAutomationCreateStatus(t, fixture, errorClass, http.StatusBadRequest)

	tooManyStatusCodes := automationCreateRequest(fixture.app)
	tooManyStatusCodes.Name = "too many status codes"
	tooManyStatusCodes.Rules[0].Trigger.RequestMetric.StatusCodes = make([]int, 101)
	for index := range tooManyStatusCodes.Rules[0].Trigger.RequestMetric.StatusCodes {
		tooManyStatusCodes.Rules[0].Trigger.RequestMetric.StatusCodes[index] = 500 + index%100
	}
	assertAutomationCreateStatus(t, fixture, tooManyStatusCodes, http.StatusBadRequest)

	negativeSamples := automationCreateRequest(fixture.app)
	negativeSamples.Name = "negative samples"
	negativeSamples.Rules[0].Trigger.MinimumSamples = -1
	assertAutomationCreateStatus(t, fixture, negativeSamples, http.StatusBadRequest)

	targetOverride := automationCreateRequest(fixture.app)
	targetOverride.Name = "target override"
	targetOverride.Rules[0].Action.Parameters["app_id"] = fixture.otherApp.ID
	assertAutomationCreateStatus(t, fixture, targetOverride, http.StatusBadRequest)

	duplicateNormalizedParameter := automationCreateRequest(fixture.app)
	duplicateNormalizedParameter.Name = "duplicate parameter"
	duplicateNormalizedParameter.Rules[0].Action.Parameters[" reason"] = "second value"
	assertAutomationCreateStatus(t, fixture, duplicateNormalizedParameter, http.StatusBadRequest)
}

type automationAPIFixture struct {
	store interface {
		ListAuditEvents(string, bool, int) ([]model.AuditEvent, error)
	}
	server           *Server
	app              model.App
	otherApp         model.App
	readKey          string
	writeKey         string
	noScopeKey       string
	otherReadKey     string
	platformAdminKey string
}

func setupAutomationAPITestServer(t *testing.T) automationAPIFixture {
	t.Helper()

	stateStore, server, _, platformAdminKey, app, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	_, readKey, err := stateStore.CreateAPIKey(app.TenantID, "automation-reader", []string{automationReadScope})
	if err != nil {
		t.Fatalf("create automation read key: %v", err)
	}
	_, writeKey, err := stateStore.CreateAPIKey(app.TenantID, "automation-writer", []string{automationWriteScope})
	if err != nil {
		t.Fatalf("create automation write key: %v", err)
	}
	_, noScopeKey, err := stateStore.CreateAPIKey(app.TenantID, "automation-no-scope", []string{"app.read"})
	if err != nil {
		t.Fatalf("create automation no-scope key: %v", err)
	}
	otherTenant, err := stateStore.CreateTenant("Other Automation Tenant")
	if err != nil {
		t.Fatalf("create other automation tenant: %v", err)
	}
	otherProject, err := stateStore.CreateProject(otherTenant.ID, "other", "")
	if err != nil {
		t.Fatalf("create other automation project: %v", err)
	}
	otherApp, err := stateStore.CreateApp(otherTenant.ID, otherProject.ID, "other", "", model.AppSpec{
		Image:     "ghcr.io/example/other:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatalf("create other automation app: %v", err)
	}
	_, otherReadKey, err := stateStore.CreateAPIKey(otherTenant.ID, "other-automation-reader", []string{automationReadScope})
	if err != nil {
		t.Fatalf("create other automation read key: %v", err)
	}
	return automationAPIFixture{
		store:            stateStore,
		server:           server,
		app:              app,
		otherApp:         otherApp,
		readKey:          readKey,
		writeKey:         writeKey,
		noScopeKey:       noScopeKey,
		otherReadKey:     otherReadKey,
		platformAdminKey: platformAdminKey,
	}
}

func automationCreateRequest(app model.App) model.CreateAutomationPolicyRequest {
	return model.CreateAutomationPolicyRequest{
		Name: "API recovery",
		Kind: model.AutomationPolicyKindAppRecovery,
		Scope: model.AutomationScope{
			Type: model.AutomationScopeApp,
			ID:   app.ID,
		},
		Mode:     model.GatePolicyModeDisabled,
		Priority: 100,
		Rules:    []model.AutomationRuleInput{automationRuleInput(1)},
		Metadata: map[string]string{"team": "platform"},
	}
}

func automationUpdateRequest(expectedGeneration int64) model.UpdateAutomationPolicyRequest {
	return model.UpdateAutomationPolicyRequest{
		ExpectedGeneration: expectedGeneration,
		Name:               "API recovery shadow",
		Description:        "observe request failures without production mutation",
		Mode:               model.GatePolicyModeShadow,
		Priority:           200,
		Rules:              []model.AutomationRuleInput{automationRuleInput(5)},
		Metadata:           map[string]string{"team": "platform"},
	}
}

func automationRuleInput(minimumSamples int) model.AutomationRuleInput {
	return model.AutomationRuleInput{
		ID:          "restart-on-503-504",
		Description: "propose a restart after repeated upstream unavailable responses",
		Trigger: model.AutomationTriggerInput{
			Type:   model.AutomationTriggerRequestMetric,
			Source: "app_request_outcomes",
			RequestMetric: &model.AutomationRequestMetricSelector{
				Metric:      "http_status",
				Window:      "2m",
				StatusCodes: []int{504, 503, 503},
			},
			RequiredEvidence:      []string{"operator_context"},
			MinimumSamples:        minimumSamples,
			MinimumFailureDomains: 0,
		},
		Action: model.AutomationActionInput{
			Type:       "restart_app",
			Parameters: map[string]string{"reason": "repeated 503/504 responses"},
		},
	}
}

func assertAutomationCreateStatus(
	t *testing.T,
	fixture automationAPIFixture,
	body any,
	wantStatus int,
) {
	t.Helper()
	recorder := performJSONRequest(t, fixture.server, http.MethodPost, "/v1/automations", fixture.writeKey, body)
	if recorder.Code != wantStatus {
		t.Fatalf("create automation: expected status %d, got %d body=%s", wantStatus, recorder.Code, recorder.Body.String())
	}
}
