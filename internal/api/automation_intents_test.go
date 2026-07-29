package api

import (
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestAutomationReplayCreatesOnlyAppendOnlyObserveIntent(t *testing.T) {
	t.Parallel()

	fixture := setupAutomationAPITestServer(t)
	appBefore, err := fixture.store.GetApp(fixture.app.ID)
	if err != nil {
		t.Fatal(err)
	}
	operationsBefore, err := fixture.store.ListOperations(fixture.app.TenantID, false)
	if err != nil {
		t.Fatal(err)
	}

	createRequest := automationCreateRequest(fixture.app)
	createRequest.Mode = model.GatePolicyModeShadow
	create := performJSONRequest(
		t,
		fixture.server,
		http.MethodPost,
		"/v1/automations",
		fixture.writeKey,
		createRequest,
	)
	if create.Code != http.StatusCreated {
		t.Fatalf("create shadow policy: status=%d body=%s", create.Code, create.Body.String())
	}
	var policyResponse model.AutomationPolicyResponse
	mustDecodeJSON(t, create, &policyResponse)
	policy := policyResponse.Policy

	now := time.Now().UTC().Truncate(time.Second)
	replayRequest := model.EvaluateAutomationPolicyRequest{
		PolicyID:           policy.ID,
		ExpectedGeneration: policy.Generation,
		RuleID:             policy.Rules[0].ID,
		WindowStartedAt:    now.Add(-2 * time.Minute),
		WindowEndedAt:      now,
		RequestOutcomes: []model.AutomationRequestOutcomeAggregate{
			{StatusCode: 503, Count: 2, FailureDomain: "edge-us"},
			{StatusCode: 504, Count: 1, FailureDomain: "edge-us"},
			{StatusCode: 200, Count: 5, FailureDomain: "edge-us"},
		},
	}
	evaluate := performJSONRequest(
		t,
		fixture.server,
		http.MethodPost,
		"/v1/admin/automation-evaluations",
		fixture.platformAdminKey,
		replayRequest,
	)
	if evaluate.Code != http.StatusOK {
		t.Fatalf("evaluate shadow replay: status=%d body=%s", evaluate.Code, evaluate.Body.String())
	}
	var response model.AutomationEvaluationResponse
	mustDecodeJSON(t, evaluate, &response)
	if !response.Decision.Matched ||
		!response.Decision.WouldAction ||
		response.Decision.ProductionMutationAllowed ||
		response.Decision.MatchingSamples != 3 ||
		!response.IntentCreated ||
		response.Intent == nil {
		t.Fatalf("unexpected replay decision: %+v", response)
	}
	intent := *response.Intent
	if intent.PolicyID != policy.ID ||
		intent.PolicyGeneration != policy.Generation ||
		intent.Scope.ID != fixture.app.ID ||
		intent.Source != model.AutomationIntentSourceAdminReplay ||
		intent.Status != model.AutomationIntentStatusObserved ||
		intent.Mode != model.GatePolicyModeShadow ||
		intent.Evidence.Trusted ||
		intent.ProductionMutationAllowed ||
		intent.Decision.ProductionMutationAllowed ||
		intent.RuleSnapshot.Safety.ActionContractID != "app.restart" ||
		intent.IdempotencyKey == "" ||
		intent.RollbackTarget == "" {
		t.Fatalf("observe-only boundary was not preserved: %+v", intent)
	}

	repeated := performJSONRequest(
		t,
		fixture.server,
		http.MethodPost,
		"/v1/admin/automation-evaluations",
		fixture.platformAdminKey,
		replayRequest,
	)
	if repeated.Code != http.StatusOK {
		t.Fatalf("repeat shadow replay: status=%d body=%s", repeated.Code, repeated.Body.String())
	}
	var repeatedResponse model.AutomationEvaluationResponse
	mustDecodeJSON(t, repeated, &repeatedResponse)
	if repeatedResponse.IntentCreated ||
		repeatedResponse.Intent == nil ||
		repeatedResponse.Intent.ID != intent.ID {
		t.Fatalf("identical replay was not idempotent: %+v", repeatedResponse)
	}

	list := performJSONRequest(
		t,
		fixture.server,
		http.MethodGet,
		"/v1/automation-intents?policy_id="+policy.ID+"&app_id="+fixture.app.ID,
		fixture.readKey,
		nil,
	)
	if list.Code != http.StatusOK {
		t.Fatalf("list intents: status=%d body=%s", list.Code, list.Body.String())
	}
	var listResponse model.AutomationActionIntentListResponse
	mustDecodeJSON(t, list, &listResponse)
	if len(listResponse.Intents) != 1 || listResponse.Intents[0].ID != intent.ID {
		t.Fatalf("unexpected intent list: %+v", listResponse.Intents)
	}
	show := performJSONRequest(
		t,
		fixture.server,
		http.MethodGet,
		"/v1/automation-intents/"+intent.ID,
		fixture.readKey,
		nil,
	)
	if show.Code != http.StatusOK {
		t.Fatalf("show intent: status=%d body=%s", show.Code, show.Body.String())
	}

	otherList := performJSONRequest(t, fixture.server, http.MethodGet, "/v1/automation-intents", fixture.otherReadKey, nil)
	if otherList.Code != http.StatusOK {
		t.Fatalf("other tenant list: status=%d body=%s", otherList.Code, otherList.Body.String())
	}
	var otherListResponse model.AutomationActionIntentListResponse
	mustDecodeJSON(t, otherList, &otherListResponse)
	if len(otherListResponse.Intents) != 0 {
		t.Fatalf("intent leaked across tenants: %+v", otherListResponse.Intents)
	}
	otherShow := performJSONRequest(
		t,
		fixture.server,
		http.MethodGet,
		"/v1/automation-intents/"+intent.ID,
		fixture.otherReadKey,
		nil,
	)
	if otherShow.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant intent get: status=%d body=%s", otherShow.Code, otherShow.Body.String())
	}

	remove := performJSONRequest(
		t,
		fixture.server,
		http.MethodDelete,
		"/v1/automations/"+policy.ID+"?expected_generation=1",
		fixture.writeKey,
		nil,
	)
	if remove.Code != http.StatusOK {
		t.Fatalf("delete source policy: status=%d body=%s", remove.Code, remove.Body.String())
	}
	historical := performJSONRequest(
		t,
		fixture.server,
		http.MethodGet,
		"/v1/automation-intents/"+intent.ID,
		fixture.readKey,
		nil,
	)
	if historical.Code != http.StatusOK {
		t.Fatalf("intent did not survive policy deletion: status=%d body=%s", historical.Code, historical.Body.String())
	}

	appAfter, err := fixture.store.GetApp(fixture.app.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(appAfter.Spec, appBefore.Spec) ||
		!reflect.DeepEqual(appAfter.Status, appBefore.Status) {
		t.Fatalf("observe-only replay mutated app: before=%+v after=%+v", appBefore, appAfter)
	}
	operationsAfter, err := fixture.store.ListOperations(fixture.app.TenantID, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(operationsAfter) != len(operationsBefore) {
		t.Fatalf("observe-only replay created an operation: before=%d after=%d", len(operationsBefore), len(operationsAfter))
	}

	events, err := fixture.store.ListAuditEvents(fixture.app.TenantID, false, 100)
	if err != nil {
		t.Fatal(err)
	}
	replayAudits := 0
	for _, event := range events {
		if event.TargetType != "automation_policy" || event.TargetID != policy.ID {
			continue
		}
		if event.Action == "automation.policy.evaluate_replay" {
			replayAudits++
			if event.Metadata["production_mutation_allowed"] != "false" ||
				event.Metadata["intent_id"] != intent.ID ||
				event.Metadata["evidence_hash"] != intent.EvidenceHash {
				t.Fatalf("incomplete replay audit: %+v", event)
			}
		}
		if strings.Contains(event.Action, "execute") || strings.Contains(event.Action, "restart") {
			t.Fatalf("replay emitted an execution audit: %+v", event)
		}
	}
	if replayAudits != 2 {
		t.Fatalf("replay audit count=%d, want 2", replayAudits)
	}
}

func TestAutomationReplayFailsClosedForDisabledStaleUnauthorizedAndMalformedRequests(t *testing.T) {
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
		t.Fatalf("create disabled policy: status=%d body=%s", create.Code, create.Body.String())
	}
	var policyResponse model.AutomationPolicyResponse
	mustDecodeJSON(t, create, &policyResponse)
	policy := policyResponse.Policy
	now := time.Now().UTC().Truncate(time.Second)
	valid := model.EvaluateAutomationPolicyRequest{
		PolicyID:           policy.ID,
		ExpectedGeneration: policy.Generation,
		RuleID:             policy.Rules[0].ID,
		WindowStartedAt:    now.Add(-2 * time.Minute),
		WindowEndedAt:      now,
		RequestOutcomes: []model.AutomationRequestOutcomeAggregate{{
			StatusCode:    503,
			Count:         3,
			FailureDomain: "edge-us",
		}},
	}

	disabled := performJSONRequest(
		t,
		fixture.server,
		http.MethodPost,
		"/v1/admin/automation-evaluations",
		fixture.platformAdminKey,
		valid,
	)
	if disabled.Code != http.StatusOK {
		t.Fatalf("evaluate disabled policy: status=%d body=%s", disabled.Code, disabled.Body.String())
	}
	var disabledResponse model.AutomationEvaluationResponse
	mustDecodeJSON(t, disabled, &disabledResponse)
	if disabledResponse.Decision.Matched ||
		disabledResponse.Decision.WouldAction ||
		disabledResponse.Intent != nil ||
		disabledResponse.IntentCreated ||
		!reflect.DeepEqual(disabledResponse.Decision.ReasonCodes, []string{"policy.disabled"}) {
		t.Fatalf("disabled policy escaped observe boundary: %+v", disabledResponse)
	}

	stale := valid
	stale.ExpectedGeneration++
	assertAutomationReplayStatus(t, fixture, fixture.platformAdminKey, stale, http.StatusConflict)
	assertAutomationReplayStatus(t, fixture, fixture.writeKey, valid, http.StatusForbidden)

	future := valid
	future.WindowEndedAt = now.Add(time.Minute)
	assertAutomationReplayStatus(t, fixture, fixture.platformAdminKey, future, http.StatusBadRequest)

	negative := valid
	negative.RequestOutcomes = []model.AutomationRequestOutcomeAggregate{{
		StatusCode:    503,
		Count:         -1,
		FailureDomain: "edge-us",
	}}
	assertAutomationReplayStatus(t, fixture, fixture.platformAdminKey, negative, http.StatusBadRequest)

	noScopeList := performJSONRequest(t, fixture.server, http.MethodGet, "/v1/automation-intents", fixture.noScopeKey, nil)
	if noScopeList.Code != http.StatusForbidden {
		t.Fatalf("no-scope intent list: status=%d body=%s", noScopeList.Code, noScopeList.Body.String())
	}
	badLimit := performJSONRequest(t, fixture.server, http.MethodGet, "/v1/automation-intents?limit=0", fixture.readKey, nil)
	if badLimit.Code != http.StatusBadRequest {
		t.Fatalf("invalid intent limit: status=%d body=%s", badLimit.Code, badLimit.Body.String())
	}
}

func assertAutomationReplayStatus(
	t *testing.T,
	fixture automationAPIFixture,
	key string,
	request model.EvaluateAutomationPolicyRequest,
	want int,
) {
	t.Helper()
	recorder := performJSONRequest(
		t,
		fixture.server,
		http.MethodPost,
		"/v1/admin/automation-evaluations",
		key,
		request,
	)
	if recorder.Code != want {
		t.Fatalf("automation replay: status=%d want=%d body=%s", recorder.Code, want, recorder.Body.String())
	}
}
