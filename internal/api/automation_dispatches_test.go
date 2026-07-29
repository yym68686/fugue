package api

import (
	"context"
	"net/http"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/observability"
)

func TestAutomationActionDispatchPreservesUserPolicyModeCeiling(t *testing.T) {
	t.Setenv("FUGUE_AUTONOMY_KILL_SWITCH", "false")
	t.Setenv("FUGUE_AUTOMATION_APP_RESTART_ENABLED", "true")
	t.Setenv("FUGUE_AUTOMATION_APP_RESTART_KILL_SWITCH", "false")
	t.Setenv("FUGUE_GATE_AUTOMATION_APP_RESTART_MODE", "")

	fixture := setupAutomationAPITestServer(t)
	createRequest := automationCreateRequest(fixture.app)
	createRequest.Mode = model.GatePolicyModeShadow
	create := performJSONRequest(t, fixture.server, http.MethodPost, "/v1/automations", fixture.writeKey, createRequest)
	if create.Code != http.StatusCreated {
		t.Fatalf("create policy: status=%d body=%s", create.Code, create.Body.String())
	}
	var policyResponse model.AutomationPolicyResponse
	mustDecodeJSON(t, create, &policyResponse)

	promote := performJSONRequest(
		t,
		fixture.server,
		http.MethodPost,
		"/v1/admin/gates/automation.app-restart/promote",
		fixture.platformAdminKey,
		model.GatePolicyPromoteRequest{
			Mode:   model.GatePolicyModeEnforced,
			Reason: "verify user policy mode ceiling",
		},
	)
	if promote.Code != http.StatusOK {
		t.Fatalf("promote app restart gate: status=%d body=%s", promote.Code, promote.Body.String())
	}
	var promotion model.GatePolicyPromotionResponse
	mustDecodeJSON(t, promote, &promotion)
	verifyPlatformArtifactReleaseAPI(t, fixture.server, fixture.platformAdminKey, promotion.Release, true)
	gatePolicy, ok := gatePolicyByID(fixture.server.gatePolicyRegistry(), "automation.app-restart")
	if !ok || gatePolicy.Mode != model.GatePolicyModeEnforced {
		t.Fatalf("app restart gate was not promoted: %+v found=%t", gatePolicy, ok)
	}

	fixture.server.automationShadowLoopConfig = AutomationShadowLoopConfig{
		Enabled:  true,
		Interval: 30 * time.Second,
	}
	fixture.server.observabilityConfig = observability.Config{
		Enabled:       true,
		ClickHouseDSN: "http://clickhouse.example.test",
	}.Normalize()
	fixture.server.automationRequestOutcomeQuery = func(
		_ context.Context,
		_ string,
		_ []int,
		startedAt time.Time,
		endedAt time.Time,
	) ([]model.AutomationRequestOutcomeAggregate, string, error) {
		return []model.AutomationRequestOutcomeAggregate{
			{StatusCode: 503, Count: 2, FailureDomain: "edge:edge-us"},
			{StatusCode: 504, Count: 1, FailureDomain: "edge:edge-us"},
		}, "edge", nil
	}
	now := time.Now().UTC().Truncate(time.Second)
	if _, _, err := fixture.server.runAutomationShadowLoopOnce(
		context.Background(),
		now,
		automationShadowLoopCursor{},
	); err != nil {
		t.Fatalf("run shadow loop: %v", err)
	}
	intents, err := fixture.server.store.ListAutomationActionIntents(
		automationIntentFilterForTest(policyResponse.Policy, fixture.app),
	)
	if err != nil || len(intents) != 1 {
		t.Fatalf("list generated intents: count=%d err=%v", len(intents), err)
	}
	intent := intents[0]
	if intent.Mode != model.GatePolicyModeShadow {
		t.Fatalf("intent mode=%q, want immutable shadow policy mode", intent.Mode)
	}
	dispatch, err := fixture.server.store.GetAutomationActionDispatchByIntent(intent.ID)
	if err != nil {
		t.Fatalf("get generated dispatch: %v", err)
	}
	if dispatch.SafetyDecision.EffectiveMode != model.GatePolicyModeShadow ||
		dispatch.SafetyDecision.ProductionMutationAllowed ||
		dispatch.Status != model.AutomationActionDispatchStatusHeld {
		t.Fatalf("platform promotion elevated user policy intent: %+v", dispatch)
	}
}

func TestAutomationActionDispatchReadEndpointsRespectBoundary(t *testing.T) {
	t.Parallel()

	fixture := setupAutomationAPITestServer(t)
	createRequest := automationCreateRequest(fixture.app)
	createRequest.Mode = model.GatePolicyModeShadow
	create := performJSONRequest(t, fixture.server, http.MethodPost, "/v1/automations", fixture.writeKey, createRequest)
	if create.Code != http.StatusCreated {
		t.Fatalf("create policy: status=%d body=%s", create.Code, create.Body.String())
	}
	var policyResponse model.AutomationPolicyResponse
	mustDecodeJSON(t, create, &policyResponse)

	fixture.server.automationShadowLoopConfig = AutomationShadowLoopConfig{
		Enabled:  true,
		Interval: 30 * time.Second,
	}
	fixture.server.observabilityConfig = observability.Config{
		Enabled:       true,
		ClickHouseDSN: "http://clickhouse.example.test",
	}.Normalize()
	fixture.server.automationRequestOutcomeQuery = func(
		_ context.Context,
		_ string,
		_ []int,
		startedAt time.Time,
		endedAt time.Time,
	) ([]model.AutomationRequestOutcomeAggregate, string, error) {
		return []model.AutomationRequestOutcomeAggregate{
			{StatusCode: 503, Count: 2, FailureDomain: "edge:edge-us"},
			{StatusCode: 504, Count: 1, FailureDomain: "edge:edge-us"},
		}, "edge", nil
	}
	now := time.Date(2026, 7, 29, 3, 10, 7, 987654321, time.UTC)
	if _, _, err := fixture.server.runAutomationShadowLoopOnce(
		context.Background(),
		now,
		automationShadowLoopCursor{},
	); err != nil {
		t.Fatalf("run shadow loop: %v", err)
	}
	intents, err := fixture.server.store.ListAutomationActionIntents(automationIntentFilterForTest(policyResponse.Policy, fixture.app))
	if err != nil || len(intents) != 1 {
		t.Fatalf("list generated intents: count=%d err=%v", len(intents), err)
	}
	intent := intents[0]
	dispatch, err := fixture.server.store.GetAutomationActionDispatchByIntent(intent.ID)
	if err != nil {
		t.Fatalf("get generated dispatch: %v", err)
	}

	list := performJSONRequest(t, fixture.server, http.MethodGet,
		"/v1/automation-dispatches?app_id="+fixture.app.ID,
		fixture.readKey, nil)
	if list.Code != http.StatusOK {
		t.Fatalf("list dispatches: status=%d body=%s", list.Code, list.Body.String())
	}
	var listResponse model.AutomationActionDispatchListResponse
	mustDecodeJSON(t, list, &listResponse)
	if len(listResponse.Dispatches) != 1 || listResponse.Dispatches[0].ID != dispatch.ID ||
		listResponse.Dispatches[0].Status != model.AutomationActionDispatchStatusHeld {
		t.Fatalf("unexpected dispatch list: %+v", listResponse.Dispatches)
	}

	show := performJSONRequest(t, fixture.server, http.MethodGet,
		"/v1/automation-dispatches/"+dispatch.ID,
		fixture.readKey, nil)
	if show.Code != http.StatusOK {
		t.Fatalf("show dispatch: status=%d body=%s", show.Code, show.Body.String())
	}
	var showResponse model.AutomationActionDispatchResponse
	mustDecodeJSON(t, show, &showResponse)
	if showResponse.Dispatch.ID != dispatch.ID || showResponse.Dispatch.IntentID != intent.ID {
		t.Fatalf("unexpected dispatch detail: %+v", showResponse.Dispatch)
	}

	otherList := performJSONRequest(t, fixture.server, http.MethodGet, "/v1/automation-dispatches", fixture.otherReadKey, nil)
	if otherList.Code != http.StatusOK {
		t.Fatalf("other tenant list: status=%d body=%s", otherList.Code, otherList.Body.String())
	}
	var otherListResponse model.AutomationActionDispatchListResponse
	mustDecodeJSON(t, otherList, &otherListResponse)
	if len(otherListResponse.Dispatches) != 0 {
		t.Fatalf("dispatch leaked across tenants: %+v", otherListResponse.Dispatches)
	}
	otherShow := performJSONRequest(t, fixture.server, http.MethodGet,
		"/v1/automation-dispatches/"+dispatch.ID,
		fixture.otherReadKey, nil)
	if otherShow.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant dispatch get: status=%d body=%s", otherShow.Code, otherShow.Body.String())
	}

	for _, test := range []struct {
		name   string
		target string
		key    string
		status int
	}{
		{name: "missing scope", target: "/v1/automation-dispatches", key: fixture.noScopeKey, status: http.StatusForbidden},
		{name: "invalid status", target: "/v1/automation-dispatches?status=bogus", key: fixture.readKey, status: http.StatusBadRequest},
		{name: "invalid limit", target: "/v1/automation-dispatches?limit=0", key: fixture.readKey, status: http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := performJSONRequest(t, fixture.server, http.MethodGet, test.target, test.key, nil)
			if response.Code != test.status {
				t.Fatalf("%s: status=%d body=%s", test.name, response.Code, response.Body.String())
			}
		})
	}
}
