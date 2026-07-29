package api

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/observability"
	"fugue/internal/store"
)

func TestAutomationShadowLoopCreatesOneTrustedIntentAcrossLeaderReplay(t *testing.T) {
	t.Parallel()

	fixture := setupAutomationAPITestServer(t)
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

	fixture.server.automationShadowLoopConfig = AutomationShadowLoopConfig{
		Enabled:  true,
		Interval: 30 * time.Second,
	}
	fixture.server.observabilityConfig = observability.Config{
		Enabled:       true,
		ClickHouseDSN: "http://clickhouse.example.test",
	}.Normalize()

	now := time.Date(2026, 7, 29, 3, 10, 7, 987654321, time.UTC)
	var queryCount int
	fixture.server.automationRequestOutcomeQuery = func(
		_ context.Context,
		appID string,
		statusCodes []int,
		startedAt time.Time,
		endedAt time.Time,
	) ([]model.AutomationRequestOutcomeAggregate, string, error) {
		queryCount++
		if appID != fixture.app.ID {
			t.Fatalf("query app=%q, want %q", appID, fixture.app.ID)
		}
		if !reflect.DeepEqual(statusCodes, []int{503, 504}) {
			t.Fatalf("query status codes=%v", statusCodes)
		}
		if endedAt.Sub(startedAt) != 2*time.Minute ||
			endedAt.After(now.Add(-automationShadowLoopSettleDelay)) {
			t.Fatalf("query window start=%s end=%s now=%s", startedAt, endedAt, now)
		}
		return []model.AutomationRequestOutcomeAggregate{
			{StatusCode: 503, Count: 2, FailureDomain: "edge:edge-us"},
			{StatusCode: 504, Count: 1, FailureDomain: "edge:edge-us"},
		}, "edge", nil
	}

	operationsBefore, err := fixture.store.ListOperations(
		fixture.app.TenantID,
		false,
	)
	if err != nil {
		t.Fatal(err)
	}
	cursor := automationShadowLoopCursor{}
	first, _, err := fixture.server.runAutomationShadowLoopOnce(
		context.Background(),
		now,
		cursor,
	)
	if err != nil {
		t.Fatalf("first shadow loop: %v", err)
	}
	if first.PoliciesScanned != 1 ||
		first.ShadowPolicies != 1 ||
		first.Evaluations != 1 ||
		first.Matches != 1 ||
		first.IntentsCreated != 1 ||
		first.IntentsReused != 0 ||
		first.Errors != 0 {
		t.Fatalf("unexpected first run summary: %+v", first)
	}

	repeated, _, err := fixture.server.runAutomationShadowLoopOnce(
		context.Background(),
		now,
		cursor,
	)
	if err != nil {
		t.Fatalf("repeat same leader window: %v", err)
	}
	if repeated.Evaluations != 0 || queryCount != 1 {
		t.Fatalf("same leader reprocessed a completed window: summary=%+v queries=%d", repeated, queryCount)
	}

	failover, _, err := fixture.server.runAutomationShadowLoopOnce(
		context.Background(),
		now,
		automationShadowLoopCursor{},
	)
	if err != nil {
		t.Fatalf("leader failover replay: %v", err)
	}
	if failover.Evaluations != 1 ||
		failover.Matches != 1 ||
		failover.IntentsCreated != 0 ||
		failover.IntentsReused != 1 ||
		queryCount != 2 {
		t.Fatalf("leader replay was not idempotent: summary=%+v queries=%d", failover, queryCount)
	}

	intents, err := fixture.server.store.ListAutomationActionIntents(
		automationIntentFilterForTest(policy, fixture.app),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(intents) != 1 {
		t.Fatalf("control loop intent count=%d, want 1", len(intents))
	}
	intent := intents[0]
	if intent.Source != model.AutomationIntentSourceControlLoop ||
		!intent.Evidence.Trusted ||
		intent.Mode != model.GatePolicyModeShadow ||
		intent.Status != model.AutomationIntentStatusObserved ||
		intent.ProductionMutationAllowed ||
		intent.Decision.ProductionMutationAllowed {
		t.Fatalf("control loop crossed observe-only boundary: %+v", intent)
	}
	if intent.Decision.EvaluatedAt.Nanosecond()%1_000 != 0 {
		t.Fatalf("control-loop decision timestamp is not PostgreSQL-canonical: %s", intent.Decision.EvaluatedAt)
	}

	events, err := fixture.store.ListAuditEvents(fixture.app.TenantID, false, 100)
	if err != nil {
		t.Fatal(err)
	}
	controlLoopAudits := 0
	for _, event := range events {
		if event.TargetType != "automation_policy" ||
			event.TargetID != policy.ID ||
			event.Action != "automation.policy.evaluate_control_loop" {
			continue
		}
		controlLoopAudits++
		if event.ActorType != model.ActorTypeSystem ||
			event.ActorID != automationShadowLoopActorID ||
			event.Metadata["intent_id"] != intent.ID ||
			event.Metadata["production_mutation_allowed"] != "false" ||
			event.Metadata["observation_layer"] != "edge" {
			t.Fatalf("incomplete control-loop audit: %+v", event)
		}
	}
	if controlLoopAudits != 1 {
		t.Fatalf("control-loop audit count=%d, want 1", controlLoopAudits)
	}

	operationsAfter, err := fixture.store.ListOperations(
		fixture.app.TenantID,
		false,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(operationsAfter) != len(operationsBefore) {
		t.Fatalf("shadow loop created an operation: before=%d after=%d", len(operationsBefore), len(operationsAfter))
	}

	fixture.server.recordAutomationShadowLoopRun(now, 25*time.Millisecond, first, nil)
	var metrics bytes.Buffer
	fixture.server.writeAutomationShadowLoopMetrics(&metrics)
	for _, expected := range []string{
		"fugue_automation_shadow_loop_enabled 1.000000",
		"fugue_automation_shadow_loop_active 1.000000",
		"fugue_automation_shadow_loop_runs_total 1.000000",
		"fugue_automation_shadow_loop_evaluations_total 1.000000",
		"fugue_automation_shadow_loop_matches_total 1.000000",
		"fugue_automation_shadow_loop_intents_created_total 1.000000",
	} {
		if !strings.Contains(metrics.String(), expected) {
			t.Fatalf("metrics missing %q:\n%s", expected, metrics.String())
		}
	}
}

func TestAutomationShadowLoopContinuesAfterPolicyQueryError(t *testing.T) {
	t.Parallel()

	fixture := setupAutomationAPITestServer(t)
	for index, app := range []model.App{fixture.app, fixture.otherApp} {
		request := automationCreateRequest(app)
		request.Mode = model.GatePolicyModeShadow
		request.Name = fmt.Sprintf("shadow-%d", index)
		response := performJSONRequest(
			t,
			fixture.server,
			http.MethodPost,
			"/v1/automations",
			fixture.platformAdminKey,
			request,
		)
		if response.Code != http.StatusCreated {
			t.Fatalf("create shadow policy %d: status=%d body=%s", index, response.Code, response.Body.String())
		}
	}

	fixture.server.automationShadowLoopConfig = AutomationShadowLoopConfig{
		Enabled:  true,
		Interval: 30 * time.Second,
	}
	fixture.server.automationRequestOutcomeQuery = func(
		_ context.Context,
		appID string,
		_ []int,
		_ time.Time,
		_ time.Time,
	) ([]model.AutomationRequestOutcomeAggregate, string, error) {
		if appID == fixture.app.ID {
			return nil, "", errorsForAutomationShadowLoopTest
		}
		return []model.AutomationRequestOutcomeAggregate{
			{StatusCode: 503, Count: 3, FailureDomain: "runtime:runtime-a"},
		}, "app", nil
	}

	summary, _, err := fixture.server.runAutomationShadowLoopOnce(
		context.Background(),
		time.Date(2026, 7, 29, 3, 20, 7, 0, time.UTC),
		automationShadowLoopCursor{},
	)
	if err == nil || summary.Errors != 1 {
		t.Fatalf("expected one isolated query error, summary=%+v err=%v", summary, err)
	}
	if summary.Evaluations != 1 ||
		summary.Matches != 1 ||
		summary.IntentsCreated != 1 {
		t.Fatalf("healthy policy was blocked by another policy error: %+v", summary)
	}
}

var errorsForAutomationShadowLoopTest = fmt.Errorf("synthetic analytics failure")

func TestAutomationRequestOutcomeQueryAndLayerSelectionAreBounded(t *testing.T) {
	t.Parallel()

	startedAt := time.Date(2026, 7, 29, 3, 0, 0, 0, time.UTC)
	endedAt := startedAt.Add(2 * time.Minute)
	query, err := buildAutomationRequestOutcomesQuery(
		"app_'quoted",
		[]int{504, 503, 503},
		startedAt,
		endedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"app_id = 'app_\\'quoted'",
		"ts >= parseDateTime64BestEffort",
		"ts < parseDateTime64BestEffort",
		"(request_id != '' OR trace_id != '')",
		"status_code IN (503,504)",
		"uniqExact(if(request_id != '', concat('request:', request_id), concat('trace:', trace_id)))",
		"LIMIT 514 FORMAT JSONEachRow",
	} {
		if !strings.Contains(query, expected) {
			t.Fatalf("query missing %q:\n%s", expected, query)
		}
	}

	outcomes, layer, err := automationRequestOutcomesFromClickHouseRows([]map[string]any{
		{
			"observation_layer": "edge",
			"status_code":       float64(503),
			"failure_domain":    "edge-a",
			"request_count":     float64(9),
		},
		{
			"observation_layer": "app",
			"status_code":       "504",
			"failure_domain":    "runtime-a",
			"request_count":     "3",
		},
		{
			"observation_layer": "app",
			"status_code":       float64(503),
			"failure_domain":    "",
			"request_count":     float64(2),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []model.AutomationRequestOutcomeAggregate{
		{StatusCode: 503, Count: 2},
		{StatusCode: 504, Count: 3, FailureDomain: "app:runtime-a"},
	}
	if layer != "app" || !reflect.DeepEqual(outcomes, want) {
		t.Fatalf("layer selection=%q outcomes=%+v, want app %+v", layer, outcomes, want)
	}

	tooMany := make([]map[string]any, 0, maxAutomationShadowOutcomeAggregates+1)
	for index := 0; index <= maxAutomationShadowOutcomeAggregates; index++ {
		tooMany = append(tooMany, map[string]any{
			"observation_layer": "app",
			"status_code":       503,
			"failure_domain":    fmt.Sprintf("runtime-%03d", index),
			"request_count":     1,
		})
	}
	if _, _, err := automationRequestOutcomesFromClickHouseRows(tooMany); err == nil {
		t.Fatal("outcome aggregate overflow was accepted")
	}
}

func TestAutomationShadowLoopSkipsIneligibleAppWithoutTelemetryQuery(t *testing.T) {
	t.Parallel()

	fixture := setupAutomationAPITestServer(t)
	request := automationCreateRequest(fixture.app)
	request.Mode = model.GatePolicyModeShadow
	secondRule := request.Rules[0]
	secondRule.ID = "restart-on-502"
	secondRule.Trigger.RequestMetric.StatusCodes = []int{502}
	request.Rules = append(request.Rules, secondRule)
	response := performJSONRequest(
		t,
		fixture.server,
		http.MethodPost,
		"/v1/automations",
		fixture.writeKey,
		request,
	)
	if response.Code != http.StatusCreated {
		t.Fatalf("create shadow policy: status=%d body=%s", response.Code, response.Body.String())
	}

	app, err := fixture.store.GetApp(fixture.app.ID)
	if err != nil {
		t.Fatal(err)
	}
	app.Spec.Replicas = 0
	disableOperation, err := fixture.server.store.CreateOperation(model.Operation{
		TenantID:        app.TenantID,
		Type:            model.OperationTypeDeploy,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "automation-shadow-loop-test",
		AppID:           app.ID,
		DesiredSpec:     &app.Spec,
		ExecutionMode:   model.ExecutionModeManaged,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.server.store.CompleteManagedOperationWithResult(
		disableOperation.ID,
		"",
		"disabled",
		&app.Spec,
		nil,
	); err != nil {
		t.Fatal(err)
	}
	queryCount := 0
	fixture.server.automationRequestOutcomeQuery = func(
		context.Context,
		string,
		[]int,
		time.Time,
		time.Time,
	) ([]model.AutomationRequestOutcomeAggregate, string, error) {
		queryCount++
		return nil, "none", nil
	}

	cursor := automationShadowLoopCursor{}
	summary, _, err := fixture.server.runAutomationShadowLoopOnce(
		context.Background(),
		time.Date(2026, 7, 29, 3, 20, 7, 0, time.UTC),
		cursor,
	)
	if err != nil {
		t.Fatalf("skip ineligible app: %v", err)
	}
	if summary.IneligiblePolicies != 1 ||
		summary.Evaluations != 0 ||
		queryCount != 0 ||
		len(cursor) != 2 {
		t.Fatalf("ineligible app did not fail closed: summary=%+v queries=%d cursor=%v", summary, queryCount, cursor)
	}
}

func TestStrictAutomationIntegerRejectsFloatInt64Overflow(t *testing.T) {
	t.Parallel()

	if _, err := strictAutomationInteger(float64(9223372036854775808.0)); err == nil {
		t.Fatal("float64 int64 overflow was accepted")
	}
	if got, err := strictAutomationInteger(float64(9)); err != nil || got != 9 {
		t.Fatalf("small integral float rejected: got=%d err=%v", got, err)
	}
}

func TestAutomationShadowDueWindowsAreStableAndBounded(t *testing.T) {
	t.Parallel()

	window := 2 * time.Minute
	now := time.Date(2026, 7, 29, 4, 0, 7, 0, time.UTC)
	first, skipped := automationShadowDueWindowEnds(time.Time{}, now, window)
	if skipped != 0 || len(first) != 1 {
		t.Fatalf("unexpected initial due windows=%v skipped=%d", first, skipped)
	}
	if repeated, skipped := automationShadowDueWindowEnds(first[0], now, window); len(repeated) != 0 || skipped != 0 {
		t.Fatalf("same completed window repeated: windows=%v skipped=%d", repeated, skipped)
	}

	last := first[0].Add(-time.Duration(maxAutomationShadowCatchupWindows+3) * window)
	catchup, skipped := automationShadowDueWindowEnds(last, now, window)
	if len(catchup) != maxAutomationShadowCatchupWindows || skipped != 3 {
		t.Fatalf("catch-up bound drifted: windows=%d skipped=%d", len(catchup), skipped)
	}
	if !catchup[len(catchup)-1].Equal(first[0]) {
		t.Fatalf("catch-up did not end at the latest complete window: got=%s want=%s", catchup[len(catchup)-1], first[0])
	}
}

func TestAutomationShadowPolicyBatchRotatesWithoutStarvation(t *testing.T) {
	t.Parallel()

	policies := make([]model.AutomationPolicy, maxAutomationShadowPoliciesPerRun+3)
	for index := range policies {
		policies[index].ID = fmt.Sprintf("policy-%04d", index)
	}
	interval := 30 * time.Second
	firstSlot := time.Unix(30, 0).UTC()
	first, deferred := automationShadowPolicyBatch(policies, firstSlot, interval)
	second, secondDeferred := automationShadowPolicyBatch(
		policies,
		firstSlot.Add(interval),
		interval,
	)
	if len(first) != maxAutomationShadowPoliciesPerRun ||
		len(second) != maxAutomationShadowPoliciesPerRun ||
		deferred != 3 ||
		secondDeferred != 3 {
		t.Fatalf(
			"unexpected rotating batch sizes: first=%d second=%d deferred=%d/%d",
			len(first),
			len(second),
			deferred,
			secondDeferred,
		)
	}
	seen := make(map[string]struct{}, len(policies))
	for _, batch := range [][]model.AutomationPolicy{first, second} {
		for _, policy := range batch {
			seen[policy.ID] = struct{}{}
		}
	}
	if len(seen) != len(policies) {
		t.Fatalf("rotating policy batches starved inventory: seen=%d want=%d", len(seen), len(policies))
	}
	if reflect.DeepEqual(first, second) {
		t.Fatal("consecutive policy batches did not rotate")
	}
}

func automationIntentFilterForTest(
	policy model.AutomationPolicy,
	app model.App,
) store.AutomationActionIntentFilter {
	return store.AutomationActionIntentFilter{
		TenantID:  app.TenantID,
		ProjectID: app.ProjectID,
		PolicyID:  policy.ID,
		AppID:     app.ID,
		Source:    model.AutomationIntentSourceControlLoop,
		Limit:     100,
	}
}
