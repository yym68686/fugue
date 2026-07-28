package cli

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestAutomationCommandsCRUDUseBoundedCASContract(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:        "app_123",
		TenantID:  "tenant_123",
		ProjectID: "project_123",
		Name:      "demo-app",
	}
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	var state struct {
		sync.Mutex
		policy     *model.AutomationPolicy
		createBody model.CreateAutomationPolicyRequest
		updateBody model.UpdateAutomationPolicyRequest
		putCount   int
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer token" {
			t.Errorf("unexpected auth header %q", got)
			http.Error(w, "bad auth", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/apps":
			if got := r.URL.Query().Get("q"); got != app.Name {
				t.Errorf("unexpected app query %q", got)
				http.Error(w, "bad app query", http.StatusBadRequest)
				return
			}
			if r.URL.Query().Get("include_live_status") != "false" ||
				r.URL.Query().Get("include_resource_usage") != "false" {
				t.Errorf("app resolution requested live state: %s", r.URL.RawQuery)
				http.Error(w, "bad app lookup", http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"apps": []model.App{app}})

		case r.Method == http.MethodPost && r.URL.Path == "/v1/automations":
			raw, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read create body: %v", err)
				http.Error(w, "read body", http.StatusBadRequest)
				return
			}
			assertAutomationMutationOmitsServerSafety(t, raw)
			var request model.CreateAutomationPolicyRequest
			if err := json.Unmarshal(raw, &request); err != nil {
				t.Errorf("decode create body: %v", err)
				http.Error(w, "decode body", http.StatusBadRequest)
				return
			}
			if request.TenantID != app.TenantID ||
				request.ProjectID != app.ProjectID ||
				request.Scope != (model.AutomationScope{Type: model.AutomationScopeApp, ID: app.ID}) ||
				request.Kind != model.AutomationPolicyKindAppRecovery ||
				request.Mode != model.GatePolicyModeShadow ||
				len(request.Rules) != 1 {
				t.Errorf("unexpected create request: %+v", request)
				http.Error(w, "bad create request", http.StatusBadRequest)
				return
			}
			rule := request.Rules[0]
			if rule.Trigger.Type != model.AutomationTriggerRequestMetric ||
				rule.Trigger.Source != automationRequestOutcomeSource ||
				rule.Trigger.RequestMetric == nil ||
				rule.Trigger.RequestMetric.Metric != automationHTTPStatusMetric ||
				intsDisplay(rule.Trigger.RequestMetric.StatusCodes) != "502,504" ||
				rule.Action.Type != automationAppRestartActionType ||
				rule.Action.Parameters["reason"] != "repeated upstream unavailability" {
				t.Errorf("unexpected create rule: %+v", rule)
				http.Error(w, "bad create rule", http.StatusBadRequest)
				return
			}

			policy := automationCLITestPolicy(now, app, request)
			state.Lock()
			state.createBody = request
			state.policy = &policy
			state.Unlock()
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(model.AutomationPolicyResponse{Policy: policy})

		case r.Method == http.MethodGet && r.URL.Path == "/v1/automations":
			state.Lock()
			policies := []model.AutomationPolicy(nil)
			if state.policy != nil {
				policies = append(policies, *state.policy)
			}
			state.Unlock()
			_ = json.NewEncoder(w).Encode(model.AutomationPolicyListResponse{
				Policies:    policies,
				GeneratedAt: now,
			})

		case r.Method == http.MethodGet && r.URL.Path == "/v1/automations/automation_policy_123":
			state.Lock()
			policy := state.policy
			state.Unlock()
			if policy == nil {
				http.Error(w, `{"error":"automation policy not found"}`, http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(model.AutomationPolicyResponse{Policy: *policy})

		case r.Method == http.MethodPut && r.URL.Path == "/v1/automations/automation_policy_123":
			raw, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read update body: %v", err)
				http.Error(w, "read body", http.StatusBadRequest)
				return
			}
			assertAutomationMutationOmitsServerSafety(t, raw)
			var request model.UpdateAutomationPolicyRequest
			if err := json.Unmarshal(raw, &request); err != nil {
				t.Errorf("decode update body: %v", err)
				http.Error(w, "decode body", http.StatusBadRequest)
				return
			}

			state.Lock()
			defer state.Unlock()
			state.putCount++
			if state.policy == nil {
				http.Error(w, `{"error":"automation policy not found"}`, http.StatusNotFound)
				return
			}
			if request.ExpectedGeneration != state.policy.Generation {
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte(`{"error":"automation policy generation conflict"}`))
				return
			}
			if request.Name != state.policy.Name ||
				request.Description != state.policy.Description ||
				request.SourceRef != state.policy.SourceRef ||
				request.Mode != model.GatePolicyModeDisabled ||
				len(request.Rules) != 1 ||
				request.Rules[0].Trigger.RequestMetric == nil ||
				request.Rules[0].Trigger.RequestMetric.Window != "4m" ||
				intsDisplay(request.Rules[0].Trigger.RequestMetric.StatusCodes) != "500,503" ||
				request.Rules[0].Action.Parameters["reason"] != "operator reviewed" ||
				request.Metadata["owner"] != "sre" {
				t.Errorf("unexpected update request: %+v", request)
				http.Error(w, "bad update request", http.StatusBadRequest)
				return
			}
			updated := *state.policy
			updated.Mode = request.Mode
			updated.Metadata = request.Metadata
			updated.Rules[0].Trigger.RequestMetric.Window = request.Rules[0].Trigger.RequestMetric.Window
			updated.Rules[0].Trigger.RequestMetric.StatusCodes = append([]int(nil), request.Rules[0].Trigger.RequestMetric.StatusCodes...)
			updated.Rules[0].Action.Parameters = cloneStringMap(request.Rules[0].Action.Parameters)
			updated.Generation++
			updated.UpdatedAt = now.Add(time.Minute)
			state.updateBody = request
			state.policy = &updated
			_ = json.NewEncoder(w).Encode(model.AutomationPolicyResponse{Policy: updated})

		case r.Method == http.MethodDelete && r.URL.Path == "/v1/automations/automation_policy_123":
			if got := r.URL.Query().Get("expected_generation"); got != "2" {
				t.Errorf("unexpected delete generation %q", got)
				http.Error(w, "bad generation", http.StatusBadRequest)
				return
			}
			state.Lock()
			defer state.Unlock()
			if state.policy == nil {
				http.Error(w, `{"error":"automation policy not found"}`, http.StatusNotFound)
				return
			}
			removed := *state.policy
			state.policy = nil
			_ = json.NewEncoder(w).Encode(model.DeleteAutomationPolicyResponse{
				Deleted: true,
				Policy:  removed,
			})

		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.String())
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	}))
	defer server.Close()

	createOutput, err := runAutomationCommand(
		server.URL,
		"--json",
		"automation", "create", app.Name,
		"--name", "API unavailable recovery",
		"--description", "observe repeated upstream failures",
		"--mode", "shadow",
		"--window", "3m",
		"--status-code", "504,502,502",
		"--require-evidence", "operator_context",
		"--reason", "repeated upstream unavailability",
		"--metadata", "team=platform",
	)
	if err != nil {
		t.Fatalf("create automation: %v", err)
	}
	var created model.AutomationPolicyResponse
	if err := json.Unmarshal([]byte(createOutput), &created); err != nil {
		t.Fatalf("decode create output: %v output=%q", err, createOutput)
	}
	if created.Policy.ID != "automation_policy_123" ||
		created.Policy.Generation != 1 ||
		created.Policy.Rules[0].Safety.ActionContractID != "app.restart" {
		t.Fatalf("unexpected created policy: %+v", created.Policy)
	}

	listOutput, err := runAutomationCommand(server.URL, "automation", "ls")
	if err != nil {
		t.Fatalf("list automation: %v", err)
	}
	if !strings.Contains(listOutput, "automation_policy_123") ||
		!strings.Contains(listOutput, "restart_app") {
		t.Fatalf("unexpected list output %q", listOutput)
	}

	showOutput, err := runAutomationCommand(server.URL, "automation", "show", "automation_policy_123")
	if err != nil {
		t.Fatalf("show automation: %v", err)
	}
	for _, want := range []string{
		"request_metric=http_status",
		"request_window=3m",
		"request_status_codes=502,504",
		"action_contract=app.restart",
	} {
		if !strings.Contains(showOutput, want) {
			t.Fatalf("show output missing %q: %q", want, showOutput)
		}
	}

	updateOutput, err := runAutomationCommand(
		server.URL,
		"--json",
		"automation", "update", "automation_policy_123",
		"--generation", "1",
		"--mode", "disabled",
		"--window", "4m",
		"--status-code", "503,500,503",
		"--reason", "operator reviewed",
		"--metadata", "owner=sre",
	)
	if err != nil {
		t.Fatalf("update automation: %v", err)
	}
	var updated model.AutomationPolicyResponse
	if err := json.Unmarshal([]byte(updateOutput), &updated); err != nil {
		t.Fatalf("decode update output: %v output=%q", err, updateOutput)
	}
	if updated.Policy.Generation != 2 || updated.Policy.Mode != model.GatePolicyModeDisabled {
		t.Fatalf("unexpected updated policy: %+v", updated.Policy)
	}

	_, err = runAutomationCommand(
		server.URL,
		"automation", "update", "automation_policy_123",
		"--generation", "1",
		"--name", "stale update",
	)
	if err == nil || !strings.Contains(err.Error(), "generation conflict") {
		t.Fatalf("expected stale CAS conflict, got %v", err)
	}
	state.Lock()
	if state.putCount != 2 {
		t.Fatalf("stale update was retried: put_count=%d", state.putCount)
	}
	state.Unlock()

	deleteOutput, err := runAutomationCommand(
		server.URL,
		"automation", "delete", "automation_policy_123",
		"--generation", "2",
	)
	if err != nil {
		t.Fatalf("delete automation: %v", err)
	}
	if !strings.Contains(deleteOutput, "deleted=true") ||
		!strings.Contains(deleteOutput, "generation=2") {
		t.Fatalf("unexpected delete output %q", deleteOutput)
	}
}

func TestAutomationCommandsRejectUnsafeOrAmbiguousMutations(t *testing.T) {
	t.Parallel()

	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path == "/v1/apps" {
			_, _ = w.Write([]byte(`{"apps":[{"id":"app_123","tenant_id":"tenant_123","project_id":"project_123","name":"demo-app"}]}`))
			return
		}
		t.Errorf("unexpected mutation request %s %s", r.Method, r.URL.String())
		http.Error(w, "unexpected", http.StatusBadRequest)
	}))
	defer server.Close()

	cases := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "enforced mode",
			args: []string{"automation", "create", "demo-app", "--name", "unsafe", "--mode", "enforced"},
			want: "mode must be disabled or shadow",
		},
		{
			name: "non-server status",
			args: []string{"automation", "create", "demo-app", "--name", "unsafe", "--status-code", "429"},
			want: "between 500 and 599",
		},
		{
			name: "update requires positive generation",
			args: []string{"automation", "update", "automation_policy_123", "--generation", "0", "--mode", "shadow"},
			want: "generation must be a positive integer",
		},
		{
			name: "update rejects no-op",
			args: []string{"automation", "update", "automation_policy_123", "--generation", "1"},
			want: "at least one automation field",
		},
		{
			name: "delete requires positive generation",
			args: []string{"automation", "delete", "automation_policy_123", "--generation", "0"},
			want: "generation must be a positive integer",
		},
	}
	for _, test := range cases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			_, err := runAutomationCommand(server.URL, test.args...)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("expected error containing %q, got %v", test.want, err)
			}
		})
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("only create app lookups should have reached the server, got %d requests", got)
	}
}

func TestAutomationMultiRuleUpdateRequiresExplicitRule(t *testing.T) {
	t.Parallel()

	policy := automationCLITestPolicy(
		time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC),
		model.App{ID: "app_123", TenantID: "tenant_123", ProjectID: "project_123"},
		model.CreateAutomationPolicyRequest{
			Name: "recovery",
			Kind: model.AutomationPolicyKindAppRecovery,
			Scope: model.AutomationScope{
				Type: model.AutomationScopeApp,
				ID:   "app_123",
			},
			Mode: model.GatePolicyModeDisabled,
			Rules: []model.AutomationRuleInput{
				automationCLIRuleInput("gateway"),
				automationCLIRuleInput("worker"),
			},
		},
	)
	rules, err := automationRuleInputsFromPolicy(policy)
	if err != nil {
		t.Fatalf("convert rules: %v", err)
	}
	if _, err := selectAutomationRule(rules, ""); err == nil ||
		!strings.Contains(err.Error(), "--rule-id is required") {
		t.Fatalf("expected ambiguous rule error, got %v", err)
	}
	index, err := selectAutomationRule(rules, "worker")
	if err != nil {
		t.Fatalf("select worker: %v", err)
	}
	if index != 1 {
		t.Fatalf("expected worker index 1, got %d", index)
	}
}

func TestAutomationHelpDocumentsRequiredCASInputs(t *testing.T) {
	t.Parallel()

	output, err := runAutomationCommand("", "automation", "--help")
	if err != nil {
		t.Fatalf("automation help: %v", err)
	}
	for _, want := range []string{
		`fugue automation create my-app --name "API unavailable recovery"`,
		"fugue automation update automation_policy_123 --generation 1 --mode shadow",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("help output missing %q: %q", want, output)
		}
	}

	deleteHelp, err := runAutomationCommand("", "automation", "delete", "--help")
	if err != nil {
		t.Fatalf("delete help: %v", err)
	}
	if !strings.Contains(deleteHelp, "fugue automation delete automation_policy_123 --generation 2") {
		t.Fatalf("delete help omits CAS generation: %q", deleteHelp)
	}
}

func automationCLITestPolicy(
	now time.Time,
	app model.App,
	request model.CreateAutomationPolicyRequest,
) model.AutomationPolicy {
	rules := make([]model.AutomationRule, 0, len(request.Rules))
	for _, input := range request.Rules {
		requiredEvidence := append([]string{
			"app_readiness",
			"app_request_outcomes",
			"app_revision",
		}, input.Trigger.RequiredEvidence...)
		requiredEvidence = uniqueSortedAutomationTestStrings(requiredEvidence)
		rules = append(rules, model.AutomationRule{
			ID:          input.ID,
			Description: input.Description,
			Trigger: model.AutomationTrigger{
				Type:                  input.Trigger.Type,
				Source:                input.Trigger.Source,
				InvariantID:           "app.request_unavailability",
				RequestMetric:         input.Trigger.RequestMetric,
				RequiredEvidence:      requiredEvidence,
				MinimumSamples:        input.Trigger.MinimumSamples,
				MinimumFailureDomains: input.Trigger.MinimumFailureDomains,
			},
			Action: model.AutomationAction{
				Type:       input.Action.Type,
				Parameters: cloneStringMap(input.Action.Parameters),
			},
			Safety: model.AutomationSafetyPolicy{
				ActionContractID:       "app.restart",
				GatePolicyID:           "automation.app-restart",
				TTL:                    "5m",
				RecoveryCondition:      "app readiness and request outcomes recover",
				RollbackAction:         "restore desired app revision",
				RequiresRollbackTarget: true,
				RequiresAudit:          true,
				RequiresWAL:            true,
				RequiresIdempotencyKey: true,
				RequiresFencingToken:   true,
			},
		})
	}
	return model.AutomationPolicy{
		ID:          "automation_policy_123",
		TenantID:    app.TenantID,
		ProjectID:   app.ProjectID,
		Name:        request.Name,
		Description: request.Description,
		Kind:        model.AutomationPolicyKindAppRecovery,
		OwnerType:   model.AutomationOwnerUser,
		Scope:       request.Scope,
		Mode:        request.Mode,
		Priority:    request.Priority,
		SourceRef:   request.SourceRef,
		Rules:       rules,
		Generation:  1,
		Metadata:    cloneStringMap(request.Metadata),
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

func automationCLIRuleInput(id string) model.AutomationRuleInput {
	return model.AutomationRuleInput{
		ID: id,
		Trigger: model.AutomationTriggerInput{
			Type:   model.AutomationTriggerRequestMetric,
			Source: automationRequestOutcomeSource,
			RequestMetric: &model.AutomationRequestMetricSelector{
				Metric:      automationHTTPStatusMetric,
				Window:      "2m",
				StatusCodes: []int{503, 504},
			},
			MinimumSamples:        3,
			MinimumFailureDomains: 1,
		},
		Action: model.AutomationActionInput{Type: automationAppRestartActionType},
	}
}

func assertAutomationMutationOmitsServerSafety(t *testing.T, raw []byte) {
	t.Helper()
	for _, forbidden := range [][]byte{
		[]byte(`"safety"`),
		[]byte(`"invariant_id"`),
		[]byte(`"gate_policy_id"`),
		[]byte(`"action_contract_id"`),
	} {
		if bytes.Contains(raw, forbidden) {
			t.Errorf("mutation body contains server-owned field %s: %s", forbidden, raw)
		}
	}
}

func runAutomationCommand(baseURL string, args ...string) (string, error) {
	commandArgs := make([]string, 0, len(args)+4)
	if strings.TrimSpace(baseURL) != "" {
		commandArgs = append(commandArgs, "--base-url", baseURL, "--token", "token")
	}
	commandArgs = append(commandArgs, args...)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams(commandArgs, &stdout, &stderr)
	return stdout.String(), err
}

func intsDisplay(values []int) string {
	return automationStatusCodesDisplay(values)
}

func uniqueSortedAutomationTestStrings(values []string) []string {
	sort.Strings(values)
	out := values[:0]
	for _, value := range values {
		if len(out) > 0 && out[len(out)-1] == value {
			continue
		}
		out = append(out, value)
	}
	return out
}
