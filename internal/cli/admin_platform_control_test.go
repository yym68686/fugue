package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestAdminPlatformControlCommands(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 8, 0, 0, 0, time.UTC)
	invariant := model.InvariantDefinition{
		ID:              "test.invariant",
		Category:        "test",
		Scope:           model.GatePolicyScopeCluster,
		Owner:           "platform",
		Description:     "test invariant",
		Severity:        model.RobustnessSeverityBlockPublish,
		DefaultMode:     model.GatePolicyModeShadow,
		HardGate:        true,
		EvidenceSources: []string{"test-probe"},
		EvidenceFreshnessPolicy: model.InvariantEvidencePolicy{
			MaxAge:         "1m",
			MinimumSources: 1,
		},
		UnknownBehavior: model.InvariantEvidenceBehaviorHold,
		StaleBehavior:   model.InvariantEvidenceBehaviorHold,
		RunbookRef:      "docs/runbooks/invariant-registry.md",
	}
	contract := model.AutomaticActionContract{
		ID:                "test.action",
		ActionType:        "test_action",
		Scope:             model.GatePolicyScopeCluster,
		TriggerInvariant:  invariant.ID,
		EvidenceSource:    "test-probe",
		GatePolicyID:      "test.gate",
		TTL:               "1m",
		RecoveryCondition: "probe passes",
		RollbackAction:    "restore",
		EnableEnv:         "FUGUE_TEST_ACTION_ENABLED",
		KillSwitchEnv:     "FUGUE_TEST_ACTION_KILL_SWITCH",
		RunbookRef:        "docs/runbooks/action-safety-evaluator.md",
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer token" {
			t.Fatalf("unexpected auth header %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/admin/invariants":
			_ = json.NewEncoder(w).Encode(model.InvariantDefinitionListResponse{
				Invariants:  []model.InvariantDefinition{invariant},
				GeneratedAt: now,
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/admin/invariants/"+invariant.ID:
			_ = json.NewEncoder(w).Encode(model.InvariantDefinitionResponse{Invariant: invariant})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/admin/invariants/inventory":
			_ = json.NewEncoder(w).Encode(model.PlatformControlInventoryResponse{
				Inventory: model.PlatformControlInventory{
					GeneratedAt:      now,
					ArtifactKinds:    []string{"test_bundle"},
					Consumers:        []model.PlatformConsumerContractDefinition{{Component: "test-consumer"}},
					GatePolicies:     []model.GatePolicy{{ID: "test.gate", Mode: model.GatePolicyModeShadow}},
					AutomaticActions: []model.AutomaticActionContract{contract},
					ReleaseSignals:   []model.ReleaseSignal{{ID: "test-signal"}},
					SyntheticProbes:  []model.PlatformSyntheticProbeDefinition{{ID: "test-probe"}},
					LKGPolicies:      []model.PlatformLKGPolicyDefinition{{ArtifactKind: "test_bundle"}},
					Mechanisms:       []model.PlatformControlMechanism{{ID: "action_safety_evaluator", Category: "automatic_action", Status: model.PlatformControlMechanismShadow, Mode: model.GatePolicyModeShadow}},
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/admin/action-contracts":
			_ = json.NewEncoder(w).Encode(model.AutomaticActionContractListResponse{
				Contracts:   []model.AutomaticActionContract{contract},
				GeneratedAt: now,
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/admin/action-contracts/"+contract.ID:
			_ = json.NewEncoder(w).Encode(model.AutomaticActionContractResponse{Contract: contract})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/admin/action-safety/evaluate":
			var request model.ActionSafetyRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode action safety request: %v", err)
			}
			if request.ContractID != contract.ID {
				t.Fatalf("unexpected action safety request: %+v", request)
			}
			_ = json.NewEncoder(w).Encode(model.ActionSafetyDecisionResponse{
				Decision: model.ActionSafetyDecision{
					Pass:          true,
					WouldAction:   true,
					EffectiveMode: model.GatePolicyModeShadow,
					ContractID:    contract.ID,
					Subject:       request.Subject,
					BlastRadius:   model.BlastRadiusEvaluation{Pass: true, Scope: request.Scope},
					GeneratedAt:   now,
				},
			})
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	requestPath := filepath.Join(t.TempDir(), "action-safety.json")
	request := model.ActionSafetyRequest{
		ActionType:       contract.ActionType,
		ContractID:       contract.ID,
		TriggerInvariant: contract.TriggerInvariant,
		Scope:            contract.Scope,
		Subject:          "platform",
	}
	payload, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("marshal action safety request: %v", err)
	}
	if err := os.WriteFile(requestPath, payload, 0o600); err != nil {
		t.Fatalf("write action safety request: %v", err)
	}

	cases := []struct {
		name string
		args []string
		want string
	}{
		{name: "invariant list", args: []string{"admin", "invariant", "ls"}, want: invariant.ID},
		{name: "invariant show", args: []string{"admin", "invariant", "show", invariant.ID}, want: "evidence_max_age=1m"},
		{name: "inventory", args: []string{"admin", "invariant", "inventory"}, want: "action_safety_evaluator"},
		{name: "action contract list", args: []string{"admin", "action-contract", "ls"}, want: contract.ID},
		{name: "action contract show", args: []string{"admin", "action-contract", "show", contract.ID}, want: "trigger_invariant=test.invariant"},
		{name: "action safety evaluate", args: []string{"admin", "action-safety", "evaluate", "--file", requestPath}, want: "would_action=true"},
	}
	for _, test := range cases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			args := append([]string{"--base-url", server.URL, "--token", "token"}, test.args...)
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			if err := runWithStreams(args, &stdout, &stderr); err != nil {
				t.Fatalf("run %v: %v stderr=%s", test.args, err, stderr.String())
			}
			if output := stdout.String(); !strings.Contains(output, test.want) {
				t.Fatalf("expected output to contain %q, got %q", test.want, output)
			}
		})
	}
}

func TestReadActionSafetyRequestRejectsUnknownFields(t *testing.T) {
	t.Parallel()

	_, err := readActionSafetyRequest("-", strings.NewReader(`{"contract_id":"test","unknown":true}`))
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("expected strict JSON decode error, got %v", err)
	}
}
