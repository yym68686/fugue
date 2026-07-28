package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestAdminAutomationCommands(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	policy := model.AutomationPolicy{
		ID:        "system.test_action",
		Name:      "test_action",
		Kind:      model.AutomationPolicyKindManagedSystem,
		OwnerType: model.AutomationOwnerSystem,
		Scope:     model.AutomationScope{Type: model.GatePolicyScopeCluster},
		Mode:      model.GatePolicyModeShadow,
		Managed:   true,
		SourceRef: "automatic-action-contract:test_action",
		Rules: []model.AutomationRule{{
			ID: "default",
			Trigger: model.AutomationTrigger{
				Type:        model.AutomationTriggerInvariant,
				Source:      "test.invariant",
				InvariantID: "test.invariant",
			},
			Action: model.AutomationAction{
				Type:       "test_action",
				Parameters: map[string]string{"mode": "observe"},
			},
			Safety: model.AutomationSafetyPolicy{
				ActionContractID:  "test_action",
				GatePolicyID:      "test.gate",
				TTL:               "5m",
				RecoveryCondition: "probe passes",
				RollbackAction:    "restore",
			},
		}},
		Generation: 1,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer token" {
			t.Fatalf("unexpected auth header %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/admin/automations":
			_ = json.NewEncoder(w).Encode(model.AutomationPolicyListResponse{
				Policies:    []model.AutomationPolicy{policy},
				GeneratedAt: now,
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/admin/automations/"+policy.ID:
			_ = json.NewEncoder(w).Encode(model.AutomationPolicyResponse{Policy: policy})
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	cases := []struct {
		name string
		args []string
		want string
	}{
		{name: "list", args: []string{"admin", "automation", "ls"}, want: "system.test_action"},
		{name: "show", args: []string{"admin", "automation", "show", policy.ID}, want: "action_type=test_action"},
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

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runWithStreams(
		append([]string{"--base-url", server.URL, "--token", "token", "--json"}, "admin", "automation", "ls"),
		&stdout,
		&stderr,
	); err != nil {
		t.Fatalf("json list: %v stderr=%s", err, stderr.String())
	}
	var jsonResponse model.AutomationPolicyListResponse
	if err := json.Unmarshal(stdout.Bytes(), &jsonResponse); err != nil {
		t.Fatalf("decode json list: %v output=%q", err, stdout.String())
	}
	if len(jsonResponse.Policies) != 1 || jsonResponse.GeneratedAt.IsZero() {
		t.Fatalf("unexpected json list response: %+v", jsonResponse)
	}
}
