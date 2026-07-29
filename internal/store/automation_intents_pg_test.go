package store

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"fugue/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPostgresAutomationActionIntentAppendReuseAndScopedReads(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stateStore := &Store{db: db, databaseURL: "postgres://automation-intent-test", dbReady: true}
	now := time.Now().UTC().Truncate(time.Second)
	policy, app := postgresAutomationIntentFixture(t, now)
	intent := testObservedAutomationIntent(t, policy, app, now)
	intent.ID = "automation_intent_test"
	storedRow := intent
	storedRow.CreatedAt = now
	storedRow.UpdatedAt = now

	mock.ExpectBegin()
	mock.ExpectQuery(`(?s)FROM fugue_automation_action_intents\s+WHERE idempotency_key = \$1 FOR SHARE`).
		WithArgs(intent.IdempotencyKey).
		WillReturnRows(automationActionIntentSQLRows(t))
	mock.ExpectQuery(`(?s)FROM fugue_automation_policies\s+WHERE id = \$1\s+FOR KEY SHARE`).
		WithArgs(policy.ID).
		WillReturnRows(automationPolicySQLRows(t, policy))
	mock.ExpectQuery(`(?s)INSERT INTO fugue_automation_action_intents .*ON CONFLICT \(idempotency_key\) DO NOTHING.*RETURNING`).
		WithArgs(
			intent.ID, intent.TenantID, intent.ProjectID, intent.PolicyID, intent.PolicyGeneration, intent.RuleID,
			intent.Scope.Type, intent.Scope.ID, intent.Mode, intent.Source, intent.Status,
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
			intent.EvidenceHash, intent.IdempotencyKey, intent.RollbackTarget,
			false, intent.ExpiresAt, sqlmock.AnyArg(), sqlmock.AnyArg(),
		).
		WillReturnRows(automationActionIntentSQLRows(t, storedRow))
	mock.ExpectCommit()

	created, wasCreated, err := stateStore.CreateAutomationActionIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	if !wasCreated || created.ID != intent.ID {
		t.Fatalf("unexpected create result: intent=%+v created=%t", created, wasCreated)
	}

	mock.ExpectBegin()
	mock.ExpectQuery(`(?s)FROM fugue_automation_action_intents\s+WHERE idempotency_key = \$1 FOR SHARE`).
		WithArgs(intent.IdempotencyKey).
		WillReturnRows(automationActionIntentSQLRows(t, storedRow))
	mock.ExpectCommit()

	laterReplay := intent
	laterReplay.Decision.EvaluatedAt = laterReplay.Decision.EvaluatedAt.Add(time.Second)
	laterReplay.ExpiresAt = laterReplay.ExpiresAt.Add(time.Second)
	reused, wasCreated, err := stateStore.CreateAutomationActionIntent(laterReplay)
	if err != nil {
		t.Fatal(err)
	}
	if wasCreated || reused.ID != created.ID {
		t.Fatalf("unexpected reuse result: intent=%+v created=%t", reused, wasCreated)
	}

	mock.ExpectQuery(`(?s)FROM fugue_automation_action_intents\s+WHERE tenant_id = \$1 AND project_id = \$2 AND policy_id = \$3 AND scope_type = 'app' AND scope_id = \$4 AND source = \$5 AND status = \$6\s+ORDER BY created_at DESC, id DESC\s+LIMIT \$7`).
		WithArgs(
			intent.TenantID,
			intent.ProjectID,
			intent.PolicyID,
			intent.Scope.ID,
			intent.Source,
			intent.Status,
			10,
		).
		WillReturnRows(automationActionIntentSQLRows(t, storedRow))
	intents, err := stateStore.ListAutomationActionIntents(AutomationActionIntentFilter{
		TenantID:  intent.TenantID,
		ProjectID: intent.ProjectID,
		PolicyID:  intent.PolicyID,
		AppID:     intent.Scope.ID,
		Source:    intent.Source,
		Status:    intent.Status,
		Limit:     10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(intents) != 1 || intents[0].ID != intent.ID {
		t.Fatalf("unexpected intent list: %+v", intents)
	}

	mock.ExpectQuery(`(?s)FROM fugue_automation_action_intents\s+WHERE id = \$1 AND tenant_id = \$2`).
		WithArgs(intent.ID, intent.TenantID).
		WillReturnRows(automationActionIntentSQLRows(t, storedRow))
	got, err := stateStore.GetAutomationActionIntent(intent.ID, intent.TenantID, false)
	if err != nil || got.ID != intent.ID {
		t.Fatalf("tenant get error=%v intent=%+v", err, got)
	}

	mock.ExpectQuery(`(?s)FROM fugue_automation_action_intents\s+WHERE id = \$1 AND tenant_id = \$2`).
		WithArgs(intent.ID, "tenant_other").
		WillReturnRows(automationActionIntentSQLRows(t))
	if _, err := stateStore.GetAutomationActionIntent(intent.ID, "tenant_other", false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant get error=%v, want not found", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func automationActionIntentSQLRows(
	t *testing.T,
	intents ...model.AutomationActionIntent,
) *sqlmock.Rows {
	t.Helper()
	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "project_id", "policy_id", "policy_generation", "rule_id",
		"scope_type", "scope_id", "mode", "source", "status",
		"rule_snapshot_json", "evidence_json", "decision_json",
		"evidence_hash", "idempotency_key", "rollback_target",
		"production_mutation_allowed", "expires_at", "created_at", "updated_at",
	})
	for _, intent := range intents {
		ruleJSON, err := json.Marshal(intent.RuleSnapshot)
		if err != nil {
			t.Fatal(err)
		}
		evidenceJSON, err := json.Marshal(intent.Evidence)
		if err != nil {
			t.Fatal(err)
		}
		decisionJSON, err := json.Marshal(intent.Decision)
		if err != nil {
			t.Fatal(err)
		}
		rows.AddRow(
			intent.ID, intent.TenantID, intent.ProjectID, intent.PolicyID, intent.PolicyGeneration, intent.RuleID,
			intent.Scope.Type, intent.Scope.ID, intent.Mode, intent.Source, intent.Status,
			ruleJSON, evidenceJSON, decisionJSON,
			intent.EvidenceHash, intent.IdempotencyKey, intent.RollbackTarget,
			intent.ProductionMutationAllowed, intent.ExpiresAt, intent.CreatedAt, intent.UpdatedAt,
		)
	}
	return rows
}

func postgresAutomationIntentFixture(
	t *testing.T,
	now time.Time,
) (model.AutomationPolicy, model.App) {
	t.Helper()
	app := model.App{
		ID:        "app_intent",
		TenantID:  "tenant_intent",
		ProjectID: "project_intent",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/intent:latest",
			Replicas:  1,
			RuntimeID: "runtime_managed_shared",
		},
	}
	policy := testAutomationPolicy(app.TenantID, app.ProjectID, "Intent Policy")
	policy.ID = "automation_policy_intent"
	policy.Scope = model.AutomationScope{Type: model.AutomationScopeApp, ID: app.ID}
	policy.Mode = model.GatePolicyModeShadow
	policy.Rules[0].Trigger.Source = "app_request_outcomes"
	policy.Rules[0].Trigger.InvariantID = "app.request_unavailability"
	policy.Rules[0].Trigger.RequiredEvidence = []string{
		"app_request_outcomes",
		"app_revision",
		"app_readiness",
	}
	policy.Rules[0].Trigger.MinimumFailureDomains = 1
	var err error
	policy, err = normalizeAutomationPolicyForStore(policy)
	if err != nil {
		t.Fatal(err)
	}
	policy.Generation = 1
	policy.CreatedAt = now.Add(-time.Hour)
	policy.UpdatedAt = policy.CreatedAt
	return policy, app
}
