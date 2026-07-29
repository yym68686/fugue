package store

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	automationdomain "fugue/internal/automation"
	"fugue/internal/model"
)

func TestAutomationActionIntentJSONStoreIsAppendOnlyIdempotentAndIsolated(t *testing.T) {
	t.Parallel()

	storePath := filepath.Join(t.TempDir(), "store.json")
	stateStore := New(storePath)
	tenant, project, app, policy := createAutomationIntentParents(t, stateStore, "Primary")
	otherTenant, err := stateStore.CreateTenant("Other Automation Tenant")
	if err != nil {
		t.Fatal(err)
	}

	intent := testObservedAutomationIntent(t, policy, app, time.Now().UTC())
	created, wasCreated, err := stateStore.CreateAutomationActionIntent(intent)
	if err != nil {
		t.Fatalf("create intent: %v", err)
	}
	if !wasCreated ||
		!strings.HasPrefix(created.ID, "automation_intent_") ||
		created.CreatedAt.IsZero() ||
		!created.CreatedAt.Equal(created.UpdatedAt) ||
		created.ProductionMutationAllowed {
		t.Fatalf("unexpected created intent: %+v created=%t", created, wasCreated)
	}

	reused, wasCreated, err := stateStore.CreateAutomationActionIntent(intent)
	if err != nil {
		t.Fatalf("reuse intent: %v", err)
	}
	if wasCreated || reused.ID != created.ID || reused.IdempotencyKey != created.IdempotencyKey {
		t.Fatalf("immutable replay was not reused: first=%+v repeated=%+v created=%t", created, reused, wasCreated)
	}
	laterReplay := intent
	laterReplay.Decision.EvaluatedAt = laterReplay.Decision.EvaluatedAt.Add(time.Second)
	laterReplay.ExpiresAt = laterReplay.ExpiresAt.Add(time.Second)
	reused, wasCreated, err = stateStore.CreateAutomationActionIntent(laterReplay)
	if err != nil {
		t.Fatalf("reuse intent evaluated later: %v", err)
	}
	if wasCreated || reused.ID != created.ID || !reused.Decision.EvaluatedAt.Equal(created.Decision.EvaluatedAt) {
		t.Fatalf("later replay did not return the first immutable record: first=%+v repeated=%+v created=%t", created, reused, wasCreated)
	}

	listed, err := stateStore.ListAutomationActionIntents(AutomationActionIntentFilter{
		TenantID:  tenant.ID,
		ProjectID: project.ID,
		PolicyID:  policy.ID,
		AppID:     app.ID,
		Source:    model.AutomationIntentSourceAdminReplay,
		Status:    model.AutomationIntentStatusObserved,
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("list intent: %v", err)
	}
	if len(listed) != 1 || listed[0].ID != created.ID {
		t.Fatalf("unexpected intent list: %+v", listed)
	}
	if _, err := stateStore.GetAutomationActionIntent(created.ID, otherTenant.ID, false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant get error=%v, want not found", err)
	}
	if _, err := stateStore.ListAutomationActionIntents(AutomationActionIntentFilter{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("unscoped non-admin list error=%v, want invalid input", err)
	}

	listed[0].Evidence.RequestOutcomes[0].Count = 999
	again, err := stateStore.GetAutomationActionIntent(created.ID, tenant.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	if again.Evidence.RequestOutcomes[0].Count == 999 {
		t.Fatal("mutating a returned intent changed persisted evidence")
	}

	reopened := New(storePath)
	persisted, err := reopened.GetAutomationActionIntent(created.ID, tenant.ID, false)
	if err != nil {
		t.Fatalf("reload intent: %v", err)
	}
	if persisted.IdempotencyKey != created.IdempotencyKey ||
		persisted.EvidenceHash != created.EvidenceHash {
		t.Fatalf("intent changed across JSON persistence: before=%+v after=%+v", created, persisted)
	}
}

func TestAutomationActionIntentSurvivesPolicyDeletionButCascadesTenantDeletion(t *testing.T) {
	t.Parallel()

	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	tenant, _, app, policy := createAutomationIntentParents(t, stateStore, "History")
	intent := testObservedAutomationIntent(t, policy, app, time.Now().UTC())
	created, _, err := stateStore.CreateAutomationActionIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stateStore.DeleteAutomationPolicy(policy.ID, tenant.ID, false, policy.Generation); err != nil {
		t.Fatalf("delete source policy: %v", err)
	}
	if _, err := stateStore.GetAutomationActionIntent(created.ID, tenant.ID, false); err != nil {
		t.Fatalf("append-only intent disappeared with policy: %v", err)
	}

	if _, err := stateStore.DeleteTenant(tenant.ID); err != nil {
		t.Fatalf("delete tenant: %v", err)
	}
	intents, err := stateStore.ListAutomationActionIntents(AutomationActionIntentFilter{
		PlatformAdmin: true,
		Limit:         10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(intents) != 0 {
		t.Fatalf("tenant deletion left automation intents: %+v", intents)
	}
}

func TestAutomationActionIntentRejectsStalePolicyAndExecutableMutation(t *testing.T) {
	t.Parallel()

	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	tenant, _, app, policy := createAutomationIntentParents(t, stateStore, "Reject")
	intent := testObservedAutomationIntent(t, policy, app, time.Now().UTC())

	update := policy
	update.Description = "new generation"
	updated, err := stateStore.UpdateAutomationPolicy(update, tenant.ID, false, policy.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Generation != policy.Generation+1 {
		t.Fatalf("policy generation did not advance: %+v", updated)
	}
	if _, _, err := stateStore.CreateAutomationActionIntent(intent); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale-generation intent error=%v, want conflict", err)
	}

	currentIntent := testObservedAutomationIntent(t, updated, app, time.Now().UTC())
	currentIntent.ProductionMutationAllowed = true
	if _, _, err := stateStore.CreateAutomationActionIntent(currentIntent); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("executable intent error=%v, want invalid input", err)
	}
}

func TestAutomationActionIntentListRejectsUnknownFiltersAndLimits(t *testing.T) {
	t.Parallel()

	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	for _, filter := range []AutomationActionIntentFilter{
		{PlatformAdmin: true, Source: "unknown"},
		{PlatformAdmin: true, Status: "pending"},
		{PlatformAdmin: true, Limit: -1},
		{PlatformAdmin: true, Limit: maxAutomationActionIntentLimit + 1},
	} {
		if _, err := stateStore.ListAutomationActionIntents(filter); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("filter=%+v error=%v, want invalid input", filter, err)
		}
	}
}

func TestAutomationActionIntentPermissionAndSchemaBoundaryIsAppendOnly(t *testing.T) {
	t.Parallel()

	grants := strings.Join(defaultControlPlaneRequiredGrants(), "\n")
	for _, required := range []string{
		"fugue_automation_action_intents:select",
		"fugue_automation_action_intents:insert",
		"fugue_automation_action_dispatches:select",
		"fugue_automation_action_dispatches:insert",
		"fugue_automation_action_dispatches:update",
		"fugue_automation_action_fencing:select",
		"fugue_automation_action_fencing:insert",
		"fugue_automation_action_fencing:update",
	} {
		if !strings.Contains(grants, required) {
			t.Fatalf("default permission audit is missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"fugue_automation_action_intents:update",
		"fugue_automation_action_intents:delete",
		"fugue_automation_action_dispatches:delete",
		"fugue_automation_action_fencing:delete",
	} {
		if strings.Contains(grants, forbidden) {
			t.Fatalf("append-only intent table unexpectedly requires %q", forbidden)
		}
	}

	schema := strings.Join(postgresSchemaStatements, "\n")
	for _, required := range []string{
		"CREATE TABLE IF NOT EXISTS fugue_automation_action_intents",
		"mode TEXT NOT NULL CHECK (mode = 'shadow')",
		"source TEXT NOT NULL CHECK (source IN ('admin_replay', 'control_loop'))",
		"status TEXT NOT NULL CHECK (status = 'observed')",
		"production_mutation_allowed BOOLEAN NOT NULL DEFAULT FALSE CHECK (production_mutation_allowed = FALSE)",
		"idempotency_key TEXT NOT NULL UNIQUE CHECK (idempotency_key ~ '^sha256:[0-9a-f]{64}$')",
		"updated_at TIMESTAMPTZ NOT NULL CHECK (updated_at = created_at)",
		"idx_fugue_automation_intents_tenant_created",
		"idx_fugue_automation_intents_project_created",
		"idx_fugue_automation_intents_policy_created",
		"idx_fugue_automation_intents_app_created",
		"CREATE TABLE IF NOT EXISTS fugue_automation_action_fencing",
		"CREATE TABLE IF NOT EXISTS fugue_automation_action_dispatches",
		"intent_id TEXT NOT NULL UNIQUE REFERENCES fugue_automation_action_intents(id) ON DELETE CASCADE",
		"wal_hash TEXT NOT NULL CHECK (wal_hash ~ '^sha256:[0-9a-f]{64}$')",
		"status TEXT NOT NULL CHECK (status IN ('held', 'ready', 'claimed', 'executing', 'succeeded', 'failed', 'rolled_back', 'expired', 'cancelled'))",
		"fencing_token BIGINT NOT NULL CHECK (fencing_token > 0)",
		"CHECK (status NOT IN ('claimed', 'executing') OR (btrim(lease_owner) <> '' AND lease_expires_at IS NOT NULL))",
		"idx_fugue_automation_dispatches_subject_status",
		"idx_fugue_automation_dispatches_status_updated",
	} {
		if !strings.Contains(schema, required) {
			t.Fatalf("postgres schema is missing append-only automation intent boundary %q", required)
		}
	}
}

func createAutomationIntentParents(
	t *testing.T,
	stateStore *Store,
	suffix string,
) (model.Tenant, model.Project, model.App, model.AutomationPolicy) {
	t.Helper()
	tenant, err := stateStore.CreateTenant("Automation Intent " + suffix)
	if err != nil {
		t.Fatal(err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "Intent Project", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "intent-app", "", model.AppSpec{
		Image:     "ghcr.io/example/intent:latest",
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	if err != nil {
		t.Fatal(err)
	}
	policyInput := testAutomationPolicy(tenant.ID, project.ID, "Intent Policy")
	policyInput.Scope = model.AutomationScope{Type: model.AutomationScopeApp, ID: app.ID}
	policyInput.Mode = model.GatePolicyModeShadow
	policyInput.Rules[0].Trigger.Source = "app_request_outcomes"
	policyInput.Rules[0].Trigger.InvariantID = "app.request_unavailability"
	policyInput.Rules[0].Trigger.RequiredEvidence = []string{
		"app_request_outcomes",
		"app_revision",
		"app_readiness",
	}
	policyInput.Rules[0].Trigger.MinimumFailureDomains = 1
	policy, err := stateStore.CreateAutomationPolicy(policyInput)
	if err != nil {
		t.Fatal(err)
	}
	return tenant, project, app, policy
}

func testObservedAutomationIntent(
	t *testing.T,
	policy model.AutomationPolicy,
	app model.App,
	now time.Time,
) model.AutomationActionIntent {
	t.Helper()
	revision, err := automationdomain.AppRevisionHash(app.Spec)
	if err != nil {
		t.Fatal(err)
	}
	result, err := automationdomain.EvaluatePolicy(automationdomain.EvaluationInput{
		Policy: policy,
		RuleID: policy.Rules[0].ID,
		Evidence: model.AutomationEvaluationEvidence{
			CollectedBy:     model.AutomationIntentSourceAdminReplay,
			Trusted:         false,
			WindowStartedAt: now.Add(-2 * time.Minute),
			WindowEndedAt:   now,
			RequestOutcomes: []model.AutomationRequestOutcomeAggregate{
				{StatusCode: 503, Count: 2, FailureDomain: "edge-us"},
				{StatusCode: 504, Count: 1, FailureDomain: "edge-us"},
			},
			AppRevision:            revision,
			AppReadiness:           "degraded",
			AppReadinessObservedAt: now.Add(-10 * time.Second),
		},
		Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := automationdomain.NewObservedActionIntent(policy, result)
	if err != nil {
		t.Fatal(err)
	}
	return intent
}
