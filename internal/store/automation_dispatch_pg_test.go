package store

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"fugue/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPostgresAutomationActionDispatchCreateClaimAndFence(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stateStore := &Store{db: db, databaseURL: "postgres://automation-dispatch-test", dbReady: true}

	now := time.Date(2026, 7, 29, 1, 0, 0, 123456789, time.UTC)
	policy, app := postgresAutomationIntentFixture(t, now)
	intent := testObservedAutomationIntent(t, policy, app, now)
	intent.ID = "automation_intent_dispatch_pg"
	intent.CreatedAt = now
	intent.UpdatedAt = now
	dispatch := testAutomationActionDispatch(intent, now)
	dispatch.ID = "automation_dispatch_pg"
	dispatch.Status = model.AutomationActionDispatchStatusReady
	dispatch.FencingToken = 1
	dispatch.Version = 1
	dispatch.CreatedAt = now
	dispatch.UpdatedAt = now
	dispatch.WALHash = automationActionDispatchWALHash(dispatch)
	decisionJSON, err := json.Marshal(dispatch.SafetyDecision)
	if err != nil {
		t.Fatal(err)
	}

	mock.ExpectBegin()
	mock.ExpectExec(`SELECT pg_advisory_xact_lock\(hashtextextended\(\$1, 0\)\)`).
		WithArgs(dispatch.IdempotencyKey).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`(?s)FROM fugue_automation_action_intents\s+WHERE id = \$1 FOR UPDATE`).
		WithArgs(intent.ID).
		WillReturnRows(automationActionIntentSQLRows(t, intent))
	mock.ExpectQuery(`(?s)FROM fugue_automation_action_dispatches\s+WHERE intent_id = \$1\s+FOR UPDATE`).
		WithArgs(intent.ID).
		WillReturnRows(automationActionDispatchSQLRows(t))
	mock.ExpectQuery(`(?s)FROM fugue_automation_action_dispatches\s+WHERE idempotency_key = \$1\s+FOR UPDATE`).
		WithArgs(dispatch.IdempotencyKey).
		WillReturnRows(automationActionDispatchSQLRows(t))
	mock.ExpectExec(`(?s)INSERT INTO fugue_automation_action_fencing .*ON CONFLICT`).
		WithArgs(automationActionDispatchSubjectKey(dispatch), dispatch.TenantID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`(?s)UPDATE fugue_automation_action_fencing\s+SET last_token = last_token \+ 1`).
		WithArgs(automationActionDispatchSubjectKey(dispatch), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"last_token"}).AddRow(int64(1)))
	mock.ExpectQuery(`(?s)INSERT INTO fugue_automation_action_dispatches .*ON CONFLICT DO NOTHING.*RETURNING`).
		WithArgs(
			dispatch.ID, dispatch.IntentID, dispatch.TenantID, dispatch.ProjectID, dispatch.PolicyID,
			dispatch.PolicyGeneration, dispatch.RuleID, dispatch.Scope.Type, dispatch.Scope.ID,
			dispatch.ActionType, dispatch.ContractID, dispatch.TriggerInvariant, dispatch.Subject,
			dispatch.SourceGeneration, dispatch.RollbackTarget, dispatch.IdempotencyKey, dispatch.WALHash,
			decisionJSON, dispatch.Status, int64(1), int64(1), dispatch.ExpiresAt,
			"", nil, nil, "", sqlmock.AnyArg(), sqlmock.AnyArg(), nil, nil,
		).
		WillReturnRows(automationActionDispatchSQLRows(t, dispatch))
	mock.ExpectCommit()

	created, wasCreated, err := stateStore.CreateAutomationActionDispatch(dispatch)
	if err != nil {
		t.Fatalf("create dispatch: %v", err)
	}
	if !wasCreated || created.ID != dispatch.ID || created.FencingToken != 1 {
		t.Fatalf("unexpected created dispatch: %+v created=%t", created, wasCreated)
	}

	// A replay is serialized on the idempotency key and the intent row, so it
	// cannot consume another fencing token.
	mock.ExpectBegin()
	mock.ExpectExec(`SELECT pg_advisory_xact_lock\(hashtextextended\(\$1, 0\)\)`).
		WithArgs(dispatch.IdempotencyKey).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`(?s)FROM fugue_automation_action_intents\s+WHERE id = \$1 FOR UPDATE`).
		WithArgs(intent.ID).
		WillReturnRows(automationActionIntentSQLRows(t, intent))
	mock.ExpectQuery(`(?s)FROM fugue_automation_action_dispatches\s+WHERE intent_id = \$1\s+FOR UPDATE`).
		WithArgs(intent.ID).
		WillReturnRows(automationActionDispatchSQLRows(t, dispatch))
	mock.ExpectCommit()
	replay := dispatch
	replay.SafetyDecision.GeneratedAt = replay.SafetyDecision.GeneratedAt.Add(time.Second)
	reused, wasCreated, err := stateStore.CreateAutomationActionDispatch(replay)
	if err != nil {
		t.Fatalf("replay dispatch: %v", err)
	}
	if wasCreated || reused.ID != dispatch.ID || reused.FencingToken != 1 {
		t.Fatalf("unexpected replay dispatch: %+v created=%t", reused, wasCreated)
	}

	claimedAt := now.Add(2 * time.Second)
	leaseExpires := claimedAt.Add(10 * time.Second)
	claimed := dispatch
	claimed.Status = model.AutomationActionDispatchStatusClaimed
	claimed.FencingToken = 2
	claimed.Version = 2
	claimed.LeaseOwner = "executor-a"
	claimed.LeaseExpiresAt = &leaseExpires
	claimed.ClaimedAt = &claimedAt
	claimed.UpdatedAt = claimedAt
	claimed.WALHash = automationActionDispatchWALHash(claimed)

	mock.ExpectBegin()
	mock.ExpectQuery(`(?s)FROM fugue_automation_action_dispatches\s+WHERE id = \$1\s+FOR UPDATE`).
		WithArgs(dispatch.ID).
		WillReturnRows(automationActionDispatchSQLRows(t, dispatch))
	mock.ExpectQuery(`(?s)FROM fugue_automation_action_fencing\s+WHERE subject_key = \$1\s+FOR UPDATE`).
		WithArgs(automationActionDispatchSubjectKey(dispatch)).
		WillReturnRows(sqlmock.NewRows([]string{"last_token"}).AddRow(int64(1)))
	mock.ExpectExec(`(?s)INSERT INTO fugue_automation_action_fencing .*ON CONFLICT`).
		WithArgs(automationActionDispatchSubjectKey(dispatch), dispatch.TenantID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`(?s)UPDATE fugue_automation_action_fencing\s+SET last_token = last_token \+ 1`).
		WithArgs(automationActionDispatchSubjectKey(dispatch), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"last_token"}).AddRow(int64(2)))
	mock.ExpectQuery(`(?s)UPDATE fugue_automation_action_dispatches\s+SET status = \$2, fencing_token = \$3, version = \$4.*WHERE id = \$1 AND version = \$10.*RETURNING`).
		WithArgs(
			dispatch.ID, claimed.Status, claimed.FencingToken, claimed.Version, claimed.LeaseOwner,
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), claimed.WALHash, int64(1),
		).
		WillReturnRows(automationActionDispatchSQLRows(t, claimed))
	mock.ExpectCommit()

	gotClaim, didClaim, err := stateStore.ClaimAutomationActionDispatch(
		dispatch.ID, "executor-a", claimedAt, 10*time.Second,
	)
	if err != nil {
		t.Fatalf("claim dispatch: %v", err)
	}
	if !didClaim || gotClaim.FencingToken != 2 || gotClaim.Version != 2 {
		t.Fatalf("unexpected claim: %+v claimed=%t", gotClaim, didClaim)
	}

	mock.ExpectQuery(`(?s)SELECT d\.tenant_id .*FROM fugue_automation_action_dispatches d\s+WHERE d\.id = \$1`).
		WithArgs(dispatch.ID).
		WillReturnRows(sqlmock.NewRows([]string{
			"subject_key", "status", "fencing_token", "version", "expires_at", "lease_expires_at", "last_token",
		}).AddRow(
			automationActionDispatchSubjectKey(claimed), claimed.Status, claimed.FencingToken, claimed.Version,
			claimed.ExpiresAt, claimed.LeaseExpiresAt, claimed.FencingToken,
		))
	if err := stateStore.ValidateAutomationActionDispatchFence(
		claimed.ID, claimed.FencingToken, claimed.Version, claimedAt.Add(time.Second),
	); err != nil {
		t.Fatalf("validate current fence: %v", err)
	}

	mock.ExpectQuery(`(?s)SELECT d\.tenant_id .*FROM fugue_automation_action_dispatches d\s+WHERE d\.id = \$1`).
		WithArgs(dispatch.ID).
		WillReturnRows(sqlmock.NewRows([]string{
			"subject_key", "status", "fencing_token", "version", "expires_at", "lease_expires_at", "last_token",
		}).AddRow(
			automationActionDispatchSubjectKey(claimed), claimed.Status, claimed.FencingToken, claimed.Version,
			claimed.ExpiresAt, claimed.LeaseExpiresAt, claimed.FencingToken,
		))
	if err := stateStore.ValidateAutomationActionDispatchFence(
		claimed.ID, 1, 1, claimedAt.Add(time.Second),
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale fence error=%v, want conflict", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func automationActionDispatchSQLRows(
	t *testing.T,
	dispatches ...model.AutomationActionDispatch,
) *sqlmock.Rows {
	t.Helper()
	rows := sqlmock.NewRows([]string{
		"id", "intent_id", "tenant_id", "project_id", "policy_id", "policy_generation", "rule_id",
		"scope_type", "scope_id", "action_type", "contract_id", "trigger_invariant", "subject",
		"source_generation", "rollback_target", "idempotency_key", "wal_hash",
		"safety_decision_json", "status", "fencing_token", "version", "expires_at",
		"lease_owner", "lease_expires_at", "cooldown_until", "last_error",
		"created_at", "updated_at", "claimed_at", "completed_at",
	})
	for _, dispatch := range dispatches {
		safetyJSON, err := json.Marshal(dispatch.SafetyDecision)
		if err != nil {
			t.Fatal(err)
		}
		rows.AddRow(
			dispatch.ID, dispatch.IntentID, dispatch.TenantID, dispatch.ProjectID, dispatch.PolicyID,
			dispatch.PolicyGeneration, dispatch.RuleID, dispatch.Scope.Type, dispatch.Scope.ID,
			dispatch.ActionType, dispatch.ContractID, dispatch.TriggerInvariant, dispatch.Subject,
			dispatch.SourceGeneration, dispatch.RollbackTarget, dispatch.IdempotencyKey, dispatch.WALHash,
			safetyJSON, dispatch.Status, dispatch.FencingToken, dispatch.Version, dispatch.ExpiresAt,
			dispatch.LeaseOwner, dispatch.LeaseExpiresAt, dispatch.CooldownUntil, dispatch.LastError,
			dispatch.CreatedAt, dispatch.UpdatedAt, dispatch.ClaimedAt, dispatch.CompletedAt,
		)
	}
	return rows
}
