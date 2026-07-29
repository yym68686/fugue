package store

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestAutomationActionDispatchJSONIsIdempotentAndFenced(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Microsecond)
	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	tenant, project, app, policy := createAutomationIntentParents(t, stateStore, "Dispatch")
	intent := testObservedAutomationIntent(t, policy, app, now)
	intent, _, err := stateStore.CreateAutomationActionIntent(intent)
	if err != nil {
		t.Fatalf("create intent: %v", err)
	}

	dispatch := testAutomationActionDispatch(intent, now)
	created, wasCreated, err := stateStore.CreateAutomationActionDispatch(dispatch)
	if err != nil {
		t.Fatalf("create dispatch: %v", err)
	}
	if !wasCreated || created.FencingToken != 1 ||
		created.Version != 1 ||
		created.Status != model.AutomationActionDispatchStatusReady {
		t.Fatalf("unexpected dispatch create: %+v created=%t", created, wasCreated)
	}

	reused, wasCreated, err := stateStore.CreateAutomationActionDispatch(dispatch)
	if err != nil {
		t.Fatalf("reuse dispatch: %v", err)
	}
	if wasCreated || reused.ID != created.ID || reused.FencingToken != created.FencingToken {
		t.Fatalf("dispatch was not idempotently reused: first=%+v second=%+v created=%t", created, reused, wasCreated)
	}

	claimed, didClaim, err := stateStore.ClaimAutomationActionDispatch(
		created.ID,
		"executor-a",
		now.Add(time.Second),
		10*time.Second,
	)
	if err != nil {
		t.Fatalf("claim dispatch: %v", err)
	}
	if !didClaim || claimed.Status != model.AutomationActionDispatchStatusClaimed ||
		claimed.FencingToken != 2 || claimed.Version != 2 {
		t.Fatalf("unexpected claimed dispatch: %+v claimed=%t", claimed, didClaim)
	}
	if _, didClaim, err := stateStore.ClaimAutomationActionDispatch(
		created.ID,
		"executor-b",
		now.Add(2*time.Second),
		10*time.Second,
	); err != nil {
		t.Fatalf("active lease claim: %v", err)
	} else if didClaim {
		t.Fatal("active dispatch lease was claimed twice")
	}
	if err := stateStore.ValidateAutomationActionDispatchFence(
		claimed.ID,
		claimed.FencingToken,
		claimed.Version,
		now.Add(2*time.Second),
	); err != nil {
		t.Fatalf("current fence was rejected: %v", err)
	}

	reclaimed, didClaim, err := stateStore.ClaimAutomationActionDispatch(
		created.ID,
		"executor-b",
		now.Add(12*time.Second),
		10*time.Second,
	)
	if err != nil {
		t.Fatalf("reclaim expired lease: %v", err)
	}
	if !didClaim || reclaimed.FencingToken != 3 || reclaimed.Version != 3 {
		t.Fatalf("unexpected reclaimed dispatch: %+v claimed=%t", reclaimed, didClaim)
	}
	if err := stateStore.ValidateAutomationActionDispatchFence(
		claimed.ID,
		claimed.FencingToken,
		claimed.Version,
		now.Add(12*time.Second),
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale fence validation error=%v, want conflict", err)
	}
	if err := stateStore.ValidateAutomationActionDispatchFence(
		reclaimed.ID,
		reclaimed.FencingToken,
		reclaimed.Version,
		now.Add(13*time.Second),
	); err != nil {
		t.Fatalf("new fence validation failed: %v", err)
	}

	got, err := stateStore.GetAutomationActionDispatch(created.ID, tenant.ID, false)
	if err != nil {
		t.Fatalf("get dispatch: %v", err)
	}
	if got.LeaseOwner != "executor-b" || got.ProjectID != project.ID || got.Scope.ID != app.ID {
		t.Fatalf("dispatch identity changed: %+v", got)
	}
	listed, err := stateStore.ListAutomationActionDispatches(AutomationActionDispatchFilter{
		TenantID:  tenant.ID,
		ProjectID: project.ID,
		AppID:     app.ID,
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("list dispatches: %v", err)
	}
	if len(listed) != 1 || listed[0].ID != created.ID {
		t.Fatalf("unexpected dispatch list: %+v", listed)
	}
}

func TestAutomationActionDispatchJSONRejectsStaleIntentAndExpires(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Microsecond)
	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	_, _, app, policy := createAutomationIntentParents(t, stateStore, "DispatchReject")
	intent := testObservedAutomationIntent(t, policy, app, now)
	intent, _, err := stateStore.CreateAutomationActionIntent(intent)
	if err != nil {
		t.Fatalf("create intent: %v", err)
	}
	dispatch := testAutomationActionDispatch(intent, now)
	dispatch.TenantID = "wrong-tenant"
	if _, _, err := stateStore.CreateAutomationActionDispatch(dispatch); !errors.Is(err, ErrConflict) {
		t.Fatalf("tenant mismatch error=%v, want conflict", err)
	}

	dispatch = testAutomationActionDispatch(intent, now)
	dispatch.ExpiresAt = now.Add(2 * time.Second)
	created, _, err := stateStore.CreateAutomationActionDispatch(dispatch)
	if err != nil {
		t.Fatalf("create expired dispatch: %v", err)
	}
	if _, didClaim, err := stateStore.ClaimAutomationActionDispatch(
		created.ID,
		"executor",
		now.Add(3*time.Second),
		10*time.Second,
	); err != nil {
		t.Fatalf("claim expired dispatch: %v", err)
	} else if didClaim {
		t.Fatal("expired dispatch was claimed")
	}
	got, err := stateStore.GetAutomationActionDispatch(created.ID, intent.TenantID, false)
	if err != nil {
		t.Fatalf("get expired dispatch: %v", err)
	}
	if got.Status != model.AutomationActionDispatchStatusExpired ||
		got.CompletedAt == nil ||
		!got.CompletedAt.Equal(now.Add(3*time.Second)) {
		t.Fatalf("expired dispatch state=%+v, want terminal timestamp", got)
	}
}

func TestAutomationActionDispatchCreateRejectsUnsafeReadyOverrideAndKeepsFirstDecision(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Microsecond)
	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	_, _, app, policy := createAutomationIntentParents(t, stateStore, "DispatchStatus")
	intent := testObservedAutomationIntent(t, policy, app, now)
	intent, _, err := stateStore.CreateAutomationActionIntent(intent)
	if err != nil {
		t.Fatalf("create intent: %v", err)
	}

	unsafe := testAutomationActionDispatch(intent, now)
	unsafe.Status = model.AutomationActionDispatchStatusReady
	unsafe.SafetyDecision.Allowed = false
	unsafe.SafetyDecision.ProductionMutationAllowed = false
	if _, _, err := stateStore.CreateAutomationActionDispatch(unsafe); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("unsafe ready override error=%v, want invalid input", err)
	}

	first := testAutomationActionDispatch(intent, now)
	first.SafetyDecision.GeneratedAt = now
	created, createdNew, err := stateStore.CreateAutomationActionDispatch(first)
	if err != nil || !createdNew {
		t.Fatalf("create first decision: created=%t err=%v", createdNew, err)
	}
	replay := first
	replay.SafetyDecision.GeneratedAt = now.Add(time.Second)
	replay.SafetyDecision.ExpiresAt = automationDispatchTestTimePtr(now.Add(2 * time.Minute))
	reused, reusedNew, err := stateStore.CreateAutomationActionDispatch(replay)
	if err != nil {
		t.Fatalf("replay same intent: %v", err)
	}
	if reusedNew || reused.ID != created.ID ||
		!reused.SafetyDecision.GeneratedAt.Equal(created.SafetyDecision.GeneratedAt) {
		t.Fatalf("first durable decision was not retained: created=%+v reused=%+v new=%t", created, reused, reusedNew)
	}
}

func TestAutomationActionDispatchCreateRejectsUnknownStateAndUnboundedSafetyTTL(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Microsecond)
	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	_, _, app, policy := createAutomationIntentParents(t, stateStore, "DispatchValidation")
	intent := testObservedAutomationIntent(t, policy, app, now)
	intent, _, err := stateStore.CreateAutomationActionIntent(intent)
	if err != nil {
		t.Fatalf("create intent: %v", err)
	}

	unknown := testAutomationActionDispatch(intent, now)
	unknown.Status = "running"
	if _, _, err := stateStore.CreateAutomationActionDispatch(unknown); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("unknown dispatch status error=%v, want invalid input", err)
	}

	unbounded := testAutomationActionDispatch(intent, now)
	safetyExpiry := now.Add(time.Minute)
	unbounded.SafetyDecision.ExpiresAt = &safetyExpiry
	unbounded.ExpiresAt = now.Add(2 * time.Minute)
	if _, _, err := stateStore.CreateAutomationActionDispatch(unbounded); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("dispatch outliving safety decision error=%v, want invalid input", err)
	}
}

func testAutomationActionDispatch(
	intent model.AutomationActionIntent,
	now time.Time,
) model.AutomationActionDispatch {
	return model.AutomationActionDispatch{
		IntentID:         intent.ID,
		TenantID:         intent.TenantID,
		ProjectID:        intent.ProjectID,
		PolicyID:         intent.PolicyID,
		PolicyGeneration: intent.PolicyGeneration,
		RuleID:           intent.RuleID,
		Scope:            intent.Scope,
		ActionType:       "restart_app",
		ContractID:       "app.restart",
		TriggerInvariant: "app.request_unavailability",
		Subject:          intent.Scope.ID,
		SourceGeneration: intent.Evidence.AppRevision,
		RollbackTarget:   intent.RollbackTarget,
		IdempotencyKey:   intent.IdempotencyKey,
		SafetyDecision: model.ActionSafetyDecision{
			Pass:                      true,
			Allowed:                   true,
			ProductionMutationAllowed: true,
			EffectiveMode:             model.GatePolicyModeEnforced,
			ContractID:                "app.restart",
			GatePolicyID:              "automation.app-restart",
			Subject:                   intent.Scope.ID,
			GeneratedAt:               now,
		},
		Status:    model.AutomationActionDispatchStatusReady,
		ExpiresAt: intent.ExpiresAt,
	}
}

func automationDispatchTestTimePtr(value time.Time) *time.Time {
	value = value.UTC()
	return &value
}
