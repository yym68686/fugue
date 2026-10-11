package store

import (
	"errors"
	"fugue/internal/model"
	"path/filepath"
	"testing"
	"time"
)

func finalizationFixture(t *testing.T) (*Store, model.App, model.Operation) {
	t.Helper()
	s := New(filepath.Join(t.TempDir(), "state.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := s.CreateTenant("Recovery Tenant")
	if err != nil {
		t.Fatal(err)
	}
	project, err := s.CreateProject(tenant.ID, "recovery", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "service", "", model.AppSpec{Image: "registry.example/service:previous", Replicas: 1})
	if err != nil {
		t.Fatal(err)
	}
	desired := app.Spec
	desired.Image = "registry.example/service:next"
	op, err := s.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &desired})
	if err != nil {
		t.Fatal(err)
	}
	op, ok, err := s.TryClaimPendingOperation(op.ID)
	if err != nil || !ok {
		t.Fatal(err)
	}
	app, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	return s, app, op
}
func TestGuardedFinalizationCompletesAndNeverOverwritesCancellation(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(map[bool]string{false: "ready", true: "cancelled"}[cancel], func(t *testing.T) {
			s, app, op := finalizationFixture(t)
			if cancel {
				if _, err := s.CancelOperation(op.ID, "user cancelled"); err != nil {
					t.Fatal(err)
				}
			}
			_, err := s.CompleteManagedOperationGuarded(op, &app, "", "observed ready", op.DesiredSpec, nil, nil)
			if cancel && !errors.Is(err, ErrConflict) {
				t.Fatal("cancelled task overwritten", err)
			}
			if !cancel && err != nil {
				t.Fatal(err)
			}
			current, _ := s.GetOperation(op.ID)
			if cancel && current.Status != model.OperationStatusCanceled {
				t.Fatal(current.Status)
			}
			if _, err = s.FailManagedOperationGuarded(op, "late failure"); !errors.Is(err, ErrConflict) {
				t.Fatal("late failure overwrote terminal", err)
			}
		})
	}
}
func TestGuardedFinalizationRejectsNewAttemptAndChangedConfiguration(t *testing.T) {
	for _, change := range []string{"attempt", "config", "intent"} {
		t.Run(change, func(t *testing.T) {
			s, app, op := finalizationFixture(t)
			if change == "attempt" {
				s.RequeueManagedOperation(op.ID, "new attempt")
				time.Sleep(time.Millisecond)
				s.TryClaimPendingOperation(op.ID)
			} else {
				err := s.withLockedState(true, func(state *model.State) error {
					if change == "config" {
						state.Apps[findApp(state, app.ID)].Spec.Env = map[string]string{"NEW": "keep"}
					} else {
						state.Operations[findOperation(state, op.ID)].DesiredSpec.Replicas = 2
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.CompleteManagedOperationGuarded(op, &app, "", "late completion", op.DesiredSpec, nil, nil); !errors.Is(err, ErrConflict) {
				t.Fatal("stale observation accepted", err)
			}
			if change == "attempt" {
				if _, err := s.RequeueManagedOperationGuarded(op, "late requeue"); !errors.Is(err, ErrConflict) {
					t.Fatal("new attempt requeued by old worker", err)
				}
				if _, err := s.FailManagedOperationGuarded(op, "late failure"); !errors.Is(err, ErrConflict) {
					t.Fatal("new attempt failed by old worker", err)
				}
			}
		})
	}
}
func TestFinalizationGuardIgnoresProgressAndObservationButPreservesBackingIntent(t *testing.T) {
	s, app, op := finalizationFixture(t)
	current := app
	now := time.Now()
	app.BackingServices = []model.BackingService{{ID: "database", Spec: model.BackingServiceSpec{}, CurrentRuntimeReadyAt: &now}}
	current.BackingServices = []model.BackingService{{ID: "database", Spec: model.BackingServiceSpec{}}}
	guard := &operationFinalizationGuard{Operation: op, App: &app}
	observed := op
	observed.UpdatedAt = time.Now().Add(time.Second)
	observed.ResultMessage = "still alive"
	if err := validateOperationFinalization(observed, current, guard); err != nil {
		t.Fatal(err)
	}
	s.UpdateOperationProgress(op.ID, "still alive")
	if _, err := s.FailManagedOperationGuarded(op, "actual failure"); err != nil {
		t.Fatal(err)
	}
}
func TestOperationStartedAtDurablePrecision(t *testing.T) {
	a := time.Date(2026, 1, 1, 0, 0, 0, 123456789, time.UTC)
	b := a.Truncate(time.Microsecond).In(time.FixedZone("offset", 3600))
	if !sameOperationStartedAt(&a, &b) {
		t.Fatal("PostgreSQL precision incorrectly fenced current worker")
	}
	b = b.Add(time.Microsecond)
	if sameOperationStartedAt(&a, &b) {
		t.Fatal("different attempt accepted")
	}
}

func TestGuardedFinalizationPostgres(t *testing.T) {
	s := billingBatchPGStore(t)
	tenant := billingBatchPGTenant(t, s)
	project, err := s.CreateProject(tenant.ID, "recovery", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "service", "", model.AppSpec{Image: "registry.example/service:old", Replicas: 1, RuntimeID: model.DefaultManagedRuntimeID, Env: map[string]string{"CONFIG": "retain"}})
	if err != nil {
		t.Fatal(err)
	}
	next := *cloneAppSpec(&app.Spec)
	next.Image = "registry.example/service:next"
	op, err := s.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &next})
	if err != nil {
		t.Fatal(err)
	}
	op, claimed, err := s.TryClaimPendingOperation(op.ID)
	if err != nil || !claimed {
		t.Fatal(err)
	}
	baseline, err := s.GetApp(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.CompleteManagedOperationGuarded(op, &baseline, "", "runtime verified", &next, nil, nil); err != nil {
		t.Fatal(err)
	}
	persisted, err := s.GetApp(app.ID)
	if err != nil || persisted.Spec.Image != next.Image || persisted.Spec.Env["CONFIG"] != "retain" {
		t.Fatalf("bad completion: %v %+v", err, persisted.Spec)
	}
	if _, err = s.FailManagedOperationGuarded(op, "late failure"); !errors.Is(err, ErrConflict) {
		t.Fatal("completed task overwritten", err)
	}
	next.Image = "registry.example/service:another"
	op, err = s.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &next})
	if err != nil {
		t.Fatal(err)
	}
	op, claimed, err = s.TryClaimPendingOperation(op.ID)
	if err != nil || !claimed {
		t.Fatal(err)
	}
	baseline, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(`UPDATE fugue_apps SET spec_json=jsonb_set(spec_json,'{env,CONFIG}','"changed"') WHERE id=$1`, app.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CompleteManagedOperationGuarded(op, &baseline, "", "stale observation", &next, nil, nil); !errors.Is(err, ErrConflict) {
		t.Fatal("configuration fence failed", err)
	}
	if _, err = s.CancelOperation(op.ID, "user cancelled"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.FailManagedOperationGuarded(op, "late failure"); !errors.Is(err, ErrConflict) {
		t.Fatal("cancellation overwritten", err)
	}
}
