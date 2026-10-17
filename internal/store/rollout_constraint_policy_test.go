package store

import (
	"errors"
	"fugue/internal/model"
	"path/filepath"
	"reflect"
	"testing"
)

func TestRolloutConstraintPolicyOperation(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "state.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	testRolloutConstraintPolicyOperation(t, s)
}

func TestRolloutConstraintPolicyOperationPostgres(t *testing.T) {
	s := billingBatchPGStore(t)
	if err := s.ensureDatabaseReady(); err != nil {
		t.Fatal(err)
	}
	testRolloutConstraintPolicyOperation(t, s)
}

func testRolloutConstraintPolicyOperation(t *testing.T, s *Store) {
	t.Helper()
	tenant, err := s.CreateTenant(model.NewID("constraint-policy"))
	if err != nil {
		t.Fatal(err)
	}
	project, err := s.CreateProject(tenant.ID, "policy", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "service", "", model.AppSpec{
		Image: "registry.example/service:v1", Replicas: 1, Ports: []int{8080},
		Continuity: &model.AppContinuityPolicy{ZeroDowntime: &model.AppZeroDowntimePolicy{Enabled: true, Mode: model.AppZeroDowntimeModeSafe, Strategy: model.AppZeroDowntimeStrategyStableCandidate}},
	})
	if err != nil {
		t.Fatal(err)
	}
	desired := app.Spec
	desired.Continuity = model.CloneAppContinuityPolicy(app.Spec.Continuity)
	desired.Continuity.ZeroDowntime.Gate = &model.AppReleaseGatePolicy{LatencyCohortModel: true}
	op, err := s.CreateOperation(model.Operation{AppID: app.ID, TenantID: app.TenantID, Type: model.OperationTypeDeploy, ExecutionMode: model.ExecutionModeManaged, DesiredSpec: &desired})
	if err != nil {
		t.Fatal(err)
	}
	before, err := s.GetApp(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before.Status, app.Status) {
		t.Fatal("policy enqueue changed runtime status")
	}
	op, claimed, err := s.TryClaimPendingOperation(op.ID)
	if err != nil || !claimed {
		t.Fatal("claim", err)
	}
	before, err = s.GetApp(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before.Status, app.Status) {
		t.Fatal("policy claim changed runtime status")
	}
	releases, err := s.ListAppReleases(model.AppReleaseFilter{TenantID: app.TenantID, AppID: app.ID, PlatformAdmin: true, IncludeRetired: true})
	if err != nil {
		t.Fatal(err)
	}
	completed, err := s.CompleteRolloutConstraintPolicyOperation(op, before, "policy recorded")
	if err != nil || completed.Status != model.OperationStatusCompleted {
		t.Fatal("complete", err)
	}
	after, err := s.GetApp(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after.Spec, desired) {
		t.Fatal("policy not stored exactly")
	}
	wantStatus := before.Status
	wantStatus.LastOperationID, wantStatus.LastMessage = op.ID, "policy recorded"
	if !reflect.DeepEqual(after.Status, wantStatus) {
		t.Fatal("configuration fabricated runtime facts", after.Status, wantStatus)
	}
	nextReleases, err := s.ListAppReleases(model.AppReleaseFilter{TenantID: app.TenantID, AppID: app.ID, PlatformAdmin: true, IncludeRetired: true})
	if err != nil || !reflect.DeepEqual(releases, nextReleases) {
		t.Fatal("configuration changed executable release history", err)
	}
	if _, err := s.CompleteRolloutConstraintPolicyOperation(op, before, "late commit"); !errors.Is(err, ErrConflict) {
		t.Fatal("replay bypassed operation fence", err)
	}

	// A concurrent configuration edit cannot be overwritten by the old reader.
	second := after.Spec
	second.Continuity = model.CloneAppContinuityPolicy(after.Spec.Continuity)
	second.Continuity.ZeroDowntime.RetireGraceSeconds = 900
	op2, err := s.CreateOperation(model.Operation{AppID: app.ID, TenantID: app.TenantID, Type: model.OperationTypeDeploy, ExecutionMode: model.ExecutionModeManaged, DesiredSpec: &second})
	if err != nil {
		t.Fatal(err)
	}
	op2, claimed, err = s.TryClaimPendingOperation(op2.ID)
	if err != nil || !claimed {
		t.Fatal(err)
	}
	stale := after
	stale.Spec.Image = "registry.example/service:stale"
	if _, err := s.CompleteRolloutConstraintPolicyOperation(op2, stale, "stale"); !errors.Is(err, ErrConflict) {
		t.Fatal("stale configuration accepted", err)
	}
	if _, err := s.CancelOperation(op2.ID, "cancel"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompleteRolloutConstraintPolicyOperation(op2, after, "cancelled"); !errors.Is(err, ErrConflict) {
		t.Fatal("cancelled policy completed", err)
	}
}

func TestPolicyLifecyclePreservesRuntimeFactsAcrossCodeAdvance(t *testing.T) {
	for _, lifecycle := range []string{"inflight", "failed", "canceled"} {
		t.Run(lifecycle, func(t *testing.T) {
			base := model.AppSpec{Image: "registry.example/service:v1", Replicas: 2, Continuity: &model.AppContinuityPolicy{
				ZeroDowntime: &model.AppZeroDowntimePolicy{Enabled: true, Mode: model.AppZeroDowntimeModeSafe},
			}}
			desired := base
			desired.Continuity = model.CloneAppContinuityPolicy(base.Continuity)
			desired.Continuity.ZeroDowntime.Gate = &model.AppReleaseGatePolicy{LatencyCohortModel: true}
			op := model.Operation{ID: "policy", AppID: "service", Type: model.OperationTypeDeploy, Status: model.OperationStatusRunning, DesiredSpec: &desired, ConfigBaseSpec: &base}
			app := model.App{ID: op.AppID, Spec: base, Status: model.AppStatus{Phase: "degraded", CurrentReplicas: 1, CurrentRuntimeID: "runtime-observed"}}
			app.Spec.Image = "registry.example/service:v2"
			fileState := model.State{Apps: []model.App{app}}
			pgApp := app
			switch lifecycle {
			case "inflight":
				if err := applyInFlightOperationToApp(&fileState, &op); err != nil {
					t.Fatal(err)
				}
				if err := applyInFlightOperationToAppModel(&pgApp, &op); err != nil {
					t.Fatal(err)
				}
			case "failed":
				op.Status = model.OperationStatusFailed
				applyFailedOperationToApp(&fileState, &op)
				applyFailedOperationToAppModel(&pgApp, &op)
			case "canceled":
				op.Status = model.OperationStatusCanceled
				applyCanceledOperationToApp(&fileState, &op)
				applyCanceledOperationToAppModel(&pgApp, &op)
			}
			for _, got := range []model.App{pgApp, fileState.Apps[0]} {
				if got.Status.Phase != app.Status.Phase || got.Status.CurrentReplicas != app.Status.CurrentReplicas || got.Status.CurrentRuntimeID != app.Status.CurrentRuntimeID || !reflect.DeepEqual(got.Spec, app.Spec) {
					t.Fatal("policy lifecycle changed concurrent executable or runtime facts", got)
				}
			}
		})
	}
}
