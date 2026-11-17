package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/schemamigrate"
)

func importBuildFixture(t *testing.T, s *Store) (model.App, model.Operation) {
	t.Helper()
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := s.CreateTenant(model.NewID("build_result_tenant"))
	if err != nil {
		t.Fatal(err)
	}
	if s.db != nil {
		t.Cleanup(func() {
			if _, err := s.db.Exec(`DELETE FROM fugue_tenants WHERE id=$1`, tenant.ID); err != nil {
				t.Error(err)
			}
		})
	}
	project, err := s.CreateProject(tenant.ID, "release", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.CreateImportedApp(tenant.ID, project.ID, "service", "", model.AppSpec{Image: "registry.example/service:old", Replicas: 1, RuntimeID: model.DefaultManagedRuntimeID}, model.AppSource{Type: model.AppSourceTypeGitHubPublic, RepoURL: "https://github.com/example/service", CommitSHA: "old"}, model.AppRoute{})
	if err != nil {
		t.Fatal(err)
	}
	source := *app.Source
	source.CommitSHA = "next"
	op, err := s.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeImport, DesiredSpec: &app.Spec, DesiredSource: &source})
	if err != nil {
		t.Fatal(err)
	}
	op, claimed, err := s.TryClaimPendingOperation(op.ID)
	if err != nil || !claimed {
		t.Fatal(err)
	}
	return app, op
}

func testImportBuildResult(t *testing.T, s *Store) {
	t.Helper()
	app, op := importBuildFixture(t, s)
	before, _ := s.GetApp(app.ID)
	spec, source := *cloneAppSpec(op.DesiredSpec), *model.CloneAppSource(op.DesiredSource)
	spec.Image, source.ResolvedImageRef = "registry.example/service@sha256:verified", "registry.example/service:build"
	_, err := s.RecordImportBuildResult(op, spec, source, &source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.FailManagedOperationGuarded(op, "queue deploy after import: billing balance depleted"); err != nil {
		t.Fatal(err)
	}
	persisted, err := s.GetOperation(op.ID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.DesiredSpec.Image != spec.Image || persisted.DesiredSource.ResolvedImageRef != source.ResolvedImageRef || persisted.DesiredOriginSource.CommitSHA != source.CommitSHA || persisted.ConfigBaseSpec == nil {
		t.Fatal("build identity lost after admission failure", persisted)
	}
	after, _ := s.GetApp(app.ID)
	if !reflect.DeepEqual(before.Spec, after.Spec) || !reflect.DeepEqual(before.Source, after.Source) {
		t.Fatal("saving build changed serving intent")
	}
	if _, err := s.RecordImportBuildResult(op, spec, source, &source); !errors.Is(err, ErrConflict) {
		t.Fatal("stale worker rewrote terminal result", err)
	}
}

func TestImportBuildResultSurvivesAdmissionFailure(t *testing.T) {
	testImportBuildResult(t, New(filepath.Join(t.TempDir(), "state.json")))
}

func TestImportBuildResultPostgres(t *testing.T) {
	s := billingBatchPGStore(t)
	prepareImportBuildPostgres(t, s)
	testImportBuildResult(t, s)
}

func prepareImportBuildPostgres(t *testing.T, s *Store) {
	t.Helper()
	if _, err := s.db.Exec(postgresSchemaBatch()); err != nil {
		t.Fatal(err)
	}
	if err := schemamigrate.MigrateEdgeInstanceFencing(context.Background(), s.databaseURL); err != nil {
		t.Fatal(err)
	}
}

func TestImportBuildResultFencesCancellationAndNewAttempts(t *testing.T) {
	for _, change := range []string{"cancel", "attempt", "intent"} {
		t.Run(change, func(t *testing.T) {
			s := New(filepath.Join(t.TempDir(), "state.json"))
			_, op := importBuildFixture(t, s)
			switch change {
			case "cancel":
				if err := s.withLockedState(true, func(state *model.State) error {
					state.Operations[findOperation(state, op.ID)].Status = model.OperationStatusCanceled
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			case "attempt":
				if _, err := s.RequeueManagedOperation(op.ID, "retry"); err != nil {
					t.Fatal(err)
				}
				time.Sleep(time.Millisecond)
				if _, _, err := s.TryClaimPendingOperation(op.ID); err != nil {
					t.Fatal(err)
				}
			case "intent":
				if err := s.withLockedState(true, func(state *model.State) error {
					state.Operations[findOperation(state, op.ID)].DesiredSpec.Replicas++
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			spec, source := *cloneAppSpec(op.DesiredSpec), *model.CloneAppSource(op.DesiredSource)
			spec.Image, source.ResolvedImageRef = "registry.example/service:build", "registry.example/service:build"
			if _, err := s.RecordImportBuildResult(op, spec, source, &source); !errors.Is(err, ErrConflict) {
				t.Fatal("stale build admitted", change, err)
			}
		})
	}
}

func TestDeployBillingPreviewDoesNotWriteAndMatchesAdmission(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "state.json"))
	app, op := importBuildFixture(t, s)
	if _, err := s.FailManagedOperationGuarded(op, "fixture finished"); err != nil {
		t.Fatal(err)
	}
	resources := model.DefaultManagedAppResources()
	if _, err := s.UpdateTenantBilling(app.TenantID, model.BillingResourceSpec{CPUMilliCores: 2000, MemoryMebibytes: 4096, StorageGibibytes: 20}); err != nil {
		t.Fatal(err)
	}
	spec := app.Spec
	spec.Resources = &resources
	spec.Replicas = 2
	if err := s.withLockedState(true, func(state *model.State) error {
		record := ensureTenantBillingRecord(state, app.TenantID, time.Now().UTC())
		record.BalanceMicroCents = 0
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(s.path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CheckDeployBilling(context.Background(), app.ID, spec); !errors.Is(err, ErrBillingBalanceDepleted) {
		t.Fatal("preview accepted unfunded deploy", err)
	}
	after, _ := os.ReadFile(s.path)
	if string(before) != string(after) {
		t.Fatal("preview wrote state")
	}
	if _, err := s.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &spec}); !errors.Is(err, ErrBillingBalanceDepleted) {
		t.Fatal("preview differs from acceptance", err)
	}
	if _, err := s.TopUpTenantBilling(app.TenantID, 1000, "fund release"); err != nil {
		t.Fatal(err)
	}
	if err := s.CheckDeployBilling(context.Background(), app.ID, spec); err != nil {
		t.Fatal("funded preview blocked", err)
	}
	if _, err := s.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, Type: model.OperationTypeDeploy, DesiredSpec: &spec}); err != nil {
		t.Fatal(err)
	}
}
