package store

import (
	"context"
	"errors"
	"reflect"
	"testing"

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
	testImportBuildResult(t, newStoreTestDatabase(t, ""))
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
