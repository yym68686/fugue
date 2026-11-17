package store

import (
	"errors"
	"reflect"
	"testing"

	"fugue/internal/model"
	"fugue/internal/pgimage"
)

func imageMigrationOperation(t *testing.T, f managedPostgresResizeFixture) model.Operation {
	t.Helper()
	image, err := pgimage.Target("")
	if err != nil {
		t.Fatal(err)
	}
	app, err := f.store.GetApp(f.app.ID)
	if err != nil {
		t.Fatal(err)
	}
	svc, err := f.store.GetBackingService(f.service.ID)
	if err != nil {
		t.Fatal(err)
	}
	return model.Operation{TenantID: app.TenantID, AppID: app.ID, ServiceID: svc.ID, Type: pgimage.OperationType, DesiredSpec: &model.AppSpec{Image: "untrusted-image", Replicas: 99, Postgres: &model.AppPostgresSpec{Image: image, ImageMigrationConfigHash: PostgresImageMigrationConfigHash(app, svc, image), Database: "untrusted"}}}
}

func TestPostgresImageMigrationOnlyPersistsVerifiedImage(t *testing.T) {
	for _, independent := range []bool{false, true} {
		name := "app-owned"
		if independent {
			name = "independent"
		}
		t.Run(name, func(t *testing.T) { testPostgresImageMigrationCompletion(t, independent) })
	}
}

func independentImageMigrationStoreFixture(t *testing.T) managedPostgresResizeFixture {
	t.Helper()
	f := newManagedPostgresResizeFixture(t)
	app, err := f.store.CreateApp(f.tenant.ID, f.project.ID, "independent-client", "", model.AppSpec{Image: "registry.invalid/client:1", Replicas: 1, RuntimeID: f.app.Spec.RuntimeID})
	if err != nil {
		t.Fatal(err)
	}
	p := model.CloneAppPostgresSpec(f.service.Spec.Postgres)
	p.ServiceName = "independent-postgres"
	svc, err := f.store.CreateBackingService(f.tenant.ID, f.project.ID, "independent-postgres", "", model.BackingServiceSpec{Postgres: p})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.BindBackingService(app.TenantID, app.ID, svc.ID, "postgres", nil); err != nil {
		t.Fatal(err)
	}
	f.app, err = f.store.GetApp(app.ID)
	if err != nil {
		t.Fatal(err)
	}
	f.service = svc
	return f
}

func testPostgresImageMigrationCompletion(t *testing.T, independent bool) {
	var f managedPostgresResizeFixture
	if independent {
		f = independentImageMigrationStoreFixture(t)
	} else {
		f = newManagedPostgresResizeFixture(t)
	}
	before, err := f.store.GetApp(f.app.ID)
	if err != nil {
		t.Fatal(err)
	}
	serviceBefore, err := f.store.GetBackingService(f.service.ID)
	if err != nil {
		t.Fatal(err)
	}
	request := imageMigrationOperation(t, f)
	local := request
	if err := preparePostgresImageMigration(before, &local); err != nil {
		t.Fatalf("local prepare: %v target=%+v", err, before.BackingServices)
	}
	if err := f.store.withLockedState(false, func(state *model.State) error {
		stored := state.Apps[findApp(state, before.ID)]
		hydrateAppBackingServices(state, &stored)
		if PostgresImageMigrationConfigHash(stored, state.BackingServices[findBackingService(state, f.service.ID)], local.DesiredSpec.Postgres.Image) != request.DesiredSpec.Postgres.ImageMigrationConfigHash {
			t.Fatal("public-read defaults changed the config fingerprint")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	op, _, err := f.store.CreateOperationWithResult(request)
	if err != nil {
		t.Fatal(err)
	}
	if op.ExecutionMode != model.ExecutionModeManaged || op.DesiredSpec.Image != before.Spec.Image || op.DesiredSpec.Replicas != before.Spec.Replicas || op.DesiredSpec.Postgres.Database != serviceBefore.Spec.Postgres.Database {
		t.Fatalf("untrusted fields accepted: %+v", op)
	}
	current, _ := f.store.GetBackingService(f.service.ID)
	if !reflect.DeepEqual(serviceBefore.Spec, current.Spec) {
		t.Fatal("image persisted before verification")
	}
	if _, _, err = f.store.CreateOperationWithResult(model.Operation{TenantID: before.TenantID, AppID: before.ID, Type: model.OperationTypeDelete}); !errors.Is(err, ErrConflict) {
		t.Fatalf("delete not interlocked: %v", err)
	}
	if _, err := f.store.UnbindBackingService(before.Bindings[0].ID); !errors.Is(err, ErrConflict) {
		t.Fatalf("unbind escaped lease: %v", err)
	}
	if _, err := f.store.UpdateBackingServiceSpec(f.service.ID, serviceBefore.Spec); !errors.Is(err, ErrConflict) {
		t.Fatalf("service update escaped lease: %v", err)
	}
	if _, err = f.store.CompleteManagedOperation(op.ID, "", "not verified"); !errors.Is(err, ErrConflict) {
		t.Fatalf("completion without runtime proof accepted: %v", err)
	}
	for _, kind := range []string{pgimage.BaselineEvidence, pgimage.IntentEvidence, pgimage.VerifiedEvidence} {
		payload := map[string]any{"pod_uid": "old-primary-uid", "cluster_uid": "cluster-uid", "system_id": "12345", "target_image": op.DesiredSpec.Postgres.Image}
		if kind == pgimage.VerifiedEvidence {
			payload["verified_pod_uid"] = "new-primary-uid"
		}
		if _, err = f.store.RecordOperationEvidence(model.OperationEvidence{TenantID: op.TenantID, OperationID: op.ID, Type: kind, Payload: payload}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = f.store.CompleteManagedOperation(op.ID, "", "verified"); err != nil {
		t.Fatal(err)
	}
	current, _ = f.store.GetBackingService(f.service.ID)
	want := serviceBefore.Spec
	want.Postgres = model.CloneAppPostgresSpec(want.Postgres)
	want.Postgres.Image = op.DesiredSpec.Postgres.Image
	if !reflect.DeepEqual(want, current.Spec) {
		t.Fatalf("migration changed more than image: %+v", current.Spec)
	}
	if current.OwnerAppID != serviceBefore.OwnerAppID {
		t.Fatal("migration changed service ownership")
	}
	after, _ := f.store.GetApp(f.app.ID)
	if !reflect.DeepEqual(before.Spec, after.Spec) || !reflect.DeepEqual(before.Status, after.Status) || !reflect.DeepEqual(before.Bindings, after.Bindings) || !before.UpdatedAt.Equal(after.UpdatedAt) {
		t.Fatal("consumer app was modified")
	}
}

func TestIndependentImageMigrationRejectsBindingAndIdentityDrift(t *testing.T) {
	for _, scenario := range []string{"unbound", "shared", "foreign-owner", "cross-project", "foreign-tenant", "binding-replaced"} {
		t.Run(scenario, func(t *testing.T) {
			f := independentImageMigrationStoreFixture(t)
			request := imageMigrationOperation(t, f)
			mutateResizeFixtureState(t, f.store, func(state *model.State) {
				i := findBackingService(state, f.service.ID)
				switch scenario {
				case "unbound":
					state.ServiceBindings = deleteServiceBindingsByApp(state.ServiceBindings, f.app.ID)
				case "shared":
					state.ServiceBindings = append(state.ServiceBindings, model.ServiceBinding{ID: "another-binding", TenantID: f.tenant.ID, AppID: "another-app", ServiceID: f.service.ID})
				case "foreign-owner":
					state.BackingServices[i].OwnerAppID = "another-app"
				case "cross-project":
					state.BackingServices[i].ProjectID = "another-project"
				case "foreign-tenant":
					state.BackingServices[i].TenantID = "another-tenant"
				case "binding-replaced":
					for i := range state.ServiceBindings {
						if state.ServiceBindings[i].ServiceID == f.service.ID {
							state.ServiceBindings[i].ID = "replacement-binding"
						}
					}
				}
			})
			if _, _, err := f.store.CreateOperationWithResult(request); err == nil {
				t.Fatalf("unsafe %s target accepted", scenario)
			}
		})
	}
}

func TestPostgresImageMigrationRejectsStalePlanAndDeduplicates(t *testing.T) {
	f := newManagedPostgresResizeFixture(t)
	request := imageMigrationOperation(t, f)
	bad := request
	bad.DesiredSpec = cloneAppSpec(request.DesiredSpec)
	bad.DesiredSpec.Postgres.ImageMigrationConfigHash = "stale"
	if _, _, err := f.store.CreateOperationWithResult(bad); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale plan accepted: %v", err)
	}
	op, _, err := f.store.CreateOperationWithResult(request)
	if err != nil {
		t.Fatal(err)
	}
	again, result, err := f.store.CreateOperationWithResult(request)
	if err != nil || again.ID != op.ID || result.Created {
		t.Fatalf("duplicate migration: %+v %v", again, err)
	}
}
