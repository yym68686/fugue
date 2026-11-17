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
	f := newManagedPostgresResizeFixture(t)
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
	after, _ := f.store.GetApp(f.app.ID)
	if !reflect.DeepEqual(before.Spec, after.Spec) || !reflect.DeepEqual(before.Status, after.Status) || !before.UpdatedAt.Equal(after.UpdatedAt) {
		t.Fatal("consumer app was modified")
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
