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
		for _, resized := range []bool{false, true} {
			suffix := "/image-only"
			if resized {
				suffix = "/applied-resize"
			}
			t.Run(name+suffix, func(t *testing.T) { testPostgresImageMigrationCompletion(t, independent, resized) })
		}
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

func testPostgresImageMigrationCompletion(t *testing.T, independent, resized bool) {
	var f managedPostgresResizeFixture
	if independent {
		f = independentImageMigrationStoreFixture(t)
	} else {
		f = newManagedPostgresResizeFixture(t)
	}
	if resized {
		mutatePostgresResizeService(t, f, func(service *model.BackingService) {
			target := managedPostgresResizeTarget()
			service.Spec.Postgres.RuntimeResources = &target
		})
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
	if PostgresImageMigrationConfigHash(before, serviceBefore, local.DesiredSpec.Postgres.Image) != request.DesiredSpec.Postgres.ImageMigrationConfigHash {
		t.Fatal("public-read defaults changed the config fingerprint")
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
		if resized {
			payload["restart_resource_target"] = serviceBefore.Spec.Postgres.RuntimeResources
			payload["restart_spec_hash"] = "verified-target-spec"
			payload["restart_resources"] = map[string]any{"requests": map[string]any{"memory": "640Mi", "cpu": "250m"}, "limits": map[string]any{"memory": "1024Mi", "cpu": "500m"}}
			payload["resources_hash"] = pgimage.Hash(payload["restart_resources"])
		}
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
	if resized {
		want.Postgres.Resources = model.CloneResourceSpec(want.Postgres.RuntimeResources)
	}
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

func TestImageMigrationCannotPersistUnapprovedRestartResources(t *testing.T) {
	f := newManagedPostgresResizeFixture(t)
	request := imageMigrationOperation(t, f)
	target := managedPostgresResizeTarget()
	intent := map[string]any{"restart_resource_target": target, "restart_spec_hash": "target", "restart_resources": map[string]any{"requests": map[string]any{"memory": "640Mi"}}}
	intent["resources_hash"] = pgimage.Hash(intent["restart_resources"])
	if _, err := imageMigrationPostgres(f.service, f.app, request, intent); !errors.Is(err, ErrConflict) {
		t.Fatalf("missing persisted runtime target accepted: %v", err)
	}
	f.service.Spec.Postgres.RuntimeResources = model.CloneResourceSpec(&f.bootstrap)
	request.DesiredSpec.Postgres.ImageMigrationConfigHash = PostgresImageMigrationConfigHash(f.app, f.service, request.DesiredSpec.Postgres.Image)
	if _, err := imageMigrationPostgres(f.service, f.app, request, intent); !errors.Is(err, ErrConflict) {
		t.Fatalf("different persisted runtime target accepted: %v", err)
	}
}

func TestImageMigrationResourceEvidenceMatchesExactEnvelope(t *testing.T) {
	target := model.ResourceSpec{CPUMilliCores: 100, MemoryMebibytes: 1024, MemoryLimitMebibytes: 1536}
	for _, scenario := range []string{"valid", "memory-drift", "cpu-limit-added", "missing-limit", "malformed"} {
		t.Run(scenario, func(t *testing.T) {
			raw := map[string]any{"requests": map[string]any{"cpu": "0.1", "memory": "1Gi"}, "limits": map[string]any{"memory": "1536Mi"}}
			switch scenario {
			case "memory-drift":
				raw["requests"].(map[string]any)["memory"] = "512Mi"
			case "cpu-limit-added":
				raw["limits"].(map[string]any)["cpu"] = "200m"
			case "missing-limit":
				delete(raw, "limits")
			case "malformed":
				raw["requests"] = "not resource requirements"
			}
			if imageMigrationResourceEvidenceMatches(raw, target) != (scenario == "valid") {
				t.Fatal("invalid resource proof accepted or equivalent quantity rejected")
			}
		})
	}
}

func TestIndependentImageMigrationRejectsBindingAndIdentityDrift(t *testing.T) {
	for _, scenario := range []string{"unbound", "shared", "foreign-owner", "cross-project", "foreign-tenant", "binding-replaced"} {
		t.Run(scenario, func(t *testing.T) {
			f := independentImageMigrationStoreFixture(t)
			request := imageMigrationOperation(t, f)
			exec := func(query string, args ...any) {
				t.Helper()
				if _, err := f.store.db.ExecContext(t.Context(), query, args...); err != nil {
					t.Fatal(err)
				}
			}
			switch scenario {
			case "unbound":
				exec(`DELETE FROM fugue_service_bindings WHERE app_id=$1 AND service_id=$2`, f.app.ID, f.service.ID)
			case "shared":
				addPostgresResizeBinding(t, f)
			case "foreign-owner":
				other := createPostgresResizePeer(t, f)
				mutatePostgresResizeService(t, f, func(svc *model.BackingService) { svc.OwnerAppID = other.ID })
			case "cross-project":
				project, err := f.store.CreateProject(f.tenant.ID, "another-project", "")
				if err != nil {
					t.Fatal(err)
				}
				exec(`UPDATE fugue_backing_services SET project_id=$2 WHERE id=$1`, f.service.ID, project.ID)
			case "foreign-tenant":
				tenant, err := f.store.CreateTenant("another-tenant")
				if err != nil {
					t.Fatal(err)
				}
				exec(`UPDATE fugue_backing_services SET tenant_id=$2 WHERE id=$1`, f.service.ID, tenant.ID)
			case "binding-replaced":
				exec(`UPDATE fugue_service_bindings SET id='replacement-binding' WHERE service_id=$1`, f.service.ID)
			}
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
