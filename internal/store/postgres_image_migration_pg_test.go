package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/pgimage"
	"github.com/DATA-DOG/go-sqlmock"
)

func TestPGIndependentImageMigrationAdmissionPreservesConsumer(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	now := time.Now().UTC()
	resources := model.ResourceSpec{CPUMilliCores: 100, MemoryMebibytes: 512, CPULimitMilliCores: 200, MemoryLimitMebibytes: 768}
	row := pgResizeBoundServiceRow(now, "tenant", "project", "app", "service", resources, nil)
	row[11] = "" // Keep the actual consumer binding, but no database owner.
	var spec model.BackingServiceSpec
	if err := json.Unmarshal(row[17].([]byte), &spec); err != nil {
		t.Fatal(err)
	}
	svc := model.BackingService{ID: "service", TenantID: "tenant", ProjectID: "project", Name: "demo-postgres", Type: model.BackingServiceTypePostgres, Provisioner: model.BackingServiceProvisionerManaged, Spec: spec}
	app := model.App{ID: "app", TenantID: "tenant", ProjectID: "project", Name: "demo", Spec: model.AppSpec{Image: "ghcr.io/example/original:1", RuntimeID: "runtime_us"}, BackingServices: []model.BackingService{svc}, Bindings: []model.ServiceBinding{{ID: "binding_service", TenantID: "tenant", AppID: "app", ServiceID: "service", Alias: "postgres", CreatedAt: now, UpdatedAt: now}}}
	image, err := pgimage.Target("")
	if err != nil {
		t.Fatal(err)
	}
	hash := PostgresImageMigrationConfigHash(app, svc, image)
	mock.ExpectBegin()
	expectPGLifecycleAppForUpdate(mock, now, app.TenantID, app.ProjectID, app.ID, 0, row)
	expectRecoveryGuardedAdmissionHydration(mock, app.ID, row)
	expectPGResizeBackingServiceForUpdate(mock, now, app.TenantID, app.ProjectID, "", svc.ID, resources, nil)
	expectPGResizeTargetBinding(mock, app.TenantID, app.ID, svc.ID)
	expectPGActiveLifecycleOperationsForTarget(mock, app.ID, svc.ID, pgLifecycleOperationRows())
	expectPGNoActiveAppDatabaseBackup(mock, app.ID, svc.Name, svc.Name)
	expectPGNoActiveAppDatabaseImport(mock, app.ID)
	expectPGNoActiveAppDatabaseRestore(mock, app.ID)
	expectPGRuntimeReservationAvailable(mock, "runtime_us")
	expectPGLifecycleBillingAccrual(mock, now, app.TenantID, app.ProjectID, app.ID, svc.ID)
	expectPGNoActiveAppCPUResize(mock, app.ID)
	mock.ExpectExec(`(?s)INSERT INTO fugue_operations`).WithArgs(
		sqlmock.AnyArg(), app.TenantID, pgimage.OperationType, model.OperationStatusPending, model.ExecutionModeManaged, model.ActorTypeAPIKey, "requester", app.ID, svc.ID, "runtime_us", "runtime_us", nil,
		jsonArgument(func(raw []byte) bool {
			var spec model.AppSpec
			return json.Unmarshal(raw, &spec) == nil && spec.Image == app.Spec.Image && spec.Replicas == 0 && spec.Postgres != nil && spec.Postgres.Image == image && spec.Postgres.ImageMigrationConfigHash == hash && spec.Postgres.Database == svc.Spec.Postgres.Database
		}),
		sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
	).WillReturnResult(sqlmock.NewResult(0, 1))
	// The existing operation transaction writes back the same consumer state,
	// never its database image, replica count, status or timestamps.
	mock.ExpectExec(`(?s)UPDATE fugue_apps`).WithArgs(app.ID, app.TenantID, app.ProjectID, app.Name, "", sqlmock.AnyArg(), sqlmock.AnyArg(),
		jsonArgument(func(raw []byte) bool {
			var spec model.AppSpec
			return json.Unmarshal(raw, &spec) == nil && spec.Image == app.Spec.Image && spec.Replicas == 0 && spec.Postgres == nil
		}),
		sqlmock.AnyArg(), now, now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`SELECT pg_notify\(\$1, \$2\)`).WithArgs(PostgresOperationChannel, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	op, result, err := s.CreateOperationWithResult(model.Operation{TenantID: app.TenantID, AppID: app.ID, ServiceID: svc.ID, Type: pgimage.OperationType, RequestedByType: model.ActorTypeAPIKey, RequestedByID: "requester", DesiredSpec: &model.AppSpec{Image: "untrusted", Replicas: 99, Postgres: &model.AppPostgresSpec{Image: image, ImageMigrationConfigHash: hash}}})
	if err != nil {
		t.Fatalf("admission: %v; remaining SQL: %v", err, mock.ExpectationsWereMet())
	}
	if !result.Created || op.AppID != app.ID || op.ServiceID != svc.ID || op.DesiredSpec.Image != app.Spec.Image {
		t.Fatalf("incorrect independent migration intent: %+v", op)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPGPostgresImageMigrationCompletesOnlyServiceImageWithDurableProof(t *testing.T) {
	for _, owner := range []string{"app", ""} {
		name := "app-owned"
		if owner == "" {
			name = "independent"
		}
		for _, resized := range []bool{false, true} {
			suffix := "/image-only"
			if resized {
				suffix = "/applied-resize"
			}
			t.Run(name+suffix, func(t *testing.T) { testPGPostgresImageMigrationCompletion(t, owner, resized) })
		}
	}
}

func testPGPostgresImageMigrationCompletion(t *testing.T, owner string, resized bool) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	mock.ExpectBegin()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	resources := model.ResourceSpec{CPUMilliCores: 100, MemoryMebibytes: 512, CPULimitMilliCores: 200, MemoryLimitMebibytes: 768}
	target := resources
	if resized {
		target.MemoryMebibytes, target.MemoryLimitMebibytes = 256, 640
	}
	image, err := pgimage.Target("")
	if err != nil {
		t.Fatal(err)
	}
	row := pgResizeBoundServiceRow(now, "tenant", "project", owner, "service", resources, &target)
	var serviceSpec model.BackingServiceSpec
	if err = json.Unmarshal(row[17].([]byte), &serviceSpec); err != nil {
		t.Fatal(err)
	}
	svc := model.BackingService{ID: "service", TenantID: "tenant", ProjectID: "project", OwnerAppID: owner, Name: "demo-postgres", Type: model.BackingServiceTypePostgres, Provisioner: model.BackingServiceProvisionerManaged, Spec: serviceSpec}
	app := model.App{ID: "app", TenantID: "tenant", ProjectID: "project", Name: "demo", Spec: model.AppSpec{Image: "registry.invalid/client:1", RuntimeID: "runtime_us"}, BackingServices: []model.BackingService{svc}, Bindings: []model.ServiceBinding{{ID: "binding", TenantID: "tenant", AppID: "app", ServiceID: "service", Alias: "postgres"}}}
	op := model.Operation{ID: "op", AppID: app.ID, TenantID: app.TenantID, ServiceID: svc.ID, Type: pgimage.OperationType, DesiredSpec: &model.AppSpec{Postgres: &model.AppPostgresSpec{Image: image, ImageMigrationConfigHash: PostgresImageMigrationConfigHash(app, svc, image)}}}
	for _, kind := range []string{pgimage.BaselineEvidence, pgimage.IntentEvidence, pgimage.VerifiedEvidence} {
		proof := map[string]any{"pod_uid": "old-pod", "cluster_uid": "cluster", "system_id": "123456", "target_image": image}
		if resized {
			proof["restart_resource_target"] = target
			proof["restart_spec_hash"] = "target-spec"
			proof["restart_resources"] = map[string]any{"requests": map[string]any{"memory": "256Mi", "cpu": "100m"}, "limits": map[string]any{"memory": "640Mi", "cpu": "200m"}}
			proof["resources_hash"] = pgimage.Hash(proof["restart_resources"])
		}
		if kind == pgimage.VerifiedEvidence {
			proof["verified_pod_uid"] = "new-pod"
		}
		b, _ := json.Marshal(proof)
		mock.ExpectQuery(`SELECT payload_json FROM fugue_operation_evidence`).WithArgs(op.ID, op.TenantID, kind).WillReturnRows(sqlmock.NewRows([]string{"payload_json"}).AddRow(b))
	}
	expectPGResizeBackingServiceForUpdate(mock, now, app.TenantID, app.ProjectID, owner, svc.ID, resources, &target)
	expectPGResizeTargetBinding(mock, app.TenantID, app.ID, svc.ID)
	var persistedOwner any = owner
	if owner == "" {
		persistedOwner = nil
	}
	mock.ExpectExec(`(?s)UPDATE fugue_backing_services.*spec_json = \$10`).WithArgs(svc.ID, app.TenantID, app.ProjectID, persistedOwner, svc.Name, "", model.BackingServiceTypePostgres, model.BackingServiceProvisionerManaged, model.BackingServiceStatusActive, jsonArgument(func(b []byte) bool {
		var actual model.BackingServiceSpec
		if json.Unmarshal(b, &actual) != nil {
			return false
		}
		want := model.CloneAppPostgresSpec(serviceSpec.Postgres)
		want.Image = image
		if resized {
			want.Resources = model.CloneResourceSpec(&target)
		}
		return pgimage.Hash(actual.Postgres) == pgimage.Hash(want)
	}), nil, nil, now, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
	if err = s.pgApplyPostgresImageMigration(context.Background(), tx, &app, &op); err != nil {
		t.Fatal(err)
	}
	mock.ExpectRollback()
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
