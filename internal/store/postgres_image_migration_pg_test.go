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

func TestPGPostgresImageMigrationCompletesOnlyServiceImageWithDurableProof(t *testing.T) {
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
	image, err := pgimage.Target("")
	if err != nil {
		t.Fatal(err)
	}
	row := pgResizeBoundServiceRow(now, "tenant", "project", "app", "service", resources, &resources)
	var serviceSpec model.BackingServiceSpec
	if err = json.Unmarshal(row[17].([]byte), &serviceSpec); err != nil {
		t.Fatal(err)
	}
	svc := model.BackingService{ID: "service", TenantID: "tenant", ProjectID: "project", OwnerAppID: "app", Name: "demo-postgres", Type: model.BackingServiceTypePostgres, Provisioner: model.BackingServiceProvisionerManaged, Spec: serviceSpec}
	app := model.App{ID: "app", TenantID: "tenant", ProjectID: "project", Name: "demo", Spec: model.AppSpec{Image: "registry.invalid/client:1", RuntimeID: "runtime_us"}, BackingServices: []model.BackingService{svc}}
	op := model.Operation{ID: "op", AppID: app.ID, TenantID: app.TenantID, ServiceID: svc.ID, Type: pgimage.OperationType, DesiredSpec: &model.AppSpec{Postgres: &model.AppPostgresSpec{Image: image, ImageMigrationConfigHash: PostgresImageMigrationConfigHash(app, svc, image)}}}
	for _, kind := range []string{pgimage.BaselineEvidence, pgimage.IntentEvidence, pgimage.VerifiedEvidence} {
		proof := map[string]any{"pod_uid": "old-pod", "cluster_uid": "cluster", "system_id": "123456", "target_image": image}
		if kind == pgimage.VerifiedEvidence {
			proof["verified_pod_uid"] = "new-pod"
		}
		b, _ := json.Marshal(proof)
		mock.ExpectQuery(`SELECT payload_json FROM fugue_operation_evidence`).WithArgs(op.ID, op.TenantID, kind).WillReturnRows(sqlmock.NewRows([]string{"payload_json"}).AddRow(b))
	}
	expectPGResizeBackingServiceForUpdate(mock, now, app.TenantID, app.ProjectID, app.ID, svc.ID, resources, &resources)
	expectPGResizeTargetBinding(mock, app.TenantID, app.ID, svc.ID)
	mock.ExpectExec(`(?s)UPDATE fugue_backing_services.*spec_json = \$10`).WithArgs(svc.ID, app.TenantID, app.ProjectID, app.ID, svc.Name, "", model.BackingServiceTypePostgres, model.BackingServiceProvisionerManaged, model.BackingServiceStatusActive, jsonArgument(func(b []byte) bool {
		var actual model.BackingServiceSpec
		if json.Unmarshal(b, &actual) != nil {
			return false
		}
		want := model.CloneAppPostgresSpec(serviceSpec.Postgres)
		want.Image = image
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
