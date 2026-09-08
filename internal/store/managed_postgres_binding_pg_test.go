package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"fugue/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPGApplyDesiredSpecUpdatesBoundManagedPostgresWhenOwnerMissing(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	defer db.Close()
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}

	const (
		appID     = "app_123"
		tenantID  = "tenant_123"
		projectID = "project_123"
		serviceID = "svc_pg"
	)
	now := time.Date(2026, time.September, 8, 11, 30, 0, 0, time.UTC)
	currentSpec := model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{
		Database: "demo", User: "demo", Password: "secret", ServiceName: "demo-postgres",
		RuntimeID: "runtime_a", Resources: &model.ResourceSpec{CPUMilliCores: 150, MemoryMebibytes: 1024, MemoryLimitMebibytes: 1536},
	}}
	currentRaw, err := json.Marshal(currentSpec)
	if err != nil {
		t.Fatalf("marshal current service spec: %v", err)
	}
	app := model.App{
		ID: appID, Name: "demo", TenantID: tenantID, ProjectID: projectID,
		Spec: model.AppSpec{RuntimeID: "runtime_a"},
	}
	desired := &model.AppSpec{RuntimeID: "runtime_a", Postgres: &model.AppPostgresSpec{
		Database: "demo", User: "demo", Password: "secret", ServiceName: "demo-postgres", RuntimeID: "runtime_a",
		Resources: &model.ResourceSpec{CPUMilliCores: 150, MemoryMebibytes: 128, MemoryLimitMebibytes: 1536},
	}}

	mock.ExpectBegin()
	mock.ExpectQuery(`(?s)FROM fugue_backing_services.*WHERE owner_app_id = \$1.*AND type = \$2`).
		WithArgs(appID, model.BackingServiceTypePostgres).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "project_id", "owner_app_id", "name", "description", "type", "provisioner", "status", "spec_json", "current_runtime_started_at", "current_runtime_ready_at", "created_at", "updated_at",
		}))
	mock.ExpectQuery(`(?s)FROM fugue_service_bindings AS b.*JOIN fugue_backing_services AS s.*WHERE b.app_id = \$1.*AND s.type = \$2.*LIMIT 1`).
		WithArgs(appID, model.BackingServiceTypePostgres, model.BackingServiceStatusDeleted).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(serviceID))
	mock.ExpectQuery(`(?s)FROM fugue_backing_services.*WHERE id = \$1.*FOR UPDATE`).
		WithArgs(serviceID).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "project_id", "owner_app_id", "name", "description", "type", "provisioner", "status", "spec_json", "current_runtime_started_at", "current_runtime_ready_at", "created_at", "updated_at",
		}).AddRow(serviceID, tenantID, projectID, nil, "demo-db", "", model.BackingServiceTypePostgres, model.BackingServiceProvisionerManaged, model.BackingServiceStatusActive, currentRaw, nil, nil, now, now))
	mock.ExpectExec(`(?s)UPDATE fugue_backing_services`).
		WithArgs(serviceID, tenantID, projectID, nil, "demo-db", "", model.BackingServiceTypePostgres,
			model.BackingServiceProvisionerManaged, model.BackingServiceStatusActive,
			jsonArgument(func(raw []byte) bool {
				var got model.BackingServiceSpec
				if json.Unmarshal(raw, &got) != nil || got.Postgres == nil || got.Postgres.Resources == nil {
					return false
				}
				return got.Postgres.Resources.MemoryMebibytes == 128 && got.Postgres.Resources.MemoryLimitMebibytes == 1536
			}), nil, nil, now, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`(?s)FROM fugue_service_bindings.*WHERE app_id = \$1.*service_id = \$2.*FOR UPDATE`).
		WithArgs(appID, serviceID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_id", "service_id", "alias", "env_json", "created_at", "updated_at"}).AddRow(
			"binding_123", tenantID, appID, serviceID, "postgres", []byte(`{}`), now, now))
	mock.ExpectExec(`(?s)UPDATE fugue_service_bindings`).
		WithArgs("binding_123", tenantID, appID, serviceID, "postgres", sqlmock.AnyArg(), now, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	if err := s.pgApplyDesiredSpecBackingServicesTx(context.Background(), tx, &app, desired); err != nil {
		t.Fatalf("apply desired spec: %v", err)
	}
	mock.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback transaction: %v", err)
	}
	if desired.Postgres != nil {
		t.Fatalf("expected desired postgres spec to be consumed, got %+v", desired.Postgres)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}
