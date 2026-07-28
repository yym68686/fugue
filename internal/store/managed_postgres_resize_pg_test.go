package store

import (
	"database/sql/driver"
	"encoding/json"
	"testing"
	"time"

	"fugue/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPGCreateManagedPostgresResizePersistsNarrowIntent(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create resize sqlmock db: %v", err)
	}
	defer db.Close()
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}

	const (
		tenantID  = "tenant_resize"
		projectID = "project_resize"
		appID     = "app_resize"
		serviceID = "service_resize"
	)
	now := time.Date(2026, time.July, 28, 10, 11, 12, 0, time.UTC)
	bootstrap := model.ResourceSpec{
		CPUMilliCores:        100,
		MemoryMebibytes:      512,
		CPULimitMilliCores:   200,
		MemoryLimitMebibytes: 768,
	}
	target := model.ResourceSpec{
		CPUMilliCores:        400,
		MemoryMebibytes:      512,
		CPULimitMilliCores:   500,
		MemoryLimitMebibytes: 768,
	}
	serviceRow := pgResizeBoundServiceRow(now, tenantID, projectID, appID, serviceID, bootstrap, nil)

	mock.ExpectBegin()
	expectPGLifecycleAppForUpdate(mock, now, tenantID, projectID, appID, 0, serviceRow)
	expectPGLifecycleAppHydration(mock, appID, serviceRow)
	expectPGNoActiveLifecycleForResizeApp(mock, appID)
	expectPGResizeBackingServiceForUpdate(mock, now, tenantID, projectID, appID, serviceID, bootstrap, nil)
	expectPGResizeTargetBinding(mock, tenantID, appID, serviceID)
	expectPGActiveLifecycleOperationsForTarget(mock, appID, serviceID, pgLifecycleOperationRows())
	expectPGNoActiveAppDatabaseBackup(mock, appID, "demo-postgres", "demo-postgres")
	expectPGNoActiveAppDatabaseImport(mock, appID)
	expectPGNoActiveAppDatabaseRestore(mock, appID)
	expectPGRuntimeReservationAvailable(mock, "runtime_us")
	expectPGLifecycleBillingAccrual(mock, now, tenantID, projectID, appID, serviceID)
	mock.ExpectExec(`(?s)INSERT INTO fugue_operations`).
		WithArgs(
			sqlmock.AnyArg(), tenantID, model.OperationTypeDatabaseResize, model.OperationStatusPending,
			model.ExecutionModeManaged, model.ActorTypeAPIKey, "requester", appID, serviceID, "runtime_us", "runtime_us",
			nil, jsonArgument(func(raw []byte) bool {
				var spec model.AppSpec
				return json.Unmarshal(raw, &spec) == nil && spec.Image == "ghcr.io/example/original:1" &&
					spec.Replicas == 0 && spec.RuntimeID == "runtime_us" && spec.Env["MUTATED"] == "" &&
					spec.Postgres != nil && spec.Postgres.Resources != nil && *spec.Postgres.Resources == bootstrap &&
					spec.Postgres.RuntimeResources != nil && *spec.Postgres.RuntimeResources == target
			}), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`(?s)UPDATE fugue_apps`).
		WithArgs(
			appID, tenantID, projectID, "demo", "", sqlmock.AnyArg(), sqlmock.AnyArg(),
			jsonArgument(func(raw []byte) bool {
				var spec model.AppSpec
				return json.Unmarshal(raw, &spec) == nil && spec.Image == "ghcr.io/example/original:1" &&
					spec.Replicas == 0 && spec.RuntimeID == "runtime_us" && spec.Postgres == nil
			}), sqlmock.AnyArg(), now, now,
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`SELECT pg_notify\(\$1, \$2\)`).
		WithArgs(PostgresOperationChannel, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	maliciousReplicas := 99
	created, result, err := s.CreateOperationWithResult(model.Operation{
		TenantID:        tenantID,
		Type:            model.OperationTypeDatabaseResize,
		AppID:           appID,
		ServiceID:       serviceID,
		TargetRuntimeID: "runtime_attacker",
		DesiredReplicas: &maliciousReplicas,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "requester",
		DesiredSource:   &model.AppSource{Type: model.AppSourceTypeDockerImage, ImageRef: "attacker/image"},
		DesiredSpec: &model.AppSpec{
			Image:     "ghcr.io/attacker/replaced:latest",
			Replicas:  99,
			RuntimeID: "runtime_attacker",
			Env:       map[string]string{"MUTATED": "true"},
			Postgres: &model.AppPostgresSpec{
				Resources:        &model.ResourceSpec{CPUMilliCores: 9999, MemoryMebibytes: 9999},
				RuntimeResources: model.CloneResourceSpec(&target),
			},
		},
	})
	if err != nil {
		t.Fatalf("create PostgreSQL resize operation: %v; remaining SQL: %v", err, mock.ExpectationsWereMet())
	}
	if !result.Created || created.Status != model.OperationStatusPending || created.ExecutionMode != model.ExecutionModeManaged ||
		created.DesiredSpec == nil || created.DesiredSpec.Postgres == nil ||
		created.DesiredSpec.Postgres.RuntimeResources == nil || *created.DesiredSpec.Postgres.RuntimeResources != target ||
		created.DesiredReplicas != nil || created.DesiredSource != nil {
		t.Fatalf("PostgreSQL resize operation was not narrowed: op=%+v result=%+v", created, result)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("resize create sqlmock expectations: %v", err)
	}
}

func TestPGCompleteManagedPostgresResizeUpdatesOnlyExactService(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create resize completion sqlmock db: %v", err)
	}
	defer db.Close()
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}

	const (
		tenantID  = "tenant_resize"
		projectID = "project_resize"
		appID     = "app_resize"
		serviceID = "service_resize"
		opID      = "op_resize"
	)
	now := time.Date(2026, time.July, 28, 11, 12, 13, 0, time.UTC)
	bootstrap := model.ResourceSpec{
		CPUMilliCores:        100,
		MemoryMebibytes:      512,
		CPULimitMilliCores:   200,
		MemoryLimitMebibytes: 768,
	}
	target := model.ResourceSpec{
		CPUMilliCores:        400,
		MemoryMebibytes:      512,
		CPULimitMilliCores:   500,
		MemoryLimitMebibytes: 768,
	}
	desiredSpec, err := json.Marshal(model.AppSpec{
		Image:     "ghcr.io/example/original:1",
		Replicas:  0,
		RuntimeID: "runtime_us",
		Postgres: &model.AppPostgresSpec{
			Database:         "demo",
			User:             "demo",
			Password:         "secret",
			ServiceName:      "demo-postgres",
			RuntimeID:        "runtime_us",
			StorageSize:      "1Gi",
			Instances:        1,
			Resources:        model.CloneResourceSpec(&bootstrap),
			RuntimeResources: model.CloneResourceSpec(&target),
		},
	})
	if err != nil {
		t.Fatalf("marshal resize desired spec: %v", err)
	}
	serviceRow := pgResizeBoundServiceRow(now, tenantID, projectID, appID, serviceID, bootstrap, nil)

	mock.ExpectBegin()
	mock.ExpectQuery(`(?s)FROM fugue_operations.*WHERE id = \$1.*FOR UPDATE`).
		WithArgs(opID).
		WillReturnRows(pgLifecycleOperationRows().AddRow(
			opID, tenantID, model.OperationTypeDatabaseResize, model.OperationStatusRunning,
			model.ExecutionModeManaged, model.ActorTypeAPIKey, "requester", appID, serviceID,
			"runtime_us", "runtime_us", nil, desiredSpec, []byte("null"), "database resize in progress",
			"", "", "", now, now, now, nil,
		))
	expectPGLifecycleAppForUpdate(mock, now, tenantID, projectID, appID, 0, serviceRow)
	expectPGResizeBackingServiceForUpdate(mock, now, tenantID, projectID, appID, serviceID, bootstrap, nil)
	expectPGResizeTargetBinding(mock, tenantID, appID, serviceID)
	mock.ExpectExec(`(?s)UPDATE fugue_backing_services`).
		WithArgs(
			serviceID, tenantID, projectID, appID, "demo-postgres", "", model.BackingServiceTypePostgres,
			model.BackingServiceProvisionerManaged, model.BackingServiceStatusActive,
			jsonArgument(func(raw []byte) bool {
				var spec model.BackingServiceSpec
				return json.Unmarshal(raw, &spec) == nil && spec.Postgres != nil &&
					spec.Postgres.Resources != nil && *spec.Postgres.Resources == bootstrap &&
					spec.Postgres.RuntimeResources != nil && *spec.Postgres.RuntimeResources == target &&
					spec.Postgres.Database == "demo" && spec.Postgres.RuntimeID == "runtime_us"
			}), nil, nil, now, sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`(?s)UPDATE fugue_operations`).
		WithArgs(
			opID, tenantID, model.OperationTypeDatabaseResize, model.OperationStatusCompleted,
			model.ExecutionModeManaged, model.ActorTypeAPIKey, "requester", appID, serviceID,
			"runtime_us", "runtime_us", nil,
			jsonArgument(func(raw []byte) bool {
				var spec model.AppSpec
				return json.Unmarshal(raw, &spec) == nil && spec.Image == "ghcr.io/example/original:1" &&
					spec.Postgres != nil && spec.Postgres.Resources != nil && *spec.Postgres.Resources == bootstrap &&
					spec.Postgres.RuntimeResources != nil && *spec.Postgres.RuntimeResources == target
			}), sqlmock.AnyArg(), "database runtime resources persisted", "", "", "", now,
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	malicious := model.AppSpec{
		Image:     "ghcr.io/attacker/completion:latest",
		Replicas:  99,
		RuntimeID: "runtime_attacker",
		Postgres: &model.AppPostgresSpec{
			Resources:        &model.ResourceSpec{CPUMilliCores: 9999, MemoryMebibytes: 9999},
			RuntimeResources: &model.ResourceSpec{CPUMilliCores: 9999, MemoryMebibytes: 9999, CPULimitMilliCores: 9999, MemoryLimitMebibytes: 9999},
		},
	}
	completed, err := s.CompleteManagedOperationWithResult(
		opID,
		"",
		"database runtime resources persisted",
		&malicious,
		&model.AppSource{Type: model.AppSourceTypeDockerImage, ImageRef: "attacker/completion"},
	)
	if err != nil {
		t.Fatalf("complete PostgreSQL resize operation: %v; remaining SQL: %v", err, mock.ExpectationsWereMet())
	}
	if completed.Status != model.OperationStatusCompleted || completed.DesiredSpec == nil ||
		completed.DesiredSpec.Postgres == nil || completed.DesiredSpec.Postgres.RuntimeResources == nil ||
		*completed.DesiredSpec.Postgres.RuntimeResources != target {
		t.Fatalf("resize completion replaced the stored target: %+v", completed)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("resize completion sqlmock expectations: %v", err)
	}
}

func pgResizeBoundServiceRow(
	now time.Time,
	tenantID, projectID, appID, serviceID string,
	bootstrap model.ResourceSpec,
	runtimeTarget *model.ResourceSpec,
) []driver.Value {
	spec, _ := json.Marshal(model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{
		Database: "demo", User: "demo", Password: "secret", ServiceName: "demo-postgres",
		RuntimeID: "runtime_us", StorageSize: "1Gi", Instances: 1,
		Resources: model.CloneResourceSpec(&bootstrap), RuntimeResources: model.CloneResourceSpec(runtimeTarget),
	}})
	return []driver.Value{
		"binding_" + serviceID, tenantID, appID, serviceID, "postgres", []byte(`{}`), now, now,
		serviceID, tenantID, projectID, appID, "demo-postgres", "", model.BackingServiceTypePostgres,
		model.BackingServiceProvisionerManaged, model.BackingServiceStatusActive, spec, nil, nil, now, now,
	}
}

func expectPGResizeBackingServiceForUpdate(
	mock sqlmock.Sqlmock,
	now time.Time,
	tenantID, projectID, appID, serviceID string,
	bootstrap model.ResourceSpec,
	runtimeTarget *model.ResourceSpec,
) {
	row := pgResizeBoundServiceRow(now, tenantID, projectID, appID, serviceID, bootstrap, runtimeTarget)
	mock.ExpectQuery(`(?s)FROM fugue_backing_services.*WHERE id = \$1.*FOR UPDATE`).
		WithArgs(serviceID).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "project_id", "owner_app_id", "name", "description", "type", "provisioner", "status", "spec_json", "current_runtime_started_at", "current_runtime_ready_at", "created_at", "updated_at",
		}).AddRow(row[8:]...))
}

func expectPGResizeTargetBinding(mock sqlmock.Sqlmock, tenantID, appID, serviceID string) {
	mock.ExpectQuery(`(?s)SELECT tenant_id, app_id.*FROM fugue_service_bindings.*WHERE service_id = \$1.*FOR UPDATE`).
		WithArgs(serviceID).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_id"}).AddRow(tenantID, appID))
}

func expectPGNoActiveLifecycleForResizeApp(mock sqlmock.Sqlmock, appID string) {
	mock.ExpectQuery(`(?s)SELECT EXISTS \(.*FROM fugue_operations.*type IN \(\$1, \$2\).*AND app_id = \$6`).
		WithArgs(
			model.OperationTypeDatabaseSuspend, model.OperationTypeDatabaseResume,
			model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent, appID,
		).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
}

func expectPGNoActiveAppDatabaseRestore(mock sqlmock.Sqlmock, appID string) {
	mock.ExpectQuery(`(?s)SELECT EXISTS \(.*FROM fugue_backup_restore_runs.*WHERE app_id = \$1.*status IN \(\$2, \$3\)`).
		WithArgs(appID, model.BackupRestoreStatusPlanned, model.BackupRestoreStatusRunning).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
}

func expectPGRuntimeReservationAvailable(mock sqlmock.Sqlmock, runtimeID string) {
	mock.ExpectExec(`SELECT pg_advisory_xact_lock\(hashtext\(\$1\)::bigint\)`).
		WithArgs(runtimeID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`(?s)FROM fugue_project_runtime_reservations.*WHERE runtime_id = \$1.*FOR UPDATE`).
		WithArgs(runtimeID).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "project_id", "runtime_id", "mode", "created_at", "updated_at"}))
}
