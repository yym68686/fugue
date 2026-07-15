package store

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"regexp"
	"strconv"
	"testing"
	"time"

	"fugue/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPGLifecycleClaimsAlwaysUseManagedController(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		operationType string
		firstQuery    string
		firstArgs     []driver.Value
		fullUpdate    bool
		claim         func(*Store, string) (model.Operation, bool, error)
	}{
		{
			name:          "try claim suspend",
			operationType: model.OperationTypeDatabaseSuspend,
			firstQuery: `(?s)SELECT id, tenant_id, type, status, execution_mode.*
FROM fugue_operations
WHERE id = \$1
  AND status = \$2
FOR UPDATE SKIP LOCKED`,
			firstArgs:  []driver.Value{"op_lifecycle", model.OperationStatusPending},
			fullUpdate: true,
			claim: func(s *Store, operationID string) (model.Operation, bool, error) {
				return s.TryClaimPendingOperation(operationID)
			},
		},
		{
			name:          "claim next resume",
			operationType: model.OperationTypeDatabaseResume,
			firstQuery:    `(?s)WITH next_op AS \(.*JOIN next_op n ON n.id = o.id`,
			firstArgs:     []driver.Value{model.OperationStatusPending},
			claim: func(s *Store, _ string) (model.Operation, bool, error) {
				return s.ClaimNextPendingOperation()
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("create sqlmock db: %v", err)
			}
			defer db.Close()
			s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
			now := time.Date(2026, time.July, 15, 2, 3, 4, 0, time.UTC)

			mock.ExpectBegin()
			mock.ExpectQuery(test.firstQuery).
				WithArgs(test.firstArgs...).
				WillReturnRows(sqlmock.NewRows([]string{
					"id", "tenant_id", "type", "status", "execution_mode", "requested_by_type", "requested_by_id", "app_id", "service_id", "source_runtime_id", "target_runtime_id", "desired_replicas", "desired_spec_json", "desired_source_json", "result_message", "manifest_path", "assigned_runtime_id", "error_message", "created_at", "updated_at", "started_at", "completed_at",
				}).AddRow(
					"op_lifecycle", "tenant_demo", test.operationType, model.OperationStatusPending, "", "", "", "app_demo", "service_demo", "runtime_external", "runtime_external", nil, nil, nil, "", "", "", "", now, now, nil, nil,
				))
			if test.fullUpdate {
				mock.ExpectExec(`(?s)UPDATE fugue_operations\s+SET tenant_id = \$2`).
					WillReturnResult(sqlmock.NewResult(0, 1))
			}
			expectPGClaimLifecycleApp(t, mock, now)
			if !test.fullUpdate {
				mock.ExpectExec(`(?s)UPDATE fugue_operations\s+SET status = \$2`).
					WillReturnResult(sqlmock.NewResult(0, 1))
			}
			mock.ExpectExec(`(?s)UPDATE fugue_apps\s+SET tenant_id = \$2`).
				WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectCommit()

			claimed, found, err := test.claim(s, "op_lifecycle")
			if err != nil {
				t.Fatalf("claim postgres lifecycle operation: %v", err)
			}
			if !found {
				t.Fatal("expected lifecycle operation to be claimed")
			}
			if claimed.Status != model.OperationStatusRunning || claimed.ExecutionMode != model.ExecutionModeManaged || claimed.AssignedRuntimeID != "" {
				t.Fatalf("postgres lifecycle operation escaped managed controller: %+v", claimed)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("sqlmock expectations: %v", err)
			}
		})
	}
}

func expectPGClaimLifecycleApp(t *testing.T, mock sqlmock.Sqlmock, now time.Time) {
	t.Helper()
	mock.ExpectQuery(regexp.QuoteMeta(`
	SELECT id, tenant_id, project_id, name, description, source_json, route_json, spec_json, status_json, created_at, updated_at
	FROM fugue_apps
WHERE id = $1
 FOR UPDATE`)).
		WithArgs("app_demo").
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "project_id", "name", "description", "source_json", "route_json", "spec_json", "status_json", "created_at", "updated_at",
		}).AddRow(
			"app_demo", "tenant_demo", "project_demo", "demo", "", []byte("null"), []byte("null"),
			[]byte(`{"image":"ghcr.io/example/demo:latest","replicas":0,"runtime_id":"runtime_external"}`),
			[]byte(`{"phase":"disabled","current_runtime_id":"runtime_external","current_replicas":0,"updated_at":"2026-07-15T02:03:04Z"}`),
			now, now,
		))
	mock.ExpectQuery(regexp.QuoteMeta(`
	SELECT b.id, b.tenant_id, b.app_id, b.service_id, b.alias, b.env_json, b.created_at, b.updated_at,
	       s.id, s.tenant_id, s.project_id, s.owner_app_id, s.name, s.description, s.type, s.provisioner, s.status, s.spec_json, s.current_runtime_started_at, s.current_runtime_ready_at, s.created_at, s.updated_at
	FROM fugue_service_bindings AS b
	JOIN fugue_backing_services AS s ON s.id = b.service_id
WHERE b.app_id = $1
ORDER BY b.created_at ASC, s.created_at ASC
`)).
		WithArgs("app_demo").
		WillReturnRows(sqlmock.NewRows([]string{
			"binding_id", "binding_tenant_id", "binding_app_id", "binding_service_id", "binding_alias", "binding_env_json", "binding_created_at", "binding_updated_at",
			"service_id", "service_tenant_id", "service_project_id", "service_owner_app_id", "service_name", "service_description", "service_type", "service_provisioner", "service_status", "service_spec_json", "service_current_runtime_started_at", "service_current_runtime_ready_at", "service_created_at", "service_updated_at",
		}))
}

func TestPGCreateManagedPostgresSuspendLocksExactLifecycleTarget(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	defer db.Close()
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}

	const (
		tenantID  = "tenant_lifecycle"
		projectID = "project_lifecycle"
		appID     = "app_lifecycle"
		serviceID = "service_target"
	)
	now := time.Date(2026, time.July, 15, 3, 4, 5, 0, time.UTC)

	mock.ExpectBegin()
	expectPGLifecycleAppForUpdate(mock, now, tenantID, projectID, appID, 0,
		pgLifecycleBoundServiceRow(now, tenantID, projectID, appID, serviceID, false),
	)
	expectPGLifecycleAppHydration(mock, appID,
		pgLifecycleBoundServiceRow(now, tenantID, projectID, appID, serviceID, false),
	)
	expectPGLifecycleBackingServiceForUpdate(mock, now, tenantID, projectID, appID, serviceID, false)
	mock.ExpectQuery(`(?s)SELECT tenant_id, app_id.*FROM fugue_service_bindings.*WHERE service_id = \$1.*FOR UPDATE`).
		WithArgs(serviceID).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_id"}).AddRow(tenantID, appID))
	expectPGActiveLifecycleOperationsForTarget(mock, appID, serviceID, pgLifecycleOperationRows())
	expectPGNoActiveAppDatabaseBackup(mock, appID, "demo-postgres", "demo-postgres")
	expectPGNoActiveAppDatabaseImport(mock, appID)
	expectPGLifecycleBillingAccrual(mock, now, tenantID, projectID, appID, serviceID)
	mock.ExpectExec(`(?s)INSERT INTO fugue_operations`).
		WithArgs(
			sqlmock.AnyArg(), tenantID, model.OperationTypeDatabaseSuspend, model.OperationStatusPending,
			model.ExecutionModeManaged, "", "", appID, serviceID, "runtime_us", "runtime_us",
			nil, jsonArgument(func(raw []byte) bool {
				var spec model.AppSpec
				return json.Unmarshal(raw, &spec) == nil && spec.Image == "ghcr.io/example/original:1" &&
					spec.Replicas == 0 && spec.Postgres != nil && spec.Postgres.Suspended
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
	mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_notify($1, $2)`)).
		WithArgs(PostgresOperationChannel, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	op, err := s.CreateOperation(model.Operation{
		TenantID:  tenantID,
		Type:      model.OperationTypeDatabaseSuspend,
		AppID:     appID,
		ServiceID: serviceID,
	})
	if err != nil {
		t.Fatalf("create postgres suspend operation: %v; remaining SQL: %v", err, mock.ExpectationsWereMet())
	}
	if op.Status != model.OperationStatusPending || op.ExecutionMode != model.ExecutionModeManaged {
		t.Fatalf("expected queued managed lifecycle operation, got %+v", op)
	}
	if op.ServiceID != serviceID || op.DesiredSpec == nil || op.DesiredSpec.Postgres == nil || !op.DesiredSpec.Postgres.Suspended {
		t.Fatalf("operation lost exact suspended service intent: %+v", op)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestPGCreateManagedPostgresLifecycleRetryRequiresExactUniqueActiveOperation(t *testing.T) {
	t.Parallel()

	const (
		tenantID  = "tenant_lifecycle"
		projectID = "project_lifecycle"
		appID     = "app_lifecycle"
		serviceID = "service_target"
		opID      = "op_existing_suspend"
	)
	now := time.Date(2026, time.July, 15, 3, 4, 5, 0, time.UTC)
	desiredSpec, err := json.Marshal(model.AppSpec{
		Image:     "ghcr.io/example/original:1",
		Replicas:  0,
		RuntimeID: "runtime_us",
		Postgres: &model.AppPostgresSpec{
			Database:    "demo",
			User:        "demo",
			Password:    "database-password-must-not-leak-on-conflict",
			RuntimeID:   "runtime_us",
			ServiceName: "demo-postgres",
			Suspended:   true,
		},
	})
	if err != nil {
		t.Fatalf("marshal existing desired spec: %v", err)
	}
	corruptDesiredSpec, err := json.Marshal(model.AppSpec{
		Postgres: &model.AppPostgresSpec{
			RuntimeID:   "runtime_us",
			ServiceName: "demo-postgres",
			Suspended:   false,
		},
	})
	if err != nil {
		t.Fatalf("marshal corrupt desired spec: %v", err)
	}

	type activeOperation struct {
		id                  string
		tenantID            string
		appID               string
		serviceID           string
		typeName            string
		desiredSpecOverride []byte
	}
	tests := []struct {
		name      string
		active    []activeOperation
		wantReuse bool
		wantErr   error
	}{
		{
			name: "exact sole retry is reused",
			active: []activeOperation{{
				id: opID, tenantID: tenantID, appID: appID, serviceID: serviceID, typeName: model.OperationTypeDatabaseSuspend,
			}},
			wantReuse: true,
		},
		{
			name: "opposite direction conflicts",
			active: []activeOperation{{
				id: opID, tenantID: tenantID, appID: appID, serviceID: serviceID, typeName: model.OperationTypeDatabaseResume,
			}},
			wantErr: ErrConflict,
		},
		{
			name: "tenant mismatch conflicts",
			active: []activeOperation{{
				id: opID, tenantID: "tenant_other", appID: appID, serviceID: serviceID, typeName: model.OperationTypeDatabaseSuspend,
			}},
			wantErr: ErrConflict,
		},
		{
			name: "app mismatch conflicts",
			active: []activeOperation{{
				id: opID, tenantID: tenantID, appID: "app_other", serviceID: serviceID, typeName: model.OperationTypeDatabaseSuspend,
			}},
			wantErr: ErrConflict,
		},
		{
			name: "service mismatch conflicts",
			active: []activeOperation{{
				id: opID, tenantID: tenantID, appID: appID, serviceID: "service_other", typeName: model.OperationTypeDatabaseSuspend,
			}},
			wantErr: ErrConflict,
		},
		{
			name: "corrupt desired direction conflicts",
			active: []activeOperation{{
				id: opID, tenantID: tenantID, appID: appID, serviceID: serviceID, typeName: model.OperationTypeDatabaseSuspend,
				desiredSpecOverride: corruptDesiredSpec,
			}},
			wantErr: ErrConflict,
		},
		{
			name: "multiple exact active operations conflict",
			active: []activeOperation{
				{id: opID, tenantID: tenantID, appID: appID, serviceID: serviceID, typeName: model.OperationTypeDatabaseSuspend},
				{id: "op_duplicate", tenantID: tenantID, appID: appID, serviceID: serviceID, typeName: model.OperationTypeDatabaseSuspend},
			},
			wantErr: ErrConflict,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("create sqlmock db: %v", err)
			}
			defer db.Close()
			s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}

			mock.ExpectBegin()
			expectPGLifecycleAppForUpdate(mock, now, tenantID, projectID, appID, 0,
				pgLifecycleBoundServiceRow(now, tenantID, projectID, appID, serviceID, false),
			)
			expectPGLifecycleAppHydration(mock, appID,
				pgLifecycleBoundServiceRow(now, tenantID, projectID, appID, serviceID, false),
			)
			expectPGLifecycleBackingServiceForUpdate(mock, now, tenantID, projectID, appID, serviceID, false)
			mock.ExpectQuery(`(?s)SELECT tenant_id, app_id.*FROM fugue_service_bindings.*WHERE service_id = \$1.*FOR UPDATE`).
				WithArgs(serviceID).
				WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_id"}).AddRow(tenantID, appID))
			rows := pgLifecycleOperationRows()
			for _, active := range test.active {
				activeDesiredSpec := desiredSpec
				if len(active.desiredSpecOverride) > 0 {
					activeDesiredSpec = active.desiredSpecOverride
				}
				rows.AddRow(
					active.id, active.tenantID, active.typeName, model.OperationStatusPending, model.ExecutionModeManaged,
					model.ActorTypeAPIKey, "requester", active.appID, active.serviceID, "runtime_us", "runtime_us", nil,
					activeDesiredSpec, nil, "database lifecycle queued", "", "", "", now, now, nil, nil,
				)
			}
			expectPGActiveLifecycleOperationsForTarget(mock, appID, serviceID, rows)
			mock.ExpectRollback()

			returned, result, err := s.CreateOperationWithResult(model.Operation{
				TenantID:  tenantID,
				Type:      model.OperationTypeDatabaseSuspend,
				AppID:     appID,
				ServiceID: serviceID,
			})
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("expected %v, got %v", test.wantErr, err)
				}
				if returned.ID != "" || returned.DesiredSpec != nil || result.Created {
					t.Fatalf("conflicting retry leaked existing operation: op=%+v result=%+v", returned, result)
				}
			} else {
				if err != nil {
					t.Fatalf("reuse exact lifecycle operation: %v", err)
				}
				if !test.wantReuse || result.Created || returned.ID != opID {
					t.Fatalf("exact retry was not reused: op=%+v result=%+v", returned, result)
				}
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("sqlmock expectations: %v", err)
			}
		})
	}
}

func TestPGGetActiveManagedPostgresLifecycleOperationValidatesTopologyAndDirection(t *testing.T) {
	t.Parallel()

	const (
		tenantID  = "tenant_lifecycle"
		projectID = "project_lifecycle"
		appID     = "app_lifecycle"
		serviceID = "service_target"
		opID      = "op_existing_suspend"
	)
	now := time.Date(2026, time.July, 15, 3, 4, 5, 0, time.UTC)
	tests := []struct {
		name          string
		operationType string
		wantFound     bool
		wantErr       error
	}{
		{name: "exact direction", operationType: model.OperationTypeDatabaseSuspend, wantFound: true},
		{name: "opposite direction", operationType: model.OperationTypeDatabaseResume, wantErr: ErrConflict},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("create sqlmock db: %v", err)
			}
			defer db.Close()
			s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}

			mock.ExpectBegin()
			expectPGLifecycleAppForUpdate(mock, now, tenantID, projectID, appID, 0,
				pgLifecycleBoundServiceRow(now, tenantID, projectID, appID, serviceID, false),
			)
			expectPGLifecycleBackingServiceForUpdate(mock, now, tenantID, projectID, appID, serviceID, false)
			mock.ExpectQuery(`(?s)SELECT tenant_id, app_id.*FROM fugue_service_bindings.*WHERE service_id = \$1.*FOR UPDATE`).
				WithArgs(serviceID).
				WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_id"}).AddRow(tenantID, appID))
			rows := pgLifecycleOperationRows().AddRow(
				opID, tenantID, model.OperationTypeDatabaseSuspend, model.OperationStatusRunning, model.ExecutionModeManaged,
				model.ActorTypeAPIKey, "requester", appID, serviceID, "runtime_us", "runtime_us", nil,
				[]byte(`{"postgres":{"password":"must-not-leak-on-conflict","runtime_id":"runtime_us","service_name":"demo-postgres","suspended":true}}`), nil,
				"database lifecycle running", "", "", "", now, now, now, nil,
			)
			expectPGActiveLifecycleOperationsForTarget(mock, appID, serviceID, rows)
			mock.ExpectRollback()

			op, found, err := s.GetActiveManagedPostgresLifecycleOperation(tenantID, appID, serviceID, test.operationType)
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) || found || op.ID != "" || op.DesiredSpec != nil {
					t.Fatalf("lookup conflict leaked operation: op=%+v found=%t err=%v", op, found, err)
				}
			} else if err != nil || found != test.wantFound || op.ID != opID {
				t.Fatalf("lookup exact lifecycle operation: op=%+v found=%t err=%v", op, found, err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("sqlmock expectations: %v", err)
			}
		})
	}
}

func TestPGCreateManagedPostgresSuspendRejectsMultipleBindings(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	defer db.Close()
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}

	const (
		tenantID  = "tenant_lifecycle"
		projectID = "project_lifecycle"
		appID     = "app_lifecycle"
		serviceID = "service_target"
	)
	now := time.Date(2026, time.July, 15, 3, 4, 5, 0, time.UTC)
	mock.ExpectBegin()
	expectPGLifecycleAppForUpdate(mock, now, tenantID, projectID, appID, 0,
		pgLifecycleBoundServiceRow(now, tenantID, projectID, appID, serviceID, false),
	)
	expectPGLifecycleAppHydration(mock, appID,
		pgLifecycleBoundServiceRow(now, tenantID, projectID, appID, serviceID, false),
	)
	expectPGLifecycleBackingServiceForUpdate(mock, now, tenantID, projectID, appID, serviceID, false)
	mock.ExpectQuery(`(?s)SELECT tenant_id, app_id.*FROM fugue_service_bindings.*WHERE service_id = \$1.*FOR UPDATE`).
		WithArgs(serviceID).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_id"}).
			AddRow(tenantID, appID).
			AddRow(tenantID, "app_unexpected"))
	mock.ExpectRollback()

	_, err = s.CreateOperation(model.Operation{
		TenantID:  tenantID,
		Type:      model.OperationTypeDatabaseSuspend,
		AppID:     appID,
		ServiceID: serviceID,
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected multiple binding conflict, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestPGCompleteManagedPostgresLifecycleUpdatesOnlyExactService(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	defer db.Close()
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}

	const (
		tenantID      = "tenant_lifecycle"
		projectID     = "project_lifecycle"
		appID         = "app_lifecycle"
		targetService = "service_target"
		otherService  = "service_other"
		opID          = "op_suspend"
	)
	now := time.Date(2026, time.July, 15, 4, 5, 6, 0, time.UTC)
	storedDesired := []byte(`{"image":"ghcr.io/example/original:1","replicas":0,"runtime_id":"runtime_us","postgres":{"database":"demo","user":"demo","password":"secret","service_name":"demo-postgres","runtime_id":"runtime_us","storage_size":"1Gi","instances":1,"suspended":true}}`)

	mock.ExpectBegin()
	mock.ExpectQuery(`(?s)FROM fugue_operations.*WHERE id = \$1.*FOR UPDATE`).
		WithArgs(opID).
		WillReturnRows(pgLifecycleOperationRows().AddRow(
			opID, tenantID, model.OperationTypeDatabaseSuspend, model.OperationStatusRunning,
			model.ExecutionModeManaged, "", "", appID, targetService, "runtime_us", "runtime_us", nil,
			storedDesired, []byte("null"), "suspending managed PostgreSQL", "", "", "", now, now, now, nil,
		))
	expectPGLifecycleAppForUpdate(mock, now, tenantID, projectID, appID, 0,
		pgLifecycleBoundServiceRow(now, tenantID, projectID, appID, targetService, false),
		pgLifecycleBoundServiceRow(now, tenantID, projectID, appID, otherService, false),
	)
	expectPGLifecycleBackingServiceForUpdate(mock, now, tenantID, projectID, appID, targetService, false)
	mock.ExpectExec(`(?s)UPDATE fugue_backing_services`).
		WithArgs(
			targetService, tenantID, projectID, appID, "demo-postgres", "", model.BackingServiceTypePostgres,
			model.BackingServiceProvisionerManaged, model.BackingServiceStatusActive,
			jsonArgument(func(raw []byte) bool {
				var spec model.BackingServiceSpec
				return json.Unmarshal(raw, &spec) == nil && spec.Postgres != nil && spec.Postgres.Suspended
			}), nil, nil, now, sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`(?s)UPDATE fugue_operations`).
		WithArgs(
			opID, tenantID, model.OperationTypeDatabaseSuspend, model.OperationStatusCompleted,
			model.ExecutionModeManaged, "", "", appID, targetService, "runtime_us", "runtime_us", nil,
			jsonArgument(func(raw []byte) bool {
				var spec model.AppSpec
				return json.Unmarshal(raw, &spec) == nil && spec.Image == "ghcr.io/example/original:1" &&
					spec.Replicas == 0 && spec.Postgres == nil
			}), sqlmock.AnyArg(), "database suspended", "/tmp/managed-app.yaml", "", "", now,
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`(?s)UPDATE fugue_apps`).
		WithArgs(
			appID, tenantID, projectID, "demo", "", sqlmock.AnyArg(), sqlmock.AnyArg(),
			jsonArgument(func(raw []byte) bool {
				var spec model.AppSpec
				return json.Unmarshal(raw, &spec) == nil && spec.Image == "ghcr.io/example/original:1" &&
					spec.Replicas == 0 && spec.RuntimeID == "runtime_us" && spec.Postgres == nil && spec.Env["MUTATED"] == ""
			}), sqlmock.AnyArg(), now, sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	malicious := model.AppSpec{
		Image:     "ghcr.io/attacker/replaced:latest",
		Replicas:  99,
		RuntimeID: "runtime_attacker",
		Env:       map[string]string{"MUTATED": "true"},
		Postgres:  &model.AppPostgresSpec{Suspended: false, RuntimeID: "runtime_attacker"},
	}
	completed, err := s.CompleteManagedOperationWithResult(
		opID,
		"/tmp/managed-app.yaml",
		"database suspended",
		&malicious,
		&model.AppSource{Type: model.AppSourceTypeDockerImage, ImageRef: "attacker/image"},
	)
	if err != nil {
		t.Fatalf("complete postgres lifecycle operation: %v", err)
	}
	if completed.Status != model.OperationStatusCompleted || completed.ServiceID != targetService {
		t.Fatalf("unexpected completed operation: %+v", completed)
	}
	if completed.DesiredSpec == nil || completed.DesiredSpec.Image != "ghcr.io/example/original:1" || completed.DesiredSpec.Postgres != nil {
		t.Fatalf("completion accepted caller-controlled app spec or lost normalized service intent: %+v", completed.DesiredSpec)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestPGActiveManagedPostgresLifecycleBlocksLaterOperation(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	defer db.Close()
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}

	const (
		tenantID  = "tenant_lifecycle"
		projectID = "project_lifecycle"
		appID     = "app_lifecycle"
		serviceID = "service_target"
	)
	now := time.Date(2026, time.July, 15, 5, 6, 7, 0, time.UTC)
	mock.ExpectBegin()
	expectPGLifecycleAppForUpdate(mock, now, tenantID, projectID, appID, 0,
		pgLifecycleBoundServiceRow(now, tenantID, projectID, appID, serviceID, false),
	)
	expectPGLifecycleAppHydration(mock, appID,
		pgLifecycleBoundServiceRow(now, tenantID, projectID, appID, serviceID, false),
	)
	mock.ExpectQuery(`(?s)SELECT EXISTS \(.*FROM fugue_operations.*type IN \(\$1, \$2\).*AND app_id = \$6`).
		WithArgs(
			model.OperationTypeDatabaseSuspend, model.OperationTypeDatabaseResume,
			model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent, appID,
		).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectRollback()

	one := 1
	_, err = s.CreateOperation(model.Operation{
		TenantID:        tenantID,
		Type:            model.OperationTypeScale,
		AppID:           appID,
		DesiredReplicas: &one,
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected active lifecycle to block later scale, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestPGBindBackingServiceRejectsLifecycleLeaseAndSuspendedDatabase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		suspended         bool
		replicas          int
		activeLifecycle   bool
		expectLeaseLookup bool
	}{
		{name: "active lifecycle blocks running app bind", replicas: 1, activeLifecycle: true, expectLeaseLookup: true},
		{name: "suspended database blocks stopped app bind", replicas: 0, suspended: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("create sqlmock db: %v", err)
			}
			defer db.Close()
			s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
			now := time.Date(2026, time.July, 15, 5, 7, 8, 0, time.UTC)

			mock.ExpectBegin()
			expectPGLifecycleAppForUpdate(mock, now, "tenant_lifecycle", "project_lifecycle", "app_consumer", test.replicas)
			expectPGLifecycleBackingServiceForUpdate(
				mock, now, "tenant_lifecycle", "project_lifecycle", "app_owner", "service_target", test.suspended,
			)
			if test.expectLeaseLookup {
				mock.ExpectQuery(`(?s)SELECT EXISTS \(.*FROM fugue_operations.*type IN \(\$1, \$2\).*AND service_id = \$6`).
					WithArgs(
						model.OperationTypeDatabaseSuspend, model.OperationTypeDatabaseResume,
						model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent, "service_target",
					).
					WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(test.activeLifecycle))
			}
			mock.ExpectRollback()

			if _, err := s.BindBackingService("tenant_lifecycle", "app_consumer", "service_target", "postgres", nil); !errors.Is(err, ErrConflict) {
				t.Fatalf("expected bind conflict, got %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("sqlmock expectations: %v", err)
			}
		})
	}
}

func TestPGActiveManagedPostgresLifecycleBlocksServiceUpdateAndDelete(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		call func(*Store) error
	}{
		{
			name: "update",
			call: func(s *Store) error {
				_, err := s.UpdateBackingServiceSpec("service_target", model.BackingServiceSpec{
					Postgres: &model.AppPostgresSpec{RuntimeID: "runtime_us", StorageSize: "2Gi", Instances: 1},
				})
				return err
			},
		},
		{
			name: "delete",
			call: func(s *Store) error {
				_, err := s.DeleteBackingService("service_target")
				return err
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("create sqlmock db: %v", err)
			}
			defer db.Close()
			s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
			now := time.Date(2026, time.July, 15, 6, 7, 8, 0, time.UTC)
			mock.ExpectBegin()
			expectPGLifecycleBackingServiceForUpdate(mock, now, "tenant_lifecycle", "project_lifecycle", "app_lifecycle", "service_target", false)
			mock.ExpectQuery(`(?s)SELECT EXISTS \(.*FROM fugue_operations.*type IN \(\$1, \$2\).*AND service_id = \$6`).
				WithArgs(
					model.OperationTypeDatabaseSuspend, model.OperationTypeDatabaseResume,
					model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent, "service_target",
				).
				WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
			mock.ExpectRollback()

			if err := test.call(s); !errors.Is(err, ErrConflict) {
				t.Fatalf("expected active lifecycle conflict, got %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("sqlmock expectations: %v", err)
			}
		})
	}
}

func TestPGActiveManagedPostgresLifecycleBlocksUnbind(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	defer db.Close()
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}

	const (
		tenantID  = "tenant_lifecycle"
		projectID = "project_lifecycle"
		appID     = "app_lifecycle"
		serviceID = "service_target"
		bindingID = "binding_target"
	)
	now := time.Date(2026, time.July, 15, 7, 8, 9, 0, time.UTC)
	mock.ExpectBegin()
	mock.ExpectQuery(`(?s)FROM fugue_service_bindings.*WHERE id = \$1\s*$`).
		WithArgs(bindingID).
		WillReturnRows(pgLifecycleBindingRows().AddRow(bindingID, tenantID, appID, serviceID, "postgres", []byte(`{}`), now, now))
	expectPGLifecycleAppForUpdate(mock, now, tenantID, projectID, appID, 0,
		pgLifecycleBoundServiceRow(now, tenantID, projectID, appID, serviceID, false),
	)
	expectPGLifecycleBackingServiceForUpdate(mock, now, tenantID, projectID, appID, serviceID, false)
	mock.ExpectQuery(`(?s)SELECT EXISTS \(.*FROM fugue_operations.*type IN \(\$1, \$2\).*AND app_id = \$6.*AND service_id = \$7`).
		WithArgs(
			model.OperationTypeDatabaseSuspend, model.OperationTypeDatabaseResume,
			model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent, appID, serviceID,
		).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectRollback()

	_, err = s.UnbindBackingService(bindingID)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected active lifecycle to block unbind, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestPGSuspendedManagedPostgresPreservesRecoveryBinding(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	defer db.Close()
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	const (
		tenantID  = "tenant_lifecycle"
		projectID = "project_lifecycle"
		appID     = "app_lifecycle"
		serviceID = "service_target"
		bindingID = "binding_target"
	)
	now := time.Date(2026, time.July, 15, 7, 9, 10, 0, time.UTC)
	mock.ExpectBegin()
	mock.ExpectQuery(`(?s)FROM fugue_service_bindings.*WHERE id = \$1\s*$`).
		WithArgs(bindingID).
		WillReturnRows(pgLifecycleBindingRows().AddRow(bindingID, tenantID, appID, serviceID, "postgres", []byte(`{}`), now, now))
	expectPGLifecycleAppForUpdate(mock, now, tenantID, projectID, appID, 0,
		pgLifecycleBoundServiceRow(now, tenantID, projectID, appID, serviceID, true),
	)
	expectPGLifecycleBackingServiceForUpdate(mock, now, tenantID, projectID, appID, serviceID, true)
	mock.ExpectRollback()

	if _, err := s.UnbindBackingService(bindingID); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected suspended database recovery binding to be preserved, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

type jsonArgument func([]byte) bool

func (matcher jsonArgument) Match(value driver.Value) bool {
	var raw []byte
	switch typed := value.(type) {
	case []byte:
		raw = typed
	case string:
		raw = []byte(typed)
	default:
		return false
	}
	return matcher(raw)
}

func pgLifecycleOperationRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"id", "tenant_id", "type", "status", "execution_mode", "requested_by_type", "requested_by_id", "app_id", "service_id", "source_runtime_id", "target_runtime_id", "desired_replicas", "desired_spec_json", "desired_source_json", "result_message", "manifest_path", "assigned_runtime_id", "error_message", "created_at", "updated_at", "started_at", "completed_at",
	})
}

func expectPGActiveLifecycleOperationsForTarget(mock sqlmock.Sqlmock, appID, serviceID string, rows *sqlmock.Rows) {
	mock.ExpectQuery(`(?s)SELECT id, tenant_id, type, status, execution_mode.*FROM fugue_operations.*WHERE \(app_id = \$1 OR service_id = \$2\).*status IN \(\$3, \$4, \$5\).*ORDER BY created_at ASC, id ASC`).
		WithArgs(appID, serviceID, model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent).
		WillReturnRows(rows)
}

func expectPGNoActiveAppDatabaseBackup(mock sqlmock.Sqlmock, appID, serviceName, postgresServiceName string) {
	mock.ExpectQuery(`(?s)SELECT EXISTS \(.*FROM fugue_backup_runs.*status IN \(\$1, \$2\).*target_type = \$3.*COALESCE\(app_id, ''\) = \$4.*target_json->>'service_name'.*= \$5.*= \$6`).
		WithArgs(
			model.BackupRunStatusPending,
			model.BackupRunStatusRunning,
			model.BackupTargetAppDatabase,
			appID,
			serviceName,
			postgresServiceName,
		).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
}

func expectPGNoActiveAppDatabaseImport(mock sqlmock.Sqlmock, appID string) {
	mock.ExpectQuery(`(?s)SELECT EXISTS \(.*FROM fugue_app_database_import_jobs.*WHERE app_id = \$1.*status IN \(\$2, \$3\)`).
		WithArgs(appID, model.OperationStatusPending, model.OperationStatusRunning).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
}

func pgLifecycleBindingRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"id", "tenant_id", "app_id", "service_id", "alias", "env_json", "created_at", "updated_at"})
}

func pgLifecycleBoundServiceColumns() []string {
	return []string{
		"binding_id", "binding_tenant_id", "binding_app_id", "binding_service_id", "binding_alias", "binding_env_json", "binding_created_at", "binding_updated_at",
		"service_id", "service_tenant_id", "service_project_id", "service_owner_app_id", "service_name", "service_description", "service_type", "service_provisioner", "service_status", "service_spec_json", "service_current_runtime_started_at", "service_current_runtime_ready_at", "service_created_at", "service_updated_at",
	}
}

func pgLifecycleBoundServiceRow(now time.Time, tenantID, projectID, appID, serviceID string, suspended bool) []driver.Value {
	spec, _ := json.Marshal(model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{
		Database: "demo", User: "demo", Password: "secret", ServiceName: "demo-postgres",
		RuntimeID: "runtime_us", StorageSize: "1Gi", Instances: 1, Suspended: suspended,
	}})
	return []driver.Value{
		"binding_" + serviceID, tenantID, appID, serviceID, "postgres", []byte(`{}`), now, now,
		serviceID, tenantID, projectID, appID, "demo-postgres", "", model.BackingServiceTypePostgres,
		model.BackingServiceProvisionerManaged, model.BackingServiceStatusActive, spec, nil, nil, now, now,
	}
}

func expectPGLifecycleAppForUpdate(mock sqlmock.Sqlmock, now time.Time, tenantID, projectID, appID string, replicas int, serviceRows ...[]driver.Value) {
	mock.ExpectQuery(`(?s)FROM fugue_apps.*WHERE id = \$1.*FOR UPDATE`).
		WithArgs(appID).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "project_id", "name", "description", "source_json", "route_json", "spec_json", "status_json", "created_at", "updated_at",
		}).AddRow(
			appID, tenantID, projectID, "demo", "", []byte("null"), []byte("null"),
			[]byte(`{"image":"ghcr.io/example/original:1","replicas":`+strconv.Itoa(replicas)+`,"runtime_id":"runtime_us"}`),
			[]byte(`{"phase":"disabled","current_runtime_id":"runtime_us","current_replicas":0,"updated_at":"2026-07-15T03:04:05Z"}`),
			now, now,
		))
	expectPGLifecycleAppHydration(mock, appID, serviceRows...)
}

func expectPGLifecycleAppHydration(mock sqlmock.Sqlmock, appID string, serviceRows ...[]driver.Value) {
	rows := sqlmock.NewRows(pgLifecycleBoundServiceColumns())
	for _, row := range serviceRows {
		rows.AddRow(row...)
	}
	mock.ExpectQuery(`(?s)FROM fugue_service_bindings AS b.*JOIN fugue_backing_services AS s.*WHERE b.app_id = \$1`).
		WithArgs(appID).
		WillReturnRows(rows)
}

func expectPGLifecycleBackingServiceForUpdate(mock sqlmock.Sqlmock, now time.Time, tenantID, projectID, appID, serviceID string, suspended bool) {
	spec, _ := json.Marshal(model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{
		Database: "demo", User: "demo", Password: "secret", ServiceName: "demo-postgres",
		RuntimeID: "runtime_us", StorageSize: "1Gi", Instances: 1, Suspended: suspended,
	}})
	mock.ExpectQuery(`(?s)FROM fugue_backing_services.*WHERE id = \$1.*FOR UPDATE`).
		WithArgs(serviceID).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "project_id", "owner_app_id", "name", "description", "type", "provisioner", "status", "spec_json", "current_runtime_started_at", "current_runtime_ready_at", "created_at", "updated_at",
		}).AddRow(
			serviceID, tenantID, projectID, appID, "demo-postgres", "", model.BackingServiceTypePostgres,
			model.BackingServiceProvisionerManaged, model.BackingServiceStatusActive, spec, nil, nil, now, now,
		))
}

func expectPGLifecycleBillingAccrual(mock sqlmock.Sqlmock, now time.Time, tenantID, projectID, appID, serviceID string) {
	future := time.Date(2099, time.January, 1, 0, 0, 0, 0, time.UTC)
	record := defaultTenantBilling(tenantID, future)
	capJSON, _ := json.Marshal(record.ManagedCap)
	priceJSON, _ := json.Marshal(record.PriceBook)
	mock.ExpectQuery(`(?s)FROM fugue_tenant_billing.*WHERE tenant_id = \$1.*FOR UPDATE`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{
			"tenant_id", "managed_cap_json", "managed_image_storage_gibibytes", "balance_microcents", "price_book_json", "last_accrued_at", "created_at", "updated_at",
		}).AddRow(
			record.TenantID, capJSON, record.ManagedImageStorageGibibytes, record.BalanceMicroCents,
			priceJSON, record.LastAccruedAt, record.CreatedAt, record.UpdatedAt,
		))
	mock.ExpectQuery(`(?s)FROM fugue_apps.*WHERE tenant_id = \$1`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "project_id", "name", "description", "source_json", "route_json", "spec_json", "status_json", "created_at", "updated_at",
		}).AddRow(
			appID, tenantID, projectID, "demo", "", []byte("null"), []byte("null"),
			[]byte(`{"image":"ghcr.io/example/original:1","replicas":0,"runtime_id":"runtime_us"}`),
			[]byte(`{"phase":"disabled","current_runtime_id":"runtime_us","current_replicas":0,"updated_at":"2026-07-15T03:04:05Z"}`),
			now, now,
		))
	serviceSpec, _ := json.Marshal(model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{
		Database: "demo", User: "demo", Password: "secret", ServiceName: "demo-postgres",
		RuntimeID: "runtime_us", StorageSize: "1Gi", Instances: 1,
	}})
	mock.ExpectQuery(`(?s)FROM fugue_backing_services.*WHERE tenant_id = \$1`).
		WithArgs(tenantID).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "project_id", "owner_app_id", "name", "description", "type", "provisioner", "status", "spec_json", "current_runtime_started_at", "current_runtime_ready_at", "created_at", "updated_at",
		}).AddRow(
			serviceID, tenantID, projectID, appID, "demo-postgres", "", model.BackingServiceTypePostgres,
			model.BackingServiceProvisionerManaged, model.BackingServiceStatusActive, serviceSpec, nil, nil, now, now,
		))
	mock.ExpectQuery(`(?s)FROM fugue_service_bindings.*WHERE tenant_id = \$1`).
		WithArgs(tenantID).
		WillReturnRows(pgLifecycleBindingRows().AddRow(
			"binding_"+serviceID, tenantID, appID, serviceID, "postgres", []byte(`{}`), now, now,
		))
	mock.ExpectQuery(`(?s)FROM fugue_runtimes.*ORDER BY created_at ASC`).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "name", "machine_name", "type", "access_mode", "public_offer_json", "pool_mode", "connection_mode", "status", "endpoint", "labels_json", "node_key_id", "cluster_node_name", "fingerprint_prefix", "fingerprint_hash", "agent_key_prefix", "agent_key_hash", "last_seen_at", "last_heartbeat_at", "created_at", "updated_at",
		}).AddRow(
			"runtime_us", nil, "US managed", nil, model.RuntimeTypeManagedShared, model.RuntimeAccessModePlatformShared,
			[]byte(`{}`), nil, nil, model.RuntimeStatusActive, nil, []byte(`{}`), nil, nil, nil, nil, nil, nil, nil, nil, now, now,
		))
	mock.ExpectQuery(`(?s)FROM fugue_billing_events.*WHERE tenant_id = \$1.*LIMIT \$2`).
		WithArgs(tenantID, billingHistoryLimit).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "type", "amount_microcents", "balance_after_microcents", "metadata_json", "created_at",
		}))
	mock.ExpectExec(`(?s)UPDATE fugue_tenant_billing`).
		WillReturnResult(sqlmock.NewResult(0, 1))
}
