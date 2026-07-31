package store

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"fugue/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

func pgPlacementBoundServiceRow(
	now time.Time,
	tenantID, projectID, appID, serviceID, runtimeID, nodeName string,
) []driver.Value {
	return pgPlacementBoundServiceRowForSpec(now, tenantID, projectID, appID, serviceID, model.AppPostgresSpec{
		Database: "demo", User: "demo", Password: "secret", ServiceName: "demo-postgres",
		RuntimeID: runtimeID, PrimaryNodeName: nodeName, StorageSize: "1Gi", Instances: 1,
	})
}

func pgPlacementBoundServiceRowForSpec(
	now time.Time,
	tenantID, projectID, appID, serviceID string,
	postgres model.AppPostgresSpec,
) []driver.Value {
	spec, _ := json.Marshal(model.BackingServiceSpec{Postgres: model.CloneAppPostgresSpec(&postgres)})
	return []driver.Value{
		"binding_" + serviceID, tenantID, appID, serviceID, "postgres", []byte(`{}`), now, now,
		serviceID, tenantID, projectID, nil, "demo-postgres", "", model.BackingServiceTypePostgres,
		model.BackingServiceProvisionerManaged, model.BackingServiceStatusActive, spec, nil, nil, now, now,
	}
}

func pgPlacementBackingServiceRows(
	now time.Time,
	tenantID, projectID, serviceID, runtimeID, nodeName string,
) *sqlmock.Rows {
	return pgPlacementBackingServiceRowsForSpec(now, tenantID, projectID, serviceID, model.AppPostgresSpec{
		Database: "demo", User: "demo", Password: "secret", ServiceName: "demo-postgres",
		RuntimeID: runtimeID, PrimaryNodeName: nodeName, StorageSize: "1Gi", Instances: 1,
	})
}

func pgPlacementBackingServiceRowsForSpec(
	now time.Time,
	tenantID, projectID, serviceID string,
	postgres model.AppPostgresSpec,
) *sqlmock.Rows {
	spec, _ := json.Marshal(model.BackingServiceSpec{Postgres: model.CloneAppPostgresSpec(&postgres)})
	return sqlmock.NewRows([]string{
		"id", "tenant_id", "project_id", "owner_app_id", "name", "description", "type", "provisioner", "status", "spec_json", "current_runtime_started_at", "current_runtime_ready_at", "created_at", "updated_at",
	}).AddRow(
		serviceID, tenantID, projectID, nil, "demo-postgres", "", model.BackingServiceTypePostgres,
		model.BackingServiceProvisionerManaged, model.BackingServiceStatusActive, spec, nil, nil, now, now,
	)
}

func expectPGPlacementLockedServiceAndBindingForSpec(
	mock sqlmock.Sqlmock,
	now time.Time,
	tenantID, projectID, appID, serviceID string,
	postgres model.AppPostgresSpec,
) {
	mock.ExpectQuery(`(?s)FROM fugue_backing_services.*WHERE id = \$1.*FOR UPDATE`).
		WithArgs(serviceID).
		WillReturnRows(pgPlacementBackingServiceRowsForSpec(now, tenantID, projectID, serviceID, postgres))
	mock.ExpectQuery(`(?s)FROM fugue_service_bindings.*WHERE app_id = \$1.*service_id = \$2.*FOR UPDATE`).
		WithArgs(appID, serviceID).
		WillReturnRows(pgLifecycleBindingRows().AddRow(
			"binding_"+serviceID, tenantID, appID, serviceID, "postgres", []byte(`{}`), now, now,
		))
	mock.ExpectQuery(`(?s)SELECT COUNT\(1\).*FROM fugue_service_bindings.*WHERE service_id = \$1`).
		WithArgs(serviceID).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
}

func pgPlacementMutation(
	tenantID, projectID, appID, serviceID string,
	expected, desired model.AppPostgresSpec,
	runtimeID, nodeName string,
) ManagedPostgresPlacementMutation {
	return ManagedPostgresPlacementMutation{
		Witness: ManagedPostgresPlacementWitness{
			AppID: appID, TenantID: tenantID, ProjectID: projectID, ServiceID: serviceID,
			ServiceName: "demo-postgres", RuntimeID: runtimeID, NodeName: nodeName,
			PrimaryPod: "demo-postgres-2", PodIP: "10.42.0.22",
		},
		Expected: ManagedPostgresPlacementStateFromSpec(expected),
		Desired:  ManagedPostgresPlacementStateFromSpec(desired),
	}
}

func expectPGPlacementActiveCount(mock sqlmock.Sqlmock, appID, serviceID string, count int) {
	mock.ExpectQuery(`(?s)SELECT COUNT\(1\).*FROM fugue_operations.*WHERE \(app_id = \$1 OR \(\$2 <> '' AND service_id = \$2\)\).*status IN \(\$3, \$4, \$5\)`).
		WithArgs(appID, serviceID, model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(count))
}

func expectPGPlacementLockedServiceAndBinding(
	mock sqlmock.Sqlmock,
	now time.Time,
	tenantID, projectID, appID, serviceID, runtimeID, nodeName string,
) {
	expectPGPlacementLockedServiceAndBindingForSpec(mock, now, tenantID, projectID, appID, serviceID, model.AppPostgresSpec{
		Database: "demo", User: "demo", Password: "secret", ServiceName: "demo-postgres",
		RuntimeID: runtimeID, PrimaryNodeName: nodeName, StorageSize: "1Gi", Instances: 1,
	})
}

func expectPGPlacementRuntimeForUpdate(
	mock sqlmock.Sqlmock,
	now time.Time,
	tenantID, runtimeID, status string,
) {
	mock.ExpectQuery(`(?s)FROM fugue_runtimes.*WHERE id = \$1.*FOR UPDATE`).
		WithArgs(runtimeID).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "name", "machine_name", "type", "access_mode", "public_offer_json", "pool_mode", "connection_mode", "status", "endpoint", "labels_json", "node_key_id", "cluster_node_name", "fingerprint_prefix", "fingerprint_hash", "agent_key_prefix", "agent_key_hash", "last_seen_at", "last_heartbeat_at", "created_at", "updated_at",
		}).AddRow(
			runtimeID, tenantID, "source-runtime", nil, model.RuntimeTypeManagedOwned, nil, []byte(`{}`), nil, nil,
			status, nil, []byte(`{}`), nil, nil, nil, nil, nil, nil, nil, nil, now, now,
		))
}

func TestPGSyncObservedManagedPostgresPlacementPersistsBoundNodeInOneTransaction(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create placement sqlmock db: %v", err)
	}
	defer db.Close()
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	const (
		tenantID  = "tenant_placement"
		projectID = "project_placement"
		appID     = "app_placement"
		serviceID = "service_placement"
	)
	now := time.Date(2026, time.July, 30, 10, 0, 0, 0, time.UTC)
	current := model.AppPostgresSpec{
		Database: "demo", User: "demo", Password: "secret", ServiceName: "demo-postgres",
		RuntimeID: "runtime_us", PrimaryNodeName: "node_us_old", StorageSize: "1Gi", Instances: 1,
	}
	desired := *model.CloneAppPostgresSpec(&current)
	desired.PrimaryNodeName = "node_us_current"

	mock.ExpectBegin()
	expectPGLifecycleAppForUpdate(mock, now, tenantID, projectID, appID, 1,
		pgPlacementBoundServiceRow(now, tenantID, projectID, appID, serviceID, current.RuntimeID, current.PrimaryNodeName),
	)
	expectPGPlacementActiveCount(mock, appID, serviceID, 0)
	expectPGPlacementLockedServiceAndBinding(mock, now, tenantID, projectID, appID, serviceID, current.RuntimeID, current.PrimaryNodeName)
	mock.ExpectExec(`(?s)UPDATE fugue_backing_services.*spec_json`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`(?s)UPDATE fugue_apps.*spec_json`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	expectPGLifecycleAppHydration(mock, appID,
		pgPlacementBoundServiceRow(now, tenantID, projectID, appID, serviceID, desired.RuntimeID, desired.PrimaryNodeName),
	)

	updated, err := s.SyncObservedManagedPostgresPlacement(
		pgPlacementMutation(tenantID, projectID, appID, serviceID, current, desired, desired.RuntimeID, desired.PrimaryNodeName),
	)
	if err != nil {
		t.Fatalf("sync PostgreSQL-backed placement: %v", err)
	}
	postgres := OwnedManagedPostgresSpec(updated)
	if postgres == nil || postgres.RuntimeID != desired.RuntimeID || postgres.PrimaryNodeName != desired.PrimaryNodeName {
		t.Fatalf("PostgreSQL-backed placement did not persist exact bound target: %+v", postgres)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("placement SQL expectations: %v", err)
	}
}

func TestPGSyncObservedManagedPostgresPlacementConsumesOfflineFailoverAtomically(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create failover placement sqlmock db: %v", err)
	}
	defer db.Close()
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	const (
		tenantID  = "tenant_failover_placement"
		projectID = "project_failover_placement"
		appID     = "app_failover_placement"
		serviceID = "service_failover_placement"
	)
	now := time.Date(2026, time.July, 30, 10, 30, 0, 0, time.UTC)
	current := model.AppPostgresSpec{
		Database: "demo", User: "demo", Password: "secret", ServiceName: "demo-postgres",
		RuntimeID: "runtime_us", FailoverTargetRuntimeID: "runtime_de", PrimaryNodeName: "node_us",
		StorageSize: "1Gi", Instances: 2, SynchronousReplicas: 1,
	}
	desired := *model.CloneAppPostgresSpec(&current)
	desired.RuntimeID = "runtime_de"
	desired.FailoverTargetRuntimeID = ""
	desired.PrimaryNodeName = "node_de"
	desired.Instances = 1
	desired.SynchronousReplicas = 0
	desired.PrimaryPlacementPendingRebalance = false

	mock.ExpectBegin()
	expectPGLifecycleAppForUpdate(mock, now, tenantID, projectID, appID, 1,
		pgPlacementBoundServiceRowForSpec(now, tenantID, projectID, appID, serviceID, current),
	)
	expectPGPlacementActiveCount(mock, appID, serviceID, 0)
	expectPGPlacementLockedServiceAndBindingForSpec(mock, now, tenantID, projectID, appID, serviceID, current)
	expectPGPlacementRuntimeForUpdate(mock, now, tenantID, current.RuntimeID, model.RuntimeStatusOffline)
	mock.ExpectExec(`(?s)UPDATE fugue_backing_services.*spec_json`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`(?s)UPDATE fugue_apps.*spec_json`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	expectPGLifecycleAppHydration(mock, appID,
		pgPlacementBoundServiceRowForSpec(now, tenantID, projectID, appID, serviceID, desired),
	)

	updated, err := s.SyncObservedManagedPostgresPlacement(
		pgPlacementMutation(tenantID, projectID, appID, serviceID, current, desired, desired.RuntimeID, desired.PrimaryNodeName),
	)
	if err != nil {
		t.Fatalf("consume PostgreSQL-backed offline failover: %v", err)
	}
	postgres := OwnedManagedPostgresSpec(updated)
	if postgres == nil || postgres.RuntimeID != desired.RuntimeID || postgres.FailoverTargetRuntimeID != "" ||
		postgres.PrimaryNodeName != desired.PrimaryNodeName {
		t.Fatalf("offline failover was not persisted atomically: %+v", postgres)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("offline failover placement SQL expectations: %v", err)
	}
}

func TestPGCompleteManagedPostgresSwitchoverPersistsPlacementAndCompletedAtomically(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create switchover placement sqlmock db: %v", err)
	}
	defer db.Close()
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	const (
		tenantID    = "tenant_switchover"
		projectID   = "project_switchover"
		appID       = "app_switchover"
		serviceID   = "service_switchover"
		operationID = "operation_switchover"
	)
	now := time.Date(2026, time.July, 30, 11, 0, 0, 0, time.UTC)
	current := model.AppPostgresSpec{
		Database: "demo", User: "demo", Password: "secret", ServiceName: "demo-postgres",
		RuntimeID: "runtime_us", PrimaryNodeName: "node_us", StorageSize: "1Gi", Instances: 1,
	}
	desired := *model.CloneAppPostgresSpec(&current)
	desired.RuntimeID = "runtime_de"
	desired.FailoverTargetRuntimeID = current.RuntimeID
	desired.PrimaryNodeName = "node_de"
	desired.Instances = 2
	desired.SynchronousReplicas = 1

	mock.ExpectBegin()
	mock.ExpectQuery(`(?s)FROM fugue_operations.*WHERE id = \$1.*FOR UPDATE`).
		WithArgs(operationID).
		WillReturnRows(pgLifecycleOperationRows().AddRow(
			operationID, tenantID, model.OperationTypeDatabaseSwitchover, model.OperationStatusRunning,
			model.ExecutionModeManaged, model.ActorTypeAPIKey, "requester", appID, serviceID,
			current.RuntimeID, desired.RuntimeID, nil, nil, nil, "switching", "", "", "",
			now, now, now, nil,
		))
	expectPGLifecycleAppForUpdate(mock, now, tenantID, projectID, appID, 1,
		pgPlacementBoundServiceRow(now, tenantID, projectID, appID, serviceID, current.RuntimeID, current.PrimaryNodeName),
	)
	expectPGPlacementActiveCount(mock, appID, serviceID, 1)
	expectPGPlacementLockedServiceAndBinding(mock, now, tenantID, projectID, appID, serviceID, current.RuntimeID, current.PrimaryNodeName)
	mock.ExpectExec(`(?s)UPDATE fugue_backing_services.*spec_json`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`(?s)UPDATE fugue_operations.*status`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`(?s)UPDATE fugue_apps.*spec_json`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	completed, err := s.CompleteManagedPostgresSwitchoverWithPlacement(
		operationID, "/tmp/switchover.yaml", "switchover complete",
		pgPlacementMutation(tenantID, projectID, appID, serviceID, current, desired, desired.RuntimeID, desired.PrimaryNodeName),
	)
	if err != nil {
		t.Fatalf("complete PostgreSQL-backed switchover placement: %v", err)
	}
	if completed.Status != model.OperationStatusCompleted || completed.CompletedAt == nil ||
		completed.TargetRuntimeID != desired.RuntimeID {
		t.Fatalf("PostgreSQL-backed switchover did not complete: %+v", completed)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("switchover placement SQL expectations: %v", err)
	}
}

func TestPGCompleteManagedPostgresSwitchoverRejectsCompetingActiveOperationBeforeWrite(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create competing placement sqlmock db: %v", err)
	}
	defer db.Close()
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	const (
		tenantID    = "tenant_competing"
		projectID   = "project_competing"
		appID       = "app_competing"
		serviceID   = "service_competing"
		operationID = "operation_competing"
	)
	now := time.Date(2026, time.July, 30, 12, 0, 0, 0, time.UTC)
	current := model.AppPostgresSpec{ServiceName: "demo-postgres", RuntimeID: "runtime_us", PrimaryNodeName: "node_us", Instances: 1}
	desired := *model.CloneAppPostgresSpec(&current)
	desired.RuntimeID = "runtime_de"
	desired.FailoverTargetRuntimeID = current.RuntimeID
	desired.PrimaryNodeName = "node_de"
	desired.Instances = 2
	desired.SynchronousReplicas = 1

	mock.ExpectBegin()
	mock.ExpectQuery(`(?s)FROM fugue_operations.*WHERE id = \$1.*FOR UPDATE`).
		WithArgs(operationID).
		WillReturnRows(pgLifecycleOperationRows().AddRow(
			operationID, tenantID, model.OperationTypeDatabaseSwitchover, model.OperationStatusRunning,
			model.ExecutionModeManaged, model.ActorTypeAPIKey, "requester", appID, serviceID,
			current.RuntimeID, desired.RuntimeID, nil, nil, nil, "switching", "", "", "",
			now, now, now, nil,
		))
	expectPGLifecycleAppForUpdate(mock, now, tenantID, projectID, appID, 1,
		pgPlacementBoundServiceRow(now, tenantID, projectID, appID, serviceID, current.RuntimeID, current.PrimaryNodeName),
	)
	expectPGPlacementActiveCount(mock, appID, serviceID, 2)
	mock.ExpectRollback()

	_, err = s.CompleteManagedPostgresSwitchoverWithPlacement(
		operationID, "/tmp/switchover.yaml", "switchover complete",
		pgPlacementMutation(tenantID, projectID, appID, serviceID, current, desired, desired.RuntimeID, desired.PrimaryNodeName),
	)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("competing operation completion error = %v, want conflict", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("competing placement SQL expectations: %v", err)
	}
}
