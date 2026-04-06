package store

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPGGetAppTxHydratesBackingServicesWithinTransaction(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	defer db.Close()

	s := &Store{
		databaseURL: "postgres://example",
		db:          db,
		dbReady:     true,
	}

	const (
		appID       = "app_123"
		tenantID    = "tenant_123"
		projectID   = "project_123"
		serviceID   = "app-postgres-app_123"
		runtimeID   = "runtime_managed_shared"
		serviceName = "demo-postgres"
	)

	now := time.Date(2026, time.April, 6, 12, 20, 30, 0, time.UTC)
	appSpecJSON := `{"image":"nginx:1.27","ports":[80],"replicas":1,"runtime_id":"runtime_managed_shared"}`
	appStatusJSON := `{"phase":"deployed","current_runtime_id":"runtime_managed_shared","current_replicas":1,"updated_at":"2026-04-06T12:20:30Z"}`
	bindingEnvJSON := `{"DB_HOST":"demo-postgres-rw","DB_NAME":"demo","DB_PASSWORD":"secret","DB_PORT":"5432","DB_TYPE":"postgres","DB_USER":"demo"}`
	serviceSpecJSON := `{"postgres":{"database":"demo","user":"demo","password":"secret","service_name":"demo-postgres","runtime_id":"runtime_managed_shared","storage_size":"1Gi","instances":1}}`

	mock.ExpectBegin()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT id, tenant_id, project_id, name, description, source_json, route_json, spec_json, status_json, created_at, updated_at
FROM fugue_apps
WHERE id = $1
 FOR UPDATE`)).
		WithArgs(appID).
		WillReturnRows(sqlmock.NewRows([]string{
			"id",
			"tenant_id",
			"project_id",
			"name",
			"description",
			"source_json",
			"route_json",
			"spec_json",
			"status_json",
			"created_at",
			"updated_at",
		}).AddRow(
			appID,
			tenantID,
			projectID,
			"demo",
			"",
			[]byte("null"),
			[]byte("null"),
			[]byte(appSpecJSON),
			[]byte(appStatusJSON),
			now,
			now,
		))

	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT b.id, b.tenant_id, b.app_id, b.service_id, b.alias, b.env_json, b.created_at, b.updated_at,
       s.id, s.tenant_id, s.project_id, s.owner_app_id, s.name, s.description, s.type, s.provisioner, s.status, s.spec_json, s.current_runtime_started_at, s.current_runtime_ready_at, s.created_at, s.updated_at
FROM fugue_service_bindings AS b
JOIN fugue_backing_services AS s ON s.id = b.service_id
WHERE b.app_id = $1
ORDER BY b.created_at ASC, s.created_at ASC
`)).
		WithArgs(appID).
		WillReturnRows(sqlmock.NewRows([]string{
			"binding_id",
			"binding_tenant_id",
			"binding_app_id",
			"binding_service_id",
			"binding_alias",
			"binding_env_json",
			"binding_created_at",
			"binding_updated_at",
			"service_id",
			"service_tenant_id",
			"service_project_id",
			"service_owner_app_id",
			"service_name",
			"service_description",
			"service_type",
			"service_provisioner",
			"service_status",
			"service_spec_json",
			"service_current_runtime_started_at",
			"service_current_runtime_ready_at",
			"service_created_at",
			"service_updated_at",
		}).AddRow(
			"binding_123",
			tenantID,
			appID,
			serviceID,
			"postgres",
			[]byte(bindingEnvJSON),
			now,
			now,
			serviceID,
			tenantID,
			projectID,
			appID,
			"demo",
			"Managed postgres for demo",
			"postgres",
			"managed",
			"active",
			[]byte(serviceSpecJSON),
			nil,
			nil,
			now,
			now,
		))

	app, err := s.pgGetAppTx(context.Background(), tx, appID, true)
	if err != nil {
		t.Fatalf("pgGetAppTx: %v", err)
	}

	if app.Spec.Postgres != nil {
		t.Fatalf("expected tx-loaded app spec postgres to remain externalized, got %+v", app.Spec.Postgres)
	}
	if len(app.BackingServices) != 1 {
		t.Fatalf("expected 1 hydrated backing service, got %d", len(app.BackingServices))
	}
	if len(app.Bindings) != 1 {
		t.Fatalf("expected 1 hydrated binding, got %d", len(app.Bindings))
	}

	postgresSpec := OwnedManagedPostgresSpec(app)
	if postgresSpec == nil {
		t.Fatal("expected owned managed postgres spec from hydrated backing service")
	}
	if got := postgresSpec.RuntimeID; got != runtimeID {
		t.Fatalf("expected hydrated postgres runtime %q, got %q", runtimeID, got)
	}
	if got := postgresSpec.ServiceName; got != serviceName {
		t.Fatalf("expected hydrated postgres service name %q, got %q", serviceName, got)
	}

	mock.ExpectRollback()
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback tx: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}
