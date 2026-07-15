package store

import (
	"regexp"
	"testing"
	"time"

	"fugue/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPGAdoptOrphanManagedAppExactRepeatUsesHydratedResources(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	defer db.Close()
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}

	createdAt := time.Date(2026, time.July, 15, 1, 2, 3, 0, time.UTC)
	snapshot := model.App{
		ID:          "app_orphan",
		TenantID:    "tenant_orphan",
		ProjectID:   "project_orphan",
		Name:        "orphan",
		Description: "retained data",
		Spec: model.AppSpec{
			Image:     "ghcr.io/example/orphan:latest",
			Replicas:  0,
			RuntimeID: model.DefaultManagedRuntimeID,
		},
		BackingServices: []model.BackingService{{
			ID:          "service_orphan",
			TenantID:    "tenant_orphan",
			ProjectID:   "project_orphan",
			OwnerAppID:  "app_orphan",
			Name:        "orphan-postgres",
			Description: "Managed postgres for orphan",
			Type:        model.BackingServiceTypePostgres,
			Provisioner: model.BackingServiceProvisionerManaged,
			Status:      model.BackingServiceStatusActive,
			Spec: model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{
				Database:    "orphan",
				User:        "orphan",
				Password:    "preserved-secret",
				ServiceName: "orphan-postgres",
				RuntimeID:   model.DefaultManagedRuntimeID,
				StorageSize: "1Gi",
				Instances:   1,
			}},
			CreatedAt: createdAt,
			UpdatedAt: createdAt,
		}},
		Bindings: []model.ServiceBinding{{
			ID:        "binding_orphan",
			TenantID:  "tenant_orphan",
			AppID:     "app_orphan",
			ServiceID: "service_orphan",
			Alias:     "postgres",
			CreatedAt: createdAt,
			UpdatedAt: createdAt,
		}},
		CreatedAt: createdAt,
		UpdatedAt: createdAt,
	}
	prepared, err := prepareOrphanManagedAppAdoption(snapshot, createdAt.Add(time.Minute))
	if err != nil {
		t.Fatalf("prepare orphan snapshot: %v", err)
	}
	sourceJSON, err := marshalAppSourceState(prepared.App)
	if err != nil {
		t.Fatalf("marshal source: %v", err)
	}
	routeJSON, err := marshalNullableJSON(prepared.App.Route)
	if err != nil {
		t.Fatalf("marshal route: %v", err)
	}
	specJSON, err := marshalJSON(prepared.App.Spec)
	if err != nil {
		t.Fatalf("marshal app spec: %v", err)
	}
	statusJSON, err := marshalJSON(prepared.App.Status)
	if err != nil {
		t.Fatalf("marshal app status: %v", err)
	}
	serviceSpecJSON, err := marshalJSON(prepared.Services[0].Spec)
	if err != nil {
		t.Fatalf("marshal service spec: %v", err)
	}
	bindingEnvJSON, err := marshalNullableJSON(prepared.Bindings[0].Env)
	if err != nil {
		t.Fatalf("marshal binding env: %v", err)
	}

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT EXISTS (SELECT 1 FROM fugue_tenants WHERE id = $1)`)).
		WithArgs(prepared.App.TenantID).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT EXISTS (
	SELECT 1
	FROM fugue_projects
	WHERE id = $1 AND tenant_id = $2
)
`)).
		WithArgs(prepared.App.ProjectID, prepared.App.TenantID).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT delete_requested_at
FROM fugue_projects
WHERE id = $1
`)).
		WithArgs(prepared.App.ProjectID).
		WillReturnRows(sqlmock.NewRows([]string{"delete_requested_at"}).AddRow(nil))
	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT type, access_mode, tenant_id
FROM fugue_runtimes
WHERE id = $1
`)).
		WithArgs(model.DefaultManagedRuntimeID).
		WillReturnRows(sqlmock.NewRows([]string{"type", "access_mode", "tenant_id"}).
			AddRow(model.RuntimeTypeManagedShared, model.RuntimeAccessModePlatformShared, nil))
	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT COUNT(1)
FROM fugue_operations
WHERE app_id = $1
  AND status IN ($2, $3, $4)
`)).
		WithArgs(prepared.App.ID, model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery(regexp.QuoteMeta(`
	SELECT id, tenant_id, project_id, name, description, source_json, route_json, spec_json, status_json, created_at, updated_at
	FROM fugue_apps
WHERE id = $1
 FOR UPDATE`)).
		WithArgs(prepared.App.ID).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "tenant_id", "project_id", "name", "description", "source_json", "route_json", "spec_json", "status_json", "created_at", "updated_at",
		}).AddRow(
			prepared.App.ID,
			prepared.App.TenantID,
			prepared.App.ProjectID,
			prepared.App.Name,
			prepared.App.Description,
			sourceJSON,
			routeJSON,
			specJSON,
			statusJSON,
			prepared.App.CreatedAt,
			prepared.App.UpdatedAt,
		))
	mock.ExpectQuery(regexp.QuoteMeta(`
	SELECT b.id, b.tenant_id, b.app_id, b.service_id, b.alias, b.env_json, b.created_at, b.updated_at,
	       s.id, s.tenant_id, s.project_id, s.owner_app_id, s.name, s.description, s.type, s.provisioner, s.status, s.spec_json, s.current_runtime_started_at, s.current_runtime_ready_at, s.created_at, s.updated_at
	FROM fugue_service_bindings AS b
	JOIN fugue_backing_services AS s ON s.id = b.service_id
WHERE b.app_id = $1
ORDER BY b.created_at ASC, s.created_at ASC
`)).
		WithArgs(prepared.App.ID).
		WillReturnRows(sqlmock.NewRows([]string{
			"binding_id", "binding_tenant_id", "binding_app_id", "binding_service_id", "binding_alias", "binding_env_json", "binding_created_at", "binding_updated_at",
			"service_id", "service_tenant_id", "service_project_id", "service_owner_app_id", "service_name", "service_description", "service_type", "service_provisioner", "service_status", "service_spec_json", "service_current_runtime_started_at", "service_current_runtime_ready_at", "service_created_at", "service_updated_at",
		}).AddRow(
			prepared.Bindings[0].ID,
			prepared.Bindings[0].TenantID,
			prepared.Bindings[0].AppID,
			prepared.Bindings[0].ServiceID,
			prepared.Bindings[0].Alias,
			bindingEnvJSON,
			prepared.Bindings[0].CreatedAt,
			prepared.Bindings[0].UpdatedAt,
			prepared.Services[0].ID,
			prepared.Services[0].TenantID,
			prepared.Services[0].ProjectID,
			prepared.Services[0].OwnerAppID,
			prepared.Services[0].Name,
			prepared.Services[0].Description,
			prepared.Services[0].Type,
			prepared.Services[0].Provisioner,
			prepared.Services[0].Status,
			serviceSpecJSON,
			nil,
			nil,
			prepared.Services[0].CreatedAt,
			prepared.Services[0].UpdatedAt,
		))
	mock.ExpectRollback()

	adopted, already, err := s.AdoptOrphanManagedApp(snapshot)
	if err != nil {
		t.Fatalf("repeat postgres adoption: %v", err)
	}
	if !already || adopted.ID != snapshot.ID || len(adopted.BackingServices) != 1 || len(adopted.Bindings) != 1 {
		t.Fatalf("expected hydrated exact repeat to be idempotent, already=%v app=%+v", already, adopted)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}
