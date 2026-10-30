package api

import (
	"database/sql"
	"net/http"
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/storagerecovery"
	"fugue/internal/store"
	"github.com/DATA-DOG/go-sqlmock"
)

func TestQueryAppDatabaseUsesRestoredStableEndpoint(t *testing.T) {
	s, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image: "example.invalid/api:v1", Ports: []int{8080}, Replicas: 1, RuntimeID: "runtime_managed_shared",
		Postgres: &model.AppPostgresSpec{ServiceName: "original-db", Database: "demo", User: "owner", Password: "fixture"},
		Env:      map[string]string{"DATABASE_URL": "postgresql://owner:fixture@original-db-rw:5432/demo?application_name=a"},
	})
	stored, err := s.GetApp(app.ID)
	if err != nil || len(stored.BackingServices) != 1 {
		t.Fatal("managed backing service missing", err)
	}
	service := stored.BackingServices[0]
	current := *service.Spec.Postgres
	next := current
	next.ServiceName, next.EndpointServiceName, next.RuntimeID = "restored-db", "original-db", app.Spec.RuntimeID
	next.StorageClassName = "fixture-storage"
	desired := stored.Spec
	requested := current
	requested.RuntimeID, requested.StorageClassName = next.RuntimeID, next.StorageClassName
	desired.Postgres = &requested
	op, err := s.CreateOperation(model.Operation{TenantID: app.TenantID, AppID: app.ID, ServiceID: service.ID, Type: storagerecovery.OperationType, TargetRuntimeID: next.RuntimeID, DesiredSpec: &desired})
	if err != nil {
		t.Fatal(err)
	}
	if _, claimed, err := s.TryClaimPendingOperation(op.ID); err != nil || !claimed {
		t.Fatal("recovery operation not claimed", err)
	}
	if _, err := s.CommitColdPostgresTarget(op.ID, store.PostgresSpecFingerprint(current), next); err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	host := "original-db." + runtime.NamespaceForTenant(app.TenantID) + ".svc.cluster.local"
	server.openAppDatabase = func(driver, dsn string) (*sql.DB, error) {
		if driver != "pgx" || dsn != "postgresql://owner:fixture@"+host+":5432/demo?application_name=a" {
			t.Fatal("diagnostic connection did not follow the stable endpoint")
		}
		return db, nil
	}
	mock.ExpectBegin()
	mock.ExpectQuery("select 1").WillReturnRows(sqlmock.NewRows([]string{"one"}).AddRow(1))
	mock.ExpectCommit()
	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/database/query", apiKey, map[string]any{"sql": "select 1"})
	if recorder.Code != http.StatusOK {
		t.Fatalf("database query status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
