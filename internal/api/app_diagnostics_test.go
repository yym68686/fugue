package api

import (
	"database/sql"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

type diagnosticRoundTripFunc func(*http.Request) (*http.Response, error)

func (fn diagnosticRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func TestQueryAppDatabaseUsesEffectiveConnection(t *testing.T) {
	t.Parallel()

	_, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Env: map[string]string{
			"DATABASE_URL": "postgresql://demo:secret@db.internal:5432/demo",
		},
	})

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock new: %v", err)
	}
	defer db.Close()

	server.openAppDatabase = func(driverName, dsn string) (*sql.DB, error) {
		if driverName != "pgx" {
			t.Fatalf("expected pgx driver, got %q", driverName)
		}
		if dsn != "postgresql://demo:secret@db.internal:5432/demo" {
			t.Fatalf("unexpected dsn %q", dsn)
		}
		return db, nil
	}

	rows := sqlmock.NewRows([]string{"id", "status"}).
		AddRow(1, "ok").
		AddRow(2, "error")
	mock.ExpectBegin()
	mock.ExpectQuery("select id, status from gateway_request_logs order by id").
		WillReturnRows(rows)
	mock.ExpectCommit()

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/database/query", apiKey, map[string]any{
		"sql":      "select id, status from gateway_request_logs order by id",
		"max_rows": 10,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	var response struct {
		Database string                   `json:"database"`
		Host     string                   `json:"host"`
		User     string                   `json:"user"`
		Columns  []appDatabaseQueryColumn `json:"columns"`
		Rows     []map[string]any         `json:"rows"`
		ReadOnly bool                     `json:"read_only"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Database != "demo" || response.Host != "db.internal" || response.User != "demo" {
		t.Fatalf("unexpected query target %+v", response)
	}
	if !response.ReadOnly {
		t.Fatalf("expected read_only=true, got %+v", response)
	}
	if len(response.Columns) != 2 || response.Columns[0].Name != "id" || response.Columns[1].Name != "status" {
		t.Fatalf("unexpected columns %+v", response.Columns)
	}
	if len(response.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %+v", response.Rows)
	}
	if got := response.Rows[0]["status"]; got != "ok" {
		t.Fatalf("expected first row status ok, got %#v", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestQueryAppDatabaseQualifiesManagedPostgresHost(t *testing.T) {
	t.Parallel()

	stateStore, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Postgres: &model.AppPostgresSpec{
			Database: "demo",
			User:     "demo",
			Password: "secret",
		},
	})

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock new: %v", err)
	}
	defer db.Close()

	storedApp, err := stateStore.GetApp(app.ID)
	if err != nil {
		t.Fatalf("get app: %v", err)
	}
	bound, ok := firstManagedPostgresBinding(storedApp)
	if !ok || bound.Service.Spec.Postgres == nil {
		t.Fatalf("expected managed postgres binding, got app=%+v", storedApp)
	}
	expectedHost := model.PostgresRWServiceName(bound.Service.Spec.Postgres.ServiceName) + "." + runtime.NamespaceForTenant(app.TenantID) + ".svc.cluster.local"
	expectedDSN := "postgresql://demo:secret@" + expectedHost + ":5432/demo?sslmode=disable"
	server.openAppDatabase = func(driverName, dsn string) (*sql.DB, error) {
		if driverName != "pgx" {
			t.Fatalf("expected pgx driver, got %q", driverName)
		}
		if dsn != expectedDSN {
			t.Fatalf("expected dsn %q, got %q", expectedDSN, dsn)
		}
		return db, nil
	}

	mock.ExpectBegin()
	mock.ExpectQuery("select 1").WillReturnRows(sqlmock.NewRows([]string{"one"}).AddRow(1))
	mock.ExpectCommit()

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/database/query", apiKey, map[string]any{
		"sql": "select 1",
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	var response struct {
		Host string `json:"host"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Host != expectedHost {
		t.Fatalf("expected host %q, got %q", expectedHost, response.Host)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestRequestAppInternalHTTPUsesEnvBackedHeader(t *testing.T) {
	t.Parallel()

	_, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
		Env: map[string]string{
			"SERVICE_KEY": "svc-secret-123",
		},
	})

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("unexpected method %s", r.Method)
		}
		if r.URL.Path != "/admin/requests" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("page"); got != "2" {
			t.Fatalf("expected page=2, got %q", got)
		}
		if got := r.URL.Query().Get("status"); got != "500" {
			t.Fatalf("expected status=500, got %q", got)
		}
		if got := r.Header.Get("X-Service-Key"); got != "svc-secret-123" {
			t.Fatalf("expected X-Service-Key from env, got %q", got)
		}
		w.Header().Set("Server-Timing", "admin;dur=7")
		_, _ = io.WriteString(w, `{"items":[{"id":"req_123"}]}`)
	}))
	defer backend.Close()

	backendURL, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatalf("parse backend url: %v", err)
	}
	backendTransport := backend.Client().Transport
	server.appRequestHTTPClient = &http.Client{
		Transport: diagnosticRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			cloned := req.Clone(req.Context())
			cloned.URL.Scheme = backendURL.Scheme
			cloned.URL.Host = backendURL.Host
			cloned.Host = backendURL.Host
			return backendTransport.RoundTrip(cloned)
		}),
	}

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/request", apiKey, map[string]any{
		"method":           "GET",
		"path":             "/admin/requests",
		"query":            map[string][]string{"page": []string{"2"}, "status": []string{"500"}},
		"headers_from_env": map[string]string{"X-Service-Key": "SERVICE_KEY"},
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d body=%s", recorder.Code, recorder.Body.String())
	}

	var response struct {
		StatusCode   int    `json:"status_code"`
		ServerTiming string `json:"server_timing"`
		Body         string `json:"body"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("expected upstream status 200, got %+v", response)
	}
	if response.ServerTiming != "admin;dur=7" {
		t.Fatalf("expected server timing, got %+v", response)
	}
	if !strings.Contains(response.Body, `"req_123"`) {
		t.Fatalf("expected response body to include req_123, got %q", response.Body)
	}
}
