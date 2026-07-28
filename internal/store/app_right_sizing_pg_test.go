package store

import (
	"regexp"
	"testing"
	"time"

	"fugue/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPGUpdateAppRightSizingPersistsControlPlanePolicyWithoutOperation(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	defer db.Close()
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}

	const (
		appID     = "app_right_sizing"
		tenantID  = "tenant_right_sizing"
		projectID = "project_right_sizing"
	)
	now := time.Date(2026, time.July, 28, 1, 0, 0, 0, time.UTC)
	appRows := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{
			"id", "tenant_id", "project_id", "name", "description",
			"source_json", "route_json", "spec_json", "status_json", "created_at", "updated_at",
		}).AddRow(
			appID,
			tenantID,
			projectID,
			"demo",
			"",
			[]byte("null"),
			[]byte("null"),
			[]byte(`{"image":"ghcr.io/example/demo:latest","ports":[8080],"replicas":1,"runtime_id":"runtime_managed_shared","right_sizing":{"mode":"auto","window_hours":168,"min_samples":12}}`),
			[]byte(`{"phase":"deployed","current_runtime_id":"runtime_managed_shared","current_replicas":1,"updated_at":"2026-07-28T01:00:00Z"}`),
			now,
			now,
		)
	}
	bindingRows := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{"binding_id"})
	}
	bindingQuery := regexp.QuoteMeta(`
	SELECT b.id, b.tenant_id, b.app_id, b.service_id, b.alias, b.env_json, b.created_at, b.updated_at,
	       s.id, s.tenant_id, s.project_id, s.owner_app_id, s.name, s.description, s.type, s.provisioner, s.status, s.spec_json, s.current_runtime_started_at, s.current_runtime_ready_at, s.created_at, s.updated_at
	FROM fugue_service_bindings AS b
	JOIN fugue_backing_services AS s ON s.id = b.service_id
WHERE b.app_id = $1
ORDER BY b.created_at ASC, s.created_at ASC
`)

	mock.ExpectBegin()
	mock.ExpectQuery(`(?s)SELECT id, tenant_id, project_id, name, description, source_json, route_json, spec_json, status_json, created_at, updated_at.*FROM fugue_apps.*WHERE id = \$1.*FOR UPDATE`).
		WithArgs(appID).
		WillReturnRows(appRows())
	mock.ExpectQuery(bindingQuery).WithArgs(appID).WillReturnRows(bindingRows())
	mock.ExpectExec(`(?s)UPDATE fugue_apps.*SET tenant_id = \$2`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectQuery(bindingQuery).WithArgs(appID).WillReturnRows(bindingRows())

	updated, err := s.UpdateAppRightSizing(appID, model.AppRightSizingSpec{
		Mode:        model.AppRightSizingModeRecommend,
		WindowHours: 24,
		MinSamples:  6,
	})
	if err != nil {
		t.Fatalf("update app right-sizing policy: %v", err)
	}
	if got := updated.Spec.RightSizing; got == nil || got.Mode != model.AppRightSizingModeRecommend || got.WindowHours != 24 || got.MinSamples != 6 {
		t.Fatalf("unexpected persisted right-sizing policy: %+v", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}
