package store

import (
	"regexp"
	"testing"
	"time"

	"fugue/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPGUpsertAppImageTrackingCreatesMissingRow(t *testing.T) {
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
		appID     = "app_123"
		imageRef  = "ghcr.io/acme/web:latest"
		projectID = "project_123"
		tenantID  = "tenant_123"
	)

	now := time.Date(2026, time.May, 17, 10, 30, 0, 0, time.UTC)
	appSpecJSON := `{"image":"registry.example/acme/web@sha256:old","ports":[80],"replicas":1,"runtime_id":"runtime_managed_shared"}`
	appStatusJSON := `{"phase":"deployed","current_runtime_id":"runtime_managed_shared","current_replicas":1,"updated_at":"2026-05-17T10:30:00Z"}`

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`
	SELECT id, tenant_id, project_id, name, description, source_json, route_json, spec_json, status_json, created_at, updated_at
	FROM fugue_apps
WHERE id = $1
`)).
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
			"web",
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
		WillReturnRows(sqlmock.NewRows([]string{"binding_id"}))
	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT id, tenant_id, app_id, image_ref, enabled, last_seen_digest, last_queued_digest, last_deployed_digest, last_operation_id, last_delivery_id, last_event, last_error, last_checked_at, last_triggered_at, created_at, updated_at
FROM fugue_app_image_trackings
WHERE app_id = $1
FOR UPDATE
`)).
		WithArgs(appID).
		WillReturnRows(sqlmock.NewRows(appImageTrackingTestColumns()))
	mock.ExpectQuery(regexp.QuoteMeta(`
INSERT INTO fugue_app_image_trackings (
	id, tenant_id, app_id, image_ref, enabled, last_seen_digest, last_queued_digest,
	last_deployed_digest, last_operation_id, last_delivery_id, last_event, last_error,
	last_checked_at, last_triggered_at, created_at, updated_at
) VALUES (
	$1, $2, $3, $4, $5, $6, $7,
	$8, $9, $10, $11, $12,
	$13, $14, $15, $16
)
RETURNING id, tenant_id, app_id, image_ref, enabled, last_seen_digest, last_queued_digest, last_deployed_digest, last_operation_id, last_delivery_id, last_event, last_error, last_checked_at, last_triggered_at, created_at, updated_at
`)).
		WithArgs(
			sqlmock.AnyArg(),
			tenantID,
			appID,
			imageRef,
			true,
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			nil,
			nil,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnRows(sqlmock.NewRows(appImageTrackingTestColumns()).AddRow(
			"imgtrack_123",
			tenantID,
			appID,
			imageRef,
			true,
			"",
			"",
			"",
			"",
			"",
			"",
			"",
			nil,
			nil,
			now,
			now,
		))
	mock.ExpectCommit()

	tracking, err := s.pgUpsertAppImageTracking(model.AppImageTracking{
		TenantID: tenantID,
		AppID:    appID,
		ImageRef: imageRef,
		Enabled:  true,
	})
	if err != nil {
		t.Fatalf("pgUpsertAppImageTracking: %v", err)
	}
	if tracking.ID != "imgtrack_123" || tracking.ImageRef != imageRef {
		t.Fatalf("unexpected tracking row: %+v", tracking)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func appImageTrackingTestColumns() []string {
	return []string{
		"id",
		"tenant_id",
		"app_id",
		"image_ref",
		"enabled",
		"last_seen_digest",
		"last_queued_digest",
		"last_deployed_digest",
		"last_operation_id",
		"last_delivery_id",
		"last_event",
		"last_error",
		"last_checked_at",
		"last_triggered_at",
		"created_at",
		"updated_at",
	}
}
