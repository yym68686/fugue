package store

import (
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPostgresImageOperationProjectionUsesOneScopedQuery(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now().UTC()
	source := &model.AppSource{Type: model.AppSourceTypeDockerImage, ImageRef: "registry.example/demo:v1"}
	encoded, err := marshalOperationSourceState(model.Operation{DesiredSource: source})
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery(`SELECT id, tenant_id, type, status, app_id, desired_spec_json->>'image', CASE WHEN jsonb_typeof\(desired_source_json\) = 'object' THEN desired_source_json - 'config_base_spec' ELSE desired_source_json END, created_at, updated_at, started_at, completed_at FROM fugue_operations WHERE app_id IN \(\$1, \$2\) AND desired_source_json IS NOT NULL AND tenant_id = \$3 ORDER BY app_id ASC, created_at ASC`).
		WithArgs("app-a", "app-b", "tenant").
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "type", "status", "app_id", "image", "source", "created", "updated", "started", "completed"}).
			AddRow("op", "tenant", "import", "running", "app-a", "registry.example/demo:v0", encoded, now, now, now, nil))
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	got, err := s.ListImageOperationsByApps("tenant", false, []string{" app-b ", "app-a", "app-b"})
	if err != nil {
		t.Fatal(err)
	}
	op := got["app-a"][0]
	if op.DesiredSpec.Image != "registry.example/demo:v0" || !reflect.DeepEqual(op.DesiredSource, source) || op.StartedAt == nil || op.CompletedAt != nil {
		t.Fatalf("projection lost runtime alias, source or timestamp: %+v", op)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresImageCandidateOperationProjectionUsesNarrowTable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now().UTC()
	source := &model.AppSource{Type: model.AppSourceTypeDockerImage, ImageRef: "registry.example/demo:v1"}
	encoded, err := marshalOperationSourceState(model.Operation{DesiredSource: source})
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery(`FROM fugue_image_candidate_operations`).
		WithArgs("app-a", "tenant").
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "type", "status", "app_id", "image", "source", "created", "updated", "started", "completed"}).
			AddRow("op", "tenant", "import", "completed", "app-a", "registry.example/demo:v1", encoded, now, now, now, now))
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	got, err := s.ListImageCandidateOperationsByApps("tenant", false, []string{"app-a"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got["app-a"]) != 1 || got["app-a"][0].DesiredSpec == nil || got["app-a"][0].DesiredSpec.Image != "registry.example/demo:v1" || !reflect.DeepEqual(got["app-a"][0].DesiredSource, source) {
		t.Fatalf("candidate projection lost image input: %+v", got["app-a"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
