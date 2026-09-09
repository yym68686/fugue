package store

import (
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"github.com/DATA-DOG/go-sqlmock"
)

func TestImageOperationProjectionPreservesCandidateInputs(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	op := model.Operation{ID: "operation", TenantID: "tenant", AppID: "app", CreatedAt: now, UpdatedAt: now, StartedAt: &now, CompletedAt: &now,
		DesiredSpec:    &model.AppSpec{Image: "registry.example/demo:v1", Env: map[string]string{"SECRET": "hidden"}},
		ConfigBaseSpec: &model.AppSpec{Env: map[string]string{"SECRET": "baseline"}},
		DesiredSource:  &model.AppSource{Type: model.AppSourceTypeGitHubPublic, RepoURL: "https://github.com/example/demo", CommitSHA: "abc", ResolvedImageRef: "registry.example/demo:v1"}}
	if err := s.withLockedState(true, func(state *model.State) error {
		state.Operations = []model.Operation{op}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	got, err := s.ListImageOperationsByApps("tenant", false, []string{" app ", "app"})
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := s.ListOperationsWithDesiredSourceByApps("tenant", false, []string{"app"})
	if err != nil {
		t.Fatal(err)
	}
	want := baseline[op.AppID][0]
	want.DesiredSpec = &model.AppSpec{Image: op.DesiredSpec.Image}
	want.ConfigBaseSpec = nil
	if !reflect.DeepEqual(got[op.AppID], []model.Operation{want}) {
		t.Fatalf("projection changed inputs: %+v", got)
	}
	foreign, err := s.ListImageOperationsByApps("foreign", false, []string{"app"})
	if err != nil || len(foreign) != 0 {
		t.Fatalf("tenant filter lost: %v %v", foreign, err)
	}
	full, err := s.ListOperationsWithDesiredSourceByApps("tenant", false, []string{"app"})
	if err != nil || full[op.AppID][0].DesiredSpec.Env["SECRET"] != "hidden" {
		t.Fatal("projection mutated stored configuration")
	}
}

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
	mock.ExpectQuery(`SELECT id, tenant_id, type, status, app_id, desired_spec_json->>'image', desired_source_json - 'config_base_spec', created_at, updated_at, started_at, completed_at FROM fugue_operations WHERE app_id IN \(\$1, \$2\) AND desired_source_json IS NOT NULL AND tenant_id = \$3 ORDER BY app_id ASC, created_at ASC`).
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
