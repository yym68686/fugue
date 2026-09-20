package store

import (
	"errors"
	"reflect"
	"regexp"
	"testing"
	"time"

	"fugue/internal/imageretention"
	"fugue/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestOperationLifecyclesPreserveRecoveryAndRetentionInputs(t *testing.T) {
	s := New(t.TempDir() + "/state.json")
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	old := now.Add(-365 * 24 * time.Hour)
	recent := now.Add(-time.Hour)
	ops := []model.Operation{
		{ID: "old-active", AppID: "app", Type: "import", Status: model.OperationStatusWaitingAgent, CreatedAt: old, StartedAt: &old},
		{ID: "completed", AppID: "app", Type: "deploy", Status: model.OperationStatusCompleted, CreatedAt: old, StartedAt: &old, CompletedAt: &recent},
		{ID: "pending", AppID: "app", Type: "deploy", Status: model.OperationStatusPending, CreatedAt: recent},
		{ID: "running", AppID: "app", Type: "scale", Status: model.OperationStatusRunning, CreatedAt: recent, StartedAt: &recent},
		{ID: "failed", AppID: "app", Type: "import", Status: model.OperationStatusFailed, CreatedAt: recent, CompletedAt: &now},
		{ID: "canceled", AppID: "other", Type: "deploy", Status: model.OperationStatusCanceled, CreatedAt: recent},
	}
	for i := range ops {
		ops[i].DesiredSpec = &model.AppSpec{Image: "registry.example/app:v1", Env: map[string]string{"SECRET": "private"}}
		ops[i].DesiredSource = &model.AppSource{Type: model.AppSourceTypeDockerImage, ImageRef: "registry.example/app:v1"}
		ops[i].ResultMessage = "large diagnostic payload"
	}
	if err := s.withLockedState(true, func(state *model.State) error { state.Operations = ops; return nil }); err != nil {
		t.Fatal(err)
	}
	all, err := s.ListOperationLifecycles()
	if err != nil || len(all) != len(ops) {
		t.Fatalf("history was lost: %d %v", len(all), err)
	}
	active, err := s.ListActiveOperationLifecycles()
	if err != nil || len(active) != 3 {
		t.Fatalf("active inventory changed: %+v %v", active, err)
	}
	if active[0].ID != "old-active" || active[0].StartedAt == nil || !active[0].StartedAt.Equal(old) {
		t.Fatal("an old stuck operation must remain visible")
	}
	for _, op := range all {
		if op.DesiredSpec != nil || op.DesiredSource != nil || op.ConfigBaseSpec != nil || op.ResultMessage != "" {
			t.Fatal("lifecycle inventory hydrated execution payloads")
		}
	}
	app := model.App{ID: "app", Spec: model.AppSpec{ImageMirrorLimit: 1}}
	images := []model.Image{
		{ID: "active-image", AppID: "app", SourceOperationID: "old-active", UpdatedAt: old},
		{ID: "recent-image", AppID: "app", SourceOperationID: "completed", UpdatedAt: old},
		{ID: "failed-image", AppID: "app", SourceOperationID: "failed", UpdatedAt: old},
		{ID: "orphan-image", AppID: "app", SourceOperationID: "missing", UpdatedAt: old},
	}
	want := imageretention.Plan(app, images, ops, nil, nil, now)
	got := imageretention.Plan(app, images, all, nil, nil, now)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("retention protection or ordering changed: got=%+v want=%+v", got, want)
	}
	full, err := s.ListOperations("", true)
	if err != nil || full[0].DesiredSpec == nil {
		t.Fatal("lifecycle projection changed stored execution state")
	}
}

func TestPostgresOperationLifecyclesFailClosedOnIncompleteRows(t *testing.T) {
	for _, activeOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "retention", true: "active"}[activeOnly], func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			s := &Store{db: db, databaseURL: "postgres://example", dbReady: true}
			query := "SELECT id, app_id, type, status, created_at, started_at, completed_at\nFROM fugue_operations"
			if activeOnly {
				query += "\nWHERE status IN ($1, $2, $3)"
			}
			query += "\nORDER BY created_at ASC, id ASC"
			expect := mock.ExpectQuery(regexp.QuoteMeta(query))
			if activeOnly {
				expect.WithArgs(model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent)
			}
			streamErr := errors.New("interrupted result stream")
			expect.WillReturnRows(sqlmock.NewRows([]string{"id", "app_id", "type", "status", "created_at", "started_at", "completed_at"}).
				AddRow("first", "app", "deploy", "running", time.Now(), nil, nil).
				AddRow("second", "app", "deploy", "running", time.Now(), nil, nil).RowError(1, streamErr)).RowsWillBeClosed()
			ops, err := s.listOperationLifecycles(activeOnly)
			if !errors.Is(err, streamErr) || ops != nil {
				t.Fatalf("partial inventory escaped on failure: %v %v", ops, err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
