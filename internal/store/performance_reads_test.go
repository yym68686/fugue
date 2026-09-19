package store

import (
	"database/sql/driver"
	"encoding/json"
	"fugue/internal/model"
	"github.com/DATA-DOG/go-sqlmock"
	"path/filepath"
	"testing"
	"time"
)

func TestReleaseMetricsSnapshotUsesTwoQueriesAndNarrowProjection(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.ValueConverterOption(performanceArrayConverter{}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &Store{db: db, databaseURL: "postgres://example", dbReady: true}
	now := time.Now()
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT id,trigger_type,status,started_at,finished_at FROM fugue_release_attempts`).WithArgs(500).WillReturnRows(sqlmock.NewRows([]string{"id", "trigger_type", "status", "started_at", "finished_at"}).AddRow("attempt", "manual", "succeeded", now, nil))
	mock.ExpectQuery(`(?s)SELECT release_attempt_id.*FROM fugue_release_steps WHERE release_attempt_id = ANY`).WithArgs(sqlmock.AnyArg()).WillReturnRows(sqlmock.NewRows([]string{"release_attempt_id", "id", "step_type", "status", "started_at", "finished_at", "phase"}).AddRow("attempt", "step", "deploy_apply", "succeeded", now, now, "candidate"))
	mock.ExpectCommit()
	got, err := s.LoadReleaseMetricsSnapshot(t.Context(), 500)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Attempts) != 1 || len(got.Steps["attempt"]) != 1 || got.Steps["attempt"][0].Payload["phase"] != "candidate" {
		t.Fatalf("bad projection %+v", got)
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestImageLocationObservationScopeKeepsNegativeEvidence(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "state.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	err := s.withLockedState(true, func(state *model.State) error {
		for i, status := range []string{"present", "missing", "pulling", "failed"} {
			state.ImageLocations = append(state.ImageLocations, model.ImageLocation{ID: string(rune('a' + i)), TenantID: "tenant", AppID: "app", RuntimeID: "target", ImageRef: "image:v1", Status: status, UpdatedAt: now})
		}
		state.ImageLocations = append(state.ImageLocations, model.ImageLocation{TenantID: "other", AppID: "app", RuntimeID: "target", ImageRef: "image:v1", Status: "present"}, model.ImageLocation{TenantID: "tenant", AppID: "app", RuntimeID: "source", ImageRef: "image:v1", Status: "present"}, model.ImageLocation{TenantID: "tenant", AppID: "app", RuntimeID: "target", ImageRef: "image:old", Status: "present"})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	scope := ImageLocationScope{TenantID: "tenant", AppID: "app", RuntimeID: "target", ImageRef: "image:v1"}
	rows, err := s.ListImageLocationObservations(t.Context(), []ImageLocationScope{scope, scope})
	if err != nil || len(rows) != 4 {
		t.Fatalf("scope or duplicates: %d %v", len(rows), err)
	}
	for _, row := range rows {
		if !row.UpdatedAt.Equal(now) {
			t.Fatal("observation timestamp changed")
		}
	}
	rows, err = s.ListImageLocationObservations(t.Context(), nil)
	if err != nil || len(rows) != 0 {
		t.Fatal("empty scope became full inventory")
	}
}

type performanceArrayConverter struct{}

func (performanceArrayConverter) ConvertValue(v any) (driver.Value, error) {
	if a, ok := v.([]string); ok {
		b, e := json.Marshal(a)
		return string(b), e
	}
	return driver.DefaultParameterConverter.ConvertValue(v)
}
