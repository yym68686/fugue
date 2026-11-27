package store

import (
	"errors"
	"regexp"
	"testing"
	"time"

	"fugue/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

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
