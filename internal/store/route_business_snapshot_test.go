package store

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestRouteBusinessSnapshotDatabaseErrorRollsBackWithoutResult(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT transaction_timestamp").WillReturnError(errors.New("snapshot unavailable"))
	mock.ExpectRollback()
	s := &Store{db: db, databaseURL: "postgres://test"}
	snapshot, err := s.CaptureRouteBusinessSnapshot(context.Background())
	if err == nil || snapshot.Revision != "" || len(snapshot.Apps) != 0 {
		t.Fatal("partial snapshot returned", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
