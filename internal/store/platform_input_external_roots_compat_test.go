package store

import (
	"database/sql"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"testing"
)

func TestRetiredExternalRegistryReaderDoesNotIgnoreUnknownOrPartialState(t *testing.T) {
	for _, scenario := range []string{"absent", "lookup-failed", "partial"} {
		t.Run(scenario, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectBegin()
			tx, err := db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			query := mock.ExpectQuery("SELECT to_regclass")
			if scenario == "lookup-failed" {
				query.WillReturnError(errors.New("catalog unavailable"))
			} else {
				query.WillReturnRows(sqlmock.NewRows([]string{"installed"}).AddRow(scenario == "partial"))
			}
			if scenario == "partial" {
				mock.ExpectQuery("SELECT").WillReturnError(sql.ErrNoRows)
			}
			roots, revision, err := compilerInputExternalRoots(t.Context(), tx, 32, nil)
			if (err == nil) != (scenario == "absent") || len(roots) != 0 || revision != 0 {
				t.Fatal(scenario, roots, revision, err)
			}
			mock.ExpectRollback()
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
