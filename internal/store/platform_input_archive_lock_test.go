package store

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestCompilerInputArchiveBarrierDoesNotRetryUnknownOrUnconfirmedRollback(t *testing.T) {
	for _, test := range []struct {
		name     string
		code     string
		rollback error
	}{
		{name: "unknown-database-error", code: "XX000"},
		{name: "rollback-not-confirmed", code: "55P03", rollback: errors.New("connection lost")},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectBegin()
			for _, q := range []string{`SET LOCAL lock_timeout='250ms'`, `SET LOCAL statement_timeout='5s'`, `SET LOCAL work_mem='1MB'`} {
				mock.ExpectExec(regexp.QuoteMeta(q)).WillReturnResult(sqlmock.NewResult(0, 0))
			}
			cause := &pgconn.PgError{Code: test.code}
			mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_advisory_xact_lock(315609238744290)`)).WillReturnError(cause)
			mock.ExpectRollback().WillReturnError(test.rollback)
			if tx, err := beginInputArchiveTx(context.Background(), db); tx != nil || !errors.Is(err, cause) {
				t.Fatal("unsafe barrier error was retried or hidden", tx, err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
