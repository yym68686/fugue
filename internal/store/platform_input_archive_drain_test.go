package store

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestCompilerInputArchiveTransactionDrainRefusesPartialVisibility(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery(`SELECT current_setting('is_superuser')::boolean OR pg_has_role(current_user,'pg_read_all_stats','USAGE')`).WillReturnRows(sqlmock.NewRows([]string{"can_inspect"}).AddRow(false))
	if _, err = compilerInputArchiveTransactionDrain(t.Context(), tx); !errors.Is(err, ErrConflict) {
		t.Fatal("hidden transaction metadata was accepted as drained", err)
	}
	mock.ExpectRollback()
	_ = tx.Rollback()
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCompilerInputArchiveTransactionDrainRequiresEffectiveStatsPrivilege(t *testing.T) {
	s := boundedContentTestStore(t)
	if _, err := s.db.Exec(`CREATE ROLE fugue_test_archive_stats_noinherit NOLOGIN NOINHERIT`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := s.db.Exec(`DROP ROLE fugue_test_archive_stats_noinherit`); err != nil {
			t.Error("clean disposable stats role", err)
		}
	}()
	if _, err := s.db.Exec(`GRANT pg_read_all_stats TO fugue_test_archive_stats_noinherit`); err != nil {
		t.Fatal(err)
	}
	check, err := s.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer check.Rollback()
	if _, err = check.Exec(`SET LOCAL ROLE fugue_test_archive_stats_noinherit`); err != nil {
		t.Fatal(err)
	}
	var member, effective bool
	if err = check.QueryRow(`SELECT pg_has_role(current_user,'pg_read_all_stats','MEMBER'),pg_has_role(current_user,'pg_read_all_stats','USAGE')`).Scan(&member, &effective); err != nil || !member || effective {
		t.Fatal("NOINHERIT fixture does not distinguish membership from effective visibility", member, effective, err)
	}
	if _, err = compilerInputArchiveTransactionDrain(t.Context(), check); !errors.Is(err, ErrConflict) {
		t.Fatal("a role without effective stats visibility was accepted", err)
	}
}

func TestCompilerInputArchiveTransactionDrainRechecksCachedStatistics(t *testing.T) {
	s := boundedContentTestStore(t)
	check, err := s.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer check.Rollback()
	if observed, err := compilerInputArchiveTransactionDrain(t.Context(), check); err != nil || observed.OldTransactions != 0 || observed.PreparedTransactions != 0 {
		t.Fatal("empty database was not drained", observed, err)
	}
	// Start a new RR snapshot after the first check on the same maintenance
	// transaction. backend_xmin protects it even though xact_start is newer.
	reader, err := s.db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Rollback()
	var snapshot string
	if err = reader.QueryRow(`SELECT pg_current_snapshot()::text`).Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	observed, err := compilerInputArchiveTransactionDrain(t.Context(), check)
	if !errors.Is(err, ErrConflict) || observed.OldTransactions != 1 || observed.OldestTransaction == nil || observed.OldestTransaction.After(time.Now()) {
		t.Fatal("second drain check reused stale statistics or ignored a newer snapshot", observed, err)
	}
	if err = reader.Rollback(); err != nil {
		t.Fatal(err)
	}
	if observed, err = compilerInputArchiveTransactionDrain(t.Context(), check); err != nil || observed.OldTransactions != 0 {
		t.Fatal("finished reader remained in the drain observation", observed, err)
	}
}

func TestCompilerInputArchiveTransactionDrainRefusesPreparedTransaction(t *testing.T) {
	s := boundedContentTestStore(t)
	var preparedLimit int
	if err := s.db.QueryRow(`SHOW max_prepared_transactions`).Scan(&preparedLimit); err != nil {
		t.Fatal(err)
	}
	if preparedLimit == 0 {
		t.Skip("disposable PostgreSQL needs max_prepared_transactions>0 for the prepared-transaction scenario")
	}
	conn, err := s.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, statement := range []string{`BEGIN`, `SELECT 1`, `PREPARE TRANSACTION 'fugue_test_archive_drain'`} {
		if _, err = conn.ExecContext(t.Context(), statement); err != nil {
			_, _ = conn.ExecContext(t.Context(), `ROLLBACK`)
			t.Fatal(err)
		}
	}
	defer func() {
		if _, err := conn.ExecContext(t.Context(), `ROLLBACK PREPARED 'fugue_test_archive_drain'`); err != nil {
			t.Error("clean disposable prepared transaction", err)
		}
	}()
	check, err := s.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer check.Rollback()
	observed, err := compilerInputArchiveTransactionDrain(t.Context(), check)
	if !errors.Is(err, ErrConflict) || observed.PreparedTransactions != 1 {
		t.Fatal("prepared transaction was not held as a retirement blocker", observed, err)
	}
}
