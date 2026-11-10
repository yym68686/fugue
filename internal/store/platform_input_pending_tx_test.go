package store

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"testing"
	"time"
)

func TestPendingRootNativePostgresPoolAcquireBound(t *testing.T) {
	s := pendingPolicyFixture(t)
	s.db.SetMaxOpenConns(1)
	held, err := s.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
	defer cancel()
	type result struct {
		tx  *pendingRootNativeTx
		err error
	}
	done := make(chan result, 1)
	go func() {
		tx, err := beginPendingRootPolicyTx(ctx, s.db, true)
		done <- result{tx, err}
	}()
	select {
	case got := <-done:
		if got.tx != nil {
			_ = got.tx.Rollback()
			t.Fatal("transaction acquired an unavailable pool connection")
		}
		if !errors.Is(got.err, context.DeadlineExceeded) {
			t.Fatal("pool acquisition did not preserve its deadline error", got.err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("connection acquisition waited for the outer operation deadline")
	}
	if s.db.Stats().InUse != 1 {
		t.Fatal("failed acquisition leaked or stole a connection", s.db.Stats())
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	// The shorter caller deadline remains authoritative as well.
	short, stop := context.WithCancel(t.Context())
	stop()
	if tx, err := beginPendingRootPolicyTx(short, s.db, true); tx != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("canceled acquisition started a transaction", err)
	}
	if s.db.Stats().InUse != 0 {
		t.Fatal("canceled acquisition leaked its connection", s.db.Stats())
	}
}

func TestPendingRootNativePostgresDurableSettingsAreLocal(t *testing.T) {
	s := pendingPolicyFixture(t)
	s.db.SetMaxOpenConns(1)
	if _, err := s.db.ExecContext(t.Context(), `SET statement_timeout='2s'; SET lock_timeout='100ms'`); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"off", "local", "remote_write", "on", "remote_apply"} {
		t.Run(mode, func(t *testing.T) {
			if _, err := s.db.ExecContext(t.Context(), `SELECT set_config('synchronous_commit',$1,false)`, mode); err != nil {
				t.Fatal(err)
			}
			tx, err := beginPendingRootPolicyTx(t.Context(), s.db, false)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			var actual, timer string
			if err := tx.QueryRowContext(t.Context(), `SELECT current_setting('synchronous_commit'),setting FROM pg_settings WHERE name='transaction_timeout'`).Scan(&actual, &timer); err != nil {
				t.Fatal(err)
			}
			want := "on"
			if mode == "remote_apply" {
				want = mode
			}
			ms, err := strconv.Atoi(timer)
			if err != nil || ms < 1 || ms > 60000 || actual != want {
				t.Fatal("durability weakened or transaction unbounded", actual, timer, err)
			}
			var stricter bool
			if err := tx.QueryRowContext(t.Context(), `SELECT current_setting('statement_timeout')='2s' AND current_setting('lock_timeout')='100ms'`).Scan(&stricter); err != nil || !stricter {
				t.Fatal("native guard relaxed stricter statement/lock policy", stricter, err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			if err := s.db.QueryRowContext(t.Context(), `SELECT current_setting('synchronous_commit'),current_setting('transaction_timeout')`).Scan(&actual, &timer); err != nil || actual != mode || timer != "0" {
				t.Fatal("transaction-local settings leaked into shared pool", actual, timer, err)
			}
		})
	}
}

func TestPendingRootNativePostgresServerStopsIdleTransaction(t *testing.T) {
	for _, readOnly := range []bool{true, false} {
		t.Run(strconv.FormatBool(readOnly), func(t *testing.T) {
			s := pendingPolicyFixture(t)
			s.db.SetMaxOpenConns(1)
			if _, err := s.db.ExecContext(t.Context(), `CREATE TABLE pending_native_probe(id integer PRIMARY KEY); SET transaction_timeout='300ms'`); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			tx, err := beginPendingRootPolicyTx(ctx, s.db, readOnly)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			var pid int
			var timer string
			if err := tx.QueryRowContext(ctx, `SELECT pg_backend_pid(),current_setting('transaction_timeout')`).Scan(&pid, &timer); err != nil || timer != "300ms" {
				t.Fatal("stricter session timer was relaxed", timer, err)
			}
			if !readOnly {
				if _, err := tx.ExecContext(ctx, `INSERT INTO pending_native_probe VALUES(1)`); err != nil {
					t.Fatal(err)
				}
			}
			// Leave the transaction idle without canceling its client context. An
			// independent connection observes backend exit and rollback, rather
			// than mistaking a client-side error for server-side cleanup.
			observer, err := sql.Open("pgx", s.databaseURL)
			if err != nil {
				t.Fatal(err)
			}
			defer observer.Close()
			deadline := time.Now().Add(3 * time.Second)
			for {
				var alive bool
				if err := observer.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1)`, pid).Scan(&alive); err != nil {
					t.Fatal(err)
				}
				if !alive {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("idle backend outlived server timer with client still connected")
				}
				time.Sleep(20 * time.Millisecond)
			}
			if ctx.Err() != nil {
				t.Fatal("client cancellation, rather than server timer, ended the backend", ctx.Err())
			}
			var count int
			if err := observer.QueryRowContext(ctx, `SELECT count(*) FROM pending_native_probe`).Scan(&count); err != nil || count != 0 {
				t.Fatal("server expiry preserved an uncommitted write", count, err)
			}
			if err := tx.Commit(); err == nil {
				t.Fatal("expired transaction claimed commit acknowledgement")
			}
			if s.db.Stats().InUse != 0 {
				t.Fatal("server-terminated transaction retained owned connection", s.db.Stats())
			}
			if err := s.db.PingContext(ctx); err != nil {
				t.Fatal("pool failed to replace server-terminated connection", err)
			}
		})
	}
}

func TestPendingRootNativePostgresAcquireDeadlineDoesNotEndTransaction(t *testing.T) {
	s := pendingPolicyFixture(t)
	s.db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
	defer cancel()
	tx, err := beginPendingRootPolicyTx(ctx, s.db, true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	// Acquisition's five-second context must neither cancel at return nor
	// replace the independent outer transaction context five seconds later.
	select {
	case <-time.After(5200 * time.Millisecond):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var guarded bool
	if err := tx.QueryRowContext(ctx, `SELECT current_setting('transaction_read_only')='on'
 AND current_setting('transaction_isolation')='repeatable read'
 AND current_setting('temp_file_limit')='0'
 AND current_setting('statement_timeout')='5s'
 AND current_setting('lock_timeout')='250ms'
 AND current_setting('work_mem')='1MB'`).Scan(&guarded); err != nil || !guarded {
		t.Fatal("acquisition deadline shortened transaction or lost safety settings", guarded, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); !errors.Is(err, sql.ErrTxDone) {
		t.Fatal("deferred rollback changed terminal semantics", err)
	}
	if s.db.Stats().InUse != 0 {
		t.Fatal("commit failed to return owned connection", s.db.Stats())
	}
	// A canceled transaction may already have been rolled back by database/sql;
	// its owning sql.Conn still has to be returned explicitly in either case.
	child, stop := context.WithCancel(t.Context())
	other, err := beginPendingRootPolicyTx(child, s.db, false)
	if err != nil {
		stop()
		t.Fatal(err)
	}
	stop()
	if err := other.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if s.db.Stats().InUse != 0 {
		t.Fatal("canceled transaction leaked owned connection", s.db.Stats())
	}
	if err := s.db.PingContext(t.Context()); err != nil {
		t.Fatal("pool could not recover after canceled transaction", err)
	}
}
