package store

import (
	"context"
	"database/sql"
	"errors"
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
