package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestAdmissionWriterContentionPreservesBudgetAndDoesNotAbortTransaction(t *testing.T) {
	db, _, _ := hotInventoryTestDatabase(t)
	holder, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback()
	if _, err := holder.ExecContext(t.Context(), `SELECT pg_advisory_xact_lock(315609238744290)`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(t.Context(), `SET LOCAL lock_timeout='50ms'`); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := waitCompilerInputAdmissionWriter(t.Context(), tx); stringSQLState(err) != "55P03" {
		t.Fatal("contention did not retain the existing retry classification", err)
	}
	if elapsed := time.Since(started); elapsed < 40*time.Millisecond || elapsed > time.Second {
		t.Fatal("stricter caller lock budget was ignored", elapsed)
	}
	if _, err := tx.ExecContext(t.Context(), `SELECT 1`); err != nil {
		t.Fatal("ordinary contention aborted the database transaction", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := waitCompilerInputAdmissionWriter(ctx, tx); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled acquisition continued waiting", err)
	}
}

func TestAdmissionWriterAcquiresAfterReleaseAndHoldsUntilRollback(t *testing.T) {
	db, _, _ := hotInventoryTestDatabase(t)
	holder, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback()
	if _, err := holder.ExecContext(t.Context(), `SELECT pg_advisory_xact_lock(315609238744290)`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	result := make(chan error, 1)
	go func() { result <- waitCompilerInputAdmissionWriter(t.Context(), tx) }()
	select {
	case err := <-result:
		t.Fatal("admission passed a live exclusive writer", err)
	case <-time.After(50 * time.Millisecond):
	}
	if err := holder.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal("admission did not resume after release", err)
	}
	other, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Rollback()
	var acquired bool
	if err := other.QueryRowContext(t.Context(), `SELECT pg_try_advisory_xact_lock(315609238744290)`).Scan(&acquired); err != nil || acquired {
		t.Fatal("admission did not hold its fence until rollback", acquired, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := other.QueryRowContext(t.Context(), `SELECT pg_try_advisory_xact_lock(315609238744290)`).Scan(&acquired); err != nil || !acquired {
		t.Fatal("admission leaked its fence", acquired, err)
	}
}
