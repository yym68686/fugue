package store

import (
	"context"
	"database/sql"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func compilerInputBarrierBusy() error {
	// Preserve the caller's existing retry classification without deliberately
	// aborting a PostgreSQL transaction and logging an ERROR for normal contention.
	return &pgconn.PgError{Code: "55P03", Message: "compiler input writer barrier is busy"}
}

// Append-only input writes serialize on a separate queue. They never replace
// committed canonical bytes: new manifests/refs become visible together only
// after their complete readback verification and commit. The shared lifecycle
// barrier still excludes retirement, migration, fencing and old exclusive
// writers, while allowing readers of already committed inputs to proceed.
// Acquire the queue first so queued writers do not hold up lifecycle changes.
func lockCompilerInputAppend(ctx context.Context, tx *sql.Tx, observation *compilerInputWriterAttempt) error {
	observation.advance(compilerInputWriterQueue)
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(315609238744291)`); err != nil {
		return err
	}
	observation.advance(compilerInputWriterBarrier)
	_, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock_shared(315609238744290)`)
	return err
}

func waitCompilerInputAdmissionWriter(ctx context.Context, tx *sql.Tx) error {
	var acquired bool
	var timeoutMS int64
	if err := tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock_shared(315609238744290),
 (SELECT setting::bigint FROM pg_settings WHERE name='lock_timeout')`).Scan(&acquired, &timeoutMS); err != nil {
		return err
	}
	if acquired {
		return nil
	}
	if timeoutMS <= 0 || timeoutMS > 250 {
		timeoutMS = 250
	}
	deadline := time.Now().Add(time.Duration(timeoutMS) * time.Millisecond)
	delay := 25 * time.Millisecond
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return compilerInputBarrierBusy()
		}
		timer := time.NewTimer(min(delay, remaining))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if !time.Now().Before(deadline) {
			return compilerInputBarrierBusy()
		}
		if err := tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock_shared(315609238744290)`).Scan(&acquired); err != nil {
			return err
		}
		if acquired {
			return nil
		}
		delay = min(delay*2, 100*time.Millisecond)
	}
}
