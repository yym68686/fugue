package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// CompilerInputArchiveTransactionDrain is an observation, not a persistent
// lease. It supplements the input-reader barrier: an old RR transaction may
// have obtained a snapshot without touching the relation or taking that lease.
type CompilerInputArchiveTransactionDrain struct {
	OldTransactions      int64      `json:"old_transactions"`
	PreparedTransactions int64      `json:"prepared_transactions"`
	OldestTransaction    *time.Time `json:"oldest_transaction,omitempty"`
}

func compilerInputArchiveTransactionDrain(ctx context.Context, tx *sql.Tx) (CompilerInputArchiveTransactionDrain, error) {
	var report CompilerInputArchiveTransactionDrain
	// PostgreSQL hides other sessions' transaction metadata from ordinary
	// users. A NULL xact_start is not evidence that such a session is idle.
	var canInspect bool
	if err := tx.QueryRowContext(ctx, `SELECT current_setting('is_superuser')::boolean OR pg_has_role(current_user,'pg_read_all_stats','USAGE')`).Scan(&canInspect); err != nil {
		return report, err
	}
	if !canInspect {
		return report, fmt.Errorf("compiler input retirement requires full transaction visibility: %w", ErrConflict)
	}
	// Statistics can otherwise be cached for the duration of this transaction.
	// Re-read immediately before the destructive statement, without cancelling
	// any session or querying/printing SQL, prepared GIDs, or credentials.
	if _, err := tx.ExecContext(ctx, `SELECT pg_stat_clear_snapshot()`); err != nil {
		return report, err
	}
	var oldest sql.NullTime
	if err := tx.QueryRowContext(ctx, `SELECT count(*),min(xact_start)
FROM pg_stat_activity
WHERE datid=(SELECT oid FROM pg_database WHERE datname=current_database())
 AND pid<>pg_backend_pid()
 AND (xact_start<=transaction_timestamp() OR backend_xmin IS NOT NULL)`).Scan(&report.OldTransactions, &oldest); err != nil {
		return report, err
	}
	if oldest.Valid {
		report.OldestTransaction = &oldest.Time
	}
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM pg_prepared_xacts WHERE database=current_database()`).Scan(&report.PreparedTransactions); err != nil {
		return report, err
	}
	if report.OldTransactions != 0 || report.PreparedTransactions != 0 {
		return report, fmt.Errorf("compiler input retirement transactions have not drained (old=%d prepared=%d): %w", report.OldTransactions, report.PreparedTransactions, ErrConflict)
	}
	return report, nil
}
