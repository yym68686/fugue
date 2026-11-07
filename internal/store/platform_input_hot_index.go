package store

import (
	"context"
	"database/sql"
	"time"

	"fugue/internal/schemamigrate"
)

// Bind both tuple address and transaction ID. Any UPDATE, including a HOT
// update in the same transaction, invalidates the index. The 64-bit build XID
// horizon refuses entries across XID wrap/restore rather than trusting a reused
// 32-bit xmin and ctid. This comparison allocates no XID in read-only inventory.
const compilerInputHotIndexCurrent = `i.source_xmin=a.xmin AND i.source_ctid=a.ctid
 AND pg_snapshot_xmax(pg_current_snapshot())::text::bigint>=i.built_xid
 AND pg_snapshot_xmax(pg_current_snapshot())::text::bigint-i.built_xid<2147483648`

type CompilerInputHotIndexOptions struct {
	AfterID string
	Limit   int
	Apply   bool
}

type CompilerInputHotIndexReport struct {
	Action               string    `json:"action"`
	ObservedAt           time.Time `json:"observed_at"`
	Selected             int       `json:"selected"`
	NextAfterID          string    `json:"next_after_id"`
	SourceBytes          int64     `json:"source_bytes"`
	IndexBytes           int64     `json:"index_bytes"`
	WALBytes             int64     `json:"wal_bytes"`
	Applied              bool      `json:"applied"`
	RetirementAuthorized bool      `json:"retirement_authorized"`
}

// One keyset page of DNS artifacts, at most 32 entries/32MiB source, never a
// full-store loop. Other kinds continue to use the original bounded projection.
// It changes only the derived index; source/retention/routing are untouched.
// Preview executes the real derivation then rolls back (and can consume WAL).
// A trusted runner must enforce fresh source-space/WAL/load gates before Apply.
func RunCompilerInputHotIndex(ctx context.Context, databaseURL string, options CompilerInputHotIndexOptions) (CompilerInputHotIndexReport, error) {
	report := CompilerInputHotIndexReport{Action: "archive-hot-index"}
	if options.Limit < 1 || options.Limit > 32 || len(options.AfterID) > 512 {
		return report, ErrInvalidInput
	}
	db, err := openPostgresDatabase(databaseURL)
	if err != nil {
		return report, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return report, err
	}
	defer tx.Rollback()
	for _, statement := range []string{`SET LOCAL statement_timeout='5s'`, `SET LOCAL lock_timeout='250ms'`, `SET LOCAL work_mem='1MB'`, `SET LOCAL max_parallel_workers_per_gather=0`, `SET LOCAL jit=off`} {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			return report, err
		}
	}
	if err = schemamigrate.EnforceCompilerInputZeroTemp(ctx, tx); err != nil {
		return report, err
	}
	if err = schemamigrate.VerifyCompilerInputHotIndex(ctx, tx); err != nil {
		return report, err
	}
	if err = tx.QueryRowContext(ctx, `SELECT transaction_timestamp()`).Scan(&report.ObservedAt); err != nil {
		return report, err
	}
	var startWAL string
	if err = tx.QueryRowContext(ctx, `SELECT pg_current_wal_insert_lsn()::text`).Scan(&startWAL); err != nil {
		return report, err
	}
	walGate := func() error {
		if err := tx.QueryRowContext(ctx, `SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),$1::pg_lsn)::bigint`, startWAL).Scan(&report.WALBytes); err != nil {
			return err
		}
		// This includes concurrent WAL, not an attribution to this operation.
		if report.WALBytes < 0 || report.WALBytes > 256<<20 {
			return ErrConflict
		}
		return nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT id FROM fugue_platform_artifacts WHERE artifact_kind='dns_answer_bundle' AND id>$1 ORDER BY id LIMIT $2`, options.AfterID, options.Limit)
	if err != nil {
		return report, err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			break
		}
		ids = append(ids, id)
	}
	if err == nil {
		err = rows.Err()
	}
	if closeErr := rows.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return report, err
	}
	for _, id := range ids {
		if err = walGate(); err != nil {
			return report, err
		}
		// Avoid ON CONFLICT DO UPDATE: PostgreSQL would derive the same large
		// source in both the INSERT and UPDATE trigger. Transaction rollback
		// preserves the old entry if this bounded page refuses.
		if _, err = tx.ExecContext(ctx, `DELETE FROM fugue_platform_input_hot_dependency_index_v1 WHERE artifact_id=$1`, id); err != nil {
			return report, err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO fugue_platform_input_hot_dependency_index_v1(artifact_id) VALUES($1)`, id); err != nil {
			return report, err
		}
		var sourceBytes, indexBytes int64
		if err = tx.QueryRowContext(ctx, `SELECT source_bytes,octet_length(content_text)+octet_length(metadata_text)
 FROM fugue_platform_input_hot_dependency_index_v1 WHERE artifact_id=$1`, id).Scan(&sourceBytes, &indexBytes); err != nil {
			return report, err
		}
		report.SourceBytes += sourceBytes
		report.IndexBytes += indexBytes
		if report.SourceBytes+report.IndexBytes > 32<<20 {
			return report, ErrConflict
		}
		report.Selected++
		report.NextAfterID = id
	}
	if err = walGate(); err != nil {
		return report, err
	}
	if options.Apply {
		if err = tx.Commit(); err != nil {
			return report, err
		}
		report.Applied = true
	}
	return report, nil
}
