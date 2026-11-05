package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"time"

	"fugue/internal/inputarchive"
)

type CompilerInputLocalReadbackOptions struct {
	Digest      string
	MaxRawBytes int64
}

type CompilerInputLocalReadbackReport struct {
	Action               string    `json:"action"`
	Digest               string    `json:"digest"`
	ReadOnly             bool      `json:"read_only"`
	Verified             bool      `json:"verified"`
	RetirementAuthorized bool      `json:"retirement_authorized"`
	Snapshot             string    `json:"database_snapshot"`
	ObservedAt           time.Time `json:"observed_at"`
	Format               string    `json:"format"`
	Location             string    `json:"location"`
	RawBytes             int64     `json:"raw_bytes"`
	ReadBytes            int64     `json:"read_bytes"`
	ElapsedMS            int64     `json:"elapsed_ms"`
}

// RunCompilerInputLocalReadback verifies one complete local representation.
// It deliberately bypasses archive routing and never resolves R2 credentials.
// A successful read is evidence for this digest only; it is not a dependency
// closure, persistent pin, or authority to retire the legacy relation.
func RunCompilerInputLocalReadback(ctx context.Context, databaseURL string, options CompilerInputLocalReadbackOptions) (CompilerInputLocalReadbackReport, error) {
	report := CompilerInputLocalReadbackReport{Action: "archive-hot-local-readback", Digest: options.Digest, ReadOnly: true}
	if !inputDigestPattern.MatchString(options.Digest) || options.MaxRawBytes < 1 || options.MaxRawBytes > inputarchive.MaxInputBytes {
		return report, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	started := time.Now()
	db, err := openPostgresDatabase(databaseURL)
	if err != nil {
		return report, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	s := &Store{db: db, compilerInputArchiveReadLeaseEnabled: true}
	tx, finish, err := s.BeginCompilerInputReadLease(ctx)
	if err != nil {
		return report, err
	}
	defer finish()
	for _, statement := range []string{`SET LOCAL statement_timeout='5s'`, `SET LOCAL lock_timeout='250ms'`, `SET LOCAL work_mem='1MB'`, `SET LOCAL max_parallel_workers_per_gather=0`, `SET LOCAL jit=off`} {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			return report, err
		}
	}
	var coherent bool
	if err = tx.QueryRowContext(ctx, `SELECT transaction_timestamp(),pg_current_snapshot()::text,current_setting('transaction_isolation')='repeatable read' AND current_setting('transaction_read_only')='on'`).Scan(&report.ObservedAt, &report.Snapshot, &coherent); err != nil {
		return report, err
	}
	if !coherent {
		return report, ErrConflict
	}
	manifest, err := readInputManifest(ctx, tx, options.Digest)
	if err != nil {
		return report, err
	}
	report.Format, report.Location, report.RawBytes = manifest.Format, manifest.Location, manifest.Size
	if report.ReadBytes, err = verifyCompilerInputLocalRepresentation(ctx, tx, manifest, options.MaxRawBytes); err != nil {
		return report, err
	}
	if err = tx.Commit(); err != nil {
		return report, err
	}
	report.Verified = true
	// No GC/re-activation barrier is part of this evidence yet.
	report.RetirementAuthorized = false
	report.ElapsedMS = time.Since(started).Milliseconds()
	return report, nil
}

// The caller owns the coherent snapshot and reader barrier. Shared v3/v4
// packs do not use the old chunks relation even though their manifest retains
// the historical default location. v1/v2 must use the independent hot table;
// neither old chunks nor a perfectly valid archive is a substitute for it.
func verifyCompilerInputLocalRepresentation(ctx context.Context, db inputQueryer, m inputManifest, maxBytes int64) (int64, error) {
	if validateCompilerInputManifest(m) != nil || maxBytes < 1 || maxBytes > inputMaxBytes || m.Size > maxBytes {
		return 0, fmt.Errorf("compiler input local readback exceeds its declared byte budget or manifest: %w", ErrConflict)
	}
	if (m.Format == inputFormatV1 || m.Format == inputFormatCodecV2) && m.Location != "hot" {
		return 0, fmt.Errorf("compiler input lacks an independent local representation: %w", ErrConflict)
	}
	reader, err := openInputChunks(ctx, db, m)
	if err != nil {
		return 0, err
	}
	hash := sha256.New()
	n, readErr := io.Copy(hash, io.LimitReader(reader, maxBytes+1))
	closeErr := reader.Close()
	if readErr != nil {
		return n, readErr
	}
	if closeErr != nil {
		return n, closeErr
	}
	if err = ctx.Err(); err != nil {
		return n, err
	}
	if n != m.Size || n > maxBytes || !reader.done || reader.err != nil || "sha256:"+hex.EncodeToString(hash.Sum(nil)) != m.Digest {
		return n, fmt.Errorf("compiler input local readback digest, length or representation differs: %w", ErrConflict)
	}
	return n, nil
}
