package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"fugue/internal/inputarchive"
)

type CompilerInputArchiveReadCheckOptions struct {
	Digest      string
	MaxRawBytes int64
}

// A read check measures one complete native archive reconstruction. It is not
// a migration receipt, a reader route or authority to retire any source data.
type CompilerInputArchiveReadCheckReport struct {
	Action               string `json:"action"`
	Digest               string `json:"digest"`
	ReadOnly             bool   `json:"read_only"`
	Verified             bool   `json:"verified"`
	RetirementAuthorized bool   `json:"retirement_authorized"`
	RawBytes             int64  `json:"raw_bytes"`
	StoredBytes          int64  `json:"stored_bytes"`
	Segments             int    `json:"segments"`
	MetadataElapsedMS    int64  `json:"metadata_elapsed_ms"`
	ReadbackElapsedMS    int64  `json:"readback_elapsed_ms"`
	TotalElapsedMS       int64  `json:"total_elapsed_ms"`
}

func RunCompilerInputArchiveReadCheck(ctx context.Context, databaseURL string, options CompilerInputArchiveReadCheckOptions) (CompilerInputArchiveReadCheckReport, error) {
	report := CompilerInputArchiveReadCheckReport{Action: "archive-read-check", Digest: options.Digest, ReadOnly: true}
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
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return report, err
	}
	defer tx.Rollback()
	for _, statement := range []string{`SET LOCAL statement_timeout='5s'`, `SET LOCAL lock_timeout='250ms'`} {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			return report, err
		}
	}
	// Resolve only the dedicated archive reader. Never discover another bucket,
	// construct a Store, run migrations, acquire a writer lease or create objects.
	cfg, configured, err := resolveCompilerInputArchiveConfig(ctx, tx, false)
	if err != nil || !configured {
		return report, fmt.Errorf("dedicated archive reader unavailable: %w", ErrConflict)
	}
	backend, err := cfg.Fingerprint()
	if err != nil {
		return report, err
	}
	manifest, err := compilerInputArchiveReadCheckManifest(ctx, tx, backend, options)
	if err != nil {
		return report, err
	}
	// All receipt/config metadata comes from one read-only snapshot. Release it
	// before network I/O; the verified immutable objects carry the exact hashes.
	if err = tx.Commit(); err != nil {
		return report, err
	}
	objects, err := inputarchive.NewS3(cfg)
	if err != nil {
		return report, err
	}
	report.MetadataElapsedMS = time.Since(started).Milliseconds()
	report, err = compilerInputArchiveReadCheckBody(ctx, objects, manifest, report)
	report.TotalElapsedMS = time.Since(started).Milliseconds()
	return report, err
}

func compilerInputArchiveReadCheckManifest(ctx context.Context, db inputQueryer, backend string, options CompilerInputArchiveReadCheckOptions) (inputarchive.Manifest, error) {
	m, err := readInputManifest(ctx, db, options.Digest)
	if err != nil {
		return inputarchive.Manifest{}, err
	}
	if validateCompilerInputManifest(m) != nil || (m.Format != inputFormatV1 && m.Format != inputFormatCodecV2) || m.Size > options.MaxRawBytes {
		return inputarchive.Manifest{}, ErrConflict
	}
	var state string
	if err = db.QueryRowContext(ctx, `SELECT state FROM fugue_platform_input_archives WHERE content_hash=$1`, options.Digest).Scan(&state); err != nil {
		return inputarchive.Manifest{}, err
	}
	if state != "verified" && state != "routed" {
		return inputarchive.Manifest{}, ErrConflict
	}
	return compilerInputArchiveManifest(ctx, db, m, backend, state)
}

func compilerInputArchiveReadCheckBody(ctx context.Context, objects inputarchive.Objects, manifest inputarchive.Manifest, report CompilerInputArchiveReadCheckReport) (CompilerInputArchiveReadCheckReport, error) {
	report.RawBytes, report.Segments = manifest.RawBytes, len(manifest.Segments)
	report.StoredBytes = 0
	for _, segment := range manifest.Segments {
		report.StoredBytes += int64(segment.StoredBytes)
	}
	started := time.Now()
	// This is the exact Go reader used by final archive verification, including
	// compressed and raw segment hashes, lengths and the full original digest.
	err := inputarchive.Verify(ctx, objects, manifest)
	report.ReadbackElapsedMS = time.Since(started).Milliseconds()
	report.Verified = err == nil
	return report, err
}
