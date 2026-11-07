package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"time"

	"fugue/internal/inputarchive"
	"fugue/internal/schemamigrate"
)

var compilerInputPinKeyPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,127}$`)

type CompilerInputLocalPinOptions struct {
	Key           string
	Digest        string
	MaxRawBytes   int64
	Apply         bool
	Release       bool
	ExpectedToken int64
}

type CompilerInputLocalPinReport struct {
	Action               string `json:"action"`
	Key                  string `json:"key"`
	Digest               string `json:"digest"`
	Token                int64  `json:"token"`
	State                string `json:"state"`
	Applied              bool   `json:"applied"`
	Verified             bool   `json:"verified"`
	RawBytes             int64  `json:"raw_bytes"`
	RetirementAuthorized bool   `json:"retirement_authorized"`
}

// A pin protects one independent local representation, not a whole release
// closure or reactivation admission. Pending pins protect bytes too: register
// in a short writer transaction, verify outside that lock, then promote by CAS.
// Failed/cancelled verification leaves a visible pending pin for exact retry or
// explicit release; it never holds the global writer across the full readback.
func RunCompilerInputLocalPin(ctx context.Context, databaseURL string, options CompilerInputLocalPinOptions) (CompilerInputLocalPinReport, error) {
	if err := validateCompilerInputLocalPinOptions(options); err != nil {
		return CompilerInputLocalPinReport{}, err
	}
	db, err := openPostgresDatabase(databaseURL)
	if err != nil {
		return CompilerInputLocalPinReport{}, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return runCompilerInputLocalPin(ctx, db, options)
}

func runCompilerInputLocalPin(ctx context.Context, db *sql.DB, options CompilerInputLocalPinOptions) (CompilerInputLocalPinReport, error) {
	report := CompilerInputLocalPinReport{Action: "archive-local-pin", Key: options.Key, Digest: options.Digest}
	if options.Release {
		report.Action = "archive-local-unpin"
	}
	if err := validateCompilerInputLocalPinOptions(options); err != nil {
		return report, err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	if options.Release {
		return releaseCompilerInputLocalPin(ctx, db, options, report)
	}
	if !options.Apply {
		tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			return report, err
		}
		defer tx.Rollback()
		for _, statement := range []string{`SET LOCAL statement_timeout='5s'`, `SET LOCAL lock_timeout='250ms'`} {
			if _, err = tx.ExecContext(ctx, statement); err != nil {
				return report, err
			}
		}
		if err = schemamigrate.VerifyCompilerInputLocalPins(ctx, tx); err != nil {
			return report, err
		}
		m, err := readInputManifest(ctx, tx, options.Digest)
		if err != nil {
			return report, err
		}
		if err = validateCompilerInputLocalPinManifest(m, options.MaxRawBytes); err != nil {
			return report, err
		}
		report.RawBytes, report.State = m.Size, "unverified-preview"
		var state, pinnedDigest string
		if err = tx.QueryRowContext(ctx, `SELECT token,state,content_hash FROM fugue_platform_input_local_pins WHERE pin_key=$1`, options.Key).Scan(&report.Token, &state, &pinnedDigest); err == nil {
			if pinnedDigest != options.Digest {
				return report, ErrConflict
			}
			report.State = state
		} else if !errors.Is(err, sql.ErrNoRows) {
			return report, err
		}
		// Metadata alone is never fresh local readback evidence.
		return report, tx.Commit()
	}
	tx, err := beginCompilerInputPinMutation(ctx, db)
	if err != nil {
		return report, err
	}
	defer tx.Rollback()
	m, err := readInputManifest(ctx, tx, options.Digest)
	if err != nil {
		return report, err
	}
	if err = validateCompilerInputLocalPinManifest(m, options.MaxRawBytes); err != nil {
		return report, err
	}
	epoch, err := readInputArchiveEpoch(ctx, tx, -1)
	if err != nil {
		return report, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO fugue_platform_input_local_pins(pin_key,content_hash,protected_content_hash,source_format,source_location,raw_bytes,chunk_size,chunk_count,source_created_at,source_epoch,state,created_at)
 VALUES($1,$2,$2,$3,$4,$5,$6,$7,$8,$9,'pending',now()) ON CONFLICT(pin_key) DO NOTHING`, options.Key, m.Digest, m.Format, m.Location, m.Size, m.ChunkSize, m.Count, m.Created, epoch)
	if err != nil {
		return report, err
	}
	if err = checkCompilerInputLocalPin(ctx, tx, options.Key, 0, m, epoch, &report); err != nil {
		return report, err
	}
	// An exact retry still needs fresh readback. Do not leave a previously
	// verified label behind if this verification is cancelled or fails.
	if _, err = tx.ExecContext(ctx, `UPDATE fugue_platform_input_local_pins SET state='pending',verified_at=NULL WHERE pin_key=$1 AND token=$2`, options.Key, report.Token); err != nil {
		return report, err
	}
	if err = tx.Commit(); err != nil {
		return report, err
	}
	report.Applied = true // includes a durable pending pin if the next read fails
	report.State = "pending"
	readback, err := runCompilerInputLocalReadback(ctx, db, CompilerInputLocalReadbackOptions{Digest: m.Digest, MaxRawBytes: options.MaxRawBytes})
	if err != nil || !readback.Verified {
		if err == nil {
			err = ErrConflict
		}
		return report, err
	}
	tx, err = beginCompilerInputPinMutation(ctx, db)
	if err != nil {
		return report, err
	}
	defer tx.Rollback()
	if err = checkCompilerInputLocalPin(ctx, tx, options.Key, report.Token, m, epoch, &report); err != nil {
		return report, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE fugue_platform_input_local_pins SET state='verified',verified_at=now() WHERE pin_key=$1 AND token=$2 AND state IN ('pending','verified')`, options.Key, report.Token); err != nil {
		return report, err
	}
	var changed int64
	if err = tx.QueryRowContext(ctx, `SELECT 1 WHERE EXISTS(SELECT 1 FROM fugue_platform_input_local_pins WHERE pin_key=$1 AND token=$2 AND state='verified' AND protected_content_hash=$3)`, options.Key, report.Token, m.Digest).Scan(&changed); err != nil || changed != 1 {
		return report, fmt.Errorf("local pin verification compare-and-swap failed: %w", errors.Join(err, ErrConflict))
	}
	if err = tx.Commit(); err != nil {
		return report, err
	}
	report.Verified, report.State = true, "verified"
	return report, nil
}

func validateCompilerInputLocalPinOptions(options CompilerInputLocalPinOptions) error {
	if !compilerInputPinKeyPattern.MatchString(options.Key) || !inputDigestPattern.MatchString(options.Digest) ||
		options.Release && (!options.Apply || options.ExpectedToken < 1 || options.MaxRawBytes != 0) ||
		!options.Release && (options.ExpectedToken != 0 || options.MaxRawBytes < 1 || options.MaxRawBytes > inputarchive.MaxInputBytes) {
		return ErrInvalidInput
	}
	if options.Apply {
		enabled, err := inputarchive.MigrationEnabledFromEnv()
		if err != nil || !enabled {
			return fmt.Errorf("local pin mutation requires migration opt-in: %w", ErrConflict)
		}
	}
	return nil
}

func validateCompilerInputLocalPinManifest(m inputManifest, maxBytes int64) error {
	if validateCompilerInputManifest(m) != nil || m.Size > maxBytes ||
		(m.Format == inputFormatV1 || m.Format == inputFormatCodecV2) && m.Location != "hot" {
		return fmt.Errorf("local pin requires a bounded independent local representation: %w", ErrConflict)
	}
	return nil
}

func beginCompilerInputPinMutation(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	// Same lock order as maintenance; a busy production writer wins. No retry,
	// network I/O, body verification or GC runs inside these short transactions.
	for _, statement := range []string{`SET LOCAL lock_timeout='250ms'`, `SET LOCAL statement_timeout='5s'`, `SELECT pg_advisory_xact_lock_shared(hashtextextended('fugue.compiler-input-gc.v1',0))`, `SELECT pg_advisory_xact_lock(315609238744290)`} {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			_ = tx.Rollback()
			return nil, err
		}
	}
	if err = schemamigrate.VerifyCompilerInputLocalPins(ctx, tx); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return tx, nil
}

func checkCompilerInputLocalPin(ctx context.Context, tx *sql.Tx, key string, token int64, expected inputManifest, epoch int64, report *CompilerInputLocalPinReport) error {
	var m inputManifest
	var storedEpoch int64
	var protected sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT token,state,content_hash,source_format,source_location,raw_bytes,chunk_size,chunk_count,source_created_at,source_epoch,protected_content_hash FROM fugue_platform_input_local_pins WHERE pin_key=$1 FOR UPDATE`, key).
		Scan(&report.Token, &report.State, &m.Digest, &m.Format, &m.Location, &m.Size, &m.ChunkSize, &m.Count, &m.Created, &storedEpoch, &protected); err != nil {
		return err
	}
	current, err := readInputManifest(ctx, tx, expected.Digest)
	if err != nil {
		return err
	}
	if _, err = readInputArchiveEpoch(ctx, tx, epoch); err != nil {
		return err
	}
	if m != expected || current != expected || storedEpoch != epoch || !protected.Valid || protected.String != expected.Digest || token != 0 && token != report.Token || report.State != "pending" && report.State != "verified" {
		return fmt.Errorf("local pin identity or maintenance epoch changed: %w", ErrConflict)
	}
	report.RawBytes = m.Size
	return nil
}

func releaseCompilerInputLocalPin(ctx context.Context, db *sql.DB, options CompilerInputLocalPinOptions, report CompilerInputLocalPinReport) (CompilerInputLocalPinReport, error) {
	tx, err := beginCompilerInputPinMutation(ctx, db)
	if err != nil {
		return report, err
	}
	defer tx.Rollback()
	if _, err = readInputArchiveEpoch(ctx, tx, -1); err != nil {
		return report, err
	}
	retiring, err := compilerInputArchiveRetirementStarted(ctx, tx)
	if err != nil || retiring {
		return report, fmt.Errorf("local pin release is fenced by input retirement: %w", errors.Join(err, ErrConflict))
	}
	// Tombstones are retained. A stale release cannot remove a later pin, and a
	// released key cannot silently become a new pin with the old identity.
	if err = tx.QueryRowContext(ctx, `UPDATE fugue_platform_input_local_pins SET state='released',protected_content_hash=NULL,released_at=now() WHERE pin_key=$1 AND token=$2 AND content_hash=$3 AND state IN ('pending','verified') RETURNING token,raw_bytes`, options.Key, options.ExpectedToken, options.Digest).Scan(&report.Token, &report.RawBytes); err != nil {
		return report, fmt.Errorf("exact local pin release not confirmed: %w", errors.Join(err, ErrConflict))
	}
	if err = tx.Commit(); err != nil {
		return report, err
	}
	report.Applied, report.State = true, "released"
	return report, nil
}

// Call only under the common compiler-input writer lock. Missing optional
// schema preserves pre-pin compatibility; a malformed installed table fails.
func compilerInputLocalPinsSource(ctx context.Context, db inputQueryer, digest string) (bool, error) {
	var installed, pinned bool
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('fugue_platform_input_local_pins') IS NOT NULL`).Scan(&installed); err != nil || !installed {
		return false, err
	}
	if err := schemamigrate.VerifyCompilerInputLocalPins(ctx, db); err != nil {
		return false, err
	}
	err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM fugue_platform_input_local_pins WHERE content_hash=$1 AND protected_content_hash=$1 AND state IN ('pending','verified'))`, digest).Scan(&pinned)
	return pinned, err
}
