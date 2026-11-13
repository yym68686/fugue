package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"fugue/internal/inputarchive"
)

type CompilerInputArchiveMigrationOptions struct {
	Digest           string
	Apply            bool
	SegmentBytes     int
	sourcePageChunks int
	wrapSource       func(io.Reader) io.Reader
}

type CompilerInputArchiveRouteOptions struct {
	Digest  string
	Apply   bool
	Unroute bool
}

type CompilerInputArchiveWriteFenceReport struct {
	Action           string `json:"action"`
	Applied          bool   `json:"applied"`
	WritesFenced     bool   `json:"writes_fenced"`
	TriggerInstalled bool   `json:"trigger_installed"`
}

type CompilerInputArchiveMigrationReport struct {
	Action       string `json:"action"`
	Digest       string `json:"digest"`
	Applied      bool   `json:"applied"`
	SourceFormat string `json:"source_format"`
	RawBytes     int64  `json:"raw_bytes"`
	SourceChunks int    `json:"source_chunks"`
	Segments     int    `json:"segments"`
	State        string `json:"state"`
	SourceEpoch  int64  `json:"source_epoch"`
	FencingToken int64  `json:"fencing_token"`
}

type inputArchiveClaim struct {
	manifest inputarchive.Manifest
	source   inputManifest
	epoch    int64
	token    int64
	owner    string
}

func newArchiveWorkerID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// RunCompilerInputArchiveMigration copies one stable v1/v2 input. It never
// deletes source rows or publishes a reader route. A verified result alone is
// insufficient authority to retire the old relation.
func RunCompilerInputArchiveMigration(ctx context.Context, databaseURL string, options CompilerInputArchiveMigrationOptions) (CompilerInputArchiveMigrationReport, error) {
	report := CompilerInputArchiveMigrationReport{Action: "archive", Digest: options.Digest}
	if !inputDigestPattern.MatchString(options.Digest) {
		return report, ErrInvalidInput
	}
	db, err := openPostgresDatabase(databaseURL)
	if err != nil {
		return report, err
	}
	defer db.Close()
	var objects inputarchive.Objects
	var backendHash string
	if options.Apply {
		cfg, configured, err := resolveCompilerInputArchiveConfig(ctx, db, true)
		if err != nil || !configured {
			return report, fmt.Errorf("dedicated compiler input archive backend is required: %w", ErrConflict)
		}
		migrationEnabled, err := inputarchive.MigrationEnabledFromEnv()
		if err != nil || !migrationEnabled {
			return report, fmt.Errorf("compiler input archive migration opt-in is required: %w", ErrConflict)
		}
		objects, err = inputarchive.NewS3(cfg)
		if err != nil {
			return report, err
		}
		backendHash, err = cfg.Fingerprint()
		if err != nil {
			return report, err
		}
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return copyCompilerInputArchive(ctx, db, objects, backendHash, options)
}

func copyCompilerInputArchive(ctx context.Context, db *sql.DB, objects inputarchive.Objects, backendHash string, options CompilerInputArchiveMigrationOptions) (CompilerInputArchiveMigrationReport, error) {
	report := CompilerInputArchiveMigrationReport{Action: "archive", Digest: options.Digest}
	if !inputDigestPattern.MatchString(options.Digest) || options.SegmentBytes < 0 || options.SegmentBytes > inputarchive.MaxSegmentBytes || options.SegmentBytes > 0 && options.SegmentBytes < 65536 || options.Apply && (!inputDigestPattern.MatchString(backendHash) || objects == nil) {
		return report, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	claim, err := claimCompilerInputArchive(ctx, db, backendHash, options)
	if err != nil {
		return report, err
	}
	report.SourceFormat, report.RawBytes, report.SourceChunks, report.SourceEpoch = claim.source.Format, claim.source.Size, claim.source.Count, claim.epoch
	if !options.Apply {
		report.State = "source-manifest-observed"
		return report, nil
	}
	source := &archiveInputStream{ctx: ctx, db: db, m: claim.source, hash: sha256.New(), sourceEpoch: claim.epoch, pageChunks: options.sourcePageChunks}
	var stream io.Reader = source
	if options.wrapSource != nil {
		stream = options.wrapSource(stream)
	}
	got, err := inputarchive.Copy(ctx, objects, stream, claim.manifest, func(seg inputarchive.Segment) error {
		return checkpointCompilerInputArchive(ctx, db, claim, seg)
	})
	if err != nil {
		// Receipts remain resumable. Never mutate another owner's newer lease
		// or claim success after a timeout with an unknown commit outcome.
		return report, err
	}
	if err = completeCompilerInputArchive(ctx, db, claim, got); err != nil {
		return report, err
	}
	report.Applied, report.State, report.Segments, report.FencingToken = true, "verified", len(got.Segments), claim.token
	return report, nil
}

// RunCompilerInputArchiveRoute changes one verified input's read location.
// The legacy chunks remain intact so an explicit unroute can restore them.
func RunCompilerInputArchiveRoute(ctx context.Context, databaseURL string, options CompilerInputArchiveRouteOptions) (CompilerInputArchiveMigrationReport, error) {
	action := "archive-route"
	if options.Unroute {
		action = "archive-unroute"
	}
	report := CompilerInputArchiveMigrationReport{Action: action, Digest: options.Digest}
	if !inputDigestPattern.MatchString(options.Digest) || !options.Apply {
		return report, ErrInvalidInput
	}
	enabled, err := inputarchive.RoutingEnabledFromEnv()
	if err != nil || !enabled {
		return report, fmt.Errorf("compiler input archive routing opt-in is required: %w", ErrConflict)
	}
	db, err := openPostgresDatabase(databaseURL)
	if err != nil {
		return report, err
	}
	defer db.Close()
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	if options.Unroute {
		report, err = unrouteCompilerInputArchive(ctx, db, options.Digest)
		if err == nil {
			report.Action = action
		}
		return report, err
	}
	cfg, configured, err := resolveCompilerInputArchiveConfig(ctx, db, false)
	if err != nil || !configured {
		return report, fmt.Errorf("dedicated compiler input archive backend is required: %w", ErrConflict)
	}
	objects, err := inputarchive.NewS3(cfg)
	if err != nil {
		return report, err
	}
	backendHash, err := cfg.Fingerprint()
	if err != nil {
		return report, err
	}
	return routeCompilerInputArchive(ctx, db, objects, backendHash, options.Digest)
}

// RunCompilerInputArchiveWriteFence installs or removes a database-level
// trigger in the same transaction that changes the writer fence. It is an
// explicit maintenance operation: merely deploying this code does not pause
// legacy writes or add per-row overhead.
func RunCompilerInputArchiveWriteFence(ctx context.Context, databaseURL string, apply, disarm bool) (CompilerInputArchiveWriteFenceReport, error) {
	action := "archive-write-fence"
	if disarm {
		action = "archive-write-unfence"
	}
	report := CompilerInputArchiveWriteFenceReport{Action: action}
	if !apply {
		return report, ErrInvalidInput
	}
	db, err := openPostgresDatabase(databaseURL)
	if err != nil {
		return report, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return report, err
	}
	defer tx.Rollback()
	for _, statement := range []string{`SET LOCAL lock_timeout='250ms'`, `SET LOCAL statement_timeout='5s'`, `SELECT pg_advisory_xact_lock(315609238744290)`} {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			return report, err
		}
	}
	var fenced bool
	var retiredAt sql.NullTime
	if err = tx.QueryRowContext(ctx, `SELECT writes_fenced,retirement_started_at FROM fugue_platform_input_archive_control WHERE singleton FOR UPDATE`).Scan(&fenced, &retiredAt); err != nil {
		return report, fmt.Errorf("compiler input archive control unavailable: %w", ErrConflict)
	}
	var hotTableInstalled bool
	if err = tx.QueryRowContext(ctx, `SELECT to_regclass('fugue_platform_input_hot_chunks') IS NOT NULL`).Scan(&hotTableInstalled); err != nil || !hotTableInstalled {
		return report, fmt.Errorf("compiler input hot chunk relation is unavailable: %w", ErrConflict)
	}
	fence, err := compilerInputArchiveWriteFenceState(ctx, tx)
	if err != nil {
		return report, err
	}
	inline, err := compilerInputArchiveInlineWriteFenceState(ctx, tx)
	if err != nil {
		return report, err
	}
	if retiredAt.Valid {
		return report, fmt.Errorf("compiler input chunks relation has entered retirement and cannot be unfenced: %w", ErrConflict)
	}
	if disarm {
		if fenced != fence.Present || fence.Present && !fence.Compatible || inline.unexpected(fenced) {
			return report, fmt.Errorf("compiler input write fence state and database trigger disagree: %w", ErrConflict)
		}
		if !fenced {
			if err = tx.Commit(); err != nil {
				return report, err
			}
			return report, nil
		}
		if inline.Present {
			if _, err = tx.ExecContext(ctx, `DROP TRIGGER fugue_platform_input_inline_write_fence ON fugue_platform_artifact_contents; DROP FUNCTION fugue_platform_input_inline_write_fence()`); err != nil {
				return report, err
			}
		}
		if inline.Truncate.Present {
			if _, err = tx.ExecContext(ctx, `DROP TRIGGER fugue_platform_input_inline_truncate_fence ON fugue_platform_artifact_contents; DROP FUNCTION fugue_platform_input_inline_truncate_fence()`); err != nil {
				return report, err
			}
		}
		if _, err = tx.ExecContext(ctx, `DROP TRIGGER fugue_platform_input_chunks_archive_write_fence ON fugue_platform_input_chunks`); err != nil {
			return report, err
		}
		if _, err = tx.ExecContext(ctx, `DROP FUNCTION fugue_platform_input_chunks_archive_write_fence()`); err != nil {
			return report, err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE fugue_platform_input_archive_control SET writes_fenced=false,updated_at=now() WHERE singleton`); err != nil {
			return report, err
		}
		if err = tx.Commit(); err != nil {
			return report, err
		}
		report.Applied = true
		return report, nil
	}
	// Capability deployment alone is not readiness to fence legacy writers.
	// Require the one-way retained-input admission switch independently of
	// the fence's own database trigger and external resource/version gates.
	var admissionPhase string
	err = tx.QueryRowContext(ctx, `SELECT phase FROM fugue_platform_input_history_admission WHERE singleton FOR SHARE`).Scan(&admissionPhase)
	if err != nil || admissionPhase != "enforced" {
		return report, fmt.Errorf("compiler input history admission must be enforced before writer fencing: %w", errors.Join(err, ErrConflict))
	}
	if fenced != fence.Present || fence.Present && !fence.Compatible || !inline.SchemaCompatible || inline.unexpected(fenced) {
		return report, fmt.Errorf("compiler input write fence state and database trigger disagree: %w", ErrConflict)
	}
	inlineChanged, err := installCompilerInputInlineWriteFence(ctx, tx, inline)
	if err != nil {
		return report, err
	}
	if fenced {
		if !fence.Always {
			if _, err = tx.ExecContext(ctx, `ALTER TABLE fugue_platform_input_chunks ENABLE ALWAYS TRIGGER fugue_platform_input_chunks_archive_write_fence`); err != nil {
				return report, err
			}
		}
		if err = tx.Commit(); err != nil {
			return report, err
		}
		report.Applied = !fence.Always || inlineChanged
		report.WritesFenced, report.TriggerInstalled = true, true
		return report, nil
	}
	if _, err = tx.ExecContext(ctx, `CREATE FUNCTION fugue_platform_input_chunks_archive_write_fence() RETURNS trigger LANGUAGE plpgsql AS $fence$`+compilerInputArchiveFenceBody+`$fence$`); err != nil {
		return report, err
	}
	if _, err = tx.ExecContext(ctx, `CREATE TRIGGER fugue_platform_input_chunks_archive_write_fence BEFORE INSERT OR UPDATE OR DELETE ON fugue_platform_input_chunks FOR EACH STATEMENT EXECUTE FUNCTION fugue_platform_input_chunks_archive_write_fence()`); err != nil {
		return report, err
	}
	if _, err = tx.ExecContext(ctx, `ALTER TABLE fugue_platform_input_chunks ENABLE ALWAYS TRIGGER fugue_platform_input_chunks_archive_write_fence`); err != nil {
		return report, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE fugue_platform_input_archive_control SET writes_fenced=true,updated_at=now() WHERE singleton AND writes_fenced=false AND retirement_started_at IS NULL`); err != nil {
		return report, err
	}
	if err = tx.Commit(); err != nil {
		return report, err
	}
	report.Applied, report.WritesFenced, report.TriggerInstalled = true, true, true
	return report, nil
}

func routeCompilerInputArchive(ctx context.Context, db *sql.DB, objects inputarchive.Objects, backendHash, digest string) (CompilerInputArchiveMigrationReport, error) {
	report := CompilerInputArchiveMigrationReport{Action: "archive-route", Digest: digest}
	if !inputDigestPattern.MatchString(digest) || !inputDigestPattern.MatchString(backendHash) || objects == nil {
		return report, ErrInvalidInput
	}
	source, epoch, err := verifiedArchiveSource(ctx, db, digest)
	if err != nil {
		return report, err
	}
	var state string
	if err = db.QueryRowContext(ctx, `SELECT state FROM fugue_platform_input_archives WHERE content_hash=$1`, digest).Scan(&state); err != nil {
		return report, err
	}
	if state != "verified" && state != "routed" {
		return report, fmt.Errorf("compiler input archive is not verified: %w", ErrConflict)
	}
	expectedState := state
	archive, err := compilerInputArchiveManifest(ctx, db, source, backendHash, expectedState)
	if err != nil {
		return report, err
	}
	if err = inputarchive.Verify(ctx, objects, archive); err != nil {
		return report, err
	}
	if err = verifyCompilerInputArchiveSource(ctx, db, source, epoch); err != nil {
		return report, err
	}
	tx, err := beginInputArchiveTx(ctx, db)
	if err != nil {
		return report, err
	}
	defer tx.Rollback()
	retiring, err := compilerInputArchiveRetirementStarted(ctx, tx)
	if err != nil {
		return report, err
	}
	if retiring {
		return report, fmt.Errorf("compiler input chunks relation is retired: %w", ErrConflict)
	}
	currentEpoch, err := readInputArchiveEpoch(ctx, tx, epoch)
	if err != nil {
		return report, err
	}
	currentSource, err := readInputManifest(ctx, tx, digest)
	if err != nil || currentSource != source {
		if err != nil {
			return report, err
		}
		return report, fmt.Errorf("compiler input archive source changed before route: %w", ErrConflict)
	}
	var currentState string
	if err = tx.QueryRowContext(ctx, `SELECT state FROM fugue_platform_input_archives WHERE content_hash=$1`, digest).Scan(&currentState); err != nil {
		return report, err
	}
	if currentState == "routed" {
		currentArchive, routeErr := compilerInputArchiveManifest(ctx, tx, currentSource, backendHash, "routed")
		if routeErr != nil || !sameCompilerInputArchiveManifest(archive, currentArchive) {
			if routeErr != nil {
				return report, routeErr
			}
			return report, fmt.Errorf("compiler input archive route changed during retry: %w", ErrConflict)
		}
		return compilerInputArchiveRouteReport(report, source, currentArchive, epoch, false), nil
	}
	if currentState != "verified" {
		return report, fmt.Errorf("compiler input archive state changed before route: %w", ErrConflict)
	}
	if expectedState == "routed" {
		return report, fmt.Errorf("compiler input archive was unrouted during verification: %w", ErrConflict)
	}
	currentArchive, err := compilerInputArchiveManifest(ctx, tx, currentSource, backendHash, "verified")
	if err != nil || !sameCompilerInputArchiveManifest(archive, currentArchive) {
		if err != nil {
			return report, err
		}
		return report, fmt.Errorf("compiler input archive receipt changed before route: %w", ErrConflict)
	}
	result, err := tx.ExecContext(ctx, `UPDATE fugue_platform_input_archives SET state='routed',routed_at=now(),updated_at=now() WHERE content_hash=$1 AND archive_format=$2 AND source_format=$3 AND raw_bytes=$4 AND backend_hash=$5 AND state='verified' AND verified_digest=content_hash AND source_epoch=$6 AND EXISTS (SELECT 1 FROM fugue_platform_input_archive_segments WHERE content_hash=$1 AND uploaded_at IS NOT NULL AND verified_at IS NOT NULL)`, digest, inputarchive.Format, source.Format, source.Size, backendHash, currentEpoch)
	if err != nil {
		return report, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return report, err
	}
	if changed == 0 {
		if err = tx.QueryRowContext(ctx, `SELECT state FROM fugue_platform_input_archives WHERE content_hash=$1`, digest).Scan(&state); err != nil {
			return report, err
		}
		if state != "routed" {
			return report, fmt.Errorf("compiler input archive route compare-and-swap failed: %w", ErrConflict)
		}
		currentArchive, err = compilerInputArchiveManifest(ctx, tx, currentSource, backendHash, "routed")
		if err != nil || !sameCompilerInputArchiveManifest(archive, currentArchive) {
			if err != nil {
				return report, err
			}
			return report, fmt.Errorf("compiler input archive route changed during retry: %w", ErrConflict)
		}
	} else if err = tx.Commit(); err != nil {
		return report, err
	}
	return compilerInputArchiveRouteReport(report, source, archive, epoch, changed == 1), nil
}

func compilerInputArchiveRouteReport(report CompilerInputArchiveMigrationReport, source inputManifest, archive inputarchive.Manifest, epoch int64, applied bool) CompilerInputArchiveMigrationReport {
	report.Applied = applied
	report.SourceFormat, report.RawBytes, report.SourceChunks = source.Format, source.Size, source.Count
	report.Segments, report.State, report.SourceEpoch = len(archive.Segments), "routed", epoch
	return report
}

func sameCompilerInputArchiveManifest(a, b inputarchive.Manifest) bool {
	if a.Format != b.Format || a.Digest != b.Digest || a.RawBytes != b.RawBytes || a.SegmentBytes != b.SegmentBytes || a.Prefix != b.Prefix || len(a.Segments) != len(b.Segments) {
		return false
	}
	for i := range a.Segments {
		if a.Segments[i] != b.Segments[i] {
			return false
		}
	}
	return true
}

func unrouteCompilerInputArchive(ctx context.Context, db *sql.DB, digest string) (CompilerInputArchiveMigrationReport, error) {
	report := CompilerInputArchiveMigrationReport{Action: "archive-unroute", Digest: digest}
	var backendHash, state string
	if !inputDigestPattern.MatchString(digest) {
		return report, ErrInvalidInput
	}
	if err := db.QueryRowContext(ctx, `SELECT backend_hash,state FROM fugue_platform_input_archives WHERE content_hash=$1 AND state IN ('verified','routed')`, digest).Scan(&backendHash, &state); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return report, fmt.Errorf("compiler input archive is not routed: %w", ErrConflict)
		}
		return report, err
	}
	var epoch int64
	if err := db.QueryRowContext(ctx, `SELECT epoch FROM fugue_platform_input_archive_control WHERE singleton`).Scan(&epoch); err != nil {
		return report, err
	}
	source, err := readInputManifest(ctx, db, digest)
	if err != nil {
		return report, err
	}
	archive, err := compilerInputArchiveManifest(ctx, db, source, backendHash, state)
	if err != nil {
		return report, err
	}
	if err = verifyCompilerInputArchiveSource(ctx, db, source, epoch); err != nil {
		return report, fmt.Errorf("compiler input archive legacy fallback is not verified: %w", err)
	}
	tx, err := beginInputArchiveTx(ctx, db)
	if err != nil {
		return report, err
	}
	defer tx.Rollback()
	retiring, err := compilerInputArchiveRetirementStarted(ctx, tx)
	if err != nil {
		return report, err
	}
	if retiring {
		return report, fmt.Errorf("compiler input chunks relation is retired: %w", ErrConflict)
	}
	currentEpoch, err := readInputArchiveEpoch(ctx, tx, epoch)
	if err != nil {
		return report, err
	}
	currentSource, err := readInputManifest(ctx, tx, digest)
	if err != nil || currentSource != source {
		if err != nil {
			return report, err
		}
		return report, fmt.Errorf("compiler input archive source changed before unroute: %w", ErrConflict)
	}
	var currentState string
	if err = tx.QueryRowContext(ctx, `SELECT state FROM fugue_platform_input_archives WHERE content_hash=$1`, digest).Scan(&currentState); err != nil {
		return report, err
	}
	if currentState != "verified" && currentState != "routed" {
		return report, fmt.Errorf("compiler input archive state changed before unroute: %w", ErrConflict)
	}
	currentArchive, err := compilerInputArchiveManifest(ctx, tx, currentSource, backendHash, currentState)
	if err != nil || !sameCompilerInputArchiveManifest(archive, currentArchive) {
		if err != nil {
			return report, err
		}
		return report, fmt.Errorf("compiler input archive receipt changed before unroute: %w", ErrConflict)
	}
	if currentState == "verified" {
		report.SourceFormat, report.RawBytes, report.SourceChunks = source.Format, source.Size, source.Count
		report.Segments, report.State, report.SourceEpoch = len(archive.Segments), "verified", epoch
		return report, nil
	}
	result, err := tx.ExecContext(ctx, `UPDATE fugue_platform_input_archives SET state='verified',routed_at=NULL,updated_at=now() WHERE content_hash=$1 AND backend_hash=$2 AND state='routed' AND verified_digest=content_hash AND source_epoch=$3`, digest, backendHash, currentEpoch)
	if err != nil {
		return report, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return report, err
	}
	if changed != 1 {
		return report, fmt.Errorf("compiler input archive unroute compare-and-swap failed: %w", ErrConflict)
	}
	if err = tx.Commit(); err != nil {
		return report, err
	}
	report.Applied, report.SourceFormat, report.RawBytes, report.SourceChunks = true, source.Format, source.Size, source.Count
	report.Segments, report.State, report.SourceEpoch = len(archive.Segments), "verified", epoch
	return report, nil
}

func verifiedArchiveSource(ctx context.Context, db *sql.DB, digest string) (inputManifest, int64, error) {
	var zero inputManifest
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return zero, 0, err
	}
	defer tx.Rollback()
	epoch, err := readInputArchiveEpoch(ctx, tx, -1)
	if err != nil {
		return zero, 0, err
	}
	source, err := readInputManifest(ctx, tx, digest)
	if err != nil {
		return zero, 0, err
	}
	if source.Format != inputFormatV1 && source.Format != inputFormatCodecV2 {
		return zero, 0, fmt.Errorf("compiler input archive source is not v1/v2: %w", ErrConflict)
	}
	if err = tx.Commit(); err != nil {
		return zero, 0, err
	}
	return source, epoch, nil
}

func verifyCompilerInputArchiveSource(ctx context.Context, db *sql.DB, source inputManifest, epoch int64) error {
	stream := &archiveInputStream{ctx: ctx, db: db, m: source, hash: sha256.New(), sourceEpoch: epoch}
	if _, err := io.Copy(io.Discard, stream); err != nil {
		return err
	}
	if stream.err != nil || !stream.done || stream.count != source.Count || stream.size != source.Size || "sha256:"+hex.EncodeToString(stream.hash.Sum(nil)) != source.Digest {
		return fmt.Errorf("compiler input archive source verification differs: %w", ErrConflict)
	}
	return nil
}

func beginInputArchiveTx(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	return beginInputArchiveLockedTx(ctx, db, false, false, true)
}

func beginInputArchiveClaimTx(ctx context.Context, db *sql.DB, apply bool) (*sql.Tx, error) {
	return beginInputArchiveLockedTx(ctx, db, !apply, !apply, false)
}

// Only acquisition of the unchanged writer barrier may be retried. An
// unsuccessful attempt is rolled back before yielding, so no transaction,
// row lock, or partial mutation survives the wait. Never retry a statement
// after acquisition, a commit, a lease conflict, or an unknown database error.
// A try-lock reports normal contention without generating a database ERROR.
// This preserves fencing, pin, epoch, compare-and-swap and the five-second bound.
func beginInputArchiveLockedTx(ctx context.Context, db *sql.DB, readOnly, shared, workMem bool) (*sql.Tx, error) {
	lock := `SELECT pg_try_advisory_xact_lock_shared(315609238744290)`
	if !shared {
		lock = `SELECT pg_try_advisory_xact_lock(315609238744290)`
	}
	deadline := time.Now().Add(5 * time.Second)
	delay := 100 * time.Millisecond
	for {
		tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: readOnly})
		if err != nil {
			return nil, err
		}
		settings := []string{`SET LOCAL lock_timeout='250ms'`, `SET LOCAL statement_timeout='5s'`}
		if workMem {
			settings = append(settings, `SET LOCAL work_mem='1MB'`)
		}
		for _, q := range settings {
			if _, err = tx.ExecContext(ctx, q); err != nil {
				_ = tx.Rollback()
				return nil, err
			}
		}
		var acquired bool
		err = tx.QueryRowContext(ctx, lock).Scan(&acquired)
		if err == nil && acquired {
			return tx, nil
		}
		if err == nil {
			err = compilerInputBarrierBusy()
		}
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return nil, err // rollback not confirmed; do not retry
		}
		var state interface{ SQLState() string }
		if !errors.As(err, &state) || state.SQLState() != "55P03" || !time.Now().Before(deadline) {
			return nil, err
		}
		timer := time.NewTimer(min(delay, time.Until(deadline)))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		delay = min(delay*2, 500*time.Millisecond)
	}
}

func readInputArchiveEpoch(ctx context.Context, tx *sql.Tx, epoch int64) (int64, error) {
	var current int64
	if err := tx.QueryRowContext(ctx, `SELECT epoch FROM fugue_platform_input_archive_control WHERE singleton`).Scan(&current); err != nil {
		return 0, err
	}
	if current < 0 || epoch >= 0 && current != epoch {
		return 0, fmt.Errorf("compiler input archive maintenance epoch differs: %w", ErrConflict)
	}
	return current, nil
}

// compilerInputWritesFenced is checked only after taking the common writer
// lock. Older databases without the additive archive ledger remain writable;
// a present but malformed control row fails closed.
func compilerInputWritesFenced(ctx context.Context, db inputQueryer) (bool, error) {
	var installed bool
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('fugue_platform_input_archive_control') IS NOT NULL`).Scan(&installed); err != nil {
		return false, err
	}
	if !installed {
		return false, nil
	}
	var fenced bool
	if err := db.QueryRowContext(ctx, `SELECT writes_fenced FROM fugue_platform_input_archive_control WHERE singleton`).Scan(&fenced); err != nil {
		return false, fmt.Errorf("compiler input write fence control unavailable: %w", ErrConflict)
	}
	return fenced, nil
}

func compilerInputArchiveRetirementStarted(ctx context.Context, db inputQueryer) (bool, error) {
	var installed bool
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('fugue_platform_input_archive_control') IS NOT NULL`).Scan(&installed); err != nil {
		return false, err
	}
	if !installed {
		return false, nil
	}
	var retired sql.NullTime
	if err := db.QueryRowContext(ctx, `SELECT retirement_started_at FROM fugue_platform_input_archive_control WHERE singleton`).Scan(&retired); err != nil {
		return false, fmt.Errorf("compiler input retirement state unavailable: %w", ErrConflict)
	}
	return retired.Valid, nil
}

func claimCompilerInputArchive(ctx context.Context, db *sql.DB, backendHash string, options CompilerInputArchiveMigrationOptions) (inputArchiveClaim, error) {
	var claim inputArchiveClaim
	owner, err := newArchiveWorkerID()
	if err != nil {
		return claim, err
	}
	claim.owner = owner
	tx, err := beginInputArchiveClaimTx(ctx, db, options.Apply)
	if err != nil {
		return claim, err
	}
	defer tx.Rollback()
	if claim.epoch, err = readInputArchiveEpoch(ctx, tx, -1); err != nil {
		return claim, err
	}
	retiring, err := compilerInputArchiveRetirementStarted(ctx, tx)
	if err != nil {
		return claim, err
	}
	if retiring {
		return claim, fmt.Errorf("compiler input chunks relation is retired: %w", ErrConflict)
	}
	if claim.source, err = readInputManifest(ctx, tx, options.Digest); err != nil {
		return claim, err
	}
	if claim.source.Format != inputFormatV1 && claim.source.Format != inputFormatCodecV2 {
		return claim, fmt.Errorf("archive source must use v1/v2 chunks: %w", ErrConflict)
	}
	if !options.Apply {
		return claim, nil
	}
	var archiveFormat, sourceFormat, prefix, backend, state, verifiedDigest string
	var verified bool
	var rawBytes, segmentBytes, segmentCount, sourceEpoch int64
	err = tx.QueryRowContext(ctx, `SELECT archive_format,source_format,raw_bytes,segment_bytes,segment_count,object_prefix,backend_hash,state,source_epoch,fencing_token,verified_digest,verified_at IS NOT NULL FROM fugue_platform_input_archives WHERE content_hash=$1 FOR UPDATE`, options.Digest).Scan(&archiveFormat, &sourceFormat, &rawBytes, &segmentBytes, &segmentCount, &prefix, &backend, &state, &sourceEpoch, &claim.token, &verifiedDigest, &verified)
	if errors.Is(err, sql.ErrNoRows) {
		archiveFormat, sourceFormat, rawBytes, segmentBytes = inputarchive.Format, claim.source.Format, claim.source.Size, inputarchive.DefaultSegmentBytes
		if options.SegmentBytes > 0 {
			segmentBytes = int64(options.SegmentBytes)
		}
		prefix, sourceEpoch, backend = "input/"+strings.TrimPrefix(options.Digest, "sha256:")+"/"+owner, claim.epoch, backendHash
		segmentCount = (rawBytes + segmentBytes - 1) / segmentBytes
		_, err = tx.ExecContext(ctx, `INSERT INTO fugue_platform_input_archives(content_hash,archive_format,source_format,raw_bytes,segment_bytes,segment_count,object_prefix,backend_hash,state,source_epoch,verified_digest,fencing_token,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'planned',$9,'',0,now(),now())`, options.Digest, archiveFormat, sourceFormat, rawBytes, segmentBytes, segmentCount, prefix, backendHash, sourceEpoch)
	}
	if err != nil {
		return claim, err
	}
	if state == "routed" || sourceFormat != claim.source.Format || rawBytes != claim.source.Size || backend != backendHash || sourceEpoch != claim.epoch || archiveFormat != inputarchive.Format || segmentBytes < 65536 || segmentBytes > inputarchive.MaxSegmentBytes || segmentCount != (rawBytes+segmentBytes-1)/segmentBytes {
		return claim, fmt.Errorf("compiler input archive claim differs from the source: %w", ErrConflict)
	}
	var active bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM fugue_platform_input_archive_leases WHERE content_hash=$1 AND expires_at>now())`, options.Digest).Scan(&active); err != nil {
		return claim, err
	}
	if active {
		return claim, fmt.Errorf("compiler input archive already has an active owner: %w", ErrConflict)
	}
	claim.manifest = inputarchive.Manifest{Format: archiveFormat, Digest: options.Digest, RawBytes: rawBytes, SegmentBytes: int(segmentBytes), Prefix: prefix}
	rows, err := tx.QueryContext(ctx, `SELECT ordinal,object_key,raw_bytes,stored_bytes,segment_hash,stored_hash FROM fugue_platform_input_archive_segments WHERE content_hash=$1 AND uploaded_at IS NOT NULL AND verified_at IS NOT NULL ORDER BY ordinal LIMIT $2`, options.Digest, segmentCount+1)
	if err != nil {
		return claim, err
	}
	for rows.Next() {
		var seg inputarchive.Segment
		if err = rows.Scan(&seg.Ordinal, &seg.Key, &seg.RawBytes, &seg.StoredBytes, &seg.RawHash, &seg.StoredHash); err != nil {
			break
		}
		claim.manifest.Segments = append(claim.manifest.Segments, seg)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return claim, err
	}
	claim.token++
	if _, err = tx.ExecContext(ctx, `INSERT INTO fugue_platform_input_archive_leases(content_hash,owner,source_epoch,fencing_token,expires_at,updated_at) VALUES($1,$2,$3,$4,now()+interval '2 minutes',now()) ON CONFLICT(content_hash) DO UPDATE SET owner=excluded.owner,source_epoch=excluded.source_epoch,fencing_token=excluded.fencing_token,expires_at=excluded.expires_at,updated_at=now()`, options.Digest, owner, claim.epoch, claim.token); err != nil {
		return claim, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE fugue_platform_input_archives SET state='uploading',verified_digest='',verified_at=NULL,fencing_token=$2,updated_at=now() WHERE content_hash=$1 AND fencing_token=$3`, options.Digest, claim.token, claim.token-1)
	if err != nil {
		return claim, err
	}
	if n, e := result.RowsAffected(); e != nil || n != 1 {
		return claim, fmt.Errorf("archive claim token changed: %w", ErrConflict)
	}
	return claim, tx.Commit()
}

func renewInputArchiveLease(ctx context.Context, tx *sql.Tx, c inputArchiveClaim) error {
	if _, err := readInputArchiveEpoch(ctx, tx, c.epoch); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE fugue_platform_input_archive_leases SET expires_at=now()+interval '2 minutes',updated_at=now() WHERE content_hash=$1 AND owner=$2 AND source_epoch=$3 AND fencing_token=$4 AND expires_at>now()`, c.source.Digest, c.owner, c.epoch, c.token)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("compiler input archive lease lost: %w", ErrConflict)
	}
	var state string
	if err = tx.QueryRowContext(ctx, `SELECT state FROM fugue_platform_input_archives WHERE content_hash=$1 AND source_epoch=$2 AND fencing_token=$3 FOR UPDATE`, c.source.Digest, c.epoch, c.token).Scan(&state); err != nil {
		return err
	}
	if state != "uploading" {
		return ErrConflict
	}
	return nil
}

func checkpointCompilerInputArchive(ctx context.Context, db *sql.DB, c inputArchiveClaim, seg inputarchive.Segment) error {
	tx, err := beginInputArchiveTx(ctx, db)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = renewInputArchiveLease(ctx, tx, c); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO fugue_platform_input_archive_segments(content_hash,ordinal,object_key,raw_bytes,stored_bytes,segment_hash,stored_hash,uploaded_at,verified_at) VALUES($1,$2,$3,$4,$5,$6,$7,now(),now()) ON CONFLICT(content_hash,ordinal) DO NOTHING`, c.source.Digest, seg.Ordinal, seg.Key, seg.RawBytes, seg.StoredBytes, seg.RawHash, seg.StoredHash)
	if err != nil {
		return err
	}
	var existing inputarchive.Segment
	if err = tx.QueryRowContext(ctx, `SELECT ordinal,object_key,raw_bytes,stored_bytes,segment_hash,stored_hash FROM fugue_platform_input_archive_segments WHERE content_hash=$1 AND ordinal=$2 AND uploaded_at IS NOT NULL AND verified_at IS NOT NULL`, c.source.Digest, seg.Ordinal).Scan(&existing.Ordinal, &existing.Key, &existing.RawBytes, &existing.StoredBytes, &existing.RawHash, &existing.StoredHash); err != nil {
		return err
	}
	if existing != seg {
		return fmt.Errorf("compiler input archive segment receipt changed: %w", ErrConflict)
	}
	return tx.Commit()
}

func completeCompilerInputArchive(ctx context.Context, db *sql.DB, c inputArchiveClaim, manifest inputarchive.Manifest) error {
	if err := manifest.Validate(); err != nil {
		return err
	}
	tx, err := beginInputArchiveTx(ctx, db)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = renewInputArchiveLease(ctx, tx, c); err != nil {
		return err
	}
	m, err := readInputManifest(ctx, tx, c.source.Digest)
	if err != nil {
		return err
	}
	if m != c.source {
		return fmt.Errorf("compiler input archive source manifest changed: %w", ErrConflict)
	}
	var count int
	var size int64
	if err = tx.QueryRowContext(ctx, `SELECT count(*),COALESCE(sum(raw_bytes),0) FROM fugue_platform_input_archive_segments WHERE content_hash=$1 AND uploaded_at IS NOT NULL AND verified_at IS NOT NULL`, m.Digest).Scan(&count, &size); err != nil {
		return err
	}
	if count != len(manifest.Segments) || size != m.Size {
		return ErrConflict
	}
	result, err := tx.ExecContext(ctx, `UPDATE fugue_platform_input_archives SET state='verified',verified_digest=$1,verified_at=now(),updated_at=now() WHERE content_hash=$1 AND source_epoch=$2 AND fencing_token=$3 AND state='uploading'`, m.Digest, c.epoch, c.token)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil || n != 1 {
		return ErrConflict
	}
	if _, err = tx.ExecContext(ctx, `UPDATE fugue_platform_input_archive_leases SET expires_at=now(),updated_at=now() WHERE content_hash=$1 AND owner=$2 AND fencing_token=$3`, m.Digest, c.owner, c.token); err != nil {
		return err
	}
	return tx.Commit()
}
