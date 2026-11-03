package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"fugue/internal/inputarchive"
)

type CompilerInputArchiveMigrationOptions struct {
	Digest string
	Apply  bool
}

type CompilerInputArchiveMigrationReport struct {
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
	report := CompilerInputArchiveMigrationReport{Digest: options.Digest}
	if !inputDigestPattern.MatchString(options.Digest) {
		return report, ErrInvalidInput
	}
	var objects inputarchive.Objects
	var backendHash string
	if options.Apply {
		cfg, configured, err := inputarchive.ConfigFromEnv()
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
	db, err := openPostgresDatabase(databaseURL)
	if err != nil {
		return report, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return copyCompilerInputArchive(ctx, db, objects, backendHash, options)
}

func copyCompilerInputArchive(ctx context.Context, db *sql.DB, objects inputarchive.Objects, backendHash string, options CompilerInputArchiveMigrationOptions) (CompilerInputArchiveMigrationReport, error) {
	report := CompilerInputArchiveMigrationReport{Digest: options.Digest}
	if !inputDigestPattern.MatchString(options.Digest) || options.Apply && (!inputDigestPattern.MatchString(backendHash) || objects == nil) {
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
	source := &archiveInputStream{ctx: ctx, db: db, m: claim.source, hash: sha256.New(), sourceEpoch: claim.epoch}
	got, err := inputarchive.Copy(ctx, objects, source, claim.manifest, func(seg inputarchive.Segment) error {
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

func beginInputArchiveTx(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	for _, q := range []string{`SET LOCAL lock_timeout='250ms'`, `SET LOCAL statement_timeout='5s'`, `SET LOCAL work_mem='1MB'`, `SELECT pg_advisory_xact_lock(315609238744290)`} {
		if _, err = tx.ExecContext(ctx, q); err != nil {
			tx.Rollback()
			return nil, err
		}
	}
	return tx, nil
}

func beginInputArchiveClaimTx(ctx context.Context, db *sql.DB, apply bool) (*sql.Tx, error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: !apply})
	if err != nil {
		return nil, err
	}
	lock := `SELECT pg_advisory_xact_lock_shared(315609238744290)`
	if apply {
		lock = `SELECT pg_advisory_xact_lock(315609238744290)`
	}
	for _, q := range []string{`SET LOCAL lock_timeout='250ms'`, `SET LOCAL statement_timeout='5s'`, lock} {
		if _, err = tx.ExecContext(ctx, q); err != nil {
			tx.Rollback()
			return nil, err
		}
	}
	return tx, nil
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
	if claim.source, err = readInputManifest(ctx, tx, options.Digest); err != nil {
		return claim, err
	}
	if claim.source.Format != inputFormatV1 && claim.source.Format != inputFormatCodecV2 {
		return claim, fmt.Errorf("archive source must use v1/v2 chunks: %w", ErrConflict)
	}
	if !options.Apply {
		return claim, nil
	}
	var archiveFormat, sourceFormat, prefix, backend, state string
	var rawBytes, segmentBytes, segmentCount, sourceEpoch int64
	err = tx.QueryRowContext(ctx, `SELECT archive_format,source_format,raw_bytes,segment_bytes,segment_count,object_prefix,backend_hash,state,source_epoch,fencing_token FROM fugue_platform_input_archives WHERE content_hash=$1 FOR UPDATE`, options.Digest).Scan(&archiveFormat, &sourceFormat, &rawBytes, &segmentBytes, &segmentCount, &prefix, &backend, &state, &sourceEpoch, &claim.token)
	if errors.Is(err, sql.ErrNoRows) {
		archiveFormat, sourceFormat, rawBytes, segmentBytes = inputarchive.Format, claim.source.Format, claim.source.Size, inputarchive.DefaultSegmentBytes
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
	claim.token++
	if _, err = tx.ExecContext(ctx, `INSERT INTO fugue_platform_input_archive_leases(content_hash,owner,source_epoch,fencing_token,expires_at,updated_at) VALUES($1,$2,$3,$4,now()+interval '2 minutes',now()) ON CONFLICT(content_hash) DO UPDATE SET owner=excluded.owner,source_epoch=excluded.source_epoch,fencing_token=excluded.fencing_token,expires_at=excluded.expires_at,updated_at=now()`, options.Digest, owner, claim.epoch, claim.token); err != nil {
		return claim, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE fugue_platform_input_archives SET state='uploading',verified_digest='',verified_at=NULL,fencing_token=$2,updated_at=now() WHERE content_hash=$1`, options.Digest, claim.token); err != nil {
		return claim, err
	}
	claim.manifest = inputarchive.Manifest{Format: archiveFormat, Digest: options.Digest, RawBytes: rawBytes, SegmentBytes: int(segmentBytes), Prefix: prefix}
	rows, err := tx.QueryContext(ctx, `SELECT ordinal,object_key,raw_bytes,stored_bytes,segment_hash,stored_hash FROM fugue_platform_input_archive_segments WHERE content_hash=$1 AND uploaded_at IS NOT NULL AND verified_at IS NOT NULL ORDER BY ordinal`, options.Digest)
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
