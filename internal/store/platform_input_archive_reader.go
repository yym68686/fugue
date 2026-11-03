package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"fugue/internal/inputarchive"
)

func (s *Store) ConfigureCompilerInputArchiveFromEnv() error {
	cfg, configured, err := inputarchive.ConfigFromEnv()
	if err != nil {
		return fmt.Errorf("compiler input archive configuration: %w", err)
	}
	backendHash := ""
	var objects inputarchive.Objects
	if configured {
		objects, err = inputarchive.NewS3(cfg)
		if err != nil {
			return err
		}
		backendHash, err = cfg.Fingerprint()
		if err != nil {
			return err
		}
	}
	if s.db != nil {
		if err = s.verifyCompilerInputArchiveStartupCompatibility(context.Background(), configured, backendHash); err != nil {
			return err
		}
	}
	s.compilerInputArchiveObjects = objects
	s.compilerInputArchiveReadLeaseEnabled = configured
	return nil
}

func (s *Store) verifyCompilerInputArchiveStartupCompatibility(ctx context.Context, configured bool, backendHash string) error {
	retired, err := compilerInputArchiveRetirementStarted(ctx, s.db)
	if err != nil {
		return fmt.Errorf("compiler input archive retirement state unavailable: %w", err)
	}
	if !retired {
		return nil
	}
	if !configured {
		return fmt.Errorf("compiler input chunks relation is retired; archive backend is required: %w", ErrConflict)
	}
	guardsInstalled, err := compilerInputArchiveRetirementGuardsInstalled(ctx, s.db)
	if err != nil || !guardsInstalled {
		return fmt.Errorf("compiler input archive retirement guards are unavailable: %w", ErrConflict)
	}
	var mismatch bool
	if err = s.db.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM fugue_platform_input_archives a
		JOIN fugue_platform_input_manifests m ON m.content_hash=a.content_hash
		WHERE a.state='routed' AND (
			a.backend_hash<>$1 OR a.verified_digest<>a.content_hash OR a.verified_at IS NULL OR a.routed_at IS NULL OR
			a.source_format<>m.format OR a.raw_bytes<>m.size_bytes OR a.segment_count<>((a.raw_bytes+a.segment_bytes-1)/a.segment_bytes) OR
			a.segment_count<>(SELECT count(*) FROM fugue_platform_input_archive_segments s WHERE s.content_hash=a.content_hash) OR
			EXISTS(SELECT 1 FROM fugue_platform_input_archive_segments s WHERE s.content_hash=a.content_hash AND (s.uploaded_at IS NULL OR s.verified_at IS NULL)) OR
			(SELECT min(ordinal) FROM fugue_platform_input_archive_segments s WHERE s.content_hash=a.content_hash)<>0 OR
			(SELECT max(ordinal) FROM fugue_platform_input_archive_segments s WHERE s.content_hash=a.content_hash)<>a.segment_count-1 OR
			(SELECT COALESCE(sum(raw_bytes),0) FROM fugue_platform_input_archive_segments s WHERE s.content_hash=a.content_hash)<>a.raw_bytes
		)
	)`, backendHash).Scan(&mismatch); err != nil {
		return fmt.Errorf("compiler input archive backend binding unavailable after retirement: %w", ErrConflict)
	}
	if mismatch {
		return fmt.Errorf("compiler input archive backend differs from routed data after retirement: %w", ErrConflict)
	}
	return nil
}

// openArchiveInput refuses fallback when a committed external route is absent,
// mismatched, or not fully receipt-validated.
func (s *Store) openArchiveInput(ctx context.Context, db inputQueryer, m inputManifest) (*inputarchive.Reader, error) {
	if s.compilerInputArchiveObjects == nil {
		return nil, fmt.Errorf("compiler input archive backend unavailable: %w", ErrConflict)
	}
	cfg, configured, err := inputarchive.ConfigFromEnv()
	if err != nil || !configured {
		return nil, fmt.Errorf("compiler input archive backend unavailable: %w", ErrConflict)
	}
	fingerprint, err := cfg.Fingerprint()
	if err != nil {
		return nil, err
	}
	manifest, err := compilerInputArchiveManifest(ctx, db, m, fingerprint, "routed")
	if err != nil {
		return nil, err
	}
	return inputarchive.Open(ctx, s.compilerInputArchiveObjects, manifest)
}

func compilerInputArchiveManifest(ctx context.Context, db inputQueryer, m inputManifest, fingerprint, expectedState string) (inputarchive.Manifest, error) {
	var format, sourceFormat, prefix, backendHash, state, verifiedDigest string
	var rawBytes, segmentBytes, segmentCount, sourceEpoch, fencingToken int64
	var verifiedAt, routedAt sql.NullTime
	if err := db.QueryRowContext(ctx, `SELECT archive_format,source_format,raw_bytes,segment_bytes,segment_count,object_prefix,backend_hash,state,verified_digest,source_epoch,fencing_token,verified_at,routed_at FROM fugue_platform_input_archives WHERE content_hash=$1`, m.Digest).Scan(&format, &sourceFormat, &rawBytes, &segmentBytes, &segmentCount, &prefix, &backendHash, &state, &verifiedDigest, &sourceEpoch, &fencingToken, &verifiedAt, &routedAt); err != nil {
		return inputarchive.Manifest{}, err
	}
	routeTimestampValid := expectedState != "routed" || routedAt.Valid
	if format != inputarchive.Format || sourceFormat != m.Format || rawBytes != m.Size || segmentCount < 1 || state != expectedState || verifiedDigest != m.Digest || !verifiedAt.Valid || !routeTimestampValid || sourceEpoch < 0 || fencingToken < 0 || backendHash != fingerprint {
		return inputarchive.Manifest{}, fmt.Errorf("compiler input archive route/verification receipt mismatch: %w", ErrConflict)
	}
	manifest := inputarchive.Manifest{Format: format, Digest: m.Digest, RawBytes: rawBytes, SegmentBytes: int(segmentBytes), Prefix: prefix}
	rows, err := db.QueryContext(ctx, `SELECT ordinal,object_key,raw_bytes,stored_bytes,segment_hash,stored_hash,uploaded_at IS NOT NULL,verified_at IS NOT NULL FROM fugue_platform_input_archive_segments WHERE content_hash=$1 ORDER BY ordinal`, m.Digest)
	if err != nil {
		return inputarchive.Manifest{}, err
	}
	for rows.Next() {
		var seg inputarchive.Segment
		var uploaded, verified bool
		if err = rows.Scan(&seg.Ordinal, &seg.Key, &seg.RawBytes, &seg.StoredBytes, &seg.RawHash, &seg.StoredHash, &uploaded, &verified); err != nil {
			break
		}
		if !uploaded || !verified {
			err = fmt.Errorf("compiler input archive segment lacks durable verification receipt: %w", ErrConflict)
			break
		}
		manifest.Segments = append(manifest.Segments, seg)
	}
	if err == nil {
		err = rows.Err()
	}
	if closeErr := rows.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return inputarchive.Manifest{}, err
	}
	if segmentBytes < 1 || segmentBytes > int64(inputarchive.MaxSegmentBytes) {
		return inputarchive.Manifest{}, fmt.Errorf("compiler input archive segment size invalid: %w", ErrConflict)
	}
	wantSegments := (rawBytes + segmentBytes - 1) / segmentBytes
	if int64(len(manifest.Segments)) != segmentCount || segmentCount != wantSegments {
		return inputarchive.Manifest{}, fmt.Errorf("compiler input archive segment set incomplete: %w", ErrConflict)
	}
	if err := manifest.Validate(); err != nil {
		return inputarchive.Manifest{}, err
	}
	return manifest, nil
}

func (s *Store) compilerInputArchiveRouted(ctx context.Context, db inputQueryer, m inputManifest) (bool, error) {
	if !s.compilerInputArchiveReadLeaseEnabled {
		return false, nil
	}
	if m.Format != inputFormatV1 && m.Format != inputFormatCodecV2 {
		return false, nil
	}
	var installed bool
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('fugue_platform_input_archives') IS NOT NULL`).Scan(&installed); err != nil || !installed {
		return false, err
	}
	var state string
	err := db.QueryRowContext(ctx, `SELECT state FROM fugue_platform_input_archives WHERE content_hash=$1`, m.Digest).Scan(&state)
	if absentInputManifest(err) || errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return state == "routed", nil
}

func (s *Store) openInputChunks(ctx context.Context, db inputQueryer, m inputManifest) (*inputChunkReader, error) {
	routed, err := s.compilerInputArchiveRouted(ctx, db, m)
	if err != nil {
		return nil, err
	}
	if routed {
		r, err := s.openArchiveInput(ctx, db, m)
		if err != nil {
			return nil, err
		}
		return &inputChunkReader{archive: r}, nil
	}
	return openInputChunks(ctx, db, m)
}

const compilerInputArchiveReadLeaseLockID int64 = 315609238744298

// BeginCompilerInputReadLease acquires a session advisory lock before the
// Repeatable Read snapshot begins. Transaction advisory locks are insufficient:
// a reader blocked behind the retirement fence would otherwise retain a
// pre-TRUNCATE snapshot, and PostgreSQL documents TRUNCATE as non-MVCC-safe.
func (s *Store) BeginCompilerInputReadLease(ctx context.Context) (*sql.Tx, func(), error) {
	if !s.compilerInputArchiveReadLeaseEnabled {
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
		if err != nil {
			return nil, nil, err
		}
		return tx, func() { _ = tx.Rollback() }, nil
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, nil, err
	}
	for {
		var acquired bool
		if err = conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock_shared($1)`, compilerInputArchiveReadLeaseLockID).Scan(&acquired); err != nil {
			conn.Close()
			return nil, nil, err
		}
		if acquired {
			break
		}
		select {
		case <-ctx.Done():
			conn.Close()
			return nil, nil, ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		_, _ = conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock_shared($1)`, compilerInputArchiveReadLeaseLockID)
		conn.Close()
		return nil, nil, err
	}
	var once sync.Once
	finish := func() {
		once.Do(func() {
			_ = tx.Rollback()
			var unlocked bool
			unlockErr := conn.QueryRowContext(context.Background(), `SELECT pg_advisory_unlock_shared($1)`, compilerInputArchiveReadLeaseLockID).Scan(&unlocked)
			if unlockErr != nil || !unlocked {
				_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			}
			_ = conn.Close()
		})
	}
	return tx, finish, nil
}

func verifyArchiveReader(r *inputarchive.Reader, expected string, expectedBytes int64) error {
	if r == nil {
		return ErrConflict
	}
	h := sha256.New()
	n, err := io.Copy(h, r)
	if err != nil {
		return err
	}
	if !r.Verified() || n != expectedBytes || "sha256:"+hex.EncodeToString(h.Sum(nil)) != expected {
		return fmt.Errorf("compiler input archive readback differs: %w", ErrConflict)
	}
	return nil
}
