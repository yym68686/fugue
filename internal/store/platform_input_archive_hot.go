package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"io"
	"time"

	"fugue/internal/inputarchive"
)

type CompilerInputArchiveHotOptions struct {
	Digest         string
	Apply          bool
	MaxStoredBytes int64
}

type CompilerInputArchiveHotReport struct {
	Action      string `json:"action"`
	Digest      string `json:"digest"`
	Applied     bool   `json:"applied"`
	State       string `json:"state"`
	SourceEpoch int64  `json:"source_epoch"`
	Format      string `json:"format"`
	RawBytes    int64  `json:"raw_bytes"`
	StoredBytes int64  `json:"stored_bytes"`
	Chunks      int    `json:"chunks"`
	// Archive restoration cannot know the recompressed local size in a
	// metadata-only preview. Keep its format-derived ceiling separate.
	WorstCaseStoredBytes int64 `json:"worst_case_stored_bytes,omitempty"`
}

// RunCompilerInputArchiveHotCopy promotes one archived v1/v2 input to an
// independent local hot copy. Preview is metadata-only. Apply preserves the
// source chunks and archive receipts, and switches only storage_location after
// reading the entire hot copy back through the original decoder and digest.
// Operators must gate disk/WAL headroom and current/rollback reader compatibility
// before apply. A failed copy may leave bounded staging rows for an exact retry;
// it never publishes a partial hot copy or overwrites conflicting stored bytes.
func RunCompilerInputArchiveHotCopy(ctx context.Context, databaseURL string, options CompilerInputArchiveHotOptions) (CompilerInputArchiveHotReport, error) {
	return runCompilerInputArchiveHot(ctx, databaseURL, options, false)
}

// RunCompilerInputArchiveHotRestore rebuilds one local v1/v2 representation
// from a routed archive, including after legacy-table retirement. It never
// changes archive receipts or requires the retired relation's bytes.
func RunCompilerInputArchiveHotRestore(ctx context.Context, databaseURL string, options CompilerInputArchiveHotOptions) (CompilerInputArchiveHotReport, error) {
	return runCompilerInputArchiveHot(ctx, databaseURL, options, true)
}

func runCompilerInputArchiveHot(ctx context.Context, databaseURL string, options CompilerInputArchiveHotOptions, restore bool) (CompilerInputArchiveHotReport, error) {
	if options.Apply {
		enabled, err := inputarchive.MigrationEnabledFromEnv()
		if err != nil || !enabled {
			return CompilerInputArchiveHotReport{}, fmt.Errorf("compiler input hot copy requires migration opt-in: %w", ErrConflict)
		}
	}
	db, err := openPostgresDatabase(databaseURL)
	if err != nil {
		return CompilerInputArchiveHotReport{}, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	var objects inputarchive.Objects
	var backendHash string
	if options.Apply || restore {
		cfg, configured, err := resolveCompilerInputArchiveConfig(ctx, db, false)
		if err != nil || !configured {
			return CompilerInputArchiveHotReport{}, fmt.Errorf("dedicated compiler input archive backend is required: %w", ErrConflict)
		}
		objects, err = inputarchive.NewS3(cfg)
		if err != nil {
			return CompilerInputArchiveHotReport{}, err
		}
		backendHash, err = cfg.Fingerprint()
		if err != nil {
			return CompilerInputArchiveHotReport{}, err
		}
	}
	if restore {
		return restoreCompilerInputArchiveHot(ctx, db, objects, backendHash, options)
	}
	return copyCompilerInputArchiveHot(ctx, db, objects, backendHash, options)
}

func copyCompilerInputArchiveHot(ctx context.Context, db *sql.DB, objects inputarchive.Objects, backendHash string, options CompilerInputArchiveHotOptions) (CompilerInputArchiveHotReport, error) {
	report := CompilerInputArchiveHotReport{Action: "archive-hot-copy", Digest: options.Digest}
	if !inputDigestPattern.MatchString(options.Digest) || options.MaxStoredBytes < 1 || options.MaxStoredBytes > 2*inputMaxBytes || options.Apply && (!inputDigestPattern.MatchString(backendHash) || objects == nil) {
		return report, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	m, epoch, err := verifiedArchiveSource(ctx, db, options.Digest)
	if err != nil {
		return report, err
	}
	report.Format, report.RawBytes, report.Chunks, report.SourceEpoch = m.Format, m.Size, m.Count, epoch
	if m.Location == "hot" {
		if err = verifyCompilerInputArchiveSource(ctx, db, m, epoch); err != nil {
			return report, err
		}
		report.State = "hot-verified"
		return report, nil
	}
	if options.Apply {
		if err = verifyCompilerInputHotArchive(ctx, db, objects, m, backendHash); err != nil {
			return report, err
		}
	}
	tx, err := beginInputArchiveLockedTx(ctx, db, true, true, false)
	if err != nil {
		return report, err
	}
	defer tx.Rollback()
	if err = checkCompilerInputHotSource(ctx, tx, m, epoch); err != nil {
		return report, err
	}
	table, _ := compilerInputChunksTable(m.Location)
	var count int
	if err = tx.QueryRowContext(ctx, `SELECT count(*),COALESCE(sum(octet_length(data)),0) FROM `+table+` WHERE content_hash=$1`, m.Digest).Scan(&count, &report.StoredBytes); err != nil {
		return report, err
	}
	if count != m.Count || report.StoredBytes < 1 || options.MaxStoredBytes > 0 && report.StoredBytes > options.MaxStoredBytes {
		return report, fmt.Errorf("compiler input hot copy exceeds its declared bound or source coverage: %w", ErrConflict)
	}
	if err = tx.Commit(); err != nil {
		return report, err
	}
	report.State = "source-metadata-observed"
	if !options.Apply {
		return report, nil
	}
	var copiedBytes int64
	for first := 0; first < m.Count; first += inputChunkBatchSize {
		last := min(first+inputChunkBatchSize, m.Count)
		n, err := copyCompilerInputHotPage(ctx, db, m, epoch, first, last, options.MaxStoredBytes-copiedBytes)
		if err != nil {
			return report, err
		}
		copiedBytes += n
	}
	if copiedBytes != report.StoredBytes {
		return report, fmt.Errorf("compiler input hot copy source size changed: %w", ErrConflict)
	}
	// The staging table is not a reader route. Compare its decoded full stream
	// with the original identity before a short, fenced manifest CAS.
	stream := &archiveInputStream{ctx: ctx, db: db, m: m, readLocation: "hot", hash: sha256.New(), sourceEpoch: epoch}
	if _, err = io.Copy(io.Discard, stream); err != nil || !stream.done || stream.err != nil {
		if err != nil {
			return report, err
		}
		return report, ErrConflict
	}
	tx, err = beginInputArchiveTx(ctx, db)
	if err != nil {
		return report, err
	}
	defer tx.Rollback()
	if err = checkCompilerInputHotSource(ctx, tx, m, epoch); err != nil {
		return report, err
	}
	// No supported writer can change staging bytes: promotions only insert and
	// compare; normal writers use the canonical location; the archive pins
	// migration/GC. The CAS makes the complete local copy visible atomically.
	result, err := tx.ExecContext(ctx, `UPDATE fugue_platform_input_manifests SET storage_location='hot' WHERE content_hash=$1 AND storage_location='legacy' AND format=$2 AND size_bytes=$3 AND chunk_size=$4 AND chunk_count=$5 AND created_at=$6`, m.Digest, m.Format, m.Size, m.ChunkSize, m.Count, m.Created)
	if err != nil {
		return report, err
	}
	if n, err := result.RowsAffected(); err != nil || n != 1 {
		return report, fmt.Errorf("compiler input hot location compare-and-swap failed: %w", ErrConflict)
	}
	if err = tx.Commit(); err != nil {
		return report, err
	}
	report.Applied, report.State = true, "hot-verified"
	return report, nil
}

func verifyCompilerInputHotArchive(ctx context.Context, db inputQueryer, objects inputarchive.Objects, source inputManifest, backendHash string) error {
	if objects == nil || !inputDigestPattern.MatchString(backendHash) {
		return fmt.Errorf("compiler input hot copy has no verifiable archive backend: %w", ErrConflict)
	}
	var state string
	if err := db.QueryRowContext(ctx, `SELECT state FROM fugue_platform_input_archives WHERE content_hash=$1 AND verified_digest=content_hash AND verified_at IS NOT NULL`, source.Digest).Scan(&state); err != nil {
		return fmt.Errorf("compiler input hot copy lacks a verified external pin: %w", ErrConflict)
	}
	if state != "verified" && state != "routed" {
		return fmt.Errorf("compiler input hot copy archive is not verified: %w", ErrConflict)
	}
	manifest, err := compilerInputArchiveManifest(ctx, db, source, backendHash, state)
	if err != nil {
		return err
	}
	if err = inputarchive.Verify(ctx, objects, manifest); err != nil {
		return fmt.Errorf("compiler input hot copy external archive verification failed: %w", err)
	}
	return nil
}

func checkCompilerInputHotSource(ctx context.Context, tx *sql.Tx, expected inputManifest, epoch int64) error {
	if _, err := readInputArchiveEpoch(ctx, tx, epoch); err != nil {
		return err
	}
	retired, err := compilerInputArchiveRetirementStarted(ctx, tx)
	if err != nil || retired {
		return fmt.Errorf("compiler input hot promotion requires an unretired relation: %w", ErrConflict)
	}
	current, err := readInputManifest(ctx, tx, expected.Digest)
	if err != nil {
		return err
	}
	if current != expected {
		return fmt.Errorf("compiler input hot source manifest changed: %w", ErrConflict)
	}
	var pinned bool
	err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM fugue_platform_input_archives a WHERE a.content_hash=$1 AND a.state IN ('verified','routed') AND a.verified_digest=a.content_hash AND a.verified_at IS NOT NULL AND a.source_format=$2 AND a.raw_bytes=$3) AND NOT EXISTS(SELECT 1 FROM fugue_platform_input_archive_leases WHERE content_hash=$1 AND expires_at>now())`, expected.Digest, expected.Format, expected.Size).Scan(&pinned)
	if err != nil {
		return err
	}
	if !pinned {
		return fmt.Errorf("compiler input hot source lacks an inactive verified archive pin: %w", ErrConflict)
	}
	return nil
}

func copyCompilerInputHotPage(ctx context.Context, db *sql.DB, m inputManifest, epoch int64, first, last int, remaining int64) (int64, error) {
	tx, err := beginInputArchiveTx(ctx, db)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if err = checkCompilerInputHotSource(ctx, tx, m, epoch); err != nil {
		return 0, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT ordinal,chunk_hash,sha256(data),octet_length(data) FROM fugue_platform_input_chunks WHERE content_hash=$1 AND ordinal >= $2 AND ordinal < $3 ORDER BY ordinal`, m.Digest, first, last)
	if err != nil {
		return 0, err
	}
	type witness struct {
		hash string
		size int
	}
	witnesses := make([]witness, 0, inputChunkBatchSize)
	var storedBytes int64
	for rows.Next() {
		var ordinal int
		var declared string
		var actual []byte
		var size int
		if err = rows.Scan(&ordinal, &declared, &actual, &size); err != nil {
			break
		}
		if ordinal != first+len(witnesses) || declared != sharedHashBytes(actual) {
			err = fmt.Errorf("compiler input hot source stored hash differs: %w", ErrConflict)
			break
		}
		storedBytes += int64(size)
		witnesses = append(witnesses, witness{hash: declared, size: size})
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return 0, err
	}
	if len(witnesses) != last-first || storedBytes > remaining {
		return 0, fmt.Errorf("compiler input hot source page coverage/budget differs: %w", ErrConflict)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO fugue_platform_input_hot_chunks(content_hash,ordinal,chunk_hash,data) SELECT content_hash,ordinal,chunk_hash,data FROM fugue_platform_input_chunks WHERE content_hash=$1 AND ordinal >= $2 AND ordinal < $3 ON CONFLICT(content_hash,ordinal) DO NOTHING`, m.Digest, first, last); err != nil {
		return 0, err
	}
	rows, err = tx.QueryContext(ctx, `SELECT ordinal,chunk_hash,sha256(data),octet_length(data) FROM fugue_platform_input_hot_chunks WHERE content_hash=$1 AND ordinal >= $2 AND ordinal < $3 ORDER BY ordinal`, m.Digest, first, last)
	if err != nil {
		return 0, err
	}
	count := 0
	for rows.Next() {
		var ordinal, size int
		var declared string
		var actual []byte
		if err = rows.Scan(&ordinal, &declared, &actual, &size); err != nil {
			break
		}
		if count >= len(witnesses) || ordinal != first+count || declared != witnesses[count].hash || sharedHashBytes(actual) != witnesses[count].hash || size != witnesses[count].size {
			err = fmt.Errorf("compiler input hot staging page differs; conflicting bytes retained: %w", ErrConflict)
			break
		}
		count++
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return 0, err
	}
	if count != len(witnesses) {
		return 0, ErrConflict
	}
	return storedBytes, tx.Commit()
}
