package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"io"
	"strings"
	"time"

	"fugue/internal/inputarchive"
)

// Restoring an immutable archive does not acquire a writable archive lease:
// after retirement all archive ledgers must remain immutable. Instead every
// bounded transaction checks the maintenance epoch, canonical manifest, exact
// routed receipt/segment set and absence of an active archive owner under the
// common writer barrier. The archive remains the canonical route until full
// local readback succeeds. No network request runs inside a database transaction.
func restoreCompilerInputArchiveHot(ctx context.Context, db *sql.DB, objects inputarchive.Objects, backendHash string, options CompilerInputArchiveHotOptions) (CompilerInputArchiveHotReport, error) {
	report := CompilerInputArchiveHotReport{Action: "archive-hot-restore", Digest: options.Digest}
	if !inputDigestPattern.MatchString(options.Digest) || options.MaxStoredBytes < 1 || options.MaxStoredBytes > 2*inputMaxBytes || !inputDigestPattern.MatchString(backendHash) || options.Apply && objects == nil {
		return report, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	m, epoch, err := verifiedArchiveSource(ctx, db, options.Digest)
	if err != nil {
		return report, err
	}
	report.Format, report.RawBytes, report.Chunks, report.SourceEpoch = m.Format, m.Size, m.Count, epoch
	report.WorstCaseStoredBytes = m.Size
	if m.Format == inputFormatCodecV2 {
		report.WorstCaseStoredBytes += int64(m.Count * inputCodecHeaderBytes)
	}
	if m.Location == "hot" {
		if err = verifyCompilerInputArchiveSource(ctx, db, m, epoch); err != nil {
			return report, err
		}
		report.State = "hot-verified"
		return report, nil
	}
	tx, err := beginInputArchiveLockedTx(ctx, db, true, true, false)
	if err != nil {
		return report, err
	}
	defer tx.Rollback()
	archive, err := compilerInputArchiveManifest(ctx, tx, m, backendHash, "routed")
	if err != nil {
		return report, err
	}
	if err = checkCompilerInputRestoreSource(ctx, tx, m, epoch, backendHash, archive); err != nil {
		return report, err
	}
	if err = tx.Commit(); err != nil {
		return report, err
	}
	report.State = "archive-metadata-observed"
	if !options.Apply {
		return report, nil
	}
	r, err := inputarchive.Open(ctx, objects, archive)
	if err != nil {
		return report, err
	}
	defer r.Close()
	encoder := inputChunkEncoder{}
	for first := 0; first < m.Count; first += inputChunkBatchSize {
		page := make([][]byte, 0, min(inputChunkBatchSize, m.Count-first))
		for ordinal := first; ordinal < min(first+inputChunkBatchSize, m.Count); ordinal++ {
			want := min(int64(m.ChunkSize), m.Size-int64(ordinal)*int64(m.ChunkSize))
			raw := make([]byte, int(want))
			if _, err = io.ReadFull(r, raw); err != nil {
				return report, err
			}
			stored := raw
			if m.Format == inputFormatCodecV2 {
				stored, err = encoder.encode(raw)
				if err != nil {
					return report, err
				}
			}
			page = append(page, bytes.Clone(stored))
		}
		used, err := restoreCompilerInputHotPage(ctx, db, m, epoch, backendHash, archive, first, page, options.MaxStoredBytes-report.StoredBytes)
		if err != nil {
			return report, err
		}
		report.StoredBytes += used
	}
	// The final EOF validates the original whole-input digest, not just the
	// independently checked archive segments. A failure never publishes staging.
	var extra [1]byte
	if n, err := r.Read(extra[:]); n != 0 || err != io.EOF || !r.Verified() {
		return report, fmt.Errorf("compiler input restore archive digest/length differs: %w", ErrConflict)
	}
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
	if err = checkCompilerInputRestoreSource(ctx, tx, m, epoch, backendHash, archive); err != nil {
		return report, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE fugue_platform_input_manifests SET storage_location='hot' WHERE content_hash=$1 AND storage_location='legacy' AND format=$2 AND size_bytes=$3 AND chunk_size=$4 AND chunk_count=$5 AND created_at=$6`, m.Digest, m.Format, m.Size, m.ChunkSize, m.Count, m.Created)
	if err != nil {
		return report, err
	}
	if n, err := result.RowsAffected(); err != nil || n != 1 {
		return report, fmt.Errorf("compiler input restore location compare-and-swap failed: %w", ErrConflict)
	}
	if err = tx.Commit(); err != nil {
		return report, err
	}
	report.Applied, report.State = true, "hot-verified"
	return report, nil
}

func checkCompilerInputRestoreSource(ctx context.Context, tx *sql.Tx, expected inputManifest, epoch int64, backendHash string, archive inputarchive.Manifest) error {
	if _, err := readInputArchiveEpoch(ctx, tx, epoch); err != nil {
		return err
	}
	current, err := readInputManifest(ctx, tx, expected.Digest)
	if err != nil {
		return err
	}
	if current != expected || current.Location != "legacy" {
		return fmt.Errorf("compiler input restore source manifest changed: %w", ErrConflict)
	}
	currentArchive, err := compilerInputArchiveManifest(ctx, tx, current, backendHash, "routed")
	if err != nil {
		return err
	}
	if !sameCompilerInputArchiveManifest(archive, currentArchive) {
		return fmt.Errorf("compiler input restore archive receipt changed: %w", ErrConflict)
	}
	var active bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM fugue_platform_input_archive_leases WHERE content_hash=$1 AND expires_at>now())`, expected.Digest).Scan(&active); err != nil {
		return err
	}
	if active {
		return fmt.Errorf("compiler input restore archive owner is active: %w", ErrConflict)
	}
	return nil
}

func restoreCompilerInputHotPage(ctx context.Context, db *sql.DB, m inputManifest, epoch int64, backendHash string, archive inputarchive.Manifest, first int, page [][]byte, remaining int64) (int64, error) {
	if first < 0 || len(page) == 0 || len(page) > inputChunkBatchSize || first+len(page) > m.Count || remaining < 1 {
		return 0, ErrInvalidInput
	}
	var statement strings.Builder
	statement.WriteString(`INSERT INTO fugue_platform_input_hot_chunks(content_hash,ordinal,chunk_hash,data) VALUES`)
	args := []any{m.Digest}
	var newBytes int64
	for index, stored := range page {
		want := min(int64(m.ChunkSize), m.Size-int64(first+index)*int64(m.ChunkSize))
		if _, err := decodeInputChunk(m.Format, stored, int(want)); err != nil {
			return 0, err
		}
		newBytes += int64(len(stored))
		if index > 0 {
			statement.WriteByte(',')
		}
		fmt.Fprintf(&statement, "($1,$%d,$%d,$%d)", 2+3*index, 3+3*index, 4+3*index)
		args = append(args, first+index, sharedHash(sha256.Sum256(stored)), stored)
	}
	if newBytes > remaining {
		return 0, fmt.Errorf("compiler input restore page exceeds stored-byte budget: %w", ErrConflict)
	}
	statement.WriteString(` ON CONFLICT(content_hash,ordinal) DO NOTHING`)
	tx, err := beginInputArchiveTx(ctx, db)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if err = checkCompilerInputRestoreSource(ctx, tx, m, epoch, backendHash, archive); err != nil {
		return 0, err
	}
	if _, err = tx.ExecContext(ctx, statement.String(), args...); err != nil {
		return 0, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT ordinal,chunk_hash,data FROM fugue_platform_input_hot_chunks WHERE content_hash=$1 AND ordinal >= $2 AND ordinal < $3 ORDER BY ordinal`, m.Digest, first, first+len(page))
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	count := 0
	var actualBytes int64
	for rows.Next() {
		var ordinal int
		var declared string
		var stored []byte
		if err = rows.Scan(&ordinal, &declared, &stored); err != nil {
			return 0, err
		}
		if count >= len(page) || ordinal != first+count || declared != sharedHash(sha256.Sum256(stored)) {
			return 0, fmt.Errorf("compiler input restore staging identity differs: %w", ErrConflict)
		}
		want := min(int64(m.ChunkSize), m.Size-int64(ordinal)*int64(m.ChunkSize))
		got, err := decodeInputChunk(m.Format, stored, int(want))
		if err != nil {
			return 0, err
		}
		expected, err := decodeInputChunk(m.Format, page[count], int(want))
		if err != nil || !bytes.Equal(got, expected) {
			return 0, fmt.Errorf("compiler input restore staging content differs; conflict retained: %w", ErrConflict)
		}
		// An exact retry may encounter another valid compression of the same
		// raw chunk. Preserve it and account its actual size, never overwrite it.
		actualBytes += int64(len(stored))
		count++
	}
	if err = rows.Err(); err != nil {
		return 0, err
	}
	if err = rows.Close(); err != nil {
		return 0, err
	}
	if count != len(page) || actualBytes > remaining {
		return 0, fmt.Errorf("compiler input restore staging coverage/budget differs: %w", ErrConflict)
	}
	return actualBytes, tx.Commit()
}
