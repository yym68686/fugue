package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"hash"
	"io"
	"time"

	"fugue/internal/platformconfig"
	"fugue/internal/schemamigrate"
)

// This collector retires replaced physical representations and unreachable
// components only. It never deletes an input identity, publication, LKG or pin.
// Historical input retirement needs the separate complete dependency-graph GC.
type CompilerInputMaintenanceOptions struct {
	Action         string
	Digest         string
	ExpectedFormat string
	MaxInputBytes  int64
	Apply          bool
}

// archiveInputStream is a bounded, keyset-paged reader over the old v1/v2
// chunks. It never holds a multi-hour database transaction. The archive ledger
// pins this source representation; every page also rechecks its manifest,
// maintenance epoch and per-chunk hash under the common writer barrier.
type archiveInputStream struct {
	ctx         context.Context
	db          *sql.DB
	m           inputManifest
	next        int
	rows        [][]byte
	row         int
	count       int
	size        int64
	hash        hash.Hash
	sourceEpoch int64
	done        bool
	err         error
}

func (r *archiveInputStream) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	if len(p) == 0 {
		return 0, nil
	}
	for len(r.rows) == 0 || r.row >= len(r.rows) {
		if r.done {
			return 0, io.EOF
		}
		if err := r.fetch(); err != nil {
			r.err = err
			return 0, err
		}
		if len(r.rows) == 0 && r.done {
			if r.count != r.m.Count || r.size != r.m.Size || sharedHashBytes(r.hash.Sum(nil)) != r.m.Digest {
				r.err = fmt.Errorf("compiler input archive source changed: %w", ErrConflict)
				return 0, r.err
			}
			return 0, io.EOF
		}
	}
	chunk := r.rows[r.row]
	n := copy(p, chunk)
	if n != len(chunk) {
		// Fetches are deliberately one decoded chunk at a time. Keep a small
		// pending buffer instead of allowing a caller to lose a suffix.
		r.rows[r.row] = chunk[n:]
		return n, nil
	}
	r.row++
	return n, nil
}

func (r *archiveInputStream) fetch() error {
	ctx, cancel := context.WithTimeout(r.ctx, 15*time.Second)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, statement := range []string{`SET LOCAL lock_timeout='250ms'`, `SET LOCAL statement_timeout='5s'`} {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock_shared(315609238744290)`); err != nil {
		return err
	}
	var epoch int64
	if err = tx.QueryRowContext(ctx, `SELECT epoch FROM fugue_platform_input_archive_control WHERE singleton`).Scan(&epoch); err != nil {
		return err
	}
	if epoch != r.sourceEpoch {
		return fmt.Errorf("compiler input archive maintenance epoch changed: %w", ErrConflict)
	}
	current, err := readInputManifest(ctx, tx, r.m.Digest)
	if err != nil || current != r.m {
		if err != nil {
			return err
		}
		return fmt.Errorf("compiler input archive source representation changed: %w", ErrConflict)
	}
	table, err := compilerInputChunksTable(r.m.Location)
	if err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT ordinal,chunk_hash,data FROM `+table+` WHERE content_hash=$1 AND ordinal >= $2 ORDER BY ordinal LIMIT 16`, r.m.Digest, r.next)
	if err != nil {
		return err
	}
	r.rows = nil
	r.row = 0
	for rows.Next() {
		var ordinal int
		var declared string
		var stored []byte
		if err = rows.Scan(&ordinal, &declared, &stored); err != nil {
			return err
		}
		if ordinal != r.next || ordinal >= r.m.Count {
			return fmt.Errorf("compiler input archive source ordinal differs: %w", ErrConflict)
		}
		h := sha256.Sum256(stored)
		if declared != "sha256:"+fmt.Sprintf("%x", h[:]) {
			return fmt.Errorf("compiler input archive source stored hash differs: %w", ErrConflict)
		}
		want := r.m.ChunkSize
		if ordinal == r.m.Count-1 {
			want = int(r.m.Size - int64(ordinal)*int64(r.m.ChunkSize))
		}
		decoded, decodeErr := decodeInputChunk(r.m.Format, stored, want)
		if decodeErr != nil {
			return decodeErr
		}
		if _, err = r.hash.Write(decoded); err != nil {
			return err
		}
		r.size += int64(len(decoded))
		r.count++
		r.next++
		r.rows = append(r.rows, decoded)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err = rows.Close(); err != nil {
		return err
	}
	if len(r.rows) == 0 {
		r.done = true
	}
	return tx.Commit()
}

func (s *archiveInputStream) Close() error { return nil }

type CompilerInputMaintenanceReport struct {
	Action               string `json:"action"`
	Applied              bool   `json:"applied"`
	Digest               string `json:"digest,omitempty"`
	OldFormat            string `json:"old_format,omitempty"`
	NewFormat            string `json:"new_format,omitempty"`
	LogicalBytes         int64  `json:"logical_bytes"`
	OldChunkPayloadBytes int64  `json:"old_chunk_payload_bytes"`
	NewPackBytes         uint64 `json:"new_pack_bytes"`
	Components           int64  `json:"components"`
	Packs                int64  `json:"packs"`
	PackPayloadBytes     int64  `json:"pack_payload_bytes"`
	FrameLogicalBytes    int64  `json:"frame_logical_bytes,omitempty"`
	UniqueEvidenceBytes  int64  `json:"unique_evidence_bytes,omitempty"`
	EvidenceReferences   int    `json:"evidence_references,omitempty"`
}

func RunCompilerInputMaintenance(ctx context.Context, databaseURL string, options CompilerInputMaintenanceOptions) (CompilerInputMaintenanceReport, error) {
	if options.Action != "inspect" && options.Action != "migrate" && options.Action != "rollback" && options.Action != "collect" {
		return CompilerInputMaintenanceReport{}, ErrInvalidInput
	}
	if options.MaxInputBytes == 0 {
		options.MaxInputBytes = 128 << 20
	}
	if options.MaxInputBytes < 1 || options.MaxInputBytes > 128<<20 || options.Action != "collect" && !inputDigestPattern.MatchString(options.Digest) {
		return CompilerInputMaintenanceReport{}, ErrInvalidInput
	}
	db, err := openPostgresDatabase(databaseURL)
	if err != nil {
		return CompilerInputMaintenanceReport{}, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	s := &Store{db: db, databaseURL: databaseURL, dbReady: true}
	if err = s.ConfigureCompilerInputArchiveFromEnv(); err != nil {
		return CompilerInputMaintenanceReport{}, err
	}
	return s.maintainCompilerInput(ctx, options)
}

func (s *Store) maintainCompilerInput(ctx context.Context, o CompilerInputMaintenanceOptions) (CompilerInputMaintenanceReport, error) {
	report := CompilerInputMaintenanceReport{Action: o.Action, Digest: o.Digest}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if o.Action == "inspect" {
		tx, finish, err := s.BeginCompilerInputReadLease(ctx)
		if err != nil {
			return report, err
		}
		defer finish()
		m, err := readInputManifest(ctx, tx, o.Digest)
		if err != nil {
			return report, err
		}
		report.OldFormat = m.Format
		report.NewFormat = inputFormatSeparatedV4
		report.LogicalBytes = m.Size
		table, tableErr := compilerInputChunksTable(m.Location)
		if tableErr != nil {
			return report, tableErr
		}
		err = tx.QueryRowContext(ctx, `SELECT COALESCE(sum(octet_length(data)),0) FROM `+table+` WHERE content_hash=$1`, o.Digest).Scan(&report.OldChunkPayloadBytes)
		if err == nil && m.Format == inputFormatSeparatedV4 {
			var r *separatedInputReader
			r, err = openSeparatedInput(ctx, tx, m)
			if err == nil {
				report.FrameLogicalBytes = int64(len(r.frame))
				seen := map[string]bool{}
				for _, part := range r.parts {
					if part.Evidence {
						report.EvidenceReferences++
						if !seen[part.Digest] {
							report.UniqueEvidenceBytes += r.components[part.Digest].Size
							seen[part.Digest] = true
						}
					}
				}
				err = tx.QueryRowContext(ctx, `SELECT COALESCE(sum(octet_length(data)),0) FROM fugue_platform_input_shared_packs WHERE pack_id IN(SELECT pack_id FROM fugue_platform_input_component_refs WHERE content_hash IN(SELECT component_hash FROM fugue_platform_input_separated_refs WHERE content_hash=$1))`, m.Digest).Scan(&report.PackPayloadBytes)
			}
		}
		return report, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return report, err
	}
	defer tx.Rollback()
	// Same writer fence as all v1/v2/v3/v4 creation; consistent lock order with
	// the optional historical collector. Short lock budgets yield to production.
	// temp_file_limit is a superuser-only GUC. The ordinary application role
	// cannot SET it, and maintenance must not acquire broader database rights.
	// Bounded inputs/batches, short statements and the CI disk reserve bound
	// this operation; work_mem limits memory, not total temporary disk usage.
	for _, q := range []string{`SET LOCAL lock_timeout='250ms'`, `SET LOCAL statement_timeout='5s'`, `SET LOCAL work_mem='1MB'`, `SET LOCAL max_parallel_workers_per_gather=0`, `SELECT pg_advisory_xact_lock_shared(hashtextextended('fugue.compiler-input-gc.v1',0))`, `SELECT pg_advisory_xact_lock(315609238744290)`} {
		if _, err = tx.ExecContext(ctx, q); err != nil {
			return report, err
		}
	}
	if err = schemamigrate.VerifyCompilerInputSeparated(ctx, tx); err != nil {
		return report, err
	}
	if o.Action == "collect" {
		err = collectInputComponents(ctx, tx, &report)
	} else {
		fenced, fenceErr := compilerInputWritesFenced(ctx, tx)
		if fenceErr != nil {
			return report, fenceErr
		}
		var m inputManifest
		m, err = readInputManifest(ctx, tx, o.Digest)
		if err != nil {
			return report, err
		}
		if fenced && o.Action != "rollback" && m.Location != "hot" {
			return report, fmt.Errorf("legacy compiler input writes are fenced: %w", ErrConflict)
		}
		archivePinned, pinErr := compilerInputArchivePinsSource(ctx, tx, m.Digest)
		if pinErr != nil {
			return report, pinErr
		}
		if archivePinned {
			return report, fmt.Errorf("compiler input representation is pinned by an external archive: %w", ErrConflict)
		}
		report.OldFormat = m.Format
		report.LogicalBytes = m.Size
		if m.Size > o.MaxInputBytes || o.ExpectedFormat != "" && o.ExpectedFormat != m.Format {
			return report, ErrConflict
		}
		target := inputFormatSeparatedV4
		if o.Action == "rollback" {
			target = inputFormatCodecV2
		}
		report.NewFormat = target
		if m.Format == target {
			return report, nil
		}
		if o.Action == "rollback" && m.Format != inputFormatSeparatedV4 && m.Format != inputFormatSharedV3 {
			return report, ErrConflict
		}
		table, tableErr := compilerInputChunksTable(m.Location)
		if tableErr != nil {
			return report, tableErr
		}
		if err = tx.QueryRowContext(ctx, `SELECT COALESCE(sum(octet_length(data)),0) FROM `+table+` WHERE content_hash=$1`, m.Digest).Scan(&report.OldChunkPayloadBytes); err != nil {
			return report, err
		}
		var snapshot platformconfig.RuntimeSnapshot
		snapshot, err = s.readCompilerInputSnapshot(ctx, tx, m)
		if err != nil {
			return report, err
		}
		actual, e := platformconfig.RuntimeSnapshotDigest(snapshot)
		if e != nil || actual != m.Digest {
			return report, ErrConflict
		}
		if target == inputFormatSeparatedV4 {
			report.NewPackBytes, err = s.pgWriteSeparatedInput(ctx, tx, snapshot, m.Digest, &m)
		} else {
			err = writeRollbackInput(ctx, tx, snapshot, m)
		}
		if err != nil {
			return report, err
		}
		// Read through the public representation decoder before retiring bytes.
		var current inputManifest
		current, err = readInputManifest(ctx, tx, m.Digest)
		if err != nil {
			return report, err
		}
		snapshot, err = s.readCompilerInputSnapshot(ctx, tx, current)
		if err != nil {
			return report, err
		}
		if actual, e = platformconfig.RuntimeSnapshotDigest(snapshot); e != nil || actual != m.Digest {
			return report, ErrConflict
		}
		if target == inputFormatSeparatedV4 {
			_, err = tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE content_hash=$1`, m.Digest)
		} else {
			_, err = tx.ExecContext(ctx, `DELETE FROM fugue_platform_input_separated_refs WHERE content_hash=$1`, m.Digest)
			if err == nil {
				_, err = tx.ExecContext(ctx, `DELETE FROM fugue_platform_input_separated_roots WHERE content_hash=$1`, m.Digest)
			}
		}
		if err != nil {
			return report, err
		}
		if m.Format == inputFormatSharedV3 {
			if _, err = tx.ExecContext(ctx, `DELETE FROM fugue_platform_input_shared_recipes WHERE content_hash=$1`, m.Digest); err != nil {
				return report, err
			}
			if _, err = tx.ExecContext(ctx, `DELETE FROM fugue_platform_input_shared_refs WHERE content_hash=$1`, m.Digest); err != nil {
				return report, err
			}
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO fugue_platform_input_storage_events(content_hash,old_format,new_format,old_payload_bytes,new_pack_bytes,created_at) VALUES($1,$2,$3,$4,$5,now())`, m.Digest, m.Format, target, report.OldChunkPayloadBytes, report.NewPackBytes)
	}
	if err != nil {
		return report, err
	}
	// Dry-run exercises encoding, verification and retirement in a transaction
	// that is always rolled back. It consumes bounded I/O/WAL; it isn't a read-
	// only audit. The CLI calls it a rehearsal, never filesystem reclamation.
	if !o.Apply {
		return report, nil
	}
	if err = tx.Commit(); err != nil {
		return report, err
	}
	report.Applied = true
	return report, nil
}

func compilerInputArchivePinsSource(ctx context.Context, db inputQueryer, digest string) (bool, error) {
	var installed bool
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('fugue_platform_input_archives') IS NOT NULL`).Scan(&installed); err != nil || !installed {
		return false, err
	}
	var pinned bool
	err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM fugue_platform_input_archives WHERE content_hash=$1)`, digest).Scan(&pinned)
	return pinned, err
}

func writeRollbackInput(ctx context.Context, tx *sql.Tx, snapshot platformconfig.RuntimeSnapshot, old inputManifest) error {
	location := old.Location
	fenced, err := compilerInputWritesFenced(ctx, tx)
	if err != nil {
		return err
	}
	if fenced {
		location = "hot"
	}
	w := &inputChunkWriter{ctx: ctx, tx: tx, digest: old.Digest, location: location, hash: sha256.New(), chunk: make([]byte, 0, inputChunkBytes-inputCodecHeaderBytes), codec: &inputChunkEncoder{}}
	if err := platformconfig.WriteRuntimeSnapshot(w, snapshot); err != nil {
		return err
	}
	if err := w.flush(); err != nil {
		return err
	}
	if sharedHashBytes(w.hash.Sum(nil)) != old.Digest || w.size != old.Size {
		return ErrConflict
	}
	m := inputManifest{Digest: old.Digest, Format: inputFormatCodecV2, Size: w.size, Count: w.count, ChunkSize: cap(w.chunk), Created: old.Created}
	if err := verifyInputChunkHashesInTable(ctx, tx, m, w.expected, location, w.storedSizes); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE fugue_platform_input_manifests SET format=$2,chunk_size=$3,chunk_count=$4 WHERE content_hash=$1 AND format=$5 AND size_bytes=$6`, m.Digest, m.Format, m.ChunkSize, m.Count, old.Format, old.Size)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err == nil && n != 1 {
		err = ErrConflict
	}
	if err == nil && location == "hot" {
		_, err = tx.ExecContext(ctx, `UPDATE fugue_platform_input_manifests SET storage_location='hot' WHERE content_hash=$1`, old.Digest)
	}
	return err
}

func collectInputComponents(ctx context.Context, tx *sql.Tx, report *CompilerInputMaintenanceReport) error {
	// FK barriers supplement the common fence. These are physical orphans,
	// not snapshots considered obsolete merely because of their age.
	rows, err := tx.QueryContext(ctx, `SELECT c.content_hash FROM fugue_platform_input_components c WHERE NOT EXISTS(SELECT 1 FROM fugue_platform_input_separated_refs r WHERE r.component_hash=c.content_hash) AND NOT EXISTS(SELECT 1 FROM fugue_platform_input_separated_roots r WHERE r.frame_hash=c.content_hash) ORDER BY c.content_hash LIMIT 16`)
	if err != nil {
		return err
	}
	var keys []string
	for rows.Next() {
		var k string
		if err = rows.Scan(&k); err != nil {
			break
		}
		keys = append(keys, k)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return err
	}
	for _, k := range keys {
		if _, err = tx.ExecContext(ctx, `DELETE FROM fugue_platform_input_component_recipes WHERE content_hash=$1`, k); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM fugue_platform_input_component_refs WHERE content_hash=$1`, k); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM fugue_platform_input_components WHERE content_hash=$1`, k); err != nil {
			return err
		}
		report.Components++
	}
	rows, err = tx.QueryContext(ctx, `SELECT p.pack_id,octet_length(p.data) FROM fugue_platform_input_shared_packs p WHERE NOT EXISTS(SELECT 1 FROM fugue_platform_input_shared_refs r WHERE r.pack_id=p.pack_id) AND NOT EXISTS(SELECT 1 FROM fugue_platform_input_component_refs r WHERE r.pack_id=p.pack_id) ORDER BY p.pack_id LIMIT 16`)
	if err != nil {
		return err
	}
	type pack struct {
		id   int64
		size int64
	}
	var packs []pack
	for rows.Next() {
		var p pack
		if err = rows.Scan(&p.id, &p.size); err != nil {
			break
		}
		packs = append(packs, p)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return err
	}
	for _, p := range packs {
		if _, err = tx.ExecContext(ctx, `DELETE FROM fugue_platform_input_shared_objects WHERE pack_id=$1`, p.id); err != nil {
			return err
		}
		result, e := tx.ExecContext(ctx, `DELETE FROM fugue_platform_input_shared_packs WHERE pack_id=$1`, p.id)
		if e != nil {
			return e
		}
		n, e := result.RowsAffected()
		if e != nil || n != 1 {
			return fmt.Errorf("pack retirement changed: %w", errors.Join(e, ErrConflict))
		}
		report.Packs++
		report.PackPayloadBytes += p.size
	}
	return nil
}
