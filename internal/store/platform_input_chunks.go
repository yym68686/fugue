package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"regexp"
	"strings"
	"time"

	"fugue/internal/inputarchive"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"github.com/jackc/pgx/v5/pgconn"
)

const inputChunkBytes = 256 << 10
const inputMaxBytes = 1 << 30

// Bound the additional write buffer to four MiB while amortizing database
// round trips. The stored chunk format and transaction are unchanged.
const inputChunkBatchSize = 16

// A large, valid snapshot must fit its encode/write/full-readback cycle after
// waiting for the shared writer. Keep the operation bounded and let a shorter
// caller deadline or cancellation release the writer queue promptly.
const compilerInputWriteTimeout = 2 * time.Minute

var inputDigestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

func compilerInputChunksTable(location string) (string, error) {
	switch location {
	case "", "legacy":
		return "fugue_platform_input_chunks", nil
	case "hot":
		return "fugue_platform_input_hot_chunks", nil
	default:
		return "", fmt.Errorf("invalid compiler input storage location: %w", ErrConflict)
	}
}

type inputManifest struct {
	Digest           string
	Format           string
	Location         string
	Size             int64
	ChunkSize, Count int
	Created          time.Time
}
type inputQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func readInputManifest(ctx context.Context, db inputQueryer, digest string) (inputManifest, error) {
	m := inputManifest{Digest: digest}
	err := db.QueryRowContext(ctx, `SELECT format,size_bytes,chunk_size,chunk_count,created_at,COALESCE(to_jsonb(m)->>'storage_location','legacy') FROM fugue_platform_input_manifests m WHERE content_hash=$1`, digest).Scan(&m.Format, &m.Size, &m.ChunkSize, &m.Count, &m.Created, &m.Location)
	if err != nil {
		return m, err
	}
	return m, validateCompilerInputManifest(m)
}

func validateCompilerInputManifest(m inputManifest) error {
	digest := m.Digest
	if m.Location != "legacy" && m.Location != "hot" {
		return fmt.Errorf("invalid compiler input storage location: %w", ErrConflict)
	}
	if m.Format == inputFormatSharedV3 || m.Format == inputFormatSeparatedV4 {
		if !inputDigestPattern.MatchString(digest) || m.Size < 1 || m.Size > inputMaxBytes || m.ChunkSize != inputChunkBytes || m.Count != int((m.Size+inputChunkBytes-1)/inputChunkBytes) || m.Created.IsZero() {
			return ErrConflict
		}
		return nil
	}
	if !inputDigestPattern.MatchString(digest) || (m.Format != inputFormatV1 && m.Format != inputFormatCodecV2) || m.Size <= 0 || m.Size > inputMaxBytes || m.ChunkSize < 1 || m.ChunkSize > inputChunkBytes || m.Count != int((m.Size+int64(m.ChunkSize)-1)/int64(m.ChunkSize)) || m.Created.IsZero() || m.Format == inputFormatCodecV2 && m.ChunkSize > inputChunkBytes-inputCodecHeaderBytes {
		return fmt.Errorf("invalid compiler input manifest: %w", ErrConflict)
	}
	return nil
}

func absentInputManifest(err error) bool {
	var pg *pgconn.PgError
	return errors.Is(err, sql.ErrNoRows) || errors.As(err, &pg) && pg.Code == "42P01"
}

type inputChunkReader struct {
	separated    *separatedInputReader
	shared       *sharedInputReader
	archive      *inputarchive.Reader
	rows         *sql.Rows
	manifest     inputManifest
	ordinal      int
	size         int64
	pending      []byte
	hash         hash.Hash
	done         bool
	err          error
	storedHashes [][sha256.Size]byte
	storedSizes  []int
}

func openInputChunks(ctx context.Context, db inputQueryer, m inputManifest) (*inputChunkReader, error) {
	if m.Format == inputFormatSeparatedV4 {
		r, err := openSeparatedInput(ctx, db, m)
		if err != nil {
			return nil, err
		}
		return &inputChunkReader{separated: r}, nil
	}
	if m.Format == inputFormatSharedV3 {
		shared, err := openSharedInput(ctx, db, m)
		if err != nil {
			return nil, err
		}
		return &inputChunkReader{shared: shared}, nil
	}
	table, err := compilerInputChunksTable(m.Location)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, `SELECT ordinal,chunk_hash,data FROM `+table+` WHERE content_hash=$1 ORDER BY ordinal`, m.Digest)
	if err != nil {
		return nil, err
	}
	return &inputChunkReader{rows: rows, manifest: m, hash: sha256.New()}, nil
}
func (r *inputChunkReader) Read(p []byte) (int, error) {
	if r.archive != nil {
		n, err := r.archive.Read(p)
		r.done = r.archive.Verified()
		if err != nil && !errors.Is(err, io.EOF) {
			r.err = err
		}
		return n, err
	}
	if r.separated != nil {
		n, err := r.separated.Read(p)
		r.done, r.err = r.separated.done, r.separated.err
		return n, err
	}
	if r.shared != nil {
		n, err := r.shared.Read(p)
		r.done, r.err = r.shared.done, r.shared.err
		return n, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	if r.err != nil {
		return 0, r.err
	}
	for len(r.pending) == 0 {
		if r.done {
			return 0, io.EOF
		}
		if !r.rows.Next() {
			r.done = true
			r.err = r.rows.Err()
			if r.err == nil && (r.ordinal != r.manifest.Count || r.size != r.manifest.Size || "sha256:"+hex.EncodeToString(r.hash.Sum(nil)) != r.manifest.Digest) {
				r.err = fmt.Errorf("compiler input content length/count/digest differs: %w", ErrConflict)
			}
			if r.err != nil {
				return 0, r.err
			}
			return 0, io.EOF
		}
		var ordinal int
		var digest string
		var raw []byte
		if r.err = r.rows.Scan(&ordinal, &digest, &raw); r.err != nil {
			return 0, r.err
		}
		h := sha256.Sum256(raw)
		want := r.manifest.ChunkSize
		if ordinal == r.manifest.Count-1 {
			want = int(r.manifest.Size - int64(ordinal)*int64(r.manifest.ChunkSize))
		}
		if ordinal != r.ordinal || ordinal >= r.manifest.Count || digest != "sha256:"+hex.EncodeToString(h[:]) {
			r.err = fmt.Errorf("compiler input chunk identity/size/hash differs: %w", ErrConflict)
			return 0, r.err
		}
		storedSize := len(raw)
		raw, r.err = decodeInputChunk(r.manifest.Format, raw, want)
		if r.err != nil {
			return 0, r.err
		}
		if r.manifest.Format == inputFormatCodecV2 && r.manifest.Count <= inputMaxBytes/inputChunkBytes {
			r.storedHashes = append(r.storedHashes, h)
			r.storedSizes = append(r.storedSizes, storedSize)
		}
		r.ordinal++
		r.size += int64(len(raw))
		r.hash.Write(raw)
		r.pending = raw
	}
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}

func (r *inputChunkReader) Close() error {
	if r.archive != nil {
		return r.archive.Close()
	}
	if r.rows != nil {
		return r.rows.Close()
	}
	return nil
}

type inputChunkWriter struct {
	ctx               context.Context
	tx                *sql.Tx
	digest            string
	location          string
	hash              hash.Hash
	chunk             []byte
	count             int
	size              int64
	expected          [][sha256.Size]byte
	storedSizes       []int
	dnsRanges         []compilerDNSRange
	dnsRangesOverflow bool
	pending           []inputPendingChunk
	batch             []byte
	codec             *inputChunkEncoder
}

type inputPendingChunk struct {
	ordinal int
	digest  string
	data    []byte
}

func (w *inputChunkWriter) Write(p []byte) (int, error) {
	if w.size+int64(len(p)) > inputMaxBytes {
		return 0, fmt.Errorf("compiler input exceeds bounded storage size")
	}
	total := len(p)
	for len(p) > 0 {
		n := min(cap(w.chunk)-len(w.chunk), len(p))
		w.chunk = append(w.chunk, p[:n]...)
		w.hash.Write(p[:n])
		w.size += int64(n)
		p = p[n:]
		if len(w.chunk) == cap(w.chunk) {
			if err := w.stageChunk(); err != nil {
				return total - len(p), err
			}
		}
	}
	return total, nil
}
func (w *inputChunkWriter) flush() error {
	if err := w.stageChunk(); err != nil {
		return err
	}
	return w.flushPending()
}

func (w *inputChunkWriter) stageChunk() error {
	if len(w.chunk) == 0 {
		return nil
	}
	if w.count >= (inputMaxBytes+cap(w.chunk)-1)/cap(w.chunk) {
		return fmt.Errorf("compiler input verification chunk inventory exceeds bound: %w", ErrConflict)
	}
	stored := w.chunk
	if w.codec != nil {
		var err error
		stored, err = w.codec.encode(w.chunk)
		if err != nil {
			return err
		}
		w.storedSizes = append(w.storedSizes, len(stored))
	}
	h := sha256.Sum256(stored)
	if w.tx != nil {
		// The encoder immediately reuses w.chunk. Retain independent bounded
		// bytes until this batch has been synchronously written.
		if w.batch == nil {
			w.batch = make([]byte, 0, inputChunkBatchSize*inputChunkBytes)
		}
		start := len(w.batch)
		w.batch = append(w.batch, stored...)
		w.pending = append(w.pending, inputPendingChunk{ordinal: w.count, digest: "sha256:" + hex.EncodeToString(h[:]), data: w.batch[start:len(w.batch):len(w.batch)]})
	}
	w.expected = append(w.expected, h)
	w.count++
	w.chunk = w.chunk[:0]
	if len(w.pending) == inputChunkBatchSize {
		return w.flushPending()
	}
	return nil
}

func (w *inputChunkWriter) flushPending() error {
	if len(w.pending) == 0 {
		return nil
	}
	table, err := compilerInputChunksTable(w.location)
	if err != nil {
		return err
	}
	var statement strings.Builder
	statement.WriteString(`INSERT INTO ` + table + `(content_hash,ordinal,chunk_hash,data) VALUES`)
	args := make([]any, 1, 2+4*len(w.pending))
	args[0] = w.digest
	// Send one binary buffer after the small parameters. Many separate bytea
	// parameters repeatedly grow the driver's encoding buffers for every batch.
	// PostgreSQL splits the bounded payload back into the original stored chunks.
	payloadParameter, offset := 2+4*len(w.pending), 1
	for index, chunk := range w.pending {
		if index > 0 {
			statement.WriteByte(',')
		}
		fmt.Fprintf(&statement, "($1,$%d,$%d,substring($%d::bytea from $%d::int for $%d::int))", 2+4*index, 3+4*index, payloadParameter, 4+4*index, 5+4*index)
		args = append(args, chunk.ordinal, chunk.digest, offset, len(chunk.data))
		offset += len(chunk.data)
	}
	args = append(args, w.batch)
	if _, err := w.tx.ExecContext(w.ctx, statement.String(), args...); err != nil {
		return err
	}
	clear(w.pending)
	w.pending = w.pending[:0]
	w.batch = w.batch[:0]
	return nil
}

func (s *Store) pgEnsureChunkedInput(parent context.Context, snapshot platformconfig.RuntimeSnapshot, digest string) (resultErr error) {
	observation := compilerInputWriterStats.beginObserved(parent)
	defer func() {
		if recovered := recover(); recovered != nil {
			observation.finish(errors.New("compiler-input writer panic"))
			panic(recovered)
		}
		observation.finish(resultErr)
	}()
	if !inputDigestPattern.MatchString(digest) {
		return ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(parent, compilerInputWriteTimeout)
	defer cancel()
	defer observation.stopWork()
	observation.setWriterContext(ctx)
	// One cluster-wide writer bounds database parse/transfer concurrency even
	// while multiple API replicas compile distinct scopes. No serving lock is held.
	observation.advance(compilerInputWriterBeginTx)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer observation.rollback(tx)
	observation.afterBegin()
	observation.advance(compilerInputWriterBarrier)
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(315609238744290)`); err != nil {
		return err
	}
	observation.afterBarrier()
	observation.advance(compilerInputWriterPersist)
	return s.pgEnsureChunkedInputTx(ctx, tx, snapshot, digest, observation)
}

// Both policy-selected writers hold the same exclusive barrier through full
// verification and commit. This helper never starts a new budget or records a
// second outer attempt when a fenced JSONB writer uses an independent format.
func (s *Store) pgEnsureChunkedInputTx(ctx context.Context, tx *sql.Tx, snapshot platformconfig.RuntimeSnapshot, digest string, observation *compilerInputWriterAttempt) error {
	m, err := readInputManifest(ctx, tx, digest)
	if err == nil {
		archiveRouted, routeErr := compilerInputArchiveRoute(ctx, tx, m)
		if routeErr != nil {
			return routeErr
		}
		if archiveRouted {
			actual, e := platformconfig.RuntimeSnapshotDigest(snapshot)
			if e != nil || actual != digest {
				return ErrConflict
			}
			// Preparation owns remote restoration and durable pin admission.
			// A normal writer must not GET cold history under the writer lock,
			// or mask missing/corrupt canonical bytes with the caller's copy.
			if _, e = verifyCompilerInputLocalRepresentation(ctx, tx, m, inputMaxBytes); e != nil {
				return e
			}
			return observation.commit(tx)
		}
		if m.Format == inputFormatSeparatedV4 {
			actual, e := platformconfig.RuntimeSnapshotDigest(snapshot)
			if e != nil {
				return e
			}
			if actual != digest {
				return ErrConflict
			}
			if cached := s.compilerDNSInputs.get(digest); !archiveRouted && cached != nil && cached.Manifest == m {
				if _, e := readSeparatedDNSSelections(ctx, tx, m, cached, func(CompilerDNSSelectionSummary) bool { return false }); e == nil {
					return observation.commit(tx)
				}
				// A completed migration can replace the closure while retaining
				// the identity. Only a full verified read can establish it again.
			}
			r, e := openSeparatedInput(ctx, tx, m)
			if e != nil {
				return e
			}
			if _, e = io.Copy(io.Discard, r); e != nil {
				return e
			}
			return observation.commit(tx)
		}
		if m.Format == inputFormatSharedV3 {
			actual, err := platformconfig.RuntimeSnapshotDigest(snapshot)
			if err != nil {
				return err
			}
			if actual != digest {
				return ErrConflict
			}
			if cached := s.compilerDNSInputs.get(digest); !archiveRouted && cached != nil && cached.Manifest == m {
				if _, err := readSharedDNSSelections(ctx, tx, m, cached, func(CompilerDNSSelectionSummary) bool { return false }); err != nil {
					return err
				}
				return observation.commit(tx)
			}
			r, err := openSharedInput(ctx, tx, m)
			if err != nil {
				return err
			}
			if _, err = io.Copy(io.Discard, r); err != nil {
				return err
			}
			if err = observation.commit(tx); err != nil {
				return err
			}
			s.rememberSharedDNSInput(snapshot, m, r)
			return nil
		}
		// Older manifests may use unusually small chunks. Keep the witness
		// vector bounded to the same maximum as a newly written input.
		if m.Count > inputMaxBytes/inputChunkBytes || m.Format == inputFormatCodecV2 {
			actual, hashErr := platformconfig.RuntimeSnapshotDigest(snapshot)
			if hashErr != nil {
				return hashErr
			}
			if actual != digest {
				return ErrConflict
			}
			if m.Format == inputFormatCodecV2 {
				if cached := s.compilerDNSInputs.get(digest); !archiveRouted && cached != nil && cached.Manifest.Format == m.Format && cached.Manifest.Size == m.Size && cached.Manifest.Count == m.Count && cached.Manifest.ChunkSize == m.ChunkSize {
					if err := verifyInputChunkHashesInTable(ctx, tx, m, cached.Hashes, m.Location, cached.StoredSizes); err != nil {
						return err
					}
					return observation.commit(tx)
				}
			}
			r, err := s.openInputChunks(ctx, tx, m)
			if err != nil {
				return err
			}
			_, err = io.Copy(io.Discard, r)
			r.Close()
			if err != nil {
				return err
			}
			if m.Format == inputFormatCodecV2 && len(r.storedHashes) == m.Count {
				w := &inputChunkWriter{digest: digest, hash: sha256.New(), chunk: make([]byte, 0, m.ChunkSize)}
				if err := platformconfig.WriteRuntimeSnapshot(w, snapshot); err != nil {
					return err
				}
				if err := w.flush(); err != nil {
					return err
				}
				w.expected, w.storedSizes = r.storedHashes, r.storedSizes
				if err := observation.commit(tx); err != nil {
					return err
				}
				s.rememberCompilerDNSInput(snapshot, m, w)
				return nil
			}
		} else {
			w := &inputChunkWriter{ctx: ctx, digest: digest, hash: sha256.New(), chunk: make([]byte, 0, m.ChunkSize)}
			if err := platformconfig.WriteRuntimeSnapshot(w, snapshot); err != nil {
				return err
			}
			if err := w.flush(); err != nil {
				return err
			}
			if w.size != m.Size || w.count != m.Count || "sha256:"+hex.EncodeToString(w.hash.Sum(nil)) != digest {
				return ErrConflict
			}
			if err := verifyInputChunkHashesInTable(ctx, tx, m, w.expected, m.Location); err != nil {
				return err
			}
			if err := observation.commit(tx); err != nil {
				return err
			}
			s.rememberCompilerDNSInput(snapshot, m, w)
			return nil
		}
		return observation.commit(tx)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	// Retain existing legacy content as the canonical observation. Never hide
	// a corrupt legacy row by replacing it with a newly created representation.
	var legacy bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM fugue_platform_artifact_contents WHERE content_hash=$1)`, digest).Scan(&legacy); err != nil {
		return err
	}
	if legacy {
		var raw []byte
		if err = tx.QueryRowContext(ctx, `SELECT content_json FROM fugue_platform_artifact_contents WHERE content_hash=$1`, digest).Scan(&raw); err != nil {
			return err
		}
		stored, err := platformconfig.DecodeRuntimeSnapshotJSON(raw)
		if err != nil {
			return err
		}
		actual, err := platformconfig.RuntimeSnapshotDigest(stored)
		if err != nil {
			return err
		}
		if actual != digest {
			return ErrConflict
		}
		actual, err = platformconfig.RuntimeSnapshotDigest(snapshot)
		if err != nil {
			return err
		}
		if actual != digest {
			return ErrConflict
		}
		return observation.commit(tx)
	}
	if s.compilerInputSeparatedEnabled() {
		if _, err = tx.ExecContext(ctx, `SAVEPOINT separated_input_budget`); err != nil {
			return err
		}
		_, err = s.pgWriteSeparatedInput(ctx, tx, snapshot, digest, nil)
		if err == nil {
			return observation.commit(tx)
		}
		if !errors.Is(err, errSharedInputBudget) {
			return err
		}
		separatedInputStats.fallback.Add(1)
		if _, err = tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT separated_input_budget`); err != nil {
			return err
		}
	}
	if s.compilerInputSharedEnabled() {
		if _, err = tx.ExecContext(ctx, `SAVEPOINT shared_input_budget`); err != nil {
			return err
		}
		err = s.pgWriteSharedInput(ctx, tx, snapshot, digest)
		if err == nil {
			return observation.commit(tx)
		}
		if !errors.Is(err, errSharedInputBudget) {
			return err
		}
		// Pathological inputs retain the established v1/v2 path. Undo partial
		// shared writes atomically before fallback; never hide corrupt content.
		if _, err = tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT shared_input_budget`); err != nil {
			return err
		}
	}
	legacyWritesFenced, err := compilerInputWritesFenced(ctx, tx)
	if err != nil {
		return err
	}
	location := "legacy"
	if legacyWritesFenced {
		location = "hot"
	}
	format, chunkSize := inputFormatV1, inputChunkBytes
	var codec *inputChunkEncoder
	if s.compilerInputCodecEnabled() {
		format = inputFormatCodecV2
		chunkSize -= inputCodecHeaderBytes
		codec = &inputChunkEncoder{}
	}
	w := &inputChunkWriter{ctx: ctx, tx: tx, digest: digest, location: location, hash: sha256.New(), chunk: make([]byte, 0, chunkSize), codec: codec}
	if err = platformconfig.WriteRuntimeSnapshot(w, snapshot); err != nil {
		return err
	}
	if err = w.flush(); err != nil {
		return err
	}
	if "sha256:"+hex.EncodeToString(w.hash.Sum(nil)) != digest {
		return ErrConflict
	}
	m = inputManifest{Digest: digest, Format: format, Location: location, Size: w.size, Count: w.count, ChunkSize: chunkSize, Created: time.Now().UTC()}
	if _, err = tx.ExecContext(ctx, `INSERT INTO fugue_platform_input_manifests(content_hash,format,size_bytes,chunk_size,chunk_count,created_at) VALUES($1,$2,$3,$4,$5,$6)`, m.Digest, m.Format, m.Size, m.ChunkSize, m.Count, m.Created); err != nil {
		return err
	}
	if location == "hot" {
		result, updateErr := tx.ExecContext(ctx, `UPDATE fugue_platform_input_manifests SET storage_location='hot' WHERE content_hash=$1 AND storage_location='legacy'`, m.Digest)
		if updateErr != nil {
			return updateErr
		}
		if changed, updateErr := result.RowsAffected(); updateErr != nil || changed != 1 {
			return fmt.Errorf("compiler input hot location was not recorded: %w", ErrConflict)
		}
	}
	// Hash the actual stored bytes in PostgreSQL and compare with the hashes
	// of the input just written. A claimed chunk_hash alone is not proof. This
	// retains read-after-write verification without returning the entire body.
	if err = verifyInputChunkHashesInTable(ctx, tx, m, w.expected, location, w.storedSizes); err != nil {
		return err
	}
	if err := observation.commit(tx); err != nil {
		return err
	}
	s.rememberCompilerDNSInput(snapshot, m, w)
	return nil
}

// The canonical input stream has already been hashed to m.Digest. Each stored
// chunk must match that stream's independent SHA-256, position and length, so
// even changing both stored data and its declared chunk_hash cannot pass.
func verifyInputChunkHashes(ctx context.Context, db inputQueryer, m inputManifest, expected [][sha256.Size]byte, storedSizes ...[]int) error {
	return verifyInputChunkHashesInTable(ctx, db, m, expected, "legacy", storedSizes...)
}

func verifyInputChunkHashesInTable(ctx context.Context, db inputQueryer, m inputManifest, expected [][sha256.Size]byte, location string, storedSizes ...[]int) error {
	if len(expected) != m.Count {
		return ErrConflict
	}
	table, err := compilerInputChunksTable(location)
	if err != nil {
		return err
	}
	var physicalSizes []int
	if m.Format == inputFormatCodecV2 {
		if len(storedSizes) != 1 || len(storedSizes[0]) != m.Count {
			return ErrConflict
		}
		physicalSizes = storedSizes[0]
	}
	rows, err := db.QueryContext(ctx, `SELECT ordinal,chunk_hash,octet_length(data),sha256(data) FROM `+table+` WHERE content_hash=$1 ORDER BY ordinal`, m.Digest)
	if err != nil {
		return err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var ordinal, size int
		var declared string
		var actual []byte
		if err := rows.Scan(&ordinal, &declared, &size, &actual); err != nil {
			return err
		}
		want := m.ChunkSize
		if count == m.Count-1 {
			want = int(m.Size - int64(count)*int64(m.ChunkSize))
		}
		if physicalSizes != nil && count < len(physicalSizes) {
			want = physicalSizes[count]
		}
		if count >= m.Count || ordinal != count || size != want || !bytes.Equal(actual, expected[count][:]) || declared != "sha256:"+hex.EncodeToString(expected[count][:]) {
			return fmt.Errorf("compiler input chunk identity/size/hash differs: %w", ErrConflict)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if count != m.Count {
		return fmt.Errorf("compiler input chunk count differs: %w", ErrConflict)
	}
	return nil
}

func (s *Store) GetPlatformCompilerInput(digest string) (platformconfig.RuntimeSnapshot, error) {
	if !inputDigestPattern.MatchString(digest) {
		return platformconfig.RuntimeSnapshot{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		// Keep the manifest and every streamed chunk in one MVCC snapshot.
		// A concurrent GC may commit after the manifest read; this reader must
		// still see the complete pre-collection representation.
		tx, finish, err := s.BeginCompilerInputReadLease(ctx)
		if err != nil {
			return platformconfig.RuntimeSnapshot{}, err
		}
		defer finish()
		m, err := readInputManifest(ctx, tx, digest)
		if err == nil {
			return s.readCompilerInputSnapshot(ctx, tx, m)
		}
		if !absentInputManifest(err) {
			return platformconfig.RuntimeSnapshot{}, err
		}
		finish() // Release the connection and session lease before the legacy fallback.
	}
	content, err := s.GetPlatformArtifactContent(digest)
	if err != nil {
		return platformconfig.RuntimeSnapshot{}, err
	}
	snapshot, err := platformconfig.DecodeRuntimeSnapshotContent(content.Content)
	if err != nil {
		return snapshot, fmt.Errorf("compiler input cannot be decoded: %w", ErrConflict)
	}
	actual, err := platformconfig.RuntimeSnapshotDigest(snapshot)
	if err != nil {
		return snapshot, fmt.Errorf("compiler input cannot be hashed: %w", ErrConflict)
	}
	if actual != digest {
		return snapshot, ErrConflict
	}
	return snapshot, nil
}

func (s *Store) readCompilerInputSnapshot(ctx context.Context, db inputQueryer, m inputManifest) (platformconfig.RuntimeSnapshot, error) {
	r, err := s.openInputChunks(ctx, db, m)
	if err != nil {
		return platformconfig.RuntimeSnapshot{}, err
	}
	defer r.Close()
	out, err := platformconfig.DecodeRuntimeSnapshotReader(r)
	if err != nil {
		return platformconfig.RuntimeSnapshot{}, fmt.Errorf("compiler input cannot be decoded or verified: %w", ErrConflict)
	}
	if !r.done || r.err != nil {
		return platformconfig.RuntimeSnapshot{}, ErrConflict
	}
	if r.shared != nil {
		s.rememberSharedDNSInput(out, m, r.shared)
	}
	if r.separated != nil {
		s.rememberSeparatedDNSInput(ctx, db, out, m, r.separated)
	}
	if m.Format == inputFormatCodecV2 && len(r.storedHashes) == m.Count {
		// Positions come from canonical JSON; physical witnesses come from
		// the verified stream, never recompression assumptions.
		w := &inputChunkWriter{digest: m.Digest, hash: sha256.New(), chunk: make([]byte, 0, m.ChunkSize)}
		if platformconfig.WriteRuntimeSnapshot(w, out) == nil && w.flush() == nil {
			w.expected, w.storedSizes = r.storedHashes, r.storedSizes
			s.rememberCompilerDNSInput(out, m, w)
		}
	}
	return out, nil
}

func (s *Store) pgReadChunkedContent(ctx context.Context, digest string) (model.PlatformArtifactContent, error) {
	tx, finish, err := s.BeginCompilerInputReadLease(ctx)
	if err != nil {
		return model.PlatformArtifactContent{}, err
	}
	defer finish()
	m, err := readInputManifest(ctx, tx, digest)
	if err != nil {
		return model.PlatformArtifactContent{}, err
	}
	r, err := s.openInputChunks(ctx, tx, m)
	if err != nil {
		return model.PlatformArtifactContent{}, err
	}
	defer r.Close()
	var content map[string]any
	d := json.NewDecoder(r)
	if err = d.Decode(&content); err != nil {
		return model.PlatformArtifactContent{}, err
	}
	if err = d.Decode(new(any)); err != io.EOF {
		return model.PlatformArtifactContent{}, ErrConflict
	}
	return model.PlatformArtifactContent{ContentHash: digest, Content: content, SizeBytes: m.Size, CreatedAt: m.Created, UpdatedAt: m.Created}, nil
}
