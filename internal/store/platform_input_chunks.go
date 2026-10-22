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

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/runtimeobservation"
	"github.com/jackc/pgx/v5/pgconn"
)

const inputChunkBytes = 256 << 10
const inputMaxBytes = 1 << 30

// Bound the additional write buffer to four MiB while amortizing database
// round trips. The stored chunk format and transaction are unchanged.
const inputChunkBatchSize = 16

var inputDigestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

type inputManifest struct {
	Digest           string
	Format           string
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
	err := db.QueryRowContext(ctx, `SELECT format,size_bytes,chunk_size,chunk_count,created_at FROM fugue_platform_input_manifests WHERE content_hash=$1`, digest).Scan(&m.Format, &m.Size, &m.ChunkSize, &m.Count, &m.Created)
	if err != nil {
		return m, err
	}
	if !inputDigestPattern.MatchString(digest) || (m.Format != inputFormatV1 && m.Format != inputFormatCodecV2) || m.Size <= 0 || m.Size > inputMaxBytes || m.ChunkSize < 1 || m.ChunkSize > inputChunkBytes || m.Count != int((m.Size+int64(m.ChunkSize)-1)/int64(m.ChunkSize)) || m.Created.IsZero() || m.Format == inputFormatCodecV2 && m.ChunkSize > inputChunkBytes-inputCodecHeaderBytes {
		return m, fmt.Errorf("invalid compiler input manifest: %w", ErrConflict)
	}
	return m, nil
}

func absentInputManifest(err error) bool {
	var pg *pgconn.PgError
	return errors.Is(err, sql.ErrNoRows) || errors.As(err, &pg) && pg.Code == "42P01"
}

type inputChunkReader struct {
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
	rows, err := db.QueryContext(ctx, `SELECT ordinal,chunk_hash,data FROM fugue_platform_input_chunks WHERE content_hash=$1 ORDER BY ordinal`, m.Digest)
	if err != nil {
		return nil, err
	}
	return &inputChunkReader{rows: rows, manifest: m, hash: sha256.New()}, nil
}
func (r *inputChunkReader) Read(p []byte) (int, error) {
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

type inputChunkWriter struct {
	ctx               context.Context
	tx                *sql.Tx
	digest            string
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
	if w.count >= inputMaxBytes/inputChunkBytes {
		return fmt.Errorf("compiler input verification chunk inventory exceeds bound: %w", ErrConflict)
	}
	h := sha256.Sum256(w.chunk)
	if w.tx != nil {
		// The encoder immediately reuses w.chunk. Retain independent bounded
		// bytes until this batch has been synchronously written.
		if w.batch == nil {
			w.batch = make([]byte, 0, inputChunkBatchSize*cap(w.chunk))
		}
		start := len(w.batch)
		w.batch = append(w.batch, w.chunk...)
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
	var statement strings.Builder
	statement.WriteString(`INSERT INTO fugue_platform_input_chunks(content_hash,ordinal,chunk_hash,data) VALUES`)
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

func (s *Store) pgEnsureChunkedInput(parent context.Context, snapshot platformconfig.RuntimeSnapshot, digest string) error {
	if !inputDigestPattern.MatchString(digest) {
		return ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(runtimeobservation.CopyDatabaseOperation(context.Background(), parent), 30*time.Second)
	defer cancel()
	// One cluster-wide writer bounds database parse/transfer concurrency even
	// while multiple API replicas compile distinct scopes. No serving lock is held.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(315609238744290)`); err != nil {
		return err
	}
	m, err := readInputManifest(ctx, tx, digest)
	if err == nil {
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
			r, err := openInputChunks(ctx, tx, m)
			if err != nil {
				return err
			}
			_, err = io.Copy(io.Discard, r)
			r.rows.Close()
			if err != nil {
				return err
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
			if err := verifyInputChunkHashes(ctx, tx, m, w.expected); err != nil {
				return err
			}
			if err := tx.Commit(); err != nil {
				return err
			}
			s.rememberCompilerDNSInput(snapshot, m, w)
			return nil
		}
		return tx.Commit()
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
		return tx.Commit()
	}
	w := &inputChunkWriter{ctx: ctx, tx: tx, digest: digest, hash: sha256.New(), chunk: make([]byte, 0, inputChunkBytes)}
	if err = platformconfig.WriteRuntimeSnapshot(w, snapshot); err != nil {
		return err
	}
	if err = w.flush(); err != nil {
		return err
	}
	if "sha256:"+hex.EncodeToString(w.hash.Sum(nil)) != digest {
		return ErrConflict
	}
	m = inputManifest{Digest: digest, Format: inputFormatV1, Size: w.size, Count: w.count, ChunkSize: inputChunkBytes, Created: time.Now().UTC()}
	if _, err = tx.ExecContext(ctx, `INSERT INTO fugue_platform_input_manifests(content_hash,format,size_bytes,chunk_size,chunk_count,created_at) VALUES($1,$2,$3,$4,$5,$6)`, m.Digest, m.Format, m.Size, m.ChunkSize, m.Count, m.Created); err != nil {
		return err
	}
	// Hash the actual stored bytes in PostgreSQL and compare with the hashes
	// of the input just written. A claimed chunk_hash alone is not proof. This
	// retains read-after-write verification without returning the entire body.
	if err = verifyInputChunkHashes(ctx, tx, m, w.expected); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.rememberCompilerDNSInput(snapshot, m, w)
	return nil
}

// The canonical input stream has already been hashed to m.Digest. Each stored
// chunk must match that stream's independent SHA-256, position and length, so
// even changing both stored data and its declared chunk_hash cannot pass.
func verifyInputChunkHashes(ctx context.Context, db inputQueryer, m inputManifest, expected [][sha256.Size]byte) error {
	if len(expected) != m.Count {
		return ErrConflict
	}
	rows, err := db.QueryContext(ctx, `SELECT ordinal,chunk_hash,octet_length(data),sha256(data) FROM fugue_platform_input_chunks WHERE content_hash=$1 ORDER BY ordinal`, m.Digest)
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
		m, err := readInputManifest(ctx, s.db, digest)
		if err == nil {
			r, err := openInputChunks(ctx, s.db, m)
			if err != nil {
				return platformconfig.RuntimeSnapshot{}, err
			}
			defer r.rows.Close()
			out, err := platformconfig.DecodeRuntimeSnapshotReader(r)
			if err != nil {
				return platformconfig.RuntimeSnapshot{}, fmt.Errorf("compiler input cannot be decoded or verified: %w", ErrConflict)
			}
			if !r.done || r.err != nil {
				return platformconfig.RuntimeSnapshot{}, ErrConflict
			}
			if m.Format == inputFormatCodecV2 && len(r.storedHashes) == m.Count {
				// Positions come from canonical JSON; physical witnesses come
				// from the verified stream, never recompression assumptions.
				w := &inputChunkWriter{digest: digest, hash: sha256.New(), chunk: make([]byte, 0, m.ChunkSize)}
				if platformconfig.WriteRuntimeSnapshot(w, out) == nil && w.flush() == nil {
					w.expected, w.storedSizes = r.storedHashes, r.storedSizes
					s.rememberCompilerDNSInput(out, m, w)
				}
			}
			return out, nil
		}
		if !absentInputManifest(err) {
			return platformconfig.RuntimeSnapshot{}, err
		}
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

func (s *Store) pgReadChunkedContent(ctx context.Context, digest string) (model.PlatformArtifactContent, error) {
	m, err := readInputManifest(ctx, s.db, digest)
	if err != nil {
		return model.PlatformArtifactContent{}, err
	}
	r, err := openInputChunks(ctx, s.db, m)
	if err != nil {
		return model.PlatformArtifactContent{}, err
	}
	defer r.rows.Close()
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
