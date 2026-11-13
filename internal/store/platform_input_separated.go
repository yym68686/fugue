package store

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"time"

	"fugue/internal/platformconfig"
)

const inputFormatSeparatedV4 = "runtime-snapshot-separated-v4"
const separatedFrameLimit = 16 << 20
const separatedPartLimit = 40001

// Frame offsets skip the four-byte null placeholders. Evidence components are
// immutable, independently addressed streams; their fact objects share packs.
// Neither component IDs nor database pointers confer publication authority.
type separatedPart struct {
	Evidence     bool
	Digest       string
	Offset, Size uint32
}

func encodeSeparatedLayout(parts []separatedPart) ([]byte, error) {
	if len(parts) == 0 || len(parts) > separatedPartLimit {
		return nil, errSharedInputBudget
	}
	out := []byte{4}
	for _, p := range parts {
		if p.Size == 0 {
			return nil, ErrConflict
		}
		var h [32]byte
		kind := byte(0)
		if p.Evidence {
			if !inputDigestPattern.MatchString(p.Digest) || p.Offset != 0 {
				return nil, ErrConflict
			}
			raw, _ := hex.DecodeString(p.Digest[7:])
			copy(h[:], raw)
			kind = 1
		} else if p.Digest != "" {
			return nil, ErrConflict
		}
		out = append(out, kind)
		out = append(out, h[:]...)
		out = binary.BigEndian.AppendUint32(out, p.Offset)
		out = binary.BigEndian.AppendUint32(out, p.Size)
	}
	return out, nil
}

func decodeSeparatedLayout(raw []byte) ([]separatedPart, error) {
	if len(raw) < 42 || raw[0] != 4 || (len(raw)-1)%41 != 0 || (len(raw)-1)/41 > separatedPartLimit {
		return nil, ErrConflict
	}
	parts := make([]separatedPart, 0, (len(raw)-1)/41)
	for i := 1; i < len(raw); i += 41 {
		p := separatedPart{Offset: binary.BigEndian.Uint32(raw[i+33 : i+37]), Size: binary.BigEndian.Uint32(raw[i+37 : i+41])}
		if raw[i] > 1 || p.Size == 0 {
			return nil, ErrConflict
		}
		p.Evidence = raw[i] == 1
		if p.Evidence {
			p.Digest = sharedHashBytes(raw[i+1 : i+33])
			if p.Offset != 0 {
				return nil, ErrConflict
			}
		} else if !bytes.Equal(raw[i+1:i+33], make([]byte, 32)) {
			return nil, ErrConflict
		}
		parts = append(parts, p)
	}
	return parts, nil
}

func readComponentManifest(ctx context.Context, db inputQueryer, digest string) (inputManifest, error) {
	m := inputManifest{Digest: digest, Format: inputFormatSharedV3, ChunkSize: inputChunkBytes}
	err := db.QueryRowContext(ctx, `SELECT size_bytes,chunk_count,created_at FROM fugue_platform_input_components WHERE content_hash=$1`, digest).Scan(&m.Size, &m.Count, &m.Created)
	if err == nil && (!inputDigestPattern.MatchString(digest) || m.Size < 1 || m.Size > inputMaxBytes || m.Count != int((m.Size+inputChunkBytes-1)/inputChunkBytes) || m.Created.IsZero()) {
		err = ErrConflict
	}
	return m, err
}

func ensureInputComponent(ctx context.Context, tx *sql.Tx, raw []byte, batch *sharedInputWriter, profile *separatedInputWriteProfile) (string, uint64, error) {
	digest := sharedHash(sha256.Sum256(raw))
	m, err := readComponentManifest(ctx, tx, digest)
	if err == nil {
		if m.Size != int64(len(raw)) {
			return "", 0, ErrConflict
		}
		// The complete reconstructed input is read and verified below before
		// commit, including reused components. Do not read every body twice.
		return digest, 0, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", 0, err
	}
	if len(raw) == 0 || len(raw) > inputMaxBytes {
		return "", 0, ErrInvalidInput
	}
	w := batch
	if w == nil {
		w = &sharedInputWriter{components: true, ctx: ctx, tx: tx, encoder: inputChunkEncoder{level: zlib.DefaultCompression}, objects: map[[32]byte]sharedObject{}, seen: map[[32]byte]bool{}, profile: profile}
	}
	m, n, err := stageInputComponent(raw, digest, w)
	if err != nil {
		return "", 0, err
	}
	if batch == nil {
		if err = w.flush(); err != nil {
			return "", 0, err
		}
		n = w.newPackBytes
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO fugue_platform_input_components(content_hash,size_bytes,chunk_count,created_at) VALUES($1,$2,$3,now())`, digest, m.Size, m.Count); err != nil {
		return "", 0, err
	}
	return digest, n, nil
}

func stageInputComponent(raw []byte, digest string, batch *sharedInputWriter) (inputManifest, uint64, error) {
	w := batch
	w.digest, w.hash, w.size, w.ordinal, w.splitter = digest, sha256.New(), 0, 0, sharedSplitter{}
	before := w.newPackBytes
	_, err := w.Write(raw)
	if err == nil {
		err = w.stage()
	}
	if err != nil {
		return inputManifest{}, 0, err
	}
	if sharedHashBytes(w.hash.Sum(nil)) != digest {
		return inputManifest{}, 0, ErrConflict
	}
	return inputManifest{Digest: digest, Size: w.size, Count: w.ordinal}, w.newPackBytes - before, nil
}

type separatedInputWriter struct {
	ctx             context.Context
	tx              *sql.Tx
	frame           []byte
	parts           []separatedPart
	hash            hash.Hash
	size            int64
	packBytes       uint64
	verified        map[string]bool
	pendingEvidence map[string][]byte
	pendingBytes    int
	batch           *sharedInputWriter
	depth           int
	quoted          bool
	escaped         bool
	dnsHeader       uint8
	dnsArray        bool
	profile         *separatedInputWriteProfile
}

// separatedInputWriteProfile contains only bounded timings and size/count
// aggregates. It never retains input bytes, identities, SQL text or evidence.
// A profile belongs to one serialized compiler-input writer invocation.
type separatedInputWriteProfile struct {
	snapshotStreamNanos int64
	evidenceCallNanos   int64
	evidenceFlushNanos  int64
	componentFlushNanos int64
	packEncodeNanos     int64
	frameStageNanos     int64
	packEncodedBytes    int64
	evidenceOccurrences int64
	evidenceBytes       int64
	uniqueEvidenceBytes int64
	snapshotComplete    bool
}

func (p *separatedInputWriteProfile) aggregate(w *separatedInputWriter) map[string]int64 {
	if p == nil || w == nil || w.batch == nil {
		return nil
	}
	return map[string]int64{
		"snapshot_stream_ms":              time.Duration(p.snapshotStreamNanos).Milliseconds(),
		"evidence_callback_ms":            time.Duration(p.evidenceCallNanos).Milliseconds(),
		"evidence_flush_ms":               time.Duration(p.evidenceFlushNanos).Milliseconds(),
		"component_flush_ms":              time.Duration(p.componentFlushNanos).Milliseconds(),
		"pack_encode_ms":                  time.Duration(p.packEncodeNanos).Milliseconds(),
		"frame_stage_ms":                  time.Duration(p.frameStageNanos).Milliseconds(),
		"snapshot_bytes":                  w.size,
		"snapshot_complete":               boolInt(p.snapshotComplete),
		"frame_bytes":                     int64(len(w.frame)),
		"evidence_occurrences":            p.evidenceOccurrences,
		"evidence_bytes":                  p.evidenceBytes,
		"unique_evidence_components_seen": int64(len(w.verified)),
		"unique_evidence_bytes":           p.uniqueEvidenceBytes,
		"pack_encoded_bytes":              p.packEncodedBytes,
		"evidence_batch_new_objects":      int64(w.batch.newObjects),
		"evidence_batch_reused_objects":   int64(w.batch.reusedObjects),
	}
}

func boolInt(value bool) int64 {
	if value {
		return 1
	}
	return 0
}

func (w *separatedInputWriter) Write(p []byte) (int, error) {
	// The canonical encoder writes each DNS selection as one JSON value.
	// Detach the leading typed audit fields without changing the encoder,
	// their field order, or a single signed byte. A retained-only observation
	// has no current physical_evidence. Unknown shapes stay literal.
	var prefix []byte
	if w.dnsArray && !w.quoted {
		if w.depth == 2 {
			for _, candidate := range []string{`{"physical_evidence":`, `{"retained_physical_evidence":`} {
				if bytes.HasPrefix(p, []byte(candidate)) {
					prefix = []byte(candidate)
					break
				}
			}
		} else if w.depth == 3 && bytes.HasPrefix(p, []byte(`,"retained_physical_evidence":`)) {
			prefix = []byte(`,"retained_physical_evidence":`)
		}
	}
	if len(prefix) > 0 && len(p) > len(prefix) {
		d := json.NewDecoder(bytes.NewReader(p[len(prefix):]))
		var evidence json.RawMessage
		if err := d.Decode(&evidence); err != nil {
			return 0, err
		}
		end := len(prefix) + int(d.InputOffset())
		if _, err := w.Write(p[:len(prefix)]); err != nil {
			return 0, err
		}
		if _, err := w.WriteRuntimeSnapshotEvidence(p[len(prefix):end]); err != nil {
			return 0, err
		}
		if _, err := w.Write(p[end:]); err != nil {
			return 0, err
		}
		return len(p), nil
	}
	if len(w.frame)+len(p) > separatedFrameLimit || w.size+int64(len(p)) > inputMaxBytes {
		return 0, errSharedInputBudget
	}
	if len(p) == 0 {
		return 0, nil
	}
	w.observeJSONShape(p)
	offset := uint32(len(w.frame))
	w.frame = append(w.frame, p...)
	if n := len(w.parts); n > 0 && !w.parts[n-1].Evidence && w.parts[n-1].Offset+w.parts[n-1].Size == offset {
		w.parts[n-1].Size += uint32(len(p))
	} else {
		w.parts = append(w.parts, separatedPart{Offset: offset, Size: uint32(len(p))})
	}
	if len(w.parts) > separatedPartLimit {
		return 0, errSharedInputBudget
	}
	w.size += int64(len(p))
	w.hash.Write(p)
	return len(p), nil
}

// Only the canonical top-level dns_selections array contains typed selection
// evidence. A configuration value with the same field name remains in frame.
// Follow the encoder's token boundaries, then track JSON depth across writes.
func (w *separatedInputWriter) observeJSONShape(p []byte) {
	switch w.dnsHeader {
	case 1:
		w.dnsHeader = 0
		if bytes.Equal(p, []byte(":")) {
			w.dnsHeader = 2
		}
	case 2:
		w.dnsHeader = 0
		if w.depth == 1 && bytes.Equal(p, []byte("[")) {
			w.dnsArray = true
		}
	default:
		if w.depth == 1 && !w.quoted && bytes.Equal(p, []byte(`"dns_selections"`)) {
			w.dnsHeader = 1
		}
	}
	for _, b := range p {
		if w.escaped {
			w.escaped = false
			continue
		}
		if w.quoted && b == '\\' {
			w.escaped = true
			continue
		}
		if b == '"' {
			w.quoted = !w.quoted
			continue
		}
		if w.quoted {
			continue
		}
		switch b {
		case '{', '[':
			w.depth++
		case '}', ']':
			if w.dnsArray && w.depth == 2 && b == ']' {
				w.dnsArray = false
			}
			w.depth--
		}
	}
}

func (w *separatedInputWriter) WriteRuntimeSnapshotEvidence(raw []byte) (int, error) {
	if w.profile != nil {
		started := time.Now()
		defer func() { w.profile.evidenceCallNanos += time.Since(started).Nanoseconds() }()
	}
	if w.size+int64(len(raw)) > inputMaxBytes || len(w.parts) >= separatedPartLimit || len(w.frame)+4 > separatedFrameLimit {
		return 0, errSharedInputBudget
	}
	digest := sharedHash(sha256.Sum256(raw))
	if w.profile != nil {
		w.profile.evidenceOccurrences++
		w.profile.evidenceBytes += int64(len(raw))
	}
	if !w.verified[digest] {
		if w.profile != nil {
			w.profile.uniqueEvidenceBytes += int64(len(raw))
		}
		if len(w.pendingEvidence) >= 16 || w.pendingBytes+len(raw) > 4<<20 {
			if err := w.flushEvidence(); err != nil {
				return 0, err
			}
		}
		if w.pendingEvidence == nil {
			w.pendingEvidence = map[string][]byte{}
		}
		// Retain bounded batches. An oversized single body is processed now,
		// without copying or keeping its encoder buffer across calls.
		if len(raw) > 4<<20 {
			w.pendingEvidence[digest] = raw
		} else {
			w.pendingEvidence[digest] = append([]byte(nil), raw...)
		}
		w.pendingBytes += len(raw)
		w.verified[digest] = true
		if len(raw) > 4<<20 {
			if err := w.flushEvidence(); err != nil {
				return 0, err
			}
		}
	}
	w.parts = append(w.parts, separatedPart{Evidence: true, Digest: digest, Size: uint32(len(raw))})
	w.frame = append(w.frame, "null"...)
	w.size += int64(len(raw))
	w.hash.Write(raw)
	return len(raw), nil
}

func (s *Store) pgWriteSeparatedInput(ctx context.Context, tx *sql.Tx, snapshot platformconfig.RuntimeSnapshot, digest string, previous *inputManifest) (packResult uint64, resultErr error) {
	profile := &separatedInputWriteProfile{}
	w := &separatedInputWriter{ctx: ctx, tx: tx, hash: sha256.New(), verified: map[string]bool{}, profile: profile}
	w.batch = &sharedInputWriter{components: true, ctx: ctx, tx: tx, encoder: inputChunkEncoder{level: zlib.DefaultCompression}, objects: map[[32]byte]sharedObject{}, seen: map[[32]byte]bool{}, profile: profile}
	w.batch.blockMinimum = s.compilerInputBlockMinimum()
	w.batch.boundaryBits = s.compilerInputBoundaryBits()
	started := time.Now()
	phase := started
	timings := map[string]int64{}
	phaseName := "encode_components"
	advance := func(next string) {
		now := time.Now()
		timings[phaseName] = now.Sub(phase).Milliseconds()
		phase, phaseName = now, next
	}
	defer func() {
		timings[phaseName] = time.Since(phase).Milliseconds()
		timings["total"] = time.Since(started).Milliseconds()
		separatedInputStats.mu.Lock()
		separatedInputStats.lastWriteTimings = timings
		separatedInputStats.lastWriteProfile = profile.aggregate(w)
		if resultErr != nil {
			separatedInputStats.lastFailureTimings = timings
			separatedInputStats.lastFailurePhase = phaseName
			separatedInputStats.lastFailureProfile = profile.aggregate(w)
		}
		separatedInputStats.mu.Unlock()
		if resultErr != nil {
			observeSeparatedFailure(resultErr)
			return
		}
		separatedInputStats.writes.Add(1)
		separatedInputStats.logical.Add(uint64(w.size))
		separatedInputStats.newPackBytes.Add(packResult)
	}()
	streamStarted := time.Now()
	err := platformconfig.WriteRuntimeSnapshot(w, snapshot)
	profile.snapshotStreamNanos += time.Since(streamStarted).Nanoseconds()
	if err != nil {
		return 0, err
	}
	profile.snapshotComplete = true
	if err := w.flushEvidence(); err != nil {
		return 0, err
	}
	advance("frame")
	if err := w.batch.flush(); err != nil {
		return 0, err
	}
	w.packBytes = w.batch.newPackBytes
	if sharedHashBytes(w.hash.Sum(nil)) != digest {
		return 0, ErrConflict
	}
	frameStarted := time.Now()
	frame, packBytes, err := ensureInputComponent(ctx, tx, w.frame, nil, profile)
	profile.frameStageNanos += time.Since(frameStarted).Nanoseconds()
	if err != nil {
		return 0, fmt.Errorf("separated frame: %w", err)
	}
	w.packBytes += packBytes
	advance("manifest")
	layout, err := encodeSeparatedLayout(w.parts)
	if err != nil {
		return 0, err
	}
	m := inputManifest{Digest: digest, Format: inputFormatSeparatedV4, Size: w.size, ChunkSize: inputChunkBytes, Count: int((w.size + inputChunkBytes - 1) / inputChunkBytes), Created: time.Now().UTC()}
	if previous == nil {
		_, err = tx.ExecContext(ctx, `INSERT INTO fugue_platform_input_manifests(content_hash,format,size_bytes,chunk_size,chunk_count,created_at) VALUES($1,$2,$3,$4,$5,$6)`, digest, m.Format, m.Size, m.ChunkSize, m.Count, m.Created)
	} else {
		m.Created = previous.Created
		var result sql.Result
		result, err = tx.ExecContext(ctx, `UPDATE fugue_platform_input_manifests SET format=$2,chunk_size=$3,chunk_count=$4 WHERE content_hash=$1 AND format=$5 AND size_bytes=$6 AND chunk_count=$7 AND chunk_size=$8 AND created_at=$9`, digest, m.Format, m.ChunkSize, m.Count, previous.Format, previous.Size, previous.Count, previous.ChunkSize, previous.Created)
		if err == nil {
			n, e := result.RowsAffected()
			err = e
			if n != 1 && err == nil {
				err = ErrConflict
			}
		}
	}
	if err != nil {
		return 0, err
	}
	lh := sha256.Sum256(layout)
	if _, err = tx.ExecContext(ctx, `INSERT INTO fugue_platform_input_separated_roots(content_hash,frame_hash,layout_hash,layout) VALUES($1,$2,$3,$4)`, digest, frame, lh[:], layout); err != nil {
		return 0, err
	}
	components := []string{frame}
	for component := range w.verified {
		if component != frame {
			components = append(components, component)
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO fugue_platform_input_separated_refs(content_hash,component_hash) SELECT $1,unnest($2::text[])`, digest, components); err != nil {
		return 0, err
	}
	advance("full_verify")
	r, err := openSeparatedInput(ctx, tx, m)
	if err != nil {
		return 0, fmt.Errorf("separated readback open: %w", err)
	}
	_, err = io.Copy(io.Discard, r)
	if err != nil {
		return 0, fmt.Errorf("separated readback body: %w", err)
	}
	return w.packBytes, err
}

type separatedInputReader struct {
	ctx        context.Context
	db         inputQueryer
	m          inputManifest
	frame      []byte
	parts      []separatedPart
	components map[string]inputManifest
	ordinal    int
	current    *sharedInputReader
	readers    map[string]*sharedInputReader
	packCache  *sharedReadPackCache
	pending    []byte
	hash       hash.Hash
	size       int64
	done       bool
	err        error
}

func openSeparatedInput(ctx context.Context, db inputQueryer, m inputManifest) (*separatedInputReader, error) {
	r := &separatedInputReader{ctx: ctx, db: db, m: m, hash: sha256.New(), components: map[string]inputManifest{}, readers: map[string]*sharedInputReader{}, packCache: &sharedReadPackCache{entries: map[int64]sharedReadPack{}}}
	var frame string
	var declared, layout []byte
	if err := db.QueryRowContext(ctx, `SELECT frame_hash,layout_hash,layout FROM fugue_platform_input_separated_roots WHERE content_hash=$1`, m.Digest).Scan(&frame, &declared, &layout); err != nil {
		return nil, err
	}
	h := sha256.Sum256(layout)
	if !bytes.Equal(declared, h[:]) {
		return nil, ErrConflict
	}
	parts, err := decodeSeparatedLayout(layout)
	if err != nil {
		return nil, err
	}
	r.parts = parts
	wanted := map[string]bool{frame: true}
	var total int64
	for _, p := range parts {
		total += int64(p.Size)
		if p.Evidence {
			wanted[p.Digest] = true
		}
	}
	if total != m.Size {
		return nil, fmt.Errorf("layout size: %w", ErrConflict)
	}
	rows, err := db.QueryContext(ctx, `SELECT c.content_hash,c.size_bytes,c.chunk_count,c.created_at FROM fugue_platform_input_separated_refs r JOIN fugue_platform_input_components c ON c.content_hash=r.component_hash WHERE r.content_hash=$1`, m.Digest)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		v := inputManifest{Format: inputFormatSharedV3, ChunkSize: inputChunkBytes}
		if err = rows.Scan(&v.Digest, &v.Size, &v.Count, &v.Created); err != nil {
			break
		}
		if !wanted[v.Digest] || v.Size < 1 || v.Size > inputMaxBytes || v.Count != int((v.Size+inputChunkBytes-1)/inputChunkBytes) {
			err = fmt.Errorf("component inventory: %w", ErrConflict)
			break
		}
		r.components[v.Digest] = v
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return nil, err
	}
	if len(wanted) != len(r.components) || r.components[frame].Size > separatedFrameLimit {
		return nil, fmt.Errorf("component roots: %w", ErrConflict)
	}
	fr, err := openSharedRepresentation(ctx, db, r.components[frame], true)
	if err != nil {
		return nil, fmt.Errorf("frame metadata: %w", err)
	}
	fr.packCache = r.packCache
	r.frame, err = io.ReadAll(io.LimitReader(fr, separatedFrameLimit+1))
	if err != nil {
		return nil, fmt.Errorf("frame read: %w", err)
	}
	if !fr.done || len(r.frame) > separatedFrameLimit {
		return nil, fmt.Errorf("frame content: %w", ErrConflict)
	}
	for _, p := range parts {
		if p.Evidence {
			if int64(p.Size) != r.components[p.Digest].Size {
				return nil, ErrConflict
			}
		} else if uint64(p.Offset)+uint64(p.Size) > uint64(len(r.frame)) {
			return nil, ErrConflict
		}
	}
	return r, nil
}

func (r *separatedInputReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.err != nil {
		return 0, r.err
	}
	if r.done {
		return 0, io.EOF
	}
	for len(r.pending) == 0 {
		if r.current != nil {
			n, err := r.current.Read(p)
			if n > 0 {
				r.size += int64(n)
				r.hash.Write(p[:n])
			}
			if err != nil && err != io.EOF {
				r.err = err
				return n, err
			}
			if err == io.EOF {
				r.current = nil
			} else {
				return n, nil
			}
			if n > 0 {
				return n, nil
			}
		}
		if r.ordinal == len(r.parts) {
			r.done = true
			if r.size != r.m.Size || sharedHashBytes(r.hash.Sum(nil)) != r.m.Digest {
				r.err = ErrConflict
				return 0, r.err
			}
			return 0, io.EOF
		}
		part := r.parts[r.ordinal]
		r.ordinal++
		if part.Evidence {
			var err error
			r.current, err = r.evidenceReader(part.Digest)
			if err != nil {
				r.err = err
				return 0, err
			}
		} else {
			r.pending = r.frame[int(part.Offset) : int(part.Offset)+int(part.Size)]
		}
	}
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	r.size += int64(n)
	r.hash.Write(p[:n])
	return n, nil
}
