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

func ensureInputComponent(ctx context.Context, tx *sql.Tx, raw []byte) (string, uint64, error) {
	digest := sharedHash(sha256.Sum256(raw))
	m, err := readComponentManifest(ctx, tx, digest)
	if err == nil {
		if m.Size != int64(len(raw)) {
			return "", 0, ErrConflict
		}
		r, e := openSharedRepresentation(ctx, tx, m, true)
		if e != nil {
			return "", 0, e
		}
		_, e = io.Copy(io.Discard, r)
		return digest, 0, e
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", 0, err
	}
	if len(raw) == 0 || len(raw) > inputMaxBytes {
		return "", 0, ErrInvalidInput
	}
	w := &sharedInputWriter{components: true, ctx: ctx, tx: tx, digest: digest, hash: sha256.New(), encoder: inputChunkEncoder{level: zlib.DefaultCompression}, objects: map[[32]byte]sharedObject{}, seen: map[[32]byte]bool{}}
	if _, err = w.Write(raw); err == nil {
		err = w.stage()
	}
	if err == nil {
		err = w.flush()
	}
	if err != nil {
		return "", 0, err
	}
	if sharedHashBytes(w.hash.Sum(nil)) != digest {
		return "", 0, ErrConflict
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO fugue_platform_input_components(content_hash,size_bytes,chunk_count,created_at) VALUES($1,$2,$3,now())`, digest, w.size, w.ordinal); err != nil {
		return "", 0, err
	}
	m, err = readComponentManifest(ctx, tx, digest)
	if err != nil {
		return "", 0, err
	}
	r, err := openSharedRepresentation(ctx, tx, m, true)
	if err != nil {
		return "", 0, err
	}
	_, err = io.Copy(io.Discard, r)
	return digest, w.newPackBytes, err
}

type separatedInputWriter struct {
	ctx       context.Context
	tx        *sql.Tx
	frame     []byte
	parts     []separatedPart
	hash      hash.Hash
	size      int64
	packBytes uint64
	verified  map[string]bool
	depth     int
	quoted    bool
	escaped   bool
	dnsHeader uint8
	dnsArray  bool
}

func (w *separatedInputWriter) Write(p []byte) (int, error) {
	// The canonical encoder writes each DNS selection as one JSON value.
	// Detach its first physical_evidence field without changing the encoder,
	// its field order, or a single signed byte. Unknown shapes stay literal.
	prefix := []byte(`{"physical_evidence":`)
	if w.dnsArray && w.depth == 2 && len(p) > len(prefix) && bytes.HasPrefix(p, prefix) {
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
	if w.size+int64(len(raw)) > inputMaxBytes || len(w.parts) >= separatedPartLimit || len(w.frame)+4 > separatedFrameLimit {
		return 0, errSharedInputBudget
	}
	digest := sharedHash(sha256.Sum256(raw))
	if !w.verified[digest] {
		_, n, err := ensureInputComponent(w.ctx, w.tx, raw)
		if err != nil {
			return 0, err
		}
		w.packBytes += n
		w.verified[digest] = true
	}
	w.parts = append(w.parts, separatedPart{Evidence: true, Digest: digest, Size: uint32(len(raw))})
	w.frame = append(w.frame, "null"...)
	w.size += int64(len(raw))
	w.hash.Write(raw)
	return len(raw), nil
}

func (s *Store) pgWriteSeparatedInput(ctx context.Context, tx *sql.Tx, snapshot platformconfig.RuntimeSnapshot, digest string, previous *inputManifest) (packResult uint64, resultErr error) {
	w := &separatedInputWriter{ctx: ctx, tx: tx, hash: sha256.New(), verified: map[string]bool{}}
	defer func() {
		if resultErr != nil {
			observeSeparatedFailure(resultErr)
			return
		}
		separatedInputStats.writes.Add(1)
		separatedInputStats.logical.Add(uint64(w.size))
		separatedInputStats.newPackBytes.Add(packResult)
	}()
	if err := platformconfig.WriteRuntimeSnapshot(w, snapshot); err != nil {
		return 0, err
	}
	if sharedHashBytes(w.hash.Sum(nil)) != digest {
		return 0, ErrConflict
	}
	frame, packBytes, err := ensureInputComponent(ctx, tx, w.frame)
	if err != nil {
		return 0, err
	}
	w.packBytes += packBytes
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
	r, err := openSeparatedInput(ctx, tx, m)
	if err != nil {
		return 0, err
	}
	_, err = io.Copy(io.Discard, r)
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
	pending    []byte
	hash       hash.Hash
	size       int64
	done       bool
	err        error
}

func openSeparatedInput(ctx context.Context, db inputQueryer, m inputManifest) (*separatedInputReader, error) {
	r := &separatedInputReader{ctx: ctx, db: db, m: m, hash: sha256.New(), components: map[string]inputManifest{}}
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
		return nil, ErrConflict
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
			err = ErrConflict
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
		return nil, ErrConflict
	}
	fr, err := openSharedRepresentation(ctx, db, r.components[frame], true)
	if err != nil {
		return nil, err
	}
	r.frame, err = io.ReadAll(io.LimitReader(fr, separatedFrameLimit+1))
	if err != nil || !fr.done || len(r.frame) > separatedFrameLimit {
		return nil, ErrConflict
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
			r.current, err = openSharedRepresentation(r.ctx, r.db, r.components[part.Digest], true)
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
