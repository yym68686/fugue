package store

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"hash"
	"io"
	"time"
)

const CompilerInputBackupSchema = "fugue.compiler-input-encoded-backup/v1"
const inputBackupRecordLimit = 8 << 20
const inputBackupStreamLimit = 32 << 20

// A backup is an exact input selection, not a publication, restore ticket,
// retention classification, pin or retirement plan. The expected digest must
// come from the independently authenticated original source.
type CompilerInputBackupContract struct {
	Schema               string `json:"schema"`
	Digest               string `json:"digest"`
	ExpectedLogicalBytes int64  `json:"expected_logical_bytes"`
	MaxEncodedBytes      int64  `json:"max_encoded_bytes"`
}

func (c CompilerInputBackupContract) Validate() error {
	if c.Schema != CompilerInputBackupSchema || !inputDigestPattern.MatchString(c.Digest) || c.ExpectedLogicalBytes < 1 || c.ExpectedLogicalBytes > 128<<20 || c.MaxEncodedBytes < 1 || c.MaxEncodedBytes > inputBackupStreamLimit {
		return ErrInvalidInput
	}
	return nil
}

type CompilerInputBackupReport struct {
	Schema                     string    `json:"schema"`
	Digest                     string    `json:"digest"`
	ReadOnly                   bool      `json:"read_only"`
	Snapshot                   string    `json:"database_snapshot,omitempty"`
	ObservedAt                 time.Time `json:"observed_at,omitempty"`
	EncodedBytes               int64     `json:"encoded_bytes"`
	LogicalBytes               int64     `json:"logical_bytes"`
	Components                 int       `json:"components"`
	Packs                      int       `json:"packs"`
	Complete                   bool      `json:"complete"`
	FullOriginalSHA256Verified bool      `json:"full_original_sha256_verified"`
}

type inputBackupHeader struct {
	Schema       string    `json:"schema"`
	Digest       string    `json:"digest"`
	LogicalBytes int64     `json:"logical_bytes"`
	CreatedAt    time.Time `json:"created_at"`
	FrameDigest  string    `json:"frame_digest"`
	LayoutHash   string    `json:"layout_hash"`
	Layout       []byte    `json:"layout"`
}

type inputBackupPackWitness struct {
	ID         int64  `json:"id"`
	Hash       string `json:"hash"`
	StoredHash string `json:"stored_hash"`
	Size       int    `json:"size"`
	StoredSize int    `json:"stored_size"`
}

type inputBackupObject struct {
	ID     int64  `json:"id"`
	Hash   string `json:"hash"`
	PackID int64  `json:"pack_id"`
	Size   int    `json:"size"`
	Offset int    `json:"offset"`
}

type inputBackupComponent struct {
	Digest    string                   `json:"digest"`
	Size      int64                    `json:"size"`
	CreatedAt time.Time                `json:"created_at"`
	Recipes   [][]byte                 `json:"recipes"`
	Objects   []inputBackupObject      `json:"objects"`
	Packs     []inputBackupPackWitness `json:"packs"`
}

type inputBackupPack struct {
	ID   int64  `json:"id"`
	Data []byte `json:"data"`
}

type inputBackupEnd struct {
	Records int    `json:"records"`
	Bytes   int64  `json:"bytes"`
	SHA256  string `json:"sha256"`
}

type inputBackupRecord struct {
	Kind      string                `json:"kind"`
	Header    *inputBackupHeader    `json:"header,omitempty"`
	Component *inputBackupComponent `json:"component,omitempty"`
	Pack      *inputBackupPack      `json:"pack,omitempty"`
	End       *inputBackupEnd       `json:"end,omitempty"`
}

type inputBackupEncoder struct {
	ctx     context.Context
	out     io.Writer
	max     int64
	bytes   int64
	records int
	hash    hash.Hash
}

func (w *inputBackupEncoder) record(record inputBackupRecord) error {
	if err := w.ctx.Err(); err != nil {
		return err
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if len(raw) > inputBackupRecordLimit || int64(len(raw)) > w.max-w.bytes {
		return errSharedInputBudget
	}
	// A slow receiver must not hold the source reader barrier indefinitely.
	// At most one write can remain pending on cancellation. It has no DB
	// access and cannot emit an end record; callers discard/close failed streams.
	type writeResult struct {
		n   int
		err error
	}
	done := make(chan writeResult, 1)
	go func() {
		n, err := w.out.Write(raw)
		done <- writeResult{n, err}
	}()
	var n int
	select {
	case <-w.ctx.Done():
		return w.ctx.Err()
	case result := <-done:
		n, err = result.n, result.err
	}
	w.bytes += int64(n)
	if err != nil {
		return err
	}
	if n != len(raw) {
		return io.ErrShortWrite
	}
	if record.Kind != "end" {
		w.records++
		_, _ = w.hash.Write(raw)
	}
	return nil
}

func (w *inputBackupEncoder) finish() error {
	return w.record(inputBackupRecord{Kind: "end", End: &inputBackupEnd{Records: w.records, Bytes: w.bytes, SHA256: sharedHashBytes(w.hash.Sum(nil))}})
}

type inputBackupGraph struct {
	header     inputBackupHeader
	parts      []separatedPart
	components map[string]*inputBackupComponent
	objects    map[string]map[int64]inputBackupObject
	allObjects map[int64]inputBackupObject
	witnesses  map[int64]inputBackupPackWitness
	packs      map[int64][]byte
	cache      map[int64][]byte
	order      []int64
	cacheBytes int
}

// DecodeCompilerInputBackup reconstructs only the original canonical bytes.
// It never opens a DB, installs a pin, publishes, or substitutes this receipt
// for source signatures. Output is untrusted until the complete digest passes;
// callers must discard partial output on any error.
func DecodeCompilerInputBackup(ctx context.Context, in io.Reader, c CompilerInputBackupContract, out io.Writer) (CompilerInputBackupReport, error) {
	r := CompilerInputBackupReport{Schema: CompilerInputBackupSchema, Digest: c.Digest, ReadOnly: true}
	if c.Validate() != nil || in == nil || out == nil {
		return r, ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	g, n, err := readInputBackup(ctx, in, c)
	r.EncodedBytes = n
	if err != nil {
		return r, err
	}
	frame := bytes.NewBuffer(make([]byte, 0, int(g.components[g.header.FrameDigest].Size)))
	if err = g.component(ctx, g.header.FrameDigest, frame); err != nil {
		return r, err
	}
	h := sha256.New()
	count := &inputBackupOutput{ctx: ctx, out: io.MultiWriter(out, h), max: c.ExpectedLogicalBytes}
	for _, p := range g.parts {
		if p.Evidence {
			err = g.component(ctx, p.Digest, count)
		} else if uint64(p.Offset)+uint64(p.Size) > uint64(frame.Len()) {
			err = ErrConflict
		} else {
			_, err = count.Write(frame.Bytes()[int(p.Offset) : int(p.Offset)+int(p.Size)])
		}
		if err != nil {
			return r, err
		}
	}
	if count.bytes != c.ExpectedLogicalBytes || sharedHashBytes(h.Sum(nil)) != c.Digest {
		return r, ErrConflict
	}
	r.LogicalBytes, r.Components, r.Packs = count.bytes, len(g.components), len(g.packs)
	r.Complete, r.FullOriginalSHA256Verified = true, true
	return r, nil
}

type inputBackupOutput struct {
	ctx   context.Context
	out   io.Writer
	max   int64
	bytes int64
}

func (w *inputBackupOutput) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	if int64(len(p)) > w.max-w.bytes {
		return 0, errSharedInputBudget
	}
	n, err := w.out.Write(p)
	w.bytes += int64(n)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	return n, err
}

func readInputBackup(ctx context.Context, in io.Reader, c CompilerInputBackupContract) (*inputBackupGraph, int64, error) {
	g := &inputBackupGraph{components: map[string]*inputBackupComponent{}, objects: map[string]map[int64]inputBackupObject{}, allObjects: map[int64]inputBackupObject{}, witnesses: map[int64]inputBackupPackWitness{}, packs: map[int64][]byte{}, cache: map[int64][]byte{}}
	scanner := bufio.NewScanner(io.LimitReader(in, c.MaxEncodedBytes+1))
	scanner.Buffer(make([]byte, 64<<10), inputBackupRecordLimit+1)
	scanner.Split(func(data []byte, atEOF bool) (int, []byte, error) {
		if i := bytes.IndexByte(data, '\n'); i >= 0 {
			return i + 1, data[:i], nil
		}
		if atEOF && len(data) > 0 {
			return 0, nil, io.ErrUnexpectedEOF
		}
		return 0, nil, nil
	})
	h := sha256.New()
	var total int64
	records, ended := 0, false
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return nil, total, err
		}
		raw := scanner.Bytes()
		total += int64(len(raw) + 1)
		if len(raw)+1 > inputBackupRecordLimit || total > c.MaxEncodedBytes || ended {
			return nil, total, ErrConflict
		}
		var record inputBackupRecord
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&record) != nil || decoder.Decode(new(any)) != io.EOF {
			return nil, total, ErrConflict
		}
		// Our wire format is canonical JSONL. Comparing the canonical encoding
		// also rejects duplicate keys, null payloads and alternate spellings.
		canonical, err := json.Marshal(record)
		if err != nil || !bytes.Equal(canonical, raw) {
			return nil, total, ErrConflict
		}
		fields := 0
		for _, present := range []bool{record.Header != nil, record.Component != nil, record.Pack != nil, record.End != nil} {
			if present {
				fields++
			}
		}
		if fields != 1 {
			return nil, total, ErrConflict
		}
		switch {
		case record.Kind == "header" && record.Header != nil && records == 0:
			g.header = *record.Header
			hdr := g.header
			if hdr.Schema != c.Schema || hdr.Digest != c.Digest || hdr.LogicalBytes != c.ExpectedLogicalBytes || hdr.CreatedAt.IsZero() || !inputDigestPattern.MatchString(hdr.FrameDigest) || hdr.LayoutHash != sharedHash(sha256.Sum256(hdr.Layout)) {
				return nil, total, ErrConflict
			}
			var err error
			g.parts, err = decodeSeparatedLayout(hdr.Layout)
			if err != nil {
				return nil, total, err
			}
		case record.Kind == "component" && record.Component != nil && records > 0 && len(g.packs) == 0:
			v := record.Component
			if g.components[v.Digest] != nil || len(g.components) >= separatedPartLimit+1 {
				return nil, total, ErrConflict
			}
			if err := g.addComponent(v); err != nil {
				return nil, total, err
			}
		case record.Kind == "pack" && record.Pack != nil && len(g.components) > 0:
			v := record.Pack
			w, ok := g.witnesses[v.ID]
			if !ok || g.packs[v.ID] != nil || len(v.Data) != w.StoredSize || sharedHash(sha256.Sum256(v.Data)) != w.StoredHash {
				return nil, total, ErrConflict
			}
			g.packs[v.ID] = v.Data
		case record.Kind == "end" && record.End != nil && records > 0:
			v := record.End
			if v.Records != records || v.Bytes != total-int64(len(raw)+1) || v.SHA256 != sharedHashBytes(h.Sum(nil)) {
				return nil, total, ErrConflict
			}
			ended = true
		default:
			return nil, total, ErrConflict
		}
		if !ended {
			_, _ = h.Write(raw)
			_, _ = h.Write([]byte{'\n'})
			records++
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, total, err
	}
	if !ended || len(g.packs) != len(g.witnesses) || len(g.packs) == 0 {
		return nil, total, ErrConflict
	}
	wanted := map[string]bool{g.header.FrameDigest: true}
	var size int64
	for _, part := range g.parts {
		size += int64(part.Size)
		if part.Evidence {
			wanted[part.Digest] = true
			if g.components[part.Digest] == nil || g.components[part.Digest].Size != int64(part.Size) {
				return nil, total, ErrConflict
			}
		}
	}
	frame := g.components[g.header.FrameDigest]
	if size != c.ExpectedLogicalBytes || len(wanted) != len(g.components) || frame == nil || frame.Size > separatedFrameLimit {
		return nil, total, ErrConflict
	}
	return g, total, nil
}

func (g *inputBackupGraph) addComponent(v *inputBackupComponent) error {
	if !inputDigestPattern.MatchString(v.Digest) || v.CreatedAt.IsZero() || v.Size < 1 || v.Size > 128<<20 || len(v.Recipes) != int((v.Size+inputChunkBytes-1)/inputChunkBytes) || len(v.Objects) == 0 || len(v.Objects) > sharedObjectLimit {
		return ErrConflict
	}
	objects := map[int64]inputBackupObject{}
	for _, o := range v.Objects {
		if o.ID < 1 || !inputDigestPattern.MatchString(o.Hash) || o.PackID < 1 || o.Size < 1 || o.Size > sharedBlockMax || o.Offset < 0 || objects[o.ID].ID != 0 {
			return ErrConflict
		}
		objects[o.ID] = o
		if prev, ok := g.allObjects[o.ID]; ok && prev != o {
			return ErrConflict
		}
		g.allObjects[o.ID] = o
		if len(g.allObjects) > sharedObjectLimit {
			return errSharedInputBudget
		}
	}
	witnesses := map[int64]inputBackupPackWitness{}
	for _, w := range v.Packs {
		if w.ID < 1 || !inputDigestPattern.MatchString(w.Hash) || !inputDigestPattern.MatchString(w.StoredHash) || w.Size < 1 || w.Size > inputChunkBytes-inputCodecHeaderBytes || w.StoredSize < inputCodecHeaderBytes || w.StoredSize > inputChunkBytes || witnesses[w.ID].ID != 0 {
			return ErrConflict
		}
		if existing, ok := g.witnesses[w.ID]; ok && existing != w {
			return ErrConflict
		}
		g.witnesses[w.ID], witnesses[w.ID] = w, w
		if len(g.witnesses) > sharedObjectLimit {
			return errSharedInputBudget
		}
	}
	usedObjects, usedPacks := map[int64]bool{}, map[int64]bool{}
	var recipeBytes int
	for ordinal, raw := range v.Recipes {
		recipeBytes += len(raw)
		if recipeBytes > sharedRecipeBytesLimit {
			return errSharedInputBudget
		}
		refs, err := decodeSharedRecipe(raw, min(inputChunkBytes, int(v.Size-int64(ordinal)*inputChunkBytes)))
		if err != nil {
			return err
		}
		for _, ref := range refs {
			o, ok := objects[ref.ID]
			w, exists := witnesses[o.PackID]
			if !ok || !exists || o.Size != ref.Size || uint64(o.Offset)+uint64(o.Size) > uint64(w.Size) {
				return ErrConflict
			}
			usedObjects[o.ID], usedPacks[o.PackID] = true, true
		}
	}
	if len(usedObjects) != len(objects) || len(usedPacks) != len(witnesses) {
		return ErrConflict
	}
	g.components[v.Digest], g.objects[v.Digest] = v, objects
	return nil
}

func (g *inputBackupGraph) component(ctx context.Context, digest string, out io.Writer) error {
	v := g.components[digest]
	if v == nil {
		return ErrConflict
	}
	h := sha256.New()
	w := &inputBackupOutput{ctx: ctx, out: io.MultiWriter(out, h), max: v.Size}
	for ordinal, recipe := range v.Recipes {
		refs, err := decodeSharedRecipe(recipe, min(inputChunkBytes, int(v.Size-int64(ordinal)*inputChunkBytes)))
		if err != nil {
			return err
		}
		for _, ref := range refs {
			o := g.objects[digest][ref.ID]
			raw := g.cache[o.PackID]
			if raw == nil {
				p := g.witnesses[o.PackID]
				raw, err = decodeInputChunk(inputFormatCodecV2, g.packs[o.PackID], p.Size)
				if err != nil || sharedHash(sha256.Sum256(raw)) != p.Hash {
					return ErrConflict
				}
				for g.cacheBytes+len(raw) > 4<<20 && len(g.order) > 0 {
					id := g.order[0]
					g.order = g.order[1:]
					g.cacheBytes -= len(g.cache[id])
					delete(g.cache, id)
				}
				g.cache[o.PackID], g.order, g.cacheBytes = raw, append(g.order, o.PackID), g.cacheBytes+len(raw)
			}
			body := raw[o.Offset : o.Offset+o.Size]
			if sharedHash(sha256.Sum256(body)) != o.Hash {
				return ErrConflict
			}
			if _, err = w.Write(body); err != nil {
				return err
			}
		}
	}
	if w.bytes != v.Size || sharedHashBytes(h.Sum(nil)) != digest {
		return ErrConflict
	}
	return nil
}
