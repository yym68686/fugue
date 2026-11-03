package store

import (
	"compress/zlib"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"strings"
	"time"

	"fugue/internal/platformconfig"
)

func sharedHash(h [32]byte) string      { return "sha256:" + hex.EncodeToString(h[:]) }
func sharedHashBytes(raw []byte) string { return "sha256:" + hex.EncodeToString(raw) }

const sharedObjectLimit = 131072
const sharedRecipeBytesLimit = 8 << 20

var errSharedInputBudget = errors.New("shared compiler input metadata exceeds budget")

type sharedLocation struct {
	Hash         [32]byte
	PackID       int64
	Size, Offset int
}
type sharedPackWitness struct {
	Hash, StoredHash [32]byte
	Size, StoredSize int
}

// A single read transaction may reuse a physical pack across components.
// Identity includes both hashes and lengths; this is never a process cache.
type sharedReadPack struct {
	witness sharedPackWitness
	data    []byte
}
type sharedReadPackCache struct {
	entries map[int64]sharedReadPack
	order   []int64
}
type sharedInputReader struct {
	ctx        context.Context
	db         inputQueryer
	m          inputManifest
	recipes    [][]byte
	locations  map[int64]sharedLocation
	packs      map[int64]sharedPackWitness
	cache      map[int64][]byte
	order      []int64
	ordinal    int
	pending    []byte
	hash       hash.Hash
	size       int64
	done       bool
	err        error
	started    time.Time
	recorded   bool
	components bool
	packCache  *sharedReadPackCache
}

func openSharedInput(ctx context.Context, db inputQueryer, m inputManifest) (*sharedInputReader, error) {
	return openSharedRepresentation(ctx, db, m, false)
}

func sharedRepresentationTables(components bool) (string, string) {
	if components {
		return "fugue_platform_input_component_recipes", "fugue_platform_input_component_refs"
	}
	return "fugue_platform_input_shared_recipes", "fugue_platform_input_shared_refs"
}

func openSharedRepresentation(ctx context.Context, db inputQueryer, m inputManifest, components bool) (*sharedInputReader, error) {
	r := &sharedInputReader{ctx: ctx, db: db, m: m, components: components, hash: sha256.New(), locations: map[int64]sharedLocation{}, packs: map[int64]sharedPackWitness{}, cache: map[int64][]byte{}, started: time.Now()}
	recipes, refs := sharedRepresentationTables(components)
	rows, err := db.QueryContext(ctx, `SELECT ordinal,recipe_hash,recipe FROM `+recipes+` WHERE content_hash=$1 ORDER BY ordinal`, m.Digest)
	if err != nil {
		return nil, err
	}
	referenced := map[int64]int{}
	recipeBytes := 0
	for rows.Next() {
		var ordinal int
		var declared string
		var raw []byte
		if err = rows.Scan(&ordinal, &declared, &raw); err != nil {
			break
		}
		if ordinal != len(r.recipes) || ordinal >= m.Count || sharedHash(sha256.Sum256(raw)) != declared {
			err = ErrConflict
			break
		}
		recipeBytes += len(raw)
		if recipeBytes > sharedRecipeBytesLimit {
			err = ErrConflict
			break
		}
		want := min(inputChunkBytes, int(m.Size-int64(ordinal)*inputChunkBytes))
		refs, e := decodeSharedRecipe(raw, want)
		if e != nil {
			err = e
			break
		}
		for _, ref := range refs {
			if n, ok := referenced[ref.ID]; ok && n != ref.Size {
				err = ErrConflict
				break
			}
			referenced[ref.ID] = ref.Size
			if len(referenced) > sharedObjectLimit {
				err = ErrConflict
				break
			}
		}
		if err != nil {
			break
		}
		r.recipes = append(r.recipes, raw)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return nil, err
	}
	if len(r.recipes) != m.Count {
		return nil, ErrConflict
	}
	rows, err = db.QueryContext(ctx, `SELECT p.pack_id,p.pack_hash,p.size_bytes,p.stored_hash,octet_length(p.data) FROM `+refs+` r JOIN fugue_platform_input_shared_packs p USING(pack_id) WHERE content_hash=$1`, m.Digest)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id int64
		var h, sh []byte
		var w sharedPackWitness
		if err = rows.Scan(&id, &h, &w.Size, &sh, &w.StoredSize); err != nil {
			break
		}
		if id < 1 || len(h) != 32 || len(sh) != 32 || w.Size < 1 || w.Size > inputChunkBytes-inputCodecHeaderBytes || w.StoredSize < 5 || w.StoredSize > inputChunkBytes {
			err = ErrConflict
			break
		}
		copy(w.Hash[:], h)
		copy(w.StoredHash[:], sh)
		r.packs[id] = w
		if len(r.packs) > len(referenced) {
			err = ErrConflict
			break
		}
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return nil, err
	}
	if len(r.packs) < 1 || len(r.packs) > len(referenced) {
		return nil, ErrConflict
	}
	ids := make([]int64, 0, len(referenced))
	for id := range referenced {
		ids = append(ids, id)
	}
	used := map[int64]bool{}
	for start := 0; start < len(ids); start += 1024 {
		rows, err = db.QueryContext(ctx, `SELECT object_id,object_hash,size_bytes,pack_id,pack_offset FROM fugue_platform_input_shared_objects WHERE object_id=ANY($1::bigint[])`, ids[start:min(start+1024, len(ids))])
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id int64
			var h []byte
			var l sharedLocation
			if err = rows.Scan(&id, &h, &l.Size, &l.PackID, &l.Offset); err != nil {
				break
			}
			w, ok := r.packs[l.PackID]
			if !ok || len(h) != 32 || referenced[id] != l.Size || l.Offset < 0 || l.Offset+l.Size > w.Size {
				err = ErrConflict
				break
			}
			copy(l.Hash[:], h)
			r.locations[id] = l
			used[l.PackID] = true
		}
		if err == nil {
			err = rows.Err()
		}
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	if len(r.locations) != len(referenced) || len(used) != len(r.packs) {
		return nil, ErrConflict
	}
	return r, nil
}

func (r *sharedInputReader) pack(id int64) ([]byte, error) {
	w, ok := r.packs[id]
	if !ok {
		return nil, ErrConflict
	}
	if r.packCache != nil {
		if cached, ok := r.packCache.entries[id]; ok && cached.witness == w {
			return cached.data, nil
		}
	} else if data, ok := r.cache[id]; ok {
		return data, nil
	}
	if err := r.prefetchPacks(id); err != nil {
		return nil, err
	}
	if r.packCache != nil {
		return r.packCache.entries[id].data, nil
	}
	return r.cache[id], nil
}

func (r *sharedInputReader) cachePack(id int64, w sharedPackWitness, raw []byte) {
	if c := r.packCache; c != nil {
		if _, ok := c.entries[id]; !ok {
			if len(c.order) == 16 {
				delete(c.entries, c.order[0])
				c.order = c.order[1:]
			}
			c.order = append(c.order, id)
		}
		c.entries[id] = sharedReadPack{witness: w, data: raw}
		return
	}
	// Decoded cache is per read, bounded to four MiB, never a verification
	// result reused across calls. It avoids decompressing a repeated pack.
	if len(r.order) == 16 {
		delete(r.cache, r.order[0])
		r.order = r.order[1:]
	}
	r.order = append(r.order, id)
	r.cache[id] = raw
}
func (r *sharedInputReader) Read(p []byte) (n int, result error) {
	defer func() {
		if result != nil && !r.recorded {
			r.recorded = true
			sharedInputStats.reads.Add(1)
			sharedInputStats.readNanos.Add(uint64(time.Since(r.started)))
			if result != io.EOF {
				sharedInputStats.readFailures.Add(1)
			}
		}
	}()
	if len(p) == 0 {
		return 0, nil
	}
	if r.err != nil {
		return 0, r.err
	}
	if r.done {
		return 0, io.EOF
	}
	if len(r.pending) == 0 {
		if r.ordinal == r.m.Count {
			r.done = true
			if r.size != r.m.Size || sharedHashBytes(r.hash.Sum(nil)) != r.m.Digest {
				r.err = fmt.Errorf("shared component digest: %w", ErrConflict)
				return 0, r.err
			}
			return 0, io.EOF
		}
		want := min(inputChunkBytes, int(r.m.Size-int64(r.ordinal)*inputChunkBytes))
		refs, err := decodeSharedRecipe(r.recipes[r.ordinal], want)
		if err != nil {
			r.err = err
			return 0, err
		}
		r.pending = make([]byte, 0, want)
		for _, ref := range refs {
			l, ok := r.locations[ref.ID]
			if !ok || l.Size != ref.Size {
				r.err = ErrConflict
				return 0, r.err
			}
			pack, err := r.pack(l.PackID)
			if err != nil {
				r.err = err
				return 0, err
			}
			raw := pack[l.Offset : l.Offset+l.Size]
			if sha256.Sum256(raw) != l.Hash {
				r.err = fmt.Errorf("shared fact hash: %w", ErrConflict)
				return 0, r.err
			}
			r.pending = append(r.pending, raw...)
		}
		r.ordinal++
		r.size += int64(len(r.pending))
		r.hash.Write(r.pending)
	}
	n = copy(p, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}

type sharedInputWriter struct {
	components                              bool
	ctx                                     context.Context
	tx                                      *sql.Tx
	digest                                  string
	chunk                                   []byte
	ordinal                                 int
	size                                    int64
	hash                                    hash.Hash
	encoder                                 inputChunkEncoder
	splitter                                sharedSplitter
	objects                                 map[[32]byte]sharedObject
	order                                   [][32]byte
	recipes                                 [][]sharedRef
	recipeOwners                            []string
	recipeOrdinals                          []int
	lengths                                 []int
	seen                                    map[[32]byte]bool
	recipeBytes                             int
	newPackBytes, newObjects, reusedObjects uint64
}

func (w *sharedInputWriter) Write(p []byte) (int, error) {
	if w.size+int64(len(p)) > inputMaxBytes {
		return 0, ErrInvalidInput
	}
	total := len(p)
	for len(p) > 0 {
		n := min(inputChunkBytes-len(w.chunk), len(p))
		w.chunk = append(w.chunk, p[:n]...)
		w.hash.Write(p[:n])
		w.size += int64(n)
		p = p[n:]
		if len(w.chunk) == inputChunkBytes {
			if err := w.stage(); err != nil {
				return total - len(p), err
			}
		}
	}
	return total, nil
}
func (w *sharedInputWriter) stage() error {
	if len(w.chunk) == 0 {
		return nil
	}
	var refs []sharedRef
	err := w.splitter.blocks(w.chunk, func(raw []byte) error {
		ref := sharedRef{Hash: sha256.Sum256(raw), Size: len(raw)}
		w.seen[ref.Hash] = true
		if len(w.seen) > sharedObjectLimit {
			return errSharedInputBudget
		}
		refs = append(refs, ref)
		if _, ok := w.objects[ref.Hash]; !ok {
			w.objects[ref.Hash] = sharedObject{Ref: ref, Data: append([]byte(nil), raw...)}
			w.order = append(w.order, ref.Hash)
		}
		return nil
	})
	if err != nil {
		return err
	}
	w.recipeBytes += 5 + 12*len(refs)
	if w.recipeBytes > sharedRecipeBytesLimit {
		return errSharedInputBudget
	}
	w.recipes = append(w.recipes, refs)
	w.recipeOwners = append(w.recipeOwners, w.digest)
	w.recipeOrdinals = append(w.recipeOrdinals, w.ordinal)
	w.lengths = append(w.lengths, len(w.chunk))
	w.ordinal++
	w.chunk = w.chunk[:0]
	if len(w.recipes) == 16 {
		return w.flush()
	}
	return nil
}
func (w *sharedInputWriter) flush() error {
	if len(w.recipes) == 0 {
		return nil
	}
	keys := make([][]byte, 0, len(w.order))
	for _, h := range w.order {
		keys = append(keys, h[:])
	}
	rows, err := w.tx.QueryContext(w.ctx, `SELECT object_hash,object_id FROM fugue_platform_input_shared_objects WHERE object_hash=ANY($1::bytea[])`, keys)
	if err != nil {
		return err
	}
	ids := map[[32]byte]int64{}
	for rows.Next() {
		var h []byte
		var id int64
		if err = rows.Scan(&h, &id); err != nil {
			break
		}
		if len(h) != 32 || id < 1 {
			err = ErrConflict
			break
		}
		var key [32]byte
		copy(key[:], h)
		ids[key] = id
		w.reusedObjects++
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return err
	}
	var pack []byte
	var hashes [][]byte
	var sizes, offsets []int32
	var packHashes, storedHashes, bodies [][]byte
	var packSizes []int32
	var objectPacks []int
	flushPack := func() error {
		if len(pack) == 0 {
			return nil
		}
		encoded, e := w.encoder.encode(pack)
		if e != nil {
			return e
		}
		h, sh := sha256.Sum256(pack), sha256.Sum256(encoded)
		w.newPackBytes += uint64(len(encoded))
		packHashes = append(packHashes, h[:])
		storedHashes = append(storedHashes, sh[:])
		packSizes = append(packSizes, int32(len(pack)))
		// The reusable encoder owns encoded until its next call.
		bodies = append(bodies, append([]byte(nil), encoded...))
		pack = nil
		return nil
	}
	for _, h := range w.order {
		if ids[h] != 0 {
			continue
		}
		obj := w.objects[h]
		if len(pack)+len(obj.Data) > inputChunkBytes-inputCodecHeaderBytes {
			if err = flushPack(); err != nil {
				return err
			}
		}
		hashes = append(hashes, h[:])
		w.newObjects++
		sizes = append(sizes, int32(obj.Ref.Size))
		offsets = append(offsets, int32(len(pack)))
		objectPacks = append(objectPacks, len(packHashes))
		pack = append(pack, obj.Data...)
	}
	if err = flushPack(); err != nil {
		return err
	}
	// A flush holds at most sixteen 256KiB recipes. Persist their new packs
	// and object placements in three bounded queries, rather than three per
	// pack. Original pack boundaries, hashes and final readback are unchanged.
	if len(packHashes) > 0 {
		if _, err = w.tx.ExecContext(w.ctx, `INSERT INTO fugue_platform_input_shared_packs(pack_hash,size_bytes,stored_hash,data,created_at) SELECT h,n,s,d,now() FROM unnest($1::bytea[],$2::int[],$3::bytea[],$4::bytea[]) AS v(h,n,s,d) ON CONFLICT DO NOTHING`, packHashes, packSizes, storedHashes, bodies); err != nil {
			return err
		}
		rows, err = w.tx.QueryContext(w.ctx, `SELECT pack_hash,pack_id FROM fugue_platform_input_shared_packs WHERE pack_hash=ANY($1::bytea[])`, packHashes)
		if err != nil {
			return err
		}
		packIDs := map[[32]byte]int64{}
		for rows.Next() {
			var h []byte
			var id int64
			if err = rows.Scan(&h, &id); err != nil {
				break
			}
			if len(h) != 32 || id < 1 {
				err = ErrConflict
				break
			}
			var key [32]byte
			copy(key[:], h)
			packIDs[key] = id
		}
		if err == nil {
			err = rows.Err()
		}
		rows.Close()
		if err != nil {
			return err
		}
		placements := make([]int64, len(objectPacks))
		for i, ordinal := range objectPacks {
			var key [32]byte
			copy(key[:], packHashes[ordinal])
			placements[i] = packIDs[key]
			if placements[i] < 1 {
				return ErrConflict
			}
		}
		rows, err = w.tx.QueryContext(w.ctx, `INSERT INTO fugue_platform_input_shared_objects(object_hash,size_bytes,pack_id,pack_offset) SELECT h,n,p,o FROM unnest($1::bytea[],$2::int[],$3::bigint[],$4::int[]) AS v(h,n,p,o) RETURNING object_hash,object_id`, hashes, sizes, placements, offsets)
		if err != nil {
			return err
		}
		inserted := 0
		for rows.Next() {
			var h []byte
			var id int64
			if err = rows.Scan(&h, &id); err != nil {
				break
			}
			if len(h) != 32 || id < 1 {
				err = ErrConflict
				break
			}
			var key [32]byte
			copy(key[:], h)
			ids[key] = id
			inserted++
		}
		if err == nil {
			err = rows.Err()
		}
		rows.Close()
		if err != nil {
			return err
		}
		if inserted != len(hashes) {
			return ErrConflict
		}
	}
	objectIDs := []int64{}
	owners := []string{}
	for i, refs := range w.recipes {
		for _, ref := range refs {
			owners = append(owners, w.recipeOwners[i])
			objectIDs = append(objectIDs, ids[ref.Hash])
		}
	}
	recipeTable, refTable := sharedRepresentationTables(w.components)
	if _, err = w.tx.ExecContext(w.ctx, `INSERT INTO `+refTable+`(content_hash,pack_id) SELECT DISTINCT v.content_hash,o.pack_id FROM unnest($1::text[],$2::bigint[]) AS v(content_hash,object_id) JOIN fugue_platform_input_shared_objects o USING(object_id) ON CONFLICT DO NOTHING`, owners, objectIDs); err != nil {
		return err
	}
	var statement strings.Builder
	statement.WriteString(`INSERT INTO ` + recipeTable + `(content_hash,ordinal,recipe_hash,recipe) VALUES`)
	args := []any{}
	for i, refs := range w.recipes {
		physical := make([]sharedRecipeRef, len(refs))
		for j, ref := range refs {
			physical[j] = sharedRecipeRef{ID: ids[ref.Hash], Size: ref.Size}
		}
		recipe, e := encodeSharedRecipe(physical, w.lengths[i])
		if e != nil {
			return e
		}
		if i > 0 {
			statement.WriteByte(',')
		}
		fmt.Fprintf(&statement, "($%d,$%d,$%d,$%d)", 1+4*i, 2+4*i, 3+4*i, 4+4*i)
		args = append(args, w.recipeOwners[i], w.recipeOrdinals[i], sharedHash(sha256.Sum256(recipe)), recipe)
	}
	if _, err = w.tx.ExecContext(w.ctx, statement.String(), args...); err != nil {
		return err
	}
	w.objects = map[[32]byte]sharedObject{}
	w.order = nil
	w.recipes = nil
	w.recipeOwners = nil
	w.recipeOrdinals = nil
	w.lengths = nil
	return nil
}
func binaryRecipeSize(raw []byte) int64 {
	if len(raw) < 5 {
		return 0
	}
	return int64(raw[1])<<24 | int64(raw[2])<<16 | int64(raw[3])<<8 | int64(raw[4])
}
func (s *Store) pgWriteSharedInput(ctx context.Context, tx *sql.Tx, snapshot platformconfig.RuntimeSnapshot, digest string) (result error) {
	started := time.Now()
	defer func() {
		sharedInputStats.writeNanos.Add(uint64(time.Since(started)))
		if result != nil {
			sharedInputStats.failures.Add(1)
		}
	}()
	w := &sharedInputWriter{ctx: ctx, tx: tx, digest: digest, hash: sha256.New(), encoder: inputChunkEncoder{level: zlib.DefaultCompression}, objects: map[[32]byte]sharedObject{}, seen: map[[32]byte]bool{}}
	if err := platformconfig.WriteRuntimeSnapshot(w, snapshot); err != nil {
		return err
	}
	if err := w.stage(); err != nil {
		return err
	}
	if err := w.flush(); err != nil {
		return err
	}
	if sharedHashBytes(w.hash.Sum(nil)) != digest {
		return ErrConflict
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO fugue_platform_input_manifests(content_hash,format,size_bytes,chunk_size,chunk_count,created_at) VALUES($1,$2,$3,$4,$5,$6)`, digest, inputFormatSharedV3, w.size, inputChunkBytes, w.ordinal, time.Now().UTC()); err != nil {
		return err
	}
	m, err := readInputManifest(ctx, tx, digest)
	if err != nil {
		return err
	}
	r, err := openSharedInput(ctx, tx, m)
	if err != nil {
		return err
	}
	_, err = io.Copy(io.Discard, r)
	if err == nil {
		sharedInputStats.writes.Add(1)
		sharedInputStats.logical.Add(uint64(w.size))
		sharedInputStats.newPackBytes.Add(w.newPackBytes)
		sharedInputStats.newObjects.Add(w.newObjects)
		sharedInputStats.reusedObjects.Add(w.reusedObjects)
	}
	return err
}
