package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"time"

	"fugue/internal/platformconfig"
)

// A compact witness commits to the complete object placement and pack
// inventory. It caches no evidence bodies; every current physical pack is
// hashed again by the database before a selective result can be returned.
func (r *sharedInputReader) closureHash() [32]byte {
	h := sha256.New()
	var b [32]byte
	ids := make([]int64, 0, len(r.locations))
	for id := range r.locations {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		l := r.locations[id]
		binary.BigEndian.PutUint64(b[:8], uint64(id))
		binary.BigEndian.PutUint64(b[8:16], uint64(l.PackID))
		binary.BigEndian.PutUint64(b[16:24], uint64(l.Size))
		binary.BigEndian.PutUint64(b[24:], uint64(l.Offset))
		h.Write(b[:])
		h.Write(l.Hash[:])
	}
	ids = ids[:0]
	for id := range r.packs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		w := r.packs[id]
		binary.BigEndian.PutUint64(b[:8], uint64(id))
		binary.BigEndian.PutUint64(b[8:16], uint64(w.Size))
		binary.BigEndian.PutUint64(b[16:24], uint64(w.StoredSize))
		h.Write(b[:24])
		h.Write(w.Hash[:])
		h.Write(w.StoredHash[:])
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}
func (s *Store) rememberSharedDNSInput(snapshot platformconfig.RuntimeSnapshot, m inputManifest, r *sharedInputReader) {
	if !r.done || r.err != nil {
		return
	}
	w := &inputChunkWriter{digest: m.Digest, hash: sha256.New(), chunk: make([]byte, 0, inputChunkBytes)}
	if platformconfig.WriteRuntimeSnapshot(w, snapshot) != nil || w.flush() != nil || w.dnsRangesOverflow || w.size != m.Size || sharedHashBytes(w.hash.Sum(nil)) != m.Digest {
		return
	}
	c := &compilerDNSInput{Manifest: m, Intent: snapshot.IntentGeneration, Policy: snapshot.PolicyGeneration, Ranges: w.dnsRanges, SharedRecipes: r.recipes, SharedClosure: r.closureHash()}
	c.Bytes = 512 + len(c.Intent) + len(c.Policy)
	for _, recipe := range c.SharedRecipes {
		c.Bytes += len(recipe) + 24
	}
	for _, v := range c.Ranges {
		c.Bytes += 256 + len(v.Summary.NodeID) + len(v.Summary.Hostname) + len(v.Summary.Type)
	}
	s.compilerDNSInputs.put(c)
}
func readSharedDNSSelections(ctx context.Context, db inputQueryer, m inputManifest, c *compilerDNSInput, needed func(CompilerDNSSelectionSummary) bool) (result CompilerInputDNSSelections, resultErr error) {
	started := time.Now()
	defer func() {
		sharedInputStats.selectiveReads.Add(1)
		sharedInputStats.selectiveNanos.Add(uint64(time.Since(started)))
		if resultErr != nil {
			sharedInputStats.selectiveFailures.Add(1)
		}
	}()
	r, err := openSharedInput(ctx, db, m)
	if err != nil {
		return CompilerInputDNSSelections{}, err
	}
	if len(c.SharedRecipes) != len(r.recipes) || c.SharedClosure != r.closureHash() {
		return CompilerInputDNSSelections{}, ErrConflict
	}
	for i, recipe := range r.recipes {
		if !bytes.Equal(recipe, c.SharedRecipes[i]) {
			return CompilerInputDNSSelections{}, ErrConflict
		}
	}
	ids := make([]int64, 0, len(r.packs))
	for id := range r.packs {
		ids = append(ids, id)
	}
	for start := 0; start < len(ids); start += 32 {
		rows, e := db.QueryContext(ctx, `SELECT pack_id,sha256(data) FROM fugue_platform_input_shared_packs WHERE pack_id=ANY($1::bigint[])`, ids[start:min(start+32, len(ids))])
		if e != nil {
			return CompilerInputDNSSelections{}, e
		}
		count := 0
		for rows.Next() {
			var id int64
			var h []byte
			if e = rows.Scan(&id, &h); e != nil {
				break
			}
			w, ok := r.packs[id]
			if !ok || !bytes.Equal(h, w.StoredHash[:]) {
				e = ErrConflict
				break
			}
			count++
		}
		if e == nil {
			e = rows.Err()
		}
		rows.Close()
		if e != nil {
			return CompilerInputDNSSelections{}, e
		}
		if count != min(32, len(ids)-start) {
			return CompilerInputDNSSelections{}, ErrConflict
		}
	}
	out := CompilerInputDNSSelections{IntentGeneration: c.Intent, PolicyGeneration: c.Policy}
	ranges := []compilerDNSRange{}
	selected := make([]bool, m.Count)
	for _, v := range c.Ranges {
		if needed != nil && !needed(v.Summary) {
			out.Omitted = append(out.Omitted, v.Summary)
			continue
		}
		if v.Offset < 0 || v.Length < 1 || v.Offset+v.Length > m.Size {
			return CompilerInputDNSSelections{}, ErrConflict
		}
		ranges = append(ranges, v)
		for i := int(v.Offset / inputChunkBytes); i <= int((v.Offset+v.Length-1)/inputChunkBytes); i++ {
			selected[i] = true
		}
	}
	chunks := make([][]byte, m.Count)
	for ordinal, recipe := range r.recipes {
		if !selected[ordinal] {
			continue
		}
		refs, e := decodeSharedRecipe(recipe, int(binaryRecipeSize(recipe)))
		if e != nil {
			return CompilerInputDNSSelections{}, e
		}
		for _, ref := range refs {
			l := r.locations[ref.ID]
			pack, e := r.pack(l.PackID)
			if e != nil {
				return CompilerInputDNSSelections{}, e
			}
			raw := pack[l.Offset : l.Offset+l.Size]
			if sha256.Sum256(raw) != l.Hash {
				return CompilerInputDNSSelections{}, ErrConflict
			}
			chunks[ordinal] = append(chunks[ordinal], raw...)
		}
	}
	return decodeCompilerDNSRanges(out, m, chunks, ranges)
}
