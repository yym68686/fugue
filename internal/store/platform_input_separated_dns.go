package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"io"
	"sort"

	"fugue/internal/platformconfig"
)

// The witness covers layout, every component/recipe, object placement and the
// actual physical bytes of every referenced pack, including omitted evidence.
// It caches no bodies or positive serving/validation decisions.
func separatedClosureWitness(ctx context.Context, db inputQueryer, r *separatedInputReader) ([32]byte, error) {
	var zero [32]byte
	h := sha256.New()
	write := func(raw []byte) {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(raw)))
		h.Write(n[:])
		h.Write(raw)
	}
	write([]byte(r.m.Digest))
	write(r.frame)
	layout, err := encodeSeparatedLayout(r.parts)
	if err != nil {
		return zero, err
	}
	write(layout)
	keys := make([]string, 0, len(r.components))
	for k := range r.components {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	wantRecipes := 0
	for _, k := range keys {
		m := r.components[k]
		write([]byte(k))
		var b [16]byte
		binary.BigEndian.PutUint64(b[:8], uint64(m.Size))
		binary.BigEndian.PutUint64(b[8:], uint64(m.Count))
		write(b[:])
		wantRecipes += m.Count
	}
	rows, err := db.QueryContext(ctx, `SELECT content_hash,ordinal,recipe_hash,sha256(recipe) FROM fugue_platform_input_component_recipes WHERE content_hash=ANY($1::text[]) ORDER BY content_hash,ordinal`, keys)
	if err != nil {
		return zero, err
	}
	count := 0
	for rows.Next() {
		var k, declared string
		var ordinal int64
		var actual []byte
		if err = rows.Scan(&k, &ordinal, &declared, &actual); err != nil {
			break
		}
		if declared != sharedHashBytes(actual) || len(actual) != 32 || ordinal < 0 || ordinal >= int64(r.components[k].Count) {
			err = ErrConflict
			break
		}
		write([]byte(k))
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(ordinal))
		write(b[:])
		write(actual)
		count++
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return zero, err
	}
	if count != wantRecipes {
		return zero, ErrConflict
	}
	rows, err = db.QueryContext(ctx, `SELECT c.content_hash,p.pack_id,p.pack_hash,p.size_bytes,p.stored_hash,octet_length(p.data) FROM fugue_platform_input_component_refs c JOIN fugue_platform_input_shared_packs p USING(pack_id) WHERE c.content_hash=ANY($1::text[]) ORDER BY c.content_hash,p.pack_id`, keys)
	if err != nil {
		return zero, err
	}
	ids := map[int64]bool{}
	storedHashes := map[int64][]byte{}
	packRefs := 0
	for rows.Next() {
		var k string
		var id, size, stored int64
		var ph, sh []byte
		if err = rows.Scan(&k, &id, &ph, &size, &sh, &stored); err != nil {
			break
		}
		if len(ph) != 32 || len(sh) != 32 || size < 1 || size > inputChunkBytes-inputCodecHeaderBytes || stored < 5 || stored > inputChunkBytes {
			err = ErrConflict
			break
		}
		write([]byte(k))
		var b [24]byte
		binary.BigEndian.PutUint64(b[:8], uint64(id))
		binary.BigEndian.PutUint64(b[8:16], uint64(size))
		binary.BigEndian.PutUint64(b[16:], uint64(stored))
		write(b[:])
		write(ph)
		write(sh)
		ids[id] = true
		storedHashes[id] = sh
		packRefs++
		if packRefs > sharedObjectLimit || len(ids) > sharedObjectLimit {
			err = errSharedInputBudget
			break
		}
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return zero, err
	}
	packIDs := make([]int64, 0, len(ids))
	for id := range ids {
		packIDs = append(packIDs, id)
	}
	// Hash each distinct physical pack once, regardless of the number of
	// evidence components sharing it. No pack bodies cross the connection.
	rows, err = db.QueryContext(ctx, `SELECT pack_id,sha256(data) FROM fugue_platform_input_shared_packs WHERE pack_id=ANY($1::bigint[])`, packIDs)
	if err != nil {
		return zero, err
	}
	checked := 0
	for rows.Next() {
		var id int64
		var actual []byte
		if err = rows.Scan(&id, &actual); err != nil {
			break
		}
		if !bytes.Equal(storedHashes[id], actual) {
			err = ErrConflict
			break
		}
		checked++
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return zero, err
	}
	if checked != len(ids) {
		return zero, ErrConflict
	}
	rows, err = db.QueryContext(ctx, `SELECT object_id,object_hash,size_bytes,pack_id,pack_offset FROM fugue_platform_input_shared_objects WHERE pack_id=ANY($1::bigint[]) ORDER BY object_id`, packIDs)
	if err != nil {
		return zero, err
	}
	count = 0
	for rows.Next() {
		var id, size, pack, offset int64
		var oh []byte
		if err = rows.Scan(&id, &oh, &size, &pack, &offset); err != nil {
			break
		}
		if len(oh) != 32 || id < 1 || size < 1 || size > sharedBlockMax || !ids[pack] || offset < 0 || offset+size > inputChunkBytes-inputCodecHeaderBytes {
			err = ErrConflict
			break
		}
		var b [32]byte
		binary.BigEndian.PutUint64(b[:8], uint64(id))
		binary.BigEndian.PutUint64(b[8:16], uint64(size))
		binary.BigEndian.PutUint64(b[16:24], uint64(pack))
		binary.BigEndian.PutUint64(b[24:], uint64(offset))
		write(b[:])
		write(oh)
		count++
		if count > sharedObjectLimit {
			err = errSharedInputBudget
			break
		}
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return zero, err
	}
	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result, nil
}

func (s *Store) rememberSeparatedDNSInput(ctx context.Context, db inputQueryer, snapshot platformconfig.RuntimeSnapshot, m inputManifest, r *separatedInputReader) {
	if !r.done || r.err != nil {
		return
	}
	w := &inputChunkWriter{digest: m.Digest, hash: sha256.New(), chunk: make([]byte, 0, inputChunkBytes)}
	if platformconfig.WriteRuntimeSnapshot(w, snapshot) != nil || w.flush() != nil || w.dnsRangesOverflow || w.size != m.Size || sharedHashBytes(w.hash.Sum(nil)) != m.Digest {
		return
	}
	witness, err := separatedClosureWitness(ctx, db, r)
	if err != nil {
		return
	}
	c := &compilerDNSInput{Manifest: m, Intent: snapshot.IntentGeneration, Policy: snapshot.PolicyGeneration, Ranges: w.dnsRanges, SeparatedWitness: witness, Bytes: 512 + len(snapshot.IntentGeneration) + len(snapshot.PolicyGeneration)}
	for _, v := range c.Ranges {
		c.Bytes += 256 + len(v.Summary.NodeID) + len(v.Summary.Hostname) + len(v.Summary.Type)
	}
	s.compilerDNSInputs.put(c)
}

func readSeparatedDNSSelections(ctx context.Context, db inputQueryer, m inputManifest, c *compilerDNSInput, needed func(CompilerDNSSelectionSummary) bool) (CompilerInputDNSSelections, error) {
	r, err := openSeparatedInput(ctx, db, m)
	if err != nil {
		return CompilerInputDNSSelections{}, err
	}
	witness, err := separatedClosureWitness(ctx, db, r)
	if err != nil {
		return CompilerInputDNSSelections{}, err
	}
	if witness != c.SeparatedWitness {
		return CompilerInputDNSSelections{}, ErrConflict
	}
	out := CompilerInputDNSSelections{IntentGeneration: c.Intent, PolicyGeneration: c.Policy}
	// Index once; a selection must not rescan all preceding layout parts.
	ends := make([]int64, len(r.parts))
	for i, part := range r.parts {
		ends[i] = int64(part.Size)
		if i > 0 {
			ends[i] += ends[i-1]
		}
	}
	for _, v := range c.Ranges {
		if needed != nil && !needed(v.Summary) {
			out.Omitted = append(out.Omitted, v.Summary)
			continue
		}
		if v.Offset < 0 || v.Length < 1 || v.Offset+v.Length > m.Size {
			return CompilerInputDNSSelections{}, ErrConflict
		}
		var raw []byte
		for i := sort.Search(len(ends), func(i int) bool { return ends[i] > v.Offset }); i < len(r.parts); i++ {
			part := r.parts[i]
			end := ends[i]
			offset := end - int64(part.Size)
			if end > v.Offset && offset < v.Offset+v.Length {
				start := max(int64(0), v.Offset-offset)
				stop := min(int64(part.Size), v.Offset+v.Length-offset)
				if part.Evidence {
					component, e := openSharedRepresentation(ctx, db, r.components[part.Digest], true)
					if e != nil {
						return CompilerInputDNSSelections{}, e
					}
					body, e := io.ReadAll(component)
					if e != nil {
						return CompilerInputDNSSelections{}, e
					}
					raw = append(raw, body[start:stop]...)
				} else {
					raw = append(raw, r.frame[int64(part.Offset)+start:int64(part.Offset)+stop]...)
				}
			}
			if end >= v.Offset+v.Length {
				break
			}
		}
		if int64(len(raw)) != v.Length {
			return CompilerInputDNSSelections{}, ErrConflict
		}
		var fact platformconfig.DNSSelectionObservation
		if err = json.Unmarshal(raw, &fact); err != nil {
			return CompilerInputDNSSelections{}, err
		}
		if !sameCompilerDNSSelectionSummary(compilerDNSSelectionSummary(fact), v.Summary) {
			return CompilerInputDNSSelections{}, ErrConflict
		}
		out.DNSSelections = append(out.DNSSelections, fact)
	}
	return out, nil
}
