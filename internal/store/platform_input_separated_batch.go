package store

import (
	"context"
	"crypto/sha256"
	"time"
)

// Prefetch only metadata for at most sixteen distinct components / four MiB
// of logical bodies. Pack bodies remain demand-read with a four MiB cache.
// Every recipe, inventory, object placement and reconstructed digest retains
// the same checks as the single-component reader.
func (r *separatedInputReader) evidenceReader(digest string) (*sharedInputReader, error) {
	if next := r.readers[digest]; next != nil {
		return freshComponentReader(next), nil
	}
	wanted := map[string]inputManifest{digest: r.components[digest]}
	logical := wanted[digest].Size
	for i := r.ordinal; i < len(r.parts) && len(wanted) < 16; i++ {
		part := r.parts[i]
		if !part.Evidence || wanted[part.Digest].Digest != "" {
			continue
		}
		m := r.components[part.Digest]
		if logical+m.Size > 4<<20 {
			break
		}
		wanted[part.Digest], logical = m, logical+m.Size
	}
	readers, err := openComponentBatch(r.ctx, r.db, wanted)
	if err != nil {
		return nil, err
	}
	r.readers = readers
	for _, next := range readers {
		next.packCache = r.packCache
	}
	next := readers[digest]
	return freshComponentReader(next), nil
}

func freshComponentReader(metadata *sharedInputReader) *sharedInputReader {
	// Recipes and placement metadata are immutable within this transaction.
	// Each occurrence still independently hashes its complete logical body.
	r := *metadata
	r.hash = sha256.New()
	return &r
}

func openComponentBatch(ctx context.Context, db inputQueryer, manifests map[string]inputManifest) (map[string]*sharedInputReader, error) {
	readers := map[string]*sharedInputReader{}
	keys := make([]string, 0, len(manifests))
	referenced := map[string]map[int64]int{}
	allIDs := map[int64]bool{}
	for key, m := range manifests {
		keys = append(keys, key)
		readers[key] = &sharedInputReader{ctx: ctx, db: db, m: m, components: true, hash: sha256.New(), locations: map[int64]sharedLocation{}, packs: map[int64]sharedPackWitness{}, cache: map[int64][]byte{}, started: time.Now()}
		referenced[key] = map[int64]int{}
	}
	rows, err := db.QueryContext(ctx, `SELECT content_hash,ordinal,recipe_hash,recipe FROM fugue_platform_input_component_recipes WHERE content_hash=ANY($1::text[]) ORDER BY content_hash,ordinal`, keys)
	if err != nil {
		return nil, err
	}
	bytes := map[string]int{}
	for rows.Next() {
		var key, declared string
		var ordinal int
		var raw []byte
		if err = rows.Scan(&key, &ordinal, &declared, &raw); err != nil {
			break
		}
		r := readers[key]
		if r == nil || ordinal != len(r.recipes) || ordinal >= r.m.Count || sharedHash(sha256.Sum256(raw)) != declared {
			err = ErrConflict
			break
		}
		bytes[key] += len(raw)
		if bytes[key] > sharedRecipeBytesLimit {
			err = ErrConflict
			break
		}
		want := min(inputChunkBytes, int(r.m.Size-int64(ordinal)*inputChunkBytes))
		refs, e := decodeSharedRecipe(raw, want)
		if e != nil {
			err = e
			break
		}
		for _, ref := range refs {
			if n, ok := referenced[key][ref.ID]; ok && n != ref.Size {
				err = ErrConflict
				break
			}
			referenced[key][ref.ID] = ref.Size
			allIDs[ref.ID] = true
			if len(referenced[key]) > sharedObjectLimit {
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
	for _, r := range readers {
		if len(r.recipes) != r.m.Count {
			return nil, ErrConflict
		}
	}
	rows, err = db.QueryContext(ctx, `SELECT c.content_hash,p.pack_id,p.pack_hash,p.size_bytes,p.stored_hash,octet_length(p.data) FROM fugue_platform_input_component_refs c JOIN fugue_platform_input_shared_packs p USING(pack_id) WHERE c.content_hash=ANY($1::text[])`, keys)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var key string
		var id int64
		var h, sh []byte
		var w sharedPackWitness
		if err = rows.Scan(&key, &id, &h, &w.Size, &sh, &w.StoredSize); err != nil {
			break
		}
		r := readers[key]
		if r == nil || id < 1 || len(h) != 32 || len(sh) != 32 || w.Size < 1 || w.Size > inputChunkBytes-inputCodecHeaderBytes || w.StoredSize < 5 || w.StoredSize > inputChunkBytes {
			err = ErrConflict
			break
		}
		copy(w.Hash[:], h)
		copy(w.StoredHash[:], sh)
		r.packs[id] = w
		if len(r.packs) > len(referenced[key]) {
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
	ids := make([]int64, 0, len(allIDs))
	for id := range allIDs {
		ids = append(ids, id)
	}
	locations := map[int64]sharedLocation{}
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
			if !allIDs[id] || len(h) != 32 {
				err = ErrConflict
				break
			}
			copy(l.Hash[:], h)
			locations[id] = l
		}
		if err == nil {
			err = rows.Err()
		}
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	if len(locations) != len(ids) {
		return nil, ErrConflict
	}
	for key, r := range readers {
		used := map[int64]bool{}
		for id, n := range referenced[key] {
			l := locations[id]
			w, ok := r.packs[l.PackID]
			if !ok || n != l.Size || l.Offset < 0 || l.Offset+l.Size > w.Size {
				return nil, ErrConflict
			}
			r.locations[id] = l
			used[l.PackID] = true
		}
		if len(r.packs) < 1 || len(used) != len(r.packs) {
			return nil, ErrConflict
		}
	}
	return readers, nil
}
