package store

import (
	"bytes"
	"crypto/sha256"
	"fmt"
)

// A read batch contains at most sixteen physical packs / four MiB. The same
// transaction's immutable witnesses still verify every stored and decoded
// byte; batching changes round trips, not what can authorize a publication.
func (r *sharedInputReader) prefetchPacks(first int64) error {
	ids := []int64{first}
	wanted := map[int64]bool{first: true}
	for ordinal := r.ordinal; ordinal < min(r.ordinal+16, len(r.recipes)) && len(ids) < 16; ordinal++ {
		want := min(inputChunkBytes, int(r.m.Size-int64(ordinal)*inputChunkBytes))
		refs, err := decodeSharedRecipe(r.recipes[ordinal], want)
		if err != nil {
			return err
		}
		for _, ref := range refs {
			id := r.locations[ref.ID].PackID
			w, ok := r.packs[id]
			if !ok {
				return ErrConflict
			}
			cached := false
			if r.packCache != nil {
				value, exists := r.packCache.entries[id]
				cached = exists && value.witness == w
			} else {
				_, cached = r.cache[id]
			}
			if !wanted[id] && !cached {
				wanted[id] = true
				ids = append(ids, id)
				if len(ids) == 16 {
					break
				}
			}
		}
	}
	rows, err := r.db.QueryContext(r.ctx, `SELECT pack_id,pack_hash,size_bytes,stored_hash,data FROM fugue_platform_input_shared_packs WHERE pack_id=ANY($1::bigint[])`, ids)
	if err != nil {
		return err
	}
	loaded := map[int64][]byte{}
	for rows.Next() {
		var id int64
		var data, h, sh []byte
		var n int
		if err = rows.Scan(&id, &h, &n, &sh, &data); err != nil {
			break
		}
		w, ok := r.packs[id]
		if !wanted[id] || !ok || loaded[id] != nil || n != w.Size || len(data) != w.StoredSize || !bytes.Equal(h, w.Hash[:]) || !bytes.Equal(sh, w.StoredHash[:]) || sha256.Sum256(data) != w.StoredHash {
			err = fmt.Errorf("shared pack envelope: %w", ErrConflict)
			break
		}
		raw, e := decodeInputChunk(inputFormatCodecV2, data, n)
		if e != nil || sha256.Sum256(raw) != w.Hash {
			err = fmt.Errorf("shared pack body: %w", ErrConflict)
			break
		}
		loaded[id] = raw
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return err
	}
	if len(loaded) != len(ids) {
		return ErrConflict
	}
	// Preserve recipe lookahead order regardless of PostgreSQL's row order.
	for _, id := range ids {
		r.cachePack(id, r.packs[id], loaded[id])
	}
	return nil
}
