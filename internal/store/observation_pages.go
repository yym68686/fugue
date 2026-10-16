package store

import (
	"container/list"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ObservationPages splits a rebuildable observation into bounded named pages.
// Metadata carries the cursor and generation references; both commit together.
type ObservationPages struct {
	Metadata []byte
	Pages    map[string][]byte
}
type ObservationPageUpdate func(ObservationPages, []byte) (ObservationPages, error)

// Only bytes are cached. Every read obtains the current head, complete page
// membership, sizes and SHA-256 of actual database values. Mutable buffers
// belong to the caller; neither revision nor integrity decisions are cached.
var observationPageBodies = newObservationPageBodyCache(16<<20, 1024)

type observationPageBodyCache struct {
	mu                          sync.Mutex
	lru                         *list.List
	entries                     map[string]*list.Element
	bytes, maxBytes, maxEntries int
}
type observationPageBodyEntry struct {
	key, hash string
	body      []byte
	cost      int
}

func newObservationPageBodyCache(maxBytes, maxEntries int) *observationPageBodyCache {
	return &observationPageBodyCache{lru: list.New(), entries: map[string]*list.Element{}, maxBytes: maxBytes, maxEntries: maxEntries}
}
func (c *observationPageBodyCache) get(key, hash string) ([]byte, bool) {
	c.mu.Lock()
	e := c.entries[key]
	if e == nil || e.Value.(observationPageBodyEntry).hash != hash {
		c.mu.Unlock()
		return nil, false
	}
	c.lru.MoveToFront(e)
	body := e.Value.(observationPageBodyEntry).body
	c.mu.Unlock()
	return append([]byte{}, body...), true
}
func (c *observationPageBodyCache) put(key, hash string, body []byte) {
	cost := 192 + len(key) + len(hash) + len(body)
	if cost > c.maxBytes || c.maxEntries <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.entries[key]; e != nil {
		c.bytes -= e.Value.(observationPageBodyEntry).cost
		c.lru.Remove(e)
		delete(c.entries, key)
	}
	for len(c.entries) >= c.maxEntries || c.bytes+cost > c.maxBytes {
		e := c.lru.Back()
		entry := e.Value.(observationPageBodyEntry)
		c.bytes -= entry.cost
		delete(c.entries, entry.key)
		c.lru.Remove(e)
	}
	c.entries[key] = c.lru.PushFront(observationPageBodyEntry{key: key, hash: hash, body: append([]byte{}, body...), cost: cost})
	c.bytes += cost
}

const observationPageReferencesSQL = "SELECT entry_key,octet_length(value),encode(sha256(value),'hex') FROM fugue_observation_checkpoint_entries WHERE checkpoint_key=$1 AND bucket='pages' ORDER BY entry_key"

type observationPageRef struct {
	size int
	hash string
}

func readObservationPages(ctx context.Context, db inputQueryer, key string) (ObservationPages, int64, error) {
	return readObservationPagesWithCache(ctx, db, key, observationPageBodies)
}
func readObservationPagesWithCache(ctx context.Context, db inputQueryer, key string, cache *observationPageBodyCache) (ObservationPages, int64, error) {
	var out ObservationPages
	var revision int64
	err := db.QueryRowContext(ctx, "SELECT revision,metadata FROM fugue_observation_checkpoint_heads WHERE checkpoint_key=$1", key).Scan(&revision, &out.Metadata)
	if err != nil {
		return out, 0, err
	}
	if revision < 1 || len(out.Metadata) > inputChunkBytes {
		return out, 0, ErrConflict
	}
	rows, err := db.QueryContext(ctx, observationPageReferencesSQL, key)
	if err != nil {
		return out, 0, err
	}
	defer rows.Close()
	refs := map[string]observationPageRef{}
	missing := []string{}
	out.Pages = map[string][]byte{}
	size := len(out.Metadata)
	for rows.Next() {
		var k string
		var ref observationPageRef
		if err = rows.Scan(&k, &ref.size, &ref.hash); err != nil {
			return out, 0, err
		}
		digest, decodeErr := hex.DecodeString(ref.hash)
		if len(k) > 4096 || ref.size < 0 || ref.size > 64<<10 || decodeErr != nil || len(digest) != sha256.Size {
			return out, 0, fmt.Errorf("observation page reference is invalid")
		}
		if _, exists := refs[k]; exists {
			return out, 0, fmt.Errorf("duplicate observation page reference")
		}
		size += ref.size
		if size > 16<<20 {
			return out, 0, fmt.Errorf("observation pages exceed bounded size")
		}
		refs[k] = ref
		if raw, ok := cache.get(key+"\x00"+k, ref.hash); ok && len(raw) == ref.size {
			out.Pages[k] = raw
		} else {
			missing = append(missing, k)
		}
	}
	if err = rows.Err(); err != nil {
		return out, 0, err
	}
	if err = rows.Close(); err != nil {
		return out, 0, err
	}
	// Bound statement parameters; all requested bodies remain in the same
	// transaction. Keyed lookups avoid quadratic work on a cold cache.
	for start := 0; start < len(missing); start += 128 {
		keys := missing[start:min(start+128, len(missing))]
		if err = readObservationPageBodies(ctx, db, key, keys, refs, out.Pages, cache); err != nil {
			return out, 0, err
		}
	}
	return out, revision, nil
}
func readObservationPageBodies(ctx context.Context, db inputQueryer, key string, keys []string, refs map[string]observationPageRef, pages map[string][]byte, cache *observationPageBodyCache) error {
	args := make([]any, 1, len(keys)+1)
	args[0] = key
	pending := make(map[string]bool, len(keys))
	for _, k := range keys {
		args = append(args, k)
		pending[k] = true
	}
	rows, err := db.QueryContext(ctx, "SELECT entry_key,value FROM fugue_observation_checkpoint_entries WHERE checkpoint_key=$1 AND bucket='pages' AND entry_key IN ("+sqlPlaceholderList(2, len(keys))+")", args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var k string
		var raw []byte
		if err = rows.Scan(&k, &raw); err != nil {
			return err
		}
		ref, known := refs[k]
		if !known || !pending[k] {
			return fmt.Errorf("observation page body is unexpected")
		}
		actual := sha256.Sum256(raw)
		if len(raw) != ref.size || hex.EncodeToString(actual[:]) != ref.hash {
			return fmt.Errorf("observation page body hash or size differs")
		}
		delete(pending, k)
		pages[k] = raw
		cache.put(key+"\x00"+k, ref.hash, raw)
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if len(pending) != 0 {
		return fmt.Errorf("observation page body is missing")
	}
	return rows.Close()
}

func (s *Store) ReadObservationPages(ctx context.Context, key string) (ObservationPages, []byte, error) {
	if s.usingDatabase() {
		// One transaction gives head and pages a consistent generation during
		// another replica's cursor update or completion swap.
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
		if err != nil {
			return ObservationPages{}, nil, err
		}
		defer tx.Rollback()
		p, _, err := readObservationPages(ctx, tx, observationCheckpointKey(key))
		if err == nil {
			return p, nil, tx.Commit()
		}
		if !absentInputManifest(err) {
			return p, nil, err
		}
		// Release the pinned read connection before the legacy lookup, including
		// when the configured pool contains only one connection.
		if err := tx.Rollback(); err != nil {
			return p, nil, err
		}
	}
	raw, err := s.ReadObservationCheckpoint(ctx, key)
	return ObservationPages{}, raw, err
}

func (s *Store) UpdateObservationPages(ctx context.Context, key string, update ObservationPageUpdate, legacy func([]byte) ([]byte, error)) (bool, error) {
	if !s.usingDatabase() {
		return s.UpdateObservationCheckpoint(ctx, key, legacy)
	}
	originalKey := key
	key = observationCheckpointKey(key)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var locked bool
	if err = tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`, key).Scan(&locked); err != nil || !locked {
		return false, err
	}
	old, revision, err := readObservationPages(ctx, tx, key)
	// Deployment orders schema before activation. A missing table is supported
	// by the read path, but a writer never mutates an unverified schema.
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		if absentInputManifest(err) && !s.StoragePolicy().CheckpointPages {
			tx.Rollback()
			return s.UpdateObservationCheckpoint(ctx, originalKey, legacy)
		}
		return true, err
	}
	var raw []byte
	if errors.Is(err, sql.ErrNoRows) {
		if err = tx.QueryRowContext(ctx, `SELECT value FROM fugue_meta WHERE key=$1`, key).Scan(&raw); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return true, err
		}
		if !s.StoragePolicy().CheckpointPages {
			b, err := legacy(raw)
			if err != nil {
				return true, err
			}
			if len(b) > 16<<20 {
				return true, fmt.Errorf("observation checkpoint exceeds size limit")
			}
			if b != nil {
				if _, err = tx.ExecContext(ctx, `INSERT INTO fugue_meta(key,value) VALUES($1,$2) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value,updated_at=NOW()`, key, string(b)); err != nil {
					return true, err
				}
			}
			return true, tx.Commit()
		}
	}
	// The callback owns mutable pages. Capture comparison evidence before it
	// can replace, mutate or delete entries in the same map.
	oldHashes := make(map[string][sha256.Size]byte, len(old.Pages))
	for k, b := range old.Pages {
		oldHashes[k] = sha256.Sum256(b)
	}
	next, err := update(old, raw)
	if err != nil {
		return true, err
	}
	if next.Metadata == nil {
		return true, nil
	}
	total := len(next.Metadata)
	if total > inputChunkBytes {
		return true, fmt.Errorf("observation metadata exceeds byte budget")
	}
	for k, b := range next.Pages {
		total += len(b)
		if len(k) > 4096 || len(b) > 64<<10 || total > 16<<20 {
			return true, fmt.Errorf("observation pages exceed byte budget")
		}
	}
	for k, b := range next.Pages {
		if previous, exists := oldHashes[k]; exists && previous == sha256.Sum256(b) {
			continue
		}
		if b == nil {
			b = []byte{}
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO fugue_observation_checkpoint_entries(checkpoint_key,bucket,entry_key,value) VALUES($1,'pages',$2,$3) ON CONFLICT(checkpoint_key,bucket,entry_key) DO UPDATE SET value=EXCLUDED.value`, key, k, b); err != nil {
			return true, err
		}
	}
	for k := range oldHashes {
		if _, ok := next.Pages[k]; !ok {
			if _, err = tx.ExecContext(ctx, `DELETE FROM fugue_observation_checkpoint_entries WHERE checkpoint_key=$1 AND bucket='pages' AND entry_key=$2`, key, k); err != nil {
				return true, err
			}
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO fugue_observation_checkpoint_heads(checkpoint_key,revision,metadata,updated_at) VALUES($1,$2,$3,$4) ON CONFLICT(checkpoint_key) DO UPDATE SET revision=EXCLUDED.revision,metadata=EXCLUDED.metadata,updated_at=EXCLUDED.updated_at`, key, revision+1, next.Metadata, time.Now().UTC()); err != nil {
		return true, err
	}
	return true, tx.Commit()
}
