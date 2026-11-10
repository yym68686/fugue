package store

import (
	"container/list"
	"crypto/sha256"
	"sync"

	"fugue/internal/platformsafety"

	"golang.org/x/sync/singleflight"
)

// This caches JSON decoding, never an artifact's validity or authority. The key
// hashes the bytes just read from the database, not its claimed ContentHash.
// Status, signatures, trust, releases and expected membership are still read
// and checked by their existing paths on every request.
var platformContentDecodings = newPlatformContentCache(64<<20, 64)

type platformContentCache struct {
	mu              sync.Mutex
	entries         map[[32]byte]*list.Element
	lru             *list.List
	bytes, maxBytes int
	maxEntries      int
	flights         singleflight.Group
}

type platformContentEntry struct {
	key           [32]byte
	content       map[string]any
	bytes         int
	fingerprint   [32]byte
	fingerprintOK bool
}

func newPlatformContentCache(maxBytes, maxEntries int) *platformContentCache {
	return &platformContentCache{entries: make(map[[32]byte]*list.Element), lru: list.New(), maxBytes: maxBytes, maxEntries: maxEntries}
}

func (c *platformContentCache) decode(raw []byte) (map[string]any, error) {
	if len(raw) < 1024 || len(raw) > 8<<20 || c.maxBytes <= 0 || c.maxEntries <= 0 {
		return decodeJSONValue[map[string]any](raw)
	}
	entry, err := c.decodeImmutable(raw, sha256.Sum256(raw))
	if err != nil {
		return nil, err
	}
	return clonePlatformContent(entry.content), nil
}

// decodeImmutable is internal to the store: its containers must never escape
// to callers. Metadata belongs to these exact bytes and is calculated once.
// The caller must hash the actual raw bytes, never a declared artifact hash.
func (c *platformContentCache) decodeImmutable(raw []byte, key [32]byte) (platformContentEntry, error) {
	// Small documents cost less to decode than to clone and manage. Huge
	// documents must not displace the entire working set or enter singleflight.
	if len(raw) < 1024 || len(raw) > 8<<20 || c.maxBytes <= 0 || c.maxEntries <= 0 {
		content, err := decodeJSONValue[map[string]any](raw)
		return newPlatformContentEntry(key, content), err
	}
	if entry, ok := c.getEntry(key); ok {
		return entry, nil
	}
	value, err, _ := c.flights.Do(string(key[:]), func() (any, error) {
		if entry, ok := c.getEntry(key); ok {
			return entry, nil
		}
		content, err := decodeJSONValue[map[string]any](raw)
		if err != nil {
			return nil, err
		}
		entry := newPlatformContentEntry(key, content)
		if content != nil {
			c.putEntry(entry)
		}
		return entry, nil
	})
	if err != nil {
		return platformContentEntry{}, err
	}
	// Only store-internal ownership boundaries may consume this entry.
	return value.(platformContentEntry), nil
}

func newPlatformContentEntry(key [32]byte, content map[string]any) platformContentEntry {
	fingerprint, ok := platformsafety.FingerprintArtifactContent(content)
	return platformContentEntry{key: key, content: content, bytes: 256 + platformContentSize(content), fingerprint: fingerprint, fingerprintOK: ok}
}

func (c *platformContentCache) get(key [32]byte) (map[string]any, bool) {
	entry, ok := c.getEntry(key)
	return entry.content, ok
}

func (c *platformContentCache) getEntry(key [32]byte) (platformContentEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok {
		return platformContentEntry{}, false
	}
	c.lru.MoveToFront(entry)
	return entry.Value.(platformContentEntry), true
}

func (c *platformContentCache) putEntry(candidate platformContentEntry) {
	key, size := candidate.key, candidate.bytes
	if size > c.maxBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.entries[key]; ok {
		return
	}
	for c.bytes+size > c.maxBytes || len(c.entries) >= c.maxEntries {
		last := c.lru.Back()
		entry := last.Value.(platformContentEntry)
		delete(c.entries, entry.key)
		c.bytes -= entry.bytes
		c.lru.Remove(last)
	}
	c.entries[key] = c.lru.PushFront(candidate)
	c.bytes += size
}

func clonePlatformContent(content map[string]any) map[string]any {
	if content == nil {
		return nil
	}
	return clonePlatformJSON(content).(map[string]any)
}

func clonePlatformJSON(value any) any {
	switch v := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			out[key] = clonePlatformJSON(item)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = clonePlatformJSON(item)
		}
		return out
	default:
		// Only encoding/json output reaches this function: strings, numbers,
		// booleans and nil have no caller-mutable storage.
		return value
	}
}

// Conservative accounting includes decoded containers, keys and strings, not
// just wire length. The raw document is not retained by the cache.
func platformContentSize(value any) int {
	switch v := value.(type) {
	case map[string]any:
		size := 128 + 128*len(v)
		for key, item := range v {
			size += len(key) + platformContentSize(item)
		}
		return size
	case []any:
		size := 64 + 32*len(v)
		for _, item := range v {
			size += platformContentSize(item)
		}
		return size
	case string:
		return 32 + len(v)
	default:
		return 32
	}
}
