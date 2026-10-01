package store

import (
	"container/list"
	"crypto/sha256"
	"sync"

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
	key     [32]byte
	content map[string]any
	bytes   int
}

func newPlatformContentCache(maxBytes, maxEntries int) *platformContentCache {
	return &platformContentCache{entries: make(map[[32]byte]*list.Element), lru: list.New(), maxBytes: maxBytes, maxEntries: maxEntries}
}

func (c *platformContentCache) decode(raw []byte) (map[string]any, error) {
	// Small documents cost less to decode than to clone and manage. Huge
	// documents must not displace the entire working set or enter singleflight.
	if len(raw) < 1024 || len(raw) > 8<<20 || c.maxBytes <= 0 || c.maxEntries <= 0 {
		return decodeJSONValue[map[string]any](raw)
	}
	key := sha256.Sum256(raw)
	if content, ok := c.get(key); ok {
		return clonePlatformContent(content), nil
	}
	value, err, _ := c.flights.Do(string(key[:]), func() (any, error) {
		if content, ok := c.get(key); ok {
			return content, nil
		}
		content, err := decodeJSONValue[map[string]any](raw)
		if err != nil {
			return nil, err
		}
		if content != nil {
			c.put(key, content)
		}
		return content, nil
	})
	if err != nil {
		return nil, err
	}
	// Cached maps/slices never escape. Scalars and immutable strings can be
	// shared; every caller owns all mutable containers, including on a miss.
	return clonePlatformContent(value.(map[string]any)), nil
}

func (c *platformContentCache) get(key [32]byte) (map[string]any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	c.lru.MoveToFront(entry)
	return entry.Value.(platformContentEntry).content, true
}

func (c *platformContentCache) put(key [32]byte, content map[string]any) {
	size := 256 + platformContentSize(content)
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
	c.entries[key] = c.lru.PushFront(platformContentEntry{key: key, content: content, bytes: size})
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
