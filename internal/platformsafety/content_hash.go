package platformsafety

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"hash"
	"math"
	"sort"
	"sync"

	"golang.org/x/sync/singleflight"
)

// Cache only the pure canonical JSON hash, never an integrity verdict. Every
// lookup fingerprints the actual complete content tree; claimed hashes, IDs,
// pointer identity and mutable authority are not cache keys. Signatures and
// current trust are checked separately on every EvaluateArtifactIntegrity call.
var artifactContentHashes = newContentHashCache(256)

type contentHashCache struct {
	mu      sync.RWMutex
	values  map[[32]byte]string
	keys    [][32]byte
	next    int
	limit   int
	flights singleflight.Group
}

func newContentHashCache(limit int) *contentHashCache {
	return &contentHashCache{values: make(map[[32]byte]string), limit: limit}
}

func (c *contentHashCache) digest(content map[string]any) string {
	if content == nil || c.limit <= 0 {
		return canonicalArtifactContentHash(content)
	}
	key, size, supported := fingerprintContent(content)
	if !supported || size < 4096 {
		return canonicalArtifactContentHash(content)
	}
	if digest := c.get(key); digest != "" {
		return digest
	}
	value, _, _ := c.flights.Do(string(key[:]), func() (any, error) {
		if digest := c.get(key); digest != "" {
			return digest, nil
		}
		digest := canonicalArtifactContentHash(content)
		if digest != "" {
			c.put(key, digest)
		}
		return digest, nil
	})
	return value.(string)
}

func (c *contentHashCache) get(key [32]byte) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.values[key]
}

func (c *contentHashCache) put(key [32]byte, digest string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, found := c.values[key]; found {
		return
	}
	if len(c.keys) < c.limit {
		c.keys = append(c.keys, key)
	} else {
		delete(c.values, c.keys[c.next])
		c.keys[c.next] = key
		c.next = (c.next + 1) % c.limit
	}
	c.values[key] = digest
}

type contentFingerprinter struct {
	hash    hash.Hash
	writer  *bufio.Writer
	scratch [8]byte
	size    int
}

var contentFingerprinters = sync.Pool{New: func() any {
	h := sha256.New()
	return &contentFingerprinter{hash: h, writer: bufio.NewWriterSize(h, 8192)}
}}

func fingerprintContent(content map[string]any) ([32]byte, int, bool) {
	f := contentFingerprinters.Get().(*contentFingerprinter)
	f.hash.Reset()
	f.writer.Reset(f.hash)
	f.size = 0
	defer contentFingerprinters.Put(f)
	_, _ = f.writer.WriteString("fugue-json-tree/v1\x00")
	ok := f.value(content, 0)
	_ = f.writer.Flush() // SHA-256 writes cannot fail.
	var key [32]byte
	f.hash.Sum(key[:0])
	return key, f.size, ok
}

func (f *contentFingerprinter) number(n uint64) {
	binary.LittleEndian.PutUint64(f.scratch[:], n)
	_, _ = f.writer.Write(f.scratch[:])
}

func (f *contentFingerprinter) text(tag byte, value string) {
	_ = f.writer.WriteByte(tag)
	f.number(uint64(len(value)))
	_, _ = f.writer.WriteString(value)
	f.size += len(value)
}

// The type tags, fixed-width lengths, sorted map keys and ordered arrays form
// an unambiguous encoding of JSON decoder values. This fingerprint is NOT the
// artifact hash or a new wire format. Distinct Go values that marshal to the
// same JSON may miss the cache, which is harmless. Unsupported/custom values
// and deep/cyclic structures always use encoding/json's original behavior.
func (f *contentFingerprinter) value(value any, depth int) bool {
	if depth > 128 {
		return false
	}
	f.size++
	switch v := value.(type) {
	case nil:
		_ = f.writer.WriteByte('n')
	case bool:
		if v {
			_ = f.writer.WriteByte('t')
		} else {
			_ = f.writer.WriteByte('f')
		}
	case string:
		f.text('s', v)
	case float64:
		_ = f.writer.WriteByte('d')
		f.number(math.Float64bits(v))
	case json.Number:
		f.text('j', string(v))
	case []any:
		if v == nil {
			_ = f.writer.WriteByte('z')
			return true
		}
		_ = f.writer.WriteByte('a')
		f.number(uint64(len(v)))
		for _, item := range v {
			if !f.value(item, depth+1) {
				return false
			}
		}
	case map[string]any:
		if v == nil {
			_ = f.writer.WriteByte('v')
			return true
		}
		_ = f.writer.WriteByte('m')
		f.number(uint64(len(v)))
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			f.text('k', key)
			if !f.value(v[key], depth+1) {
				return false
			}
		}
	default:
		return false
	}
	return true
}
