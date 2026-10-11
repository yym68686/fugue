package platformconfig

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"hash"
	"math"
	"sort"
)

// This is a private cache identity, not a persisted/signature digest. It walks
// actual JSON trees without creating a full policy-sized encoding on cache hits.
// Type/length framing makes mutations, absent values and nested boundaries
// distinguishable. Unsupported typed values use encoding/json and fail closed.
func policyTreeFingerprint(value any) ([32]byte, int, bool) {
	w := policyTreeHasher{hash: sha256.New(), buffer: make([]byte, 0, 32<<10)}
	if !w.tree(value, 0) {
		return [32]byte{}, 0, false
	}
	w.flush()
	var key [32]byte
	w.hash.Sum(key[:0])
	return key, w.size, true
}

type policyTreeHasher struct {
	hash    hash.Hash
	buffer  []byte
	size    int
	scratch [9]byte
}

func (w *policyTreeHasher) flush() { w.hash.Write(w.buffer); w.buffer = w.buffer[:0] }
func (w *policyTreeHasher) bytes(raw []byte) {
	for len(raw) > 0 {
		n := min(cap(w.buffer)-len(w.buffer), len(raw))
		w.buffer = append(w.buffer, raw[:n]...)
		raw = raw[n:]
		if len(w.buffer) == cap(w.buffer) {
			w.flush()
		}
	}
}
func (w *policyTreeHasher) header(tag byte, length int) {
	w.scratch[0] = tag
	binary.LittleEndian.PutUint64(w.scratch[1:], uint64(length))
	w.bytes(w.scratch[:])
	w.size += length + 1
}
func (w *policyTreeHasher) text(tag byte, value string) {
	w.header(tag, len(value))
	for len(value) > 0 {
		n := min(cap(w.buffer)-len(w.buffer), len(value))
		w.buffer = append(w.buffer, value[:n]...)
		value = value[n:]
		if len(w.buffer) == cap(w.buffer) {
			w.flush()
		}
	}
}
func (w *policyTreeHasher) write(tag byte, raw []byte) { w.header(tag, len(raw)); w.bytes(raw) }
func (w *policyTreeHasher) tree(value any, depth int) bool {
	if depth > 256 {
		return false
	}
	switch v := value.(type) {
	case nil:
		w.write('n', nil)
	case string:
		w.text('s', v)
	case bool:
		if v {
			w.write('t', nil)
		} else {
			w.write('f', nil)
		}
	case float64:
		var raw [8]byte
		binary.LittleEndian.PutUint64(raw[:], math.Float64bits(v))
		w.write('d', raw[:])
	case map[string]any:
		if v == nil {
			w.write('m', nil)
			return true
		}
		w.write('{', nil)
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			w.text('k', key)
			if !w.tree(v[key], depth+1) {
				return false
			}
		}
		w.write('}', nil)
	case []any:
		if v == nil {
			w.write('a', nil)
			return true
		}
		w.write('[', nil)
		for _, item := range v {
			if !w.tree(item, depth+1) {
				return false
			}
		}
		w.write(']', nil)
	default:
		raw, err := json.Marshal(value)
		if err != nil {
			return false
		}
		w.write('j', raw)
	}
	return true
}
