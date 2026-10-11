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
	h := sha256.New()
	size := 0
	if !hashPolicyTree(h, value, 0, &size) {
		return [32]byte{}, 0, false
	}
	var key [32]byte
	h.Sum(key[:0])
	return key, size, true
}
func hashPolicyTree(h hash.Hash, value any, depth int, size *int) bool {
	if depth > 256 {
		return false
	}
	write := func(tag byte, raw []byte) {
		var header [9]byte
		header[0] = tag
		binary.LittleEndian.PutUint64(header[1:], uint64(len(raw)))
		h.Write(header[:])
		h.Write(raw)
		*size += len(raw) + 1
	}
	switch v := value.(type) {
	case nil:
		write('n', nil)
	case string:
		write('s', []byte(v))
	case bool:
		if v {
			write('t', nil)
		} else {
			write('f', nil)
		}
	case float64:
		var raw [8]byte
		binary.LittleEndian.PutUint64(raw[:], math.Float64bits(v))
		write('d', raw[:])
	case map[string]any:
		if v == nil {
			write('m', nil)
			return true
		}
		write('{', nil)
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			write('k', []byte(key))
			if !hashPolicyTree(h, v[key], depth+1, size) {
				return false
			}
		}
		write('}', nil)
	case []any:
		if v == nil {
			write('a', nil)
			return true
		}
		write('[', nil)
		for _, item := range v {
			if !hashPolicyTree(h, item, depth+1, size) {
				return false
			}
		}
		write(']', nil)
	default:
		raw, err := json.Marshal(value)
		if err != nil {
			return false
		}
		write('j', raw)
	}
	return true
}
