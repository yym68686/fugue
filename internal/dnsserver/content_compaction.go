package dnsserver

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
	"reflect"
	"sort"

	"fugue/internal/model"
)

// Compact only fresh, private decoded JSON trees before publishing them as
// immutable serving content. The index is bounded and dies with the call; no
// artifact authority or old runtime observation is retained by a global cache.
func compactDNSArtifactContent(a *model.PlatformArtifact) {
	interner := dnsContentInterner{objects: make(map[[32]byte]any)}
	value, _, ok := interner.visit(a.Content, 0)
	if ok {
		a.Content = value.(map[string]any)
	}
}

type dnsContentInterner struct{ objects map[[32]byte]any }

const dnsContentInternLimit = 65536

func (c *dnsContentInterner) visit(value any, depth int) (any, [32]byte, bool) {
	if depth > 128 {
		return value, [32]byte{}, false
	}
	h := sha256.New()
	var buffer [8]byte
	text := func(tag byte, s string) {
		h.Write([]byte{tag})
		binary.LittleEndian.PutUint64(buffer[:], uint64(len(s)))
		h.Write(buffer[:])
		h.Write([]byte(s))
	}
	compound := false
	switch v := value.(type) {
	case nil:
		h.Write([]byte{'n'})
	case bool:
		if v {
			h.Write([]byte{'t'})
		} else {
			h.Write([]byte{'f'})
		}
	case string:
		text('s', v)
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return value, [32]byte{}, false
		}
		h.Write([]byte{'d'})
		binary.LittleEndian.PutUint64(buffer[:], math.Float64bits(v))
		h.Write(buffer[:])
	case map[string]any:
		if v == nil {
			h.Write([]byte{'m'})
			break
		}
		compound = true
		h.Write([]byte{'{'})
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			child, digest, ok := c.visit(v[key], depth+1)
			if !ok {
				return value, [32]byte{}, false
			}
			v[key] = child
			text('k', key)
			h.Write(digest[:])
		}
		h.Write([]byte{'}'})
	case []any:
		if v == nil {
			h.Write([]byte{'a'})
			break
		}
		compound = true
		h.Write([]byte{'['})
		for i, child := range v {
			replacement, digest, ok := c.visit(child, depth+1)
			if !ok {
				return value, [32]byte{}, false
			}
			v[i] = replacement
			h.Write(digest[:])
		}
		h.Write([]byte{']'})
	default:
		return value, [32]byte{}, false
	}
	var key [32]byte
	h.Sum(key[:0])
	if compound {
		// Equal digests are just an index: compare actual values before sharing.
		if previous, ok := c.objects[key]; ok && reflect.DeepEqual(previous, value) {
			return previous, key, true
		}
		if len(c.objects) < dnsContentInternLimit {
			c.objects[key] = value
		}
	}
	return value, key, true
}
