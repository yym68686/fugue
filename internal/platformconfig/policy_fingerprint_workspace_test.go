package platformconfig

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash"
	"math"
	"sort"
	"strings"
	"sync"
	"testing"
)

func policyFingerprintWorkspaceFixture(width, depth int) map[string]any {
	root := map[string]any{"payload": strings.Repeat("x", 4096)}
	for level := 0; level < depth; level++ {
		child := root
		root = make(map[string]any, width+2)
		for i := width - 1; i >= 0; i-- {
			root[fmt.Sprintf("key-%06d", i)] = []any{float64(i), nil, true, "<&>", map[string]any{"leaf": false}}
		}
		root["a-child"] = child
		root["z-after-child"] = "verify parent workspace after recursive growth"
	}
	return root
}
func TestPolicyFingerprintWorkspaceMatchesLegacy(t *testing.T) {
	cases := []any{nil, map[string]any(nil), map[string]any{}, []any(nil), []any{}, math.Copysign(0, -1), math.NaN(), math.Inf(1), json.Number("1.00"), json.Number("invalid"), []string{"typed"}, struct{ Value string }{"custom"}, string([]byte{0xff}), make(chan int), policyFingerprintWorkspaceFixture(8, 8), policyFingerprintWorkspaceFixture(64, 8), policyFingerprintWorkspaceFixture(6000, 2)}
	deep := map[string]any{"leaf": true}
	for i := 0; i < 260; i++ {
		deep = map[string]any{"nested": deep}
	}
	cases = append(cases, deep)
	for pass := 0; pass < 3; pass++ {
		for i, v := range cases {
			want, ws, wo := legacyPolicyTreeFingerprint(v)
			got, gs, goOK := policyTreeFingerprint(v)
			if got != want || gs != ws || goOK != wo {
				t.Fatalf("pass=%d case=%d fingerprint differs", pass, i)
			}
		}
	}
	content := policyFingerprintWorkspaceFixture(8, 8)
	want, ws, wo := legacyPolicyTreeFingerprint(content)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 20; n++ {
				got, gs, goOK := policyTreeFingerprint(content)
				if got != want || gs != ws || goOK != wo {
					t.Error("concurrent fingerprint differs")
				}
			}
		}()
	}
	wg.Wait()
}
func FuzzPolicyFingerprintWorkspaceMatchesLegacy(f *testing.F) {
	f.Add([]byte(`{"a":{"b":[1,null,true,{"c":"x"}]},"z":false}`))
	f.Add([]byte(`{"b":{},"a":[],"value":-0}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 1<<18 {
			t.Skip()
		}
		var value any
		if json.Unmarshal(raw, &value) != nil {
			return
		}
		want, ws, wo := legacyPolicyTreeFingerprint(value)
		got, gs, goOK := policyTreeFingerprint(value)
		if got != want || gs != ws || goOK != wo {
			t.Fatal("fingerprint differs from existing encoding")
		}
	})
}
func BenchmarkPolicyFingerprintWorkspace(b *testing.B) {
	for name, shape := range map[string][2]int{"nested": {8, 8}, "wide": {6000, 2}} {
		content := policyFingerprintWorkspaceFixture(shape[0], shape[1])
		policyTreeFingerprint(content)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				policyTreeFingerprint(content)
			}
		})
	}
}

// Frozen pre-optimization encoder for exact private-key compatibility checks.
func legacyPolicyTreeFingerprint(value any) ([32]byte, int, bool) {
	w := legacyPolicyTreeHasher{hash: sha256.New(), buffer: make([]byte, 0, 32<<10)}
	if !w.tree(value, 0) {
		return [32]byte{}, 0, false
	}
	w.flush()
	var key [32]byte
	w.hash.Sum(key[:0])
	return key, w.size, true
}

type legacyPolicyTreeHasher struct {
	hash    hash.Hash
	buffer  []byte
	size    int
	scratch [9]byte
}

func (w *legacyPolicyTreeHasher) flush() { w.hash.Write(w.buffer); w.buffer = w.buffer[:0] }
func (w *legacyPolicyTreeHasher) bytes(raw []byte) {
	for len(raw) > 0 {
		n := min(cap(w.buffer)-len(w.buffer), len(raw))
		w.buffer = append(w.buffer, raw[:n]...)
		raw = raw[n:]
		if len(w.buffer) == cap(w.buffer) {
			w.flush()
		}
	}
}
func (w *legacyPolicyTreeHasher) header(tag byte, length int) {
	w.scratch[0] = tag
	binary.LittleEndian.PutUint64(w.scratch[1:], uint64(length))
	w.bytes(w.scratch[:])
	w.size += length + 1
}
func (w *legacyPolicyTreeHasher) text(tag byte, value string) {
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
func (w *legacyPolicyTreeHasher) write(tag byte, raw []byte) { w.header(tag, len(raw)); w.bytes(raw) }
func (w *legacyPolicyTreeHasher) tree(value any, depth int) bool {
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

func TestPolicyFingerprintWorkspaceReleasesKeys(t *testing.T) {
	w := &policyTreeHasher{hash: sha256.New(), buffer: make([]byte, 0, 32<<10)}
	cyclic := map[string]any{}
	cyclic["cycle"] = cyclic
	for _, value := range []any{policyFingerprintWorkspaceFixture(64, 8), policyFingerprintWorkspaceFixture(6000, 2), map[string]any{"a": make(chan int), "z": "unvisited"}, cyclic} {
		w.tree(value, 0)
		if w.keyUsed != 0 {
			t.Fatal("recursive workspace offset leaked")
		}
		for _, key := range w.keys {
			if key != "" {
				t.Fatal("workspace retained caller key")
			}
		}
		if cap(w.buffer) != 32<<10 {
			t.Fatal("pooled byte buffer grew")
		}
	}
}
func BenchmarkPolicyFingerprintStructured(b *testing.B) {
	items := make([]any, 2000)
	for i := range items {
		items[i] = map[string]any{"id": fmt.Sprint(i), "ready": true, "proof": map[string]any{"generation": "v1", "digest": strings.Repeat("a", 64), "body": strings.Repeat("x", 1024)}, "addresses": []any{"192.0.2.1", "2001:db8::1"}}
	}
	content := map[string]any{"items": items}
	for _, optimized := range []bool{false, true} {
		b.Run(fmt.Sprintf("optimized=%t", optimized), func(b *testing.B) {
			policyTreeFingerprint(content)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if optimized {
					policyTreeFingerprint(content)
				} else {
					legacyPolicyTreeFingerprint(content)
				}
			}
		})
	}
}
