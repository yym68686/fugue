package platformsafety

import (
	"bufio"
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

// Frozen pre-optimization encoding. Keep this independent of the reusable
// workspace so tests catch recursion, reuse and early-return mistakes.
type legacyContentFingerprinter struct {
	hash    hash.Hash
	writer  *bufio.Writer
	scratch [8]byte
	size    int
}

func legacyContentFingerprint(content map[string]any) ([32]byte, int, bool) {
	h := sha256.New()
	f := legacyContentFingerprinter{hash: h, writer: bufio.NewWriterSize(h, 8192)}
	_, _ = f.writer.WriteString("fugue-json-tree/v1\x00")
	ok := f.value(content, 0)
	_ = f.writer.Flush()
	var key [32]byte
	h.Sum(key[:0])
	return key, f.size, ok
}

func nestedFingerprintFixture(width, depth int) map[string]any {
	root := map[string]any{"value": strings.Repeat("x", 4096)}
	for level := 0; level < depth; level++ {
		child := root
		root = make(map[string]any, width+2)
		for i := width - 1; i >= 0; i-- {
			root[fmt.Sprintf("key-%06d", i)] = []any{float64(i), nil, true, "<&>", map[string]any{"leaf": false}}
		}
		root["a-child"] = child
		root["z-after-child"] = "must remain after recursively grown workspace"
	}
	return root
}
func TestContentFingerprintWorkspaceMatchesLegacy(t *testing.T) {
	values := []any{nil, map[string]any(nil), []any(nil), []any{}, math.Copysign(0, -1), math.NaN(), math.Inf(1), json.Number("1.00"), json.Number("invalid"), []string{"unsupported"}, string([]byte{0xff}), map[string]any{"nested": []any{true, nil, "x"}}}
	cases := []map[string]any{nil, {}, nestedFingerprintFixture(8, 8), nestedFingerprintFixture(64, 8), nestedFingerprintFixture(6000, 2)}
	for _, value := range values {
		cases = append(cases, map[string]any{"a": value, "z": "tail"})
	}
	cyclic := map[string]any{}
	cyclic["self"] = cyclic
	cases = append(cases, cyclic)
	for pass := 0; pass < 3; pass++ {
		for i, v := range cases {
			want, ws, wo := legacyContentFingerprint(v)
			got, gs, goOK := fingerprintContent(v)
			if got != want || gs != ws || goOK != wo {
				t.Fatalf("pass=%d case=%d fingerprint differs: %x/%d/%t != %x/%d/%t", pass, i, got, gs, goOK, want, ws, wo)
			}
		}
	}
}
func FuzzContentFingerprintWorkspaceMatchesLegacy(f *testing.F) {
	f.Add([]byte(`{"a":{"b":[1,null,true,{"c":"x"}]},"z":false}`))
	f.Add([]byte(`{"b":{},"a":[],"value":-0}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 1<<18 {
			t.Skip()
		}
		var content map[string]any
		if json.Unmarshal(raw, &content) != nil {
			return
		}
		want, ws, wo := legacyContentFingerprint(content)
		got, gs, goOK := fingerprintContent(content)
		if got != want || gs != ws || goOK != wo {
			t.Fatal("fingerprint differs from existing encoding")
		}
	})
}
func BenchmarkContentFingerprintWorkspace(b *testing.B) {
	for name, shape := range map[string][2]int{"nested": {8, 8}, "wide": {6000, 2}} {
		content := nestedFingerprintFixture(shape[0], shape[1])
		fingerprintContent(content)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				fingerprintContent(content)
			}
		})
	}
}

func (f *legacyContentFingerprinter) number(n uint64) {
	binary.LittleEndian.PutUint64(f.scratch[:], n)
	_, _ = f.writer.Write(f.scratch[:])
}

func (f *legacyContentFingerprinter) text(tag byte, value string) {
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
func (f *legacyContentFingerprinter) value(value any, depth int) bool {
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

// Check a specific workspace: pool Get may return another instance and hide
// retained references from an early error in the previous call.
func TestContentFingerprintWorkspaceReleasesKeys(t *testing.T) {
	h := sha256.New()
	w := &contentFingerprinter{hash: h, writer: bufio.NewWriterSize(h, 8192)}
	cyclic := map[string]any{}
	cyclic["cycle"] = cyclic
	for _, value := range []any{nestedFingerprintFixture(64, 8), nestedFingerprintFixture(6000, 2), map[string]any{"a": make(chan int), "z": "unvisited"}, cyclic} {
		w.value(value, 0)
		if w.keyUsed != 0 {
			t.Fatal("recursive workspace offset leaked")
		}
		for _, key := range w.keys {
			if key != "" {
				t.Fatal("workspace retained caller key")
			}
		}
	}
	content := nestedFingerprintFixture(64, 8)
	want, ws, wo := legacyContentFingerprint(content)
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 10; n++ {
				got, gs, ok := fingerprintContent(content)
				if got != want || gs != ws || ok != wo {
					t.Error("concurrent fingerprint differs")
				}
			}
		}()
	}
	wg.Wait()
}
func BenchmarkContentFingerprintStructured(b *testing.B) {
	items := make([]any, 2000)
	for i := range items {
		items[i] = map[string]any{"id": fmt.Sprint(i), "ready": true, "proof": map[string]any{"generation": "v1", "digest": strings.Repeat("a", 64), "body": strings.Repeat("x", 1024)}, "addresses": []any{"192.0.2.1", "2001:db8::1"}}
	}
	content := map[string]any{"items": items}
	for _, optimized := range []bool{false, true} {
		b.Run(fmt.Sprintf("optimized=%t", optimized), func(b *testing.B) {
			fingerprintContent(content)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if optimized {
					fingerprintContent(content)
				} else {
					legacyContentFingerprint(content)
				}
			}
		})
	}
}
