package platformsafety

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
)

func TestContentHashCachePreservesJSONEncoding(t *testing.T) {
	values := []any{nil, true, false, "", "a\x00b", "<>&\u2028\u2029", string([]byte{0xff}), float64(0), math.Copysign(0, -1), 1.234e30, math.SmallestNonzeroFloat64, math.NaN(), math.Inf(1), json.Number("1.00"), json.Number("invalid"), []any(nil), []any{}, []any{"a", "bc"}, []any{"ab", "c"}, map[string]any(nil), map[string]any{}, map[string]any{"a": "bc"}, map[string]any{"ab": "c"}, []string{"typed", "slice"}, struct{ Value string }{"custom"}}
	cache := newContentHashCache(256)
	for i, value := range values {
		content := map[string]any{"payload": strings.Repeat("x", 4096), "value": value}
		want := canonicalArtifactContentHash(content)
		for j := 0; j < 2; j++ {
			if got := cache.digest(content); got != want {
				t.Fatalf("value %d pass %d: digest %q != JSON digest %q", i, j, got, want)
			}
		}
	}
	if cache.digest(nil) != "" {
		t.Fatal("nil artifact content accepted")
	}
	cyclic := map[string]any{}
	cyclic["self"] = cyclic
	if cache.digest(cyclic) != "" {
		t.Fatal("cycle accepted")
	}
	deep := map[string]any{"leaf": strings.Repeat("x", 4096)}
	for i := 0; i < 150; i++ {
		deep = map[string]any{"nested": deep}
	}
	if cache.digest(deep) != canonicalArtifactContentHash(deep) {
		t.Fatal("deep valid JSON did not use compatible fallback")
	}
}

func TestContentFingerprintIsUnambiguousAndOrderIndependent(t *testing.T) {
	values := []any{nil, true, false, "1", float64(1), json.Number("1"), []any(nil), []any{}, []any{"a", "bc"}, []any{"ab", "c"}, map[string]any(nil), map[string]any{}, map[string]any{"a": "bc"}, map[string]any{"ab": "c"}, map[string]any{"a": "", "b": ""}, map[string]any{"ak": "b"}}
	seen := map[[32]byte]bool{}
	for _, value := range values {
		key, _, ok := fingerprintContent(map[string]any{"v": value})
		if !ok || seen[key] {
			t.Fatalf("ambiguous fingerprint for %#v", value)
		}
		seen[key] = true
	}
	a, _, _ := fingerprintContent(map[string]any{"a": float64(1), "b": []any{true, "x"}})
	b, _, _ := fingerprintContent(map[string]any{"b": []any{true, "x"}, "a": float64(1)})
	if a != b {
		t.Fatal("map insertion order changed the fingerprint")
	}
}

func TestContentHashCacheDoesNotCacheIntegrityAuthority(t *testing.T) {
	for _, change := range []string{"nested content", "array order", "claimed hash", "schema", "signature", "trust revocation", "status"} {
		t.Run(change, func(t *testing.T) {
			artifact := testSignedPlatformArtifact(t, map[string]any{"payload": strings.Repeat("x", 4096), "nested": map[string]any{"enabled": true}, "array": []any{"a", "b"}})
			keys := testPlatformSafetyKeyring()
			for i := 0; i < 2; i++ {
				if !EvaluateArtifactIntegrity(artifact, keys).Pass {
					t.Fatal("valid artifact rejected")
				}
			}
			switch change {
			case "nested content":
				artifact.Content["nested"].(map[string]any)["enabled"] = false
			case "array order":
				artifact.Content["array"] = []any{"b", "a"}
			case "claimed hash":
				artifact.ContentHash = "sha256:" + strings.Repeat("0", 64)
			case "schema":
				artifact.SchemaVersion = "unsupported"
			case "signature":
				artifact.Provenance.Signature = "invalid"
			case "trust revocation":
				keys = bundleauth.NewKeyring("platform-safety-test-key", "platform-safety-test", "", "", []string{"platform-safety-test"})
			case "status":
				artifact.Status = model.PlatformArtifactStatusDraft
				if EvaluateArtifactRelease(artifact, model.PlatformArtifactReleaseChannelFull, "stable", "", 0, keys).Pass {
					t.Fatal("cached content authorized a draft release")
				}
				return
			}
			if EvaluateArtifactIntegrity(artifact, keys).Pass {
				t.Fatal("cached content bypassed fresh integrity/trust checks")
			}
		})
	}
}

func TestContentHashCacheBoundedAndConcurrent(t *testing.T) {
	cache := newContentHashCache(2)
	content := map[string]any{"payload": strings.Repeat("x", 4096), "nested": []any{map[string]any{"x": true}}}
	want := canonicalArtifactContentHash(content)
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if cache.digest(content) != want {
				t.Error("concurrent hash differs")
			}
		}()
	}
	wg.Wait()
	for i := 0; i < 10; i++ {
		content["generation"] = float64(i)
		if cache.digest(content) != canonicalArtifactContentHash(content) || len(cache.values) > 2 || len(cache.keys) > 2 {
			t.Fatal("hash or cache bound differs after eviction")
		}
	}
	cache = newContentHashCache(2)
	content["invalid"] = math.NaN()
	for i := 0; i < 2; i++ {
		if cache.digest(content) != "" || len(cache.values) != 0 {
			t.Fatal("failed canonical encoding cached")
		}
	}
}

func FuzzContentHashCacheMatchesJSON(f *testing.F) {
	f.Add([]byte(`{"a":[true,null,1.25,"<>&"],"b":{"n":-0}}`))
	f.Add([]byte(`{"a":"x","b":"y"}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 1<<20 {
			t.Skip()
		}
		var content map[string]any
		if json.Unmarshal(raw, &content) != nil || content == nil {
			return
		}
		content["padding"] = strings.Repeat("x", 4096)
		cache := newContentHashCache(2)
		want := canonicalArtifactContentHash(content)
		if cache.digest(content) != want || cache.digest(content) != want {
			t.Fatal("memoized hash differs from canonical JSON")
		}
	})
}

func BenchmarkArtifactContentHash(b *testing.B) {
	structured := make([]any, 2000)
	for i := range structured {
		structured[i] = map[string]any{"id": fmt.Sprint(i), "ready": true, "addresses": []any{"192.0.2.1", "2001:db8::1"}, "proof": map[string]any{"digest": strings.Repeat("a", 64), "payload": strings.Repeat("x", 1024)}}
	}
	for name, content := range map[string]map[string]any{"small": {"value": true}, "string-4MiB": {"payload": strings.Repeat("x", 4<<20)}, "structured": {"facts": structured}} {
		for _, cached := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/cached=%t", name, cached), func(b *testing.B) {
				cache := newContentHashCache(256)
				cache.digest(content)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if cached {
						cache.digest(content)
					} else {
						canonicalArtifactContentHash(content)
					}
				}
			})
		}
	}
}
