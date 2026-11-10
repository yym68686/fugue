package platformconfig_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"fugue/internal/platformconfig"
)

func TestDNSPayloadCacheRevalidatesInputAndReturnsOwnedTypedPayload(t *testing.T) {
	a := largeDNSValidationArtifact(t)
	raw, _ := json.Marshal(a.Content)
	if len(raw) < 16<<10 {
		t.Fatal("fixture does not exercise payload cache")
	}
	cache := platformconfig.DecodeDNSArtifact
	first, err := cache(a)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.ReadinessPlan.Probes) == 0 {
		t.Fatal("fixture did not decode typed readiness")
	}
	first.ReadinessPlan.Probes[0].RouteDigest = "mutated"
	second, err := cache(a)
	if err != nil || second.ReadinessPlan.Probes[0].RouteDigest == "mutated" {
		t.Fatal("cached typed result was caller-mutable", err)
	}
	a.Content["readiness_plan"].(map[string]any)["records"] = []any{}
	if _, err = cache(a); err == nil {
		t.Fatal("prior validation accepted changed content")
	}
	a.Metadata["intent_digest"] = "changed"
	if _, err = cache(a); err == nil {
		t.Fatal("cache key ignored actual typed envelope")
	}
	// Unsupported native payloads preserve uncached decoder semantics.
	a.Content["foreign"] = make(chan int)
	if _, err = cache(a); err == nil {
		t.Fatal("unsupported payload behavior changed")
	}
}

func BenchmarkDNSPayloadDecodeReuse(b *testing.B) {
	a := largeDNSValidationArtifact(b)
	session := platformconfig.DecodeDNSArtifact
	if _, err := session(a); err != nil {
		b.Fatal(err)
	}
	for _, warm := range []bool{false, true} {
		b.Run(fmt.Sprintf("reuse=%t", warm), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if !warm {
					a.ID = fmt.Sprintf("uncached-payload-%d", i)
				}
				if _, err := session(a); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
