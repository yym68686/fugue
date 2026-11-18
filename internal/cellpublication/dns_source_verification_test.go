package cellpublication

import (
	"errors"
	"strings"
	"testing"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
	"fugue/internal/testfixture/celldns"
)

func dnsSourceVerificationFixture(t testing.TB) model.PlatformArtifact {
	t.Helper()
	artifact := celldns.Compile(t, celldns.TransitionRequest(t)).DNSArtifact
	artifact.Content["diagnostic_fixture"] = strings.Repeat("synthetic <&> route\n", 1<<14)
	artifact = celldns.Sign(t, artifact, artifact.ID, artifact.GenerationSequence)
	if _, ok := platformconfig.DNSConfigurationValidationFingerprint(artifact); !ok {
		t.Fatal("expected cacheable native content")
	}
	return artifact
}

func TestDNSRouteSourceCheckReusesOnlyUnchangedContent(t *testing.T) {
	a := dnsSourceVerificationFixture(t)
	original := celldns.Keys()
	keys := bundleauth.NewKeyring("synthetic-replacement-key", "rotated", original.PrimaryKey, original.PrimaryKeyID, nil)
	a, err := platformsafety.SignPlatformArtifact(a, keys)
	if err != nil {
		t.Fatal(err)
	}
	var cache dnsSourceVerificationCache
	for i := 0; i < 2; i++ {
		if err := cache.check(a, keys); err != nil {
			t.Fatal(err)
		}
	}
	if len(cache.entries) != 1 {
		t.Fatalf("expected one reusable verification plan, got %d", len(cache.entries))
	}
	revoked := keys
	revoked.RevokedKeyIDs = map[string]struct{}{original.PrimaryKeyID: {}}
	if err := cache.check(a, revoked); err == nil || errors.Is(err, ErrDNSRouteSourceArtifactIntegrity) {
		t.Fatalf("expected rejection of an embedded signature, got %v", err)
	}
	changedKey := keys
	changedKey.PrimaryKey = "different material with same key identifier"
	if err := cache.check(a, changedKey); !errors.Is(err, ErrDNSRouteSourceArtifactIntegrity) {
		t.Fatalf("cached outer signature ignored active key material: %v", err)
	}
	ineligible := a
	ineligible.CompatibilityFloor = "changed"
	if err := cache.check(ineligible, keys); !errors.Is(err, ErrDNSRouteSourceArtifactIntegrity) {
		t.Fatalf("envelope changed with an unchanged declared digest: %v", err)
	}
	saved := a.Content["diagnostic_fixture"]
	a.Content["diagnostic_fixture"] = "changed actual content, same declared digest"
	if err := cache.check(a, keys); !errors.Is(err, ErrDNSRouteSourceArtifactIntegrity) {
		t.Fatalf("body changed with an unchanged declared digest: %v", err)
	}
	a.Content["diagnostic_fixture"] = saved
	if err := cache.check(a, keys); err != nil {
		t.Fatalf("a failed current trust check poisoned reusable material: %v", err)
	}
	if got := cache.hits.Load(); got != 4 || cache.misses.Load()+cache.bypasses.Load() != 3 {
		t.Fatalf("lookup metrics did not reflect fresh trust checks or misses: %d/%d/%d", got, cache.misses.Load(), cache.bypasses.Load())
	}
}

func BenchmarkDNSRouteSourceArtifactCheck(b *testing.B) {
	a := dnsSourceVerificationFixture(b)
	keys := celldns.Keys()
	b.Run("full", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if !platformsafety.EvaluateArtifactIntegrity(a, keys).Pass {
				b.Fatal("outer artifact rejected")
			}
			if _, err := VerifyDNSArtifact(a, keys); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("same-content-and-envelope", func(b *testing.B) {
		var cache dnsSourceVerificationCache
		if err := cache.check(a, keys); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := cache.check(a, keys); err != nil {
				b.Fatal(err)
			}
		}
	})
}
