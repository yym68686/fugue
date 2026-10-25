package platformconfig

import (
	"crypto/sha256"
	"fmt"
	"fugue/internal/model"
	"strings"
	"sync"
	"testing"
)

func TestDNSValidationCacheBoundedAndConcurrent(t *testing.T) {
	var c dnsValidationCache
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 128; j++ {
				key := sha256.Sum256([]byte(fmt.Sprintf("%d/%d", i, j)))
				c.remember(key)
				c.contains(key)
			}
		}(i)
	}
	wg.Wait()
	c.Lock()
	defer c.Unlock()
	if len(c.keys) != 64 {
		t.Fatalf("cache contains %d keys; want bounded 64", len(c.keys))
	}
}

func TestDNSFingerprintDoesNotInvokeCustomMarshalers(t *testing.T) {
	calls := 0
	v := map[string]any{"policy": countedDNSPlanJSON{raw: []byte(`{}`), calls: &calls}}
	if _, _, ok := fingerprintPolicyTree(v, true); ok || calls != 0 {
		t.Fatal("cache probe executed a custom marshaler")
	}
	if _, _, ok := policyTreeFingerprint(v); !ok || calls != 1 {
		t.Fatal("existing policy fingerprint fallback changed")
	}
	if _, _, ok := fingerprintPolicyTree(v, true); ok || calls != 1 {
		t.Fatal("pooled hasher retained permissive mode")
	}
}

func TestExportedDNSConfigurationFingerprintKeepsFullInputBoundary(t *testing.T) {
	a := model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle, ID: "artifact", ContentHash: "claimed", Content: map[string]any{"padding": strings.Repeat("x", 20<<10)}}
	first, ok := DNSConfigurationValidationFingerprint(a)
	if !ok {
		t.Fatal("native large input not fingerprinted")
	}
	a.Content["padding"] = strings.Repeat("y", 20<<10)
	second, ok := DNSConfigurationValidationFingerprint(a)
	if !ok || first == second {
		t.Fatal("claimed digest hid actual content change")
	}
	a.Content["padding"] = strings.Repeat("x", 20<<10)
	a.Metadata = map[string]string{"intent_digest": "changed"}
	third, ok := DNSConfigurationValidationFingerprint(a)
	if !ok || first == third {
		t.Fatal("envelope omitted from input fingerprint")
	}
	calls := 0
	a.Content["custom"] = countedDNSPlanJSON{raw: []byte(`{}`), calls: &calls}
	if _, ok := DNSConfigurationValidationFingerprint(a); ok || calls != 0 {
		t.Fatal("cache key invoked custom marshaler")
	}
}
