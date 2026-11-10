package platformconfig

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
)

func TestDNSPayloadCacheBoundedConcurrentAndPreservesPartialErrors(t *testing.T) {
	request := dnsReadinessFixture()
	for i := 0; i < 256; i++ {
		request.Intent.DNS = append(request.Intent.DNS, DNSIntent{Hostname: fmt.Sprintf("entry-%d.example.test", i), Type: "TXT", TTL: 60, Values: []string{"synthetic-answer"}})
	}
	compiled, err := Compile(request)
	if err != nil {
		t.Fatal(err)
	}
	a := compiled.DNSArtifact
	a.ScopeKey = a.Scope.Key
	if _, size, ok := dnsValidationArtifactKey(a); !ok || size < 16<<10 {
		t.Fatal("fixture does not exercise cache", size)
	}
	var cache dnsPayloadCache
	for i := 0; i < 20; i++ {
		a.ID = fmt.Sprint(i)
		if _, err := cache.decode(a); err != nil {
			t.Fatal(err)
		}
	}
	if len(cache.entries) != 16 || len(cache.order) != 16 || cache.bytes > 32<<20 {
		t.Fatal("typed payload budget exceeded")
	}
	want, err := decodeDNSArtifactUncached(a)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := cache.decode(a)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Errorf("concurrent payload differs: %v", err)
				return
			}
			got.ReadinessPlan.Probes[0].RouteDigest = "caller mutation"
		}()
	}
	wg.Wait()
	// Invalid inputs keep the decoder's partially populated output and error;
	// a failed validation is never retained as a positive typed cache entry.
	a.Metadata["intent_digest"] = "changed"
	want, wantErr := decodeDNSArtifactUncached(a)
	got, gotErr := cache.decode(a)
	if wantErr == nil || fmt.Sprint(wantErr) != fmt.Sprint(gotErr) || !reflect.DeepEqual(want, got) || len(cache.entries) != 16 {
		t.Fatal("partial error semantics changed", wantErr, gotErr)
	}
}
