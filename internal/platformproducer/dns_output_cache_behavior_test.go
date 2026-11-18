package platformproducer_test

import (
	"encoding/json"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/testfixture/celldns"
)

var dnsOutputObservationRun atomic.Uint64

func TestDNSOutputValidationObservation(t *testing.T) {
	p, request, source := dnsProducerFixture(t)
	p.Generation = fmt.Sprintf("dns-output-observation-%d", dnsOutputObservationRun.Add(1))
	projected, err := platformproducer.ComposeDNSProjection(p, *request.PreviousTrafficPublication, source.Intent, source.Policy, source.RuntimeSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	child := celldns.Compile(t, projected).DNSArtifact
	type event struct {
		stage platformproducer.DNSOutputValidationStage
		cache platformproducer.DNSOutputCacheResult
	}
	var observations []event
	observe := func(stage platformproducer.DNSOutputValidationStage, elapsed time.Duration, cache platformproducer.DNSOutputCacheResult) {
		if elapsed < 0 || stage.String() == "" {
			t.Fatal("unexpected DNS output timing", stage, elapsed)
		}
		observations = append(observations, event{stage, cache})
	}
	check := func(a model.PlatformArtifact, want []event) error {
		t.Helper()
		observations = nil
		err := platformproducer.ValidateDNSProducerOutputObserved(p, a, observe)
		if !slices.Equal(observations, want) {
			t.Fatalf("observed %v, want %v (validation error: %v)", observations, want, err)
		}
		return err
	}
	none := platformproducer.DNSOutputCacheUnobserved
	policy := event{platformproducer.DNSOutputPolicy, none}
	fingerprint := event{platformproducer.DNSOutputFingerprint, none}
	cacheMiss := event{platformproducer.DNSOutputCacheLookup, platformproducer.DNSOutputCacheMiss}
	childDecode := event{platformproducer.DNSOutputChildDecode, none}
	if err := check(child, []event{
		policy, fingerprint, cacheMiss, childDecode,
		{platformproducer.DNSOutputAuthority, none}, {platformproducer.DNSOutputPredecessorDecode, none},
		{platformproducer.DNSOutputPolicyComparison, none}, {platformproducer.DNSOutputMembership, none},
		{platformproducer.DNSOutputHandoffComparison, none}, {platformproducer.DNSOutputCacheStore, none},
	}); err != nil {
		t.Fatal(err)
	}
	if err := check(child, []event{policy, fingerprint, {platformproducer.DNSOutputCacheLookup, platformproducer.DNSOutputCacheHit}}); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(child)
	if err != nil {
		t.Fatal(err)
	}
	var changed model.PlatformArtifact
	if err := json.Unmarshal(raw, &changed); err != nil {
		t.Fatal(err)
	}
	changed.Content["generation"] = "unexpected-generation" // Keep the original claimed hash.
	want := platformproducer.ValidateDNSProducerOutput(p, changed)
	got := check(changed, []event{policy, fingerprint, cacheMiss, childDecode})
	if want == nil || got == nil || got.Error() != want.Error() {
		t.Fatalf("changed content changed validation result: plain %v, observed %v", want, got)
	}
	changed.Content["unknown"] = json.RawMessage("true")
	want = platformproducer.ValidateDNSProducerOutput(p, changed)
	got = check(changed, []event{policy, {platformproducer.DNSOutputFingerprint, platformproducer.DNSOutputCacheNotApplicable}, childDecode})
	if want == nil || got == nil || got.Error() != want.Error() {
		t.Fatalf("non-native input changed validation result: plain %v, observed %v", want, got)
	}
	p.DNSSource = nil
	if err := check(child, []event{policy}); err == nil {
		t.Fatal("invalid policy accepted")
	}
}

func TestDNSOutputValidationReuseReadsActualInputs(t *testing.T) {
	p, request, source := dnsProducerFixture(t)
	projected, err := platformproducer.ComposeDNSProjection(p, *request.PreviousTrafficPublication, source.Intent, source.Policy, source.RuntimeSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	child := celldns.Compile(t, projected).DNSArtifact
	if _, ok := platformconfig.DNSConfigurationValidationFingerprint(child); !ok {
		t.Fatal("fixture does not exercise large-content reuse")
	}
	for i := 0; i < 2; i++ {
		if err := platformproducer.ValidateDNSProducerOutput(p, child); err != nil {
			t.Fatal(err)
		}
	}
	baseline, _ := json.Marshal(child)
	for _, scenario := range []string{"lineage", "source intent", "policy bounds", "unrelated invalid JSON", "unknown field", "case alias", "producer authority", "producer members", "producer activation"} {
		t.Run(scenario, func(t *testing.T) {
			var changed model.PlatformArtifact
			json.Unmarshal(baseline, &changed)
			policyRaw, _ := json.Marshal(p)
			var policy platformproducer.Policy
			json.Unmarshal(policyRaw, &policy)
			switch scenario {
			case "lineage":
				changed.Metadata["intent_generation"] = "changed"
			case "source intent":
				changed.Content["cell_dns_source"].(map[string]any)["intent"].(map[string]any)["generation"] = "changed"
			case "policy bounds":
				changed.Content["policy"].(map[string]any)["max_stale_seconds"] = float64(1)
			case "unrelated invalid JSON":
				changed.Content["unrelated"] = make(chan int)
			case "unknown field":
				changed.Content["unknown"] = true
			case "case alias":
				changed.Content["Policy"] = false
			case "producer authority":
				policy.AuthorityCellID = "cell-foreign"
			case "producer members":
				policy.DNSSource.DNSNodeIDs = []string{"foreign-node"}
			case "producer activation":
				policy.DNSSource.ProducerPolicy.ReleaseID = "foreign-activation"
			}
			// Deliberately keep the original claimed content hash and generation.
			if platformproducer.ValidateDNSProducerOutput(policy, changed) == nil {
				t.Fatal("warm pure validation accepted changed content or policy")
			}
		})
	}
	after, _ := json.Marshal(child)
	if string(after) != string(baseline) {
		t.Fatal("validation mutated caller's artifact")
	}
	if err := platformproducer.ValidateDNSProducerOutput(p, child); err != nil {
		t.Fatal("invalid reads poisoned original validation", err)
	}
}

func BenchmarkDNSProducerOutputValidatedReuse(b *testing.B) {
	p, request, source := dnsProducerFixture(b)
	projected, err := platformproducer.ComposeDNSProjection(p, *request.PreviousTrafficPublication, source.Intent, source.Policy, source.RuntimeSnapshot)
	if err != nil {
		b.Fatal(err)
	}
	child := celldns.Compile(b, projected).DNSArtifact
	if err := platformproducer.ValidateDNSProducerOutput(p, child); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := platformproducer.ValidateDNSProducerOutput(p, child); err != nil {
			b.Fatal(err)
		}
	}
}
