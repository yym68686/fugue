package platformproducer_test

import (
	"encoding/json"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/testfixture/celldns"
)

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
