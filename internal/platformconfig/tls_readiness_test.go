package platformconfig

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestTLSReadinessPolicyIsBoundedVersionedAndImmutable(t *testing.T) {
	req := dnsQueryFixture()
	req.Policy.TLSReadiness = &ReadinessProbePolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 5, FactFreshnessSeconds: 120, MaxConcurrency: 8, MaxProbes: 4096}
	rebindPlacement(&req)
	before, _ := json.Marshal(req)
	compiled, err := Compile(req)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := Compile(req)
	if err != nil || !reflect.DeepEqual(compiled.TLSArtifact.Content, replay.TLSArtifact.Content) {
		t.Fatal("TLS policy replay changed", err)
	}
	after, _ := json.Marshal(req)
	if string(before) != string(after) {
		t.Fatal("compiler mutated TLS policy")
	}
	normalized := NormalizePolicySnapshot(req.Policy)
	normalized.TLSReadiness.MaxConcurrency = 4
	if req.Policy.TLSReadiness.MaxConcurrency != 8 {
		t.Fatal("normalized TLS policy aliases caller")
	}
	req.Policy.TLSReadiness.FactFreshnessSeconds = 90
	rebindPlacement(&req)
	changed, err := Compile(req)
	if err != nil {
		t.Fatal(err)
	}
	if changed.Lineage.PolicyDigest == compiled.Lineage.PolicyDigest || changed.TLSArtifact.Generation == compiled.TLSArtifact.Generation || changed.Lineage.IntentDigest != compiled.Lineage.IntentDigest {
		t.Fatal("TLS observation policy is not independently versioned")
	}
	for name, mutate := range map[string]func(*ReadinessProbePolicy){
		"zero concurrency":      func(p *ReadinessProbePolicy) { p.MaxConcurrency = 0 },
		"unbounded concurrency": func(p *ReadinessProbePolicy) { p.MaxConcurrency = 17 },
		"timeout":               func(p *ReadinessProbePolicy) { p.ProbeTimeoutSeconds = 11 },
		"short freshness":       func(p *ReadinessProbePolicy) { p.FactFreshnessSeconds = 30 },
		"long freshness":        func(p *ReadinessProbePolicy) { p.FactFreshnessSeconds = 601 },
		"zero probes":           func(p *ReadinessProbePolicy) { p.MaxProbes = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			p := *req.Policy.TLSReadiness
			mutate(&p)
			invalid := req
			invalid.Policy.TLSReadiness = &p
			rebindPlacement(&invalid)
			if _, err := Compile(invalid); err == nil {
				t.Fatal("unbounded TLS policy accepted")
			}
		})
	}
}
