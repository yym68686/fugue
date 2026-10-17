package platformconfig

import (
	"encoding/json"
	"fmt"
	"testing"

	"fugue/internal/model"
)

type countedDNSPlanJSON struct {
	raw   json.RawMessage
	calls *int
}

func (c countedDNSPlanJSON) MarshalJSON() ([]byte, error) {
	(*c.calls)++
	return c.raw, nil
}

func TestDNSCellPlanSerializesContentOnce(t *testing.T) {
	compiled, err := Compile(dnsReadinessFixture())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(compiled.DNSArtifact.Content["readiness_plan"])
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	compiled.DNSArtifact.Content["readiness_plan"] = countedDNSPlanJSON{raw: raw, calls: &calls}
	if err := ValidateDNSCellPlan(compiled.DNSArtifact); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("one validation serialized the same content %d times; want 1", calls)
	}
}

func BenchmarkDNSCellPlanOrdinaryDecode(b *testing.B) {
	for _, records := range []int{32, 1024} {
		b.Run(fmt.Sprint(records), func(b *testing.B) {
			req := dnsViewRequest()
			req.Intent.DNS = make([]DNSIntent, records)
			for i := range req.Intent.DNS {
				req.Intent.DNS[i] = DNSIntent{Hostname: fmt.Sprintf("record-%d.example.test", i), Type: "A", Values: []string{"192.0.2.1"}, TTL: 60}
			}
			compiled, err := Compile(req)
			if err != nil {
				b.Fatal(err)
			}
			artifact := compiled.DNSArtifact
			if err := ValidateDNSCellPlan(artifact); err != nil {
				b.Fatal(err)
			}
			raw, err := json.Marshal(artifact.Content)
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := ValidateDNSCellPlan(artifact); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestDNSCellPlanRejectsUnencodableWholeContent(t *testing.T) {
	for _, invalid := range []any{func() {}, make(chan int)} {
		artifact := model.PlatformArtifact{Content: map[string]any{"policy": map[string]any{}, "unrelated": invalid}}
		if err := ValidateDNSCellPlan(artifact); err == nil {
			t.Fatal("unencodable content was ignored")
		}
	}
}
