package platformconfig_test

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/testfixture/celldns"
)

func largeDNSValidationArtifact(t testing.TB) model.PlatformArtifact {
	t.Helper()
	req := celldns.TransitionRequest(t)
	for i := 0; i < 128; i++ {
		req.Intent.DNS = append(req.Intent.DNS, platformconfig.DNSIntent{Hostname: fmt.Sprintf("record-%d.example.test", i), Type: "TXT", TTL: 60, Values: []string{"synthetic-static-answer"}})
	}
	return celldns.Compile(t, req).DNSArtifact
}

func TestDNSValidationRechecksMutatedInputs(t *testing.T) {
	for _, change := range []string{"plan", "policy", "metadata", "previous content", "previous envelope", "alias"} {
		t.Run(change, func(t *testing.T) {
			a := largeDNSValidationArtifact(t)
			if err := platformconfig.ValidateDNSCellPlan(a); err != nil {
				t.Fatal(err)
			}
			switch change {
			case "plan":
				a.Content["readiness_plan"].(map[string]any)["records"] = []any{}
			case "policy":
				a.Content["policy"].(map[string]any)["minimum_healthy_edges"] = float64(9)
			case "metadata":
				a.Metadata["intent_digest"] = "changed"
			case "previous content":
				a.Content["previous_traffic_publication"].(map[string]any)["route"].(map[string]any)["content"].(map[string]any)["routes"] = []any{}
			case "previous envelope":
				a.Content["previous_traffic_publication"].(map[string]any)["route"].(map[string]any)["scope_key"] = "foreign"
			case "alias":
				a.Content["Policy"] = map[string]any{"canary_weights": []any{float64(101)}}
			}
			if err := platformconfig.ValidateDNSCellPlan(a); err == nil {
				t.Fatal("accepted mutation after successful validation")
			}
		})
	}
}

func TestPreviousValidationRechecksContentAndEnvelope(t *testing.T) {
	for _, change := range []string{"content", "metadata", "reference", "scope", "status"} {
		t.Run(change, func(t *testing.T) {
			a := largeDNSValidationArtifact(t)
			p, err := platformconfig.DecodePreviousTrafficPublication(a)
			if err != nil {
				t.Fatal(err)
			}
			if err = platformconfig.ValidatePreviousTrafficPublication(*p); err != nil {
				t.Fatal(err)
			}
			switch change {
			case "content":
				p.Route.Content["routes"] = []any{}
			case "metadata":
				p.Route.Metadata["policy_digest"] = "changed"
			case "reference":
				p.Reference.RouteArtifactID = "other-route"
			case "scope":
				p.Route.ScopeKey = "foreign"
			case "status":
				p.Route.Status = "rejected"
			}
			if err = platformconfig.ValidatePreviousTrafficPublication(*p); err == nil {
				t.Fatal("accepted mutated predecessor")
			}
		})
	}
}

var dnsValidationBenchmarkID atomic.Uint64

func BenchmarkDNSValidationReuse(b *testing.B) {
	a := largeDNSValidationArtifact(b)
	raw, _ := json.Marshal(a.Content)
	for _, warm := range []bool{false, true} {
		b.Run(fmt.Sprintf("warm=%t", warm), func(b *testing.B) {
			if err := platformconfig.ValidateDNSCellPlan(a); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// ID is part of the complete envelope key, but does not alter plan replay.
				if !warm {
					a.ID = fmt.Sprintf("uncached-dns-%d", dnsValidationBenchmarkID.Add(1))
				}
				if err := platformconfig.ValidateDNSCellPlan(a); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
