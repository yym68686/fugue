package platformconfig_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"fugue/internal/platformconfig"
	"fugue/internal/testfixture/celldns"
)

// Exercise full signed publication composition and typed source validation,
// including unknown policy data that remains part of the immutable envelope.
func BenchmarkDNSRouteDependencyTypedDecode(b *testing.B) {
	for _, transition := range []bool{false, true} {
		for _, padding := range []int{0, 256 << 10} {
			b.Run(fmt.Sprintf("transition-%t/padding-%d", transition, padding), func(b *testing.B) {
				req, _, _ := celldns.SourceAuthorizedRequest(b, transition)
				a := celldns.Compile(b, req).DNSArtifact
				a.Content["policy"].(map[string]any)["unknown_padding"] = strings.Repeat("x", padding)
				raw, err := json.Marshal(a.Content)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := platformconfig.DecodeDNSRouteDependencyEvidence(a); err != nil {
					b.Fatal(err)
				}
				b.SetBytes(int64(len(raw)))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := platformconfig.DecodeDNSRouteDependencyEvidence(a); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkDNSDependencyStageObservation(b *testing.B) {
	req, _, _ := celldns.SourceAuthorizedRequest(b, true)
	a := celldns.Compile(b, req).DNSArtifact
	a.Content["policy"].(map[string]any)["unknown_padding"] = strings.Repeat("x", 256<<10)
	for _, observed := range []bool{false, true} {
		b.Run(fmt.Sprintf("observed-%t", observed), func(b *testing.B) {
			var cost *platformconfig.DNSDependencyDecodeCost
			if observed {
				cost = &platformconfig.DNSDependencyDecodeCost{}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := platformconfig.DecodeDNSRouteDependencyEvidenceObserved(a, cost); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Retained DNS content belongs to the exact previous publication and must
// still be fully decoded and hashed. Measure the temporary envelope copying
// separately from a large unknown field in the current policy.
func BenchmarkDNSDependencyRetainedContent(b *testing.B) {
	for _, padding := range []int{0, 256 << 10} {
		b.Run(fmt.Sprintf("previous-DNS-padding-%d", padding), func(b *testing.B) {
			req, _, _ := celldns.SourceAuthorizedRequest(b, true)
			previous := req.PreviousTrafficPublication
			previous.DNS.Content["unknown_padding"] = strings.Repeat("x", padding)
			previous.DNS = celldns.Sign(b, previous.DNS, previous.DNS.ID, previous.DNS.GenerationSequence)
			previous.Reference.DNSArtifactDigest = previous.DNS.ContentHash
			req.Intent.RouteAuthorityTransition.PreviousPublication = previous.Reference
			a := celldns.Compile(b, req).DNSArtifact
			raw, err := json.Marshal(a.Content)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := platformconfig.DecodeDNSRouteDependencyEvidence(a); err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := platformconfig.DecodeDNSRouteDependencyEvidence(a); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
