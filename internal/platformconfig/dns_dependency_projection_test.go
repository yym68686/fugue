package platformconfig_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/testfixture/celldns"
)

func trialDNSProjection(t testing.TB, transition bool, padding int) model.PlatformArtifact {
	t.Helper()
	req, _, _ := celldns.SourceAuthorizedRequest(t, transition)
	a := celldns.Compile(t, req).DNSArtifact
	for k := range a.Content {
		switch k {
		case "policy", "cell_route_publications", "cell_dns_source", "previous_traffic_publication", "lineage":
		default:
			delete(a.Content, k)
		}
	}
	a.Content["policy"].(map[string]any)["unknown_padding"] = strings.Repeat("x", padding)
	raw, err := json.Marshal(a.Content)
	if err != nil {
		t.Fatal(err)
	}
	a.Content = nil
	if err := json.Unmarshal(raw, &a.Content); err != nil {
		t.Fatal(err)
	}
	return a
}

func TestProjectedDNSFieldTrial(t *testing.T) {
	for _, transition := range []bool{false, true} {
		changes := []func(map[string]any){
			func(map[string]any) {},
			func(c map[string]any) {
				c["policy"] = map[string]any{"dns_query_policy": map[string]any{"ranking_mode": "active"}}
			},
			func(c map[string]any) { c["policy"], c["cell_dns_source"] = false, false },
			func(c map[string]any) { c["policy"], c["previous_traffic_publication"] = nil, false },
			func(c map[string]any) { c["Policy"], c["policy"] = c["policy"], nil },
			func(c map[string]any) { c["cell_route_publications"] = []any{nil} },
			func(c map[string]any) { c["lineage"] = func() {} },
		}
		for i, change := range changes {
			a := trialDNSProjection(t, transition, 256<<10)
			change(a.Content)
			want, oldErr := platformconfig.DecodeDNSRouteDependencyEvidence(a)
			var cost platformconfig.DNSDependencyDecodeCost
			got, newErr := platformconfig.DecodeProjectedDNSRouteDependencyEvidenceObserved(a, &cost)
			if !reflect.DeepEqual(want, got) || fmt.Sprint(oldErr) != fmt.Sprint(newErr) || cost.Attempts != 1 || (cost.Rejected == 1) != (newErr != nil) {
				t.Fatalf("transition=%t mutation=%d typed views changed (old=%v new=%v)", transition, i, oldErr, newErr)
			}
			if i == 0 && oldErr != nil {
				t.Fatalf("invalid fixture: %v", oldErr)
			}
		}
	}
}

func BenchmarkProjectedDNSFieldTrial(b *testing.B) {
	for _, transition := range []bool{false, true} {
		for _, padding := range []int{0, 256 << 10} {
			a := trialDNSProjection(b, transition, padding)
			for _, v := range []struct {
				name string
				run  func(model.PlatformArtifact) (platformconfig.DNSRouteDependencyEvidence, error)
			}{
				{"baseline", platformconfig.DecodeDNSRouteDependencyEvidence},
				{"per-field", func(a model.PlatformArtifact) (platformconfig.DNSRouteDependencyEvidence, error) {
					return platformconfig.DecodeProjectedDNSRouteDependencyEvidenceObserved(a, nil)
				}},
			} {
				b.Run(fmt.Sprintf("transition-%t/padding-%d/%s", transition, padding, v.name), func(b *testing.B) {
					if _, err := v.run(a); err != nil {
						b.Fatal(err)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if _, err := v.run(a); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}
