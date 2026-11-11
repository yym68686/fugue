package platformconfig

import (
	"encoding/json"
	"fmt"
	"testing"
)

// A retained inventory sees many independent publication envelopes. Compare
// the current fixed-size cache with an equally bounded trial working set;
// neither path trusts claimed hashes or skips actual-content fingerprinting.
func BenchmarkCellPublicationWorkingSet(b *testing.B) {
	for _, count := range []int{32, 96, 384} {
		p := cachedCellPublicationFixture(b, 32<<10)
		raw, _ := json.Marshal(p)
		for _, capacity := range []int{64, dnsCellPublicationValidationLimit} {
			b.Run(fmt.Sprintf("publications-%d/capacity-%d", count, capacity), func(b *testing.B) {
				cache := dnsValidationCache{capacity: capacity}
				validate := func(i int) error {
					p.Reference.ReleaseID = fmt.Sprintf("retained-release-%d", i)
					return validateDNSCellRoutePublicationCached(p, len(raw), nil, &cache)
				}
				for i := 0; i < count; i++ {
					if err := validate(i); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := validate(i % count); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func TestCellPublicationWorkingSetKeepsExactValidation(t *testing.T) {
	p := cachedCellPublicationFixture(t, 32<<10)
	raw, _ := json.Marshal(p)
	cache := dnsValidationCache{capacity: dnsCellPublicationValidationLimit}
	var cost DNSDependencyDecodeCost
	for pass := 0; pass < 2; pass++ {
		for i := 0; i < 384; i++ {
			p.Reference.ReleaseID = fmt.Sprintf("retained-release-%d", i)
			if err := validateDNSCellRoutePublicationCached(p, len(raw), &cost, &cache); err != nil {
				t.Fatal(err)
			}
		}
	}
	if cost.CellValidationChecks != 768 || cost.CellValidationMisses != 384 || cost.CellValidationHits != 384 || cost.CellValidationUncached != 0 {
		t.Fatal("working set was not reused with bounded fingerprints", cost)
	}
	// Preserve a claimed digest while changing actual content: a warm entry
	// must not hide rejection. Failed checks must not become cache entries.
	p.Route.Content["padding"] = "changed"
	for i := 0; i < 2; i++ {
		got := validateDNSCellRoutePublicationCached(p, len(raw), &cost, &cache)
		want := ValidateCellRoutePublication(p)
		if want == nil || fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("warm mutation changed rejection: got=%v want=%v", got, want)
		}
	}
	if cost.CellValidationHits != 384 || cost.CellValidationMisses != 386 {
		t.Fatal("invalid mutation was remembered", cost)
	}
}
