package api

import (
	"encoding/json"
	"net/netip"
	"testing"
	"time"

	"fugue/internal/edgequality"
	"fugue/internal/platformconfig"
)

func TestPassiveASNMappingNeedsAgreementAcrossDNSAuthorities(t *testing.T) {
	policies := []platformconfig.DNSClientPolicy{
		{NodeID: "dns-a", Rules: []platformconfig.DNSClientRule{{CIDR: "198.51.100.0/24", ASN: "AS64496"}}},
		{NodeID: "dns-b", Rules: []platformconfig.DNSClientRule{{CIDR: "198.51.100.0/24", ASN: "64496"}}},
	}
	prefix := netip.MustParsePrefix("198.51.100.0/24")
	resolve, rules, err := buildPassiveASNResolver(policies)
	if err != nil || rules != 2 || resolve(prefix) != 64496 {
		t.Fatal("shared signed mapping was not used", err)
	}
	for _, asn := range []string{"64497", ""} {
		policies[1].Rules[0].ASN = asn
		resolve, _, err := buildPassiveASNResolver(policies)
		if err != nil || resolve(prefix) != 0 {
			t.Fatal("conflicting or missing ASN was used", err)
		}
	}
}

func TestPassiveASNRetainsLatestOriginalComparisonThroughRestart(t *testing.T) {
	ts := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	cmp := edgequality.PassiveASNComparison{
		ASN: 64496, Family: 4, Left: edgequality.PassiveEndpoint{EdgeID: "edge-a"},
		Right: edgequality.PassiveEndpoint{EdgeID: "edge-b"}, ObservedAt: ts, Preferred: "edge-a",
	}
	saved, changed, _ := retainPassiveASN(passiveASNCheckpoint{}, "mapping", []edgequality.PassiveASNComparison{cmp})
	if !changed || len(saved.Comparisons) != 1 {
		t.Fatal(saved)
	}
	raw, _ := json.Marshal(saved)
	var restored passiveASNCheckpoint
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	for _, next := range [][]edgequality.PassiveASNComparison{nil, {cmp}} {
		got, changed, _ := retainPassiveASN(restored, "mapping", next)
		if changed || got.Comparisons[cmp.Key()].Preferred != "edge-a" {
			t.Fatal("absent or identical samples displaced history", got)
		}
	}
	older := cmp
	older.ObservedAt, older.Preferred = ts.Add(-time.Minute), "edge-b"
	got, changed, _ := retainPassiveASN(restored, "mapping", []edgequality.PassiveASNComparison{older})
	if changed || got.Comparisons[cmp.Key()].Preferred != "edge-a" {
		t.Fatal("older observation replaced latest result", got)
	}
	newer := older
	newer.ObservedAt = ts.Add(240 * time.Hour)
	got, changed, _ = retainPassiveASN(restored, "mapping", []edgequality.PassiveASNComparison{newer})
	if !changed || got.Comparisons[cmp.Key()] != newer {
		t.Fatal("new valid result did not supersede history", got)
	}
	got, changed, _ = retainPassiveASN(restored, "changed-mapping", nil)
	if changed || len(got.Comparisons) != 0 {
		t.Fatal("reused history with incompatible mapping", got)
	}
}
