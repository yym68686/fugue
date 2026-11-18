package api

import (
	"encoding/json"
	"net/netip"
	"reflect"
	"testing"
	"time"

	"fugue/internal/dnsserver"
	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/passiveasn"
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

func TestPassiveASNServingRequiresEveryCurrentAuthority(t *testing.T) {
	a := model.PlatformConsumerAssignment{ArtifactID: "dns-1", ContentHash: "digest-1", ReleaseSetID: "release-1", FencingToken: 3}
	release := dnsserver.DNSDecisionPublication{ArtifactID: a.ArtifactID, Digest: a.ContentHash, ReleaseSetID: a.ReleaseSetID, FencingToken: a.FencingToken}
	policies := []platformconfig.DNSClientPolicy{{NodeID: "node-a"}, {NodeID: "node-b"}}
	proof := passiveASNDNSProof{Process: "process", PodUID: "pod", Publication: release, Verified: true}
	proofs := map[string]passiveASNDNSProof{"node-a": proof}
	if passiveASNServingReady(proofs, policies, a, "node-a") {
		t.Fatal("one authority activated ranking")
	}
	proofs["node-b"] = passiveASNDNSProof{Process: "restarted", PodUID: "new-pod", Publication: release}
	if passiveASNServingReady(proofs, policies, a, "node-a") {
		t.Fatal("unverified peer activated ranking")
	}
	proofs["node-b"] = proof
	if !passiveASNServingReady(proofs, policies, a, "node-a") {
		t.Fatal("verified authorities failed admission")
	}
	a.FencingToken++
	if passiveASNServingReady(proofs, policies, a, "node-a") {
		t.Fatal("previous serving release authorized ranking")
	}
}

func TestPassiveASNCohortsSurviveMissingSamplesButNotASNChange(t *testing.T) {
	p := netip.MustParsePrefix("203.0.113.0/24")
	history := []passiveasn.Binding{{Prefix: p, ASN: 64496}}
	if got := retainPassiveASNCohorts(nil, history, func(netip.Prefix) uint32 { return 64496 }); !reflect.DeepEqual(got, history) {
		t.Fatal("missing samples dropped known eligible cohort", got)
	}
	if got := retainPassiveASNCohorts(nil, history, func(netip.Prefix) uint32 { return 64497 }); len(got) != 0 {
		t.Fatal("historical cohort followed an incompatible ASN reassignment", got)
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
	if !changed || !reflect.DeepEqual(got.Comparisons[cmp.Key()], newer) {
		t.Fatal("new valid result did not supersede history", got)
	}
	got, changed, _ = retainPassiveASN(restored, "changed-mapping", nil)
	if changed || len(got.Comparisons) != 0 {
		t.Fatal("reused history with incompatible mapping", got)
	}
}

func TestPassiveASNBulkMappingRefreshCannotReinterpretHistoricalEvidence(t *testing.T) {
	first := netip.MustParsePrefix("198.51.100.0/24")
	second := netip.MustParsePrefix("203.0.113.0/24")
	obs := edgequality.PassiveASNComparison{ASN: 64496, Prefixes: []netip.Prefix{first, second}}
	signedSource := map[netip.Prefix]uint32{first: 64496, second: 64496}
	lookup := func(p netip.Prefix) uint32 { return signedSource[p] }
	if !passiveASNMatchesMapping(obs, lookup) {
		t.Fatal("stable mapping did not retain evidence")
	}
	signedSource[second] = 64497
	if passiveASNMatchesMapping(obs, lookup) || passiveASNMatchesMapping(edgequality.PassiveASNComparison{ASN: 64496}, lookup) {
		t.Fatal("changed or unbound mapping retained old evidence")
	}
}
