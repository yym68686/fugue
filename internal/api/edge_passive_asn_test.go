package api

import (
	"encoding/json"
	"net/netip"
	"reflect"
	"testing"
	"time"

	"fugue/internal/dnsserver"
	"fugue/internal/edgequality"
	"fugue/internal/frontnetwork"
	"fugue/internal/model"
	"fugue/internal/passiveasn"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
)

func TestPassiveFrontExplorationHold(t *testing.T) {
	for _, test := range []struct {
		stats *frontnetwork.PassiveStats
		want  bool
	}{
		{nil, true},
		{&frontnetwork.PassiveStats{RecentForwardOK: 30, RecentForwardError: 3}, false},
		{&frontnetwork.PassiveStats{RecentForwardOK: 6, RecentForwardError: 3}, false},
		{&frontnetwork.PassiveStats{RecentForwardOK: 7, RecentForwardError: 3}, true},
		{&frontnetwork.PassiveStats{RecentForwardOK: 30, RecentForwardError: 9}, true},
	} {
		if got := passiveFrontExplorationHold(test.stats); got != test.want {
			t.Fatalf("incorrect exploration brake for %+v: %v", test.stats, got)
		}
	}
}

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
	release := dnsserver.DNSDecisionPublication{ArtifactID: a.ArtifactID, Digest: a.ContentHash, ReleaseSetID: a.ReleaseSetID,
		FencingToken: a.FencingToken, ReleaseChannel: model.PlatformArtifactReleaseChannelFull}
	policies := []platformconfig.DNSClientPolicy{{NodeID: "node-a"}, {NodeID: "node-b"}}
	claims := platformcontrol.PlatformComponentIdentityClaims{NodeID: "node-a", Component: model.PlatformConsumerComponentDNSServer, ScopeKey: "global",
		CredentialID: "kubernetes:system:dns-a:pod", ArtifactKinds: []string{model.PlatformArtifactKindDNSAnswerBundle}}
	proof := passiveASNDNSProof{Process: "process", PodUID: "pod", ScopeKey: claims.ScopeKey, Publication: release, WireVerified: true}
	proofs := map[string]passiveASNDNSProof{"node-a": proof}
	if passiveASNServingReady(proofs, policies, a, claims) {
		t.Fatal("one authority activated ranking")
	}
	proofs["node-b"] = passiveASNDNSProof{Process: "restarted", PodUID: "new-pod", ScopeKey: claims.ScopeKey, Publication: release}
	if passiveASNServingReady(proofs, policies, a, claims) {
		t.Fatal("unverified peer activated ranking")
	}
	foreign := proof
	foreign.ScopeKey = "authority-cell:cell-b"
	proofs["node-b"] = foreign
	if passiveASNServingReady(proofs, policies, a, claims) {
		t.Fatal("mixed global and cell backends activated ranking")
	}
	proofs["node-b"] = proof
	if !passiveASNServingReady(proofs, policies, a, claims) {
		t.Fatal("replayed ordinary answers from both sites failed admission before the first classified query")
	}
	incomplete := proof
	incomplete.Publication.ReleaseChannel = ""
	proofs["node-b"] = incomplete
	if passiveASNServingReady(proofs, policies, a, claims) {
		t.Fatal("peer without a full release activated ranking")
	}
	proofs["node-b"] = proof
	replace := claims
	replace.CredentialID = "kubernetes:system:dns-a:new-pod"
	if passiveASNServingReady(proofs, policies, a, replace) {
		t.Fatal("standby inherited positive evidence from the previous public process")
	}
	a.FencingToken++
	if passiveASNServingReady(proofs, policies, a, claims) {
		t.Fatal("previous serving release authorized ranking")
	}
}

func TestPassiveASNDNSProofOnlyPersistsForExactServingIdentity(t *testing.T) {
	current := passiveASNDNSProof{Process: "p", PodUID: "pod", ScopeKey: "global",
		Publication: dnsserver.DNSDecisionPublication{ArtifactID: "a", ReleaseSetID: "r", FencingToken: 1}}
	prior := current
	prior.WireVerified = true
	if got := current.inherit(prior); !got.WireVerified || got.Verified {
		t.Fatal("lost wire proof after journal turnover or counted it as ASN evidence", got)
	}
	legacy := current
	legacy.Verified = true
	if got := current.inherit(legacy); !got.WireVerified || !got.Verified {
		t.Fatal("legacy classified proof did not carry its implied wire verification", got)
	}
	for _, mutate := range []func(*passiveASNDNSProof){
		func(p *passiveASNDNSProof) { p.Process = "new-process" },
		func(p *passiveASNDNSProof) { p.PodUID = "new-pod" },
		func(p *passiveASNDNSProof) { p.ScopeKey = "authority-cell:cell-new" },
		func(p *passiveASNDNSProof) { p.Publication.FencingToken++ },
	} {
		replacement := current
		mutate(&replacement)
		if got := replacement.inherit(legacy); got.WireVerified || got.Verified {
			t.Fatal("changed backend inherited prior serving evidence", got)
		}
	}
}

func TestPassiveASNRequiresExactKubernetesDNSMemberAndUsesItsCohort(t *testing.T) {
	for _, authority := range []string{"", "cell-a"} {
		scope, cohort := "global", "region-a"
		if authority != "" {
			scope, cohort = platformconfig.AuthorityCellScope(authority), authority
		}
		const kind = model.PlatformArtifactKindDNSAnswerBundle
		c := platformcontrol.PlatformComponentIdentityClaims{Component: model.PlatformConsumerComponentDNSServer,
			NodeID: "dns-a", ScopeKey: scope, AuthorityID: authority, CredentialID: "kubernetes:system:dns:pod", ArtifactKinds: []string{kind}}
		member := model.PlatformExpectedConsumer{ConsumerID: c.ConsumerID(), Component: c.Component, NodeID: c.NodeID,
			ScopeKey: scope, AuthorityID: authority, ArtifactKind: kind, Cohort: cohort}
		set := model.PlatformExpectedConsumerSet{ArtifactKind: kind, ScopeKey: scope, Consumers: []model.PlatformExpectedConsumer{member}}
		if got, ok := passiveASNExpectedCohort(set, c); !ok || got != cohort || passiveASNDNSPodUID(c) != "pod" {
			t.Fatalf("exact assigned DNS identity rejected: %s, %v", got, ok)
		}
		for _, mutate := range []func(*platformcontrol.PlatformComponentIdentityClaims){
			func(x *platformcontrol.PlatformComponentIdentityClaims) {
				x.Component = model.PlatformConsumerComponentEdgeWorker
			},
			func(x *platformcontrol.PlatformComponentIdentityClaims) { x.CredentialID = "static:dns" },
			func(x *platformcontrol.PlatformComponentIdentityClaims) { x.ScopeKey = "authority-cell:cell-b" },
			func(x *platformcontrol.PlatformComponentIdentityClaims) { x.ArtifactKinds = nil },
		} {
			foreign := c
			mutate(&foreign)
			if passiveASNDNSPodUID(foreign) != "" {
				t.Fatal("unrelated credential admitted")
			}
		}
		c.NodeID = "dns-foreign"
		if _, ok := passiveASNExpectedCohort(set, c); ok {
			t.Fatal("a different node inherited the DNS assignment")
		}
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
