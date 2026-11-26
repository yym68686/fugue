package api

import (
	"slices"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
)

func TestOfflineContractionDoesNotChangeUnrelatedConsumers(t *testing.T) {
	offline := map[string]string{"offline": "edge-group-fixture"}
	assessment := model.PlatformConsumerEvidenceAssessment{Required: true, State: model.InvariantEvidenceStateUnknown,
		Expected: model.PlatformExpectedConsumer{NodeID: "offline", ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle, Component: model.PlatformConsumerComponentEdgeWorker, AuthorityID: platformcontrol.ConsumerAuthorityID("edge-group-fixture")}}
	survivor := model.PlatformConsumerEvidenceAssessment{Required: true, State: model.InvariantEvidenceStatePass,
		Expected: model.PlatformExpectedConsumer{NodeID: "healthy", Component: model.PlatformConsumerComponentEdgeWorker}}
	st := model.PlatformConsumerConvergenceStatus{RequiredExpected: 2, Assessments: []model.PlatformConsumerEvidenceAssessment{assessment, survivor}}
	if !offlineContractionSurvivorsPass(st, offline) {
		t.Fatal("stale receipts from the withdrawn Worker blocked its own withdrawal")
	}
	st.Assessments[0].Expected.Component = model.PlatformConsumerComponentDNSServer
	if offlineContractionSurvivorsPass(st, offline) {
		t.Fatal("removed a DNS dependency at the same node")
	}
	st.Assessments[0] = assessment
	st.Assessments[1].State = model.InvariantEvidenceStateUnknown
	if offlineContractionSurvivorsPass(st, offline) {
		t.Fatal("admitted a stale surviving Worker")
	}
	st.Assessments = st.Assessments[:1]
	st.RequiredExpected = 1
	if offlineContractionSurvivorsPass(st, offline) {
		t.Fatal("removed the last required member")
	}
}

func TestOfflineContractionCanOnlyAddEffectiveHolds(t *testing.T) {
	a := platformconfig.HostedAddressHold{EdgeID: "a", EdgeGroupID: "pool", Address: "8.8.8.8"}
	b := platformconfig.HostedAddressHold{EdgeID: "b", EdgeGroupID: "pool", Address: "9.9.9.9"}
	changed := a
	changed.Address = "1.1.1.1"
	prior := []platformconfig.HostedAddressHold{a}
	if got := newlyHeldEdges(prior, []platformconfig.HostedAddressHold{b, a}); !slices.Equal(got, []platformconfig.HostedAddressHold{b}) {
		t.Fatal("did not select the one newly held endpoint", got)
	}
	for _, next := range [][]platformconfig.HostedAddressHold{nil, {a}, {b}, {changed, b}, {a, b, b}} {
		if got := newlyHeldEdges(prior, next); len(got) != 0 {
			t.Fatalf("non-contraction admitted: %+v", next)
		}
	}
}

func TestOfflineContractionPreservesDNSDeclarationsExceptProvenAddressWithdrawal(t *testing.T) {
	rr := platformconfig.DNSIntent{Hostname: "app.example.test", Type: "A", TTL: 60, Values: []string{"8.8.8.8", "9.9.9.9"}}
	prior := platformconfig.DNSArtifactPayload{
		Records: []platformconfig.DNSIntent{rr},
		ReadinessPlan: &platformconfig.DNSReadinessPlan{Records: []platformconfig.DNSReadinessRecord{{Hostname: rr.Hostname,
			Targets: []platformconfig.DNSReadinessTarget{{EdgeID: "offline", Family: "A", Address: "8.8.8.8"}, {EdgeID: "healthy", Family: "A", Address: "9.9.9.9"}}}}},
	}
	for _, tc := range []struct {
		name string
		edit func(*platformconfig.DNSIntent)
		pass bool
	}{
		{"retained reservation", func(*platformconfig.DNSIntent) {}, true},
		{"offline address removed", func(r *platformconfig.DNSIntent) { r.Values = []string{"9.9.9.9"} }, true},
		{"healthy address removed", func(r *platformconfig.DNSIntent) { r.Values = []string{"8.8.8.8"} }, false},
		{"address substituted", func(r *platformconfig.DNSIntent) { r.Values = []string{"9.9.9.9", "1.1.1.1"} }, false},
		{"TTL changed", func(r *platformconfig.DNSIntent) { r.TTL = 30 }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			next := prior
			next.Records = []platformconfig.DNSIntent{rr}
			tc.edit(&next.Records[0])
			if got := onlyOfflineDNSRecordsChanged(prior, next, map[string]bool{"offline": true}); got != tc.pass {
				t.Fatalf("want %t got %t", tc.pass, got)
			}
		})
	}
}

func TestOfflineContractionReadsSameProgramAcrossCompilerGenerations(t *testing.T) {
	art := func(generation, target string) model.PlatformArtifact {
		return model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle,
			Content: map[string]any{"generation": generation, "lineage": generation, "routes": target,
				"policy": map[string]any{"generation": generation, "scope": "global"}}}
	}
	prior := art("old", "unchanged")
	if !sameTrafficProgram(&prior, art("new", "unchanged")) {
		t.Fatal("compiler lineage must not change the comparison of serving programs")
	}
	if sameTrafficProgram(&prior, art("new", "different")) {
		t.Fatal("membership-only publication changed the route")
	}
}

func TestOfflineContractionPreservesDNSReadinessAndQuorum(t *testing.T) {
	target := func(name, addr string) platformconfig.DNSReadinessTarget {
		return platformconfig.DNSReadinessTarget{EdgeID: name, EdgeGroupID: "edge-group-fixture", Address: addr, Family: "A", ProbeIDs: []string{name}}
	}
	probe := func(t platformconfig.DNSReadinessTarget) platformconfig.DNSReadinessProbe {
		return platformconfig.DNSReadinessProbe{ID: t.EdgeID, EdgeID: t.EdgeID, EdgeGroupID: t.EdgeGroupID, Address: t.Address}
	}
	a, b := target("active", "8.8.8.8"), target("offline", "9.9.9.9")
	held := map[string]platformconfig.HostedAddressHold{b.EdgeID: {EdgeID: b.EdgeID, EdgeGroupID: b.EdgeGroupID, Address: b.Address}}
	record := platformconfig.DNSReadinessRecord{Hostname: "app.example.test", MinimumHealthyEdges: 1, Targets: []platformconfig.DNSReadinessTarget{a, b}}
	before := &platformconfig.DNSReadinessPlan{Probes: []platformconfig.DNSReadinessProbe{probe(a), probe(b)}, Records: []platformconfig.DNSReadinessRecord{record}}
	record.Targets = []platformconfig.DNSReadinessTarget{a}
	after := &platformconfig.DNSReadinessPlan{Probes: []platformconfig.DNSReadinessProbe{probe(a)}, Records: []platformconfig.DNSReadinessRecord{record}}
	if !offlineDNSPlanContracts(before, after, held) {
		t.Fatal("verified withdrawal with sufficient survivors blocked")
	}
	before.Records[0].MinimumHealthyEdges = 2
	if offlineDNSPlanContracts(before, after, held) {
		t.Fatal("withdrawal silently lowered quorum")
	}
	before.Records[0].MinimumHealthyEdges = 1
	after.Probes[0].Path = "/other"
	if offlineDNSPlanContracts(before, after, held) {
		t.Fatal("withdrawal changed unrelated route proof")
	}
}
