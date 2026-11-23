package store

import (
	"testing"
	"time"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
)

func TestHostedAdmissionCapabilityCannotBeBorrowedFromAnotherReleaseOrProcess(t *testing.T) {
	now := time.Now().UTC()
	kind := model.PlatformArtifactKindDNSAnswerBundle
	m := model.PlatformExpectedConsumer{ConsumerID: "dns-server:one", Component: model.PlatformConsumerComponentDNSServer, NodeID: "one"}
	a := model.PlatformConsumerAssignment{ArtifactKind: kind, ScopeKey: "global", ReleaseSetID: "parent", ExpectedConsumerSetID: "members", ExpectedGeneration: "g1", FencingToken: 7}
	f := model.PlatformConsumerInstance{ConsumerID: m.ConsumerID, Component: m.Component, NodeID: m.NodeID, ArtifactKind: kind, ScopeKey: "global", ReleaseSetID: a.ReleaseSetID, ExpectedConsumerSetID: a.ExpectedConsumerSetID, FencingToken: a.FencingToken, GenerationSequence: 1, Sequence: 1, Nonce: "nonce", ProtocolVersion: "v1", SchemaVersion: "v1", CredentialID: "cred", TokenID: "token", EvidenceHash: "evidence", IdentityVerified: true, IssuedAt: &now, LastHeartbeatAt: now, DesiredGeneration: "g1", ActualGeneration: "g1", ApplyStatus: "applied", ProbeStatus: "passed", SupportedKinds: []string{kind}, CompatibilityCapabilities: []string{platformcontrol.TrafficReleaseCapabilityV1, hostedMemberProtocol}}
	if !hostedMemberCapable(m, a, f, now) {
		t.Fatal("fresh serving capability lost")
	}
	for _, alter := range []func(*model.PlatformConsumerInstance){
		func(v *model.PlatformConsumerInstance) { v.CompatibilityCapabilities = v.CompatibilityCapabilities[:1] },
		func(v *model.PlatformConsumerInstance) { v.ConsumerID += "-replacement" },
		func(v *model.PlatformConsumerInstance) { v.ExpectedConsumerSetID += "-other" },
		func(v *model.PlatformConsumerInstance) { v.ReleaseSetID += "-other" },
		func(v *model.PlatformConsumerInstance) { v.FencingToken++ },
		func(v *model.PlatformConsumerInstance) { v.ScopeKey = "cell:foreign" },
		func(v *model.PlatformConsumerInstance) { v.ActualGeneration = "stale" },
		func(v *model.PlatformConsumerInstance) { v.ApplyStatus = "staged" },
		func(v *model.PlatformConsumerInstance) { v.ProbeStatus = "failed" },
		func(v *model.PlatformConsumerInstance) { v.IdentityVerified = false },
		func(v *model.PlatformConsumerInstance) { v.SupportedKinds = nil },
		func(v *model.PlatformConsumerInstance) { v.LastHeartbeatAt = now.Add(-2 * time.Minute) },
	} {
		v := f
		alter(&v)
		if hostedMemberCapable(m, a, v, now) {
			t.Fatal("legacy, foreign, stale or negative fact accepted")
		}
	}
}

func TestHostedHoldsCannotLiftOrInventAVeto(t *testing.T) {
	h := func(id string) pc.HostedAddressHold {
		return pc.HostedAddressHold{EdgeID: id, EdgeGroupID: "group", Address: id + ".example.test"}
	}
	a, b, other := h("a"), h("b"), h("other")
	p := pc.HostedScopeProposal{Physical: []pc.HostedAddressHold{a, b}, Eligible: []pc.HostedAddressHold{a}}
	if !hostedHoldsCurrent(p, []pc.HostedAddressHold{other, b}) {
		t.Fatal("unrelated hold vetoed A")
	}
	for _, v := range [][]pc.HostedAddressHold{nil, {other}, {a}, {a, b}} {
		if hostedHoldsCurrent(p, v) {
			t.Fatal("stale held image accepted")
		}
	}
	p.Physical = append(p.Physical, b)
	if hostedHoldsCurrent(p, []pc.HostedAddressHold{b}) {
		t.Fatal("duplicate physical ingress accepted")
	}
}
