package store

import (
	"encoding/json"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

func TestTrustedConsumerAuthorityCursorsCannotOverwriteEachOther(t *testing.T) {
	for _, component := range []string{model.PlatformConsumerComponentEdgeWorker, model.PlatformConsumerComponentDNSServer} {
		t.Run(component, func(t *testing.T) { testTrustedAuthorityCursors(t, component) })
	}
}

func testTrustedAuthorityCursors(t *testing.T, component string) {
	t.Helper()
	now := time.Now().UTC()
	req := platformcontrol.ExpectedConsumerSetBuildRequest{ReleaseSetID: "set", ArtifactKind: model.PlatformArtifactKindEdgeRankingPolicy, ScopeKey: "global", Generation: "generation-42", PreparedAt: now, Topology: platformcontrol.ExpectedConsumerTopology{EdgeNodes: []model.EdgeNode{{ID: "edge-node-1", EdgeGroupID: "edge-group-old"}, {ID: "edge-node-1", EdgeGroupID: "cell-a"}, {ID: "edge-node-1", EdgeGroupID: "cell-b"}}}}
	if component == model.PlatformConsumerComponentDNSServer {
		req.ArtifactKind = model.PlatformArtifactKindDNSAnswerBundle
		for _, edge := range req.Topology.EdgeNodes {
			req.Topology.DNSNodes = append(req.Topology.DNSNodes, model.DNSNode{ID: edge.ID, EdgeGroupID: edge.EdgeGroupID})
		}
		req.Topology.EdgeNodes = nil
	}
	set, err := platformcontrol.BuildExpectedConsumerSet(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := normalizePlatformExpectedConsumerSetForStore(set); err != nil {
		t.Fatal(err)
	}
	state := &model.State{ExpectedConsumerSets: []model.PlatformExpectedConsumerSet{set}}
	for _, authority := range []string{"", "cell-a", "cell-b"} {
		claims := trustedHeartbeatClaims(t, now)
		claims.AuthorityID, claims.Component = authority, component
		claims.ArtifactKinds = []string{req.ArtifactKind}
		if component == model.PlatformConsumerComponentDNSServer {
			claims.CredentialID = "kubernetes:test-system:dns-account:pod-a"
		}
		h := trustedHeartbeatEnvelope(t, claims, set, now)
		if _, err := acceptTrustedPlatformConsumerHeartbeatInState(state, claims, set.ID, h, now, platformcontrol.PlatformConsumerHeartbeatValidationPolicy{}); err != nil {
			t.Fatal(authority, err)
		}
		before, _ := json.Marshal(state)
		for _, member := range set.Consumers {
			other := member.ConsumerID
			if other == h.ConsumerID {
				continue
			}
			forged := h
			forged.ConsumerID, forged.Sequence = other, h.Sequence+100
			forged.EvidenceHash, err = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(forged)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := acceptTrustedPlatformConsumerHeartbeatInState(state, claims, set.ID, forged, now, platformcontrol.PlatformConsumerHeartbeatValidationPolicy{}); err == nil {
				t.Fatal("cross-authority heartbeat accepted")
			}
		}
		after, _ := json.Marshal(state)
		if string(before) != string(after) {
			t.Fatal("rejected scope changed durable state")
		}
	}
	if len(state.PlatformConsumerInstances) != 3 {
		t.Fatal("consumer cursor collision", state.PlatformConsumerInstances)
	}
	bad := set
	bad.Consumers = append([]model.PlatformExpectedConsumer(nil), set.Consumers...)
	for i := range bad.Consumers {
		if bad.Consumers[i].AuthorityID != "" {
			bad.Consumers[i].Cohort = "cell-foreign"
			break
		}
	}
	if _, err := normalizePlatformExpectedConsumerSetForStore(bad); err == nil {
		t.Fatal("unbound authority persisted")
	}
}
