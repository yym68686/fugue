package store

import (
	"encoding/json"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

func TestTrustedConsumerAuthorityCursorsCannotOverwriteEachOther(t *testing.T) {
	now := time.Now().UTC()
	set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{ReleaseSetID: "set", ArtifactKind: model.PlatformArtifactKindEdgeRankingPolicy, ScopeKey: "global", Generation: "generation-42", PreparedAt: now, Topology: platformcontrol.ExpectedConsumerTopology{EdgeNodes: []model.EdgeNode{{ID: "edge-node-1", EdgeGroupID: "edge-group-old"}, {ID: "edge-node-1", EdgeGroupID: "cell-a"}, {ID: "edge-node-1", EdgeGroupID: "cell-b"}}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := normalizePlatformExpectedConsumerSetForStore(set); err != nil {
		t.Fatal(err)
	}
	state := &model.State{ExpectedConsumerSets: []model.PlatformExpectedConsumerSet{set}}
	for _, authority := range []string{"", "cell-a", "cell-b"} {
		claims := trustedHeartbeatClaims(t, now)
		claims.AuthorityID = authority
		h := trustedHeartbeatEnvelope(t, claims, set, now)
		if _, err := acceptTrustedPlatformConsumerHeartbeatInState(state, claims, set.ID, h, now, platformcontrol.PlatformConsumerHeartbeatValidationPolicy{}); err != nil {
			t.Fatal(authority, err)
		}
		before, _ := json.Marshal(state)
		for _, other := range []string{"edge-worker:edge-node-1", "edge-worker:cell-a:edge-node-1", "edge-worker:cell-b:edge-node-1"} {
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
