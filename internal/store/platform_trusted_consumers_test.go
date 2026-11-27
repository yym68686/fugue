package store

import (
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

func trustedHeartbeatExpectedSet(t *testing.T, now time.Time) model.PlatformExpectedConsumerSet {
	t.Helper()
	set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{
		ReleaseSetID: "release-set-1",
		ArtifactKind: model.PlatformArtifactKindEdgeRankingPolicy,
		ScopeKey:     "global",
		Generation:   "generation-42",
		PreparedAt:   now,
		Topology: platformcontrol.ExpectedConsumerTopology{EdgeNodes: []model.EdgeNode{{
			ID: "edge-node-1", EdgeGroupID: "edge-group-1", Country: "US",
		}}},
	})
	if err != nil {
		t.Fatalf("build expected consumer set: %v", err)
	}
	return set
}

func trustedHeartbeatClaims(t *testing.T, now time.Time) platformcontrol.PlatformComponentIdentityClaims {
	t.Helper()
	keyring := platformcontrol.PlatformComponentIdentityKeyring{
		ActiveKeyID: "component-key-1",
		Keys: map[string]string{
			"component-key-1": "component-signing-secret",
		},
	}
	token, err := platformcontrol.IssuePlatformComponentIdentity(keyring, platformcontrol.PlatformComponentIdentityClaims{
		CredentialID:  "credential-1",
		Component:     model.PlatformConsumerComponentEdgeWorker,
		NodeID:        "edge-node-1",
		ScopeKey:      "global",
		ArtifactKinds: []string{model.PlatformArtifactKindEdgeRankingPolicy},
	}, now.Add(-time.Minute), 5*time.Minute)
	if err != nil {
		t.Fatalf("issue component identity: %v", err)
	}
	claims, err := platformcontrol.ParsePlatformComponentIdentity(keyring, token, now)
	if err != nil {
		t.Fatalf("parse component identity: %v", err)
	}
	return claims
}

func trustedHeartbeatEnvelope(
	t *testing.T,
	claims platformcontrol.PlatformComponentIdentityClaims,
	set model.PlatformExpectedConsumerSet,
	now time.Time,
) platformcontrol.PlatformConsumerHeartbeatEnvelope {
	t.Helper()
	heartbeat, err := platformcontrol.BindPlatformConsumerHeartbeatToExpectedSet(claims, set, platformcontrol.PlatformConsumerHeartbeatEnvelope{
		ArtifactKind:       set.ArtifactKind,
		FencingToken:       8,
		ProtocolVersion:    model.PlatformConsumerProtocolVersionV1,
		SchemaVersion:      model.PlatformConsumerSchemaVersionV1,
		Sequence:           12,
		IssuedAt:           now.Add(-time.Second),
		Nonce:              "nonce-value-0001",
		GenerationSequence: 42,
		ActualGeneration:   set.ExpectedGeneration,
		LKGGeneration:      "generation-41",
		ApplyStatus:        model.PlatformConsumerApplyStatusApplied,
		ProbeStatus:        model.PlatformConsumerProbeStatusPassed,
	})
	if err != nil {
		t.Fatalf("bind trusted heartbeat: %v", err)
	}
	heartbeat.EvidenceHash, err = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(heartbeat)
	if err != nil {
		t.Fatalf("hash trusted heartbeat: %v", err)
	}
	return heartbeat
}
