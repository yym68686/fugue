package store

import (
	"errors"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

func TestAcceptTrustedPlatformConsumerHeartbeatInStateIsMonotonicAndServerBound(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	set := trustedHeartbeatExpectedSet(t, now)
	claims := trustedHeartbeatClaims(t, now)
	heartbeat := trustedHeartbeatEnvelope(t, claims, set, now)
	state := &model.State{ExpectedConsumerSets: []model.PlatformExpectedConsumerSet{set}}

	consumer, err := acceptTrustedPlatformConsumerHeartbeatInState(
		state, claims, set.ID, heartbeat, now, platformcontrol.PlatformConsumerHeartbeatValidationPolicy{},
	)
	if err != nil {
		t.Fatalf("accept trusted heartbeat: %v", err)
	}
	if !consumer.IdentityVerified || consumer.CredentialID != claims.CredentialID ||
		consumer.TokenID != claims.TokenID || consumer.ExpectedConsumerSetID != set.ID ||
		consumer.ReleaseSetID != set.ReleaseSetID || len(state.PlatformConsumerInstances) != 1 {
		t.Fatalf("unexpected trusted consumer state: consumer=%+v state=%+v", consumer, state.PlatformConsumerInstances)
	}

	if _, err := acceptTrustedPlatformConsumerHeartbeatInState(
		state, claims, set.ID, heartbeat, now.Add(time.Second), platformcontrol.PlatformConsumerHeartbeatValidationPolicy{},
	); !errors.Is(err, platformcontrol.ErrPlatformConsumerHeartbeatReplay) {
		t.Fatalf("replayed trusted heartbeat must be rejected, got %v", err)
	}
	if len(state.PlatformConsumerInstances) != 1 || state.PlatformConsumerInstances[0].Sequence != heartbeat.Sequence {
		t.Fatalf("replay must not mutate trusted consumer state: %+v", state.PlatformConsumerInstances)
	}
}

func TestLegacyHeartbeatCannotOverwriteVerifiedConsumerState(t *testing.T) {
	t.Parallel()

	s := New(t.TempDir() + "/store.json")
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	now := time.Date(2026, 7, 10, 12, 30, 0, 0, time.UTC)
	set := trustedHeartbeatExpectedSet(t, now)
	claims := trustedHeartbeatClaims(t, now)
	heartbeat := trustedHeartbeatEnvelope(t, claims, set, now)
	if err := s.withLockedState(true, func(state *model.State) error {
		state.ExpectedConsumerSets = append(state.ExpectedConsumerSets, set)
		_, err := acceptTrustedPlatformConsumerHeartbeatInState(
			state, claims, set.ID, heartbeat, now, platformcontrol.PlatformConsumerHeartbeatValidationPolicy{},
		)
		return err
	}); err != nil {
		t.Fatalf("seed verified heartbeat: %v", err)
	}

	_, err := s.UpsertPlatformConsumerHeartbeat(model.PlatformConsumerHeartbeatRequest{
		ConsumerID:   "edge-worker:edge-node-1",
		ArtifactKind: model.PlatformArtifactKindEdgeRankingPolicy,
		ScopeKey:     "global",
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("legacy heartbeat must not downgrade verified state, got %v", err)
	}
	consumers, err := s.ListPlatformConsumers(model.PlatformArtifactKindEdgeRankingPolicy, "global")
	if err != nil {
		t.Fatalf("list consumers: %v", err)
	}
	if len(consumers) != 1 || !consumers[0].IdentityVerified || consumers[0].Sequence != heartbeat.Sequence {
		t.Fatalf("verified consumer was mutated by legacy heartbeat: %+v", consumers)
	}
}

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
