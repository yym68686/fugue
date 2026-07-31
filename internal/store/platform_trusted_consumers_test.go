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

func TestImageCacheTrustedHeartbeatIsActiveReleaseBoundAndRollbackSafe(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 30, 8, 0, 0, 0, time.UTC)
	claims := imageCacheTrustedHeartbeatClaims(t, now, "worker-a")
	forwardSet := imageCacheTrustedHeartbeatExpectedSet(t, now, "worker-a", "image-plan-2", "release-image-plan-2")
	forwardRelease := imageCacheTrustedHeartbeatRelease(forwardSet, "artifact-image-plan-2", 2)
	forwardHeartbeat := imageCacheTrustedHeartbeatEnvelope(t, claims, forwardSet, now, 12, 2, forwardRelease.FencingToken, "nonce-image-cache-forward-0001")
	state := &model.State{
		ExpectedConsumerSets:     []model.PlatformExpectedConsumerSet{forwardSet},
		PlatformArtifactReleases: []model.PlatformArtifactRelease{forwardRelease},
	}
	consumer, err := acceptTrustedPlatformConsumerHeartbeatInState(
		state,
		claims,
		forwardSet.ID,
		forwardHeartbeat,
		now,
		platformcontrol.PlatformConsumerHeartbeatValidationPolicy{},
	)
	if err != nil {
		t.Fatalf("accept active release-bound image-cache heartbeat: %v", err)
	}
	if consumer.FencingToken != forwardRelease.FencingToken || consumer.GenerationSequence != 2 {
		t.Fatalf("unexpected forward image-cache consumer: %+v", consumer)
	}

	state.PlatformArtifactReleases[0].Status = model.PlatformArtifactReleaseStatusSuperseded
	rollbackSet := imageCacheTrustedHeartbeatExpectedSet(t, now.Add(time.Second), "worker-a", "image-plan-1", "release-image-plan-rollback")
	rollbackRelease := imageCacheTrustedHeartbeatRelease(rollbackSet, "artifact-image-plan-1", 3)
	state.ExpectedConsumerSets = append(state.ExpectedConsumerSets, rollbackSet)
	state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, rollbackRelease)
	rollbackHeartbeat := imageCacheTrustedHeartbeatEnvelope(t, claims, rollbackSet, now.Add(time.Second), 13, 1, rollbackRelease.FencingToken, "nonce-image-cache-rollback-0002")
	rolledBack, err := acceptTrustedPlatformConsumerHeartbeatInState(
		state,
		claims,
		rollbackSet.ID,
		rollbackHeartbeat,
		now.Add(time.Second),
		platformcontrol.PlatformConsumerHeartbeatValidationPolicy{},
	)
	if err != nil {
		t.Fatalf("accept fenced image-cache generation rollback: %v", err)
	}
	if rolledBack.GenerationSequence != 1 || rolledBack.DesiredGeneration != rollbackSet.ExpectedGeneration ||
		rolledBack.FencingToken != rollbackRelease.FencingToken || len(state.PlatformConsumerInstances) != 1 {
		t.Fatalf("fenced rollback did not atomically replace consumer status: %+v", rolledBack)
	}

	stale := forwardHeartbeat
	stale.Sequence = 14
	stale.IssuedAt = now.Add(2 * time.Second)
	stale.Nonce = "nonce-image-cache-stale-0003"
	stale.EvidenceHash, err = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(stale)
	if err != nil {
		t.Fatalf("hash stale heartbeat: %v", err)
	}
	if _, err := acceptTrustedPlatformConsumerHeartbeatInState(
		state, claims, forwardSet.ID, stale, now.Add(2*time.Second), platformcontrol.PlatformConsumerHeartbeatValidationPolicy{},
	); !errors.Is(err, platformcontrol.ErrPlatformConsumerHeartbeatRelease) {
		t.Fatalf("superseded expected set heartbeat must be rejected, got %v", err)
	}

	wrongFence := rollbackHeartbeat
	wrongFence.Sequence = 14
	wrongFence.IssuedAt = now.Add(2 * time.Second)
	wrongFence.Nonce = "nonce-image-cache-fence-0004"
	wrongFence.FencingToken++
	wrongFence.EvidenceHash, err = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(wrongFence)
	if err != nil {
		t.Fatalf("hash wrong-fence heartbeat: %v", err)
	}
	if _, err := acceptTrustedPlatformConsumerHeartbeatInState(
		state, claims, rollbackSet.ID, wrongFence, now.Add(2*time.Second), platformcontrol.PlatformConsumerHeartbeatValidationPolicy{},
	); !errors.Is(err, platformcontrol.ErrPlatformConsumerHeartbeatRelease) {
		t.Fatalf("client-selected image-cache fence must be rejected, got %v", err)
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

func imageCacheTrustedHeartbeatClaims(t *testing.T, now time.Time, nodeID string) platformcontrol.PlatformComponentIdentityClaims {
	t.Helper()
	keyring := platformcontrol.DerivePlatformComponentIdentityKeyring("image-cache-heartbeat-key", "image-cache-heartbeat-v1", "", "", nil)
	token, err := platformcontrol.IssuePlatformComponentIdentity(keyring, platformcontrol.PlatformComponentIdentityClaims{
		CredentialID:  model.PlatformConsumerComponentImageCache + ":" + nodeID,
		Component:     model.PlatformConsumerComponentImageCache,
		NodeID:        nodeID,
		ScopeKey:      "node:" + nodeID,
		ArtifactKinds: []string{model.PlatformArtifactKindImageReplicationPlan},
	}, now.Add(-time.Minute), 5*time.Minute)
	if err != nil {
		t.Fatalf("issue image-cache heartbeat identity: %v", err)
	}
	claims, err := platformcontrol.ParsePlatformComponentIdentity(keyring, token, now)
	if err != nil {
		t.Fatalf("parse image-cache heartbeat identity: %v", err)
	}
	return claims
}

func imageCacheTrustedHeartbeatExpectedSet(
	t *testing.T,
	now time.Time,
	nodeID string,
	generation string,
	releaseID string,
) model.PlatformExpectedConsumerSet {
	t.Helper()
	set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{
		ReleaseSetID:      "release-set-" + releaseID,
		ArtifactReleaseID: releaseID,
		ArtifactKind:      model.PlatformArtifactKindImageReplicationPlan,
		Scope:             model.PlatformArtifactScope{ScopeType: "node", Key: "node:" + nodeID, NodeID: nodeID},
		ScopeKey:          "node:" + nodeID,
		Generation:        generation,
		PreparedAt:        now,
		Topology: platformcontrol.ExpectedConsumerTopology{NodeUpdaters: []model.NodeUpdater{{
			ID:              "updater-" + nodeID,
			ClusterNodeName: nodeID,
			Status:          model.NodeUpdaterStatusActive,
		}}},
	})
	if err != nil {
		t.Fatalf("build image-cache expected set: %v", err)
	}
	return set
}

func imageCacheTrustedHeartbeatRelease(
	set model.PlatformExpectedConsumerSet,
	artifactID string,
	fencingToken int64,
) model.PlatformArtifactRelease {
	return model.PlatformArtifactRelease{
		ID:                set.ArtifactReleaseID,
		ArtifactID:        artifactID,
		ArtifactKind:      set.ArtifactKind,
		Scope:             set.Scope,
		ScopeKey:          set.ScopeKey,
		Generation:        set.ExpectedGeneration,
		ReleaseChannel:    model.PlatformArtifactReleaseChannelShadow,
		Status:            model.PlatformArtifactReleaseStatusActive,
		FencingToken:      fencingToken,
		VerificationState: model.PlatformArtifactVerificationStateServingUnverified,
	}
}

func imageCacheTrustedHeartbeatEnvelope(
	t *testing.T,
	claims platformcontrol.PlatformComponentIdentityClaims,
	set model.PlatformExpectedConsumerSet,
	issuedAt time.Time,
	sequence int64,
	generationSequence int64,
	fencingToken int64,
	nonce string,
) platformcontrol.PlatformConsumerHeartbeatEnvelope {
	t.Helper()
	heartbeat, err := platformcontrol.BindPlatformConsumerHeartbeatToExpectedSet(claims, set, platformcontrol.PlatformConsumerHeartbeatEnvelope{
		ArtifactKind:       set.ArtifactKind,
		FencingToken:       fencingToken,
		ProtocolVersion:    model.PlatformConsumerProtocolVersionV1,
		SchemaVersion:      model.PlatformConsumerSchemaVersionV1,
		Sequence:           sequence,
		IssuedAt:           issuedAt,
		Nonce:              nonce,
		GenerationSequence: generationSequence,
		ActualGeneration:   set.ExpectedGeneration,
		ApplyStatus:        model.PlatformConsumerApplyStatusApplied,
		ProbeStatus:        model.PlatformConsumerProbeStatusPassed,
	})
	if err != nil {
		t.Fatalf("bind image-cache heartbeat: %v", err)
	}
	heartbeat.EvidenceHash, err = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(heartbeat)
	if err != nil {
		t.Fatalf("hash image-cache heartbeat: %v", err)
	}
	return heartbeat
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
