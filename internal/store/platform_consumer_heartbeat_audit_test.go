package store

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
)

func TestAcceptTrustedPlatformConsumerHeartbeatWithAuditIsAtomicAndChained(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	now := time.Date(2026, 7, 11, 6, 0, 0, 0, time.UTC)
	set := trustedHeartbeatExpectedSet(t, now)
	if _, err := s.CreatePlatformExpectedConsumerSet(set); err != nil {
		t.Fatalf("create expected consumer set: %v", err)
	}
	claims := trustedHeartbeatClaims(t, now)
	heartbeat := trustedHeartbeatEnvelope(t, claims, set, now)
	keyring := testPlatformConsumerHeartbeatAuditKeyring("audit-key-v1", "heartbeat-audit:key-v1", "", "")

	first, err := s.AcceptTrustedPlatformConsumerHeartbeatWithAudit(
		claims, set.ID, heartbeat, now, platformcontrol.PlatformConsumerHeartbeatValidationPolicy{}, keyring,
	)
	if err != nil {
		t.Fatalf("accept first audited heartbeat: %v", err)
	}

	heartbeat.Sequence++
	heartbeat.IssuedAt = now.Add(time.Second)
	heartbeat.Nonce = "nonce-value-0002"
	heartbeat.EvidenceHash, err = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(heartbeat)
	if err != nil {
		t.Fatalf("hash second heartbeat: %v", err)
	}
	second, err := s.AcceptTrustedPlatformConsumerHeartbeatWithAudit(
		claims, set.ID, heartbeat, now.Add(time.Second), platformcontrol.PlatformConsumerHeartbeatValidationPolicy{}, keyring,
	)
	if err != nil {
		t.Fatalf("accept second audited heartbeat: %v", err)
	}
	if first.ID != second.ID || second.Sequence != first.Sequence+1 {
		t.Fatalf("unexpected audited heartbeat state: first=%+v second=%+v", first, second)
	}

	events, err := s.ListAuditEvents("", true, 100)
	if err != nil {
		t.Fatalf("list heartbeat audit events: %v", err)
	}
	chainID := platformConsumerHeartbeatAuditChainID(second)
	chain := auditEventsForChain(events, chainID)
	if len(chain) != 2 || chain[0].ChainSequence != 1 || chain[1].ChainSequence != 2 ||
		chain[1].PreviousHash != chain[0].EventHash {
		t.Fatalf("unexpected heartbeat audit chain: %+v", chain)
	}
	if err := platformsafety.VerifyTamperEvidentAuditChain(chain, chainID, keyring); err != nil {
		t.Fatalf("verify heartbeat audit chain: %v", err)
	}
	if chain[1].Metadata["heartbeat_sequence"] != "13" ||
		chain[1].Metadata["evidence_hash"] != second.EvidenceHash ||
		chain[1].ActorID != claims.CredentialID || chain[1].TargetID != second.ID {
		t.Fatalf("heartbeat audit metadata is incomplete: %+v", chain[1])
	}
}

func TestAcceptTrustedPlatformConsumerHeartbeatWithAuditRollsBackWhenSigningFails(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	now := time.Date(2026, 7, 11, 6, 15, 0, 0, time.UTC)
	set := trustedHeartbeatExpectedSet(t, now)
	if _, err := s.CreatePlatformExpectedConsumerSet(set); err != nil {
		t.Fatalf("create expected consumer set: %v", err)
	}
	claims := trustedHeartbeatClaims(t, now)
	heartbeat := trustedHeartbeatEnvelope(t, claims, set, now)

	_, err := s.AcceptTrustedPlatformConsumerHeartbeatWithAudit(
		claims,
		set.ID,
		heartbeat,
		now,
		platformcontrol.PlatformConsumerHeartbeatValidationPolicy{},
		bundleauth.Keyring{},
	)
	if err == nil {
		t.Fatal("missing heartbeat audit signing key must fail closed")
	}
	consumers, listErr := s.ListPlatformConsumers(set.ArtifactKind, set.ScopeKey)
	if listErr != nil {
		t.Fatalf("list consumers after failed audit: %v", listErr)
	}
	if len(consumers) != 0 {
		t.Fatalf("failed audit signing must roll back consumer heartbeat: %+v", consumers)
	}
	events, listErr := s.ListAuditEvents("", true, 100)
	if listErr != nil {
		t.Fatalf("list audit after failed signing: %v", listErr)
	}
	if len(events) != 0 {
		t.Fatalf("failed audit signing must not persist an audit event: %+v", events)
	}
}

func TestPlatformConsumerHeartbeatAuditSupportsOverlappingKeyRotation(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 11, 6, 30, 0, 0, time.UTC)
	consumer := model.PlatformConsumerInstance{
		ID:                    "artifactconsumer-key-rotation",
		CredentialID:          "credential-key-rotation",
		TokenID:               "token-key-rotation",
		Component:             model.PlatformConsumerComponentEdgeWorker,
		NodeID:                "edge-key-rotation",
		ArtifactKind:          model.PlatformArtifactKindEdgeRankingPolicy,
		ScopeKey:              "global",
		ExpectedConsumerSetID: "expected-key-rotation",
		Sequence:              1,
		IssuedAt:              &now,
		EvidenceHash:          "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		IdentityVerified:      true,
		LastHeartbeatAt:       now,
	}
	previous := testPlatformConsumerHeartbeatAuditKeyring("audit-v1", "heartbeat-audit:key-v1", "", "")
	first, err := buildPlatformConsumerHeartbeatAuditEvent(consumer, 1, "", previous)
	if err != nil {
		t.Fatalf("build previous-key heartbeat audit: %v", err)
	}
	rotated := testPlatformConsumerHeartbeatAuditKeyring("audit-v2", "heartbeat-audit:key-v2", "audit-v1", "heartbeat-audit:key-v1")
	consumer.Sequence = 2
	consumer.LastHeartbeatAt = now.Add(time.Second)
	second, err := buildPlatformConsumerHeartbeatAuditEvent(consumer, 2, first.EventHash, rotated)
	if err != nil {
		t.Fatalf("build active-key heartbeat audit: %v", err)
	}
	if err := platformsafety.VerifyTamperEvidentAuditChain(
		[]model.AuditEvent{first, second}, platformConsumerHeartbeatAuditChainID(consumer), rotated,
	); err != nil {
		t.Fatalf("overlapping audit keys must verify the complete chain: %v", err)
	}
	rotated.RevokedKeyIDs = map[string]struct{}{"heartbeat-audit:key-v1": {}}
	if err := platformsafety.VerifyTamperEvidentAuditEvent(first, rotated); err == nil {
		t.Fatal("revoked previous audit key must no longer verify")
	}
}

func TestAcceptTrustedPlatformConsumerHeartbeatWithAuditPostgres(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("FUGUE_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set FUGUE_TEST_DATABASE_URL to run trusted heartbeat audit Postgres integration test")
	}
	if !strings.Contains(databaseURL, "fugue-pgtest") && !strings.Contains(databaseURL, "fugue_test") {
		t.Fatalf("refusing to run trusted heartbeat audit integration test against non-test database URL %q", databaseURL)
	}

	s := New("", databaseURL)
	if err := s.Init(); err != nil {
		t.Fatalf("init postgres store: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	suffix := model.NewID("heartbeat-audit")
	nodeID := "edge-" + suffix
	set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{
		ReleaseSetID: "release-" + suffix,
		ArtifactKind: model.PlatformArtifactKindEdgeRankingPolicy,
		ScopeKey:     "global",
		Generation:   "generation-" + suffix,
		PreparedAt:   now,
		Topology: platformcontrol.ExpectedConsumerTopology{EdgeNodes: []model.EdgeNode{{
			ID: nodeID, EdgeGroupID: "edge-group-" + suffix, Country: "US",
		}}},
	})
	if err != nil {
		t.Fatalf("build postgres expected consumer set: %v", err)
	}
	if _, err := s.CreatePlatformExpectedConsumerSet(set); err != nil {
		t.Fatalf("create postgres expected consumer set: %v", err)
	}
	identityKeyring := platformcontrol.PlatformComponentIdentityKeyring{
		ActiveKeyID: "component-key-" + suffix,
		Keys: map[string]string{
			"component-key-" + suffix: "component-secret-" + suffix,
		},
	}
	token, err := platformcontrol.IssuePlatformComponentIdentity(identityKeyring, platformcontrol.PlatformComponentIdentityClaims{
		CredentialID:  "credential-" + suffix,
		Component:     model.PlatformConsumerComponentEdgeWorker,
		NodeID:        nodeID,
		ScopeKey:      "global",
		ArtifactKinds: []string{model.PlatformArtifactKindEdgeRankingPolicy},
	}, now.Add(-time.Second), 5*time.Minute)
	if err != nil {
		t.Fatalf("issue postgres component identity: %v", err)
	}
	claims, err := platformcontrol.ParsePlatformComponentIdentity(identityKeyring, token, now)
	if err != nil {
		t.Fatalf("parse postgres component identity: %v", err)
	}
	heartbeat, err := platformcontrol.BindPlatformConsumerHeartbeatToExpectedSet(claims, set, platformcontrol.PlatformConsumerHeartbeatEnvelope{
		ArtifactKind:       set.ArtifactKind,
		FencingToken:       1,
		ProtocolVersion:    model.PlatformConsumerProtocolVersionV1,
		SchemaVersion:      model.PlatformConsumerSchemaVersionV1,
		Sequence:           1,
		IssuedAt:           now,
		Nonce:              "nonce-" + suffix,
		GenerationSequence: 1,
		ActualGeneration:   set.ExpectedGeneration,
		LKGGeneration:      set.ExpectedGeneration,
		ApplyStatus:        model.PlatformConsumerApplyStatusApplied,
		ProbeStatus:        model.PlatformConsumerProbeStatusPassed,
	})
	if err != nil {
		t.Fatalf("bind postgres heartbeat: %v", err)
	}
	heartbeat.EvidenceHash, err = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(heartbeat)
	if err != nil {
		t.Fatalf("hash postgres heartbeat: %v", err)
	}
	auditKeyring := testPlatformConsumerHeartbeatAuditKeyring("audit-"+suffix, "heartbeat-audit:"+suffix, "", "")
	consumer, err := s.AcceptTrustedPlatformConsumerHeartbeatWithAudit(
		claims, set.ID, heartbeat, now, platformcontrol.PlatformConsumerHeartbeatValidationPolicy{}, auditKeyring,
	)
	if err != nil {
		t.Fatalf("accept postgres audited heartbeat: %v", err)
	}
	failedHeartbeat := heartbeat
	failedHeartbeat.Sequence++
	failedHeartbeat.IssuedAt = now.Add(time.Second)
	failedHeartbeat.Nonce = "nonce-failed-" + suffix
	failedHeartbeat.EvidenceHash, err = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(failedHeartbeat)
	if err != nil {
		t.Fatalf("hash postgres failed heartbeat: %v", err)
	}
	if _, err := s.AcceptTrustedPlatformConsumerHeartbeatWithAudit(
		claims,
		set.ID,
		failedHeartbeat,
		now.Add(time.Second),
		platformcontrol.PlatformConsumerHeartbeatValidationPolicy{},
		bundleauth.Keyring{},
	); err == nil {
		t.Fatal("postgres heartbeat must roll back when audit signing fails")
	}

	consumers, err := s.ListPlatformConsumers(set.ArtifactKind, set.ScopeKey)
	if err != nil {
		t.Fatalf("list postgres consumers: %v", err)
	}
	foundConsumer := false
	for _, candidate := range consumers {
		if candidate.ID == consumer.ID && candidate.IdentityVerified && candidate.Sequence == heartbeat.Sequence {
			foundConsumer = true
			break
		}
	}
	if !foundConsumer {
		t.Fatalf("postgres trusted consumer was not persisted: %+v", consumers)
	}
	events, err := s.ListAuditEvents("", true, 200)
	if err != nil {
		t.Fatalf("list postgres heartbeat audit: %v", err)
	}
	chainID := platformConsumerHeartbeatAuditChainID(consumer)
	chain := auditEventsForChain(events, chainID)
	if len(chain) != 1 || chain[0].TargetID != consumer.ID {
		t.Fatalf("postgres heartbeat audit was not committed atomically: %+v", chain)
	}
	if err := platformsafety.VerifyTamperEvidentAuditChain(chain, chainID, auditKeyring); err != nil {
		t.Fatalf("verify postgres heartbeat audit chain: %v", err)
	}
}

func testPlatformConsumerHeartbeatAuditKeyring(primaryKey, primaryID, previousKey, previousID string) bundleauth.Keyring {
	return bundleauth.NewKeyring(primaryKey, primaryID, previousKey, previousID, nil)
}

func auditEventsForChain(events []model.AuditEvent, chainID string) []model.AuditEvent {
	out := make([]model.AuditEvent, 0, len(events))
	for _, event := range events {
		if event.ChainID == chainID {
			out = append(out, event)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ChainSequence < out[j].ChainSequence
	})
	return out
}
