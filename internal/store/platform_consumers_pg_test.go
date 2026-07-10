package store

import (
	"encoding/json"
	"testing"
	"time"

	"fugue/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPostgresPlatformConsumerHeartbeatPersistsShadowEnvelope(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	issuedAt := time.Date(2026, 7, 10, 11, 0, 0, 0, time.UTC)
	consumer := normalizePlatformConsumerHeartbeat(model.PlatformConsumerHeartbeatRequest{
		ConsumerID:                "edge-worker:edge-node-1",
		Component:                 model.PlatformConsumerComponentEdgeWorker,
		NodeID:                    "edge-node-1",
		ArtifactKind:              model.PlatformArtifactKindEdgeRankingPolicy,
		ScopeKey:                  "global",
		ReleaseSetID:              "release-set-1",
		ExpectedConsumerSetID:     "expected-set-1",
		FencingToken:              8,
		SupportedKinds:            []string{model.PlatformArtifactKindEdgeRankingPolicy},
		ProtocolVersion:           model.PlatformConsumerProtocolVersionV1,
		SchemaVersion:             model.PlatformConsumerSchemaVersionV1,
		CompatibilityCapabilities: []string{"route-bundle-v1"},
		Sequence:                  12,
		IssuedAt:                  &issuedAt,
		Nonce:                     "nonce-value-0001",
		GenerationSequence:        42,
		EvidenceHash:              "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		DesiredGeneration:         "generation-42",
		ActualGeneration:          "generation-42",
		LKGGeneration:             "generation-41",
		ApplyStatus:               model.PlatformConsumerApplyStatusApplied,
		ProbeStatus:               model.PlatformConsumerProbeStatusPassed,
	})

	mock.ExpectQuery(`(?s)INSERT INTO fugue_platform_consumer_instances .*RETURNING`).
		WillReturnRows(platformConsumerRows(t, consumer))
	created, err := s.UpsertPlatformConsumerHeartbeat(model.PlatformConsumerHeartbeatRequest{
		ConsumerID:                consumer.ConsumerID,
		Component:                 consumer.Component,
		NodeID:                    consumer.NodeID,
		ArtifactKind:              consumer.ArtifactKind,
		ScopeKey:                  consumer.ScopeKey,
		ReleaseSetID:              consumer.ReleaseSetID,
		ExpectedConsumerSetID:     consumer.ExpectedConsumerSetID,
		FencingToken:              consumer.FencingToken,
		SupportedKinds:            consumer.SupportedKinds,
		ProtocolVersion:           consumer.ProtocolVersion,
		SchemaVersion:             consumer.SchemaVersion,
		CompatibilityCapabilities: consumer.CompatibilityCapabilities,
		Sequence:                  consumer.Sequence,
		IssuedAt:                  consumer.IssuedAt,
		Nonce:                     consumer.Nonce,
		GenerationSequence:        consumer.GenerationSequence,
		EvidenceHash:              consumer.EvidenceHash,
		DesiredGeneration:         consumer.DesiredGeneration,
		ActualGeneration:          consumer.ActualGeneration,
		LKGGeneration:             consumer.LKGGeneration,
		ApplyStatus:               consumer.ApplyStatus,
		ProbeStatus:               consumer.ProbeStatus,
	})
	if err != nil {
		t.Fatalf("upsert postgres consumer heartbeat: %v", err)
	}
	assertPlatformConsumerEnvelope(t, created, issuedAt)

	mock.ExpectQuery(`(?s)FROM fugue_platform_consumer_instances\s+WHERE artifact_kind = \$1 AND scope_key = \$2`).
		WithArgs(consumer.ArtifactKind, consumer.ScopeKey).
		WillReturnRows(platformConsumerRows(t, consumer))
	listed, err := s.ListPlatformConsumers(consumer.ArtifactKind, consumer.ScopeKey)
	if err != nil {
		t.Fatalf("list postgres consumer heartbeats: %v", err)
	}
	if len(listed) != 1 {
		t.Fatalf("expected one consumer heartbeat, got %+v", listed)
	}
	assertPlatformConsumerEnvelope(t, listed[0], issuedAt)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func platformConsumerRows(t *testing.T, consumer model.PlatformConsumerInstance) *sqlmock.Rows {
	t.Helper()
	supportedKindsJSON, err := json.Marshal(consumer.SupportedKinds)
	if err != nil {
		t.Fatalf("marshal supported kinds: %v", err)
	}
	compatibilityCapabilitiesJSON, err := json.Marshal(consumer.CompatibilityCapabilities)
	if err != nil {
		t.Fatalf("marshal compatibility capabilities: %v", err)
	}
	var issuedAt any
	if consumer.IssuedAt != nil {
		issuedAt = *consumer.IssuedAt
	}
	return sqlmock.NewRows([]string{
		"id", "consumer_id", "credential_id", "token_id", "component", "node_id", "artifact_kind", "scope_key",
		"release_set_id", "expected_consumer_set_id", "fencing_token", "supported_kinds_json",
		"protocol_version", "schema_version", "compatibility_capabilities_json",
		"sequence", "issued_at", "nonce", "generation_sequence", "evidence_hash", "identity_verified",
		"desired_generation", "actual_generation", "lkg_generation", "apply_status", "probe_status",
		"serving_lkg", "lkg_expired", "last_error", "last_heartbeat_at", "updated_at",
	}).AddRow(
		consumer.ID, consumer.ConsumerID, consumer.CredentialID, consumer.TokenID, consumer.Component, consumer.NodeID, consumer.ArtifactKind, consumer.ScopeKey,
		consumer.ReleaseSetID, consumer.ExpectedConsumerSetID, consumer.FencingToken, supportedKindsJSON,
		consumer.ProtocolVersion, consumer.SchemaVersion, compatibilityCapabilitiesJSON,
		consumer.Sequence, issuedAt, consumer.Nonce, consumer.GenerationSequence, consumer.EvidenceHash, consumer.IdentityVerified,
		consumer.DesiredGeneration, consumer.ActualGeneration, consumer.LKGGeneration, consumer.ApplyStatus, consumer.ProbeStatus,
		consumer.ServingLKG, consumer.LKGExpired, consumer.LastError, consumer.LastHeartbeatAt, consumer.UpdatedAt,
	)
}

func assertPlatformConsumerEnvelope(t *testing.T, consumer model.PlatformConsumerInstance, issuedAt time.Time) {
	t.Helper()
	if consumer.ReleaseSetID != "release-set-1" ||
		consumer.ExpectedConsumerSetID != "expected-set-1" ||
		consumer.FencingToken != 8 ||
		consumer.ProtocolVersion != model.PlatformConsumerProtocolVersionV1 ||
		consumer.SchemaVersion != model.PlatformConsumerSchemaVersionV1 ||
		len(consumer.CompatibilityCapabilities) != 1 ||
		consumer.Sequence != 12 ||
		consumer.IssuedAt == nil || !consumer.IssuedAt.Equal(issuedAt) ||
		consumer.Nonce != "nonce-value-0001" ||
		consumer.GenerationSequence != 42 ||
		consumer.EvidenceHash == "" ||
		consumer.IdentityVerified {
		t.Fatalf("unexpected platform consumer envelope: %+v", consumer)
	}
}
