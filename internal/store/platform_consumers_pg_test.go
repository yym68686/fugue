package store

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"

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

func TestPostgresAcceptTrustedPlatformConsumerHeartbeatIsTransactional(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	now := time.Date(2026, 7, 10, 13, 0, 0, 0, time.UTC)
	set := trustedHeartbeatExpectedSet(t, now)
	claims := trustedHeartbeatClaims(t, now)
	heartbeat := trustedHeartbeatEnvelope(t, claims, set, now)
	verified, _, err := platformcontrol.VerifyTrustedPlatformConsumerHeartbeat(
		claims, set, heartbeat, nil, now, platformcontrol.PlatformConsumerHeartbeatValidationPolicy{},
	)
	if err != nil {
		t.Fatalf("build verified consumer: %v", err)
	}
	verified.ID = platformConsumerInstanceID(verified.ConsumerID, verified.ArtifactKind, verified.ScopeKey)

	mock.ExpectBegin()
	mock.ExpectQuery(`(?s)FROM fugue_platform_expected_consumer_sets\s+WHERE id = \$1\s+FOR SHARE`).
		WithArgs(set.ID).
		WillReturnRows(expectedConsumerSetRows(t, set))
	mock.ExpectQuery(`(?s)FROM fugue_platform_consumer_instances\s+WHERE consumer_id = \$1 AND artifact_kind = \$2 AND scope_key = \$3\s+FOR UPDATE`).
		WithArgs(verified.ConsumerID, verified.ArtifactKind, verified.ScopeKey).
		WillReturnRows(sqlmock.NewRows(platformConsumerColumns()))
	mock.ExpectQuery(`(?s)INSERT INTO fugue_platform_consumer_instances .*WHERE NOT fugue_platform_consumer_instances.identity_verified.*EXCLUDED.sequence > fugue_platform_consumer_instances.sequence.*RETURNING`).
		WillReturnRows(platformConsumerRows(t, verified))
	mock.ExpectCommit()

	created, err := s.AcceptTrustedPlatformConsumerHeartbeat(
		claims, set.ID, heartbeat, now, platformcontrol.PlatformConsumerHeartbeatValidationPolicy{},
	)
	if err != nil {
		t.Fatalf("accept postgres trusted heartbeat: %v", err)
	}
	if !created.IdentityVerified || created.ID != verified.ID || created.Sequence != heartbeat.Sequence ||
		created.ExpectedConsumerSetID != set.ID || created.CredentialID != claims.CredentialID {
		t.Fatalf("unexpected trusted postgres consumer: %+v", created)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestPostgresTrustedHeartbeatRejectsConcurrentReplayAtUpsert(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	now := time.Date(2026, 7, 10, 13, 15, 0, 0, time.UTC)
	set := trustedHeartbeatExpectedSet(t, now)
	claims := trustedHeartbeatClaims(t, now)
	heartbeat := trustedHeartbeatEnvelope(t, claims, set, now)
	bound, err := platformcontrol.BindPlatformConsumerHeartbeatToExpectedSet(claims, set, heartbeat)
	if err != nil {
		t.Fatalf("bind heartbeat: %v", err)
	}

	mock.ExpectBegin()
	mock.ExpectQuery(`(?s)FROM fugue_platform_expected_consumer_sets\s+WHERE id = \$1\s+FOR SHARE`).
		WithArgs(set.ID).
		WillReturnRows(expectedConsumerSetRows(t, set))
	mock.ExpectQuery(`(?s)FROM fugue_platform_consumer_instances\s+WHERE consumer_id = \$1 AND artifact_kind = \$2 AND scope_key = \$3\s+FOR UPDATE`).
		WithArgs(bound.ConsumerID, bound.ArtifactKind, bound.ScopeKey).
		WillReturnRows(sqlmock.NewRows(platformConsumerColumns()))
	mock.ExpectQuery(`(?s)INSERT INTO fugue_platform_consumer_instances .*EXCLUDED.nonce <> fugue_platform_consumer_instances.nonce.*RETURNING`).
		WillReturnRows(sqlmock.NewRows(platformConsumerColumns()))
	mock.ExpectRollback()

	_, err = s.AcceptTrustedPlatformConsumerHeartbeat(
		claims, set.ID, heartbeat, now, platformcontrol.PlatformConsumerHeartbeatValidationPolicy{},
	)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("concurrent replay must lose guarded upsert, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestPostgresTrustedHeartbeatRejectsReplayBeforeWrite(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	now := time.Date(2026, 7, 10, 13, 30, 0, 0, time.UTC)
	set := trustedHeartbeatExpectedSet(t, now)
	claims := trustedHeartbeatClaims(t, now)
	heartbeat := trustedHeartbeatEnvelope(t, claims, set, now)
	existing, _, err := platformcontrol.VerifyTrustedPlatformConsumerHeartbeat(
		claims, set, heartbeat, nil, now, platformcontrol.PlatformConsumerHeartbeatValidationPolicy{},
	)
	if err != nil {
		t.Fatalf("build existing consumer: %v", err)
	}
	existing.ID = platformConsumerInstanceID(existing.ConsumerID, existing.ArtifactKind, existing.ScopeKey)

	mock.ExpectBegin()
	mock.ExpectQuery(`(?s)FROM fugue_platform_expected_consumer_sets\s+WHERE id = \$1\s+FOR SHARE`).
		WithArgs(set.ID).
		WillReturnRows(expectedConsumerSetRows(t, set))
	mock.ExpectQuery(`(?s)FROM fugue_platform_consumer_instances\s+WHERE consumer_id = \$1 AND artifact_kind = \$2 AND scope_key = \$3\s+FOR UPDATE`).
		WithArgs(existing.ConsumerID, existing.ArtifactKind, existing.ScopeKey).
		WillReturnRows(platformConsumerRows(t, existing))
	mock.ExpectRollback()

	_, err = s.AcceptTrustedPlatformConsumerHeartbeat(
		claims, set.ID, heartbeat, now.Add(time.Second), platformcontrol.PlatformConsumerHeartbeatValidationPolicy{},
	)
	if !errors.Is(err, platformcontrol.ErrPlatformConsumerHeartbeatReplay) {
		t.Fatalf("stored replay must be rejected before write, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestPostgresLegacyHeartbeatCannotOverwriteVerifiedConsumer(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	mock.ExpectQuery(`(?s)INSERT INTO fugue_platform_consumer_instances .*WHERE NOT fugue_platform_consumer_instances.identity_verified\s+RETURNING`).
		WillReturnRows(sqlmock.NewRows(platformConsumerColumns()))

	_, err = s.UpsertPlatformConsumerHeartbeat(model.PlatformConsumerHeartbeatRequest{
		ConsumerID:   "edge-worker:edge-node-1",
		ArtifactKind: model.PlatformArtifactKindEdgeRankingPolicy,
		ScopeKey:     "global",
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("legacy postgres heartbeat must not downgrade verified state, got %v", err)
	}
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
	return sqlmock.NewRows(platformConsumerColumns()).AddRow(
		consumer.ID, consumer.ConsumerID, consumer.CredentialID, consumer.TokenID, consumer.Component, consumer.NodeID, consumer.ArtifactKind, consumer.ScopeKey,
		consumer.ReleaseSetID, consumer.ExpectedConsumerSetID, consumer.FencingToken, supportedKindsJSON,
		consumer.ProtocolVersion, consumer.SchemaVersion, compatibilityCapabilitiesJSON,
		consumer.Sequence, issuedAt, consumer.Nonce, consumer.GenerationSequence, consumer.EvidenceHash, consumer.IdentityVerified,
		consumer.DesiredGeneration, consumer.ActualGeneration, consumer.LKGGeneration, consumer.ApplyStatus, consumer.ProbeStatus,
		consumer.ServingLKG, consumer.LKGExpired, consumer.LastError, consumer.LastHeartbeatAt, consumer.UpdatedAt,
	)
}

func platformConsumerColumns() []string {
	return []string{
		"id", "consumer_id", "credential_id", "token_id", "component", "node_id", "artifact_kind", "scope_key",
		"release_set_id", "expected_consumer_set_id", "fencing_token", "supported_kinds_json",
		"protocol_version", "schema_version", "compatibility_capabilities_json",
		"sequence", "issued_at", "nonce", "generation_sequence", "evidence_hash", "identity_verified",
		"desired_generation", "actual_generation", "lkg_generation", "apply_status", "probe_status",
		"serving_lkg", "lkg_expired", "last_error", "last_heartbeat_at", "updated_at",
	}
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
