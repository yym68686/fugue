package main

import (
	"errors"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
)

func TestPlatformComponentIdentityKeyringDoesNotReuseBundleKey(t *testing.T) {
	t.Setenv("FUGUE_BUNDLE_SIGNING_KEY", "widely-distributed-bundle-key")
	t.Setenv("FUGUE_BUNDLE_SIGNING_KEY_ID", "bundle-key-1")
	t.Setenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY", "")
	t.Setenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY_ID", "")

	keyring := platformComponentIdentityKeyringFromEnv()
	if keyring.ActiveKeyID != "" || len(keyring.Keys) != 0 {
		t.Fatalf("bundle key must not populate component identity keyring: %+v", keyring)
	}
	if _, err := platformcontrol.IssuePlatformComponentIdentity(
		keyring, platformComponentIdentityConfigTestClaims(), time.Now().UTC(), time.Minute,
	); !errors.Is(err, platformcontrol.ErrPlatformComponentIdentityInvalid) {
		t.Fatalf("missing dedicated key must fail closed, got %v", err)
	}
}

func TestCompromisedBundleKeyCannotForgePlatformComponentIdentity(t *testing.T) {
	t.Setenv("FUGUE_BUNDLE_SIGNING_KEY", "widely-distributed-bundle-key")
	t.Setenv("FUGUE_BUNDLE_SIGNING_KEY_ID", "bundle-key-1")
	t.Setenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY", "dedicated-component-key")
	t.Setenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY_ID", "component-key-1")

	now := time.Now().UTC()
	forgedKeyring := platformcontrol.DerivePlatformComponentIdentityKeyring(
		"widely-distributed-bundle-key", "bundle-key-1", "", "", nil,
	)
	forgedToken, err := platformcontrol.IssuePlatformComponentIdentity(
		forgedKeyring, platformComponentIdentityConfigTestClaims(), now, time.Minute,
	)
	if err != nil {
		t.Fatalf("issue syntactically valid forged component identity: %v", err)
	}
	apiKeyring := platformComponentIdentityKeyringFromEnv()
	if _, err := platformcontrol.ParsePlatformComponentIdentity(apiKeyring, forgedToken, now); !errors.Is(err, platformcontrol.ErrPlatformComponentIdentityInvalid) {
		t.Fatalf("API verifier accepted a token signed with the distributed bundle key: %v", err)
	}
	legitimateToken, err := platformcontrol.IssuePlatformComponentIdentity(
		apiKeyring, platformComponentIdentityConfigTestClaims(), now, time.Minute,
	)
	if err != nil {
		t.Fatalf("issue legitimate component identity: %v", err)
	}
	if _, err := platformcontrol.ParsePlatformComponentIdentity(apiKeyring, legitimateToken, now); err != nil {
		t.Fatalf("API verifier rejected its dedicated component identity: %v", err)
	}
}

func TestPlatformComponentIdentityKeyringUsesDedicatedRotatingKeys(t *testing.T) {
	t.Setenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY", "dedicated-component-key-current")
	t.Setenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY_ID", "component-key-current")
	t.Setenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_PREVIOUS_SIGNING_KEY", "dedicated-component-key-previous")
	t.Setenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_PREVIOUS_SIGNING_KEY_ID", "component-key-previous")
	t.Setenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_REVOKED_KEY_IDS", "revoked-1; revoked-2\nrevoked-3")

	keyring := platformComponentIdentityKeyringFromEnv()
	if keyring.ActiveKeyID != "component-key-current" || len(keyring.Keys) != 2 {
		t.Fatalf("unexpected dedicated component keyring: %+v", keyring)
	}
	for _, keyID := range []string{"revoked-1", "revoked-2", "revoked-3"} {
		if _, ok := keyring.RevokedKeyIDs[keyID]; !ok {
			t.Fatalf("missing revoked key id %q: %+v", keyID, keyring.RevokedKeyIDs)
		}
	}
	now := time.Now().UTC()
	token, err := platformcontrol.IssuePlatformComponentIdentity(
		keyring, platformComponentIdentityConfigTestClaims(), now, time.Minute,
	)
	if err != nil {
		t.Fatalf("issue component identity: %v", err)
	}
	if _, err := platformcontrol.ParsePlatformComponentIdentity(keyring, token, now.Add(30*time.Second)); err != nil {
		t.Fatalf("parse component identity: %v", err)
	}
}

func TestPlatformConsumerHeartbeatAuditKeyringIsDedicatedRotatableAndRevocable(t *testing.T) {
	now := time.Date(2026, 7, 10, 9, 30, 0, 0, time.UTC)
	t.Setenv("FUGUE_BUNDLE_SIGNING_KEY", "widely-distributed-bundle-key")
	t.Setenv("FUGUE_BUNDLE_SIGNING_KEY_ID", "bundle-key-1")
	t.Setenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY", "component-secret-v1")
	t.Setenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY_ID", "component-key-v1")
	t.Setenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_PREVIOUS_SIGNING_KEY", "")
	t.Setenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_PREVIOUS_SIGNING_KEY_ID", "")
	t.Setenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_REVOKED_KEY_IDS", "")

	identityKeyring := platformComponentIdentityKeyringFromEnv()
	previousAudit := platformConsumerHeartbeatAuditKeyringFromEnv()
	if previousAudit.PrimaryKeyID != "heartbeat-audit:component-key-v1" ||
		previousAudit.PrimaryKey == "" ||
		previousAudit.PrimaryKey == identityKeyring.Keys[identityKeyring.ActiveKeyID] ||
		previousAudit.PrimaryKey == "widely-distributed-bundle-key" {
		t.Fatalf("heartbeat audit key must be dedicated and domain-separated: identity=%+v audit=%+v", identityKeyring, previousAudit)
	}

	event, err := platformsafety.SignTamperEvidentAuditEvent(model.AuditEvent{
		ID:            "audit-heartbeat-1",
		ActorType:     "platform_component",
		ActorID:       "credential-1",
		Action:        "platform_consumer.heartbeat_accepted",
		TargetType:    "platform_consumer",
		TargetID:      "edge-worker:edge-node-1",
		ChainID:       "platform-consumer-heartbeat",
		ChainSequence: 1,
		CreatedAt:     now,
	}, previousAudit)
	if err != nil {
		t.Fatalf("sign heartbeat audit event: %v", err)
	}

	t.Setenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY", "component-secret-v2")
	t.Setenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY_ID", "component-key-v2")
	t.Setenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_PREVIOUS_SIGNING_KEY", "component-secret-v1")
	t.Setenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_PREVIOUS_SIGNING_KEY_ID", "component-key-v1")
	rotatedAudit := platformConsumerHeartbeatAuditKeyringFromEnv()
	if rotatedAudit.PrimaryKeyID != "heartbeat-audit:component-key-v2" ||
		rotatedAudit.PreviousKeyID != "heartbeat-audit:component-key-v1" {
		t.Fatalf("rotated heartbeat audit keyring has wrong key ids: %+v", rotatedAudit)
	}
	if err := platformsafety.VerifyTamperEvidentAuditEvent(event, rotatedAudit); err != nil {
		t.Fatalf("rotation overlap must verify an audit event signed by the previous key: %v", err)
	}

	t.Setenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_REVOKED_KEY_IDS", "component-key-v1")
	revokedAudit := platformConsumerHeartbeatAuditKeyringFromEnv()
	if err := platformsafety.VerifyTamperEvidentAuditEvent(event, revokedAudit); err == nil {
		t.Fatal("revoked previous component identity key must not verify heartbeat audit events")
	}
}

func TestPlatformConsumerHeartbeatAuditKeyringDoesNotFallBackToBundleKey(t *testing.T) {
	t.Setenv("FUGUE_BUNDLE_SIGNING_KEY", "widely-distributed-bundle-key")
	t.Setenv("FUGUE_BUNDLE_SIGNING_KEY_ID", "bundle-key-1")
	t.Setenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY", "")
	t.Setenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_SIGNING_KEY_ID", "")
	t.Setenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_PREVIOUS_SIGNING_KEY", "")
	t.Setenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_PREVIOUS_SIGNING_KEY_ID", "")
	t.Setenv("FUGUE_PLATFORM_COMPONENT_IDENTITY_REVOKED_KEY_IDS", "")

	auditKeyring := platformConsumerHeartbeatAuditKeyringFromEnv()
	if auditKeyring.PrimaryKey != "" || auditKeyring.PrimaryKeyID != "" {
		t.Fatalf("bundle key must not populate heartbeat audit keyring: %+v", auditKeyring)
	}
	if _, err := platformsafety.SignTamperEvidentAuditEvent(model.AuditEvent{
		ChainID:       "platform-consumer-heartbeat",
		ChainSequence: 1,
	}, auditKeyring); err == nil {
		t.Fatal("missing dedicated component identity secret must fail heartbeat audit signing closed")
	}
}

func platformComponentIdentityConfigTestClaims() platformcontrol.PlatformComponentIdentityClaims {
	return platformcontrol.PlatformComponentIdentityClaims{
		CredentialID:  "credential-api-test",
		Component:     model.PlatformConsumerComponentEdgeWorker,
		NodeID:        "edge-api-test",
		ScopeKey:      "global",
		ArtifactKinds: []string{model.PlatformArtifactKindEdgeRankingPolicy},
	}
}
