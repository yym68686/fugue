package main

import (
	"errors"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
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

func platformComponentIdentityConfigTestClaims() platformcontrol.PlatformComponentIdentityClaims {
	return platformcontrol.PlatformComponentIdentityClaims{
		CredentialID:  "credential-api-test",
		Component:     model.PlatformConsumerComponentEdgeWorker,
		NodeID:        "edge-api-test",
		ScopeKey:      "global",
		ArtifactKinds: []string{model.PlatformArtifactKindEdgeRankingPolicy},
	}
}
