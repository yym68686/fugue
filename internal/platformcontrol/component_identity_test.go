package platformcontrol

import (
	"errors"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestPlatformComponentIdentitySupportsSigningKeyRotation(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 8, 0, 0, 0, time.UTC)
	keyring := PlatformComponentIdentityKeyring{
		ActiveKeyID: "identity-key-2",
		Keys: map[string]string{
			"identity-key-1": "old-signing-secret",
			"identity-key-2": "current-signing-secret",
		},
	}
	token, err := IssuePlatformComponentIdentity(keyring, platformComponentTestClaims(), now, 5*time.Minute)
	if err != nil {
		t.Fatalf("issue component identity: %v", err)
	}
	claims, err := ParsePlatformComponentIdentity(keyring, token, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("parse component identity: %v", err)
	}
	if claims.Component != model.PlatformConsumerComponentEdgeWorker || claims.NodeID != "edge-node-1" {
		t.Fatalf("unexpected parsed claims: %+v", claims)
	}

	rotated := PlatformComponentIdentityKeyring{
		ActiveKeyID: "identity-key-3",
		Keys: map[string]string{
			"identity-key-2": "current-signing-secret",
			"identity-key-3": "next-signing-secret",
		},
	}
	if _, err := ParsePlatformComponentIdentity(rotated, token, now.Add(2*time.Minute)); err != nil {
		t.Fatalf("retained verification key must validate an in-flight token: %v", err)
	}
	delete(rotated.Keys, "identity-key-2")
	if _, err := ParsePlatformComponentIdentity(rotated, token, now.Add(2*time.Minute)); !errors.Is(err, ErrPlatformComponentIdentityInvalid) {
		t.Fatalf("removed verification key must revoke its tokens, got %v", err)
	}
}

func TestDerivePlatformComponentIdentityKeyringSupportsRotationAndRevocation(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 9, 0, 0, 0, time.UTC)
	keyring := DerivePlatformComponentIdentityKeyring(
		"active-bundle-secret",
		"bundle-key-2",
		"previous-bundle-secret",
		"bundle-key-1",
		nil,
	)
	if keyring.ActiveKeyID != "bundle-key-2" ||
		keyring.Keys["bundle-key-2"] == "active-bundle-secret" ||
		keyring.Keys["bundle-key-1"] == "previous-bundle-secret" {
		t.Fatalf("component keyring must use domain-separated derived keys: %+v", keyring)
	}

	oldToken, err := IssuePlatformComponentIdentity(PlatformComponentIdentityKeyring{
		ActiveKeyID: "bundle-key-1",
		Keys: map[string]string{
			"bundle-key-1": keyring.Keys["bundle-key-1"],
		},
	}, platformComponentTestClaims(), now, 5*time.Minute)
	if err != nil {
		t.Fatalf("issue token with previous key: %v", err)
	}
	if _, err := ParsePlatformComponentIdentity(keyring, oldToken, now.Add(time.Minute)); err != nil {
		t.Fatalf("rotation overlap must accept the previous derived key: %v", err)
	}

	revoked := DerivePlatformComponentIdentityKeyring(
		"active-bundle-secret",
		"bundle-key-2",
		"previous-bundle-secret",
		"bundle-key-1",
		[]string{"bundle-key-1"},
	)
	if _, ok := revoked.Keys["bundle-key-1"]; ok {
		t.Fatal("revoked key must not remain in the component keyring")
	}
	if _, err := ParsePlatformComponentIdentity(revoked, oldToken, now.Add(time.Minute)); !errors.Is(err, ErrPlatformComponentIdentityInvalid) {
		t.Fatalf("revoked component signing key must reject in-flight tokens, got %v", err)
	}

	activeRevoked := DerivePlatformComponentIdentityKeyring(
		"active-bundle-secret",
		"bundle-key-2",
		"",
		"",
		[]string{"bundle-key-2"},
	)
	if activeRevoked.ActiveKeyID != "" {
		t.Fatalf("revoked active key must disable token issuance: %+v", activeRevoked)
	}
	if _, err := IssuePlatformComponentIdentity(activeRevoked, platformComponentTestClaims(), now, time.Minute); !errors.Is(err, ErrPlatformComponentIdentityInvalid) {
		t.Fatalf("revoked active key must fail issuance closed, got %v", err)
	}
}

func TestPlatformComponentIdentityRejectsExpiredAndTamperedTokens(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 8, 0, 0, 0, time.UTC)
	keyring := platformComponentTestKeyring()
	token, err := IssuePlatformComponentIdentity(keyring, platformComponentTestClaims(), now, time.Minute)
	if err != nil {
		t.Fatalf("issue component identity: %v", err)
	}
	if _, err := ParsePlatformComponentIdentity(keyring, token, now.Add(time.Minute)); !errors.Is(err, ErrPlatformComponentIdentityExpired) {
		t.Fatalf("expired token must be rejected, got %v", err)
	}
	replacement := "x"
	if strings.HasSuffix(token, replacement) {
		replacement = "y"
	}
	tampered := token[:len(token)-1] + replacement
	if _, err := ParsePlatformComponentIdentity(keyring, tampered, now.Add(30*time.Second)); !errors.Is(err, ErrPlatformComponentIdentityInvalid) {
		t.Fatalf("tampered token must be rejected, got %v", err)
	}
}

func TestBindPlatformConsumerHeartbeatUsesCredentialClaims(t *testing.T) {
	t.Parallel()

	claims := platformComponentTestClaims()
	claims.Version = platformComponentIdentityVersion
	claims.TokenID = "token-1"
	claims.IssuedAtUnix = 1
	claims.ExpiresAtUnix = 2
	bound, err := BindPlatformConsumerHeartbeat(claims, PlatformConsumerHeartbeatEnvelope{
		ArtifactKind: model.PlatformArtifactKindEdgeRankingPolicy,
	})
	if err != nil {
		t.Fatalf("bind heartbeat: %v", err)
	}
	if bound.ConsumerID != "edge-worker:edge-node-1" ||
		bound.Component != model.PlatformConsumerComponentEdgeWorker ||
		bound.NodeID != "edge-node-1" ||
		bound.ScopeKey != "global" {
		t.Fatalf("heartbeat was not bound to credential claims: %+v", bound)
	}

	_, err = BindPlatformConsumerHeartbeat(claims, PlatformConsumerHeartbeatEnvelope{
		Component:    model.PlatformConsumerComponentDNSServer,
		ArtifactKind: model.PlatformArtifactKindEdgeRankingPolicy,
		ScopeKey:     "global",
	})
	if !errors.Is(err, ErrPlatformConsumerHeartbeatImpersonation) {
		t.Fatalf("cross-component claim must be rejected, got %v", err)
	}
	_, err = BindPlatformConsumerHeartbeat(claims, PlatformConsumerHeartbeatEnvelope{
		ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle,
		ScopeKey:     "global",
	})
	if !errors.Is(err, ErrPlatformConsumerHeartbeatImpersonation) {
		t.Fatalf("unsupported artifact claim must be rejected, got %v", err)
	}
}

func TestValidatePlatformConsumerHeartbeatRejectsReplayAndRollback(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 8, 0, 0, 0, time.UTC)
	heartbeat := validPlatformConsumerHeartbeat(t, now)
	if err := ValidatePlatformConsumerHeartbeat(heartbeat, nil, now, PlatformConsumerHeartbeatValidationPolicy{}); err != nil {
		t.Fatalf("validate initial heartbeat: %v", err)
	}
	previous := AdvancePlatformConsumerHeartbeatCursor(nil, heartbeat, 4)

	tests := []struct {
		name    string
		mutate  func(*PlatformConsumerHeartbeatEnvelope)
		wantErr error
	}{
		{
			name: "duplicate sequence",
			mutate: func(candidate *PlatformConsumerHeartbeatEnvelope) {
				candidate.Nonce = "nonce-value-0002"
			},
			wantErr: ErrPlatformConsumerHeartbeatReplay,
		},
		{
			name: "duplicate nonce",
			mutate: func(candidate *PlatformConsumerHeartbeatEnvelope) {
				candidate.Sequence++
			},
			wantErr: ErrPlatformConsumerHeartbeatReplay,
		},
		{
			name: "generation rollback",
			mutate: func(candidate *PlatformConsumerHeartbeatEnvelope) {
				candidate.Sequence++
				candidate.Nonce = "nonce-value-0003"
				candidate.GenerationSequence--
			},
			wantErr: ErrPlatformConsumerHeartbeatGenerationBack,
		},
		{
			name: "fencing rollback",
			mutate: func(candidate *PlatformConsumerHeartbeatEnvelope) {
				candidate.Sequence++
				candidate.Nonce = "nonce-value-0004"
				candidate.FencingToken--
			},
			wantErr: ErrPlatformConsumerHeartbeatFencingBack,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			candidate := heartbeat
			test.mutate(&candidate)
			candidate.EvidenceHash = mustPlatformConsumerHeartbeatHash(t, candidate)
			if err := ValidatePlatformConsumerHeartbeat(candidate, &previous, now.Add(time.Second), PlatformConsumerHeartbeatValidationPolicy{}); !errors.Is(err, test.wantErr) {
				t.Fatalf("expected %v, got %v", test.wantErr, err)
			}
		})
	}
}

func TestValidatePlatformConsumerHeartbeatRejectsBadTimeAndEvidence(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 8, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		mutate  func(*PlatformConsumerHeartbeatEnvelope)
		wantErr error
	}{
		{
			name: "future",
			mutate: func(candidate *PlatformConsumerHeartbeatEnvelope) {
				candidate.IssuedAt = now.Add(31 * time.Second)
			},
			wantErr: ErrPlatformConsumerHeartbeatFuture,
		},
		{
			name: "stale",
			mutate: func(candidate *PlatformConsumerHeartbeatEnvelope) {
				candidate.IssuedAt = now.Add(-2*time.Minute - time.Second)
			},
			wantErr: ErrPlatformConsumerHeartbeatStale,
		},
		{
			name: "tampered evidence",
			mutate: func(candidate *PlatformConsumerHeartbeatEnvelope) {
				candidate.ActualGeneration = "generation-tampered"
			},
			wantErr: ErrPlatformConsumerHeartbeatEvidence,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			candidate := validPlatformConsumerHeartbeat(t, now)
			test.mutate(&candidate)
			if test.wantErr != ErrPlatformConsumerHeartbeatEvidence {
				candidate.EvidenceHash = mustPlatformConsumerHeartbeatHash(t, candidate)
			}
			if err := ValidatePlatformConsumerHeartbeat(candidate, nil, now, PlatformConsumerHeartbeatValidationPolicy{}); !errors.Is(err, test.wantErr) {
				t.Fatalf("expected %v, got %v", test.wantErr, err)
			}
		})
	}
}

func platformComponentTestKeyring() PlatformComponentIdentityKeyring {
	return PlatformComponentIdentityKeyring{
		ActiveKeyID: "identity-key-1",
		Keys: map[string]string{
			"identity-key-1": "component-identity-signing-secret",
		},
	}
}

func platformComponentTestClaims() PlatformComponentIdentityClaims {
	return PlatformComponentIdentityClaims{
		CredentialID:  "credential-1",
		Component:     model.PlatformConsumerComponentEdgeWorker,
		NodeID:        "edge-node-1",
		ScopeKey:      "global",
		ArtifactKinds: []string{model.PlatformArtifactKindEdgeRankingPolicy},
	}
}

func validPlatformConsumerHeartbeat(t *testing.T, now time.Time) PlatformConsumerHeartbeatEnvelope {
	t.Helper()
	claims := platformComponentTestClaims()
	claims.Version = platformComponentIdentityVersion
	claims.TokenID = "token-1"
	claims.IssuedAtUnix = now.Add(-time.Minute).Unix()
	claims.ExpiresAtUnix = now.Add(time.Minute).Unix()
	heartbeat, err := BindPlatformConsumerHeartbeat(claims, PlatformConsumerHeartbeatEnvelope{
		ArtifactKind:       model.PlatformArtifactKindEdgeRankingPolicy,
		ScopeKey:           "global",
		ReleaseSetID:       "release-set-1",
		FencingToken:       8,
		ProtocolVersion:    model.PlatformConsumerProtocolVersionV1,
		SchemaVersion:      model.PlatformConsumerSchemaVersionV1,
		Sequence:           12,
		IssuedAt:           now,
		Nonce:              "nonce-value-0001",
		GenerationSequence: 42,
		DesiredGeneration:  "generation-42",
		ActualGeneration:   "generation-42",
		LKGGeneration:      "generation-41",
		ApplyStatus:        model.PlatformConsumerApplyStatusApplied,
		ProbeStatus:        model.PlatformConsumerProbeStatusPassed,
	})
	if err != nil {
		t.Fatalf("bind valid heartbeat: %v", err)
	}
	heartbeat.EvidenceHash = mustPlatformConsumerHeartbeatHash(t, heartbeat)
	return heartbeat
}

func mustPlatformConsumerHeartbeatHash(t *testing.T, heartbeat PlatformConsumerHeartbeatEnvelope) string {
	t.Helper()
	hash, err := ComputePlatformConsumerHeartbeatEvidenceHash(heartbeat)
	if err != nil {
		t.Fatalf("compute heartbeat evidence hash: %v", err)
	}
	if !strings.HasPrefix(hash, "sha256:") {
		t.Fatalf("unexpected evidence hash %q", hash)
	}
	return hash
}
