package platformsafety

import (
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
)

func testPlatformSafetyKeyring() bundleauth.Keyring {
	return bundleauth.NewKeyring("platform-safety-test-key", "platform-safety-test", "", "", nil)
}

func testSignedPlatformArtifact(t *testing.T, content map[string]any) model.PlatformArtifact {
	t.Helper()
	artifact := model.PlatformArtifact{
		ID:                 "artifact-test",
		ArtifactKind:       model.PlatformArtifactKindEdgeRouteBundle,
		Scope:              model.PlatformArtifactScope{ScopeType: "global", Key: "global"},
		ScopeKey:           "global",
		SchemaVersion:      model.PlatformArtifactSchemaVersionV1,
		Generation:         "generation-test",
		GenerationSequence: 1,
		Status:             model.PlatformArtifactStatusValidated,
		Content:            content,
		Metadata:           map[string]string{},
		CreatedAt:          time.Date(2026, 7, 10, 1, 0, 0, 0, time.UTC),
		UpdatedAt:          time.Date(2026, 7, 10, 1, 0, 0, 0, time.UTC),
	}
	artifact.ContentHash = artifactContentHash(artifact.Content)
	signed, err := SignPlatformArtifact(artifact, testPlatformSafetyKeyring())
	if err != nil {
		t.Fatalf("sign artifact: %v", err)
	}
	return signed
}

func TestImmutableInvariantsCannotBeRemovedByCallerMutation(t *testing.T) {
	first := ImmutableInvariantIDs()
	if len(first) == 0 {
		t.Fatal("expected immutable invariants")
	}
	first[0] = "mutated"
	second := ImmutableInvariantIDs()
	if second[0] == "mutated" {
		t.Fatal("immutable invariant registry leaked mutable backing storage")
	}
}

func TestFullReleaseRequiresValidatedArtifactHashAndPinnedRollback(t *testing.T) {
	artifact := testSignedPlatformArtifact(t, map[string]any{"version": "v1"})
	if decision := EvaluateArtifactRelease(artifact, model.PlatformArtifactReleaseChannelFull, "gen_stable", "", 0, testPlatformSafetyKeyring()); !decision.Pass {
		t.Fatalf("expected valid full release, got %+v", decision)
	}
	if decision := EvaluateArtifactRelease(artifact, model.PlatformArtifactReleaseChannelFull, "", "", 0, testPlatformSafetyKeyring()); decision.Pass {
		t.Fatalf("full release without rollback target must fail: %+v", decision)
	}
	if decision := EvaluateArtifactRelease(artifact, model.PlatformArtifactReleaseChannelShadow, "", "", 0, testPlatformSafetyKeyring()); !decision.Pass {
		t.Fatalf("shadow release does not require a production rollback target: %+v", decision)
	}
	artifact.Content["version"] = "tampered"
	if decision := EvaluateArtifactRelease(artifact, model.PlatformArtifactReleaseChannelShadow, "", "", 0, testPlatformSafetyKeyring()); decision.Pass {
		t.Fatalf("tampered content must not pass with a stale content hash: %+v", decision)
	}
}

func TestGrayReleaseRequiresBoundedCanaryScope(t *testing.T) {
	artifact := testSignedPlatformArtifact(t, map[string]any{"version": "v1"})
	for _, ref := range []string{"edge=bwg", "node:test-node", "failure_domain:provider-us"} {
		if decision := EvaluateArtifactRelease(artifact, model.PlatformArtifactReleaseChannelGray, "", ref, 0, testPlatformSafetyKeyring()); !decision.Pass {
			t.Fatalf("expected bounded canary scope %q to pass: %+v", ref, decision)
		}
	}
	for _, ref := range []string{"", "*", "all", "global", "scope=global", "edge=*", "edge=a,b", "unknown=x"} {
		if decision := EvaluateArtifactRelease(artifact, model.PlatformArtifactReleaseChannelGray, "", ref, 0, testPlatformSafetyKeyring()); decision.Pass {
			t.Fatalf("expected unbounded canary scope %q to fail: %+v", ref, decision)
		}
	}
}

func TestOrdinaryReleaseRequiresMonotonicSequenceButRollbackDoesNot(t *testing.T) {
	artifact := testSignedPlatformArtifact(t, map[string]any{"version": "v1"})
	if decision := EvaluateArtifactRelease(
		artifact,
		model.PlatformArtifactReleaseChannelFull,
		"gen_stable",
		"",
		artifact.GenerationSequence,
		testPlatformSafetyKeyring(),
	); decision.Pass {
		t.Fatalf("ordinary release reused a non-monotonic sequence: %+v", decision)
	}
	if decision := EvaluateArtifactRollback(
		artifact,
		model.PlatformArtifactReleaseChannelFull,
		"gen_current",
		"",
		testPlatformSafetyKeyring(),
	); !decision.Pass {
		t.Fatalf("explicit rollback must be allowed to publish an older signed generation: %+v", decision)
	}
}

func TestPublicationOverridesCannotBypassImmutableIntegrityOrCanaryScope(t *testing.T) {
	artifact := testSignedPlatformArtifact(t, map[string]any{"version": "v1"})
	artifact.Status = model.PlatformArtifactStatusDraft

	soft := EvaluateArtifactReleaseWithOverride(
		artifact,
		model.PlatformArtifactReleaseChannelShadow,
		"",
		"",
		0,
		model.PlatformArtifactOverrideModeSoft,
		testPlatformSafetyKeyring(),
	)
	if !soft.Pass ||
		!decisionHasBypassedInvariant(soft, InvariantArtifactValidated) ||
		len(soft.Violations) != 0 {
		t.Fatalf("soft override must bypass only ordinary validation status: %+v", soft)
	}

	tampered := artifact
	tampered.Content = map[string]any{"version": "tampered"}
	soft = EvaluateArtifactReleaseWithOverride(
		tampered,
		model.PlatformArtifactReleaseChannelShadow,
		"",
		"",
		0,
		model.PlatformArtifactOverrideModeSoft,
		testPlatformSafetyKeyring(),
	)
	if soft.Pass ||
		!decisionHasInvariant(soft, InvariantArtifactContentHash) ||
		decisionHasBypassedInvariant(soft, InvariantArtifactContentHash) {
		t.Fatalf("soft override must not bypass content integrity: %+v", soft)
	}

	kernel := EvaluateArtifactReleaseWithOverride(
		artifact,
		model.PlatformArtifactReleaseChannelFull,
		"",
		"",
		artifact.GenerationSequence,
		model.PlatformArtifactOverrideModeKernelBreakGlass,
		testPlatformSafetyKeyring(),
	)
	if !kernel.Pass ||
		!decisionHasBypassedInvariant(kernel, InvariantArtifactValidated) ||
		!decisionHasBypassedInvariant(kernel, InvariantGenerationMonotonic) ||
		!decisionHasBypassedInvariant(kernel, InvariantFullPinnedRollback) {
		t.Fatalf("kernel break-glass must bypass only its explicit recovery allowlist: %+v", kernel)
	}

	kernel = EvaluateArtifactReleaseWithOverride(
		artifact,
		model.PlatformArtifactReleaseChannelGray,
		"",
		"*",
		0,
		model.PlatformArtifactOverrideModeKernelBreakGlass,
		testPlatformSafetyKeyring(),
	)
	if kernel.Pass ||
		!decisionHasInvariant(kernel, InvariantCanaryScopeIsolation) ||
		decisionHasBypassedInvariant(kernel, InvariantCanaryScopeIsolation) {
		t.Fatalf("kernel break-glass must not bypass canary isolation: %+v", kernel)
	}
}

func TestKernelBreakGlassAuthorizationRequiresBoundedTTLAndDualConfirmation(t *testing.T) {
	now := time.Date(2026, 7, 10, 3, 0, 0, 0, time.UTC)
	artifact := testSignedPlatformArtifact(t, map[string]any{"version": "v1"})
	valid := &model.PlatformKernelBreakGlassRequest{
		ExpiresAt:          now.Add(5 * time.Minute),
		Confirmation:       KernelBreakGlassConfirmation,
		TargetConfirmation: artifact.ID,
	}
	if err := ValidateKernelBreakGlassAuthorization(valid, artifact, now); err != nil {
		t.Fatalf("valid bounded kernel break-glass authorization was rejected: %v", err)
	}

	tests := []struct {
		name          string
		authorization *model.PlatformKernelBreakGlassRequest
	}{
		{name: "missing", authorization: nil},
		{name: "expired", authorization: &model.PlatformKernelBreakGlassRequest{
			ExpiresAt:          now,
			Confirmation:       KernelBreakGlassConfirmation,
			TargetConfirmation: artifact.ID,
		}},
		{name: "ttl-too-long", authorization: &model.PlatformKernelBreakGlassRequest{
			ExpiresAt:          now.Add(KernelBreakGlassMaxTTL + time.Second),
			Confirmation:       KernelBreakGlassConfirmation,
			TargetConfirmation: artifact.ID,
		}},
		{name: "bad-safety-confirmation", authorization: &model.PlatformKernelBreakGlassRequest{
			ExpiresAt:          now.Add(time.Minute),
			Confirmation:       "BYPASS",
			TargetConfirmation: artifact.ID,
		}},
		{name: "bad-target-confirmation", authorization: &model.PlatformKernelBreakGlassRequest{
			ExpiresAt:          now.Add(time.Minute),
			Confirmation:       KernelBreakGlassConfirmation,
			TargetConfirmation: "other-artifact",
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateKernelBreakGlassAuthorization(test.authorization, artifact, now); err == nil {
				t.Fatalf("invalid authorization was accepted: %+v", test.authorization)
			}
		})
	}
}

func TestTamperEvidentAuditChainRejectsMutationAndBrokenLinks(t *testing.T) {
	now := time.Date(2026, 7, 10, 4, 0, 0, 0, time.UTC)
	first, err := SignTamperEvidentAuditEvent(model.AuditEvent{
		ID:            "audit-1",
		ActorType:     model.ActorTypeAPIKey,
		ActorID:       "operator-1",
		Action:        "platform_artifact.kernel_break_glass",
		TargetType:    "platform_artifact_release",
		TargetID:      "release-1",
		Metadata:      map[string]string{"reason": "recovery"},
		ChainID:       PlatformSafetyAuditChainID,
		ChainSequence: 1,
		CreatedAt:     now,
	}, testPlatformSafetyKeyring())
	if err != nil {
		t.Fatalf("sign first audit event: %v", err)
	}
	second, err := SignTamperEvidentAuditEvent(model.AuditEvent{
		ID:            "audit-2",
		ActorType:     model.ActorTypeAPIKey,
		ActorID:       "operator-2",
		Action:        "platform_artifact.soft_override",
		TargetType:    "platform_artifact_release",
		TargetID:      "release-2",
		Metadata:      map[string]string{"reason": "validation exception"},
		ChainID:       PlatformSafetyAuditChainID,
		ChainSequence: 2,
		PreviousHash:  first.EventHash,
		CreatedAt:     now.Add(time.Second),
	}, testPlatformSafetyKeyring())
	if err != nil {
		t.Fatalf("sign second audit event: %v", err)
	}
	chain := []model.AuditEvent{first, second}
	if err := VerifyTamperEvidentAuditChain(chain, PlatformSafetyAuditChainID, testPlatformSafetyKeyring()); err != nil {
		t.Fatalf("valid audit chain was rejected: %v", err)
	}

	tampered := append([]model.AuditEvent(nil), chain...)
	tampered[0].Metadata = map[string]string{"reason": "rewritten"}
	if err := VerifyTamperEvidentAuditChain(tampered, PlatformSafetyAuditChainID, testPlatformSafetyKeyring()); err == nil {
		t.Fatal("tampered audit event was accepted")
	}

	tamperedTenant := append([]model.AuditEvent(nil), chain...)
	tamperedTenant[0].TenantID = "forged-tenant"
	if err := VerifyTamperEvidentAuditChain(tamperedTenant, PlatformSafetyAuditChainID, testPlatformSafetyKeyring()); err == nil {
		t.Fatal("tampered audit tenant identity was accepted")
	}

	brokenLink := append([]model.AuditEvent(nil), chain...)
	brokenLink[1].PreviousHash = strings.Repeat("0", 64)
	if err := VerifyTamperEvidentAuditChain(brokenLink, PlatformSafetyAuditChainID, testPlatformSafetyKeyring()); err == nil {
		t.Fatal("broken audit chain link was accepted")
	}

	brokenSequence := append([]model.AuditEvent(nil), chain...)
	brokenSequence[1].ChainSequence = 3
	if err := VerifyTamperEvidentAuditChain(brokenSequence, PlatformSafetyAuditChainID, testPlatformSafetyKeyring()); err == nil {
		t.Fatal("non-contiguous audit sequence was accepted")
	}
}

func TestArtifactIntegrityRejectsSchemaSequenceSignatureAndRevocation(t *testing.T) {
	artifact := testSignedPlatformArtifact(t, map[string]any{"version": "v1"})
	if decision := EvaluateArtifactIntegrity(artifact, testPlatformSafetyKeyring()); !decision.Pass {
		t.Fatalf("expected signed artifact integrity to pass: %+v", decision)
	}

	invalidSchema := artifact
	invalidSchema.SchemaVersion = "2.0"
	if decision := EvaluateArtifactIntegrity(invalidSchema, testPlatformSafetyKeyring()); decision.Pass ||
		!decisionHasInvariant(decision, InvariantArtifactSchema) {
		t.Fatalf("unsupported schema must fail the schema invariant: %+v", decision)
	}

	invalidSequence := artifact
	invalidSequence.GenerationSequence = 0
	if decision := EvaluateArtifactIntegrity(invalidSequence, testPlatformSafetyKeyring()); decision.Pass ||
		!decisionHasInvariant(decision, InvariantArtifactSchema) {
		t.Fatalf("non-positive generation sequence must fail the schema invariant: %+v", decision)
	}

	tamperedSignature := artifact
	tamperedSignature.Metadata = map[string]string{"tampered": "true"}
	if decision := EvaluateArtifactIntegrity(tamperedSignature, testPlatformSafetyKeyring()); decision.Pass ||
		!decisionHasInvariant(decision, InvariantArtifactSignature) {
		t.Fatalf("tampered signed metadata must fail signature verification: %+v", decision)
	}

	revoked := bundleauth.NewKeyring(
		"platform-safety-test-key",
		"platform-safety-test",
		"",
		"",
		[]string{"platform-safety-test"},
	)
	if decision := EvaluateArtifactIntegrity(artifact, revoked); decision.Pass ||
		!decisionHasInvariant(decision, InvariantArtifactSignature) {
		t.Fatalf("revoked signer must fail signature verification: %+v", decision)
	}
}

func TestLKGSnapshotRejectsExpiredCorruptAndSignatureInvalidState(t *testing.T) {
	artifact := testSignedPlatformArtifact(t, map[string]any{"version": "v1"})
	now := time.Date(2026, 7, 10, 2, 0, 0, 0, time.UTC)
	snapshot, err := SignPlatformLKGSnapshot(model.PlatformLKGSnapshot{
		ID:                       "lkg-test",
		ArtifactID:               artifact.ID,
		ArtifactKind:             artifact.ArtifactKind,
		Scope:                    artifact.Scope,
		ScopeKey:                 artifact.ScopeKey,
		SchemaVersion:            artifact.SchemaVersion,
		Generation:               artifact.Generation,
		GenerationSequence:       artifact.GenerationSequence,
		ContentHash:              artifact.ContentHash,
		ArtifactProvenance:       artifact.Provenance,
		VerifiedByReleaseID:      "release-test",
		VerificationEvidenceHash: "sha256:" + strings.Repeat("a", 64),
		ExpiresAt:                now.Add(time.Hour),
		CreatedAt:                now,
		UpdatedAt:                now,
	}, testPlatformSafetyKeyring())
	if err != nil {
		t.Fatalf("sign LKG snapshot: %v", err)
	}
	if decision := EvaluatePlatformLKGSnapshot(snapshot, artifact, testPlatformSafetyKeyring(), now); !decision.Pass {
		t.Fatalf("expected signed fresh LKG snapshot to pass: %+v", decision)
	}

	expired := snapshot
	expired.ExpiresAt = now.Add(-time.Second)
	if decision := EvaluatePlatformLKGSnapshot(expired, artifact, testPlatformSafetyKeyring(), now); decision.Pass ||
		!decisionHasInvariant(decision, InvariantLKGNotExpired) {
		t.Fatalf("expired LKG snapshot must fail: %+v", decision)
	}

	corrupt := snapshot
	corrupt.ContentHash = "sha256:" + strings.Repeat("b", 64)
	if decision := EvaluatePlatformLKGSnapshot(corrupt, artifact, testPlatformSafetyKeyring(), now); decision.Pass ||
		!decisionHasInvariant(decision, InvariantLKGContentIntegrity) {
		t.Fatalf("corrupt LKG snapshot must fail: %+v", decision)
	}

	corruptScope := snapshot
	corruptScope.Scope.Region = "unexpected"
	if decision := EvaluatePlatformLKGSnapshot(corruptScope, artifact, testPlatformSafetyKeyring(), now); decision.Pass ||
		!decisionHasInvariant(decision, InvariantLKGContentIntegrity) {
		t.Fatalf("LKG snapshot with a mismatched structured scope must fail: %+v", decision)
	}

	invalidSignature := snapshot
	invalidSignature.SnapshotProvenance.Signature = "invalid"
	if decision := EvaluatePlatformLKGSnapshot(invalidSignature, artifact, testPlatformSafetyKeyring(), now); decision.Pass ||
		!decisionHasInvariant(decision, InvariantLKGSignature) {
		t.Fatalf("signature-invalid LKG snapshot must fail: %+v", decision)
	}
}

func TestPlatformSignaturesCanonicalizePostgresTimestampPrecision(t *testing.T) {
	rawTime := time.Date(2026, 7, 10, 2, 30, 0, 123456789, time.FixedZone("test", 8*60*60))
	artifact := testSignedPlatformArtifact(t, map[string]any{"version": "v1"})
	artifact.CreatedAt = rawTime
	artifact.UpdatedAt = rawTime
	artifact, err := SignPlatformArtifact(artifact, testPlatformSafetyKeyring())
	if err != nil {
		t.Fatalf("sign artifact with nanosecond timestamp: %v", err)
	}
	if artifact.CreatedAt.Location() != time.UTC ||
		artifact.CreatedAt.Nanosecond()%int(time.Microsecond) != 0 ||
		artifact.Provenance.SignedAt != artifact.CreatedAt {
		t.Fatalf("artifact signature time was not canonicalized to PostgreSQL precision: %+v", artifact)
	}

	snapshot, err := SignPlatformLKGSnapshot(model.PlatformLKGSnapshot{
		ID:                       "lkg-timestamp-precision",
		ArtifactID:               artifact.ID,
		ArtifactKind:             artifact.ArtifactKind,
		Scope:                    artifact.Scope,
		ScopeKey:                 artifact.ScopeKey,
		SchemaVersion:            artifact.SchemaVersion,
		Generation:               artifact.Generation,
		GenerationSequence:       artifact.GenerationSequence,
		ContentHash:              artifact.ContentHash,
		ArtifactProvenance:       artifact.Provenance,
		VerifiedByReleaseID:      "release-timestamp-precision",
		VerificationEvidenceHash: "sha256:" + strings.Repeat("c", 64),
		ExpiresAt:                rawTime.Add(time.Hour),
		CreatedAt:                rawTime,
		UpdatedAt:                rawTime,
	}, testPlatformSafetyKeyring())
	if err != nil {
		t.Fatalf("sign LKG with nanosecond timestamp: %v", err)
	}
	for name, value := range map[string]time.Time{
		"created_at": snapshot.CreatedAt,
		"updated_at": snapshot.UpdatedAt,
		"expires_at": snapshot.ExpiresAt,
		"signed_at":  snapshot.SnapshotProvenance.SignedAt,
	} {
		if value.Location() != time.UTC || value.Nanosecond()%int(time.Microsecond) != 0 {
			t.Fatalf("%s was not canonicalized to PostgreSQL precision: %s", name, value)
		}
	}
	if decision := EvaluatePlatformLKGSnapshot(
		snapshot,
		artifact,
		testPlatformSafetyKeyring(),
		snapshot.CreatedAt,
	); !decision.Pass {
		t.Fatalf("canonicalized LKG signature did not verify: %+v", decision)
	}
}

func decisionHasInvariant(decision Decision, invariant string) bool {
	for _, violation := range decision.Violations {
		if violation.Invariant == invariant {
			return true
		}
	}
	return false
}

func decisionHasBypassedInvariant(decision Decision, invariant string) bool {
	for _, violation := range decision.Bypassed {
		if violation.Invariant == invariant {
			return true
		}
	}
	return false
}

func TestLKGPromotionRequiresCurrentFenceAndCompleteEvidence(t *testing.T) {
	release := model.PlatformArtifactRelease{
		ReleaseChannel: model.PlatformArtifactReleaseChannelShadow,
		FencingToken:   7,
	}
	req := model.PlatformArtifactVerifyLKGRequest{
		FencingToken:    7,
		Reason:          "verified after canary",
		AllowInitialLKG: true,
		Evidence: model.PlatformArtifactVerificationEvidence{
			ConsumerConvergence:        true,
			LocalProbe:                 true,
			PublicSynthetic:            true,
			WatchWindow:                true,
			BaselineMonotonic:          true,
			DatabaseRollbackCompatible: true,
		},
	}
	if decision := EvaluateLKGPromotion(release, req, false); !decision.Pass {
		t.Fatalf("expected complete verification evidence to pass: %+v", decision)
	}
	release.ReleaseChannel = model.PlatformArtifactReleaseChannelFull
	if decision := EvaluateLKGPromotion(release, req, false); decision.Pass {
		t.Fatalf("initial full release must not seed verified LKG: %+v", decision)
	}
	release.ReleaseChannel = model.PlatformArtifactReleaseChannelShadow
	req.FencingToken = 6
	if decision := EvaluateLKGPromotion(release, req, false); decision.Pass {
		t.Fatalf("stale fencing token must fail: %+v", decision)
	}
	req.FencingToken = 7
	req.Evidence.PublicSynthetic = false
	if decision := EvaluateLKGPromotion(release, req, false); decision.Pass {
		t.Fatalf("missing public synthetic evidence must fail: %+v", decision)
	}
	req.Evidence.PublicSynthetic = true
	req.AllowInitialLKG = false
	if decision := EvaluateLKGPromotion(release, req, false); decision.Pass {
		t.Fatalf("initial LKG seed must require explicit approval: %+v", decision)
	}
	req.AllowInitialLKG = true
	if decision := EvaluateLKGPromotion(release, req, true); decision.Pass {
		t.Fatalf("shadow release must not replace an existing verified LKG: %+v", decision)
	}
}
