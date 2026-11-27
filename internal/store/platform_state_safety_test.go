package store

import (
	"errors"
	"fmt"
	"testing"
	"testing/quick"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformsafety"
)

func TestShadowAndGrayLedgerEntriesNeverClaimProductionServingProperty(t *testing.T) {
	property := func(sequence uint32) bool {
		generation := fmt.Sprintf("generation-%d", sequence)
		artifact := model.PlatformArtifact{
			ID:           "artifact-test",
			ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle,
			ScopeKey:     "global",
			Generation:   generation,
		}
		lane := model.PlatformReleaseLane{
			LaneKey:        "test",
			FencingToken:   1,
			Version:        1,
			ReleaseChannel: model.PlatformArtifactReleaseChannelShadow,
		}
		for _, channel := range []string{
			model.PlatformArtifactReleaseChannelShadow,
			model.PlatformArtifactReleaseChannelGray,
		} {
			entry := buildPlatformArtifactReleaseLedgerEntry(
				artifact,
				channel,
				"stable",
				"edge=test",
				"property test",
				"",
				model.PlatformReleaseMessageTypeRelease,
				model.PlatformArtifactOverrideModeNone,
				nil,
				nil,
				testPlatformPrincipal(),
				lane,
				time.Unix(int64(sequence)+1, 0).UTC(),
			)
			if entry.Release.ServingUnverifiedGeneration != "" {
				return false
			}
		}
		return true
	}
	if err := quick.Check(property, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatal(err)
	}
}

func TestGrayReleaseCannotEscapeBoundedCanaryScope(t *testing.T) {
	s := newStoreTestDatabase(t, "")
	configureTestPlatformArtifactSigning(s)
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	artifact := createValidatedPlatformArtifact(
		t,
		s,
		model.PlatformArtifactKindEdgeRouteBundle,
		"gray-scope-test",
		map[string]any{"routes": []any{"candidate"}},
	)
	for _, canaryRef := range []string{"", "*", "global", "edge=*", "edge=a,b"} {
		if _, _, _, _, err := s.ReleasePlatformArtifact(artifact.ID, model.PlatformArtifactReleaseRequest{
			ReleaseChannel: model.PlatformArtifactReleaseChannelGray,
			CanaryRuleRef:  canaryRef,
		}, testPlatformPrincipal()); err == nil {
			t.Fatalf("unbounded canary scope %q was accepted", canaryRef)
		}
	}
	if _, release, _, _, err := s.ReleasePlatformArtifact(artifact.ID, model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelGray,
		CanaryRuleRef:  "edge=test",
	}, testPlatformPrincipal()); err != nil {
		t.Fatalf("bounded canary scope was rejected: %v", err)
	} else if release.CanaryRuleRef != "edge=test" || release.ServingUnverifiedGeneration != "" {
		t.Fatalf("unexpected bounded canary release: %+v", release)
	}
}

func TestPlatformOverrideAuditFailureAbortsReleaseTransaction(t *testing.T) {
	s := newStoreTestDatabase(t, "")
	configureTestPlatformArtifactSigning(s)
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	artifact, err := s.CreatePlatformArtifact(model.PlatformArtifact{
		ArtifactKind: model.PlatformArtifactKindTrafficSafetyPolicy,
		Scope:        model.PlatformArtifactScope{ScopeType: "global"},
		Generation:   "audit-signing-failure",
		Content:      map[string]any{"min_healthy_edges": 1},
	})
	if err != nil {
		t.Fatalf("create artifact: %v", err)
	}

	s.ConfigurePlatformArtifactSigning(bundleauth.NewKeyring(
		"",
		"",
		"platform-artifact-test-signing-key",
		"platform-artifact-test",
		nil,
	))
	if _, _, _, _, err := s.ReleasePlatformArtifact(artifact.ID, model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelShadow,
		SoftOverride:   true,
		Reason:         "audit signing must remain transactional",
	}, testPlatformSoftOverridePrincipal()); !errors.Is(err, platformsafety.ErrPlatformSigningKeyUnavailable) {
		t.Fatalf("expected audit signing failure to abort release, got %v", err)
	}
	if _, _, found, err := s.GetActivePlatformArtifact(
		artifact.ArtifactKind,
		artifact.ScopeKey,
		model.PlatformArtifactReleaseChannelShadow,
	); err != nil {
		t.Fatalf("get active artifact after failed release: %v", err)
	} else if found {
		t.Fatal("release was persisted despite audit signing failure")
	}
	events, err := s.ListAuditEvents("", true, 0)
	if err != nil {
		t.Fatalf("list audit events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("audit transaction partially committed: %+v", events)
	}
}

func TestPlatformOverrideAuthorizationIsEnforcedAtStoreBoundary(t *testing.T) {
	s := newStoreTestDatabase(t, "")
	configureTestPlatformArtifactSigning(s)
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	artifact, err := s.CreatePlatformArtifact(model.PlatformArtifact{
		ArtifactKind: model.PlatformArtifactKindTrafficSafetyPolicy,
		Scope:        model.PlatformArtifactScope{ScopeType: "global"},
		Generation:   "store-authorization-boundary",
		Content:      map[string]any{"min_healthy_edges": 1},
	})
	if err != nil {
		t.Fatalf("create artifact: %v", err)
	}
	if _, _, _, _, err := s.ReleasePlatformArtifact(artifact.ID, model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelShadow,
		SoftOverride:   true,
		Reason:         "missing override permission",
	}, testPlatformPrincipal()); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("store accepted soft override without authorization: %v", err)
	}
	if _, _, _, _, err := s.ReleasePlatformArtifact(artifact.ID, model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelFull,
		KernelBreakGlass: &model.PlatformKernelBreakGlassRequest{
			ExpiresAt:          time.Now().UTC().Add(5 * time.Minute),
			Confirmation:       platformsafety.KernelBreakGlassConfirmation,
			TargetConfirmation: artifact.ID,
		},
		Reason: "missing explicit kernel permission",
	}, testPlatformSoftOverridePrincipal()); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("store accepted kernel break-glass without explicit authorization: %v", err)
	}
}

func TestOrdinaryAuditAppendCannotInjectPlatformSafetyChain(t *testing.T) {
	s := newStoreTestDatabase(t, "")
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	if err := s.AppendAuditEvent(model.AuditEvent{
		Action:        "forged",
		TargetType:    "platform_artifact_release",
		ChainID:       platformsafety.PlatformSafetyAuditChainID,
		ChainSequence: 1,
	}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("ordinary audit append accepted reserved chain fields: %v", err)
	}
	events, err := s.ListAuditEvents("", true, 0)
	if err != nil {
		t.Fatalf("list audit events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("forged chain event was persisted: %+v", events)
	}
}

func testPlatformSoftOverridePrincipal() model.Principal {
	return model.Principal{
		ActorType: model.ActorTypeAPIKey,
		ActorID:   "soft-override-test",
		Scopes:    map[string]struct{}{"platform.admin": {}},
	}
}

func testPlatformKernelBreakGlassPrincipal() model.Principal {
	return model.Principal{
		ActorType: model.ActorTypeAPIKey,
		ActorID:   "kernel-break-glass-test",
		Scopes: map[string]struct{}{
			"platform.admin":              {},
			"artifact.kernel_break_glass": {},
		},
	}
}

func stringSliceContains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
