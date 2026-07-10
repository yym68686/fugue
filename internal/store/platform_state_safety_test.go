package store

import (
	"fmt"
	"path/filepath"
	"testing"
	"testing/quick"
	"time"

	"fugue/internal/model"
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
	s := New(filepath.Join(t.TempDir(), "store.json"))
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
