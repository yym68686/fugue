package platformsafety

import (
	"testing"

	"fugue/internal/model"
)

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
	artifact := model.PlatformArtifact{
		Status:  model.PlatformArtifactStatusValidated,
		Content: map[string]any{"version": "v1"},
	}
	artifact.ContentHash = artifactContentHash(artifact.Content)
	if decision := EvaluateArtifactRelease(artifact, model.PlatformArtifactReleaseChannelFull, "gen_stable"); !decision.Pass {
		t.Fatalf("expected valid full release, got %+v", decision)
	}
	if decision := EvaluateArtifactRelease(artifact, model.PlatformArtifactReleaseChannelFull, ""); decision.Pass {
		t.Fatalf("full release without rollback target must fail: %+v", decision)
	}
	if decision := EvaluateArtifactRelease(artifact, model.PlatformArtifactReleaseChannelShadow, ""); !decision.Pass {
		t.Fatalf("shadow release does not require a production rollback target: %+v", decision)
	}
	artifact.Content["version"] = "tampered"
	if decision := EvaluateArtifactRelease(artifact, model.PlatformArtifactReleaseChannelShadow, ""); decision.Pass {
		t.Fatalf("tampered content must not pass with a stale content hash: %+v", decision)
	}
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
