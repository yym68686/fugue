package store

import (
	"fmt"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

func (s *Store) VerifyProducedTrafficLKG(releaseID, policyReleaseID, baselineArtifactID string, request model.PlatformArtifactVerifyLKGRequest, principal model.Principal) (model.PlatformArtifact, model.PlatformArtifactRelease, model.PlatformReleaseMessage, *model.PlatformLKGSnapshot, error) {
	if request.AllowInitialLKG {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, ErrInvalidInput
	}
	return s.verifyPlatformArtifactReleaseLKG(releaseID, request, principal, &platformProducerReleaseGuard{PolicyReleaseID: policyReleaseID, PreviousReleaseID: releaseID, PreviousFullReleaseID: releaseID, BaselineArtifactID: baselineArtifactID, Phase: "verify"})
}

func (s *Store) RollbackProducedTraffic(id, failedReleaseID, policyReleaseID, previousFullReleaseID, baselineArtifactID string, req model.PlatformArtifactRollbackRequest, principal model.Principal) (model.PlatformArtifact, model.PlatformArtifactRelease, model.PlatformReleaseMessage, *model.PlatformLKGSnapshot, error) {
	if req.ReleaseChannel != model.PlatformArtifactReleaseChannelFull || req.SoftOverride || req.ForcePublish || req.KernelBreakGlass != nil {
		return model.PlatformArtifact{}, model.PlatformArtifactRelease{}, model.PlatformReleaseMessage{}, nil, ErrInvalidInput
	}
	return s.rollbackPlatformArtifact(id, req, principal, &platformProducerReleaseGuard{PolicyReleaseID: policyReleaseID, PreviousReleaseID: failedReleaseID, PreviousFullReleaseID: previousFullReleaseID, BaselineArtifactID: baselineArtifactID, FailedReleaseID: failedReleaseID, Phase: "rollback"})
}

func producerActivePublication(state *model.State, scope, channel string) (model.PlatformArtifactRelease, bool) {
	lane, ok := platformReleaseLaneByKey(state.PlatformReleaseLanes, platformsafety.ReleaseLaneKey(model.PlatformArtifactKindReleaseSet, scope, channel))
	if !ok || lane.Frozen || lane.ActiveReleaseID == "" {
		return model.PlatformArtifactRelease{}, false
	}
	index := platformArtifactReleaseIndex(state.PlatformArtifactReleases, lane.ActiveReleaseID)
	if index < 0 {
		return model.PlatformArtifactRelease{}, false
	}
	r := state.PlatformArtifactReleases[index]
	return r, r.Status == model.PlatformArtifactReleaseStatusActive && r.LaneKey == lane.LaneKey && r.FencingToken == lane.FencingToken && r.FencingToken > 0
}

func producerOwnsPublication(r model.PlatformArtifactRelease) bool {
	return r.ReleasedByType == model.ActorTypeBootstrap && r.ReleasedByID == platformproducer.Actor
}

func validateProducerServingPhase(state *model.State, parent model.PlatformArtifact, req model.PlatformArtifactReleaseRequest, policy platformproducer.Policy, keys bundleauth.Keyring, guard *platformProducerReleaseGuard) error {
	fail := func(reason string) error {
		return fmt.Errorf("%w: automatic traffic publication: %s", ErrConflict, reason)
	}
	if policy.Mode != "serving" || policy.Serving == nil || guard.BaselineArtifactID == "" {
		return fail("serving authority unavailable")
	}
	now := time.Now().UTC()
	lkg := verifiedPlatformLKGSnapshotFromState(state, parent.ArtifactKind, parent.ScopeKey, now, keys)
	if lkg == nil || lkg.ArtifactID != guard.BaselineArtifactID {
		return fail("verified baseline changed or missing")
	}
	i := platformArtifactReleaseIndex(state.PlatformArtifactReleases, lkg.VerifiedByReleaseID)
	if i < 0 {
		return fail("baseline verification release missing")
	}
	baseline := state.PlatformArtifactReleases[i]
	if baseline.ArtifactID != lkg.ArtifactID || baseline.ReleaseChannel != model.PlatformArtifactReleaseChannelFull || baseline.VerificationState != model.PlatformArtifactVerificationStateVerified || baseline.VerifiedLKGGeneration != lkg.Generation {
		return fail("initial full verified baseline required")
	}
	full, ok := producerActivePublication(state, parent.ScopeKey, model.PlatformArtifactReleaseChannelFull)
	if !ok || full.ID != guard.PreviousFullReleaseID {
		return fail("full authority changed")
	}
	gray, hasGray := producerActivePublication(state, parent.ScopeKey, model.PlatformArtifactReleaseChannelGray)
	shadow, hasShadow := producerActivePublication(state, parent.ScopeKey, model.PlatformArtifactReleaseChannelShadow)
	owned := func(r model.PlatformArtifactRelease) bool {
		return producerOwnsPublication(r) && r.ArtifactID == parent.ID && r.Generation == parent.Generation && r.VerificationState != model.PlatformArtifactVerificationStateFailed
	}
	settings := policy.Serving
	switch guard.Phase {
	case model.PlatformArtifactReleaseChannelGray:
		if req.ReleaseChannel != model.PlatformArtifactReleaseChannelGray || req.CanaryRuleRef != settings.CanaryRuleRef || full.ArtifactID != lkg.ArtifactID || !hasShadow || !owned(shadow) {
			return fail("gray must extend the verified full baseline with its current produced shadow")
		}
		if hasGray && gray.ReleasedAt.After(full.ReleasedAt) && gray.VerificationState != model.PlatformArtifactVerificationStateFailed && gray.ArtifactID != parent.ID {
			return fail("another gray publication is still active")
		}
		if _, err := platformconfig.ResolveTrafficCanary(parent, settings.CanaryRuleRef); err != nil {
			return fail("declared cohort unavailable")
		}
	case model.PlatformArtifactReleaseChannelFull:
		if hasGray && !now.Before(gray.ReleasedAt.Add(time.Duration(settings.RolloutTimeoutSeconds)*time.Second)) {
			return fail("gray publication timed out")
		}
		if req.ReleaseChannel != model.PlatformArtifactReleaseChannelFull || req.CanaryRuleRef != "" || !hasGray || !owned(gray) || !hasShadow || !owned(shadow) || gray.CanaryRuleRef != settings.CanaryRuleRef || !gray.ReleasedAt.After(full.ReleasedAt) || gray.PinnedRollbackGeneration != lkg.Generation || now.Before(gray.ReleasedAt.Add(time.Duration(settings.GrayMinSeconds)*time.Second)) {
			return fail("current gray publication and minimum age required")
		}
	case "verify":
		if !now.Before(full.ReleasedAt.Add(time.Duration(settings.RolloutTimeoutSeconds) * time.Second)) {
			return fail("full publication timed out")
		}
		if req.ReleaseChannel != model.PlatformArtifactReleaseChannelFull || !owned(full) || full.PinnedRollbackGeneration != lkg.Generation || now.Before(full.ReleasedAt.Add(time.Duration(settings.FullMinSeconds)*time.Second)) {
			return fail("current full publication and minimum age required")
		}
	case "rollback":
		i := platformArtifactReleaseIndex(state.PlatformArtifactReleases, guard.FailedReleaseID)
		if i < 0 {
			return fail("failed publication missing")
		}
		failed := state.PlatformArtifactReleases[i]
		latest := full
		if hasGray && gray.ReleasedAt.After(full.ReleasedAt) {
			latest = gray
		}
		if parent.ID == lkg.ArtifactID || failed.VerificationState == model.PlatformArtifactVerificationStateVerified || failed.ID != latest.ID || !owned(failed) || failed.PinnedRollbackGeneration != lkg.Generation || req.ReleaseChannel != failed.ReleaseChannel || (parent.Metadata[platformproducer.PolicyReleaseMetadata] == guard.PolicyReleaseID && now.Before(failed.ReleasedAt.Add(time.Duration(settings.RolloutTimeoutSeconds)*time.Second))) {
			return fail("only the timed-out current serving candidate may recover")
		}
	default:
		return fail("unknown automatic action")
	}
	return nil
}
