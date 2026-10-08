package store

import (
	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

func validateProducerPhysicalDNSInputs(state *model.State, previous, next platformproducer.Policy, keys bundleauth.Keyring) error {
	inputs := make([]model.PlatformArtifact, 0, 3)
	for _, reference := range []struct{ id, digest, kind string }{
		{previous.StaticIntentArtifactID, previous.StaticIntentDigest, model.PlatformArtifactKindPlatformIntent},
		{previous.DNSPolicyArtifactID, previous.DNSPolicyDigest, model.PlatformArtifactKindPolicySnapshot},
		{next.DNSPolicyArtifactID, next.DNSPolicyDigest, model.PlatformArtifactKindPolicySnapshot},
	} {
		index := platformArtifactIndex(state.PlatformArtifacts, reference.id)
		if index < 0 {
			return ErrConflict
		}
		artifact := state.PlatformArtifacts[index]
		if artifact.ID != reference.id || artifact.ContentHash != reference.digest || artifact.ArtifactKind != reference.kind || artifact.ScopeKey != "global" || artifact.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(artifact, keys).Pass {
			return ErrConflict
		}
		inputs = append(inputs, artifact)
	}
	if platformproducer.ValidatePhysicalDNSOptIn(previous, next, inputs[0], inputs[1], inputs[2]) != nil {
		return ErrConflict
	}
	return nil
}

func validatePhysicalDNSGrayBaseline(state *model.State, policy platformproducer.Policy, full model.PlatformArtifactRelease) error {
	laneKey := platformsafety.ReleaseLaneKey(model.PlatformArtifactKindReleaseSet, policy.TargetScope, "gray")
	lane, found := platformReleaseLaneByKey(state.PlatformReleaseLanes, laneKey)
	if !found {
		return nil
	}
	if lane.Frozen {
		return ErrConflict
	}
	if lane.ActiveReleaseID == "" {
		return nil
	}
	index := platformArtifactReleaseIndex(state.PlatformArtifactReleases, lane.ActiveReleaseID)
	if index < 0 {
		return ErrConflict
	}
	gray := state.PlatformArtifactReleases[index]
	if gray.Status != model.PlatformArtifactReleaseStatusActive || gray.FencingToken != lane.FencingToken || gray.ReleasedAt.After(full.ReleasedAt) && gray.VerificationState != model.PlatformArtifactVerificationStateFailed {
		return ErrConflict
	}
	return nil
}
