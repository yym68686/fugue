package store

import (
	"context"
	"database/sql"
	"reflect"
	"sort"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

func validateProducerMembershipInputs(state *model.State, previous, next platformproducer.Policy, keys bundleauth.Keyring) error {
	inputs := make([]model.PlatformArtifact, 0, 4)
	for _, p := range []platformproducer.Policy{previous, next} {
		for _, ref := range []struct{ id, digest, kind string }{
			{p.StaticIntentArtifactID, p.StaticIntentDigest, model.PlatformArtifactKindPlatformIntent},
			{p.DNSPolicyArtifactID, p.DNSPolicyDigest, model.PlatformArtifactKindPolicySnapshot},
		} {
			index := platformArtifactIndex(state.PlatformArtifacts, ref.id)
			if index < 0 {
				return ErrConflict
			}
			a := state.PlatformArtifacts[index]
			if a.ID != ref.id || a.ContentHash != ref.digest || a.ArtifactKind != ref.kind || a.ScopeKey != p.TargetScope || a.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(a, keys).Pass {
				return ErrConflict
			}
			inputs = append(inputs, a)
		}
	}
	if inputs[2].GenerationSequence <= inputs[0].GenerationSequence || inputs[3].GenerationSequence <= inputs[1].GenerationSequence || platformproducer.ValidateMembershipExpansion(previous, next, inputs[0], inputs[2], inputs[1], inputs[3]) != nil {
		return ErrConflict
	}
	return nil
}

func validateMembershipBaseline(state *model.State, policy platformproducer.Policy, full model.PlatformArtifact, release model.PlatformArtifactRelease) error {
	index := platformArtifactIndex(state.PlatformArtifacts, policy.StaticIntentArtifactID)
	if index < 0 {
		return ErrConflict
	}
	static, err := platformproducer.DecodeStaticIntent(state.PlatformArtifacts[index])
	if err != nil {
		return ErrConflict
	}
	want, err := platformconfig.TrafficConsumerTopologyFromIntent(platformconfig.PlatformIntent{PublicationRole: static.PublicationRole, Scope: static.Scope, AuthorityCellID: static.AuthorityCellID, EdgeTopology: static.EdgeTopology, DNSConsumers: static.Consumers})
	actual, actualErr := platformconfig.TrafficConsumersFromRelease(full)
	if err != nil || actualErr != nil || !reflect.DeepEqual(want, actual) {
		return ErrConflict
	}
	key := platformsafety.ReleaseLaneKey(model.PlatformArtifactKindReleaseSet, policy.TargetScope, "gray")
	if lane, found := platformReleaseLaneByKey(state.PlatformReleaseLanes, key); found {
		if lane.Frozen {
			return ErrConflict
		}
		if lane.ActiveReleaseID != "" {
			i := platformArtifactReleaseIndex(state.PlatformArtifactReleases, lane.ActiveReleaseID)
			if i < 0 {
				return ErrConflict
			}
			gray := state.PlatformArtifactReleases[i]
			if gray.Status != model.PlatformArtifactReleaseStatusActive || gray.FencingToken != lane.FencingToken || gray.ReleasedAt.After(release.ReleasedAt) && gray.VerificationState != model.PlatformArtifactVerificationStateFailed {
				return ErrConflict
			}
		}
	}
	return nil
}

func pgLoadProducerMembershipInputs(ctx context.Context, tx *sql.Tx, state *model.State, previous, next platformproducer.Policy) error {
	ids := []string{previous.StaticIntentArtifactID, previous.DNSPolicyArtifactID, next.StaticIntentArtifactID, next.DNSPolicyArtifactID}
	sort.Strings(ids)
	for _, id := range ids {
		if platformArtifactIndex(state.PlatformArtifacts, id) >= 0 {
			continue
		}
		a, err := pgGetPlatformArtifactForUpdate(ctx, tx, id, true)
		if err != nil {
			return err
		}
		state.PlatformArtifacts = append(state.PlatformArtifacts, a)
	}
	return nil
}
