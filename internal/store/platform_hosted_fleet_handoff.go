package store

import (
	"context"
	"database/sql"
	"strings"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

func pgLoadHostedFleetHandoff(ctx context.Context, tx *sql.Tx, state *storeProjection, baselineID string) error {
	i := platformArtifactIndex(state.PlatformArtifacts, baselineID)
	if i < 0 {
		return ErrConflict
	}
	prior := state.PlatformArtifacts[i]
	previous, err := pgGetPlatformArtifactRelease(ctx, tx, prior.Metadata[platformproducer.PolicyReleaseMetadata], false)
	if err != nil {
		return err
	}
	state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, previous)
	for _, id := range []string{previous.ArtifactID, prior.Metadata[platformproducer.DNSPolicyIDMetadata]} {
		old, err := pgGetPlatformArtifactForUpdate(ctx, tx, id, true)
		if err != nil {
			return err
		}
		state.PlatformArtifacts = append(state.PlatformArtifacts, old)
	}
	return nil
}

// The activation's existing canonical idempotency key binds the operation,
// predecessor policy, exact serving full, and verification evidence hash.
// Reconstruct it instead of treating any two signed policies as a handoff.
// Both callers verify the active producer and positive LKG in their own read.
func validateHostedFleetHandoff(read dnsRouteSourceReader, activeID string, prior model.PlatformArtifact, full model.PlatformArtifactRelease, lkg model.PlatformLKGSnapshot, keys bundleauth.Keyring) error {
	ref, _, err := platformconfig.HostedAddressAuthority(prior)
	if err != nil || ref == nil || !producerOwnsPublication(full) || prior.ScopeKey != platformconfig.GlobalScopeKey ||
		full.ArtifactID != prior.ID || lkg.ArtifactID != prior.ID || lkg.ContentHash != prior.ContentHash || lkg.VerifiedByReleaseID != full.ID {
		return ErrConflict
	}
	active, err := read.release(activeID)
	if err != nil || !strings.HasPrefix(active.IdempotencyKey, "producer-reconfiguration/sha256:") {
		return ErrConflict
	}
	previous, err := read.release(prior.Metadata[platformproducer.PolicyReleaseMetadata])
	if err != nil || previous.ID == active.ID || active.FencingToken != previous.FencingToken+1 || previous.FencingToken <= 0 ||
		previous.Status != model.PlatformArtifactReleaseStatusSuperseded || previous.LaneKey != active.LaneKey ||
		previous.ArtifactKind != model.PlatformArtifactKindPolicySnapshot || previous.ScopeKey != platformproducer.Scope ||
		previous.ReleaseChannel != model.PlatformArtifactReleaseChannelShadow || full.ReleasedAt.After(active.ReleasedAt) {
		return ErrConflict
	}
	old, err := read.artifact(previous.ArtifactID)
	if err != nil || old.ScopeKey != platformproducer.Scope || old.ArtifactKind != model.PlatformArtifactKindPolicySnapshot ||
		old.Generation != previous.Generation || old.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(old, keys).Pass {
		return ErrConflict
	}
	next, err := read.artifact(active.ArtifactID)
	if err != nil {
		return ErrConflict
	}
	b, err := platformproducer.Decode(old)
	if err != nil || b.InputSource != "business-static-intent" || !hostedPolicyBindingsMatch(prior, b, previous.ID) {
		return ErrConflict
	}
	a, err := platformproducer.Decode(next)
	if err != nil {
		return ErrConflict
	}
	pin := func(artifact model.PlatformArtifact, release model.PlatformArtifactRelease) model.PlatformPublicationPrecondition {
		return model.PlatformPublicationPrecondition{ArtifactID: artifact.ID, ContentHash: artifact.ContentHash, ReleaseID: release.ID, FencingToken: release.FencingToken}
	}
	key, err := producerReconfigurationKey(next, model.PlatformProducerReconfiguration{Operation: "dynamic_quality", PreviousPolicy: pin(old, previous), ServingFull: pin(prior, full), VerificationEvidenceHash: lkg.VerificationEvidenceHash})
	if err != nil || key != active.IdempotencyKey {
		return ErrConflict
	}
	inputs := &storeProjection{}
	for _, id := range []string{b.StaticIntentArtifactID, b.DNSPolicyArtifactID, a.DNSPolicyArtifactID} {
		input, err := read.artifact(id)
		if err != nil {
			return ErrConflict
		}
		inputs.PlatformArtifacts = append(inputs.PlatformArtifacts, input)
	}
	return validateProducerDNSInputChange(inputs, b, a, keys, platformproducer.ValidateFleetDefaultOnlyUpdate)
}

func hostedPolicyBindingsMatch(a model.PlatformArtifact, p platformproducer.Policy, policyRelease string) bool {
	return policyRelease != "" && a.Metadata[platformproducer.PolicyReleaseMetadata] == policyRelease &&
		p.DNSPolicyArtifactID != "" && a.Metadata[platformproducer.DNSPolicyIDMetadata] == p.DNSPolicyArtifactID &&
		p.DNSPolicyDigest != "" && a.Metadata[platformproducer.DNSPolicyDigestMetadata] == p.DNSPolicyDigest &&
		p.StaticIntentArtifactID != "" && a.Metadata[platformproducer.StaticIntentIDMetadata] == p.StaticIntentArtifactID &&
		p.StaticIntentDigest != "" && a.Metadata[platformproducer.StaticIntentDigestMetadata] == p.StaticIntentDigest
}
