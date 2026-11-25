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

// A stored shadow is not fresh serving authority. For these artifacts only,
// a manual path edit goes through recapture of the user's saved intent; a
// direct artifact rollback uses the producer's existing exact LKG recovery.
func rejectUnboundHostedPublication(parent model.PlatformArtifact, channel string) error {
	ref, _, err := platformconfig.HostedAddressAuthority(parent)
	_, restoreErr := platformconfig.HostedDeclarationRestoreAuthority(parent)
	if err != nil || restoreErr != nil || ref != nil && NormalizePlatformReleaseChannel(channel) != model.PlatformArtifactReleaseChannelShadow {
		return fmt.Errorf("%w: hosted address publication requires its fenced producer", ErrConflict)
	}
	return nil
}

// Run with the producer scope, serving lane and LKG held by the same
// transaction. No lookup by hostname, latest generation or mutable label.
func validateHostedAddressBaseline(state *model.State, parent, dns model.PlatformArtifact, baseline model.PlatformArtifactRelease, lkg model.PlatformLKGSnapshot, now time.Time, keys bundleauth.Keyring) error {
	reject := fmt.Errorf("%w: hosted address publication lost its verified predecessor or member identity", ErrConflict)
	ref, digest, err := platformconfig.HostedAddressAuthority(dns)
	parentRef, parentDigest, parentErr := platformconfig.HostedAddressAuthority(parent)
	restore, restoreErr := platformconfig.HostedDeclarationRestoreAuthority(dns)
	parentRestore, parentRestoreErr := platformconfig.HostedDeclarationRestoreAuthority(parent)
	handoff := parent.Metadata[platformconfig.HostedFleetHandoffMetadata]
	if err != nil || parentErr != nil || digest != parentDigest || restoreErr != nil || parentRestoreErr != nil ||
		(restore == nil) != (parentRestore == nil) || restore != nil && *restore != *parentRestore ||
		handoff != dns.Metadata[platformconfig.HostedFleetHandoffMetadata] {
		return reject
	}
	if ref == nil {
		if handoff != "" {
			return reject
		}
		return nil
	}
	if parentRef == nil || *parentRef != *ref || !lkg.ExpiresAt.After(now) ||
		ref.ReleaseSetID != lkg.ArtifactID || ref.ReleaseSetDigest != lkg.ContentHash || ref.ReleaseID != baseline.ID ||
		baseline.ArtifactID != ref.ReleaseSetID || ref.FencingToken != baseline.FencingToken || lkg.VerifiedByReleaseID != ref.ReleaseID ||
		dns.ScopeKey != parent.ScopeKey || parent.ID == ref.ReleaseSetID {
		return reject
	}
	member := func(id, kind, digest string) (model.PlatformArtifact, bool) {
		i := platformArtifactIndex(state.PlatformArtifacts, id)
		if i < 0 {
			return model.PlatformArtifact{}, false
		}
		a := state.PlatformArtifacts[i]
		return a, a.ArtifactKind == kind && a.ScopeKey == parent.ScopeKey && a.Status == model.PlatformArtifactStatusValidated &&
			a.ContentHash == digest && platformsafety.EvaluateArtifactIntegrity(a, keys).Pass
	}
	src, valid := member(ref.ReleaseSetID, model.PlatformArtifactKindReleaseSet, ref.ReleaseSetDigest)
	if !valid {
		return reject
	}
	if handoff == "" {
		for _, k := range []string{platformproducer.PolicyReleaseMetadata, platformproducer.DNSPolicyIDMetadata, platformproducer.DNSPolicyDigestMetadata, platformproducer.StaticIntentIDMetadata, platformproducer.StaticIntentDigestMetadata} {
			if parent.Metadata[k] == "" || parent.Metadata[k] != src.Metadata[k] {
				return reject
			}
		}
	} else {
		inherited, sourceDigest, err := platformconfig.HostedAddressAuthority(src)
		if err != nil || inherited == nil || handoff != sourceDigest || restore != nil || parent.Metadata[HostedSuccessorMetadata] != "" ||
			parent.CreatedByType != model.ActorTypeBootstrap || parent.CreatedByID != platformproducer.Actor ||
			dns.CreatedByType != model.ActorTypeBootstrap || dns.CreatedByID != platformproducer.Actor ||
			validateHostedFleetHandoff(stateDNSRouteSourceReader(state), parent.Metadata[platformproducer.PolicyReleaseMetadata], src, baseline, lkg, keys) != nil {
			return reject
		}
	}
	kinds := map[string]string{ref.RouteArtifactID: model.PlatformArtifactKindEdgeRouteBundle, ref.DNSArtifactID: model.PlatformArtifactKindDNSAnswerBundle, ref.TLSArtifactID: model.PlatformArtifactKindCaddyRouteConfig}
	hashes := map[string]string{ref.RouteArtifactID: ref.RouteArtifactDigest, ref.DNSArtifactID: ref.DNSArtifactDigest, ref.TLSArtifactID: ref.TLSArtifactDigest}
	ids, ok := src.Content["artifact_ids"].([]any)
	if !ok || len(ids) != len(kinds) {
		return reject
	}
	for _, raw := range ids {
		id, ok := raw.(string)
		if !ok || kinds[id] == "" {
			return reject
		}
		sourceChild, ok := member(id, kinds[id], hashes[id])
		if !ok {
			return reject
		}
		if id == ref.DNSArtifactID {
			if handoff != "" {
				_, inheritedDigest, err := platformconfig.HostedAddressAuthority(sourceChild)
				if err != nil || inheritedDigest != handoff || platformconfig.ValidateHostedFleetHandoffTransition(sourceChild, dns) != nil {
					return fmt.Errorf("%w: fleet default handoff includes a custody or hosted path change", ErrConflict)
				}
			} else if restore != nil {
				if err := validateHostedDeclarationRestore(state, src, parent, dns, sourceChild, *ref, *restore, keys); err != nil {
					return err
				}
			} else if platformconfig.ValidateScopedHostedPublicationTransition(sourceChild, dns) != nil {
				return fmt.Errorf("%w: hosted publication includes unrelated DNS changes", ErrConflict)
			}
		}
		delete(kinds, id)
	}
	return nil
}
