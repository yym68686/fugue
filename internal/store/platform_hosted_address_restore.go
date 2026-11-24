package store

import (
	"context"
	"encoding/json"
	"fmt"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

type hostedDeclarationHistoryReader struct {
	release  func(string) (model.PlatformArtifactRelease, error)
	artifact func(string) (model.PlatformArtifact, error)
	keys     bundleauth.Keyring
}

// Authenticate the exact previously verified full. It may now be superseded,
// and its readiness is never evidence for the current release.
func (r hostedDeclarationHistoryReader) read(plan pc.HostedDeclarationRestorePlan) (pc.DNSArtifactPayload, error) {
	ref := plan.History
	bad := fmt.Errorf("%w: shared DNS declaration history is not the pinned verified full", ErrConflict)
	if plan.Validate() != nil {
		return pc.DNSArtifactPayload{}, bad
	}
	release, err := r.release(ref.ReleaseID)
	if err != nil || release.ID != ref.ReleaseID || release.ArtifactID != ref.ReleaseSetID || release.FencingToken != ref.FencingToken ||
		release.ArtifactKind != model.PlatformArtifactKindReleaseSet || release.ScopeKey != pc.GlobalScopeKey ||
		release.ReleaseChannel != model.PlatformArtifactReleaseChannelFull || release.Status != model.PlatformArtifactReleaseStatusSuperseded ||
		release.VerificationState != model.PlatformArtifactVerificationStateVerified || release.VerifiedAt == nil || len(release.BypassedInvariants) != 0 {
		return pc.DNSArtifactPayload{}, bad
	}
	p := pc.PreviousTrafficPublicationInput{Reference: ref}
	for _, e := range []struct {
		id  string
		out *model.PlatformArtifact
	}{{ref.ReleaseSetID, &p.Parent}, {ref.RouteArtifactID, &p.Route}, {ref.DNSArtifactID, &p.DNS}, {ref.TLSArtifactID, &p.TLS}} {
		a, err := r.artifact(e.id)
		if err != nil || !platformsafety.EvaluateArtifactIntegrity(a, r.keys).Pass {
			return pc.DNSArtifactPayload{}, bad
		}
		*e.out = a
	}
	if release.Generation != p.Parent.Generation || release.VerifiedLKGGeneration != p.Parent.Generation || pc.ValidatePreviousTrafficPublication(p) != nil {
		return pc.DNSArtifactPayload{}, bad
	}
	return pc.DecodeDNSArtifact(p.DNS)
}

func (s *Store) ReadHostedDeclarationHistory(ctx context.Context, plan pc.HostedDeclarationRestorePlan) (pc.DNSArtifactPayload, error) {
	return (hostedDeclarationHistoryReader{
		release: func(id string) (model.PlatformArtifactRelease, error) {
			return s.GetPlatformArtifactReleaseContext(ctx, id)
		},
		artifact: func(id string) (model.PlatformArtifact, error) { return s.GetPlatformArtifactContext(ctx, id) },
		keys:     s.platformArtifactSigningKeyring(),
	}).read(plan)
}

// Run under the producer's scope/full/LKG transaction, after decoding the
// signed current pin. Recover only the exact declaration the compiler got.
func validateHostedDeclarationRestore(state *model.State, previous, parent, dns, priorDNS model.PlatformArtifact,
	ref pc.PreviousTrafficPublicationReference, plan pc.HostedDeclarationRestorePlan, keys bundleauth.Keyring) error {
	bad := fmt.Errorf("%w: shared DNS restore lacks current signed scope or offline authority", ErrConflict)
	reader := hostedDeclarationHistoryReader{
		release: func(id string) (model.PlatformArtifactRelease, error) {
			i := platformArtifactReleaseIndex(state.PlatformArtifactReleases, id)
			if i < 0 {
				return model.PlatformArtifactRelease{}, ErrNotFound
			}
			return state.PlatformArtifactReleases[i], nil
		},
		artifact: func(id string) (model.PlatformArtifact, error) {
			i := platformArtifactIndex(state.PlatformArtifacts, id)
			if i < 0 {
				return model.PlatformArtifact{}, ErrNotFound
			}
			return state.PlatformArtifacts[i], nil
		},
		keys: keys,
	}
	history, err := reader.read(plan)
	if err != nil {
		return err
	}
	policy, err := reader.artifact(previous.Metadata[platformproducer.DNSPolicyIDMetadata])
	if err != nil || policy.ContentHash != previous.Metadata[platformproducer.DNSPolicyDigestMetadata] ||
		policy.ArtifactKind != model.PlatformArtifactKindPolicySnapshot || policy.ScopeKey != pc.GlobalScopeKey ||
		policy.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(policy, keys).Pass {
		return bad
	}
	raw, err := json.Marshal(policy.Content["offline_edges"])
	var holds []platformproducer.OfflineEdge
	if err != nil || json.Unmarshal(raw, &holds) != nil || platformproducer.ValidateOfflineEdges(holds) != nil {
		return bad
	}
	beforeHolds, e1 := platformproducer.PublicationOfflineHolds(previous, holds)
	afterHolds, e2 := platformproducer.PublicationOfflineHolds(parent, holds)
	before, e3 := pc.DecodeDNSArtifact(priorDNS)
	after, e4 := pc.DecodeDNSArtifact(dns)
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil {
		return bad
	}
	physical := func(in []platformproducer.OfflineEdge) []pc.HostedAddressHold {
		out := make([]pc.HostedAddressHold, 0, len(in))
		for _, h := range in {
			out = append(out, pc.HostedAddressHold{EdgeID: h.EdgeID, EdgeGroupID: h.EdgeGroupID, Address: h.PublicIPv4})
		}
		return out
	}
	route, e1 := reader.artifact(ref.RouteArtifactID)
	tls, e2 := reader.artifact(ref.TLSArtifactID)
	nroute, ntls := model.PlatformArtifact{}, model.PlatformArtifact{}
	members, ok := parent.Content["artifact_ids"].([]any)
	if e1 != nil || e2 != nil || !ok || len(members) != 3 {
		return bad
	}
	for _, rawID := range members {
		id, ok := rawID.(string)
		a, err := reader.artifact(id)
		if !ok || err != nil || a.ScopeKey != pc.GlobalScopeKey || a.Status != model.PlatformArtifactStatusValidated ||
			!platformsafety.EvaluateArtifactIntegrity(a, keys).Pass {
			return bad
		}
		switch a.ArtifactKind {
		case model.PlatformArtifactKindEdgeRouteBundle:
			nroute = a
		case model.PlatformArtifactKindCaddyRouteConfig:
			ntls = a
		}
	}
	if err = pc.ValidateHostedDeclarationRestoreDelta(before, history, after, plan, physical(beforeHolds), physical(afterHolds), route, tls, nroute, ntls); err != nil {
		return fmt.Errorf("%w: %v", ErrConflict, err)
	}
	return nil
}
