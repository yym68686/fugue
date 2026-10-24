package store

import (
	"context"
	"fmt"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/cellpublication"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

// Called while the publication transaction holds its authority locks. A DNS
// publication cannot endorse a stale Cell fence read before that transaction.
func validateCellDNSReferencesInState(state *model.State, child model.PlatformArtifact, keys bundleauth.Keyring) error {
	_, err := cellpublication.VerifyDNSArtifact(child, keys)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrConflict, err)
	}
	if err := validateDNSRouteProducerBindings(state, child, keys); err != nil {
		return err
	}
	fail := func() error {
		return platformReleaseRejected("routing_source_selection", fmt.Errorf("%w: referenced Cell publication is no longer selected", ErrConflict))
	}
	dependencies, err := platformconfig.DNSRouteDependencies(child)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrConflict, err)
	}
	for _, dependency := range dependencies {
		var selected *model.PlatformArtifactRelease
		for _, r := range state.PlatformArtifactReleases {
			if r.ScopeKey != dependency.Parent.ScopeKey || r.ArtifactKind != model.PlatformArtifactKindReleaseSet || r.Status != model.PlatformArtifactReleaseStatusActive || (r.ReleaseChannel != "gray" && r.ReleaseChannel != "full") {
				continue
			}
			lane, ok := platformReleaseLaneByKey(state.PlatformReleaseLanes, r.LaneKey)
			if !ok || lane.Frozen || lane.ActiveReleaseID != r.ID || lane.FencingToken != r.FencingToken {
				return fail()
			}
			index := platformArtifactIndex(state.PlatformArtifacts, r.ArtifactID)
			if index < 0 {
				return fail()
			}
			if r.ReleaseChannel == "gray" {
				groups, err := platformconfig.ResolveTrafficCanary(state.PlatformArtifacts[index], r.CanaryRuleRef)
				if err != nil {
					return fail()
				}
				if !platformconfig.TrafficCanaryContains(groups, dependency.GroupID) {
					continue
				}
			}
			if selected != nil && selected.ReleasedAt.Equal(r.ReleasedAt) && selected.ID != r.ID {
				return fail()
			}
			if selected == nil || r.ReleasedAt.After(selected.ReleasedAt) {
				copy := r
				selected = &copy
			}
		}
		if selected == nil || selected.ArtifactID != dependency.Parent.ID || selected.Generation != dependency.Parent.Generation || selected.ReleaseChannel != dependency.ReleaseChannel || selected.CanaryRuleRef != dependency.CanaryRuleRef {
			return fail()
		}
		renewed := selected.ID != dependency.ReleaseID || selected.FencingToken != dependency.FencingToken
		if renewed && !dnsIdenticalSourceRenewal(state, child, dependency, *selected) {
			return fail()
		}
		// For an identical dynamic-source renewal, verification belongs to the
		// pinned historical configuration. The selected release's live DNS
		// proofs and consumer convergence remain independent mandatory gates.
		if dependency.RequireVerified && !renewed && (selected.VerificationState != model.PlatformArtifactVerificationStateVerified || selected.VerifiedLKGGeneration != dependency.Parent.Generation) {
			return fail()
		}
		for _, embedded := range dependency.Artifacts {
			index := platformArtifactIndex(state.PlatformArtifacts, embedded.ID)
			if index < 0 {
				return fail()
			}
			a := state.PlatformArtifacts[index]
			if a.ContentHash != embedded.ContentHash || a.GenerationSequence != embedded.GenerationSequence || a.ScopeKey != embedded.ScopeKey || a.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(a, keys).Pass {
				return fail()
			}
		}
	}
	return nil
}

// Dynamic DNS source authority follows independently published routing facts.
// Renewing the SAME previously verified immutable full parent must not strand
// its DNS baseline. Static pins, different content, gray, failed recovery,
// rollback and missing historical authority remain strict conflicts. This only
// admits the configuration reference; live DNS proof fences are not rewritten.
func dnsIdenticalSourceRenewal(state *model.State, child model.PlatformArtifact, dependency platformconfig.DNSRouteDependency, selected model.PlatformArtifactRelease) bool {
	sources, err := platformconfig.DNSRouteSourceAuthorizations(child)
	if err != nil {
		return false
	}
	approved := false
	for _, source := range sources {
		approved = approved || source.ScopeKey == dependency.Parent.ScopeKey
	}
	if !approved || dependency.ReleaseChannel != "full" || dependency.CanaryRuleRef != "" || selected.ReleaseChannel != "full" || selected.CanaryRuleRef != "" || selected.ArtifactID != dependency.Parent.ID || selected.Generation != dependency.Parent.Generation || selected.ScopeKey != dependency.Parent.ScopeKey || selected.Status != model.PlatformArtifactReleaseStatusActive || (selected.VerificationState != model.PlatformArtifactVerificationStateVerified && selected.VerificationState != model.PlatformArtifactVerificationStateServingUnverified) || (selected.VerificationState == model.PlatformArtifactVerificationStateVerified && selected.VerifiedLKGGeneration != dependency.Parent.Generation) || selected.FencingToken <= dependency.FencingToken {
		return false
	}
	index := platformArtifactReleaseIndex(state.PlatformArtifactReleases, dependency.ReleaseID)
	if index < 0 {
		return false
	}
	original := state.PlatformArtifactReleases[index]
	return original.ID == dependency.ReleaseID && original.ArtifactID == dependency.Parent.ID && original.ArtifactKind == model.PlatformArtifactKindReleaseSet && original.ScopeKey == dependency.Parent.ScopeKey && original.Generation == dependency.Parent.Generation && original.ReleaseChannel == "full" && original.CanaryRuleRef == "" && original.FencingToken == dependency.FencingToken && original.Status == model.PlatformArtifactReleaseStatusSuperseded && original.VerificationState == model.PlatformArtifactVerificationStateVerified && original.VerifiedLKGGeneration == dependency.Parent.Generation && !original.ReleasedAt.IsZero() && selected.ReleasedAt.After(original.ReleasedAt)
}

func validateDNSRouteProducerBindings(state *model.State, child model.PlatformArtifact, keys bundleauth.Keyring) error {
	sources, err := platformconfig.DNSRouteSourceAuthorizations(child)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrConflict, err)
	}
	if len(sources) == 0 {
		return nil
	}
	fail := func() error { return fmt.Errorf("%w: DNS routing source producer policy binding invalid", ErrConflict) }
	for _, source := range sources {
		index := platformArtifactIndex(state.PlatformArtifacts, source.PolicyArtifactID)
		if index < 0 {
			return fail()
		}
		artifact := state.PlatformArtifacts[index]
		policy, err := platformproducer.Decode(artifact)
		role := platformconfig.PublicationRoleCellRoutes
		if source.ScopeKey == platformconfig.GlobalScopeKey {
			role = ""
		}
		if err != nil || artifact.ContentHash != source.PolicyDigest || artifact.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(artifact, keys).Pass || policy.TargetScope != source.ScopeKey || policy.PublicationRole != role || policy.Mode != "serving" || policy.Serving.SinglePublication {
			return fail()
		}
	}
	check := func(parent model.PlatformArtifact, binding *model.PlatformPublicationPrecondition) error {
		if binding == nil {
			return fail()
		}
		index := platformArtifactReleaseIndex(state.PlatformArtifactReleases, binding.ReleaseID)
		if index < 0 {
			return fail()
		}
		release := state.PlatformArtifactReleases[index]
		scope, err := platformproducer.PolicyScopeForTarget(parent.ScopeKey)
		policyIndex := platformArtifactIndex(state.PlatformArtifacts, binding.ArtifactID)
		if policyIndex < 0 {
			return fail()
		}
		artifact := state.PlatformArtifacts[policyIndex]
		if err != nil || release.ArtifactID != binding.ArtifactID || artifact.ContentHash != binding.ContentHash || release.Generation != artifact.Generation || release.ScopeKey != scope || release.ArtifactKind != model.PlatformArtifactKindPolicySnapshot || release.ReleaseChannel != "shadow" || release.FencingToken != binding.FencingToken || (release.Status != model.PlatformArtifactReleaseStatusActive && release.Status != model.PlatformArtifactReleaseStatusSuperseded) || parent.Metadata[platformproducer.PolicyReleaseMetadata] != release.ID {
			return fail()
		}
		return nil
	}
	cells, err := platformconfig.DecodeCellRoutePublications(child)
	if err != nil {
		return fail()
	}
	for _, cell := range cells {
		if err := check(cell.Parent, cell.ProducerPolicy); err != nil {
			return err
		}
	}
	previous, err := platformconfig.DecodePreviousTrafficPublication(child)
	if err != nil {
		return fail()
	}
	if previous != nil {
		return check(previous.Parent, previous.ProducerPolicy)
	}
	return nil
}

// ValidateDNSPublicationReferences checks one coherent read snapshot during
// compilation. Serving publication repeats these same checks under its own
// transaction; compiling immutable content is never a serving grant.
func (s *Store) ValidateDNSPublicationReferences(child model.PlatformArtifact) error {
	dependencies, err := platformconfig.DNSRouteDependencies(child)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrConflict, err)
	}
	if len(dependencies) == 0 {
		return nil
	}
	keys := s.platformArtifactSigningKeyring()
	if s.db == nil {
		return s.withLockedState(false, func(state *model.State) error { return validateCellDNSReferencesInState(state, child, keys) })
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	state := &model.State{PlatformArtifacts: []model.PlatformArtifact{child}}
	parent := model.PlatformArtifact{ScopeKey: child.ScopeKey, Content: map[string]any{"publication_role": platformconfig.PublicationRoleCellDNS}}
	if err := s.pgLoadCellDNSReferences(ctx, tx, parent, state); err != nil {
		return err
	}
	return validateCellDNSReferencesInState(state, child, keys)
}
