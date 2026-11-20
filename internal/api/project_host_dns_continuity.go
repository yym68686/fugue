package api

import (
	"context"
	"slices"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformproducer"
)

// Keep old routing intent intact while a DNS authority is upgrading. No
// timestamp, process version, or merely staged consumer counts as readiness.
func (s *Server) checkProjectHostDNSContinuity(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	if !s.projectHostDNSAuthoritySupportsContinuity(ctx, platformconfig.GlobalScopeKey, "") {
		return errProjectHostDNSContinuity
	}
	scopes, err := s.store.ListPlatformProducerScopes()
	if err != nil || ctx.Err() != nil {
		return errProjectHostDNSContinuity
	}
	for _, scope := range scopes {
		artifact, _, found, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindPolicySnapshot, scope, model.PlatformArtifactReleaseChannelShadow)
		if err != nil || !found || s.store.VerifyPlatformArtifactIntegrity(artifact) != nil {
			return errProjectHostDNSContinuity
		}
		policy, err := platformproducer.Decode(artifact)
		if err != nil {
			return errProjectHostDNSContinuity
		}
		if policy.PublicationRole != platformconfig.PublicationRoleCellDNS || policy.Mode != "serving" {
			continue
		}
		if !s.projectHostDNSAuthoritySupportsContinuity(ctx, policy.TargetScope, platformconfig.PublicationRoleCellDNS) {
			return errProjectHostDNSContinuity
		}
	}
	if ctx.Err() != nil {
		return errProjectHostDNSContinuity
	}
	return nil
}

func (s *Server) projectHostDNSAuthoritySupportsContinuity(ctx context.Context, scope, expectedRole string) bool {
	parent, release, found, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, scope, model.PlatformArtifactReleaseChannelFull)
	role, _ := parent.Content["publication_role"].(string)
	if err != nil || !found || s.store.VerifyPlatformArtifactIntegrity(parent) != nil || role != expectedRole ||
		release.VerificationState != model.PlatformArtifactVerificationStateVerified {
		return false
	}
	sets, err := s.store.ListPlatformExpectedConsumerSetsContext(ctx, model.PlatformExpectedConsumerSetFilter{ReleaseSetID: parent.ID, ArtifactReleaseID: release.ID, ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle, ScopeKey: scope})
	if err != nil {
		return false
	}
	var latest model.PlatformExpectedConsumerSet
	for _, set := range sets {
		if set.Revision == latest.Revision && set.ID != latest.ID {
			return false
		}
		if set.Revision > latest.Revision {
			latest = set
		}
	}
	if !latest.RequiresConsumers || platformcontrol.ValidateDeclaredTrafficConsumerSet(parent, latest) != nil {
		return false
	}
	binding := s.platformConvergenceBinding(latest)
	consumers, err := s.store.ListPlatformConsumers(latest.ArtifactKind, scope)
	if err != nil || binding == nil || binding.ArtifactReleaseID != release.ID {
		return false
	}
	status := s.evaluateLiveConsumerConvergence(ctx, latest, consumers, binding)
	if !projectHostDNSConsumersReady(status) {
		return false
	}
	// Recheck the fence after observing actual backend bindings.
	_, current, found, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, scope, model.PlatformArtifactReleaseChannelFull)
	return ctx.Err() == nil && err == nil && found && current.ID == release.ID && current.FencingToken == release.FencingToken && current.VerificationState == release.VerificationState
}

func projectHostDNSConsumersReady(status model.PlatformConsumerConvergenceStatus) bool {
	if !status.Pass || status.RequiredPassing == 0 || status.RequiredPassing != status.RequiredExpected || len(status.UnexpectedConsumers) != 0 {
		return false
	}
	count := 0
	for _, item := range status.Assessments {
		if !item.Required {
			continue
		}
		count++
		if item.State != model.InvariantEvidenceStatePass || item.Observed == nil ||
			!slices.Contains(item.Observed.CompatibilityCapabilities, platformconfig.DNSManagedHostContinuityCapabilityV1) {
			return false
		}
	}
	return count == status.RequiredExpected
}
