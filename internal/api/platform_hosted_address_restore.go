package api

import (
	"context"
	"fmt"
	"slices"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
)

// A deployment setting opts into one exact historic declaration. No public
// route request (including preparation) can provide this publication authority.
func (s *Server) discoverHostedDeclarationRestore(ctx context.Context, policy platformproducer.Policy) (*pc.HostedDeclarationRestorePlan, model.PlatformArtifactRelease, error) {
	plan := s.hostedDeclarationRestore
	var empty model.PlatformArtifactRelease
	if plan == nil || policy.Mode != "serving" || policy.InputSource != "business-static-intent" ||
		policy.TargetScope != pc.GlobalScopeKey || policy.PublicationRole != "" {
		return nil, empty, nil
	}
	parent, full, found, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, pc.GlobalScopeKey, model.PlatformArtifactReleaseChannelFull)
	if err != nil || !found {
		return nil, empty, err
	}
	if full.VerificationState != model.PlatformArtifactVerificationStateVerified || full.FencingToken <= plan.History.FencingToken {
		return nil, empty, fmt.Errorf("shared DNS restore needs a newer verified full as its baseline")
	}
	dns, err := consumerAssignmentChild(parent, model.PlatformArtifactKindDNSAnswerBundle, func(id string) (model.PlatformArtifact, error) {
		return s.store.GetPlatformArtifactContext(ctx, id)
	})
	if err != nil || s.store.VerifyPlatformArtifactIntegrity(parent) != nil || s.store.VerifyPlatformArtifactIntegrity(dns) != nil {
		return nil, empty, fmt.Errorf("shared DNS restore discovery lacks the verified current DNS child")
	}
	payload, err := pc.DecodeDNSArtifact(dns)
	if err != nil {
		return nil, empty, err
	}
	target, ok := pc.HostedExternalDNSSharedTarget(plan.Target, payload.Records, payload.ReadinessPlan)
	if !ok {
		return nil, empty, fmt.Errorf("shared DNS restore has no current external target")
	}
	if slices.ContainsFunc(payload.Records, func(r pc.DNSIntent) bool {
		return r.Hostname == target.Target && r.Type == plan.Family && slices.Contains(r.Values, plan.Held.Address)
	}) {
		// Idempotent after the first verified commit. Future compiles carry
		// custody from the active signed input, not from old history.
		return nil, empty, nil
	}
	return plan, full, nil
}

func (s *Server) compileHostedDeclarationRestore(ctx context.Context, policy platformproducer.Policy, authority, selected model.PlatformArtifactRelease,
	holds []pc.HostedAddressHold, req pc.CompileRequest, plan pc.HostedDeclarationRestorePlan) (pc.CompileResult, pc.PreviousTrafficPublicationReference, error) {
	var empty pc.CompileResult
	var noRef pc.PreviousTrafficPublicationReference
	source, err := s.readHostedAddressSource(ctx, policy, authority, holds)
	if err != nil {
		return empty, noRef, err
	}
	if source.Publication.ReleaseID != selected.ID || source.Publication.ReleaseSetID != selected.ArtifactID || source.Publication.FencingToken != selected.FencingToken {
		return empty, noRef, fmt.Errorf("shared DNS restore lost the current full before compilation")
	}
	history, err := s.store.ReadHostedDeclarationHistory(ctx, plan)
	if err != nil {
		return empty, noRef, err
	}
	result, err := pc.CompileHostedDeclarationRestore(req, source, plan, history)
	return result, source.Publication, err
}
