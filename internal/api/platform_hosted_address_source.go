package api

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/store"
)

type hostedCapturePlan struct {
	selected model.PlatformArtifactRelease
	phase    string
	hosts    []string
	external []string
}

type externalHostHint struct {
	full, intent string
	names        []string
}

// A cache hit is a discovery hint only. Selection reopens the verified full
// and the current project intent in their respective transactions.
func (s *Server) pendingExternalPaths(parent model.PlatformArtifact, intent platformconfig.PlatformIntent) ([]string, error) {
	if !slices.ContainsFunc(intent.DNS, func(r platformconfig.DNSIntent) bool {
		return r.Type == "FUGUE_ROUTE" && r.RecordKind == model.EdgeDNSRecordKindCustomDomainTarget && r.Route != nil && !r.Route.ManagedHost
	}) {
		return nil, nil
	}
	if parent.Metadata["intent_generation"] == intent.Generation && intent.Generation != "" {
		return nil, nil
	}
	if q := s.hostedExternalHint.Load(); q != nil && q.full == parent.ContentHash && q.intent == intent.Generation && q.intent != "" {
		return q.names, nil
	}
	var old platformconfig.PlatformIntent
	a, err := s.store.GetPlatformArtifactByIdentity(model.PlatformArtifactKindPlatformIntent, platformconfig.GlobalScopeKey, parent.Metadata["intent_generation"])
	if err != nil {
		return nil, err
	}
	if a.Status != model.PlatformArtifactStatusValidated || s.store.VerifyPlatformArtifactIntegrity(a) != nil {
		return nil, errors.New("selected external domain intent is not validated")
	}
	raw, err := json.Marshal(a.Content)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(raw, &old); err != nil {
		return nil, err
	}
	d, err := platformconfig.Digest(old)
	if err != nil || d != parent.Metadata["intent_digest"] {
		return nil, errors.New("selected external domain intent lineage differs")
	}
	names := platformconfig.ExternalChangedHosts(old, intent)
	s.hostedExternalHint.Store(&externalHostHint{parent.ContentHash, intent.Generation, names})
	return names, nil
}

// Discovery is cheap and does not authorize compilation. Don't read the full
// operational input twice on every unchanged-source poll or for unrelated
// scopes. Promotions, verifications and repairs of pending releases precede
// this new-input boundary.
func (s *Server) discoverHostedCapture(policy platformproducer.Policy, p platformIntentProjectionResponse) (hostedCapturePlan, error) {
	var plan hostedCapturePlan
	if policy.InputSource != "business-static-intent" || policy.Mode != "serving" || policy.TargetScope != platformconfig.GlobalScopeKey || policy.PublicationRole != "" || policy.DNSPolicyArtifactID == "" {
		return plan, nil
	}
	parent, full, ok, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, policy.TargetScope, "full")
	if err != nil || !ok {
		return plan, err
	}
	external, err := s.pendingExternalPaths(parent, p.Intent)
	if err != nil {
		return plan, err
	}
	if len(external) > 0 {
		plan = hostedCapturePlan{selected: full, phase: "external", external: external}
	}
	if len(p.hostedAddressHolds) == 0 {
		return plan, nil
	}
	ref, _, err := platformconfig.HostedAddressAuthority(parent)
	if err != nil {
		return plan, err
	}
	if ref != nil {
		phase := "retained"
		if raw := parent.Metadata[store.HostedSuccessorMetadata]; raw != "" {
			// A verified phase is a new actionable input even when the
			// business edit is unchanged. Do not immediately reuse its old
			// shadow instead of moving to paths or releasing deferred B.
			phase = "after-scoped/" + full.ID
		}
		return hostedCapturePlan{selected: full, phase: phase, external: external}, nil
	}
	if !slices.ContainsFunc(p.Intent.DNS, func(d platformconfig.DNSIntent) bool {
		return d.Type == "FUGUE_ROUTE" && d.Route != nil && d.Route.ManagedHost
	}) {
		return plan, nil
	}
	dns, err := s.consumerAssignmentChild(parent, model.PlatformArtifactKindDNSAnswerBundle)
	if err != nil {
		return plan, err
	}
	previous, err := platformconfig.DecodeDNSArtifact(dns)
	if err != nil {
		return plan, err
	}
	hosts := platformconfig.NewHostedPathReservationHosts(previous, p.Intent, p.hostedAddressHolds)
	if len(hosts) > 0 {
		return hostedCapturePlan{selected: full, phase: "initial", hosts: slices.Sorted(maps.Keys(hosts)), external: external}, nil
	}
	return plan, nil
}

func (p hostedCapturePlan) sourceDigest(ordinary string) (string, error) {
	if len(p.external) > 0 {
		return platformconfig.Digest([]any{"scoped-external-target/v1", ordinary, p.phase, p.selected.ID, p.hosts, p.external})
	}
	if p.phase == "" {
		return ordinary, nil
	}
	if p.phase == "initial" {
		// Only a pending root-to-path transition gets a new compilation
		// contract. Do not churn unrelated steady-state or Cell captures.
		return platformconfig.Digest(struct {
			Source, Phase, Contract, SelectedFull string
			Hosts                                 []string
		}{ordinary, p.phase, platformconfig.HostedProposalSchema + "/scoped-successor-dns-inputs-v2", p.selected.ID, p.hosts})
	}
	return platformconfig.Digest(struct {
		Source, Phase string
		Hosts         []string
	}{ordinary, p.phase, p.hosts})
}

// This is a read, not a publication or an acknowledgement. Check the source
// policy and each child through their real selected full, the positive LKG and
// the same scoped producer selection used by the independent DNS producer.
// No redacted diagnostic GET, unverified rollback or guessed physical IP can
// be an input to a route-only publication.
func (s *Server) readHostedAddressSource(ctx context.Context, policy platformproducer.Policy, authority model.PlatformArtifactRelease, effective []platformconfig.HostedAddressHold) (platformconfig.HostedAddressSource, error) {
	return s.readHostedAddressSourceWith(ctx, policy, authority, effective, s.observeRetainedTrafficSource)
}

func (s *Server) readHostedAddressSourceWith(ctx context.Context, policy platformproducer.Policy, authority model.PlatformArtifactRelease, effective []platformconfig.HostedAddressHold, observe trafficSourceObservation) (platformconfig.HostedAddressSource, error) {
	var out platformconfig.HostedAddressSource
	bad := errors.New("hosted DNS reservation requires current verified global producer, full and physical input")
	if policy.TargetScope != platformconfig.GlobalScopeKey || policy.Mode != "serving" || policy.PublicationRole != "" || policy.InputSource != "business-static-intent" || authority.ID == "" || authority.FencingToken < 1 {
		return out, bad
	}
	old, err := s.readBoundTrafficSource(ctx, policy.TargetScope, observe)
	if err != nil {
		return out, err
	}
	producer, active, ok, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, model.PlatformArtifactReleaseChannelShadow)
	if err != nil || !ok || producer.ID != authority.ArtifactID || active.ID != authority.ID || active.FencingToken != authority.FencingToken ||
		s.store.VerifyPlatformArtifactIntegrity(producer) != nil || old.parent.Metadata[platformproducer.PolicyReleaseMetadata] != active.ID ||
		old.parent.Metadata[platformproducer.DNSPolicyIDMetadata] != policy.DNSPolicyArtifactID || old.parent.Metadata[platformproducer.DNSPolicyDigestMetadata] != policy.DNSPolicyDigest ||
		old.parent.Metadata[platformproducer.StaticIntentIDMetadata] != policy.StaticIntentArtifactID || old.parent.Metadata[platformproducer.StaticIntentDigestMetadata] != policy.StaticIntentDigest {
		return out, bad
	}
	decoded, err := platformproducer.Decode(producer)
	if err != nil {
		return out, bad
	}
	left, e1 := platformconfig.Digest(decoded)
	right, e2 := platformconfig.Digest(policy)
	if e1 != nil || e2 != nil || left != right {
		return out, bad
	}
	input, err := s.store.GetPlatformArtifact(policy.DNSPolicyArtifactID)
	if err != nil || input.ContentHash != policy.DNSPolicyDigest || input.Status != model.PlatformArtifactStatusValidated || s.store.VerifyPlatformArtifactIntegrity(input) != nil {
		return out, bad
	}
	static, err := s.loadStaticPlatformIntent(policy.StaticIntentArtifactID, policy.StaticIntentDigest)
	if err != nil {
		return out, err
	}
	projected, err := platformproducer.DecodeProjectionPolicy(input, static.Consumers, policy.HostedZoneTemplates)
	if err != nil {
		return out, err
	}
	previous, err := platformproducer.PublicationOfflineHolds(old.parent, projected.OfflineEdges)
	if err != nil {
		return out, err
	}
	holds := func(v []platformproducer.OfflineEdge) []platformconfig.HostedAddressHold {
		out := make([]platformconfig.HostedAddressHold, 0, len(v))
		for _, h := range v {
			out = append(out, platformconfig.HostedAddressHold{EdgeID: h.EdgeID, EdgeGroupID: h.EdgeGroupID, Address: h.PublicIPv4})
		}
		return out
	}
	known := map[platformconfig.HostedAddressHold]bool{}
	for _, h := range holds(projected.OfflineEdges) {
		known[h] = true
	}
	for _, h := range effective {
		if !known[h] {
			return out, bad
		}
		delete(known, h)
	}
	dns, err := s.consumerAssignmentChild(old.parent, model.PlatformArtifactKindDNSAnswerBundle)
	if err != nil || dns.Status != model.PlatformArtifactStatusValidated || s.store.VerifyPlatformArtifactIntegrity(dns) != nil {
		return out, bad
	}
	tls, err := s.consumerAssignmentChild(old.parent, model.PlatformArtifactKindCaddyRouteConfig)
	if err != nil || tls.Status != model.PlatformArtifactStatusValidated || s.store.VerifyPlatformArtifactIntegrity(tls) != nil {
		return out, bad
	}
	payload, err := platformconfig.DecodeDNSArtifact(dns)
	if err != nil {
		return out, err
	}
	pRef, pDigest, e := platformconfig.HostedAddressAuthority(old.parent)
	dRef, dDigest, f := platformconfig.HostedAddressAuthority(dns)
	if e != nil || f != nil || pDigest != dDigest || (pRef == nil) != (len(old.snapshot.HostedAddressReservations) == 0) {
		return out, bad
	}
	if pRef != nil {
		d, err := platformconfig.Digest(old.snapshot.HostedAddressReservations)
		if err != nil || d != pDigest || dRef == nil || *pRef != *dRef {
			return out, bad
		}
		for _, r := range old.snapshot.HostedAddressReservations {
			if r.Source != *pRef {
				return out, bad
			}
		}
	}
	pin := platformconfig.PreviousTrafficPublicationReference{ReleaseSetID: old.parent.ID, ReleaseSetDigest: old.parent.ContentHash, ReleaseID: old.release.ID, FencingToken: old.release.FencingToken, RouteArtifactID: old.route.ID, RouteArtifactDigest: old.route.ContentHash, TLSArtifactID: tls.ID, TLSArtifactDigest: tls.ContentHash, DNSArtifactID: dns.ID, DNSArtifactDigest: dns.ContentHash}
	if pin.Validate() != nil {
		return out, bad
	}
	err = s.store.CheckPlatformInputRead(ctx, []store.PlatformInputReadSelection{
		{ArtifactID: producer.ID, ContentHash: producer.ContentHash, ReleaseID: active.ID},
		{ArtifactID: old.parent.ID, ContentHash: old.parent.ContentHash, ReleaseID: old.release.ID, LKGKind: old.lkg.ArtifactKind, LKGScope: old.lkg.ScopeKey, LKG: &old.lkg},
		{ArtifactID: input.ID, ContentHash: input.ContentHash}, {ArtifactID: dns.ID, ContentHash: dns.ContentHash}, {ArtifactID: tls.ID, ContentHash: tls.ContentHash},
	})
	if err != nil {
		return out, err
	}
	if !old.lkg.ExpiresAt.After(time.Now()) || ctx.Err() != nil {
		return out, bad
	}
	if err = s.store.ValidateDNSProducerSelection(ctx, model.PlatformPublicationPrecondition{ArtifactID: producer.ID, ContentHash: producer.ContentHash, ReleaseID: active.ID, FencingToken: active.FencingToken}, model.PlatformPublicationPrecondition{ArtifactID: old.parent.ID, ContentHash: old.parent.ContentHash, ReleaseID: old.release.ID, FencingToken: old.release.FencingToken}); err != nil {
		return out, err
	}
	return platformconfig.HostedAddressSource{Publication: pin, PriorIntent: old.intent, Prior: payload, PriorRoute: &old.route, PriorTLS: &tls,
		Authority:          platformconfig.HostedProposalAuthority{Producer: model.PlatformPublicationPrecondition{ArtifactID: producer.ID, ContentHash: producer.ContentHash, ReleaseID: active.ID, FencingToken: active.FencingToken}, LKGID: old.lkg.ID, EvidenceHash: old.lkg.VerificationEvidenceHash, ExpiresAt: old.lkg.ExpiresAt},
		PriorDNSSelections: old.snapshot.DNSSelections, Inherited: old.snapshot.HostedAddressReservations, PriorHolds: holds(previous), EffectiveHolds: effective}, nil
}
