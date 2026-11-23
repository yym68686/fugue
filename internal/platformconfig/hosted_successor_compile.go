package platformconfig

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"

	"fugue/internal/edgetopology"
	"fugue/internal/model"
)

// CompileHostedSuccessor compiles a normal successor, not an overlay applied
// to an already signed child. The selected full supplies configuration for
// other hosts; only the live capture supplies observations. Never renew an
// observation, rewrite its source time or use an old request as a health ACK.
func CompileHostedSuccessor(req CompileRequest, source HostedAddressSource, p HostedScopeProposal, phase string) (CompileResult, error) {
	var empty CompileResult
	bad := errors.New("host successor would change configuration outside its authorized phase")
	if source.PriorRoute == nil || source.PriorTLS == nil || source.Publication.Validate() != nil ||
		source.PriorRoute.ID != source.Publication.RouteArtifactID || source.PriorRoute.ContentHash != source.Publication.RouteArtifactDigest ||
		source.PriorTLS.ID != source.Publication.TLSArtifactID || source.PriorTLS.ContentHash != source.Publication.TLSArtifactDigest ||
		p.Authority.Producer != source.Authority.Producer || req.Intent.Scope != GlobalScopeKey || source.PriorIntent.Scope != GlobalScopeKey ||
		req.Intent.PublicationRole != "" || req.Policy.PublicationRole != "" || req.RuntimeSnapshot.IntentGeneration != req.Intent.Generation ||
		phase == "prepare" && (p.Base != source.Publication || !p.Authority.Same(source.Authority)) {
		return empty, fmt.Errorf("%w: source or producer identity", bad)
	}
	want, err := ComposeHostedDNSPhase(source.Prior, p, phase)
	if err != nil {
		return empty, err
	}
	owned := map[string]bool{p.Host: true}
	req.Intent = NormalizePlatformIntent(req.Intent)
	source.PriorIntent = NormalizePlatformIntent(source.PriorIntent)
	aliases := stableHostedAliasDeclarations(source.PriorIntent, req.Intent)
	for _, r := range p.Records {
		if r.Hostname != p.Host && aliases[r.Hostname] != p.Host {
			return empty, fmt.Errorf("%w: alias declaration", bad)
		}
		owned[r.Hostname] = true
	}
	intent := source.PriorIntent
	intent.Routes = spliceHostedInput(intent.Routes, req.Intent.Routes, func(r RouteIntent) bool { return r.Hostname == p.Host })
	intent.DNS = spliceHostedInput(intent.DNS, req.Intent.DNS, func(r DNSIntent) bool { return owned[r.Hostname] })
	// A path change cannot alter certificate or topology ownership. Do not
	// use an old cache policy as a substitute for a changed shared definition.
	if !reflect.DeepEqual(intent.CachePolicies, req.Intent.CachePolicies) ||
		!reflect.DeepEqual(intent.EdgeTopology, req.Intent.EdgeTopology) ||
		!reflect.DeepEqual(intent.DNSConsumers, req.Intent.DNSConsumers) || !reflect.DeepEqual(intent.ApplicationDomains, req.Intent.ApplicationDomains) ||
		!reflect.DeepEqual(intent.ACMEChallenges, req.Intent.ACMEChallenges) {
		return empty, fmt.Errorf("%w: shared intent", bad)
	}
	// The certificate declaration remains exactly the selected full's, even
	// when a different host's new enrollment is already in the business
	// projection. Never attach its fact to A, or use yesterday's TLS facts.
	req.RuntimeSnapshot.TLSDomains, err = scopedHostedTLSFacts(intent.TLS, req.Intent.TLS, owned, req.RuntimeSnapshot.TLSDomains)
	if err != nil {
		return empty, err
	}
	intent = NormalizePlatformIntent(intent)
	intent.Generation, err = PlatformIntentGeneration(intent)
	if err != nil {
		return empty, err
	}
	req.Intent, req.RuntimeSnapshot.IntentGeneration = intent, intent.Generation
	if err := ValidatePlatformIntent(intent); err != nil {
		return empty, err
	}
	got, capture, err := CompileHostedPathWithCapture(req, source)
	if err != nil {
		return empty, err
	}
	if (capture != nil) != (phase == "prepare") || len(got.InputSnapshot.HostedAddressReservations) == 0 {
		return empty, fmt.Errorf("%w: reservation phase", bad)
	}
	// Compare the complete output against composing exactly one phase on the
	// selected predecessor, including unrelated ready targets, answer views,
	// same-owner aliases, route cache policy, and per-physical-ingress grants.
	dnsArtifact, err := globalProposalArtifact(got.DNSArtifact, model.PlatformArtifactKindDNSAnswerBundle)
	if err != nil {
		return empty, err
	}
	dns, err := DecodeDNSArtifact(dnsArtifact)
	if err != nil {
		return empty, err
	}
	// A v1 offer removed held addresses from every selector but could retain
	// their now-empty group in the allow-list. The ordinary compiler emits
	// only eligible groups. Permit that exact narrowing on prepare, never an
	// added group, changed order, or change to a foreign record.
	if phase == "prepare" {
		for i := range want.Views {
			for j := range want.Views[i].Records {
				r := &want.Views[i].Records[j]
				for _, h := range p.Hold {
					if h.Record != r.Name || h.Family != r.Type {
						continue
					}
					live, previous := map[string]bool{}, map[string]bool{}
					for _, t := range h.After.Targets {
						live[t.EdgeGroupID] = true
					}
					for _, t := range h.Before.Targets {
						previous[t.EdgeGroupID] = true
					}
					r.AnswerPolicy.AllowedEdgeGroups = slices.DeleteFunc(r.AnswerPolicy.AllowedEdgeGroups, func(g string) bool { return previous[g] && !live[g] })
				}
			}
		}
	}
	for _, field := range []struct {
		name string
		a, b any
	}{
		{"wire records", source.Prior.Records, dns.Records}, {"local DNS views", source.Prior.ConsumerViews, dns.ConsumerViews},
		{"readiness requirements", &want.Plan, dns.ReadinessPlan}, {"query views", want.Views, dns.QueryViews}, {"answer rules", want.Rules, dns.Policy.DNSAnswerRules},
	} {
		if !reflect.DeepEqual(field.a, field.b) {
			return empty, fmt.Errorf("%w: %s", bad, field.name)
		}
	}
	oldPolicy, newPolicy := source.Prior.Policy, dns.Policy
	oldPolicy.DNSAnswerRules, newPolicy.DNSAnswerRules = nil, nil
	og, oe := PolicySnapshotGeneration(oldPolicy)
	ng, ne := PolicySnapshotGeneration(newPolicy)
	if oe != nil || ne != nil || og != ng {
		return empty, fmt.Errorf("%w: shared policy", bad)
	}
	if _, err := ComposeHostedSuccessorRoutes(*source.PriorRoute, got.RouteArtifact, p, phase); err != nil {
		return empty, err
	}
	before, err := ProjectRouteArtifact(*source.PriorRoute)
	if err != nil {
		return empty, err
	}
	after, err := ProjectRouteArtifact(got.RouteArtifact)
	if err != nil {
		return empty, err
	}
	foreign := func(rows []model.EdgeRouteIntent) []model.EdgeRouteIntent {
		return slices.DeleteFunc(slices.Clone(rows), func(r model.EdgeRouteIntent) bool { return r.Hostname == p.Host })
	}
	if !reflect.DeepEqual(foreign(before.Routes), foreign(after.Routes)) || !reflect.DeepEqual(before.TLSAllowlist, after.TLSAllowlist) ||
		!reflect.DeepEqual(before.CachePolicies, after.CachePolicies) {
		return empty, fmt.Errorf("%w: foreign dispatch, TLS or cache", bad)
	}
	grants := func(a model.PlatformArtifact) ([]edgetopology.RouteGrant, error) {
		var v []edgetopology.RouteGrant
		raw, e := json.Marshal(a.Content["edge_selection_grants"])
		if e != nil {
			return nil, e
		}
		if e = json.Unmarshal(raw, &v); e != nil {
			return nil, e
		}
		return slices.DeleteFunc(v, func(g edgetopology.RouteGrant) bool { return owned[g.Hostname] }), nil
	}
	left, e := grants(*source.PriorRoute)
	right, f := grants(got.RouteArtifact)
	if e != nil || f != nil || !reflect.DeepEqual(left, right) {
		return empty, fmt.Errorf("%w: foreign grants", bad)
	}
	return got, nil
}

func spliceHostedInput[T any](base, desired []T, owned func(T) bool) []T {
	out := slices.DeleteFunc(slices.Clone(base), owned)
	for _, x := range desired {
		if owned(x) {
			out = append(out, x)
		}
	}
	return out
}

func scopedHostedTLSFacts(selected, desired []TLSIntent, owned map[string]bool, facts []TLSDomainObservation) ([]TLSDomainObservation, error) {
	bad := errors.New("host successor TLS declaration or current observation is not independently bound")
	forHost := func(rows []TLSIntent) []TLSIntent {
		return slices.DeleteFunc(slices.Clone(rows), func(r TLSIntent) bool { return !owned[r.Hostname] })
	}
	if !slices.Equal(forHost(selected), forHost(desired)) {
		return nil, bad
	}
	refs, pending := map[string]TLSIntent{}, map[string]TLSIntent{}
	for _, r := range selected {
		if r.DomainRef != "" {
			refs[r.DomainRef] = r
		}
	}
	for _, r := range desired {
		if r.DomainRef != "" {
			pending[r.DomainRef] = r
		}
	}
	out := make([]TLSDomainObservation, 0, len(facts))
	seen := map[string]bool{}
	for _, f := range facts {
		if seen[f.Ref] {
			return nil, bad
		}
		seen[f.Ref] = true
		if _, keep := refs[f.Ref]; keep {
			out = append(out, f) // CompileTLSDomains validates exact identity and original timestamps.
			continue
		}
		r, ok := pending[f.Ref]
		if !ok || owned[f.Hostname] || r.Hostname != f.Hostname || r.AppID != f.AppID || r.TenantID != f.TenantID {
			return nil, bad
		}
	}
	return out, nil
}
