package routeartifact

import (
	"errors"
	"reflect"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/routeproof"
)

// HostedPhase contains no signed envelope. A reader must not take a digest of
// it as proof of publication or consumer readiness. The publisher selects only
// the next phase, after comparing the live predecessor and its observations.
type HostedPhase struct {
	Groups map[string]HostedGroupContent
	DNS    pc.HostedDNSContent
}

// BuildHostedProgram materializes both halves from one authenticated global
// compiler source. Never use a Cell's route artifact as a global group base.
// Absence of observations for a held ingress must not omit its route.
func BuildHostedProgram(source pc.HostedAddressSource, p pc.HostedScopeProposal) (prepared, forward HostedPhase, err error) {
	var empty HostedPhase
	bad := errors.New("hosted DNS requirements and group routes do not form a reversible program")
	if p.Base != source.Publication || p.Base.Validate() != nil || !p.Authority.Same(source.Authority) || source.PriorRoute == nil || source.PriorTLS == nil ||
		source.PriorRoute.ID != p.Base.RouteArtifactID || source.PriorRoute.ContentHash != p.Base.RouteArtifactDigest ||
		source.PriorTLS.ID != p.Base.TLSArtifactID || source.PriorTLS.ContentHash != p.Base.TLSArtifactDigest ||
		source.PriorRoute.ScopeKey != pc.GlobalScopeKey || source.PriorTLS.ScopeKey != pc.GlobalScopeKey ||
		source.PriorRoute.ArtifactKind != model.PlatformArtifactKindEdgeRouteBundle || source.PriorTLS.ArtifactKind != model.PlatformArtifactKindCaddyRouteConfig ||
		source.Prior.ReadinessPlan == nil || source.Prior.Policy.Scope != pc.GlobalScopeKey || source.Prior.Policy.PublicationRole != "" || len(p.Physical) == 0 {
		return empty, empty, bad
	}
	base := map[string]model.EdgeRouteBundle{}
	for _, n := range p.Physical {
		if _, exists := base[n.EdgeGroupID]; !exists {
			b, e := MaterializeForGroup(*source.PriorRoute, n.EdgeGroupID)
			if e != nil {
				return empty, empty, e
			}
			base[n.EdgeGroupID] = b
		}
	}
	toBundles := func(phase HostedPhase) map[string]model.EdgeRouteBundle {
		out := map[string]model.EdgeRouteBundle{}
		for g, c := range phase.Groups {
			out[g] = model.EdgeRouteBundle{EdgeGroupID: g, Routes: c.Routes, TLSAllowlist: c.TLSAllowlist, CachePolicies: c.CachePolicies}
		}
		return out
	}
	run := func(groups map[string]model.EdgeRouteBundle, dns pc.DNSArtifactPayload, name string) (HostedPhase, error) {
		selected, e := pc.ComposeHostedDNSPhase(dns, p, name)
		if e != nil {
			return empty, e
		}
		out := HostedPhase{DNS: selected, Groups: map[string]HostedGroupContent{}}
		for g, b := range groups {
			content, e := ComposeHostedGroupEdit(b, p, name)
			if e != nil {
				return empty, e
			}
			out.Groups[g] = content
		}
		paths := p.Prepared
		if name == "forward" {
			paths = p.After
		}
		if !hostedCoverage(toBundles(out), &out.DNS.Plan, p, paths, p.Eligible) {
			return empty, bad
		}
		return out, nil
	}
	if !hostedCoverage(base, source.Prior.ReadinessPlan, p, p.Before, p.Physical) {
		return empty, empty, bad
	}
	prepared, e := run(base, source.Prior, "prepare")
	if e != nil {
		return empty, empty, e
	}
	dns := source.Prior
	dns.Policy.DNSAnswerRules = prepared.DNS.Rules
	dns.QueryViews, dns.ReadinessPlan = prepared.DNS.Views, &prepared.DNS.Plan
	forward, e = run(toBundles(prepared), dns, "forward")
	if e != nil {
		return empty, empty, e
	}
	dns.QueryViews, dns.ReadinessPlan = forward.DNS.Views, &forward.DNS.Plan
	dns.Policy.DNSAnswerRules = forward.DNS.Rules
	restored, e := run(toBundles(forward), dns, "withdraw")
	if e != nil || !reflect.DeepEqual(restored, prepared) {
		return empty, empty, errors.Join(bad, e)
	}
	return prepared, forward, nil
}

func hostedCoverage(groups map[string]model.EdgeRouteBundle, plan *pc.DNSReadinessPlan, p pc.HostedScopeProposal, rows pc.HostedDispatchRows, eligible []pc.HostedAddressHold) bool {
	type key struct {
		n    pc.HostedAddressHold
		path string
	}
	allowed, physical := map[pc.HostedAddressHold]bool{}, map[pc.HostedAddressHold]bool{}
	for _, n := range p.Physical {
		if physical[n] || n.EdgeID == "" || n.Address == "" || n.EdgeGroupID == "" {
			return false
		}
		physical[n] = true
	}
	for _, n := range eligible {
		if !physical[n] || allowed[n] {
			return false
		}
		allowed[n] = true
	}
	want := map[string]string{}
	for _, r := range rows.Routes {
		if want[r.PathPrefix] != "" || r.AppID == "" || r.Hostname != p.Host || r.TenantID != rows.Owner.TenantID {
			return false
		}
		want[r.PathPrefix] = r.AppID
	}
	if len(allowed) == 0 || want["/"] == "" || plan == nil {
		return false
	}
	byGroup := map[string]map[string]string{}
	for g, b := range groups {
		if b.EdgeGroupID != g {
			return false
		}
		proofs := map[string]string{}
		for _, r := range b.Routes {
			if r.Hostname == p.Host {
				path := model.NormalizeAppRoutePathPrefix(r.PathPrefix)
				if r.TenantID != rows.Owner.TenantID || want[path] != r.AppID || proofs[path] != "" {
					return false
				}
				d, e := routeproof.Digest(r)
				if e != nil {
					return false
				}
				proofs[path] = d
			}
		}
		if len(proofs) != len(want) {
			return false
		}
		byGroup[g] = proofs
	}
	expected := map[key]string{}
	for n := range physical {
		proofs, ok := byGroup[n.EdgeGroupID]
		if !ok {
			return false
		}
		if allowed[n] {
			for path, d := range proofs {
				expected[key{n, path}] = d
			}
		}
	}
	for _, pr := range plan.Probes {
		if pr.Hostname != p.Host {
			continue
		}
		k := key{pc.HostedAddressHold{EdgeID: pr.EdgeID, EdgeGroupID: pr.EdgeGroupID, Address: pr.Address}, pr.Path}
		if d, ok := expected[k]; !ok || d != pr.RouteDigest {
			return false
		}
		delete(expected, k)
	}
	return len(expected) == 0
}
