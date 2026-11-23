package routeartifact

import (
	"errors"
	"reflect"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
)

// The cache ID namespace is group-wide, even though routes are host-owned.
// Prove shared dependencies on the full before dropping foreign rows, so A
// can switch releases without gaining authority over B's policy.
func hostedGroupSource(b model.EdgeRouteBundle, p pc.HostedScopeProposal) (model.EdgeRouteBundle, error) {
	bad := errors.New("host program conflicts with an independently owned cache policy")
	own, foreign := map[string]bool{}, map[string]bool{}
	out := model.EdgeRouteBundle{EdgeGroupID: b.EdgeGroupID}
	for _, r := range b.Routes {
		if r.Hostname == p.Host {
			out.Routes = append(out.Routes, r)
			own[r.CachePolicyID] = true
		} else {
			foreign[r.CachePolicyID] = true
		}
	}
	byID := map[string]model.CachePolicy{}
	for _, c := range b.CachePolicies {
		if _, dup := byID[c.ID]; dup || c.ID == "" {
			return model.EdgeRouteBundle{}, bad
		}
		byID[c.ID] = c
		if own[c.ID] {
			out.CachePolicies = append(out.CachePolicies, c)
		}
	}
	for _, rows := range []pc.HostedDispatchRows{p.Before, p.Prepared, p.After} {
		for _, c := range rows.Policies {
			if foreign[c.ID] && !reflect.DeepEqual(c, byID[c.ID]) {
				return model.EdgeRouteBundle{}, bad
			}
		}
	}
	for _, c := range b.TLSAllowlist {
		if c.Hostname == p.Host {
			out.TLSAllowlist = append(out.TLSAllowlist, c)
		}
	}
	return out, nil
}

// BuildHostedProgramUnits returns two independently executable sets of
// configuration rows, not whole replacement group bundles or zone files.
// They are not serving grants. Apply only through a host-scoped transition
// with its own authority; an absent foreign hostname is not a deletion.
// Keep the existing shared-cache and physical-ingress checks at the source.
func BuildHostedProgramUnits(source pc.HostedAddressSource, p pc.HostedScopeProposal) (HostedPhase, HostedPhase, error) {
	a, b, err := buildHostedProgram(source, p, true)
	if err != nil {
		return HostedPhase{}, HostedPhase{}, err
	}
	a.DNS.Views = hostedUnitViews(p, a.DNS.Views)
	b.DNS.Views = hostedUnitViews(p, b.DNS.Views)
	return a, b, nil
}

func hostedUnitViews(p pc.HostedScopeProposal, views []pc.DNSQueryView) []pc.DNSQueryView {
	owned := map[[2]string]bool{}
	for _, r := range p.Records {
		owned[[2]string{r.Hostname, r.Type}] = true
	}
	var result []pc.DNSQueryView
	for _, v := range views {
		zone := v
		zone.Records = nil
		for _, r := range v.Records {
			if owned[[2]string{r.Name, r.Type}] {
				zone.Records = append(zone.Records, r)
			}
		}
		if len(zone.Records) > 0 {
			result = append(result, zone)
		}
	}
	return result
}
