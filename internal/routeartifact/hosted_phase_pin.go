package routeartifact

import (
	"errors"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/routeproof"
)

// These are content identities, not leases or receipts. In particular a
// concurrent release of another host's cache, routes or DNS must not become
// part of this host's independently withdrawable unit.
func HostedProgramDigests(source pc.HostedAddressSource, p pc.HostedScopeProposal) (prepared, forward string, err error) {
	a, b, err := BuildHostedProgramUnits(source, p)
	if err != nil {
		return "", "", err
	}
	return hostedProgramPins(p, a, b)
}

func hostedProgramPins(p pc.HostedScopeProposal, a, b HostedPhase) (prepared, forward string, err error) {
	prepared, err = hostedPhaseDigest(p, a)
	if err != nil {
		return "", "", err
	}
	forward, err = hostedPhaseDigest(p, b)
	if err == nil && prepared == forward {
		err = errors.New("host program does not change scoped configuration")
	}
	return
}

func hostedPhaseDigest(p pc.HostedScopeProposal, phase HostedPhase) (string, error) {
	type group struct {
		Paths map[string]string
		TLS   []model.EdgeTLSAllowlistEntry
		Cache []model.CachePolicy
	}
	groups := map[string]group{}
	for k, v := range phase.Groups {
		g := group{Paths: map[string]string{}}
		policies := map[string]bool{}
		for _, r := range v.Routes {
			if r.Hostname != p.Host {
				continue
			}
			d, err := routeproof.Digest(r)
			if err != nil {
				return "", err
			}
			g.Paths[model.NormalizeAppRoutePathPrefix(r.PathPrefix)] = d
			policies[r.CachePolicyID] = true
		}
		for _, c := range v.CachePolicies {
			if policies[c.ID] {
				g.Cache = append(g.Cache, c)
			}
		}
		for _, t := range v.TLSAllowlist {
			if t.Hostname == p.Host {
				g.TLS = append(g.TLS, t)
			}
		}
		groups[k] = g
	}
	owned := map[[2]string]bool{}
	for _, r := range p.Records {
		owned[[2]string{r.Hostname, r.Type}] = true
	}
	required := map[string]bool{}
	var targets []pc.DNSReadinessRecord
	for _, r := range phase.DNS.Plan.Records {
		copy := r
		copy.Targets = nil
		for _, t := range r.Targets {
			if owned[[2]string{r.Hostname, t.Family}] {
				copy.Targets = append(copy.Targets, t)
				for _, id := range t.ProbeIDs {
					required[id] = true
				}
			}
		}
		if len(copy.Targets) > 0 {
			targets = append(targets, copy)
		}
	}
	var probes []pc.DNSReadinessProbe
	for _, r := range phase.DNS.Plan.Probes {
		if r.Hostname == p.Host || required[r.ID] {
			probes = append(probes, r)
		}
	}
	var views []pc.DNSQueryView
	for _, v := range phase.DNS.Views {
		copy := v
		copy.Records = nil
		for _, r := range v.Records {
			if owned[[2]string{r.Name, r.Type}] {
				copy.Records = append(copy.Records, r)
			}
		}
		if len(copy.Records) > 0 {
			views = append(views, copy)
		}
	}
	var rules []pc.DNSAnswerRule
	for _, r := range phase.DNS.Rules {
		if owned[[2]string{r.Hostname, r.Type}] {
			rules = append(rules, r)
		}
	}
	return pc.Digest([]any{"hosted-phase-content/v1", p.Host, p.Before.Owner, p.Records, p.Physical, p.Eligible, groups, probes, targets, views, rules})
}
