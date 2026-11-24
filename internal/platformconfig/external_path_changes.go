package platformconfig

import (
	"errors"
	"maps"
	"reflect"
	"slices"
	"strings"

	"fugue/internal/model"
	"fugue/internal/routebinding"
	"fugue/internal/routeproof"
)

// Both ends must name the same platform target and certificate owner. A new
// domain, target transfer or certificate edit is not a path-only publication.
type externalPathChange struct {
	target        ExternalPathTarget
	before, after map[string]string
}

func externalPathChanges(before, after PlatformIntent) map[string]externalPathChange {
	type declaration struct {
		target ExternalPathTarget
		paths  map[string]string
		cert   *TLSIntent
		bad    bool
	}
	index := func(in PlatformIntent) map[string]*declaration {
		rows := map[string]*declaration{}
		if in.Scope != GlobalScopeKey {
			return rows
		}
		for _, r := range in.DNS {
			if r.Type != "FUGUE_ROUTE" || r.RecordKind != model.EdgeDNSRecordKindCustomDomainTarget || r.Route == nil {
				continue
			}
			members := slices.Clone(r.Route.Hostnames)
			slices.Sort(members)
			if len(members) != len(slices.Compact(slices.Clone(members))) {
				continue
			}
			for _, h := range members {
				if prior := rows[h]; prior != nil {
					prior.bad = true
					continue
				}
				rows[h] = &declaration{target: ExternalPathTarget{Hostname: h, Target: r.Hostname, AppID: r.AppID, TenantID: r.TenantID, Members: members},
					paths: map[string]string{}, bad: r.Route.ManagedHost || !validDNSConsumerZone(h) || !validDNSConsumerZone(r.Hostname) ||
						h == r.Hostname || strings.TrimSpace(r.AppID) == "" || strings.TrimSpace(r.TenantID) == ""}
			}
		}
		for _, r := range in.DNS {
			if d := rows[r.Hostname]; d != nil {
				d.bad = true // a managed owner, not an external wire name
			}
		}
		for _, r := range in.Routes {
			d := rows[r.Hostname]
			if d == nil || d.bad {
				continue
			}
			path := model.NormalizeAppRoutePathPrefix(r.PathPrefix)
			if path != r.PathPrefix || r.AppID == "" || r.TenantID != d.target.TenantID || d.paths[path] != "" || len(d.paths) >= 256 {
				d.bad = true
				continue
			}
			d.paths[path] = r.AppID
		}
		for i := range in.TLS {
			c := &in.TLS[i]
			if d := rows[c.Hostname]; d != nil {
				if d.cert != nil || c.AppID != d.target.AppID || c.TenantID != d.target.TenantID || c.DomainRef == "" {
					d.bad = true
				}
				d.cert = c
			}
		}
		return rows
	}
	left, right := index(before), index(after)
	out := map[string]externalPathChange{}
	for h, a := range left {
		b := right[h]
		if b == nil || a.bad || b.bad || !reflect.DeepEqual(a.target, b.target) || a.cert == nil || !reflect.DeepEqual(a.cert, b.cert) ||
			a.paths["/"] == "" || b.paths["/"] == "" || maps.Equal(a.paths, b.paths) {
			continue
		}
		out[h] = externalPathChange{a.target, a.paths, b.paths}
	}
	return out
}

func ExternalChangedHosts(before, after PlatformIntent) []string {
	return slices.Sorted(maps.Keys(externalPathChanges(before, after)))
}

func buildExternalPathProposal(src HostedAddressSource, original, desired hostProposalSide, change externalPathChange) (HostedScopeProposal, error) {
	var empty HostedScopeProposal
	bad := errors.New("external path proposal differs from selected DNS target or route owner")
	target := change.target
	shape, members, err := externalPathShape(target, original.dns)
	if err != nil || !reflect.DeepEqual(original.dns.Policy.DNSReadiness, desired.dns.Policy.DNSReadiness) {
		return empty, errors.Join(bad, err)
	}
	_, delta, err := ComposeExternalPathRequirements(target, original.dns.ReadinessPlan, desired.dns.ReadinessPlan, original.dns.Policy.DNSReadiness,
		slices.Sorted(maps.Keys(change.before)), slices.Sorted(maps.Keys(change.after)), original.dns.Records, desired.dns.Records)
	if err != nil {
		return empty, err
	}
	build := func(side hostProposalSide, paths map[string]string) (HostedDispatchRows, []DNSReadinessProbe, error) {
		rows, e := SelectHostedDispatchRows(target.Hostname, target.TenantID, target.AppID, paths, side.routes, side.certs)
		if e != nil {
			return rows, nil, e
		}
		routes := map[string]model.EdgeRouteIntent{}
		for _, r := range rows.Routes {
			routes[r.PathPrefix] = r
		}
		var own []DNSReadinessProbe
		for _, pr := range side.dns.ReadinessPlan.Probes {
			if pr.Hostname != target.Hostname {
				continue
			}
			r, found := routes[pr.Path]
			if !found {
				return rows, nil, bad
			}
			d, e := routeproof.Digest(routebinding.FromIntent(r, pr.EdgeGroupID))
			if e != nil || d != pr.RouteDigest {
				return rows, nil, errors.Join(bad, e)
			}
			own = append(own, pr)
		}
		return rows, own, nil
	}
	before, baseProbes, e := build(original, change.before)
	if e != nil {
		return empty, e
	}
	after, pathProbes, e := build(desired, change.after)
	if e != nil || before.Owner != after.Owner {
		return empty, errors.Join(bad, e)
	}
	offer := HostedScopeProposal{Schema: ExternalPathProposalSchema, Host: target.Hostname, Base: src.Publication, Authority: src.Authority, ExternalPath: &ExternalPathAnchor{Target: target, Shape: shape},
		Before: before, Prepared: before, After: after, BaseProbes: baseProbes, HoldProbes: baseProbes, PathProbes: pathProbes, Physical: members, Eligible: slices.Clone(members), Changes: delta.Edits}
	for _, r := range original.dns.Records {
		if r.Hostname == target.Target && (r.Type == "A" || r.Type == "AAAA") {
			offer.Records = append(offer.Records, r)
		}
	}
	if _, err := ComposeHostedDNSPhase(original.dns, offer, "prepare"); err != nil {
		return empty, err
	}
	return offer, nil
}
