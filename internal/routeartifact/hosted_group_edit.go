package routeartifact

import (
	"encoding/json"
	"errors"
	"maps"
	"reflect"
	"slices"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/routeproof"
)

// HostedGroupContent contains executor input, not an envelope. In particular a
// new set of paths must never inherit the old bundle's signature or authority.
type HostedGroupContent struct {
	Routes        []model.EdgeRouteBinding
	CachePolicies []model.CachePolicy
	TLSAllowlist  []model.EdgeTLSAllowlistEntry
}

// ComposeHostedGroupEdit performs a semantic compare-and-replace of one
// host in an already authenticated physical group's bundle. The publisher
// must validate proposal and base selection, intent, expiration, and fences.
// "withdraw" restores the prepared paths, keeping the address hold in place.
func ComposeHostedGroupEdit(base model.EdgeRouteBundle, proposal pc.HostedScopeProposal, phase string) (HostedGroupContent, error) {
	fail := errors.New("host group edit does not match the selected ingress, paths or shared dependencies")
	var empty HostedGroupContent
	group, host := base.EdgeGroupID, proposal.Host
	owner := proposal.Before.Owner
	if proposal.Schema != pc.HostedProposalSchema || proposal.Base.Validate() != nil || host == "" || owner.Hostname != host || owner.AppID == "" || owner.TenantID == "" ||
		owner != proposal.Prepared.Owner || owner != proposal.After.Owner ||
		!slices.ContainsFunc(proposal.Physical, func(n pc.HostedAddressHold) bool { return n.EdgeGroupID == group && n.Address != "" && n.EdgeID != "" }) {
		return empty, fail
	}
	from, to := proposal.Prepared, proposal.After
	switch phase {
	case "prepare":
		from, to = proposal.Before, proposal.Prepared
	case "forward":
	case "withdraw":
		from, to = proposal.After, proposal.Prepared
	default:
		return empty, fail
	}
	return PreviewHostedGroupRows(base, host, from, to)
}

// PreviewHostedGroupRows is deliberately unable to issue a release, even
// when its inputs are public diagnostic projections with no fencing token.
func PreviewHostedGroupRows(base model.EdgeRouteBundle, host string, from, to pc.HostedDispatchRows) (HostedGroupContent, error) {
	fail := errors.New("host group content does not match its predecessor")
	var empty HostedGroupContent
	group, owner := base.EdgeGroupID, from.Owner
	if len(from.Routes) == 0 || owner != to.Owner || owner.Hostname != host {
		return empty, fail
	}
	n := 0
	for _, o := range base.TLSAllowlist {
		if o.Hostname == host {
			if o != owner {
				return empty, fail
			}
			n++
		}
	}
	if n != 1 {
		return empty, fail
	}
	render := func(rows pc.HostedDispatchRows) (model.EdgeRouteBundle, error) {
		paths := map[string]string{}
		for _, r := range rows.Routes {
			if r.Hostname != host || r.TenantID != owner.TenantID || r.AppID == "" || paths[r.PathPrefix] != "" {
				return model.EdgeRouteBundle{}, fail
			}
			paths[r.PathPrefix] = r.AppID
		}
		s := model.EdgeRouteIntentSnapshot{Generation: from.Routes[0].Generation, Routes: rows.Routes, CachePolicies: rows.Policies, TLSAllowlist: []model.EdgeTLSAllowlistEntry{owner}}
		normalized, err := pc.SelectHostedDispatchRows(host, owner.TenantID, owner.AppID, paths, s, s.TLSAllowlist)
		if err != nil || len(normalized.Policies) != len(rows.Policies) {
			return model.EdgeRouteBundle{}, errors.Join(fail, err)
		}
		b, err := MaterializeSnapshotForGroup(s, group)
		if err != nil || len(b.Routes) != len(paths) {
			return model.EdgeRouteBundle{}, errors.Join(fail, err)
		}
		return b, nil
	}
	old, err := render(from)
	if err != nil {
		return empty, err
	}
	next, err := render(to)
	if err != nil {
		return empty, err
	}
	proofs := func(routes []model.EdgeRouteBinding) (map[string]string, error) {
		m := map[string]string{}
		for _, r := range routes {
			if r.Hostname != host {
				continue
			}
			p := model.NormalizeAppRoutePathPrefix(r.PathPrefix)
			if m[p] != "" || r.TenantID != owner.TenantID || r.EdgeGroupID != group {
				return nil, fail
			}
			d, e := routeproof.Digest(r)
			if e != nil {
				return nil, e
			}
			m[p] = d
		}
		return m, nil
	}
	oldProof, err := proofs(old.Routes)
	if err != nil {
		return empty, err
	}
	current, err := proofs(base.Routes)
	if err != nil || !maps.Equal(current, oldProof) {
		return empty, errors.Join(fail, err)
	}
	foreign := map[string]bool{}
	result := HostedGroupContent{TLSAllowlist: base.TLSAllowlist}
	inserted := false
	for _, r := range base.Routes {
		if r.Hostname == host {
			if !inserted {
				result.Routes = append(result.Routes, next.Routes...)
				inserted = true
			}
			continue
		}
		foreign[r.CachePolicyID] = true
		result.Routes = append(result.Routes, r)
	}
	live := map[string]model.CachePolicy{}
	for _, p := range base.CachePolicies {
		if _, exists := live[p.ID]; exists || p.ID == "" {
			return empty, fail
		}
		live[p.ID] = p
	}
	before := map[string]bool{}
	for _, p := range old.CachePolicies {
		v, ok := live[p.ID]
		if !ok || !reflect.DeepEqual(v, p) {
			return empty, fail
		}
		before[p.ID] = true
	}
	after := map[string]bool{}
	for _, p := range next.CachePolicies {
		v, ok := live[p.ID]
		if !ok && foreign[p.ID] {
			return empty, fail
		}
		if ok && !reflect.DeepEqual(v, p) && (foreign[p.ID] || !before[p.ID]) {
			return empty, fail
		}
		after[p.ID] = true
		live[p.ID] = p
	}
	for id := range before {
		if !after[id] && !foreign[id] {
			delete(live, id)
		}
	}
	for _, id := range slices.Sorted(maps.Keys(live)) {
		result.CachePolicies = append(result.CachePolicies, live[id])
	}
	// No aliased nested policy maps, upstream lists, or time pointers.
	wire, err := json.Marshal(result)
	if err != nil {
		return empty, err
	}
	var detached HostedGroupContent
	if err = json.Unmarshal(wire, &detached); err != nil {
		return empty, err
	}
	return detached, nil
}
