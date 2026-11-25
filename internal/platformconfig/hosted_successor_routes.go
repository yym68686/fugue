package platformconfig

import (
	"encoding/json"
	"errors"
	"maps"
	"reflect"

	"fugue/internal/model"
)

// Route rows and DNS continuity markers in the established global compiler
// wire format. Unlike a per-group Worker bundle, these can be used to build
// a new ordinary route artifact. They deliberately carry no prior signature,
// release ID, lineage, edge-selection grant, or permission to advance a
// consumer. The release compiler must separately compute grants from the
// composed intent, policy, routes and topology, including shared DNS deps.
type HostedSuccessorRoutes struct {
	Rows            []json.RawMessage
	ManagedBindings []json.RawMessage
}

// ComposeHostedSuccessorRoutes works on selected compiler output, not
// diagnostic projections. Apply the ordinary reader to the prospective rows
// under the predecessor's actual shared policy and validate what it projects.
// An unrelated app can have been redeployed since the proposal; withdrawal
// preserves its current bytes, it does not restore an old global artifact.
func ComposeHostedSuccessorRoutes(base, target model.PlatformArtifact, p HostedScopeProposal, phase string) (HostedSuccessorRoutes, error) {
	var empty HostedSuccessorRoutes
	bad := errors.New("cannot compose host successor from independently bound route rows")
	host, owner := p.Host, p.Before.Owner
	if !p.SupportedSchema() || p.Base.Validate() != nil || host == "" || owner.Hostname != host || owner != p.After.Owner || owner != p.Prepared.Owner ||
		base.ArtifactKind != model.PlatformArtifactKindEdgeRouteBundle || target.ArtifactKind != base.ArtifactKind ||
		base.Scope.Key != GlobalScopeKey || target.Scope.Key != GlobalScopeKey ||
		base.ScopeKey != "" && base.ScopeKey != GlobalScopeKey || target.ScopeKey != "" && target.ScopeKey != GlobalScopeKey {
		return empty, bad
	}
	from, to := p.Prepared, p.After
	switch phase {
	case "prepare":
		if base.ID != p.Base.RouteArtifactID || base.ContentHash != p.Base.RouteArtifactDigest {
			return empty, bad
		}
		from, to = p.Before, p.Prepared
	case "forward":
	case "withdraw":
		from, to = p.After, p.Prepared
	default:
		return empty, bad
	}
	prove := func(a model.PlatformArtifact, want HostedDispatchRows) error {
		s, err := ProjectRouteArtifact(a)
		if err != nil {
			return err
		}
		paths := map[string]string{}
		for _, r := range want.Routes {
			paths[r.PathPrefix] = r.AppID
		}
		actual, err := SelectHostedDispatchRows(host, owner.TenantID, owner.AppID, paths, s, s.TLSAllowlist)
		if err != nil || !reflect.DeepEqual(want, actual) {
			return errors.Join(bad, err)
		}
		return nil
	}
	if err := prove(base, from); err != nil {
		return empty, err
	}
	if err := prove(target, to); err != nil {
		return empty, err
	}
	read := func(a model.PlatformArtifact, field string) ([]json.RawMessage, error) {
		if _, ok := a.Content[field]; !ok && field == managedHostDNSBindingsField {
			return nil, nil
		}
		b, err := json.Marshal(a.Content[field])
		var rows []json.RawMessage
		if err != nil || json.Unmarshal(b, &rows) != nil || rows == nil {
			return nil, bad
		}
		return rows, nil
	}
	merge := func(field string) ([]json.RawMessage, error) {
		var foreign, selected []json.RawMessage
		insert := -1
		for _, src := range []struct {
			a      model.PlatformArtifact
			target bool
		}{{base, false}, {target, true}} {
			rows, e := read(src.a, field)
			if e != nil {
				return nil, e
			}
			for _, raw := range rows {
				var k struct{ Hostname string }
				if json.Unmarshal(raw, &k) != nil || k.Hostname == "" || k.Hostname != normalizeHostname(k.Hostname) {
					return nil, bad
				}
				if src.target && k.Hostname == host {
					selected = append(selected, raw)
				}
				if !src.target && k.Hostname != host {
					foreign = append(foreign, raw)
				} else if !src.target && insert < 0 {
					insert = len(foreign)
				}
			}
		}
		if field == "routes" && len(selected) != len(to.Routes) {
			return nil, bad
		}
		if field == managedHostDNSBindingsField && len(selected) > 1 {
			return nil, bad
		}
		if insert < 0 {
			insert = len(foreign)
		}
		out := make([]json.RawMessage, 0, len(foreign)+len(selected))
		out = append(out, foreign[:insert]...)
		out = append(out, selected...)
		return append(out, foreign[insert:]...), nil
	}
	// Verify continuity marker (including its absence on a sole-root
	// predecessor) separately from the executable rows.
	markerOK := func(a model.PlatformArtifact, rows HostedDispatchRows) bool {
		all, e := readManagedHostDNSBindings(a)
		if e != nil {
			return false
		}
		m, ok := all[host]
		if !ok {
			return len(rows.Routes) == 1 && rows.Routes[0].PathPrefix == "/" && rows.Routes[0].AppID == owner.AppID
		}
		if m.Hostname != host || m.TenantID != owner.TenantID || m.AppID != owner.AppID || len(m.Bindings) != len(rows.Routes) {
			return false
		}
		wanted := map[string]string{}
		for _, r := range rows.Routes {
			wanted[r.PathPrefix] = r.AppID
		}
		for _, v := range m.Bindings {
			if v.Hostname != host || v.AppID == "" || wanted[v.PathPrefix] != v.AppID {
				return false
			}
			delete(wanted, v.PathPrefix)
		}
		return len(wanted) == 0
	}
	if !markerOK(base, from) || !markerOK(target, to) {
		return empty, bad
	}
	rows, e := merge("routes")
	if e != nil {
		return empty, e
	}
	bindings, e := merge(managedHostDNSBindingsField)
	if e != nil {
		return empty, e
	}
	projected := model.PlatformArtifact{Content: maps.Clone(base.Content)}
	delete(projected.Content, "edge_selection_grants") // never adopt the prior grant set as fresh authority
	projected.Content["routes"] = rows
	if len(bindings) > 0 {
		projected.Content[managedHostDNSBindingsField] = bindings
	} else {
		delete(projected.Content, managedHostDNSBindingsField)
	}
	if e := prove(projected, to); e != nil {
		return empty, e
	}
	return HostedSuccessorRoutes{rows, bindings}, nil
}
