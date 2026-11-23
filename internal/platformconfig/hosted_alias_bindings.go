package platformconfig

import (
	"errors"
	"reflect"
	"slices"

	"fugue/internal/model"
)

// Update the path dependencies of a still-declared alias, not the alias's
// wire record. An absent alias is never appended. Both the previous and the
// desired binding sets must describe their respective real route inputs.
func stageHostedAliasBindings(prior, desired, stage PlatformIntent, hosts map[string]bool) error {
	bad := errors.New("hosted intermediate cannot preserve the signed external DNS route binding")
	indexRoutes := func(i PlatformIntent) map[string][]CompiledRoute {
		m := map[string][]CompiledRoute{}
		for _, r := range i.Routes {
			m[r.Hostname] = append(m[r.Hostname], CompiledRoute{RouteIntent: r})
		}
		return m
	}
	oldRoutes, newRoutes, stagedRoutes := indexRoutes(prior), indexRoutes(desired), indexRoutes(stage)
	prev := map[string]DNSIntent{}
	for _, r := range prior.DNS {
		if r.Route == nil {
			continue
		}
		if _, dup := prev[r.Hostname]; dup {
			return bad
		}
		prev[r.Hostname] = r
	}
	deps := func(r DNSIntent, index map[string][]CompiledRoute) ([]CompiledRoute, error) {
		var rows []CompiledRoute
		for _, h := range r.Route.Hostnames {
			rr := index[h]
			if len(rr) == 0 || len(rows)+len(rr) > 4096 {
				return nil, bad
			}
			rows = append(rows, rr...)
		}
		return rows, nil
	}
	for i, r := range stage.DNS {
		if r.Route == nil || hosts[r.Hostname] || !slices.ContainsFunc(r.Route.Hostnames, func(h string) bool { return hosts[h] }) {
			continue
		}
		old, ok := prev[r.Hostname]
		if !ok || r.Route.ManagedHost || r.AppID == "" || r.TenantID == "" || validateDNSRouteConfiguration(old) != nil || validateDNSRouteConfiguration(r) != nil {
			return bad
		}
		a, b := *old.Route, *r.Route
		a.Bindings, b.Bindings = nil, nil
		left, right := old, r
		left.Route, right.Route = &a, &b
		if !reflect.DeepEqual(left, right) {
			return bad
		}
		oldRows, e1 := deps(old, oldRoutes)
		newRows, e2 := deps(r, newRoutes)
		rows, e3 := deps(r, stagedRoutes)
		if e1 != nil || e2 != nil || e3 != nil || ValidateDNSRouteOwners(old, oldRows) != nil || ValidateDNSRouteOwners(r, newRows) != nil {
			return bad
		}
		// Only replace bindings of the host being staged. All other bindings
		// (including other apps sharing the alias) come from current intent.
		b = *r.Route
		b.Bindings = slices.DeleteFunc(slices.Clone(b.Bindings), func(v DNSRouteBinding) bool { return hosts[v.Hostname] })
		if len(r.Route.Bindings) > 0 {
			for _, v := range rows {
				if hosts[v.Hostname] {
					b.Bindings = append(b.Bindings, DNSRouteBinding{Hostname: v.Hostname, PathPrefix: model.NormalizeAppRoutePathPrefix(v.PathPrefix), AppID: v.AppID})
				}
			}
		}
		r.Route = &b
		if ValidateDNSRouteOwners(r, rows) != nil {
			return bad
		}
		stage.DNS[i] = r
	}
	return nil
}
