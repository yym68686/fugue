package platformconfig

import (
	"errors"
	"fmt"
	"reflect"
	"slices"

	"fugue/internal/model"
)

// StageHostedPathBaseline builds an intermediate input, never a stored
// tenant edit. While the current-signed root still serves, first publish the
// separately authorized address hold with that exact root. A later path-only
// release then has a rollback target which will not re-offer the held edge.
// Do not revive a withdrawn App, copy a stale deployment, or guess at TLS.
func StageHostedPathBaseline(prior, desired PlatformIntent, hosts []string) (PlatformIntent, error) {
	bad := errors.New("cannot stage hosted path baseline within current ownership and live route intent")
	if len(hosts) == 0 || len(hosts) > 4096 || prior.Scope != GlobalScopeKey || desired.Scope != prior.Scope || prior.PublicationRole != "" || desired.PublicationRole != "" {
		return PlatformIntent{}, bad
	}
	wanted := map[string]bool{}
	type binding struct {
		routes  []RouteIntent
		records []DNSIntent
		tls     []TLSIntent
	}
	for _, host := range hosts {
		if !validDNSConsumerZone(host) || wanted[host] {
			return PlatformIntent{}, bad
		}
		wanted[host] = true
	}
	view := func(in PlatformIntent) map[string]*binding {
		out := map[string]*binding{}
		for h := range wanted {
			out[h] = &binding{}
		}
		for _, r := range in.Routes {
			if b := out[r.Hostname]; b != nil {
				b.routes = append(b.routes, r)
			}
		}
		for _, r := range in.DNS {
			if b := out[r.Hostname]; b != nil {
				b.records = append(b.records, r)
			}
		}
		for _, r := range in.TLS {
			if b := out[r.Hostname]; b != nil {
				b.tls = append(b.tls, r)
			}
		}
		return out
	}
	before, after := view(prior), view(desired)
	out := desired
	out.Routes = slices.DeleteFunc(slices.Clone(desired.Routes), func(r RouteIntent) bool { return wanted[r.Hostname] })
	out.DNS = slices.DeleteFunc(slices.Clone(desired.DNS), func(r DNSIntent) bool { return wanted[r.Hostname] })
	for _, h := range hosts {
		a, b := before[h], after[h]
		if len(a.routes) != 1 || len(b.routes) < 2 || len(a.records) != 1 || len(b.records) != 1 || len(a.tls) != 1 || !reflect.DeepEqual(a.tls, b.tls) {
			return PlatformIntent{}, fmt.Errorf("%w: root/DNS/TLS shape", bad)
		}
		old, new := a.records[0], b.records[0]
		if old.RecordKind != "hosted" || old.Type != "FUGUE_APP" || old.Application == nil || old.Route != nil || new.Type != "FUGUE_ROUTE" || new.Route == nil || !new.Route.ManagedHost || new.Application != nil ||
			validateDNSApplicationConfiguration(old) != nil || validateDNSRouteConfiguration(new) != nil || !reflect.DeepEqual(*old.Application, new.Route.DNSApplicationIntent) {
			return PlatformIntent{}, fmt.Errorf("%w: managed DNS placement policy", bad)
		}
		base, change := old, new
		base.Type, change.Type = "", ""
		base.Values, change.Values = nil, nil
		base.Application, change.Route = nil, nil
		if !reflect.DeepEqual(base, change) || old.AppID == "" || old.TenantID == "" || a.tls[0].AppID != old.AppID || a.tls[0].TenantID != old.TenantID {
			return PlatformIntent{}, fmt.Errorf("%w: DNS or TLS owner", bad)
		}
		root := a.routes[0]
		active := func(r RouteIntent) bool {
			return r.Enabled && (r.Status == "" || r.Status == model.EdgeRouteStatusActive) && model.EdgeRoutePolicyAllowsTraffic(r.RoutePolicy)
		}
		if !active(root) || root.AppID != old.AppID || root.TenantID != old.TenantID || model.NormalizeAppRoutePathPrefix(root.PathPrefix) != "/" {
			return PlatformIntent{}, fmt.Errorf("%w: original active root", bad)
		}
		ref, err := OriginObservationRef(h, root.PathPrefix, root.AppID, root.RuntimeID)
		if err != nil || root.RuntimeID == "" || ref != root.OriginRef {
			return PlatformIntent{}, fmt.Errorf("%w: original root origin identity", bad)
		}
		matches := false
		var currentRoot RouteIntent
		byPath := map[string]string{}
		for _, r := range b.routes {
			path := model.NormalizeAppRoutePathPrefix(r.PathPrefix)
			if !active(r) || r.AppID == "" || r.TenantID != old.TenantID || byPath[path] != "" {
				return PlatformIntent{}, fmt.Errorf("%w: current path authority", bad)
			}
			byPath[path] = r.AppID
			if r.AppID == root.AppID {
				// Retain only the routing shape, never the previous deployment.
				// The currently captured same-owner binding supplies all three
				// deployment-scoped fields as a unit. Normal ResolveRouteOrigins
				// must still bind that origin to the tenant/app and generation.
				origin, err := OriginObservationRef(h, path, r.AppID, r.RuntimeID)
				if err != nil || origin != r.OriginRef {
					return PlatformIntent{}, fmt.Errorf("%w: current path origin identity", bad)
				}
				r.PathPrefix, r.OriginRef = root.PathPrefix, root.OriginRef
				shape := r
				shape.OriginRef, shape.DeploymentGeneration, shape.CacheNamespace = root.OriginRef, root.DeploymentGeneration, root.CacheNamespace
				if !reflect.DeepEqual(root, shape) || matches && !reflect.DeepEqual(currentRoot, r) {
					return PlatformIntent{}, fmt.Errorf("%w: inconsistent current owner binding", bad)
				}
				currentRoot = r
				matches = true
			}
		}
		if !matches || byPath["/"] == "" || len(byPath) != len(new.Route.Bindings) {
			return PlatformIntent{}, bad
		}
		for _, v := range new.Route.Bindings {
			if v.Hostname != h || byPath[v.PathPrefix] != v.AppID {
				return PlatformIntent{}, bad
			}
		}
		out.Routes = append(out.Routes, currentRoot)
		out.DNS = append(out.DNS, old)
	}
	if err := stageHostedAliasBindings(prior, desired, out, wanted); err != nil {
		return PlatformIntent{}, err
	}
	out = NormalizePlatformIntent(out)
	out.Generation = ""
	gen, err := PlatformIntentGeneration(out)
	if err != nil {
		return PlatformIntent{}, err
	}
	out.Generation = gen
	if err := ValidatePlatformIntent(out); err != nil {
		return PlatformIntent{}, err
	}
	return out, nil
}

// StageHostedOriginFacts projects *current app-runtime observations*, never
// route or TLS proofs, onto the intermediate root's canonical key. Verify the
// key and the complete observation for every current binding of that owner.
// All observations must agree, including observation time and negative status.
// No historical origin, renewed timestamp or inferred successful request.
func StageHostedOriginFacts(desired, stage PlatformIntent, in RuntimeSnapshot, hosts []string) (RuntimeSnapshot, error) {
	bad := errors.New("cannot bind staged root to exact current origin observations")
	if len(hosts) == 0 || len(hosts) > 4096 || in.IntentGeneration != desired.Generation || in.CapturedAt == nil || in.CapturedAt.IsZero() {
		return RuntimeSnapshot{}, bad
	}
	facts := map[string]OriginObservation{}
	for _, o := range in.Origins {
		if _, ok := facts[o.Ref]; ok || o.Ref == "" {
			return RuntimeSnapshot{}, bad
		}
		facts[o.Ref] = o
	}
	out := in
	out.Origins = slices.Clone(in.Origins)
	roots := map[string]RouteIntent{}
	paths := map[string][]RouteIntent{}
	for _, host := range hosts {
		if _, exists := paths[host]; exists {
			return RuntimeSnapshot{}, bad
		}
		paths[host] = nil
	}
	for _, r := range stage.Routes {
		if _, ok := paths[r.Hostname]; ok {
			if _, dup := roots[r.Hostname]; dup || model.NormalizeAppRoutePathPrefix(r.PathPrefix) != "/" {
				return RuntimeSnapshot{}, bad
			}
			roots[r.Hostname] = r
		}
	}
	for _, r := range desired.Routes {
		if _, ok := paths[r.Hostname]; ok {
			paths[r.Hostname] = append(paths[r.Hostname], r)
		}
	}
	for _, host := range hosts {
		root := roots[host]
		rr, e := OriginObservationRef(host, "/", root.AppID, root.RuntimeID)
		if e != nil || root.AppID == "" || root.RuntimeID == "" || root.TenantID == "" || root.OriginRef != rr {
			return RuntimeSnapshot{}, bad
		}
		var witness OriginObservation
		for _, r := range paths[host] {
			if r.AppID != root.AppID {
				continue
			}
			ref, e := OriginObservationRef(host, r.PathPrefix, r.AppID, r.RuntimeID)
			o, ok := facts[ref]
			if e != nil || !ok || r.OriginRef != ref || r.RuntimeID != root.RuntimeID || r.TenantID != root.TenantID || o.RuntimeID != r.RuntimeID || o.ObservedAt.IsZero() || o.ObservedAt.After(*in.CapturedAt) {
				return RuntimeSnapshot{}, bad
			}
			o.Ref = rr
			if witness.Ref != "" && witness != o {
				return RuntimeSnapshot{}, bad
			}
			witness = o
		}
		if witness.Ref == "" {
			return RuntimeSnapshot{}, bad
		}
		if old, ok := facts[rr]; ok {
			if old != witness {
				return RuntimeSnapshot{}, bad
			}
		} else {
			facts[rr] = witness
			out.Origins = append(out.Origins, witness)
		}
	}
	out.IntentGeneration = stage.Generation
	return out, nil
}
