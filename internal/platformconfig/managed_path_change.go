package platformconfig

import (
	"errors"
	"maps"
	"reflect"
	"slices"

	"fugue/internal/model"
)

// Remove independently deliverable path edits from an ordinary global
// refresh. Keep unrelated current intent. A retained path must still have a
// real current origin fact for its own app/runtime; never re-date a fact.
func stageSelectedManagedPaths(prior PlatformIntent, req CompileRequest, hosts []string) (CompileRequest, error) {
	if len(hosts) == 0 {
		return req, nil
	}
	bad := errors.New("cannot retain managed paths with current origin and domain ownership")
	valid := ManagedChangedHosts(prior, req.Intent)
	wanted := map[string]bool{}
	for _, h := range hosts {
		if !slices.Contains(valid, h) || wanted[h] {
			return CompileRequest{}, bad
		}
		wanted[h] = true
	}
	in := req.Intent
	next := in
	next.Routes = nil
	next.DNS = nil
	byOwner := map[[3]string][]RouteIntent{}
	for _, r := range in.Routes {
		if !wanted[r.Hostname] {
			next.Routes = append(next.Routes, r)
			continue
		}
		key := [3]string{r.Hostname, r.TenantID, r.AppID}
		byOwner[key] = append(byOwner[key], r)
	}
	for _, d := range in.DNS {
		if !wanted[d.Hostname] {
			next.DNS = append(next.DNS, d)
		} else if d.RecordKind != "hosted" || d.Type != "FUGUE_ROUTE" {
			return CompileRequest{}, bad
		}
	}
	for _, d := range prior.DNS {
		if wanted[d.Hostname] {
			if d.RecordKind != "hosted" || (d.Type != "FUGUE_APP" && d.Type != "FUGUE_ROUTE") {
				return CompileRequest{}, bad
			}
			next.DNS = append(next.DNS, d)
		}
	}
	facts := make(map[string]OriginObservation, len(req.RuntimeSnapshot.Origins))
	for _, f := range req.RuntimeSnapshot.Origins {
		if _, ok := facts[f.Ref]; ok || f.Ref == "" {
			return CompileRequest{}, bad
		}
		facts[f.Ref] = f
	}
	req.RuntimeSnapshot.Origins = slices.Clone(req.RuntimeSnapshot.Origins)
	if req.RuntimeSnapshot.CapturedAt == nil || req.RuntimeSnapshot.IntentGeneration != in.Generation {
		return CompileRequest{}, bad
	}
	for _, old := range prior.Routes {
		if !wanted[old.Hostname] {
			continue
		}
		bindings := byOwner[[3]string{old.Hostname, old.TenantID, old.AppID}]
		// Prefer an exact surviving path; another path may have genuinely
		// different cache/dispatch settings while sharing the same app.
		if i := slices.IndexFunc(bindings, func(r RouteIntent) bool { return r.PathPrefix == old.PathPrefix }); i >= 0 {
			bindings = bindings[i : i+1]
		}
		var current RouteIntent
		var proof OriginObservation
		for _, r := range bindings {
			ref, e := OriginObservationRef(r.Hostname, r.PathPrefix, r.AppID, r.RuntimeID)
			o, ok := facts[ref]
			if e != nil || !ok || r.OriginRef != ref || o.RuntimeID != r.RuntimeID || o.ObservedAt.IsZero() || o.ObservedAt.After(*req.RuntimeSnapshot.CapturedAt) {
				return CompileRequest{}, bad
			}
			oldRef, e := OriginObservationRef(old.Hostname, old.PathPrefix, old.AppID, old.RuntimeID)
			if e != nil || old.OriginRef != oldRef {
				return CompileRequest{}, bad
			}
			r.PathPrefix = old.PathPrefix
			r.OriginRef = oldRef
			shape := r
			shape.DeploymentGeneration, shape.CacheNamespace = old.DeploymentGeneration, old.CacheNamespace
			if !reflect.DeepEqual(shape, old) {
				return CompileRequest{}, bad
			}
			o.Ref = oldRef
			if current.AppID != "" && (!reflect.DeepEqual(current, r) || proof != o) {
				return CompileRequest{}, bad
			}
			current, proof = r, o
		}
		if current.AppID == "" {
			return CompileRequest{}, bad
		}
		if prev, ok := facts[proof.Ref]; ok && prev != proof {
			return CompileRequest{}, bad
		} else if !ok {
			req.RuntimeSnapshot.Origins = append(req.RuntimeSnapshot.Origins, proof)
			facts[proof.Ref] = proof
		}
		next.Routes = append(next.Routes, current)
	}
	if e := stageHostedAliasBindings(prior, in, next, wanted); e != nil {
		return CompileRequest{}, e
	}
	next = NormalizePlatformIntent(next)
	next.Generation = ""
	gen, e := PlatformIntentGeneration(next)
	if e != nil {
		return CompileRequest{}, e
	}
	next.Generation = gen
	if e = ValidatePlatformIntent(next); e != nil {
		return CompileRequest{}, e
	}
	req.Intent, req.RuntimeSnapshot.IntentGeneration = next, gen
	return req, nil
}

// Called only after per-host offers have been recorded. If the ordinary
// publisher cannot refresh an unrelated input it cannot consume either
// domain's route edits or invalidate their already-built proposals.
func IndependentManagedBaselineRequest(source HostedAddressSource, req CompileRequest, c *HostedTransitionCapture) (CompileRequest, error) {
	if c == nil || c.baseline == nil && len(c.StableHosts)+len(c.ExternalHosts) == 0 {
		return CompileRequest{}, errors.New("missing ordinary managed path capture")
	}
	if c.baseline != nil {
		req = *c.baseline
	}
	// An ordinary refresh must also retain any independently held path
	// change. Do not establish a new global reservation on this lane.
	hosts := ManagedChangedHosts(source.PriorIntent, req.Intent)
	stable, e := stageSelectedManagedPaths(source.PriorIntent, req, hosts)
	if e != nil {
		return CompileRequest{}, e
	}
	return stageBaselineExternalPaths(source.PriorIntent, stable, c.ExternalHosts)
}

func CompileIndependentManagedBaseline(source HostedAddressSource, stable CompileRequest) (CompileResult, error) {
	if len(source.Inherited) == 0 {
		return Compile(stable)
	}
	return compileWithInheritedCustody(stable, source)
}

// Discovery is based on two domain declarations, not on whether a machine
// happened to leave the DNS roster during this compilation. The actual
// compiler independently binds addresses, route rows and verified TLS.
func ManagedChangedHosts(before, after PlatformIntent) []string {
	if before.Scope != GlobalScopeKey || after.Scope != GlobalScopeKey || before.PublicationRole != "" || after.PublicationRole != "" {
		return nil
	}
	type item struct {
		wire         DNSIntent
		routes       map[string]string
		tls          *TLSIntent
		managed, bad bool
	}
	index := func(in PlatformIntent) map[string]*item {
		out := map[string]*item{}
		for _, d := range in.DNS {
			if d.RecordKind != "hosted" || (d.Type != "FUGUE_ROUTE" && d.Type != "FUGUE_APP") {
				continue
			}
			if x := out[d.Hostname]; x != nil {
				x.bad = true
				continue
			}
			if !validDNSConsumerZone(d.Hostname) || d.AppID == "" || d.TenantID == "" {
				continue
			}
			m := d.Type == "FUGUE_ROUTE"
			if m {
				if d.Route == nil || !d.Route.ManagedHost || validateDNSRouteConfiguration(d) != nil {
					continue
				}
				app := d.Route.DNSApplicationIntent
				d.Type = "FUGUE_APP"
				d.Route = nil
				d.Application = &app
			} else if validateDNSApplicationConfiguration(d) != nil {
				continue
			}
			d.Values = nil
			out[d.Hostname] = &item{wire: d, routes: map[string]string{}, managed: m}
		}
		for _, r := range in.Routes {
			if x := out[r.Hostname]; x != nil {
				p := model.NormalizeAppRoutePathPrefix(r.PathPrefix)
				if _, exists := x.routes[p]; exists || r.AppID == "" || r.TenantID != x.wire.TenantID {
					x.bad = true
				}
				x.routes[p] = r.AppID
			}
		}
		for i := range in.TLS {
			r := &in.TLS[i]
			if x := out[r.Hostname]; x != nil {
				if x.tls != nil || r.AppID != x.wire.AppID || r.TenantID != x.wire.TenantID {
					x.bad = true
				}
				x.tls = r
			}
		}
		return out
	}
	b, a := index(before), index(after)
	var result []string
	for h, v := range b {
		w := a[h]
		if w != nil && w.managed && !w.bad && !v.bad && v.tls != nil && reflect.DeepEqual(w.tls, v.tls) && reflect.DeepEqual(w.wire, v.wire) && v.routes["/"] != "" && w.routes["/"] != "" && !maps.Equal(v.routes, w.routes) {
			result = append(result, h)
		}
	}
	slices.Sort(result)
	return result
}
