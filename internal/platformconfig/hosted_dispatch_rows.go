package platformconfig

import (
	"errors"
	"maps"
	"slices"
	"strings"

	"fugue/internal/model"
)

type HostedDispatchRows struct {
	Owner    model.EdgeTLSAllowlistEntry `json:"owner"`
	Routes   []model.EdgeRouteIntent     `json:"routes"`
	Policies []model.CachePolicy         `json:"policies,omitempty"`
}

// Select exactly the paths in the approved plan, including its default target.
// Retain each projected route's own app, runtime, request and cache policies;
// changing the default route cannot transfer the TLS identity to its new app.
func SelectHostedDispatchRows(host, tenant, owner string, paths map[string]string, projected model.EdgeRouteIntentSnapshot, tls []model.EdgeTLSAllowlistEntry) (HostedDispatchRows, error) {
	bad := errors.New("host-only route projection differs from path plan or TLS ownership")
	if host == "" || host != normalizeHostname(host) || tenant == "" || owner == "" || paths["/"] == "" || len(paths) > 256 {
		return HostedDispatchRows{}, bad
	}
	readOwner := func(entries []model.EdgeTLSAllowlistEntry) (model.EdgeTLSAllowlistEntry, error) {
		var result model.EdgeTLSAllowlistEntry
		for _, v := range entries {
			if v.Hostname != host {
				continue
			}
			if result.Hostname != "" || v.AppID != owner || v.TenantID != tenant || v.Status != model.AppDomainStatusVerified {
				return result, bad
			}
			result = v
		}
		if result.Hostname == "" {
			return result, bad
		}
		return result, nil
	}
	a, e := readOwner(projected.TLSAllowlist)
	if e != nil {
		return HostedDispatchRows{}, e
	}
	b, e := readOwner(tls)
	if e != nil || a != b {
		return HostedDispatchRows{}, bad
	}
	result := HostedDispatchRows{Owner: a}
	actual, needed := map[string]string{}, map[string]bool{}
	for _, r := range projected.Routes {
		if r.Hostname != host {
			continue
		}
		// Reading the signed route is independent of tenant availability.
		// Preserve unavailable/disabled states verbatim. This source is
		// neither an availability observation nor an override of route policy.
		switch r.OriginStatus {
		case model.EdgeRouteStatusActive, model.EdgeRouteStatusDisabled, model.EdgeRouteStatusUnavailable, model.EdgeRouteStatusRuntimeMissing:
		default:
			return HostedDispatchRows{}, errors.Join(bad, errors.New("unrecognized signed origin state"))
		}
		if r.TenantID != tenant || r.AppID == "" || r.PathPrefix != model.NormalizeAppRoutePathPrefix(r.PathPrefix) || actual[r.PathPrefix] != "" || paths[r.PathPrefix] != r.AppID ||
			r.RoutePolicy == "" || model.NormalizeEdgeRoutePolicy(r.RoutePolicy) != r.RoutePolicy || r.Generation == "" {
			return HostedDispatchRows{}, bad
		}
		actual[r.PathPrefix] = r.AppID
		if r.CachePolicyID != "" {
			needed[r.CachePolicyID] = true
		}
		result.Routes = append(result.Routes, r)
	}
	if !maps.Equal(actual, paths) {
		return HostedDispatchRows{}, bad
	}
	for _, p := range projected.CachePolicies {
		if present, exists := needed[p.ID]; exists {
			if !present {
				return HostedDispatchRows{}, bad
			}
			needed[p.ID] = false
			result.Policies = append(result.Policies, p)
		}
	}
	for _, missing := range needed {
		if missing {
			return HostedDispatchRows{}, bad
		}
	}
	slices.SortFunc(result.Routes, func(a, b model.EdgeRouteIntent) int { return strings.Compare(a.PathPrefix, b.PathPrefix) })
	slices.SortFunc(result.Policies, func(a, b model.CachePolicy) int { return strings.Compare(a.ID, b.ID) })
	return result, nil
}
