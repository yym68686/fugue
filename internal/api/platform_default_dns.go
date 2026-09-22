package api

import (
	"fmt"
	"sort"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

// Only the frozen App's default hostname authorizes implicit DNS. An HTTP
// project route alias needs its own DNS declaration or verified domain binding.
func projectDefaultAppDNS(result *platformIntentProjectionResponse, apps map[string]model.App, appBaseDomain string, ttl int) error {
	return projectDefaultAppDNSWithTTL(result, apps, appBaseDomain, edgeDNSPolicyTTL(ttl))
}

func projectDefaultAppDNSWithTTL(result *platformIntentProjectionResponse, apps map[string]model.App, appBaseDomain string, ttl int) error {
	if ttl < 1 || ttl > 86400 {
		return fmt.Errorf("default application DNS TTL invalid")
	}
	base := normalizeExternalAppDomain(appBaseDomain)
	if base == "" {
		return nil
	}
	existing := map[string]bool{}
	for _, record := range result.Intent.DNS {
		existing[record.Hostname] = true
	}
	routes := map[string]platformconfig.RouteIntent{}
	for _, route := range result.Intent.Routes {
		if route.AppID == "" || route.TenantID == "" || route.Hostname == "" || !edgeDNSTargetWithinZone(route.Hostname, base) || existing[route.Hostname] {
			continue
		}
		if route.PathPrefix != "/" && route.PathPrefix != "" {
			continue
		}
		app, ok := apps[route.AppID]
		if !ok || app.ID != route.AppID || app.TenantID != route.TenantID || app.Route == nil ||
			normalizeExternalAppDomain(app.Route.Hostname) != route.Hostname {
			continue
		}
		if prior, ok := routes[route.Hostname]; ok && (prior.AppID != route.AppID || prior.TenantID != route.TenantID) {
			return fmt.Errorf("default app DNS route owner is ambiguous")
		}
		routes[route.Hostname] = route
	}
	keys := make([]string, 0, len(routes))
	for host := range routes {
		keys = append(keys, host)
	}
	sort.Strings(keys)
	for _, host := range keys {
		route := routes[host]
		result.Intent.DNS = append(result.Intent.DNS, platformconfig.DNSIntent{Hostname: host, Type: "FUGUE_ROUTE", Values: []string{}, TTL: ttl, RecordKind: model.EdgeDNSRecordKindPlatform, AppID: route.AppID, TenantID: route.TenantID, Status: route.Status, Route: &platformconfig.DNSRouteIntent{Hostnames: []string{host}, DNSApplicationIntent: platformconfig.DNSApplicationIntent{IPv4Policy: "auto", IPv6Policy: "auto", TTLPolicy: "record", FallbackPolicy: "fail_closed"}}})
	}
	if len(keys) == 0 {
		return nil
	}
	result.Intent = platformconfig.NormalizePlatformIntent(result.Intent)
	result.Intent.Generation = ""
	gen, err := platformconfig.PlatformIntentGeneration(result.Intent)
	if err != nil {
		return err
	}
	result.Intent.Generation = gen
	result.RuntimeSnapshot.IntentGeneration = gen
	return nil
}
