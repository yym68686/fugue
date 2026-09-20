package api

import (
	"sort"
	"strings"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"github.com/miekg/dns"
)

// A DNS target serves all of its verified aliases, regardless of who manages
// the aliases' external zones. Health is never used to edit desired membership.
// The collector must subsequently prove every referenced hostname and path.
func (s *Server) projectCustomDomainDNS(result *platformIntentProjectionResponse, domains []model.AppDomain, apps map[string]model.App) error {
	return projectCustomDomainDNSWithDomains(result, domains, apps, s.legacyApplicationDomains())
}

func projectCustomDomainDNSWithDomains(result *platformIntentProjectionResponse, domains []model.AppDomain, apps map[string]model.App, domainsConfig applicationDomainConfig) error {
	routes := map[string][]platformconfig.RouteIntent{}
	for _, route := range result.Intent.Routes {
		routes[route.Hostname] = append(routes[route.Hostname], route)
	}
	existing := map[string][]platformconfig.DNSIntent{}
	for _, record := range result.Intent.DNS {
		existing[record.Hostname] = append(existing[record.Hostname], record)
	}
	type targetIntent struct {
		app     model.App
		shared  bool
		hosts   map[string]bool
		blocked bool
	}
	targets := map[string]*targetIntent{}
	issue := func(code, host, reason string) {
		result.Issues = append(result.Issues, platformProjectionIssue{Code: code, Hostname: host, Reason: reason})
	}
	add := func(target, host string, app model.App, domainOwner string) {
		_, valid := dns.IsDomainName(dns.Fqdn(target))
		if target == "" || !valid || strings.ContainsAny(target, "* \t\r\n") ||
			(!edgeDNSTargetWithinZone(target, domainsConfig.AppBaseDomain) && !edgeDNSTargetWithinZone(target, domainsConfig.CustomDomainBaseDomain)) {
			issue("dns_custom_domain_target_invalid", host, "target is not a name in configured authoritative base domains")
			return
		}
		t := targets[target]
		if t == nil {
			t = &targetIntent{app: app, hosts: map[string]bool{}}
			targets[target] = t
		}
		t.hosts[host] = true
		if app.ID == "" || app.TenantID == "" || domainOwner != app.TenantID || t.app.TenantID != app.TenantID {
			t.blocked = true
			issue("dns_custom_domain_owner_mismatch", host, "domain, app and shared target must have one owner")
			return
		}
		if len(routes[host]) == 0 {
			t.blocked = true
			issue("dns_custom_domain_route_missing", host, "referenced hostname is absent from the frozen route intent")
		}
		ownerFound := false
		for _, route := range routes[host] {
			routeApp, exists := apps[route.AppID]
			if route.TenantID != app.TenantID || route.AppID == "" || !exists || routeApp.ID != route.AppID || routeApp.TenantID != app.TenantID {
				t.blocked = true
				issue("dns_custom_domain_owner_mismatch", host, "every path must have a verified app owner in the target tenant")
				break
			}
			ownerFound = ownerFound || route.AppID == app.ID
			t.shared = t.shared || route.AppID != app.ID
		}
		if len(routes[host]) > 0 && !ownerFound {
			t.blocked = true
			issue("dns_custom_domain_owner_mismatch", host, "target owning app is absent from the referenced hostname")
		}
	}
	// Sorting detached inputs makes both diagnostics and output independent of
	// input order and map iteration. A conflicting alias blocks the whole target.
	ordered := append([]model.AppDomain(nil), domains...)
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].Hostname+"\x00"+ordered[i].AppID+"\x00"+ordered[i].TenantID < ordered[j].Hostname+"\x00"+ordered[j].AppID+"\x00"+ordered[j].TenantID
	})
	for _, domain := range ordered {
		host := normalizeExternalAppDomain(domain.Hostname)
		if domain.Status != model.AppDomainStatusVerified || domainsConfig.isPlatformOwnedDomainBinding(host) || !domainsConfig.managedEdgeCustomDomain(host) {
			continue
		}
		app, found := apps[domain.AppID]
		target := normalizeExternalAppDomain(domain.RouteTarget)
		if !found || app.ID != domain.AppID {
			issue("dns_custom_domain_app_missing", host, "domain app is absent from the frozen business snapshot")
			// Still block an explicitly shared target; another alias must not
			// silently authorize its traffic after this member failed validation.
			add(target, host, model.App{}, domain.TenantID)
			continue
		}
		if target == "" {
			target = normalizeExternalAppDomain(domainsConfig.primaryCustomDomainTarget(app))
		}
		add(target, host, app, domain.TenantID)
	}
	ids := make([]string, 0, len(apps))
	for id := range apps {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		app := apps[id]
		if app.Route == nil {
			continue
		}
		host := normalizeExternalAppDomain(app.Route.Hostname)
		if host == "" || !edgeDNSTargetWithinZone(host, domainsConfig.AppBaseDomain) {
			continue
		}
		target := normalizeExternalAppDomain(domainsConfig.primaryCustomDomainTarget(app))
		if targets[target] != nil {
			continue
		}
		// When no dedicated target is configured, the normal route record
		// already owns the default hostname (including hosted FUGUE_APP).
		if target == host && len(existing[target]) > 0 {
			continue
		}
		add(target, host, app, app.TenantID)
	}
	names := make([]string, 0, len(targets))
	for target := range targets {
		names = append(names, target)
	}
	sort.Strings(names)
	for _, target := range names {
		t := targets[target]
		if t.blocked {
			continue
		}
		hosts := make([]string, 0, len(t.hosts))
		for host := range t.hosts {
			hosts = append(hosts, host)
		}
		sort.Strings(hosts)
		if len(hosts) > 128 {
			issue("dns_custom_domain_target_limit", target, "shared target exceeds the supported hostname bound")
			continue
		}
		conflict := false
		for _, record := range existing[target] {
			// Protected name policy applies even to TXT/MX, matching legacy
			// generated custom targets. Unprotected non-address RRsets coexist.
			if record.RecordKind == model.EdgeDNSRecordKindProtected {
				conflict = true
			}
			switch record.Type {
			case "A", "AAAA", "CNAME", "ALIAS", "ANAME", "FUGUE_APP", "FUGUE_ROUTE":
				conflict = true
			}
		}
		if conflict {
			issue("dns_custom_domain_target_conflict", target, "existing DNS source owns this target; no record was overwritten")
			continue
		}
		result.Intent.DNS = append(result.Intent.DNS, platformconfig.DNSIntent{
			Hostname: target, Type: "FUGUE_ROUTE", Values: []string{}, TTL: domainsConfig.DefaultDNSTTL,
			RecordKind: model.EdgeDNSRecordKindCustomDomainTarget, AppID: t.app.ID, TenantID: t.app.TenantID,
			Route: &platformconfig.DNSRouteIntent{Hostnames: hosts, DNSApplicationIntent: platformconfig.DNSApplicationIntent{IPv4Policy: "auto", IPv6Policy: "auto", TTLPolicy: "record", FallbackPolicy: "fail_closed"}},
		})
		if t.shared {
			last := &result.Intent.DNS[len(result.Intent.DNS)-1]
			for _, host := range hosts {
				for _, route := range routes[host] {
					last.Route.Bindings = append(last.Route.Bindings, platformconfig.DNSRouteBinding{Hostname: host, PathPrefix: route.PathPrefix, AppID: route.AppID})
				}
			}
		}
	}
	result.Intent = platformconfig.NormalizePlatformIntent(result.Intent)
	result.Intent.Generation = ""
	generation, err := platformconfig.PlatformIntentGeneration(result.Intent)
	if err != nil {
		return err
	}
	result.Intent.Generation, result.RuntimeSnapshot.IntentGeneration = generation, generation
	if err := platformconfig.ValidatePlatformIntent(result.Intent); err != nil {
		issue("intent_requires_validation_repair", "", "projected intent failed compiler input validation")
	}
	return nil
}
