package api

import (
	"fmt"
	"sort"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

// Verified business bindings in the platform namespace have the same legacy
// precedence as explicit platform entries. Preserve that desired ownership,
// while all address selection and route/TLS readiness stay in fixed facts.
func (s *Server) projectPlatformDomainDNS(result *platformIntentProjectionResponse, domains []model.AppDomain) error {
	routes := make(map[string][]platformconfig.RouteIntent)
	for _, route := range result.Intent.Routes {
		routes[route.Hostname] = append(routes[route.Hostname], route)
	}
	desired := make(map[string]platformconfig.DNSIntent)
	for _, domain := range domains {
		host := normalizeExternalAppDomain(domain.Hostname)
		if domain.Status != model.AppDomainStatusVerified || !s.isPlatformOwnedDomainBinding(host) {
			continue
		}
		if _, exists := desired[host]; exists || domain.AppID == "" || domain.TenantID == "" || len(routes[host]) == 0 {
			return fmt.Errorf("platform domain DNS has ambiguous or missing route ownership")
		}
		ownerFound := false
		bindings := []platformconfig.DNSRouteBinding{}
		for _, route := range routes[host] {
			if route.AppID == "" || route.TenantID != domain.TenantID {
				return fmt.Errorf("platform domain DNS route tenant does not match its binding")
			}
			ownerFound = ownerFound || route.AppID == domain.AppID
			bindings = append(bindings, platformconfig.DNSRouteBinding{Hostname: host, PathPrefix: model.NormalizeAppRoutePathPrefix(route.PathPrefix), AppID: route.AppID})
		}
		if !ownerFound {
			return fmt.Errorf("platform domain DNS binding owner is absent from its routes")
		}
		desired[host] = platformconfig.DNSIntent{Hostname: host, Type: "FUGUE_ROUTE", Values: []string{}, TTL: edgeDNSPolicyTTL(s.dnsBundleTTL),
			RecordKind: model.EdgeDNSRecordKindPlatformDomain, AppID: domain.AppID, TenantID: domain.TenantID,
			Route: &platformconfig.DNSRouteIntent{Hostnames: []string{host}, Bindings: bindings, DNSApplicationIntent: platformconfig.DNSApplicationIntent{IPv4Policy: "auto", IPv6Policy: "auto", TTLPolicy: "record", FallbackPolicy: "fail_closed"}}}
	}
	if len(desired) == 0 {
		return nil
	}
	staticRecords, _ := projectBusinessDNSDraft(&platformIntentProjectionResponse{}, nil, nil, nil, s.dnsStaticRecords)
	staticDigests := map[string]int{}
	for _, record := range staticRecords {
		if record.Type != "A" && record.Type != "AAAA" && record.Type != "CNAME" {
			continue
		}
		digest, err := platformconfig.Digest(record)
		if err != nil {
			return err
		}
		staticDigests[digest]++
	}
	out := make([]platformconfig.DNSIntent, 0, len(result.Intent.DNS)+len(desired))
	for _, record := range result.Intent.DNS {
		if _, replaces := desired[record.Hostname]; replaces {
			switch record.Type {
			case "A", "AAAA", "CNAME", "ALIAS", "ANAME", "FUGUE_APP", "FUGUE_ROUTE":
				digest, err := platformconfig.Digest(record)
				if err != nil {
					return err
				}
				if staticDigests[digest] > 0 {
					staticDigests[digest]--
					result.DNSExclusions = append(result.DNSExclusions, platformDNSExclusion{RecordID: "static:" + digest, Hostname: record.Hostname, Reason: "static_address_replaced_by_platform_domain"})
					continue
				}
				result.Issues = append(result.Issues, platformProjectionIssue{Code: "platform_domain_dns_conflicting_business_record", Hostname: record.Hostname})
			}
		}
		out = append(out, record)
	}
	for _, record := range desired {
		out = append(out, record)
	}
	result.Intent.DNS = out
	result.Intent = platformconfig.NormalizePlatformIntent(result.Intent)
	generation, err := platformconfig.PlatformIntentGeneration(result.Intent)
	if err != nil {
		return err
	}
	result.Intent.Generation, result.RuntimeSnapshot.IntentGeneration = generation, generation
	if err := platformconfig.ValidatePlatformIntent(result.Intent); err != nil {
		result.Issues = append(result.Issues, platformProjectionIssue{Code: "intent_requires_validation_repair"})
	}
	sort.Slice(result.DNSExclusions, func(i, j int) bool { return result.DNSExclusions[i].RecordID < result.DNSExclusions[j].RecordID })
	return nil
}
