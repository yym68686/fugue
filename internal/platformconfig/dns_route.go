package platformconfig

import (
	"fmt"
	"strings"

	"fugue/internal/model"
	"github.com/miekg/dns"
)

func DNSPlacementHostnames(record DNSIntent) []string {
	if record.Route != nil {
		return append([]string(nil), record.Route.Hostnames...)
	}
	if record.Application != nil {
		return []string{record.Hostname}
	}
	return nil
}

func DNSPlacementOptions(record DNSIntent) *DNSApplicationIntent {
	if record.Route != nil {
		value := record.Route.DNSApplicationIntent
		return &value
	}
	if record.Application != nil {
		value := *record.Application
		return &value
	}
	return nil
}

func validateDNSRouteConfiguration(record DNSIntent) error {
	if record.Route == nil || record.Type != "FUGUE_ROUTE" || record.Application != nil || record.Flatten != nil || len(record.ValueExpirations) != 0 || len(record.Values) != 0 {
		return fmt.Errorf("DNS route configuration requires exclusive FUGUE_ROUTE with empty values")
	}
	if (record.AppID == "") != (record.TenantID == "") || record.AppID != strings.TrimSpace(record.AppID) || record.TenantID != strings.TrimSpace(record.TenantID) {
		return fmt.Errorf("DNS route configuration requires a canonical owner pair")
	}
	if len(record.Route.Hostnames) < 1 || len(record.Route.Hostnames) > 128 {
		return fmt.Errorf("DNS route hostname references must be bounded and nonempty")
	}
	seen := map[string]bool{}
	for _, host := range record.Route.Hostnames {
		_, valid := dns.IsDomainName(dns.Fqdn(host))
		if !valid || host == "" || host != normalizedImportHostname(host) || strings.ContainsAny(host, "* \t\r\n") || seen[host] {
			return fmt.Errorf("DNS route hostname references must be canonical and unique")
		}
		seen[host] = true
	}
	if len(record.Route.Bindings) > 4096 || (len(record.Route.Bindings) > 0 && record.AppID == "") {
		return fmt.Errorf("DNS route bindings require a tenant owner and at most 4096 paths")
	}
	bindings, ownerHosts := map[string]bool{}, map[string]bool{}
	for _, binding := range record.Route.Bindings {
		key := binding.Hostname + "\x00" + binding.PathPrefix
		if !seen[binding.Hostname] || binding.PathPrefix == "" || binding.PathPrefix != model.NormalizeAppRoutePathPrefix(binding.PathPrefix) || binding.AppID == "" || binding.AppID != strings.TrimSpace(binding.AppID) || bindings[key] {
			return fmt.Errorf("DNS route binding identity is invalid or duplicated")
		}
		bindings[key] = true
		if binding.AppID == record.AppID {
			ownerHosts[binding.Hostname] = true
		}
	}
	if len(bindings) > 0 && len(ownerHosts) != len(seen) {
		return fmt.Errorf("DNS record owner must occur at each referenced hostname")
	}
	if err := validateDNSPlacementOptions(record.Route.DNSApplicationIntent); err != nil {
		return err
	}
	header := record
	header.Route, header.Type, header.Values = nil, "TXT", []string{"route-reference"}
	return ValidateDNSIntents([]DNSIntent{header})
}

// ValidateDNSRouteOwners checks the complete dependency set before health is
// considered. Even an unavailable route cannot conceal an ownership mismatch.
func ValidateDNSRouteOwners(record DNSIntent, routes []CompiledRoute) error {
	bindings := map[string]DNSRouteBinding{}
	if record.Route != nil {
		if err := validateDNSRouteConfiguration(record); err != nil {
			return err
		}
		for _, b := range record.Route.Bindings {
			bindings[b.Hostname+"\x00"+model.NormalizeAppRoutePathPrefix(b.PathPrefix)] = b
		}
	}
	seen := map[string]bool{}
	for _, route := range routes {
		if len(bindings) == 0 {
			if route.AppID != record.AppID || route.TenantID != record.TenantID {
				return fmt.Errorf("DNS placement route ownership differs")
			}
			continue
		}
		key := route.Hostname + "\x00" + model.NormalizeAppRoutePathPrefix(route.PathPrefix)
		binding, ok := bindings[key]
		if !ok || seen[key] || binding.AppID != route.AppID || route.TenantID != record.TenantID {
			return fmt.Errorf("DNS placement route binding differs for %s %s (binding=%s route=%s/%s tenant=%s)", route.Hostname, model.NormalizeAppRoutePathPrefix(route.PathPrefix), binding.AppID, route.AppID, route.TenantID, record.TenantID)
		}
		seen[key] = true
	}
	if len(bindings) != len(seen) {
		return fmt.Errorf("DNS placement route binding is missing")
	}
	return nil
}
