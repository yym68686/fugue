package platformconfig

import (
	"fmt"
	"strings"

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
	if err := validateDNSPlacementOptions(record.Route.DNSApplicationIntent); err != nil {
		return err
	}
	header := record
	header.Route, header.Type, header.Values = nil, "TXT", []string{"route-reference"}
	return ValidateDNSIntents([]DNSIntent{header})
}
