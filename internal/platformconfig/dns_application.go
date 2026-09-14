package platformconfig

import (
	"fmt"
	"strings"
)

func validateDNSApplicationConfiguration(record DNSIntent) error {
	policy := record.Application
	if record.Type != "FUGUE_APP" || policy == nil || record.Route != nil || record.Flatten != nil || len(record.ValueExpirations) != 0 {
		return fmt.Errorf("DNS application configuration requires an exclusive FUGUE_APP binding")
	}
	if record.AppID == "" || record.TenantID == "" || strings.TrimSpace(record.AppID) != record.AppID || strings.TrimSpace(record.TenantID) != record.TenantID || len(record.Values) != 1 || record.Values[0] != record.AppID {
		return fmt.Errorf("FUGUE_APP requires a canonical app_id, tenant_id and an exact value reference")
	}
	if err := validateDNSPlacementOptions(*policy); err != nil {
		return err
	}
	// Validate common owner-name, TTL and status syntax without resolving IPs.
	header := record
	header.Application, header.Type = nil, "TXT"
	return ValidateDNSIntents([]DNSIntent{header})
}

func validateDNSPlacementOptions(policy DNSApplicationIntent) error {
	for _, family := range []string{policy.IPv4Policy, policy.IPv6Policy} {
		switch family {
		case "auto", "ipv4_only", "ipv6_only", "dual_stack_required":
		default:
			return fmt.Errorf("DNS application IP policy is invalid")
		}
	}
	only4 := policy.IPv4Policy == "ipv4_only" || policy.IPv6Policy == "ipv4_only"
	only6 := policy.IPv4Policy == "ipv6_only" || policy.IPv6Policy == "ipv6_only"
	dual := policy.IPv4Policy == "dual_stack_required" || policy.IPv6Policy == "dual_stack_required"
	if (only4 && only6) || (dual && (only4 || only6)) {
		return fmt.Errorf("DNS application IP policies conflict")
	}
	switch policy.TTLPolicy {
	case "record", "target", "min", "bounded":
	default:
		return fmt.Errorf("DNS application TTL policy is invalid")
	}
	switch policy.FallbackPolicy {
	case "fail_closed", "stale_if_error", "empty_noerror":
	default:
		return fmt.Errorf("DNS application fallback policy is invalid")
	}
	return nil
}

func validateDNSApplicationOwners(records []DNSIntent, routes []RouteIntent) error {
	byHost := make(map[string][]RouteIntent, len(routes))
	for _, route := range routes {
		byHost[normalizedImportHostname(route.Hostname)] = append(byHost[normalizedImportHostname(route.Hostname)], route)
	}
	for _, record := range records {
		if record.Application == nil && record.Route == nil {
			continue
		}
		for _, host := range DNSPlacementHostnames(record) {
			owners := byHost[host]
			if len(owners) == 0 {
				return fmt.Errorf("DNS placement binding requires routes at every referenced hostname")
			}
			for _, route := range owners {
				if route.AppID != record.AppID || route.TenantID != record.TenantID {
					return fmt.Errorf("DNS placement binding and route ownership differ")
				}
			}
		}
	}
	return nil
}
