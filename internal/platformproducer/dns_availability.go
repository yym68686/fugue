package platformproducer

import (
	"fmt"
	"reflect"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func ValidateDNSAvailabilityUpdate(previous, next Policy, static, oldDNS, newDNS model.PlatformArtifact) error {
	return validatePhysicalDNSUpdate(previous, next, static, oldDNS, newDNS, "dns_availability")
}

func validateDNSAvailabilityDelta(old, next ProjectionPolicyInput) error {
	old, next = cloneBusinessProjectionPolicy(old), cloneBusinessProjectionPolicy(next)
	if old.Generation == next.Generation || old.DNSQueryPolicy == nil || next.DNSQueryPolicy == nil {
		return fmt.Errorf("availability requires a new explicit policy generation")
	}
	oldRoutes, newRoutes := old.DNSQueryPolicy.AvailabilityRoutes, next.DNSQueryPolicy.AvailabilityRoutes
	before, after := map[string]platformconfig.DNSAvailabilityRoute{}, map[string]platformconfig.DNSAvailabilityRoute{}
	for _, route := range oldRoutes {
		before[route.Hostname] = route
	}
	for _, route := range newRoutes {
		after[route.Hostname] = route
	}
	changed := map[string]bool{}
	for host, route := range before {
		if value, ok := after[host]; !ok || !reflect.DeepEqual(route, value) {
			changed[host] = true
		}
	}
	for host, route := range after {
		if value, ok := before[host]; !ok || !reflect.DeepEqual(route, value) {
			changed[host] = true
		}
	}
	if len(changed) != 1 {
		return fmt.Errorf("availability transition must change exactly one hostname")
	}
	// DecodeProjectionPolicy returns detached inputs; removal here is comparison
	// only and cannot alter either signed source artifact.
	old.Generation, next.Generation = "", ""
	old.DNSQueryPolicy.AvailabilityRoutes, next.DNSQueryPolicy.AvailabilityRoutes = nil, nil
	if !reflect.DeepEqual(old, next) {
		return fmt.Errorf("availability transition changed unrelated constraints")
	}
	return nil
}
