package platformproducer

import (
	"fmt"
	"reflect"
	"slices"

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

// Recovery can add one explicit offline guard to the selected availability pool.
// Existing suspensions and unrelated services remain immutable. Restoring a node
// later requires an explicitly configured recovery policy or a new authorized
// configuration change, never a date, Ready, or a code release. Omitting probe
// recovery deliberately keeps a capacity-impaired endpoint held.
func ValidateDNSAvailabilityRecoveryUpdate(previous, next Policy, static, oldDNS, newDNS model.PlatformArtifact) error {
	return validatePhysicalDNSUpdate(previous, next, static, oldDNS, newDNS, "dns_availability_recovery")
}

func validateDNSAvailabilityRecoveryDelta(old, next ProjectionPolicyInput) error {
	old, next = cloneBusinessProjectionPolicy(old), cloneBusinessProjectionPolicy(next)
	if old.DNSQueryPolicy == nil || next.DNSQueryPolicy == nil || ValidateOfflineEdges(old.OfflineEdges) != nil || ValidateOfflineEdges(next.OfflineEdges) != nil {
		return fmt.Errorf("invalid availability recovery sources")
	}
	var changed *model.DNSAvailabilityPolicy
	for _, route := range next.DNSQueryPolicy.AvailabilityRoutes {
		if !reflect.DeepEqual(platformconfig.DNSAvailabilityForHost(old.DNSQueryPolicy, route.Hostname), &route.Policy) {
			changed = &route.Policy
		}
	}
	if changed == nil {
		return fmt.Errorf("recovery must explicitly enable or revise availability")
	}
	before := map[string]OfflineEdge{}
	for _, hold := range old.OfflineEdges {
		before[hold.EdgeID] = hold
	}
	added := 0
	for _, hold := range next.OfflineEdges {
		if prior, found := before[hold.EdgeID]; found {
			if !reflect.DeepEqual(prior, hold) {
				return fmt.Errorf("recovery cannot alter an existing offline hold")
			}
			delete(before, hold.EdgeID)
		} else {
			added++
			if !slices.Contains(changed.OrderedEdgeIDs, hold.EdgeID) || hold.Reason != "quota_exhausted" && hold.Reason != "network_unavailable" {
				return fmt.Errorf("new recovery hold must belong to the availability pool and declare quota or network impairment")
			}
		}
	}
	if added > 1 || len(before) > 0 {
		return fmt.Errorf("recovery may add at most one hold and cannot remove holds")
	}
	old.OfflineEdges, next.OfflineEdges = nil, nil
	return validateDNSAvailabilityDelta(old, next)
}
