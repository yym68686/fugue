package platformconfig

import (
	"fmt"

	"fugue/internal/model"
)

// DNSRouteStateConstraint controls DNS availability independently of origin
// availability. The executor must prove the exact non-origin route state.
type DNSRouteStateConstraint struct {
	RecordKind       string `json:"record_kind"`
	InactiveBehavior string `json:"inactive_behavior"`
}

func validateDNSRouteStateConstraints(rules []DNSRouteStateConstraint) error {
	if len(rules) > 5 {
		return fmt.Errorf("too many DNS route state constraints")
	}
	seen := map[string]bool{}
	for _, rule := range rules {
		switch rule.RecordKind {
		case model.EdgeDNSRecordKindPlatform, model.EdgeDNSRecordKindPlatformRoute, model.EdgeDNSRecordKindPlatformDomain, model.EdgeDNSRecordKindCustomDomainTarget, model.EdgeDNSRecordKindHosted:
		default:
			return fmt.Errorf("unsupported DNS route state record kind")
		}
		if seen[rule.RecordKind] || (rule.InactiveBehavior != "omit" && rule.InactiveBehavior != "serve_error_page") {
			return fmt.Errorf("ambiguous or invalid DNS route state constraint")
		}
		seen[rule.RecordKind] = true
	}
	return nil
}

func DNSRouteStateAllowed(record DNSIntent, route CompiledRoute, policy PolicySnapshot) bool {
	if route.RoutePolicy != "" && !model.EdgeRoutePolicyAllowsTraffic(route.RoutePolicy) {
		return false
	}
	state := DNSRouteServingState(route)
	if state == model.EdgeRouteStatusActive {
		return true
	}
	if state != model.EdgeRouteStatusDisabled && state != model.EdgeRouteStatusUnavailable {
		return false
	}
	if !model.EdgeRoutePolicyAllowsTraffic(route.RoutePolicy) {
		return false
	}
	for _, rule := range policy.DNSRouteStateConstraints {
		if rule.RecordKind == record.RecordKind {
			return rule.InactiveBehavior == "serve_error_page"
		}
	}
	return false
}

// Match artifact projection: disabled intent wins even when the origin is
// active; a more specific non-active origin state remains visible.
func DNSRouteServingState(route CompiledRoute) string {
	if route.Status != "" && route.Status != model.EdgeRouteStatusActive {
		return route.Status
	}
	if !route.Enabled {
		return model.EdgeRouteStatusDisabled
	}
	return model.EdgeRouteStatusActive
}
