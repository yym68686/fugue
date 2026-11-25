package platformproducer

import (
	"fmt"
	"reflect"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func ValidatePhysicalDNSOptIn(previous, next Policy, static, oldDNS, newDNS model.PlatformArtifact) error {
	return validatePhysicalDNSUpdate(previous, next, static, oldDNS, newDNS, "physical_dns")
}

func ValidateDynamicQualityUpdate(previous, next Policy, static, oldDNS, newDNS model.PlatformArtifact) error {
	return validatePhysicalDNSUpdate(previous, next, static, oldDNS, newDNS, "dynamic_quality")
}

// ValidateFleetDefaultOnlyUpdate is narrower than dynamic_quality admission:
// it cannot change an eligibility, policy, topology or configured order.
func ValidateFleetDefaultOnlyUpdate(previous, next Policy, static, oldDNS, newDNS model.PlatformArtifact) error {
	if err := ValidateDynamicQualityUpdate(previous, next, static, oldDNS, newDNS); err != nil {
		return err
	}
	intent, err := DecodeStaticIntent(static)
	if err != nil {
		return err
	}
	before, err := DecodeProjectionPolicy(oldDNS, intent.Consumers, previous.HostedZoneTemplates)
	if err != nil {
		return err
	}
	after, err := DecodeProjectionPolicy(newDNS, intent.Consumers, next.HostedZoneTemplates)
	if err != nil {
		return err
	}
	b, a := before.DNSQueryPolicy.DynamicQuality, after.DNSQueryPolicy.DynamicQuality
	if b == nil || a == nil || b.FleetDefault == a.FleetDefault && reflect.DeepEqual(b.FleetDefaultHosts, a.FleetDefaultHosts) {
		return fmt.Errorf("expected an explicit fleet default change")
	}
	a.FleetDefault, a.FleetDefaultHosts = b.FleetDefault, b.FleetDefaultHosts
	after.Generation = before.Generation
	if !reflect.DeepEqual(before, after) {
		return fmt.Errorf("fleet default handoff cannot change any other DNS input")
	}
	return nil
}

func ValidateBusinessExplorationUpdate(previous, next Policy, static, oldDNS, newDNS model.PlatformArtifact) error {
	return validatePhysicalDNSUpdate(previous, next, static, oldDNS, newDNS, "business_exploration")
}

func validatePhysicalDNSUpdate(previous, next Policy, static, oldDNS, newDNS model.PlatformArtifact, operation string) error {
	if previous.TargetScope != "global" || next.TargetScope != "global" || previous.PublicationRole != "" || next.PublicationRole != "" || previous.RoutePlacementTransition != nil || next.RoutePlacementTransition != nil ||
		previous.Mode != "serving" || next.Mode != "serving" || previous.Serving == nil || next.Serving == nil || previous.Serving.SinglePublication || next.Serving.SinglePublication || !previous.RequireDNSQueryPolicy || !next.RequireDNSQueryPolicy ||
		previous.StaticIntentArtifactID != next.StaticIntentArtifactID || previous.StaticIntentDigest != next.StaticIntentDigest || previous.DNSPolicyArtifactID == next.DNSPolicyArtifactID || previous.Generation == next.Generation ||
		static.ID != previous.StaticIntentArtifactID || static.ContentHash != previous.StaticIntentDigest || oldDNS.ID != previous.DNSPolicyArtifactID || oldDNS.ContentHash != previous.DNSPolicyDigest ||
		newDNS.ID != next.DNSPolicyArtifactID || newDNS.ContentHash != next.DNSPolicyDigest || newDNS.GenerationSequence <= oldDNS.GenerationSequence {
		return fmt.Errorf("physical DNS transition requires exact continuous global source bindings")
	}
	previous.Generation, next.Generation = "", ""
	previous.DNSPolicyArtifactID, next.DNSPolicyArtifactID = "", ""
	previous.DNSPolicyDigest, next.DNSPolicyDigest = "", ""
	if !reflect.DeepEqual(previous, next) {
		return fmt.Errorf("physical DNS transition cannot change producer controls or static intent")
	}
	intent, err := DecodeStaticIntent(static)
	if err != nil {
		return err
	}
	oldPolicy, err := DecodeProjectionPolicy(oldDNS, intent.Consumers, previous.HostedZoneTemplates)
	if err != nil {
		return err
	}
	newPolicy, err := DecodeProjectionPolicy(newDNS, intent.Consumers, next.HostedZoneTemplates)
	if err != nil {
		return err
	}
	if oldPolicy.DNSQueryPolicy == nil || newPolicy.DNSQueryPolicy == nil || oldPolicy.Generation == newPolicy.Generation {
		return fmt.Errorf("physical DNS transition requires versioned explicit query policy")
	}
	if operation == "dns_availability_recovery" {
		return validateDNSAvailabilityRecoveryDelta(oldPolicy, newPolicy)
	}
	if operation == "dns_availability" {
		return validateDNSAvailabilityDelta(oldPolicy, newPolicy)
	}
	if operation == "edge_offline" {
		return validateOfflinePolicyDelta(oldPolicy, newPolicy)
	}
	if operation == "business_exploration" {
		return validateBusinessExplorationDelta(oldPolicy, newPolicy)
	}
	if operation == "dynamic_quality" {
		if dnsBootstrapOnlyDelta(oldPolicy, newPolicy) {
			return nil
		}
		if newPolicy.DNSQueryPolicy.DynamicQuality == nil || newPolicy.DNSQueryPolicy.OrderedProjection == nil || reflect.DeepEqual(oldPolicy.DNSQueryPolicy.DynamicQuality, newPolicy.DNSQueryPolicy.DynamicQuality) {
			return fmt.Errorf("dynamic quality transition requires changed complete default policy")
		}
		if len(newPolicy.DNSQueryPolicy.PhysicalRoutes) == 0 {
			for _, route := range oldPolicy.DNSQueryPolicy.PhysicalRoutes {
				if route.BusinessExploration != nil {
					return fmt.Errorf("universal adoption cannot discard explicit business exploration intent")
				}
				prior := route.Policy
				prior.Version = newPolicy.DNSQueryPolicy.DynamicQuality.Policy.Version
				if !reflect.DeepEqual(prior, newPolicy.DNSQueryPolicy.DynamicQuality.Policy) {
					return fmt.Errorf("universal adoption cannot discard a distinct per-service quality constraint")
				}
			}
			oldPolicy.DNSQueryPolicy.PhysicalRoutes = nil
			newPolicy.DNSQueryPolicy.PhysicalRoutes = nil
		}
		oldPolicy.DNSQueryPolicy.DynamicQuality, newPolicy.DNSQueryPolicy.DynamicQuality = nil, nil
		oldPolicy.Generation, newPolicy.Generation = "", ""
		if !reflect.DeepEqual(oldPolicy, newPolicy) {
			return fmt.Errorf("dynamic quality transition cannot change constraints, static intent or configured orders")
		}
		return nil
	}
	oldRoutes, newRoutes := oldPolicy.DNSQueryPolicy.PhysicalRoutes, newPolicy.DNSQueryPolicy.PhysicalRoutes
	if len(newRoutes) != len(oldRoutes)+1 {
		return fmt.Errorf("physical DNS transition must add exactly one hostname")
	}
	byHostname := make(map[string]platformconfig.PhysicalQualityRoute, len(newRoutes))
	for _, route := range newRoutes {
		byHostname[route.Hostname] = route
	}
	for _, route := range oldRoutes {
		if found, ok := byHostname[route.Hostname]; !ok || !reflect.DeepEqual(found, route) {
			return fmt.Errorf("physical DNS transition changed existing hostname policy")
		}
	}
	oldPolicy.Generation, newPolicy.Generation = "", ""
	oldPolicy.DNSQueryPolicy.PhysicalRoutes, newPolicy.DNSQueryPolicy.PhysicalRoutes = nil, nil
	if !reflect.DeepEqual(oldPolicy, newPolicy) {
		return fmt.Errorf("physical DNS transition changed unrelated constraint or DNS strategy")
	}
	return nil
}

// Authorize only the transition from a fixed declaration to the same order
// as a bootstrap seed. This must be a separate atom from changes to quality,
// availability, defaults, membership, static routes or any configured order.
func dnsBootstrapOnlyDelta(old, next ProjectionPolicyInput) bool {
	if old.DNSQueryPolicy.DynamicQuality == nil || next.DNSQueryPolicy.DynamicQuality == nil ||
		old.DNSQueryPolicy.OrderedProjection == nil || next.DNSQueryPolicy.OrderedProjection == nil {
		return false
	}
	query := *next.DNSQueryPolicy
	query.OrderedProjection = platformconfig.CloneDNSOrderedProjection(query.OrderedProjection)
	before, after := old.DNSQueryPolicy.OrderedProjection.Overrides, query.OrderedProjection.Overrides
	if len(before) != len(after) {
		return false
	}
	changed := false
	for i := range after {
		if before[i].BootstrapOnly && !after[i].BootstrapOnly {
			return false
		}
		changed = changed || after[i].BootstrapOnly != before[i].BootstrapOnly
		after[i].BootstrapOnly = before[i].BootstrapOnly
	}
	next.DNSQueryPolicy = &query
	old.Generation, next.Generation = "", ""
	return changed && reflect.DeepEqual(old, next)
}
