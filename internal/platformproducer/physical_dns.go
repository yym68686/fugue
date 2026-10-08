package platformproducer

import (
	"fmt"
	"reflect"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func ValidatePhysicalDNSOptIn(previous, next Policy, static, oldDNS, newDNS model.PlatformArtifact) error {
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
