package platformproducer

import (
	"errors"
	"reflect"

	"fugue/internal/platformconfig"
)

func cloneBusinessProjectionPolicy(policy ProjectionPolicyInput) ProjectionPolicyInput {
	policy.DNSQueryPolicy = platformconfig.NormalizePolicySnapshot(platformconfig.PolicySnapshot{DNSQueryPolicy: policy.DNSQueryPolicy}).DNSQueryPolicy
	return policy
}

func validateBusinessExplorationDelta(previous, next ProjectionPolicyInput) error {
	previous, next = cloneBusinessProjectionPolicy(previous), cloneBusinessProjectionPolicy(next)
	if previous.Generation == next.Generation || previous.DNSQueryPolicy == nil || next.DNSQueryPolicy == nil {
		return errors.New("business exploration requires new explicit query-policy generation")
	}
	oldRoutes, newRoutes := previous.DNSQueryPolicy.PhysicalRoutes, next.DNSQueryPolicy.PhysicalRoutes
	previous.Generation, next.Generation = "", ""
	previous.DNSQueryPolicy.PhysicalRoutes, next.DNSQueryPolicy.PhysicalRoutes = nil, nil
	if !reflect.DeepEqual(previous, next) {
		return errors.New("business exploration cannot change unrelated constraints")
	}
	before, after := map[string]platformconfig.PhysicalQualityRoute{}, map[string]platformconfig.PhysicalQualityRoute{}
	for _, route := range oldRoutes {
		before[route.Hostname] = route
	}
	for _, route := range newRoutes {
		after[route.Hostname] = route
	}
	changed := 0
	for hostname, old := range before {
		current, present := after[hostname]
		if present && reflect.DeepEqual(old, current) {
			continue
		}
		changed++
		if old.BusinessExploration == nil && (!present || current.BusinessExploration == nil) {
			return errors.New("business exploration cannot remove or alter an unrelated quality route")
		}
		if present {
			if old.BusinessExploration != nil && current.BusinessExploration != nil && old.BusinessExploration.IntentID == current.BusinessExploration.IntentID {
				return errors.New("changed business exploration requires a new intent identity")
			}
			old.BusinessExploration, current.BusinessExploration = nil, nil
			old.PathPrefix, current.PathPrefix = "", ""
			if !reflect.DeepEqual(old, current) {
				return errors.New("business exploration cannot alter route ownership or legacy quality settings")
			}
		}
	}
	for hostname, current := range after {
		if _, present := before[hostname]; !present {
			changed++
			if current.BusinessExploration == nil {
				return errors.New("new business exploration route requires explicit bounded intent")
			}
		}
	}
	if changed != 1 {
		return errors.New("business exploration must change exactly one hostname")
	}
	return nil
}
