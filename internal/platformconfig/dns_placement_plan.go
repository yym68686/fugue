package platformconfig

import (
	"fmt"

	"fugue/internal/model"
)

const DNSPlacementConsumerReadiness = "consumer_readiness"

// ValidateDNSPlacementMode keeps planning opt-in and inseparable from the
// existing consumer's signed query and route/TLS proof requirements.
func ValidateDNSPlacementMode(p PolicySnapshot) error {
	switch p.DNSPlacementMode {
	case "", "captured_readiness":
		return nil
	case DNSPlacementConsumerReadiness:
		if p.DNSQueryPolicy == nil || p.DNSReadiness == nil || p.TLSReadiness == nil || len(p.DNSAuthorities) == 0 || len(p.DNSClientPolicies) == 0 || len(p.TrafficRolloutCohorts) == 0 {
			return fmt.Errorf("consumer DNS placement requires complete query, authority, client, cohort and route/TLS readiness policy")
		}
		return nil
	default:
		return fmt.Errorf("unsupported DNS placement mode")
	}
}

// planDNSPlacements emits candidates, never readiness. The same compiler builds
// the exact route-proof plan and consumer query views. Only the traffic serving
// consumer may turn those candidates into answers after proving that release.
// Standalone publication of this representation is rejected by admission.
func planDNSPlacements(intent PlatformIntent, routes []CompiledRoute, snapshot RuntimeSnapshot, policy PolicySnapshot) ([]DNSIntent, error) {
	if err := ValidateDNSPlacementMode(policy); err != nil {
		return nil, err
	}
	if len(snapshot.DNSPlacements) != 0 || len(intent.DNSConsumers) == 0 {
		return nil, fmt.Errorf("consumer DNS placement requires declared consumers and cannot mix captured placement leases")
	}
	plan, err := CompileDNSReadiness(intent, routes, snapshot, policy)
	if err != nil {
		return nil, err
	}
	byHost := make(map[string]DNSReadinessRecord, len(plan.Records))
	for _, record := range plan.Records {
		byHost[record.Hostname] = record
	}
	out := make([]DNSIntent, 0, len(intent.DNS))
	for _, record := range intent.DNS {
		if DNSPlacementOptions(record) == nil {
			out = append(out, record)
			continue
		}
		var err error
		if record.Route != nil {
			err = validateDNSRouteConfiguration(record)
		} else {
			err = validateDNSApplicationConfiguration(record)
		}
		if err != nil {
			return nil, err
		}
		requirement, found := byHost[record.Hostname]
		if !found {
			// CompileDNSReadiness excludes only intent/policy-disabled records;
			// it validates dependency ownership before making that decision.
			continue
		}
		if record.Status != "" && record.Status != model.EdgeRouteStatusActive {
			continue
		}
		if len(requirement.Targets) == 0 {
			return nil, fmt.Errorf("DNS placement has no declared endpoint candidates for %s", record.Hostname)
		}
		for _, family := range []string{"A", "AAAA"} {
			values := []string{}
			for _, target := range requirement.Targets {
				if target.Family == family {
					values = append(values, target.Address)
				}
			}
			if len(values) == 0 {
				continue
			}
			resolved := record
			resolved.Application, resolved.Route = nil, nil
			resolved.Type, resolved.Values = family, uniqueSorted(values)
			// The intent's TTL is an upper bound. Answer-time proof expiration
			// and the query policy bound the actual TTL; an old probe lease is
			// not copied into immutable desired configuration.
			out = append(out, resolved)
		}
	}
	return NormalizePlatformIntent(PlatformIntent{DNS: out}).DNS, nil
}
