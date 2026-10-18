package api

import (
	"slices"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
)

// Configuration is reviewable even when no runtime exists. No endpoint, DNS
// selection or freshness is invented to make a draft appear publishable.
func projectConfigurationDNS(result *platformIntentProjectionResponse, declared []platformconfig.DNSConsumerIntent, input platformproducer.ProjectionPolicyInput, templates []platformproducer.HostedZoneTemplate, hosted []model.HostedZone) error {
	consumers, authorities, err := pinnedDNSDeclarations(declared, input, templates, hosted)
	if err != nil {
		return err
	}
	result.Intent.DNSConsumers = consumers
	result.Intent = platformconfig.NormalizePlatformIntent(result.Intent)
	result.Intent.Generation, err = platformconfig.PlatformIntentGeneration(result.Intent)
	if err != nil {
		return err
	}
	result.Policy.DNSAuthorities = authorities
	result.Policy.DNSClientPolicies = input.Clients
	result.Policy.DNSPlacementMode = input.DNSPlacementMode
	result.Policy.DNSQueryPolicy = input.DNSQueryPolicy
	result.Policy.DNSReadiness = input.DNSReadiness
	result.Policy.TLSReadiness = input.TLSReadiness
	result.Policy.TrafficRolloutCohorts = input.Cohorts
	result.Policy = platformconfig.NormalizePolicySnapshot(result.Policy)
	result.Policy.Generation, err = platformconfig.PolicySnapshotGeneration(result.Policy)
	return err
}

func configurationPreview(result *platformIntentProjectionResponse) {
	result.RuntimeSnapshot = platformconfig.RuntimeSnapshot{IntentGeneration: result.Intent.Generation, PolicyGeneration: result.Policy.Generation}
	result.MigrationReady = false
	// These describe observations intentionally not collected, not configuration defects.
	result.Issues = slices.DeleteFunc(result.Issues, func(issue platformProjectionIssue) bool {
		switch issue.Code {
		case "transaction_snapshot_not_frozen", "origin_observation_not_fresh", "origin_observation_time_missing", "origin_status_missing", "resolved_upstream_differs":
			return true
		}
		return false
	})
	result.Issues = append(result.Issues, platformProjectionIssue{Code: "runtime_evidence_not_collected", Reason: "Configuration preview only; use view=runtime for live evidence. This draft does not authorize publication."})
	result.OmittedRuntimeFields = []string{"runtime_snapshot.origins", "runtime_snapshot.releases", "runtime_snapshot.tls_domains", "runtime_snapshot.dns_consumers", "runtime_snapshot.dns_edge_endpoints", "runtime_snapshot.dns_selections", "runtime_snapshot.dns_placements", "runtime_snapshot.dns_flatten", "runtime_snapshot.facts"}
}
