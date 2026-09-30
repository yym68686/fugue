package dnsserver

import (
	"slices"

	"fugue/internal/platformconfig"
)

// A transition is volatile query evidence, never a positive checkpoint. Each
// record uses one complete signed plan and its exact publication's proofs;
// quorum is never assembled from different route digests or publications.
type dnsRecordTransition struct {
	state *dnsServingState
	hosts map[string]bool
}

func dnsTransitionRecords(old *dnsServingState, bridge *dnsReleaseBridge) map[string]bool {
	if old == nil || bridge == nil || old.payload.Plan == nil || bridge.payload.Plan == nil {
		return nil
	}
	// Keep the existing hard-policy, tenant, hostname, endpoint and per-app
	// authorization checks. Only the comparison projection omits route digests:
	// execution still validates the successor's original signed requirements.
	previous, next := *old, *bridge
	previous.payload.Plan = dnsTransitionRequirementShape(old.payload.Plan)
	next.payload.Plan = dnsTransitionRequirementShape(bridge.payload.Plan)
	if previous.payload.Plan == nil || next.payload.Plan == nil {
		return nil
	}
	allowed := compatibleDNSReleaseProbes(&previous, &next)
	hosts := map[string]bool{}
	for _, record := range previous.payload.Plan.Records {
		complete := len(record.Targets) > 0
		for _, target := range record.Targets {
			if len(target.ProbeIDs) == 0 {
				complete = false
			}
			for _, id := range target.ProbeIDs {
				complete = complete && allowed[id]
			}
		}
		if complete {
			hosts[record.Hostname] = true
		}
	}
	return hosts
}

func dnsTransitionRequirementShape(plan *platformconfig.DNSReadinessPlan) *platformconfig.DNSReadinessPlan {
	out := &platformconfig.DNSReadinessPlan{}
	ids, seen := map[string]string{}, map[string]bool{}
	for _, probe := range plan.Probes {
		original := probe.ID
		probe.RouteDigest = ""
		id, err := platformconfig.DNSReadinessProbeID(probe)
		if err != nil || seen[id] {
			return nil
		}
		probe.ID, ids[original], seen[id] = id, id, true
		out.Probes = append(out.Probes, probe)
	}
	for _, record := range plan.Records {
		record.Targets = append([]platformconfig.DNSReadinessTarget(nil), record.Targets...)
		for i := range record.Targets {
			target := &record.Targets[i]
			target.ProbeIDs = append([]string(nil), target.ProbeIDs...)
			for j, id := range target.ProbeIDs {
				if ids[id] == "" {
					return nil
				}
				target.ProbeIDs[j] = ids[id]
			}
			slices.Sort(target.ProbeIDs)
		}
		out.Records = append(out.Records, record)
	}
	return out
}
