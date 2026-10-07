package dnsserver

import (
	"slices"
	"time"

	"fugue/internal/dnsroutesource"
	"fugue/internal/platformconfig"
)

// This affects diagnostics only: no proof is rebound and no readiness is
// granted. Every pending requirement must have a fresh exact old-publication
// observation; unknown, negative and missing observations remain failures.
func dnsKnownReleaseOverlap(old *dnsServingState, candidate dnsServingPayload, observations, filtered []dnsReadinessFact, now time.Time) bool {
	if old.payload.routeSources != nil || candidate.routeSources != nil {
		return false
	}
	previous := map[string]platformconfig.DNSReadinessProbe{}
	for _, p := range old.payload.Plan.Probes {
		previous[dnsroutesource.ProbeKey(p)] = p
	}
	observed := map[string]dnsReadinessFact{}
	for _, f := range observations {
		observed[f.ProbeID] = f
	}
	valid := validDNSReadinessFacts(candidate.Plan, candidate.Policy.DNSReadiness, filtered, now)
	pending := false
	for _, requirement := range candidate.Plan.Probes {
		if _, ok := valid[requirement.ID]; ok {
			continue
		}
		prior, ok := previous[dnsroutesource.ProbeKey(requirement)]
		if !ok {
			return false
		}
		fact, ok := observed[requirement.ID]
		if !ok || (!fact.Ready && fact.Reason != "route_digest_mismatch") || !dnsProofMatchesRelease(fact.Proof, old.record.Parent, old.record.Candidate, old.routeID, old.payload) {
			return false
		}
		fact = evaluateDNSReadinessFact(prior, old.payload.Policy.DNSReadiness, fact.Proof, nil, now)
		if !fact.Ready {
			return false
		}
		pending = true
	}
	return pending
}

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
