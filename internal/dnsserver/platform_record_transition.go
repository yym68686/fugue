package dnsserver

import (
	"slices"
	"time"

	"fugue/internal/dnsroutesource"
	"fugue/internal/model"
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

// A worker may activate between the candidate scan and a later retained-plan
// probe. Re-evaluate only those newly collected observations against the exact
// successor plan and publication. Never renew timestamps or copy old readiness
// decisions; cached observations from before the candidate scan are excluded.
func dnsSupplementTransitionFacts(successor, retained *dnsServingState, now time.Time) ([]dnsReadinessFact, bool) {
	if successor == nil || retained == nil || successor.payload.Plan == nil || retained.payload.Plan == nil {
		return nil, false
	}
	nextRequirements := make(map[string]*platformconfig.DNSReadinessProbe, len(successor.payload.Plan.Probes))
	for index := range successor.payload.Plan.Probes {
		probe := &successor.payload.Plan.Probes[index]
		key := dnsroutesource.ProbeKey(*probe)
		if _, exists := nextRequirements[key]; exists {
			nextRequirements[key] = nil
		} else {
			nextRequirements[key] = probe
		}
	}
	oldRequirements := make(map[string]*platformconfig.DNSReadinessProbe, len(retained.payload.Plan.Probes))
	for index := range retained.payload.Plan.Probes {
		probe := &retained.payload.Plan.Probes[index]
		if _, exists := oldRequirements[probe.ID]; exists {
			oldRequirements[probe.ID] = nil
		} else {
			oldRequirements[probe.ID] = probe
		}
	}
	counts := make(map[string]int, len(retained.facts))
	for _, fact := range retained.facts {
		counts[fact.ProbeID]++
	}
	positions := make(map[string]int, len(successor.facts))
	for index, fact := range successor.facts {
		if _, exists := positions[fact.ProbeID]; exists {
			positions[fact.ProbeID] = -1
		} else {
			positions[fact.ProbeID] = index
		}
	}
	result, changed := successor.facts, false
	for _, observation := range retained.facts {
		// A failed or cached probe is not a newly authenticated observation,
		// even if a future probe implementation supplies partial proof fields.
		eligible := observation.Ready && observation.Reason == "" ||
			observation.Reason == "route_digest_mismatch" || observation.Reason == "traffic_release_mismatch"
		prior := oldRequirements[observation.ProbeID]
		if !eligible || prior == nil || counts[observation.ProbeID] != 1 || !observation.Proof.CheckedAt.After(successor.checkedAt) {
			continue
		}
		requirement := nextRequirements[dnsroutesource.ProbeKey(*prior)]
		if requirement == nil {
			continue
		}
		index, exists := positions[requirement.ID]
		if exists && (index < 0 || !observation.Proof.CheckedAt.After(result[index].Proof.CheckedAt)) {
			continue
		}
		fact := evaluateDNSReadinessFact(*requirement, successor.payload.Policy.DNSReadiness, observation.Proof, nil, now)
		if !fact.Ready || !dnsFactMatchesRelease(fact, successor.record.Parent, successor.record.Candidate, successor.routeID, successor.payload) {
			continue
		}
		fact.ObservationID = observation.ObservationID
		if !changed {
			result = slices.Clone(result)
			changed = true
		}
		if exists {
			result[index] = fact
		} else {
			positions[fact.ProbeID] = len(result)
			result = append(result, fact)
		}
	}
	return result, changed
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
	previous.payload.Policy.TrafficConstraints, next.payload.Policy.TrafficConstraints =
		dnsTransitionTrafficConstraintPair(old.payload.Policy.TrafficConstraints, bridge.payload.Policy.TrafficConstraints)
	if previous.payload.Plan == nil || next.payload.Plan == nil {
		return nil
	}
	allowed, records := dnsReleaseCompatibility(&previous, &next)
	hosts := map[string]bool{}
	for _, record := range previous.payload.Plan.Records {
		complete := records[record.Hostname] && len(record.Targets) > 0
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

// An application release transition does not change DNS endpoint authority.
// A signed successor may recover its records when it replaces a sole stable
// target while preserving every other policy field, or keeps the exact stable
// release and every other policy boundary, with positive shares for both
// releases. A signed replacement canary may retain the same stable authority;
// previously zero-weight releases still cannot be revived by this comparison.
// This projection is used only for forward recovery, never old-proof bridging.
func dnsTransitionTrafficConstraintPair(before, after []platformconfig.TrafficPolicyConstraint) ([]platformconfig.TrafficPolicyConstraint, []platformconfig.TrafficPolicyConstraint) {
	previous, next := slices.Clone(before), slices.Clone(after)
	previousCounts := make(map[string]int, len(before))
	for _, constraint := range before {
		previousCounts[constraint.ID]++
	}
	nextIndex := make(map[string]int, len(after))
	for index, constraint := range after {
		if _, exists := nextIndex[constraint.ID]; exists {
			nextIndex[constraint.ID] = -1
		} else {
			nextIndex[constraint.ID] = index
		}
	}
	// A DNS consumer can observe a direct deployment, or miss an intermediate
	// canary, and see one sole stable release replaced by another. The current
	// signed successor authorizes its own target; the old stable ID is not a
	// DNS ownership boundary. Compare only after both policies explicitly assign
	// all traffic to one target with the same tenant, app, policy and behavior.
	// Execution still uses the successor's original exact route/release proofs.
	// This does not permit a zero-weight candidate to become active, nor does it
	// rebind successor proofs to the retained positive checkpoint.
	for index, before := range previous {
		if !dnsSingleTrafficConstraint(before) || previousCounts[before.ID] != 1 {
			continue
		}
		position, exists := nextIndex[before.ID]
		if !exists || position < 0 {
			continue
		}
		after := next[position]
		if !dnsSingleTrafficConstraint(after) || after.StableReleaseID == before.StableReleaseID {
			continue
		}
		comparison := after
		comparison.StableReleaseID = before.StableReleaseID
		if comparison == before {
			previous[index] = after
		}
	}
	for index, single := range previous {
		if single.ID == "" || single.AppID == "" || single.TenantID == "" || single.StableReleaseID == "" ||
			previousCounts[single.ID] != 1 || single.Mode != model.AppTrafficModeSingle ||
			single.CandidateReleaseID != "" || single.StableWeight != 100 || single.CandidateWeight != 0 {
			continue
		}
		nextPosition, exists := nextIndex[single.ID]
		if !exists || nextPosition < 0 {
			continue
		}
		canary := next[nextPosition]
		if canary.Mode != model.AppTrafficModeCanary || canary.CandidateReleaseID == "" ||
			canary.CandidateReleaseID == canary.StableReleaseID || canary.StableWeight <= 0 ||
			canary.CandidateWeight <= 0 || canary.StableWeight+canary.CandidateWeight != 100 {
			continue
		}
		retained := canary
		retained.Mode, retained.CandidateReleaseID = model.AppTrafficModeSingle, ""
		retained.StableWeight, retained.CandidateWeight = 100, 0
		if retained == single {
			previous[index] = canary
		}
	}
	// A consumer can miss the brief single-stable publication between aborting
	// one canary and starting another. The newer signed artifact independently
	// authorizes its candidate; DNS may use its complete exact per-record proofs
	// when stable release and every non-weight policy boundary are unchanged.
	// This never rebinds new proofs to the old artifact or promotes its LKG.
	for index, before := range previous {
		if before.ID == "" || before.AppID == "" || before.TenantID == "" || previousCounts[before.ID] != 1 ||
			before.Mode != model.AppTrafficModeCanary || before.StableReleaseID == "" || before.CandidateReleaseID == "" ||
			before.StableReleaseID == before.CandidateReleaseID || before.StableWeight <= 0 || before.CandidateWeight <= 0 || before.StableWeight+before.CandidateWeight != 100 {
			continue
		}
		position, exists := nextIndex[before.ID]
		if !exists || position < 0 {
			continue
		}
		after := next[position]
		if after.Mode != model.AppTrafficModeCanary || after.CandidateReleaseID == "" || after.CandidateReleaseID == before.CandidateReleaseID ||
			after.StableReleaseID == after.CandidateReleaseID || after.StableWeight <= 0 || after.CandidateWeight <= 0 || after.StableWeight+after.CandidateWeight != 100 {
			continue
		}
		comparison := after
		comparison.CandidateReleaseID, comparison.StableWeight, comparison.CandidateWeight = before.CandidateReleaseID, before.StableWeight, before.CandidateWeight
		if comparison == before {
			previous[index] = after
		}
	}
	// Finishing or aborting an already-authorized rollout keeps the same DNS
	// owner. The successor may select either known release as its sole stable
	// target, or reduce one member's weight to zero. These comparison copies
	// never authorize reviving a zero-weight candidate or an unrelated release.
	for _, canary := range previous {
		if canary.ID == "" || canary.AppID == "" || canary.TenantID == "" || previousCounts[canary.ID] != 1 ||
			canary.Mode != model.AppTrafficModeCanary || canary.StableReleaseID == "" || canary.CandidateReleaseID == "" ||
			canary.StableReleaseID == canary.CandidateReleaseID || canary.StableWeight < 0 || canary.CandidateWeight < 0 || canary.StableWeight+canary.CandidateWeight != 100 {
			continue
		}
		position, exists := nextIndex[canary.ID]
		if !exists || position < 0 {
			continue
		}
		candidate := next[position]
		comparison := candidate
		switch {
		case candidate.Mode == model.AppTrafficModeSingle && candidate.CandidateReleaseID == "" && candidate.StableWeight == 100 && candidate.CandidateWeight == 0 &&
			(candidate.StableReleaseID == canary.StableReleaseID || candidate.StableReleaseID == canary.CandidateReleaseID):
			comparison.Mode, comparison.StableReleaseID, comparison.CandidateReleaseID = canary.Mode, canary.StableReleaseID, canary.CandidateReleaseID
		case candidate.Mode == model.AppTrafficModeCanary && canary.StableWeight > 0 && canary.CandidateWeight > 0 &&
			candidate.StableWeight >= 0 && candidate.CandidateWeight >= 0 && candidate.StableWeight+candidate.CandidateWeight == 100:
		default:
			continue
		}
		comparison.StableWeight, comparison.CandidateWeight = canary.StableWeight, canary.CandidateWeight
		if comparison == canary {
			next[position] = canary
		}
	}
	return dnsTransitionTrafficConstraints(previous), dnsTransitionTrafficConstraints(next)
}

func dnsSingleTrafficConstraint(constraint platformconfig.TrafficPolicyConstraint) bool {
	return constraint.ID != "" && constraint.AppID != "" && constraint.TenantID != "" &&
		constraint.Mode == model.AppTrafficModeSingle && constraint.StableReleaseID != "" &&
		constraint.CandidateReleaseID == "" && constraint.StableWeight == 100 && constraint.CandidateWeight == 0
}

// A signed canary step between the same two already-active releases changes
// route digests, not DNS endpoint authority. Only this comparison omits weights;
// the successor still answers with its original policy and exact fresh proofs.
// Unrelated releases, changed identity, fallback or stickiness remain incompatible.
// Initial admission and finishing an existing rollout are compared above;
// the old-proof bridge deliberately retains its stricter checks.
func dnsTransitionTrafficConstraints(constraints []platformconfig.TrafficPolicyConstraint) []platformconfig.TrafficPolicyConstraint {
	result := slices.Clone(constraints)
	for index := range result {
		constraint := &result[index]
		if constraint.Mode == model.AppTrafficModeCanary && constraint.ID != "" && constraint.AppID != "" && constraint.TenantID != "" &&
			constraint.StableReleaseID != "" && constraint.CandidateReleaseID != "" && constraint.StableReleaseID != constraint.CandidateReleaseID &&
			constraint.StableWeight > 0 && constraint.CandidateWeight > 0 && constraint.StableWeight+constraint.CandidateWeight == 100 {
			constraint.StableWeight, constraint.CandidateWeight = -1, -1
		}
	}
	return result
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
