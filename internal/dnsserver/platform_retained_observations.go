package dnsserver

import (
	"reflect"
	"time"

	"fugue/internal/platformconfig"
)

// Refresh only exact retained-release proofs after an assignment race. Neither
// negative observations nor proofs of the superseded successor are authority
// to replace retained facts. This performs no extra network scan, does not renew
// the checkpoint/source deadlines, and never changes the serving configuration.
func (s *Service) refreshRetainedDNSFactsFromObservations(old *dnsServingState, observedPlan *platformconfig.DNSReadinessPlan, observations []dnsReadinessFact, now time.Time) *dnsServingState {
	if old == nil || !old.record.Positive || observedPlan == nil || len(observations) == 0 {
		return nil
	}
	requirements := make(map[string]platformconfig.DNSReadinessProbe, len(observedPlan.Probes))
	for _, requirement := range observedPlan.Probes {
		requirements[requirement.ID] = requirement
	}
	// The retained policy may have a shorter freshness limit than the candidate.
	// Clipping a deadline is allowed; changing CheckedAt or extending it is not.
	clipped := append([]dnsReadinessFact(nil), observations...)
	oldRequirements := make(map[string]platformconfig.DNSReadinessProbe, len(old.payload.Plan.Probes))
	for _, requirement := range old.payload.Plan.Probes {
		oldRequirements[requirement.ID] = requirement
	}
	for i := range clipped {
		if requirement, ok := oldRequirements[clipped[i].ProbeID]; ok {
			limit := clipped[i].Proof.CheckedAt.Add(time.Duration(platformconfig.DNSReadinessFactMaxAge(requirement, old.payload.Policy.DNSReadiness)) * time.Second)
			if limit.Before(clipped[i].Proof.ValidUntil) {
				clipped[i].Proof.ValidUntil = limit
			}
		}
	}
	boundDNSRouteSourceFacts(old.payload, clipped, now)
	valid := validDNSReadinessFacts(old.payload.Plan, old.payload.Policy.DNSReadiness, clipped, now)
	facts := append([]dnsReadinessFact(nil), old.facts...)
	changed := false
	for i, previous := range facts {
		requirement, known := oldRequirements[previous.ProbeID]
		observedRequirement, observed := requirements[previous.ProbeID]
		fact, fresh := valid[previous.ProbeID]
		if known && observed && reflect.DeepEqual(requirement, observedRequirement) && fresh &&
			fact.Proof.CheckedAt.After(previous.Proof.CheckedAt) &&
			dnsFactMatchesRelease(fact, old.record.Parent, old.record.Candidate, old.routeID, old.payload) {
			observation := []dnsReadinessFact{fact}
			s.observeRetainedDNSRouteRecovery(old, observation, now)
			facts[i] = observation[0]
			changed = true
		}
	}
	if !changed {
		return nil
	}
	st, err := buildDNSServingState(old.record, old.payload, old.routeID, s.Config.DNSNodeID, s.Config.EdgeGroupID, facts, now)
	if err != nil {
		return nil
	}
	// A partial proof refresh is not a completed assignment scan. Preserve its
	// cache clock and pending transition, including all original expiry bounds.
	st.checkedAt = old.checkedAt
	st.transition = old.transition
	st.fallback = "assignment_changed"
	return st
}
