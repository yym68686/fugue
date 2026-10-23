package dnsserver

import (
	"slices"
	"sort"
	"sync"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

const dnsProbeHistoryLimit = 16

// A history is observation only. It cannot make an expired/wrong publication
// proof valid; materialization checks those independent gates first.
func dnsAvailabilityRecovered(policy *model.DNSAvailabilityPolicy, fact dnsReadinessFact, maxGap time.Duration, now time.Time) bool {
	if model.ValidateDNSAvailabilityPolicy(policy) != nil || !fact.Ready || !fact.Proof.ValidUntil.After(now) {
		return false
	}
	history := fact.ProbeHistory
	if len(history) < policy.RecoverySuccesses || len(history) > dnsProbeHistoryLimit || !history[len(history)-1].Equal(fact.Proof.CheckedAt) {
		return false
	}
	for i, at := range history {
		if at.IsZero() || at.After(now) || i > 0 && (!at.After(history[i-1]) || at.Sub(history[i-1]) > maxGap) {
			return false
		}
	}
	return history[len(history)-1].Sub(history[0]) >= time.Duration(policy.RecoveryMinSeconds)*time.Second
}

// The candidate, serving and retained-LKG schedulers share only observation
// continuity. Their assignment, authorization, proof validation and retry
// budgets remain independent. Losing the control plane cannot reset a healthy
// endpoint's recovery merely because another scheduler performs the probe.
type dnsProbeHistoryStore struct {
	mu     sync.Mutex
	values map[string]dnsProbeHistory
}

type dnsProbeHistory struct {
	successes  []time.Time
	failedAt   time.Time
	observedAt time.Time
}

func (scheduler *dnsReadinessScheduler) shareHistory(history *dnsProbeHistoryStore) *dnsReadinessScheduler {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	scheduler.historyStore = history
	return scheduler
}

func (scheduler *dnsReadinessScheduler) observeProbeHistory(fact *dnsReadinessFact, policy *platformconfig.DNSReadinessPolicy, observedAt time.Time) {
	if scheduler.historyStore == nil {
		scheduler.historyStore = &dnsProbeHistoryStore{}
	}
	store := scheduler.historyStore
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.values == nil {
		store.values = map[string]dnsProbeHistory{}
	}
	fact.ProbeHistory = nil
	if observedAt.IsZero() {
		return
	}
	entry, exists := store.values[fact.ProbeID]
	if !exists && len(store.values) >= 16384 {
		// Keep failed observations as barriers too. Delayed success from
		// another scheduler must never resurrect pre-failure continuity.
		for id, prior := range store.values {
			if observedAt.Sub(prior.observedAt) > 10*time.Minute {
				delete(store.values, id)
			}
		}
		if len(store.values) >= 16384 {
			return
		}
	}
	if !fact.Ready {
		if observedAt.After(entry.failedAt) {
			entry.failedAt = observedAt
		}
		entry.successes = nil
		if observedAt.After(entry.observedAt) {
			entry.observedAt = observedAt
		}
		store.values[fact.ProbeID] = entry
		return
	}
	at := fact.Proof.CheckedAt
	if at.IsZero() || at.After(observedAt) || !entry.failedAt.IsZero() && !at.After(entry.failedAt) {
		return
	}
	history := entry.successes
	if len(history) > 0 {
		last := history[len(history)-1]
		// A delayed positive result may not roll shared history backwards.
		if at.Before(last) {
			return
		}
		if at.Sub(last) > time.Duration(policy.FactFreshnessSeconds)*time.Second {
			history = nil
		}
	}
	if len(history) == 0 || at.After(history[len(history)-1]) {
		history = append(slices.Clone(history), at)
	}
	if len(history) > dnsProbeHistoryLimit {
		history = slices.Clone(history[len(history)-dnsProbeHistoryLimit:])
	}
	entry.successes = history
	if observedAt.After(entry.observedAt) {
		entry.observedAt = observedAt
	}
	store.values[fact.ProbeID] = entry
	fact.ProbeHistory = slices.Clone(history)
}

// The normal selector runs first. Fallback requires independent signed pool
// membership and the exact same positive per-record proof gates as normal use.
func availabilityFallback(record model.EdgeDNSRecord, ordered []model.EdgeDNSAnswerCandidate, decision edgeDNSCandidateOrderDecision) ([]model.EdgeDNSAnswerCandidate, edgeDNSCandidateOrderDecision) {
	policy := record.AnswerPolicy.Availability
	if policy == nil || len(ordered) > 0 {
		return ordered, decision
	}
	if model.ValidateDNSAvailabilityPolicy(policy) != nil {
		return nil, decision
	}
	positions := make(map[string]int, len(policy.OrderedEdgeIDs))
	for i, id := range policy.OrderedEdgeIDs {
		positions[id] = i
	}
	fallback := []model.EdgeDNSAnswerCandidate{}
	for _, c := range record.Candidates {
		if _, exists := positions[c.EdgeID]; exists && (len(record.AnswerPolicy.AllowedEdgeGroups) == 0 || slices.Contains(record.AnswerPolicy.AllowedEdgeGroups, c.EdgeGroupID)) && edgeDNSCandidateEligible(c, record.AnswerPolicy, false) {
			fallback = append(fallback, c)
		}
	}
	sort.Slice(fallback, func(i, j int) bool {
		if positions[fallback[i].EdgeID] != positions[fallback[j].EdgeID] {
			return positions[fallback[i].EdgeID] < positions[fallback[j].EdgeID]
		}
		return fallback[i].IP < fallback[j].IP
	})
	if len(fallback) > 0 {
		decision.AvailabilityFallback = true
		decision.Ranking = nil
		for i, candidate := range fallback {
			decision.Ranking = append(decision.Ranking, DNSDecisionRank{Candidate: candidate, SortScore: i})
		}
	}
	return fallback, decision
}

func availabilityMaterializationReason(entry dnsServingRecord, candidate model.EdgeDNSAnswerCandidate, now time.Time) string {
	byID := map[string]dnsReadinessFact{}
	for _, fact := range entry.facts {
		byID[fact.ProbeID] = fact
	}
	for _, record := range entry.plan.Records {
		for _, target := range record.Targets {
			if target.EdgeID != candidate.EdgeID || target.Address != candidate.IP {
				continue
			}
			for _, id := range target.ProbeIDs {
				fact, found := byID[id]
				if !found || !fact.Ready {
					return "readiness_unavailable"
				}
				if !fact.Proof.ValidUntil.After(now) {
					return "readiness_expired"
				}
				history := fact.ProbeHistory
				if len(history) < entry.record.AnswerPolicy.Availability.RecoverySuccesses || history[len(history)-1].Sub(history[0]) < time.Duration(entry.record.AnswerPolicy.Availability.RecoveryMinSeconds)*time.Second {
					return "availability_recovering"
				}
			}
		}
	}
	return "readiness_quorum_or_identity_unavailable"
}
