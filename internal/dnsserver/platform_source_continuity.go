package dnsserver

import (
	"errors"
	"slices"
	"time"

	"fugue/internal/dnsroutesource"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

// Recovery is physical observation continuity, never a substitute for current
// authority. Build has authenticated every source; only an exact fresh matching
// publication contributes a success. Different releases of unchanged behavior
// share history, while DNS/producer/policy/endpoint/behavior changes cannot.
func dnsRouteRecoveryKeys(dns model.PlatformArtifact, plans *dnsroutesource.Plans) (map[string]string, error) {
	type producer struct{ ID, Digest string }
	producers := map[string]producer{}
	for _, scope := range plans.Snapshot.Scopes {
		for _, pub := range scope.Publications {
			producers[scope.ScopeKey+"\x00"+pub.Release.ID] = producer{pub.ProducerPolicy.ID, pub.ProducerPolicy.ContentHash}
		}
	}
	keys := make(map[string]string, len(plans.Probes))
	for id, requirement := range plans.Probes {
		binding := plans.Bindings[requirement.CellPublicationDigest]
		if binding == nil {
			return nil, errors.New("DNS recovery source binding missing")
		}
		approved, ok := producers[binding.ScopeKey+"\x00"+binding.ReleaseID]
		if !ok {
			return nil, errors.New("DNS recovery producer binding missing")
		}
		requirement.ID, requirement.CellPublicationDigest = "", ""
		if requirement.PreviousAuthority != nil {
			prior := *requirement.PreviousAuthority
			prior.PublicationDigest = ""
			requirement.PreviousAuthority = &prior
		}
		key, err := platformconfig.Digest(struct {
			DNSID, DNSDigest, Scope, SourcePolicy string
			Producer                              producer
			Probe                                 platformconfig.DNSReadinessProbe
		}{dns.ID, dns.ContentHash, binding.ScopeKey, binding.PolicyDigest, approved, requirement})
		if err != nil {
			return nil, err
		}
		keys[id] = "dns-source-recovery:" + key
	}
	return keys, nil
}

func (s *Service) observeDNSRouteRecovery(dns model.PlatformArtifact, plans *dnsroutesource.Plans, facts []dnsReadinessFact, policy *platformconfig.DNSReadinessPolicy, observedAt time.Time) error {
	keys, err := dnsRouteRecoveryKeys(dns, plans)
	if err != nil {
		return err
	}
	groups := map[string][]int{}
	for i := range facts {
		fact := &facts[i]
		if fact.Ready && !plans.Matches(plans.Probes[fact.ProbeID], fact.Proof) {
			fact.Ready, fact.Reason = false, "traffic_release_mismatch"
		}
		fact.ProbeHistory = nil
		if fact.Reason == "not_observed" {
			continue
		}
		key, ok := keys[fact.ProbeID]
		if !ok {
			return errors.New("DNS recovery probe binding missing")
		}
		groups[key] = append(groups[key], i)
	}
	scheduler := s.servingDNSProbes.shareHistory(&s.dnsProbeHistories)
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	for key, indices := range groups {
		chosen := indices[0]
		for _, i := range indices {
			if facts[i].Ready {
				chosen = i
				break
			}
		}
		// The one physical observation may match one of several authorized release
		// variants. A mismatch against another variant of the SAME behavior is not
		// a failed endpoint observation and must not erase that success.
		observation := facts[chosen]
		scheduler.observeProbeHistoryKey(key, &observation, policy, observedAt)
		history := scheduler.historyStore
		history.mu.Lock()
		for _, i := range indices {
			id := facts[i].ProbeID
			entry, exists := history.values[id]
			if !exists && len(history.values) >= 16384 {
				history.mu.Unlock()
				return errors.New("DNS recovery history bound exceeded")
			}
			entry.recoveryKey = key
			if observedAt.After(entry.observedAt) {
				entry.observedAt = observedAt
			}
			history.values[id] = entry
		}
		history.mu.Unlock()

		for _, i := range indices {
			if facts[i].Ready {
				facts[i].ProbeHistory = slices.Clone(observation.ProbeHistory)
			}
		}
	}
	return nil
}
