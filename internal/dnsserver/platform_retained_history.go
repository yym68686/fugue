package dnsserver

import (
	"time"
)

// Use the retained artifact's independently authenticated semantic history, not
// the publication-specific transport scheduler history. The keys are derived
// from that artifact's complete source snapshot and constraints on load/read.
// Never borrow an alias installed by a different candidate, and never count a
// fresh proof from an unrecognized publication as positive continuity.
func (s *Service) observeRetainedDNSRouteRecovery(old *dnsServingState, facts []dnsReadinessFact, now time.Time) {
	if old == nil || old.payload.routeSources == nil || len(old.payload.sourceRecoveryKeys) == 0 {
		return
	}
	scheduler := s.retainedDNSProbes.shareHistory(s.probeHistoryStore())
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	for i := range facts {
		fact := &facts[i]
		key, ok := old.payload.sourceRecoveryKeys[fact.ProbeID]
		if !ok {
			fact.ProbeHistory = nil
			fact.HistoryObservation = nil
			continue
		}
		observation := *fact
		if observation.Ready && !dnsFactMatchesRelease(observation, old.record.Parent, old.record.Candidate, old.routeID, old.payload) {
			observation.Ready = false
			observation.Reason = "traffic_release_mismatch"
		}
		if observation.Reason == "not_observed" {
			continue
		}
		scheduler.observeProbeHistoryKey(key, &observation, old.payload.Policy.DNSReadiness, now)
		fact.ProbeHistory = observation.ProbeHistory
		fact.HistoryObservation = cloneDNSHistoryObservation(observation.HistoryObservation)
	}
}
