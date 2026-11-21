package dnsserver

import (
	"time"
)

// Use the retained artifact's independently authenticated semantic history, not
// the publication-specific transport scheduler history. The keys are derived
// from that artifact's complete source snapshot and constraints on load/read.
// Never borrow an alias installed by a different candidate, and never count a
// fresh proof from an unrecognized publication as positive continuity.
func (s *Service) observeRetainedDNSRouteRecovery(old *dnsServingState, facts []dnsReadinessFact, now time.Time, successor *dnsReleaseBridge) {
	if old == nil || old.payload.routeSources == nil || len(old.payload.sourceRecoveryKeys) == 0 {
		return
	}
	var successorProbes map[string][]string
	if successor != nil && successor.payload.routeSources != nil &&
		successor.candidate.Artifact.ID == old.record.Candidate.Artifact.ID &&
		successor.candidate.Artifact.ContentHash == old.record.Candidate.Artifact.ContentHash {
		successorProbes = make(map[string][]string, len(successor.payload.sourceRecoveryKeys))
		for id, key := range successor.payload.sourceRecoveryKeys {
			successorProbes[key] = append(successorProbes[key], id)
		}
	}
	scheduler := s.retainedDNSProbes.shareHistory(s.probeHistoryStore())
	matchedSuccessors := 0
	scheduler.mu.Lock()
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
			// A local fallback can interrogate the old binding after the
			// selected source has advanced. A proof of that authenticated
			// successor is still unusable as old-release recovery, but it
			// must not reset the same behavior's shared recovery window.
			// The selected loop owns successor positives and all serving
			// rechecks. An unknown or changed behavior still resets here.
			matchedSuccessor := false
			for _, id := range successorProbes[key] {
				proof := observation
				proof.ProbeID = id
				if dnsFactMatchesRelease(proof, successor.parent, successor.candidate, successor.routeID, successor.payload) {
					matchedSuccessor = true
					break
				}
			}
			if matchedSuccessor {
				matchedSuccessors++
				fact.ProbeHistory = nil
				fact.HistoryObservation = nil
				continue
			}
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
	scheduler.mu.Unlock()
	if matchedSuccessors > 0 && s.Logger != nil {
		s.Logger.Printf("DNS retained recovery ignored %d probes bound to the observed successor", matchedSuccessors)
	}
}
