package platformproducer

import "time"

// A complete, authenticated round restores candidate eligibility. This is not a
// route lease: publishing and DNS still require current identity/health/TLS facts.
// Review freshness is advisory; a collector gap is not a new subject failure.
func (o OfflineRecoveryObservation) singleReady(h OfflineEdge, at time.Time) bool {
	p := h.Recovery
	validResult := o.LastResult == "capacity_verified" || inconclusiveCapacityResult(o.LastResult)
	return p != nil && p.Capacity != nil && p.Capacity.Version == CapacitySingleVersion && p.Validate() == nil &&
		validResult && o.EdgeID == h.EdgeID && o.DeclarationDigest == OfflineRecoveryDigest(h) &&
		o.Admitted && o.Successes == 1 && o.Failures == 0 && !at.IsZero() &&
		!o.LastAttempt.IsZero() && !o.LastAttempt.After(at) && !o.LastSuccess.IsZero() && !o.LastSuccess.After(o.LastAttempt) &&
		o.FirstSuccess.Equal(o.LastSuccess) && o.ProofValidUntil.After(o.LastSuccess) &&
		o.Capacity.Passes(*p.Capacity, h.EdgeID) && o.Capacity.CheckedAt.Equal(o.LastSuccess)
}

func inconclusiveCapacityResult(result string) bool {
	return result == "reference_unavailable" || result == "evidence_unavailable" || result == "code_observation_unavailable"
}

func (o OfflineRecoveryObservation) recordSingle(h OfflineEdge, at time.Time, passed bool, until time.Time, capacity *CapacityRecoveryEvidence, result string) OfflineRecoveryObservation {
	p := h.Recovery
	if p.Validate() != nil || at.IsZero() {
		return OfflineRecoveryObservation{}
	}
	if o.EdgeID != h.EdgeID || o.DeclarationDigest != OfflineRecoveryDigest(h) {
		o = OfflineRecoveryObservation{}
	}
	if !at.After(o.LastAttempt) {
		return o
	}
	passed = passed && result == "capacity_verified" && until.After(at) && capacity.Passes(*p.Capacity, h.EdgeID) && capacity.CheckedAt.Equal(at)
	if passed && !o.LastAttempt.IsZero() && at.Sub(o.LastAttempt) < time.Duration(p.ProbeIntervalSeconds)*time.Second {
		return o // A replay or early report cannot spend another scheduled round.
	}
	wasReady := o.singleReady(h, at)
	if passed {
		o.Successes, o.Failures, o.Admitted = 1, 0, true
		o.FirstSuccess, o.LastSuccess, o.ProofValidUntil = at, at, until
		copy := *capacity
		o.Capacity = &copy
	} else if !inconclusiveCapacityResult(result) || !wasReady {
		o.Successes, o.FirstSuccess, o.Admitted = 0, time.Time{}, false
		if !inconclusiveCapacityResult(result) {
			o.Failures = 1
		}
	}
	o.EdgeID, o.DeclarationDigest = h.EdgeID, OfflineRecoveryDigest(h)
	o.LastAttempt, o.LastResult = at, result
	return o
}
