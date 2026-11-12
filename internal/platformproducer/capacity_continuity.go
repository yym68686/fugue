package platformproducer

import "time"

// Recovery is a completed transition, distinct from observation freshness. The
// signed v2 policy explicitly bounds how long that transition may be retained.
// Serving still independently requires current inventory and route/TLS proofs.
func (o OfflineRecoveryObservation) continuityReady(h OfflineEdge, at time.Time) bool {
	p := h.Recovery
	validResult := o.LastResult == "capacity_verified" || o.LastResult == "reference_unavailable" || o.LastResult == "evidence_unavailable" || o.LastResult == "code_observation_unavailable"
	return p != nil && p.Validate() == nil && p.Capacity != nil && p.Capacity.Version == CapacityContinuityVersion &&
		validResult && o.EdgeID == h.EdgeID && o.DeclarationDigest == OfflineRecoveryDigest(h) && o.Admitted &&
		o.Successes == p.SuccessThreshold && o.Failures == 0 && !at.IsZero() &&
		!o.LastAttempt.IsZero() && !o.LastAttempt.After(at) && !o.LastSuccess.IsZero() && !o.LastSuccess.After(o.LastAttempt) &&
		!o.FirstSuccess.IsZero() && !o.FirstSuccess.After(o.LastSuccess) && o.LastSuccess.Sub(o.FirstSuccess) >= time.Duration(p.Capacity.MinimumStableSeconds)*time.Second &&
		o.ProofValidUntil.After(o.LastSuccess) && at.Sub(o.LastSuccess) <= time.Duration(p.Capacity.RetainAdmissionSeconds)*time.Second &&
		o.Capacity.Passes(*p.Capacity, h.EdgeID) && o.Capacity.CheckedAt.Equal(o.LastSuccess)
}

func (o OfflineRecoveryObservation) ReviewOverdue(h OfflineEdge, at time.Time) bool {
	return o.Ready(h, at) && (at.Sub(o.LastSuccess) > time.Duration(h.Recovery.FreshnessSeconds)*time.Second || o.LastResult != "capacity_verified")
}

func (o OfflineRecoveryObservation) recordContinuity(h OfflineEdge, at time.Time, passed bool, until time.Time, capacity *CapacityRecoveryEvidence, result string) OfflineRecoveryObservation {
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
		return o
	}
	wasReady := o.continuityReady(h, at)
	if passed {
		if !sameCapacityCode(o.Capacity, capacity) || !wasReady && (o.LastSuccess.IsZero() || at.Sub(o.LastSuccess) > time.Duration(p.FreshnessSeconds)*time.Second) {
			o.Successes, o.FirstSuccess, o.Admitted = 0, time.Time{}, false
		}
		if o.Successes == 0 {
			o.FirstSuccess = at
		}
		o.Successes, o.Failures = min(o.Successes+1, p.SuccessThreshold), 0
		o.LastSuccess, o.ProofValidUntil = at, until
		copy := *capacity
		o.Capacity = &copy
		o.Admitted = o.Successes == p.SuccessThreshold && at.Sub(o.FirstSuccess) >= time.Duration(p.Capacity.MinimumStableSeconds)*time.Second
	} else {
		switch result {
		case "reference_unavailable", "evidence_unavailable", "code_observation_unavailable":
			// A failed control sample or collector is inconclusive. Preserve the
			// last completed transition without renewing its success timestamp.
			if !wasReady {
				o.Successes, o.FirstSuccess, o.Admitted = 0, time.Time{}, false
			}
		default:
			// A new subject failure, identity conflict or route failure retracts
			// admission. It cannot be masked by older successful measurements.
			o.Successes, o.FirstSuccess, o.Admitted = 0, time.Time{}, false
			o.Failures = min(o.Failures+1, p.FailureThreshold)
		}
	}
	o.EdgeID, o.DeclarationDigest = h.EdgeID, OfflineRecoveryDigest(h)
	o.LastAttempt, o.LastResult = at, result
	return o
}
