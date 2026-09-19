package releaseguardian

import (
	"bytes"
	"strings"
)

// CommittedMonitorRecoveryEligible permits a read-only forward reconciliation,
// never a rollout or rollback. Only the exact published candidate can proceed;
// the executor must still prove its CurrentAuthority, Front/Worker cohort,
// image, manifest, health and immutable predecessor under the component Lease.
func CommittedMonitorRecoveryEligible(s Snapshot) bool {
	p := s.PreviousStatus
	r := s.Bundle.Release
	if !s.Managed || s.DesiredRecordMissing || s.Record.Validate() != nil || s.Key != s.Record.Key() ||
		s.Desired.Key() != s.Key || s.Desired.RecordDigest != s.Record.RecordDigest || s.CurrentRecordDigest == s.Desired.RecordDigest ||
		s.LastSuccessfulLKG != s.CurrentRecordDigest || s.Bundle.Prepared.ConfigSHA != s.Record.ConfigSHA || s.Bundle.Prepared.Component != s.Key.Component ||
		r.ComponentID != s.Key.Component || r.Delivery == nil || r.Delivery.Writer != "guardian" || r.Delivery.Group != s.Key.Group ||
		r.Transition == nil || r.Transition.Type != "edge-group-ab" || r.Transition.EdgeGroupAB == nil ||
		s.Health.Dependency.State != HealthHealthy || s.Health.Route.State != HealthHealthy {
		return false
	}
	// Resume the second metadata CAS if an earlier recovery persisted the exact
	// verified monitor but lost its DesiredRelease acknowledgement.
	if s.Health.Local.State == HealthHealthy && committedMonitorMatchesCandidate(s) {
		return true
	}
	return s.Record.LKGRecordDigest == s.CurrentRecordDigest && p != nil && p.Key() == s.Key && p.State == StateRecoveryRequired &&
		p.CurrentRecordDigest == s.CurrentRecordDigest && p.TargetRecordDigest == s.Desired.RecordDigest && p.LastSuccessfulLKG == s.CurrentRecordDigest &&
		p.RolloutReceiptDigest == "" && p.RollbackReceiptDigest == "" &&
		p.Reason == "desired rollout is fenced because the current component is degraded; local=DaemonSet release identity differs from the stable record" &&
		s.Health.Local.State == HealthDegraded && s.Health.Local.Reason == "DaemonSet release identity differs from the stable record"

}

func committedMonitorMatchesCandidate(s Snapshot) bool {
	canonical, monitor, err := canonicalStableReleaseRecord(s.Key, s.CurrentMonitorData)
	if err != nil || canonical.RecordDigest == s.Desired.RecordDigest || (s.CurrentRecordDigest != canonical.RecordDigest && s.CurrentRecordDigest != s.Record.RecordDigest) || !SameCommittedReleaseTarget(s.Record, canonical) || monitor.ExecutionPlanDigest != s.Bundle.Prepared.PlanDigest {
		return false
	}
	for _, name := range executionFileNames {
		if len(bytes.TrimSpace(s.Bundle.Files[name])) == 0 || strings.TrimSpace(s.CurrentMonitorData[name]) != string(bytes.TrimSpace(s.Bundle.Files[name])) {
			return false
		}
	}
	return true
}
