package managedappstatus

import (
	"strings"
	"time"

	"fugue/internal/model"
)

const MaxObservationAge = time.Minute

// ObservationFresh reports whether the observation is current and marked fresh.
func ObservationFresh(observed *model.AppObservedStatus, now time.Time) bool {
	if observed == nil || !observed.Fresh {
		return false
	}
	return TimestampFresh(observed.ObservedAt, now)
}

// DisableConverged is the publication gate for the disable idempotency
// response. Durable app status is only a control-plane projection and can lag
// a workload that still exists in Kubernetes, so a zero desired/current count
// is not enough to claim that disable completed. Require a fresh, cluster-
// identified observation with explicit zero counts for both the controller's
// desired replicas and the physical serving cohort. Missing evidence fails
// closed and lets the normal scale-to-zero reconcile repair the state.
func DisableConverged(app model.App, now time.Time) bool {
	if app.Spec.Replicas != 0 || app.Status.CurrentReplicas != 0 {
		return false
	}
	observed := app.ObservedStatus
	if observed == nil ||
		!ObservationFresh(observed, now) ||
		!strings.EqualFold(strings.TrimSpace(observed.Phase), "disabled") ||
		observed.DesiredReplicas != 0 ||
		strings.TrimSpace(observed.ClusterID) == "" ||
		strings.TrimSpace(observed.EvidenceSource) == "" ||
		observed.RuntimeObjectPresent == nil || !*observed.RuntimeObjectPresent ||
		observed.NamespacePresent == nil || !*observed.NamespacePresent ||
		observed.ReadyReplicas == nil || *observed.ReadyReplicas != 0 ||
		observed.PhysicalReplicas == nil || *observed.PhysicalReplicas != 0 ||
		observed.PhysicalDesired == nil || *observed.PhysicalDesired != 0 ||
		len(observed.InvariantViolations) > 0 {
		return false
	}
	return true
}

// TimestampFresh applies the publication age limit and clock-skew allowance.
func TimestampFresh(observedAt, now time.Time) bool {
	if observedAt.IsZero() {
		return false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	observedAt = observedAt.UTC()
	return !observedAt.After(now.Add(30*time.Second)) && now.Sub(observedAt) <= MaxObservationAge
}

// ReadyForServing is the shared publication gate for API surfaces
// that may render or advertise a green/active application. The observed phase
// is necessary but never sufficient: every affirmative field must belong to a
// fresh, cluster-identified, current-generation observation.
func ReadyForServing(app model.App, now time.Time) bool {
	observed := app.ObservedStatus
	if observed == nil || !strings.EqualFold(strings.TrimSpace(observed.Phase), "deployed") ||
		app.Spec.Replicas <= 0 || observed.DesiredReplicas != app.Spec.Replicas ||
		!ObservationFresh(observed, now) || strings.TrimSpace(observed.ClusterID) == "" ||
		strings.TrimSpace(observed.EvidenceSource) == "" || observed.Generation <= 0 ||
		observed.ObservedGeneration < observed.Generation || len(observed.InvariantViolations) > 0 {
		return false
	}
	if observed.RuntimeObjectPresent == nil || !*observed.RuntimeObjectPresent ||
		observed.NamespacePresent == nil || !*observed.NamespacePresent ||
		observed.EndpointPresent == nil || !*observed.EndpointPresent ||
		observed.EndpointReady == nil || !*observed.EndpointReady ||
		observed.ImagePresent == nil || !*observed.ImagePresent {
		return false
	}
	if observed.ReadyReplicas == nil || *observed.ReadyReplicas < app.Spec.Replicas ||
		observed.PhysicalReplicas == nil || *observed.PhysicalReplicas < app.Spec.Replicas ||
		(app.Spec.Replicas > 0 && *observed.PhysicalReplicas <= 0) {
		return false
	}
	if observed.PhysicalDesired != nil && *observed.PhysicalDesired < app.Spec.Replicas {
		return false
	}
	if model.AppHasClusterService(app.Spec) || model.AppSSHEnabled(app.Spec) {
		if observed.ServicePresent == nil || !*observed.ServicePresent {
			return false
		}
	}
	return true
}
