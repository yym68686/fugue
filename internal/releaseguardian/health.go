package releaseguardian

import "strings"

type Decision struct {
	State            ReleaseState
	Reason           string
	RolloutEligible  bool
	RollbackEligible bool
}

// Classify keeps attribution component-scoped. Dependency-only and
// route-only failures remain visible but cannot roll back a locally healthy
// component. Unknown evidence always fails closed.
func Classify(currentRecordDigest, targetRecordDigest string, health HealthSnapshot) Decision {
	if health.Local.State == HealthUnknown || health.Dependency.State == HealthUnknown || health.Route.State == HealthUnknown {
		return Decision{State: StateRecoveryRequired, Reason: joinedReason("health evidence is incomplete", health)}
	}
	if currentRecordDigest != targetRecordDigest {
		return Decision{State: StateRolloutPending, Reason: "desired release differs from the current component record", RolloutEligible: true}
	}
	if health.Local.State == HealthDegraded {
		return Decision{State: StateRollbackPending, Reason: joinedReason("component-local health is degraded", health), RollbackEligible: true}
	}
	if health.Dependency.State == HealthDegraded {
		return Decision{State: StateDegraded, Reason: joinedReason("component dependency is degraded", health)}
	}
	if health.Route.State == HealthDegraded {
		return Decision{State: StateDegraded, Reason: joinedReason("independent route canary is degraded", health)}
	}
	return Decision{State: StateStable, Reason: "local, dependency, and route health are verified"}
}

func joinedReason(prefix string, health HealthSnapshot) string {
	parts := []string{prefix}
	for _, layer := range []struct {
		name string
		data LayerHealth
	}{{"local", health.Local}, {"dependency", health.Dependency}, {"route", health.Route}} {
		if layer.data.State == HealthHealthy || strings.TrimSpace(layer.data.Reason) == "" {
			continue
		}
		parts = append(parts, layer.name+"="+strings.TrimSpace(layer.data.Reason))
	}
	return strings.Join(parts, "; ")
}
