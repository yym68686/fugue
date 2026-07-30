package failover

import (
	"fmt"
	"strings"

	"fugue/internal/model"
)

const (
	AppClassificationReady   = "ready"
	AppClassificationCaution = "caution"
	AppClassificationBlocked = "blocked"
)

type AppAssessment struct {
	Classification string   `json:"classification"`
	Summary        string   `json:"summary"`
	Blockers       []string `json:"blockers,omitempty"`
	Warnings       []string `json:"warnings,omitempty"`
	RuntimeID      string   `json:"runtime_id,omitempty"`
	RuntimeType    string   `json:"runtime_type,omitempty"`
	RuntimeStatus  string   `json:"runtime_status,omitempty"`
}

func AssessApp(app model.App, runtime *model.Runtime) AppAssessment {
	blockers := MigrationBlockers(app)
	warnings := failoverWarnings(app, runtime)

	assessment := AppAssessment{
		Blockers: append([]string(nil), blockers...),
		Warnings: append([]string(nil), warnings...),
	}

	runtimeID := currentRuntimeID(app)
	if runtime != nil {
		assessment.RuntimeID = strings.TrimSpace(runtime.ID)
		assessment.RuntimeType = strings.TrimSpace(runtime.Type)
		assessment.RuntimeStatus = strings.TrimSpace(runtime.Status)
	} else if runtimeID != "" {
		assessment.RuntimeID = runtimeID
	}

	switch {
	case len(blockers) > 0:
		assessment.Classification = AppClassificationBlocked
		assessment.Summary = "blocked by " + joinHumanList(blockers)
	case len(warnings) > 0:
		assessment.Classification = AppClassificationCaution
		assessment.Summary = "review " + joinHumanList(warnings)
	default:
		assessment.Classification = AppClassificationReady
		assessment.Summary = "eligible for live transfer"
	}

	return assessment
}

func MigrationBlockers(app model.App) []string {
	blockers := make([]string, 0, 1)
	if app.Spec.Workspace != nil {
		blockers = append(blockers, "persistent storage")
	}
	if app.Spec.PersistentStorage != nil && !model.AppPersistentStorageSpecIsMigratable(app.Spec.PersistentStorage) {
		blockers = append(blockers, "persistent storage")
	}
	return blockers
}

func MigrationBlockerMessage(app model.App) string {
	blockers := MigrationBlockers(app)
	if len(blockers) == 0 {
		return ""
	}
	return fmt.Sprintf(
		"live transfer is blocked by %s; externalize state or move it to replicated operator-backed storage before failover",
		joinHumanList(blockers),
	)
}

func failoverWarnings(app model.App, runtime *model.Runtime) []string {
	warnings := make([]string, 0, 4)

	switch {
	case app.Spec.Replicas <= 0:
		warnings = append(warnings, "desired replicas are 0")
	case app.Spec.Replicas < 2:
		warnings = append(warnings, "desired replicas are below 2")
	}

	readyReplicas, observationReady := failoverObservedReplicaState(app)
	if app.Spec.Replicas > 0 && readyReplicas == 0 {
		warnings = append(warnings, "app is not currently serving replicas")
	}
	if app.Spec.Replicas > 0 && app.ObservedStatus != nil && !observationReady {
		warnings = append(warnings, "live runtime observation is not ready or complete")
	}

	if runtime == nil {
		if runtimeID := currentRuntimeID(app); runtimeID != "" {
			warnings = append(warnings, "runtime inventory is unavailable for "+runtimeID)
		}
		return warnings
	}

	status := strings.TrimSpace(strings.ToLower(runtime.Status))
	if status != "" && status != model.RuntimeStatusActive {
		warnings = append(warnings, "runtime status is "+strings.TrimSpace(runtime.Status))
	}

	switch strings.TrimSpace(strings.ToLower(runtime.Type)) {
	case model.RuntimeTypeManagedOwned, model.RuntimeTypeExternalOwned:
		warnings = append(warnings, "runtime is dedicated; Fugue does not infer redundant node placement")
	}

	return warnings
}

// failoverObservedReplicaState keeps continuity decisions aligned with the
// shared observed-status contract. The legacy fallback is intentionally used
// only when an older API response has no observed envelope at all; an explicit
// unknown/unavailable observation always wins over historical replicas.
func failoverObservedReplicaState(app model.App) (ready int, observedReady bool) {
	if observed := app.ObservedStatus; observed != nil {
		if observed.ReadyReplicas != nil {
			ready = *observed.ReadyReplicas
		}
		observedReady = observed.Fresh && strings.EqualFold(strings.TrimSpace(observed.Phase), "deployed") &&
			app.Spec.Replicas > 0 && observed.DesiredReplicas == app.Spec.Replicas &&
			observed.ClusterID != "" && observed.Generation > 0 && observed.ObservedGeneration >= observed.Generation &&
			ready >= app.Spec.Replicas &&
			observed.RuntimeObjectPresent != nil && *observed.RuntimeObjectPresent &&
			observed.NamespacePresent != nil && *observed.NamespacePresent &&
			observed.EndpointPresent != nil && *observed.EndpointPresent &&
			observed.EndpointReady != nil && *observed.EndpointReady &&
			observed.PhysicalReplicas != nil && *observed.PhysicalReplicas >= app.Spec.Replicas &&
			observed.ImagePresent != nil && *observed.ImagePresent && len(observed.InvariantViolations) == 0
		return ready, observedReady
	}
	return app.Status.CurrentReplicas, app.Status.CurrentReplicas >= app.Spec.Replicas
}

func currentRuntimeID(app model.App) string {
	runtimeID := strings.TrimSpace(app.Status.CurrentRuntimeID)
	if runtimeID != "" {
		return runtimeID
	}
	return strings.TrimSpace(app.Spec.RuntimeID)
}

func joinHumanList(items []string) string {
	switch len(items) {
	case 0:
		return ""
	case 1:
		return items[0]
	case 2:
		return items[0] + " and " + items[1]
	default:
		prefix := strings.Join(items[:len(items)-1], ", ")
		return prefix + ", and " + items[len(items)-1]
	}
}
