package cli

import (
	"strings"

	"fugue/internal/model"
)

// appObservedDisplayProjection is the CLI's one rendering adapter for the
// backend observed-status contract. It never promotes durable CurrentReplicas
// into a live count. When an older/partial API response has no observed
// envelope, active apps are shown as unknown until the caller receives a
// current cluster observation.
func appObservedDisplayProjection(app model.App) (phase string, ready int, runtimeID, message string) {
	runtimeID = firstNonEmptyTrimmed(app.Status.CurrentRuntimeID, app.Spec.RuntimeID)
	message = strings.TrimSpace(app.Status.LastMessage)
	if observed := app.ObservedStatus; observed != nil {
		phase = strings.TrimSpace(observed.Phase)
		if observed.ReadyReplicas != nil {
			ready = *observed.ReadyReplicas
		}
		runtimeID = firstNonEmptyTrimmed(observed.RuntimeID, runtimeID)
		message = firstNonEmptyTrimmed(observed.Message, observed.Reason, message)
		if !observed.Fresh && strings.EqualFold(phase, "deployed") {
			phase = "unknown"
		}
		if phase == "" {
			phase = "unknown"
		}
		return phase, ready, runtimeID, message
	}

	// No observed envelope means no current runtime proof. Preserve explicit
	// operation phases such as deploying/failed, but never render a durable
	// green phase or its historical replica count as live.
	phase = strings.TrimSpace(app.Status.Phase)
	if app.Spec.Replicas > 0 && isLegacyGreenAppPhase(phase) {
		phase = "unknown"
	}
	if phase == "" {
		phase = "unknown"
	}
	return phase, 0, runtimeID, message
}

func observedPhaseForDisplay(app model.App) string {
	phase, _, _, _ := appObservedDisplayProjection(app)
	return phase
}

func isLegacyGreenAppPhase(phase string) bool {
	switch strings.ToLower(strings.TrimSpace(phase)) {
	case "deployed", "running", "ready", "active", "healthy":
		return true
	default:
		return false
	}
}
