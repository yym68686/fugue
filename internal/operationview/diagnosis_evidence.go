package operationview

import (
	"fmt"
	"strings"

	"fugue/internal/model"
)

// SetDefaultDiagnosisConfidence records the base confidence before any evidence lookup.
func SetDefaultDiagnosisConfidence(op model.Operation, diagnosis *model.OperationDiagnosis) {
	if strings.TrimSpace(diagnosis.Confidence) == "" {
		diagnosis.Confidence = defaultDiagnosisConfidence(op, *diagnosis)
	}
}

// ApplyDiagnosisEvidence ranks historical observations for an operation and
// records absent evidence explicitly instead of inferring a root cause.
func ApplyDiagnosisEvidence(op model.Operation, evidence []model.OperationEvidence, diagnosis *model.OperationDiagnosis) {
	if attachSchedulingDecisionDiagnosis(op, evidence, diagnosis) {
		return
	}
	best, ok := primaryDiagnosisEvidence(evidence)
	if ok {
		diagnosis.PrimaryEvidenceID = best.ID
		diagnosis.Confidence = strongestDiagnosisConfidence(diagnosis.Confidence, best.Confidence)
		cause := operationDiagnosisCauseFromEvidence(best)
		if best.Confidence == model.OperationEvidenceConfidenceConfirmed {
			diagnosis.ConfirmedCause = &cause
			diagnosis.ProbableCause = nil
		} else if diagnosis.ConfirmedCause == nil {
			diagnosis.ProbableCause = &cause
		}
		evidenceLine := strings.TrimSpace(best.Summary)
		if evidenceLine != "" {
			evidenceLine = fmt.Sprintf("primary evidence %s (%s): %s", best.ID, best.Type, evidenceLine)
		} else {
			evidenceLine = fmt.Sprintf("primary evidence %s (%s)", best.ID, best.Type)
		}
		diagnosis.Evidence = appendUniqueStrings(diagnosis.Evidence, evidenceLine)
		if strings.TrimSpace(diagnosis.Hint) == "" && (best.Type == model.OperationEvidenceTypeRolloutPreviousLogs || best.Type == model.OperationEvidenceTypeRolloutCurrentLogs) {
			diagnosis.Hint = "Inspect the confirmed container log evidence before retrying the deploy."
		}
	}
	if strings.EqualFold(op.Status, model.OperationStatusFailed) && !ok && shouldRequireFailureEvidence(diagnosis.Category) {
		diagnosis.Confidence = model.OperationEvidenceConfidenceInsufficientEvidence
		diagnosis.MissingEvidence = missingFailureEvidence(evidence)
		if len(diagnosis.MissingEvidence) == 0 {
			diagnosis.MissingEvidence = []string{"previous_container_logs", "pod_events", "deployment_snapshot"}
		}
		diagnosis.RecommendedNextActions = appendUniqueStrings(diagnosis.RecommendedNextActions,
			"rerun the deploy after the operation evidence collector is enabled",
			"inspect application logs around the operation failure window",
		)
	}
	return
}

func defaultDiagnosisConfidence(op model.Operation, diagnosis model.OperationDiagnosis) string {
	switch strings.TrimSpace(diagnosis.Category) {
	case "completed":
		return model.OperationEvidenceConfidenceConfirmed
	case "image-manifest-missing", "builder-placement-blocked", "builder-placement-capacity", "builder-placement-reservation", "builder-placement-lock", "builder-placement-unavailable":
		return model.OperationEvidenceConfidenceEvidenceBacked
	}
	switch strings.TrimSpace(op.Status) {
	case model.OperationStatusCompleted:
		return model.OperationEvidenceConfidenceConfirmed
	case model.OperationStatusFailed:
		return model.OperationEvidenceConfidenceProbable
	case model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent:
		return model.OperationEvidenceConfidenceEvidenceBacked
	default:
		return model.OperationEvidenceConfidenceInsufficientEvidence
	}
}

func shouldRequireFailureEvidence(category string) bool {
	switch strings.TrimSpace(category) {
	case "", "failed":
		return true
	default:
		return false
	}
}

func primaryDiagnosisEvidence(items []model.OperationEvidence) (model.OperationEvidence, bool) {
	priority := map[string]int{
		model.OperationEvidenceTypeRolloutPreviousLogs:        100,
		model.OperationEvidenceTypeRolloutCurrentLogs:         99,
		model.OperationEvidenceTypeImagePullFailure:           95,
		model.OperationEvidenceTypeSchedulerFailure:           95,
		model.OperationEvidenceTypeVolumeMountFailure:         95,
		model.OperationEvidenceTypeReadinessProbeFailure:      90,
		model.OperationEvidenceTypeLivenessProbeFailure:       90,
		model.OperationEvidenceTypeStartupProbeFailure:        90,
		model.OperationEvidenceTypeRolloutContainerTerminated: 85,
		model.OperationEvidenceTypeRolloutPodFailure:          70,
		model.OperationEvidenceTypeRolloutKubernetesEvent:     50,
		model.OperationEvidenceTypeCollectorError:             10,
	}
	var best model.OperationEvidence
	bestScore := -1
	for _, item := range items {
		score, ok := priority[item.Type]
		if !ok {
			continue
		}
		if item.Confidence == model.OperationEvidenceConfidenceConfirmed {
			score += 10
		}
		if item.Severity == model.OperationEvidenceSeverityError {
			score += 5
		}
		if score > bestScore || (score == bestScore && item.CollectedAt.After(best.CollectedAt)) {
			best = item
			bestScore = score
		}
	}
	return best, bestScore >= 0
}

func operationDiagnosisCauseFromEvidence(e model.OperationEvidence) model.OperationDiagnosisCause {
	category := "rollout_failure"
	source := e.Source
	switch e.Type {
	case model.OperationEvidenceTypeRolloutPreviousLogs:
		category = "application_startup_failure"
		source = "previous_container_logs"
	case model.OperationEvidenceTypeRolloutCurrentLogs:
		category = "application_startup_failure"
		source = "current_container_logs"
	case model.OperationEvidenceTypeImagePullFailure:
		category = "image_pull_failure"
		source = "kubernetes_event"
	case model.OperationEvidenceTypeSchedulerFailure:
		category = "scheduler_failure"
		source = "kubernetes_event"
	case model.OperationEvidenceTypeVolumeMountFailure:
		category = "volume_mount_failure"
		source = "kubernetes_event"
	case model.OperationEvidenceTypeReadinessProbeFailure:
		category = "readiness_probe_failure"
		source = "kubernetes_event"
	case model.OperationEvidenceTypeLivenessProbeFailure:
		category = "liveness_probe_failure"
		source = "kubernetes_event"
	case model.OperationEvidenceTypeStartupProbeFailure:
		category = "startup_probe_failure"
		source = "kubernetes_event"
	case model.OperationEvidenceTypeRolloutContainerTerminated:
		category = "container_terminated"
	}
	message := firstNonEmptyDiagnosisText(operationEvidencePayloadString(e.Payload, "log_tail"), e.Message, e.Summary)
	return model.OperationDiagnosisCause{
		Category: category,
		Source:   source,
		Message:  message,
		Reason:   e.Reason,
	}
}

func operationEvidencePayloadString(payload map[string]any, key string) string {
	if len(payload) == 0 {
		return ""
	}
	value, ok := payload[key]
	if !ok || value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func strongestDiagnosisConfidence(current, candidate string) string {
	rank := map[string]int{
		model.OperationEvidenceConfidenceInsufficientEvidence: 0,
		model.OperationEvidenceConfidenceProbable:             1,
		model.OperationEvidenceConfidenceEvidenceBacked:       2,
		model.OperationEvidenceConfidenceConfirmed:            3,
	}
	current = strings.TrimSpace(current)
	candidate = strings.TrimSpace(candidate)
	if rank[candidate] > rank[current] {
		return candidate
	}
	if current == "" {
		return candidate
	}
	return current
}

func missingFailureEvidence(items []model.OperationEvidence) []string {
	hasPreviousLogs := false
	hasPodEvents := false
	hasDeploymentSnapshot := false
	for _, item := range items {
		switch item.Type {
		case model.OperationEvidenceTypeRolloutPreviousLogs:
			hasPreviousLogs = true
		case model.OperationEvidenceTypeRolloutKubernetesEvent, model.OperationEvidenceTypeImagePullFailure, model.OperationEvidenceTypeSchedulerFailure, model.OperationEvidenceTypeVolumeMountFailure, model.OperationEvidenceTypeReadinessProbeFailure, model.OperationEvidenceTypeLivenessProbeFailure, model.OperationEvidenceTypeStartupProbeFailure:
			hasPodEvents = true
		case model.OperationEvidenceTypeRolloutDeploymentSnapshot:
			hasDeploymentSnapshot = true
		}
	}
	missing := []string{}
	if !hasPreviousLogs {
		missing = append(missing, "previous_container_logs")
	}
	if !hasPodEvents {
		missing = append(missing, "pod_events")
	}
	if !hasDeploymentSnapshot {
		missing = append(missing, "deployment_snapshot")
	}
	return missing
}

func appendUniqueStrings(values []string, additions ...string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values)+len(additions))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	for _, value := range additions {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func firstNonEmptyDiagnosisText(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			continue
		}
		return strings.TrimSpace(value)
	}
	return ""
}
