package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"fugue/internal/model"
	"fugue/internal/observability"
	"fugue/internal/runtime"
)

const (
	operationEvidencePreviousLogTailLines = 200
	operationEvidenceCurrentLogTailLines  = 100
	operationEvidenceMaxLogBytes          = 64 * 1024
)

func (s *Service) captureDeploymentRolloutFailureEvidence(
	ctx context.Context,
	client *kubeClient,
	app model.App,
	operationID string,
	namespace string,
	deployment kubeDeployment,
	pods []kubePod,
	summary string,
) string {
	operationID = strings.TrimSpace(operationID)
	if s == nil || s.Store == nil || client == nil || operationID == "" {
		return ""
	}
	var primaryEvidenceID string
	for _, pod := range pods {
		if !podHasDeploymentTemplateIdentity(pod, deployment) {
			continue
		}
		podSummary := summarizeManagedAppPodFailure(pod)
		if podSummary == "" {
			continue
		}
		if strings.TrimSpace(summary) == "" {
			summary = podSummary
		}
		status, detail, stateKind := primaryFailingContainerStatus(pod)
		evidenceType := classifyPodFailureEvidenceType(pod, status, detail)
		evidence := model.OperationEvidence{
			TenantID:         app.TenantID,
			ProjectID:        app.ProjectID,
			AppID:            app.ID,
			OperationID:      operationID,
			Type:             evidenceType,
			Source:           model.OperationEvidenceSourceRolloutObserver,
			Severity:         model.OperationEvidenceSeverityError,
			Confidence:       model.OperationEvidenceConfidenceConfirmed,
			SubjectKind:      "Pod",
			SubjectName:      pod.Metadata.Name,
			SubjectNamespace: namespace,
			Summary:          summary,
			Message:          strings.TrimSpace(detail.Message),
			Reason:           firstNonEmptyControllerString(strings.TrimSpace(detail.Reason), strings.TrimSpace(pod.Status.Reason), stateKind),
			ContainerName:    strings.TrimSpace(status.Name),
			PodName:          strings.TrimSpace(pod.Metadata.Name),
			DeploymentName:   strings.TrimSpace(deployment.Metadata.Name),
			NodeName:         strings.TrimSpace(pod.Spec.NodeName),
			RedactionStatus:  model.OperationEvidenceRedactionNone,
			Payload: map[string]any{
				"phase":         pod.Status.Phase,
				"state_kind":    stateKind,
				"restart_count": status.RestartCount,
			},
		}
		if detail.ExitCode != 0 {
			exitCode := detail.ExitCode
			evidence.ExitCode = &exitCode
		}
		if t := parseKubeTimestamp(detail.StartedAt); !t.IsZero() {
			evidence.StartedAt = &t
		}
		if t := parseKubeTimestamp(detail.FinishedAt); !t.IsZero() {
			evidence.FinishedAt = &t
		}
		recorded := s.recordOperationEvidenceBestEffort(evidence)
		if primaryEvidenceID == "" {
			primaryEvidenceID = recorded
		}
		s.capturePodSnapshotEvidence(ctx, app, operationID, namespace, pod)
		s.capturePodLogsEvidence(ctx, client, app, operationID, namespace, pod, status.Name)
		s.captureKubernetesEventsEvidence(ctx, client, app, operationID, namespace, "Pod", pod.Metadata.Name)
		break
	}
	s.captureDeploymentSnapshotEvidence(ctx, app, operationID, namespace, deployment)
	s.captureReplicaSetSnapshotEvidence(ctx, client, app, operationID, namespace)
	s.captureKubernetesEventsEvidence(ctx, client, app, operationID, namespace, "Deployment", deployment.Metadata.Name)
	if primaryEvidenceID == "" && strings.TrimSpace(summary) != "" {
		primaryEvidenceID = s.recordOperationEvidenceBestEffort(model.OperationEvidence{
			TenantID:         app.TenantID,
			ProjectID:        app.ProjectID,
			AppID:            app.ID,
			OperationID:      operationID,
			Type:             model.OperationEvidenceTypeRolloutPodFailure,
			Source:           model.OperationEvidenceSourceRolloutObserver,
			Severity:         model.OperationEvidenceSeverityError,
			Confidence:       model.OperationEvidenceConfidenceEvidenceBacked,
			SubjectKind:      "Deployment",
			SubjectName:      deployment.Metadata.Name,
			SubjectNamespace: namespace,
			Summary:          summary,
			DeploymentName:   deployment.Metadata.Name,
			RedactionStatus:  model.OperationEvidenceRedactionNone,
		})
	}
	return primaryEvidenceID
}

func (s *Service) captureManagedAppRolloutFailureEvidence(
	ctx context.Context,
	app model.App,
	operationID string,
	namespace string,
	managed runtime.ManagedAppObject,
	summary string,
) string {
	operationID = strings.TrimSpace(operationID)
	if s == nil || s.Store == nil || operationID == "" || strings.TrimSpace(summary) == "" {
		return ""
	}
	payload := objectToPayloadMap(managed)
	return s.recordOperationEvidenceBestEffort(model.OperationEvidence{
		TenantID:         app.TenantID,
		ProjectID:        app.ProjectID,
		AppID:            app.ID,
		OperationID:      operationID,
		Type:             model.OperationEvidenceTypeRolloutPodFailure,
		Source:           model.OperationEvidenceSourceRolloutObserver,
		Severity:         model.OperationEvidenceSeverityError,
		Confidence:       model.OperationEvidenceConfidenceEvidenceBacked,
		SubjectKind:      "ManagedApp",
		SubjectName:      managed.Metadata.Name,
		SubjectNamespace: namespace,
		Summary:          summary,
		Message:          managed.Status.Message,
		Reason:           managed.Status.Phase,
		RedactionStatus:  model.OperationEvidenceRedactionNone,
		Payload:          payload,
	})
}

func (s *Service) capturePodSnapshotEvidence(ctx context.Context, app model.App, operationID, namespace string, pod kubePod) {
	payload := objectToPayloadMap(pod)
	s.recordOperationEvidenceBestEffort(model.OperationEvidence{
		TenantID:         app.TenantID,
		ProjectID:        app.ProjectID,
		AppID:            app.ID,
		OperationID:      operationID,
		Type:             model.OperationEvidenceTypeRolloutPodSnapshot,
		Source:           model.OperationEvidenceSourceKubernetesAPI,
		Severity:         model.OperationEvidenceSeverityInfo,
		Confidence:       model.OperationEvidenceConfidenceConfirmed,
		SubjectKind:      "Pod",
		SubjectName:      pod.Metadata.Name,
		SubjectNamespace: namespace,
		Summary:          "captured failing pod snapshot",
		PodName:          pod.Metadata.Name,
		NodeName:         pod.Spec.NodeName,
		RedactionStatus:  model.OperationEvidenceRedactionRedacted,
		Payload:          payload,
	})
}

func (s *Service) captureDeploymentSnapshotEvidence(ctx context.Context, app model.App, operationID, namespace string, deployment kubeDeployment) {
	if strings.TrimSpace(deployment.Metadata.Name) == "" {
		return
	}
	s.recordOperationEvidenceBestEffort(model.OperationEvidence{
		TenantID:         app.TenantID,
		ProjectID:        app.ProjectID,
		AppID:            app.ID,
		OperationID:      operationID,
		Type:             model.OperationEvidenceTypeRolloutDeploymentSnapshot,
		Source:           model.OperationEvidenceSourceKubernetesAPI,
		Severity:         model.OperationEvidenceSeverityInfo,
		Confidence:       model.OperationEvidenceConfidenceConfirmed,
		SubjectKind:      "Deployment",
		SubjectName:      deployment.Metadata.Name,
		SubjectNamespace: namespace,
		Summary:          "captured deployment rollout snapshot",
		DeploymentName:   deployment.Metadata.Name,
		RedactionStatus:  model.OperationEvidenceRedactionRedacted,
		Payload:          objectToPayloadMap(deployment),
	})
}

func (s *Service) captureReplicaSetSnapshotEvidence(ctx context.Context, client *kubeClient, app model.App, operationID, namespace string) {
	replicaSets, err := client.listReplicaSetsBySelector(ctx, namespace, managedAppPodLabelSelector(app))
	if err != nil {
		s.captureCollectorError(app, operationID, namespace, "ReplicaSet", "", fmt.Errorf("list replica sets: %w", err))
		return
	}
	for _, replicaSet := range replicaSets {
		s.recordOperationEvidenceBestEffort(model.OperationEvidence{
			TenantID:         app.TenantID,
			ProjectID:        app.ProjectID,
			AppID:            app.ID,
			OperationID:      operationID,
			Type:             model.OperationEvidenceTypeRolloutReplicaSetSnapshot,
			Source:           model.OperationEvidenceSourceKubernetesAPI,
			Severity:         model.OperationEvidenceSeverityInfo,
			Confidence:       model.OperationEvidenceConfidenceConfirmed,
			SubjectKind:      "ReplicaSet",
			SubjectName:      replicaSet.Metadata.Name,
			SubjectNamespace: namespace,
			Summary:          "captured replica set rollout snapshot",
			ReplicaSetName:   replicaSet.Metadata.Name,
			RedactionStatus:  model.OperationEvidenceRedactionRedacted,
			Payload:          objectToPayloadMap(replicaSet),
		})
	}
}

func (s *Service) capturePodLogsEvidence(ctx context.Context, client *kubeClient, app model.App, operationID, namespace string, pod kubePod, containerName string) {
	for _, spec := range []struct {
		previous bool
		lines    int
		typ      string
		summary  string
	}{
		{previous: true, lines: operationEvidencePreviousLogTailLines, typ: model.OperationEvidenceTypeRolloutPreviousLogs, summary: "captured previous container logs"},
		{previous: false, lines: operationEvidenceCurrentLogTailLines, typ: model.OperationEvidenceTypeRolloutCurrentLogs, summary: "captured current container logs"},
	} {
		logText, found, err := client.getPodLogs(ctx, namespace, pod.Metadata.Name, containerName, spec.previous, spec.lines)
		if err != nil {
			s.captureCollectorError(app, operationID, namespace, "Pod", pod.Metadata.Name, fmt.Errorf("get pod logs previous=%v: %w", spec.previous, err))
			continue
		}
		if !found || strings.TrimSpace(logText) == "" {
			continue
		}
		logText = trimToLastBytes(logText, operationEvidenceMaxLogBytes)
		redacted := observability.RedactDiagnosticText(logText)
		status := model.OperationEvidenceRedactionNone
		if redacted.Changed {
			status = model.OperationEvidenceRedactionRedacted
		}
		s.recordOperationEvidenceBestEffort(model.OperationEvidence{
			TenantID:         app.TenantID,
			ProjectID:        app.ProjectID,
			AppID:            app.ID,
			OperationID:      operationID,
			Type:             spec.typ,
			Source:           model.OperationEvidenceSourceAppLogs,
			Severity:         model.OperationEvidenceSeverityError,
			Confidence:       model.OperationEvidenceConfidenceConfirmed,
			SubjectKind:      "Pod",
			SubjectName:      pod.Metadata.Name,
			SubjectNamespace: namespace,
			Summary:          spec.summary,
			Message:          firstLogLine(redacted.Text),
			ContainerName:    containerName,
			PodName:          pod.Metadata.Name,
			NodeName:         pod.Spec.NodeName,
			RedactionStatus:  status,
			Payload: map[string]any{
				"previous":        spec.previous,
				"tail_lines":      spec.lines,
				"log_tail":        redacted.Text,
				"redaction_count": len(redacted.Findings),
			},
		})
	}
}

func (s *Service) captureKubernetesEventsEvidence(ctx context.Context, client *kubeClient, app model.App, operationID, namespace, kind, name string) {
	events, err := client.listEventsForObject(ctx, namespace, kind, name)
	if err != nil {
		s.captureCollectorError(app, operationID, namespace, kind, name, fmt.Errorf("list events: %w", err))
		return
	}
	if len(events) == 0 {
		return
	}
	start := 0
	if len(events) > 50 {
		start = len(events) - 50
	}
	for _, event := range events[start:] {
		evidenceType := classifyKubernetesEventEvidenceType(event)
		severity := model.OperationEvidenceSeverityInfo
		confidence := model.OperationEvidenceConfidenceEvidenceBacked
		if evidenceType != model.OperationEvidenceTypeRolloutKubernetesEvent {
			severity = model.OperationEvidenceSeverityError
			confidence = model.OperationEvidenceConfidenceConfirmed
		}
		redactedMessage := observability.RedactDiagnosticText(event.Message)
		status := model.OperationEvidenceRedactionNone
		if redactedMessage.Changed {
			status = model.OperationEvidenceRedactionRedacted
		}
		payload := objectToPayloadMap(event)
		augmentKubernetesEventEvidencePayload(app, event, payload)
		s.recordOperationEvidenceBestEffort(model.OperationEvidence{
			TenantID:         app.TenantID,
			ProjectID:        app.ProjectID,
			AppID:            app.ID,
			OperationID:      operationID,
			Type:             evidenceType,
			Source:           model.OperationEvidenceSourceKubernetesAPI,
			Severity:         severity,
			Confidence:       confidence,
			SubjectKind:      kind,
			SubjectName:      name,
			SubjectNamespace: namespace,
			ObservedAt:       kubeEventTime(event),
			Summary:          kubernetesEventEvidenceSummary(app, event),
			Message:          redactedMessage.Text,
			Reason:           event.Reason,
			RedactionStatus:  status,
			Payload:          payload,
		})
	}
}

func (s *Service) captureCollectorError(app model.App, operationID, namespace, kind, name string, err error) {
	if err == nil {
		return
	}
	s.recordOperationEvidenceBestEffort(model.OperationEvidence{
		TenantID:         app.TenantID,
		ProjectID:        app.ProjectID,
		AppID:            app.ID,
		OperationID:      operationID,
		Type:             model.OperationEvidenceTypeCollectorError,
		Source:           model.OperationEvidenceSourceRolloutObserver,
		Severity:         model.OperationEvidenceSeverityWarning,
		Confidence:       model.OperationEvidenceConfidenceInsufficientEvidence,
		SubjectKind:      kind,
		SubjectName:      name,
		SubjectNamespace: namespace,
		Summary:          "failed to collect rollout evidence",
		Message:          err.Error(),
		RedactionStatus:  model.OperationEvidenceRedactionRedacted,
	})
}

func (s *Service) recordOperationEvidenceBestEffort(evidence model.OperationEvidence) string {
	if s == nil || s.Store == nil || strings.TrimSpace(evidence.OperationID) == "" {
		return ""
	}
	if strings.TrimSpace(evidence.ReleaseAttemptID) == "" {
		if attempt, found, err := s.Store.FindReleaseAttemptForOperation(evidence.OperationID); err == nil && found {
			evidence.ReleaseAttemptID = attempt.ID
		}
	}
	recorded, err := s.Store.RecordOperationEvidence(evidence)
	if err != nil {
		if s.Logger != nil {
			s.Logger.Printf("record operation evidence failed operation=%s type=%s: %v", evidence.OperationID, evidence.Type, err)
		}
		return ""
	}
	return recorded.ID
}

func primaryFailingContainerStatus(pod kubePod) (kubeContainerStatus, kubeStateDetail, string) {
	for _, status := range append(append([]kubeContainerStatus{}, pod.Status.InitContainerStatuses...), pod.Status.ContainerStatuses...) {
		if status.State.Terminated != nil && isFailingManagedAppTermination(*status.State.Terminated) {
			return status, *status.State.Terminated, "terminated"
		}
		if !managedAppContainerRecovered(status) && status.LastState.Terminated != nil && isFailingManagedAppTermination(*status.LastState.Terminated) {
			return status, *status.LastState.Terminated, "last_terminated"
		}
		if status.State.Waiting != nil && isFailingManagedAppWaitingReason(status.State.Waiting.Reason) {
			return status, *status.State.Waiting, "waiting"
		}
		if !managedAppContainerRecovered(status) && status.LastState.Waiting != nil && isFailingManagedAppWaitingReason(status.LastState.Waiting.Reason) {
			return status, *status.LastState.Waiting, "last_waiting"
		}
	}
	return kubeContainerStatus{}, kubeStateDetail{Reason: pod.Status.Reason, Message: pod.Status.Message}, "pod"
}

func classifyPodFailureEvidenceType(pod kubePod, status kubeContainerStatus, detail kubeStateDetail) string {
	reason := strings.ToLower(strings.TrimSpace(firstNonEmptyControllerString(detail.Reason, pod.Status.Reason)))
	switch reason {
	case "imagepullbackoff", "errimagepull", "invalidimagename", "errimageneverpull":
		return model.OperationEvidenceTypeImagePullFailure
	}
	if strings.Contains(strings.ToLower(detail.Message), "readiness probe failed") {
		return model.OperationEvidenceTypeReadinessProbeFailure
	}
	if strings.TrimSpace(status.Name) != "" || detail.ExitCode != 0 {
		return model.OperationEvidenceTypeRolloutContainerTerminated
	}
	return model.OperationEvidenceTypeRolloutPodFailure
}

func classifyKubernetesEventEvidenceType(event kubeEvent) string {
	reason := strings.ToLower(strings.TrimSpace(event.Reason))
	message := strings.ToLower(strings.TrimSpace(event.Message))
	switch {
	case reason == "failedscheduling" || strings.Contains(message, "failedscheduling"):
		return model.OperationEvidenceTypeSchedulerFailure
	case reason == "failedmount" || strings.Contains(message, "failedmount") || strings.Contains(message, "mount"):
		return model.OperationEvidenceTypeVolumeMountFailure
	case reason == "errimagepull" || reason == "imagepullbackoff" || strings.Contains(message, "imagepullbackoff") || strings.Contains(message, "errimagepull"):
		return model.OperationEvidenceTypeImagePullFailure
	case strings.Contains(message, "readiness probe failed"):
		return model.OperationEvidenceTypeReadinessProbeFailure
	case strings.Contains(message, "liveness probe failed"):
		return model.OperationEvidenceTypeLivenessProbeFailure
	case strings.Contains(message, "startup probe failed"):
		return model.OperationEvidenceTypeStartupProbeFailure
	default:
		return model.OperationEvidenceTypeRolloutKubernetesEvent
	}
}

func kubernetesEventEvidenceSummary(app model.App, event kubeEvent) string {
	if kubernetesEventIndicatesSameNodeOnlineMountUnsupported(event) {
		return model.AppStorageClassSameNodeOnlineMountUnsupportedSummary(appStorageClassNameForController(app))
	}
	return firstNonEmptyControllerString(event.Reason, "kubernetes event")
}

func augmentKubernetesEventEvidencePayload(app model.App, event kubeEvent, payload map[string]any) {
	if payload == nil || !kubernetesEventIndicatesSameNodeOnlineMountUnsupported(event) {
		return
	}
	payload["storage_class_name"] = appStorageClassNameForController(app)
	payload["storage_rollout_failure"] = "same_node_online_dual_mount_unsupported"
	payload["same_node_online_mount_supported"] = false
}

func kubernetesEventIndicatesSameNodeOnlineMountUnsupported(event kubeEvent) bool {
	return strings.EqualFold(strings.TrimSpace(event.Reason), "FailedMount") &&
		model.StorageEventIndicatesSameNodeOnlineMountUnsupported(event.Message)
}

func appStorageClassNameForController(app model.App) string {
	if storage := app.Spec.PersistentStorage; storage != nil {
		if value := strings.TrimSpace(storage.StorageClassName); value != "" {
			return value
		}
	}
	if workspace := app.Spec.Workspace; workspace != nil {
		return strings.TrimSpace(workspace.StorageClassName)
	}
	return ""
}

func objectToPayloadMap(value any) map[string]any {
	data, err := json.Marshal(value)
	if err != nil {
		return map[string]any{"marshal_error": err.Error()}
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		return map[string]any{"unmarshal_error": err.Error()}
	}
	return out
}

func trimToLastBytes(value string, maxBytes int) string {
	if maxBytes <= 0 || len(value) <= maxBytes {
		return value
	}
	return value[len(value)-maxBytes:]
}

func firstLogLine(value string) string {
	for _, line := range strings.Split(strings.TrimSpace(value), "\n") {
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			if len(trimmed) > 500 {
				return trimmed[:500]
			}
			return trimmed
		}
	}
	return ""
}
