package sourceimport

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/observability"
)

type builderEvidenceRecorderKey struct{}
type builderEvidenceRecorder func(model.BuilderAttemptEvidence, map[string]any)

// WithBuilderEvidenceRecorder attaches an observational sink. It must not mutate
// serving state or cause an otherwise successful import to fail.
func WithBuilderEvidenceRecorder(ctx context.Context, record func(model.BuilderAttemptEvidence, map[string]any)) context.Context {
	return context.WithValue(ctx, builderEvidenceRecorderKey{}, builderEvidenceRecorder(record))
}

func recordBuilderAttempt(ctx context.Context, jobName string, attempt builderJobAttempt, started time.Time, runErr error) {
	record, _ := ctx.Value(builderEvidenceRecorderKey{}).(builderEvidenceRecorder)
	if record == nil {
		return
	}
	diagnostics := map[string]any{"job_name": jobName}
	fact := model.BuilderAttemptEvidence{Attempt: attempt.Number, Outcome: "succeeded", StartedAt: started, FinishedAt: time.Now().UTC(), Causes: []string{}, MissingEvidence: []string{}}
	if runErr != nil {
		fact.Outcome = "failed"
	}
	// The operation deadline has often expired. A separate bounded read context
	// captures terminal evidence before retry deletion/zombie cleanup can remove it.
	captureCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 8*time.Second)
	defer cancel()
	namespace, err := currentNamespace()
	if err != nil {
		fact.MissingEvidence = append(fact.MissingEvidence, "build_snapshot")
		record(fact, diagnostics)
		return
	}
	diagnostics["namespace"] = namespace
	var snapshots []any
	pods, err := listBuilderPods(captureCtx, namespace, jobName)
	if err != nil || len(pods) == 0 {
		fact.MissingEvidence = append(fact.MissingEvidence, "build_snapshot")
	}
	for i, pod := range pods {
		if i >= 4 {
			fact.MissingEvidence = append(fact.MissingEvidence, "additional_build_pods")
			break
		}
		if pod.Metadata.CreationTimestamp.IsZero() || pod.Metadata.CreationTimestamp.Before(started.Truncate(time.Second)) {
			fact.MissingEvidence = append(fact.MissingEvidence, "build_snapshot")
			continue
		}
		observeBuilderPod(&fact, pod)
		// The typed pod decoder contains resource/status fields only, never env.
		data, _ := json.Marshal(pod)
		var snapshot map[string]any
		_ = json.Unmarshal(data, &snapshot)
		snapshots = append(snapshots, snapshot)
		// Query by UID: a reused Job name must never attach an earlier attempt's events.
		if pod.Metadata.UID == "" {
			fact.MissingEvidence = append(fact.MissingEvidence, "scheduling_events")
			continue
		}
		var events struct {
			Items []struct {
				Reason  string `json:"reason"`
				Message string `json:"message"`
			} `json:"items"`
		}
		err := kubectlJSON(captureCtx, &events, "-n", namespace, "get", "events", "--field-selector=involvedObject.uid="+pod.Metadata.UID, "-o", "json")
		if err != nil {
			fact.MissingEvidence = append(fact.MissingEvidence, "scheduling_events")
			continue
		}
		snapshot["events"] = events.Items[:min(len(events.Items), 32)]
		for j, event := range events.Items {
			if j >= 32 {
				fact.MissingEvidence = append(fact.MissingEvidence, "additional_scheduling_events")
				break
			}
			addBuilderCauses(&fact, model.DeploymentCauseCodes(event.Reason+" "+event.Message)...)
		}
		container := failingBuilderContainerName(pod)
		if container == "" && len(pod.Spec.Containers) > 0 {
			container = pod.Spec.Containers[0].Name
		}
		if runErr != nil && container != "" && pod.Spec.NodeName != "" {
			logs, logErr := kubectlOutput(captureCtx, nil, "-n", namespace, "logs", pod.Metadata.Name, "-c", container, "--tail=30", "--limit-bytes=4096")
			if logErr == nil {
				snapshot["log_tail"] = observability.RedactDiagnosticText(string(logs)).Text
			} else {
				fact.MissingEvidence = append(fact.MissingEvidence, "build_log_tail")
			}
		}
	}
	diagnostics["pods"] = snapshots
	if runErr != nil {
		addBuilderCauses(&fact, model.DeploymentCauseCodes(runErr.Error())...)
		if len(fact.Causes) == 0 {
			addBuilderCauses(&fact, "evidence_unavailable")
		}
	}
	// Disk usage composition and build output are not inferred from a resource cap.
	if hasBuilderCause(fact, "ephemeral_storage_limit_exceeded") {
		fact.MissingEvidence = append(fact.MissingEvidence, "temporary_storage_breakdown")
	}
	record(fact, diagnostics)
}

func observeBuilderPod(fact *model.BuilderAttemptEvidence, pod builderPod) {
	addBuilderCauses(fact, model.DeploymentCauseCodes(pod.Status.Reason+" "+pod.Status.Message)...)
	for _, condition := range pod.Status.Conditions {
		if condition.Status == "False" {
			addBuilderCauses(fact, model.DeploymentCauseCodes(condition.Reason+" "+condition.Message)...)
		}
	}
	for _, c := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
		fact.MemoryRequestBytes = max(fact.MemoryRequestBytes, parseBuilderBytes(c.Resources.Requests["memory"]))
		fact.MemoryLimitBytes = max(fact.MemoryLimitBytes, parseBuilderBytes(c.Resources.Limits["memory"]))
		fact.EphemeralRequestBytes = max(fact.EphemeralRequestBytes, parseBuilderBytes(c.Resources.Requests["ephemeral-storage"]))
		fact.EphemeralLimitBytes = max(fact.EphemeralLimitBytes, parseBuilderBytes(c.Resources.Limits["ephemeral-storage"]))
	}
	for _, c := range append(pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses...) {
		for _, state := range []builderRuntimeState{c.State, c.LastState} {
			if state.Waiting != nil {
				addBuilderCauses(fact, model.DeploymentCauseCodes(state.Waiting.Reason+" "+state.Waiting.Message)...)
			}
			if state.Terminated != nil && state.Terminated.ExitCode != 0 {
				codes := model.DeploymentCauseCodes(state.Terminated.Reason + " " + state.Terminated.Message)
				if len(codes) == 0 && !strings.EqualFold(pod.Status.Reason, "Evicted") {
					codes = []string{"build_command_failed"}
				}
				addBuilderCauses(fact, codes...)
			}
		}
	}
}
func addBuilderCauses(fact *model.BuilderAttemptEvidence, codes ...string) {
	for _, code := range codes {
		if !hasBuilderCause(*fact, code) {
			fact.Causes = append(fact.Causes, code)
		}
	}
}
func hasBuilderCause(fact model.BuilderAttemptEvidence, code string) bool {
	for _, s := range fact.Causes {
		if s == code {
			return true
		}
	}
	return false
}

// BuilderAttemptPayload keeps the evidence contract compatible with the generic
// durable payload store; all fields are explicitly allowlisted above.
func BuilderAttemptPayload(fact model.BuilderAttemptEvidence) map[string]any {
	data, _ := json.Marshal(fact)
	var payload map[string]any
	_ = json.Unmarshal(data, &payload)
	return map[string]any{"build_attempt": payload}
}
