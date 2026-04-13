package sourceimport

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

type builderJob struct {
	Metadata struct {
		Name string `json:"name"`
	} `json:"metadata"`
	Status builderJobStatus `json:"status"`
}

type builderJobStatus struct {
	Active     int                   `json:"active,omitempty"`
	Succeeded  int                   `json:"succeeded,omitempty"`
	Failed     int                   `json:"failed,omitempty"`
	Conditions []builderJobCondition `json:"conditions,omitempty"`
}

type builderJobCondition struct {
	Type    string `json:"type"`
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

type builderPodList struct {
	Items []builderPod `json:"items"`
}

type builderPod struct {
	Metadata struct {
		Name              string    `json:"name"`
		CreationTimestamp time.Time `json:"creationTimestamp"`
	} `json:"metadata"`
	Spec struct {
		NodeName string `json:"nodeName,omitempty"`
	} `json:"spec"`
	Status builderPodStatus `json:"status"`
}

type builderPodStatus struct {
	Phase                 string                   `json:"phase,omitempty"`
	Reason                string                   `json:"reason,omitempty"`
	Message               string                   `json:"message,omitempty"`
	InitContainerStatuses []builderContainerStatus `json:"initContainerStatuses,omitempty"`
	ContainerStatuses     []builderContainerStatus `json:"containerStatuses,omitempty"`
}

type builderContainerStatus struct {
	Name      string              `json:"name"`
	State     builderRuntimeState `json:"state,omitempty"`
	LastState builderRuntimeState `json:"lastState,omitempty"`
}

type builderRuntimeState struct {
	Waiting    *builderStateDetail `json:"waiting,omitempty"`
	Terminated *builderStateDetail `json:"terminated,omitempty"`
}

type builderStateDetail struct {
	Reason   string `json:"reason,omitempty"`
	Message  string `json:"message,omitempty"`
	ExitCode int    `json:"exitCode,omitempty"`
}

func waitForBuilderJob(ctx context.Context, namespace, jobName string, timeout time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		job, err := getBuilderJob(waitCtx, namespace, jobName)
		if err != nil {
			return err
		}
		if builderJobCompleted(job.Status) {
			return nil
		}
		if builderJobFailed(job.Status) {
			summary, summaryErr := summarizeBuilderJobFailure(waitCtx, namespace, jobName)
			if summaryErr == nil && strings.TrimSpace(summary) != "" {
				return fmt.Errorf("%s", summary)
			}
			return fmt.Errorf("builder job %s failed", jobName)
		}

		select {
		case <-waitCtx.Done():
			return fmt.Errorf("wait for builder job %s: %w", jobName, waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func getBuilderJob(ctx context.Context, namespace, jobName string) (builderJob, error) {
	var job builderJob
	if err := kubectlJSON(ctx, &job, "-n", namespace, "get", "job", jobName, "-o", "json"); err != nil {
		return builderJob{}, err
	}
	return job, nil
}

func listBuilderPods(ctx context.Context, namespace, jobName string) ([]builderPod, error) {
	var podList builderPodList
	if err := kubectlJSON(ctx, &podList, "-n", namespace, "get", "pods", "-l", "job-name="+jobName, "-o", "json"); err != nil {
		return nil, err
	}
	sort.Slice(podList.Items, func(i, j int) bool {
		if !podList.Items[i].Metadata.CreationTimestamp.Equal(podList.Items[j].Metadata.CreationTimestamp) {
			return podList.Items[i].Metadata.CreationTimestamp.Before(podList.Items[j].Metadata.CreationTimestamp)
		}
		return podList.Items[i].Metadata.Name < podList.Items[j].Metadata.Name
	})
	return podList.Items, nil
}

func summarizeBuilderJobFailure(ctx context.Context, namespace, jobName string) (string, error) {
	job, err := getBuilderJob(ctx, namespace, jobName)
	if err != nil {
		return "", err
	}

	pods, err := listBuilderPods(ctx, namespace, jobName)
	if err != nil {
		return "", err
	}

	lines := make([]string, 0, len(pods)+1)
	for _, pod := range pods {
		if summary := summarizeBuilderPodFailure(pod); summary != "" {
			lines = append(lines, summary)
		}
	}
	if len(lines) > 0 {
		return strings.Join(lines, "\n"), nil
	}
	for _, condition := range job.Status.Conditions {
		if strings.EqualFold(strings.TrimSpace(condition.Type), "Failed") && strings.EqualFold(strings.TrimSpace(condition.Status), "True") {
			return formatFailureLine("job "+jobName, strings.TrimSpace(condition.Reason), strings.TrimSpace(condition.Message)), nil
		}
	}
	return fmt.Sprintf("builder job %s failed", jobName), nil
}

func summarizeBuilderPodFailure(pod builderPod) string {
	prefix := "pod " + strings.TrimSpace(pod.Metadata.Name)
	if node := strings.TrimSpace(pod.Spec.NodeName); node != "" {
		prefix += " on node " + node
	}

	if reason := strings.TrimSpace(pod.Status.Reason); reason != "" {
		return formatFailureLine(prefix, reason, strings.TrimSpace(pod.Status.Message))
	}

	statuses := append([]builderContainerStatus(nil), pod.Status.InitContainerStatuses...)
	statuses = append(statuses, pod.Status.ContainerStatuses...)
	for _, status := range statuses {
		if summary := summarizeBuilderContainerFailure(prefix, status); summary != "" {
			return summary
		}
	}

	phase := strings.TrimSpace(pod.Status.Phase)
	if phase != "" && !strings.EqualFold(phase, "Running") && !strings.EqualFold(phase, "Succeeded") {
		return fmt.Sprintf("%s failed with phase %s", prefix, phase)
	}
	return ""
}

func summarizeBuilderContainerFailure(prefix string, status builderContainerStatus) string {
	name := strings.TrimSpace(status.Name)
	if status.State.Terminated != nil {
		if !isFailingBuilderContainerTermination(*status.State.Terminated) {
			return ""
		}
		return formatContainerFailureLine(prefix, name, "terminated", *status.State.Terminated)
	}
	if status.LastState.Terminated != nil {
		if !isFailingBuilderContainerTermination(*status.LastState.Terminated) {
			return ""
		}
		return formatContainerFailureLine(prefix, name, "terminated", *status.LastState.Terminated)
	}
	if status.State.Waiting != nil {
		return formatContainerFailureLine(prefix, name, "waiting", *status.State.Waiting)
	}
	if status.LastState.Waiting != nil {
		return formatContainerFailureLine(prefix, name, "waiting", *status.LastState.Waiting)
	}
	return ""
}

func isFailingBuilderContainerTermination(detail builderStateDetail) bool {
	reason := strings.TrimSpace(detail.Reason)
	if detail.ExitCode != 0 {
		return true
	}
	return reason != "" && !strings.EqualFold(reason, "Completed")
}

func formatContainerFailureLine(prefix, containerName, state string, detail builderStateDetail) string {
	subject := prefix
	if containerName != "" {
		subject += " container " + containerName
	}
	reason := strings.TrimSpace(detail.Reason)
	message := strings.TrimSpace(detail.Message)
	if detail.ExitCode != 0 {
		if message == "" {
			message = fmt.Sprintf("exit_code=%d", detail.ExitCode)
		} else {
			message = fmt.Sprintf("%s (exit_code=%d)", message, detail.ExitCode)
		}
	}
	if reason == "" {
		reason = state
	}
	return formatFailureLine(subject, reason, message)
}

func formatFailureLine(subject, reason, message string) string {
	subject = strings.TrimSpace(subject)
	reason = strings.TrimSpace(reason)
	message = strings.TrimSpace(message)
	switch {
	case reason != "" && message != "":
		return fmt.Sprintf("%s failed: %s: %s", subject, reason, message)
	case reason != "":
		return fmt.Sprintf("%s failed: %s", subject, reason)
	case message != "":
		return fmt.Sprintf("%s failed: %s", subject, message)
	default:
		return fmt.Sprintf("%s failed", subject)
	}
}

func builderJobCompleted(status builderJobStatus) bool {
	for _, condition := range status.Conditions {
		if strings.EqualFold(strings.TrimSpace(condition.Type), "Complete") && strings.EqualFold(strings.TrimSpace(condition.Status), "True") {
			return true
		}
	}
	return status.Succeeded > 0
}

func builderJobFailed(status builderJobStatus) bool {
	for _, condition := range status.Conditions {
		if strings.EqualFold(strings.TrimSpace(condition.Type), "Failed") && strings.EqualFold(strings.TrimSpace(condition.Status), "True") {
			return true
		}
	}
	return status.Failed > 0 && status.Active == 0 && status.Succeeded == 0
}

func kubectlJSON(ctx context.Context, dst any, args ...string) error {
	output, err := kubectlOutput(ctx, nil, args...)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(output, dst); err != nil {
		return fmt.Errorf("decode kubectl json: %w", err)
	}
	return nil
}
