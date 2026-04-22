package cli

import (
	"fmt"
	"io"
	"path"
	"strings"
	"text/tabwriter"
	"time"

	"fugue/internal/model"
	fugueruntime "fugue/internal/runtime"

	"github.com/spf13/cobra"
)

const filesystemDiagnosisSchemaVersion = "fugue.diagnose-fs.v1"

type filesystemDiagnosisOptions struct {
	Path      string
	Source    string
	Component string
	Pod       string
	TailLines int
}

type filesystemContainerSnapshot struct {
	Name         string `json:"name"`
	Ready        bool   `json:"ready"`
	RestartCount int32  `json:"restart_count,omitempty"`
	State        string `json:"state,omitempty"`
	Reason       string `json:"reason,omitempty"`
	Message      string `json:"message,omitempty"`
}

type filesystemPodSnapshot struct {
	Name       string                        `json:"name"`
	Phase      string                        `json:"phase"`
	Ready      bool                          `json:"ready"`
	Node       string                        `json:"node,omitempty"`
	StartTime  *time.Time                    `json:"start_time,omitempty"`
	Containers []filesystemContainerSnapshot `json:"containers,omitempty"`
}

type filesystemDiagnosisResult struct {
	SchemaVersion     string                  `json:"schema_version"`
	App               string                  `json:"app"`
	AppID             string                  `json:"app_id"`
	Phase             string                  `json:"phase"`
	Component         string                  `json:"component"`
	Source            string                  `json:"source"`
	AccessMode        string                  `json:"access_mode"`
	Path              string                  `json:"path"`
	MountRoot         string                  `json:"mount_root"`
	Namespace         string                  `json:"namespace,omitempty"`
	ProbeStatus       string                  `json:"probe_status"`
	ProbeKind         string                  `json:"probe_kind,omitempty"`
	SelectedPod       string                  `json:"selected_pod,omitempty"`
	SelectedPodOK     bool                    `json:"selected_pod_ready,omitempty"`
	SelectedContainer string                  `json:"selected_container,omitempty"`
	FailureClass      string                  `json:"failure_class,omitempty"`
	Summary           string                  `json:"summary"`
	RawError          string                  `json:"raw_error,omitempty"`
	RawExecError      string                  `json:"raw_exec_error,omitempty"`
	RuntimeDiagnosis  *appDiagnosis           `json:"runtime_diagnosis,omitempty"`
	Pods              []filesystemPodSnapshot `json:"pods,omitempty"`
	Events            []model.ClusterEvent    `json:"events,omitempty"`
	LogEvidence       []string                `json:"log_evidence,omitempty"`
	Warnings          []string                `json:"warnings,omitempty"`
	Redacted          bool                    `json:"redacted"`
}

func (c *CLI) newDiagnoseFilesystemCommand() *cobra.Command {
	opts := filesystemDiagnosisOptions{
		Source:    "auto",
		Component: "app",
		TailLines: 200,
	}
	cmd := &cobra.Command{
		Use:   "fs <app>",
		Short: "Diagnose filesystem and exec-path failures for one app",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(opts.Path) == "" {
				return withExitCode(fmt.Errorf("--path is required"), ExitCodeUserInput)
			}
			client, err := c.newClient()
			if err != nil {
				return err
			}
			app, err := c.resolveWorkspaceApp(client, args[0])
			if err != nil {
				return err
			}
			result, runErr := c.runFilesystemDiagnosis(client, app, opts)
			sanitized := sanitizeFilesystemDiagnosisResult(result, c.shouldRedact())
			if c.wantsJSON() {
				if err := writeJSON(c.stdout, sanitized); err != nil {
					return err
				}
			} else {
				if err := renderFilesystemDiagnosisResult(c.stdout, sanitized); err != nil {
					return err
				}
			}
			if runErr != nil {
				return runErr
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&opts.Path, "path", "", "Target path to diagnose")
	cmd.Flags().StringVar(&opts.Source, "source", opts.Source, "Filesystem source: auto, persistent, or live")
	cmd.Flags().StringVar(&opts.Component, "component", opts.Component, "Runtime component. Currently only 'app' is supported")
	cmd.Flags().StringVar(&opts.Pod, "pod", "", "Specific pod name")
	cmd.Flags().IntVar(&opts.TailLines, "tail", opts.TailLines, "Maximum pod log lines to collect as evidence")
	return cmd
}

func (c *CLI) runFilesystemDiagnosis(client *Client, app model.App, opts filesystemDiagnosisOptions) (filesystemDiagnosisResult, error) {
	component, err := normalizeFilesystemComponent(opts.Component)
	if err != nil {
		return filesystemDiagnosisResult{}, withExitCode(err, ExitCodeUserInput)
	}
	requestPath, err := resolveFilesystemPathForCLI(app, opts.Path, false, opts.Source)
	if err != nil {
		return filesystemDiagnosisResult{}, withExitCode(err, ExitCodeUserInput)
	}

	accessMode, mountRoot := determineFilesystemDiagnosisScope(app, requestPath, opts.Source)
	namespace := fugueruntime.NamespaceForTenant(app.TenantID)
	result := filesystemDiagnosisResult{
		SchemaVersion: filesystemDiagnosisSchemaVersion,
		App:           strings.TrimSpace(app.Name),
		AppID:         strings.TrimSpace(app.ID),
		Phase:         strings.TrimSpace(app.Status.Phase),
		Component:     component,
		Source:        strings.TrimSpace(opts.Source),
		AccessMode:    accessMode,
		Path:          requestPath,
		MountRoot:     mountRoot,
		Namespace:     namespace,
		ProbeStatus:   workflowResultStatusOK,
		Summary:       "filesystem path is accessible",
		Redacted:      c.shouldRedact(),
	}

	podInventory, podInventoryErr := client.GetAppRuntimePods(app.ID, component)
	if podInventoryErr != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("runtime pod inventory unavailable: %v", podInventoryErr))
	} else {
		selectedPod, selectedContainer, selectedPodOK := chooseFilesystemDiagnosisTarget(podInventory, accessMode, opts.Pod)
		result.SelectedPod = selectedPod
		result.SelectedContainer = selectedContainer
		result.SelectedPodOK = selectedPodOK
		result.Pods = snapshotFilesystemPods(podInventory)
	}

	if diagnosis, err := client.TryGetAppDiagnosis(app.ID, component); err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("runtime diagnosis unavailable: %v", err))
	} else if diagnosis != nil {
		result.RuntimeDiagnosis = diagnosis
	}

	if probeKind, probeErr := c.probeFilesystemPath(client, app.ID, component, requestPath, opts.Pod); probeErr == nil {
		result.ProbeKind = probeKind
		return result, nil
	} else {
		result.ProbeStatus = workflowResultStatusFail
		result.RawError = probeErr.Error()
	}

	if strings.TrimSpace(result.SelectedPod) != "" && strings.TrimSpace(result.SelectedContainer) != "" {
		if rawExecErr := c.tryRawFilesystemExecProbe(client, namespace, result.SelectedPod, result.SelectedContainer, requestPath); rawExecErr != nil {
			result.RawExecError = rawExecErr.Error()
		}
		if logs, err := c.collectFilesystemLogEvidence(client, namespace, result.SelectedPod, result.SelectedContainer, opts.TailLines); err != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("pod log evidence unavailable: %v", err))
		} else {
			result.LogEvidence = logs
		}
		if events, err := client.ListClusterEvents(clusterEventsOptions{
			Namespace: namespace,
			Kind:      "Pod",
			Name:      result.SelectedPod,
			Limit:     20,
		}); err != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("pod events unavailable: %v", err))
			if result.RuntimeDiagnosis != nil && len(result.RuntimeDiagnosis.Events) > 0 {
				result.Events = append([]model.ClusterEvent(nil), result.RuntimeDiagnosis.Events...)
			}
		} else {
			result.Events = events
		}
	} else if result.RuntimeDiagnosis != nil && len(result.RuntimeDiagnosis.Events) > 0 {
		result.Events = append([]model.ClusterEvent(nil), result.RuntimeDiagnosis.Events...)
	}

	result.FailureClass = classifyFilesystemDiagnosisFailure(result)
	result.Summary = summarizeFilesystemDiagnosis(result)
	return result, filesystemDiagnosisExitError(result)
}

func determineFilesystemDiagnosisScope(app model.App, requestPath, rawSource string) (string, string) {
	source, err := normalizeFilesystemSource(rawSource)
	if err != nil {
		return "live", "/"
	}
	workspace, workspaceErr := workspaceRoot(app)
	switch source {
	case "persistent":
		if workspaceErr == nil {
			return "persistent", workspace
		}
		return "persistent", ""
	case "live":
		return "live", "/"
	default:
		if workspaceErr == nil && isPathWithinFilesystemRootForCLI(workspace, requestPath) {
			return "persistent", workspace
		}
		return "live", "/"
	}
}

func chooseFilesystemDiagnosisTarget(inventory model.AppRuntimePodInventory, accessMode, requestedPod string) (string, string, bool) {
	containerName := strings.TrimSpace(inventory.Container)
	if accessMode == "persistent" {
		containerName = fugueruntime.AppWorkspaceContainerName
	}
	pods := flattenRuntimeInventoryPods(inventory)
	if strings.TrimSpace(requestedPod) != "" {
		for _, pod := range pods {
			if strings.EqualFold(strings.TrimSpace(pod.Name), strings.TrimSpace(requestedPod)) {
				return pod.Name, containerName, pod.Ready
			}
		}
		return strings.TrimSpace(requestedPod), containerName, false
	}
	sortClusterPodsForEvidence(pods)
	for _, pod := range pods {
		if !strings.EqualFold(strings.TrimSpace(pod.Phase), "Running") {
			continue
		}
		if podContainerReady(pod, containerName) {
			return pod.Name, containerName, true
		}
	}
	if len(pods) > 0 {
		return pods[0].Name, containerName, pods[0].Ready
	}
	return "", containerName, false
}

func flattenRuntimeInventoryPods(inventory model.AppRuntimePodInventory) []model.ClusterPod {
	pods := make([]model.ClusterPod, 0)
	for _, group := range inventory.Groups {
		pods = append(pods, group.Pods...)
	}
	return pods
}

func podContainerReady(pod model.ClusterPod, containerName string) bool {
	for _, container := range pod.Containers {
		if strings.EqualFold(strings.TrimSpace(container.Name), strings.TrimSpace(containerName)) {
			return container.Ready
		}
	}
	return false
}

func snapshotFilesystemPods(inventory model.AppRuntimePodInventory) []filesystemPodSnapshot {
	pods := flattenRuntimeInventoryPods(inventory)
	sortClusterPodsForEvidence(pods)
	snapshots := make([]filesystemPodSnapshot, 0, len(pods))
	for _, pod := range pods {
		snapshot := filesystemPodSnapshot{
			Name:      strings.TrimSpace(pod.Name),
			Phase:     strings.TrimSpace(pod.Phase),
			Ready:     pod.Ready,
			Node:      strings.TrimSpace(pod.NodeName),
			StartTime: pod.StartTime,
		}
		for _, container := range pod.Containers {
			snapshot.Containers = append(snapshot.Containers, filesystemContainerSnapshot{
				Name:         strings.TrimSpace(container.Name),
				Ready:        container.Ready,
				RestartCount: container.RestartCount,
				State:        strings.TrimSpace(container.State),
				Reason:       strings.TrimSpace(container.Reason),
				Message:      strings.TrimSpace(container.Message),
			})
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots
}

func (c *CLI) probeFilesystemPath(client *Client, appID, component, requestPath, pod string) (string, error) {
	if _, err := client.GetAppFilesystemTree(appID, component, requestPath, pod); err == nil {
		return "directory", nil
	} else if strings.Contains(strings.ToLower(err.Error()), "path must reference a directory") {
		if _, fileErr := client.GetAppFilesystemFile(appID, component, requestPath, pod, workflowBodyPreviewLimit); fileErr == nil {
			return "file", nil
		} else {
			return "", fileErr
		}
	} else {
		return "", err
	}
}

func (c *CLI) tryRawFilesystemExecProbe(client *Client, namespace, pod, container, requestPath string) error {
	command := []string{"sh", "-lc", "ls -ld -- " + shellSingleQuote(path.Clean(requestPath))}
	_, err := client.ExecClusterPod(clusterExecRequest{
		Namespace: namespace,
		Pod:       pod,
		Container: container,
		Command:   command,
		Retries:   1,
		Timeout:   15 * time.Second,
	})
	return err
}

func shellSingleQuote(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "''"
	}
	return "'" + strings.ReplaceAll(value, "'", `'"'"'`) + "'"
}

func (c *CLI) collectFilesystemLogEvidence(client *Client, namespace, pod, container string, tailLines int) ([]string, error) {
	logs, err := client.GetClusterLogs(clusterLogsOptions{
		Namespace: namespace,
		Pod:       pod,
		Container: container,
		TailLines: tailLines,
	})
	if err != nil {
		return nil, err
	}
	lines := extractRelevantLogLines(logs.Logs, []string{
		"error",
		"denied",
		"eof",
		"upgrade",
		"timeout",
		"not found",
		"permission",
	}, 12)
	if len(lines) > 0 {
		return lines, nil
	}
	return tailNonEmptyLines(logs.Logs, 10), nil
}

func tailNonEmptyLines(raw string, limit int) []string {
	if strings.TrimSpace(raw) == "" || limit <= 0 {
		return nil
	}
	all := strings.Split(strings.TrimSpace(raw), "\n")
	out := make([]string, 0, limit)
	for index := len(all) - 1; index >= 0 && len(out) < limit; index-- {
		line := strings.TrimSpace(all[index])
		if line == "" {
			continue
		}
		out = append([]string{line}, out...)
	}
	return out
}

func classifyFilesystemDiagnosisFailure(result filesystemDiagnosisResult) string {
	message := strings.ToLower(strings.TrimSpace(firstNonEmptyTrimmed(result.RawExecError, result.RawError)))
	if strings.Contains(message, "permission denied") {
		return "permission-denied"
	}
	if strings.Contains(message, "path not found") {
		return "path-not-found"
	}
	if strings.Contains(message, "parent directory does not exist") {
		return "path-parent-missing"
	}
	if strings.Contains(message, "no matching pods found") {
		return "pod-not-found"
	}
	if strings.Contains(message, "no running app pods found") || strings.Contains(message, "no running pod") {
		return "pod-not-found"
	}
	if strings.Contains(message, "does not include container") || strings.Contains(message, "container not found") {
		return "container-not-found"
	}
	if strings.Contains(message, "is not ready for filesystem access") || strings.Contains(message, "filesystem target is not ready") {
		return "container-not-ready"
	}
	if selected := lookupFilesystemPod(result.Pods, result.SelectedPod); selected != nil {
		if !filesystemPodHasContainer(*selected, result.SelectedContainer) {
			return "container-not-found"
		}
		if !filesystemPodContainerReady(*selected, result.SelectedContainer) {
			return "container-not-ready"
		}
	}
	if strings.Contains(message, "waiting to start") || strings.Contains(message, "containercreating") {
		return "container-not-ready"
	}
	if strings.Contains(message, "unable to upgrade connection") {
		return "exec-stream-upgrade-failed"
	}
	if strings.Contains(message, "apiserver not ready") || strings.Contains(message, "service unavailable") || strings.Contains(message, "temporarily unavailable") {
		return "api-server-unavailable"
	}
	if strings.Contains(message, "unexpected eof") || strings.Contains(message, "eof") || strings.Contains(message, "connection reset") {
		return "eof-connection-interrupted"
	}
	if strings.Contains(message, "i/o timeout") || strings.Contains(message, "connection refused") || strings.Contains(message, "tls handshake timeout") {
		return "api-server-unavailable"
	}
	return "indeterminate"
}

func summarizeFilesystemDiagnosis(result filesystemDiagnosisResult) string {
	if result.ProbeStatus == workflowResultStatusOK {
		return fmt.Sprintf("filesystem %s path %s is accessible through %s", firstNonEmptyTrimmed(result.ProbeKind, "target"), result.Path, firstNonEmptyTrimmed(result.SelectedPod, "the current app pod"))
	}
	switch result.FailureClass {
	case "pod-not-found":
		return fmt.Sprintf("filesystem access failed because no usable pod was available for path %s", result.Path)
	case "container-not-found":
		return fmt.Sprintf("filesystem access failed because pod %s does not include container %s", result.SelectedPod, result.SelectedContainer)
	case "container-not-ready":
		return fmt.Sprintf("filesystem access failed because pod %s container %s is not ready", result.SelectedPod, result.SelectedContainer)
	case "exec-stream-upgrade-failed":
		return fmt.Sprintf("filesystem access reached pod %s but the Kubernetes exec stream upgrade failed", result.SelectedPod)
	case "api-server-unavailable":
		return "filesystem access failed because the Kubernetes API/exec path was temporarily unavailable"
	case "eof-connection-interrupted":
		return "filesystem access failed because the exec stream was interrupted before the command completed"
	case "permission-denied":
		return fmt.Sprintf("filesystem access failed with a permission error for path %s", result.Path)
	case "path-not-found":
		return fmt.Sprintf("filesystem access reached the target container, but path %s does not exist", result.Path)
	case "path-parent-missing":
		return fmt.Sprintf("filesystem access failed because the parent directory for %s does not exist", result.Path)
	default:
		return firstNonEmptyTrimmed(result.RawError, "filesystem access failed and the CLI could not classify the fault with confidence")
	}
}

func filesystemDiagnosisExitError(result filesystemDiagnosisResult) error {
	if result.ProbeStatus == workflowResultStatusOK {
		return nil
	}
	err := fmt.Errorf("%s", result.Summary)
	switch result.FailureClass {
	case "permission-denied":
		return withExitCode(err, ExitCodePermissionDenied)
	case "path-not-found", "path-parent-missing", "pod-not-found", "container-not-found":
		return withExitCode(err, ExitCodeNotFound)
	case "container-not-ready", "exec-stream-upgrade-failed", "api-server-unavailable", "eof-connection-interrupted":
		return withExitCode(err, ExitCodeSystemFault)
	default:
		return withExitCode(err, ExitCodeIndeterminate)
	}
}

func sanitizeFilesystemDiagnosisResult(result filesystemDiagnosisResult, redact bool) filesystemDiagnosisResult {
	result.Redacted = redact
	if !redact {
		return result
	}
	result.RawError = redactDiagnosticString(result.RawError)
	result.RawExecError = redactDiagnosticString(result.RawExecError)
	result.Summary = redactDiagnosticString(result.Summary)
	result.LogEvidence = redactDiagnosticStringSlice(result.LogEvidence)
	result.Warnings = redactDiagnosticStringSlice(result.Warnings)
	if result.RuntimeDiagnosis != nil {
		diagnosis := *result.RuntimeDiagnosis
		diagnosis.Summary = redactDiagnosticString(diagnosis.Summary)
		diagnosis.Hint = redactDiagnosticString(diagnosis.Hint)
		diagnosis.Evidence = redactDiagnosticStringSlice(diagnosis.Evidence)
		diagnosis.Warnings = redactDiagnosticStringSlice(diagnosis.Warnings)
		result.RuntimeDiagnosis = &diagnosis
	}
	for index := range result.Pods {
		for cIndex := range result.Pods[index].Containers {
			result.Pods[index].Containers[cIndex].Message = redactDiagnosticString(result.Pods[index].Containers[cIndex].Message)
		}
	}
	for index := range result.Events {
		result.Events[index].Message = redactDiagnosticString(result.Events[index].Message)
	}
	return result
}

func renderFilesystemDiagnosisResult(w io.Writer, result filesystemDiagnosisResult) error {
	pairs := []kvPair{
		{Key: "schema_version", Value: result.SchemaVersion},
		{Key: "app", Value: result.App},
		{Key: "app_id", Value: result.AppID},
		{Key: "phase", Value: result.Phase},
		{Key: "component", Value: result.Component},
		{Key: "source", Value: result.Source},
		{Key: "access_mode", Value: result.AccessMode},
		{Key: "path", Value: result.Path},
		{Key: "mount_root", Value: result.MountRoot},
		{Key: "namespace", Value: result.Namespace},
		{Key: "probe_status", Value: result.ProbeStatus},
		{Key: "probe_kind", Value: result.ProbeKind},
		{Key: "selected_pod", Value: result.SelectedPod},
		{Key: "selected_container", Value: result.SelectedContainer},
		{Key: "failure_class", Value: result.FailureClass},
		{Key: "summary", Value: result.Summary},
		{Key: "raw_error", Value: result.RawError},
		{Key: "raw_exec_error", Value: result.RawExecError},
		{Key: "redacted", Value: fmt.Sprintf("%t", result.Redacted)},
	}
	if err := writeKeyValues(w, pairs...); err != nil {
		return err
	}
	for _, warning := range result.Warnings {
		if _, err := fmt.Fprintf(w, "warning=%s\n", warning); err != nil {
			return err
		}
	}
	if result.RuntimeDiagnosis != nil {
		if _, err := fmt.Fprintln(w, "\nruntime_diagnosis"); err != nil {
			return err
		}
		if err := renderAppDiagnosis(w, *result.RuntimeDiagnosis); err != nil {
			return err
		}
	}
	if len(result.Pods) > 0 {
		if _, err := fmt.Fprintln(w, "\npods"); err != nil {
			return err
		}
		if err := writeFilesystemPodSnapshotTable(w, result.Pods); err != nil {
			return err
		}
	}
	if len(result.LogEvidence) > 0 {
		if _, err := fmt.Fprintln(w, "\nlog_evidence"); err != nil {
			return err
		}
		for _, line := range result.LogEvidence {
			if _, err := fmt.Fprintln(w, line); err != nil {
				return err
			}
		}
	}
	if len(result.Events) > 0 {
		if _, err := fmt.Fprintln(w, "\nevents"); err != nil {
			return err
		}
		if err := writeClusterEventTable(w, result.Events); err != nil {
			return err
		}
	}
	return nil
}

func writeFilesystemPodSnapshotTable(w io.Writer, pods []filesystemPodSnapshot) error {
	tw := newTabWriter(w)
	if _, err := fmt.Fprintln(tw, "POD\tPHASE\tREADY\tNODE\tCONTAINER\tCONTAINER_READY\tRESTARTS\tSTATE\tREASON"); err != nil {
		return err
	}
	for _, pod := range pods {
		if len(pod.Containers) == 0 {
			if _, err := fmt.Fprintf(tw, "%s\t%s\t%t\t%s\t\t\t\t\t\n", pod.Name, pod.Phase, pod.Ready, pod.Node); err != nil {
				return err
			}
			continue
		}
		for _, container := range pod.Containers {
			if _, err := fmt.Fprintf(
				tw,
				"%s\t%s\t%t\t%s\t%s\t%t\t%d\t%s\t%s\n",
				pod.Name,
				pod.Phase,
				pod.Ready,
				pod.Node,
				container.Name,
				container.Ready,
				container.RestartCount,
				container.State,
				firstNonEmptyTrimmed(container.Reason, container.Message),
			); err != nil {
				return err
			}
		}
	}
	return tw.Flush()
}

func newTabWriter(w io.Writer) *tabwriter.Writer {
	return tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
}

func lookupFilesystemPod(pods []filesystemPodSnapshot, podName string) *filesystemPodSnapshot {
	for index := range pods {
		if strings.EqualFold(strings.TrimSpace(pods[index].Name), strings.TrimSpace(podName)) {
			return &pods[index]
		}
	}
	return nil
}

func filesystemPodHasContainer(pod filesystemPodSnapshot, containerName string) bool {
	for _, container := range pod.Containers {
		if strings.EqualFold(strings.TrimSpace(container.Name), strings.TrimSpace(containerName)) {
			return true
		}
	}
	return false
}

func filesystemPodContainerReady(pod filesystemPodSnapshot, containerName string) bool {
	for _, container := range pod.Containers {
		if strings.EqualFold(strings.TrimSpace(container.Name), strings.TrimSpace(containerName)) {
			return container.Ready
		}
	}
	return false
}
