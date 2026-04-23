package cli

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"

	"github.com/spf13/cobra"
)

const diagnosticEvidenceSchemaVersion = "fugue.logs-collect.v1"

type diagnosticCollectOptions struct {
	Since        string
	Until        string
	RequestID    string
	ResourceID   string
	OperationID  string
	TailLines    int
	WorkflowFile string
}

type diagnosticTimelineEntry struct {
	At      time.Time `json:"at"`
	Kind    string    `json:"kind"`
	Ref     string    `json:"ref,omitempty"`
	Status  string    `json:"status,omitempty"`
	Message string    `json:"message"`
}

type diagnosticCollectedLog struct {
	Name      string   `json:"name"`
	Category  string   `json:"category"`
	Namespace string   `json:"namespace,omitempty"`
	Pods      []string `json:"pods,omitempty"`
	Container string   `json:"container,omitempty"`
	Status    string   `json:"status"`
	Summary   string   `json:"summary,omitempty"`
	Lines     []string `json:"lines,omitempty"`
	Warnings  []string `json:"warnings,omitempty"`
}

type diagnosticEvidenceResult struct {
	SchemaVersion      string                        `json:"schema_version"`
	App                string                        `json:"app"`
	AppID              string                        `json:"app_id"`
	ObservedAt         time.Time                     `json:"observed_at"`
	Since              string                        `json:"since,omitempty"`
	Until              string                        `json:"until,omitempty"`
	RequestID          string                        `json:"request_id,omitempty"`
	ResourceID         string                        `json:"resource_id,omitempty"`
	OperationID        string                        `json:"operation_id,omitempty"`
	Summary            string                        `json:"summary"`
	Redacted           bool                          `json:"redacted"`
	AppOverview        *appOverviewSnapshot          `json:"app_overview,omitempty"`
	RuntimeDiagnosis   *appDiagnosis                 `json:"runtime_diagnosis,omitempty"`
	OperationDiagnosis *model.OperationDiagnosis     `json:"operation_diagnosis,omitempty"`
	PodInventory       *model.AppRuntimePodInventory `json:"pod_inventory,omitempty"`
	Workflow           *workflowRunResult            `json:"workflow,omitempty"`
	Timeline           []diagnosticTimelineEntry     `json:"timeline,omitempty"`
	Logs               []diagnosticCollectedLog      `json:"logs,omitempty"`
	Warnings           []string                      `json:"warnings,omitempty"`
}

func (c *CLI) newLogsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logs",
		Short: "Collect correlated log evidence for investigation workflows",
	}
	cmd.AddCommand(c.newLogsCollectCommand(), c.newLogsQueryCommand())
	return cmd
}

func (c *CLI) newLogsCollectCommand() *cobra.Command {
	opts := diagnosticCollectOptions{TailLines: 200}
	cmd := &cobra.Command{
		Use:   "collect <app>",
		Short: "Collect app, control-plane, and operation log fragments into one result",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := c.newClient()
			if err != nil {
				return err
			}
			if _, err := resolveLogsQueryTimeWindow(opts.Since, opts.Until, time.Now().UTC()); err != nil {
				return withExitCode(err, ExitCodeUserInput)
			}
			app, err := c.resolveWorkspaceApp(client, args[0])
			if err != nil {
				return err
			}
			result := c.collectDiagnosticEvidence(client, app, opts)
			sanitized := sanitizeDiagnosticEvidenceResult(result, c.shouldRedact())
			if c.wantsJSON() {
				return writeJSON(c.stdout, sanitized)
			}
			return renderDiagnosticEvidenceResult(c.stdout, sanitized)
		},
	}
	cmd.Flags().StringVar(&opts.Since, "since", "", "Requested time window label for the investigation, for example 1h")
	cmd.Flags().StringVar(&opts.Until, "until", "", "Upper time bound for the investigation, for example 15m or an RFC3339 timestamp")
	cmd.Flags().StringVar(&opts.RequestID, "request-id", "", "Request or trace identifier used to filter log fragments")
	cmd.Flags().StringVar(&opts.ResourceID, "resource-id", "", "Resource identifier used to filter log fragments")
	cmd.Flags().StringVar(&opts.OperationID, "operation", "", "Operation identifier to correlate build/deploy evidence")
	cmd.Flags().IntVar(&opts.TailLines, "tail", opts.TailLines, "Maximum lines to request from each log source before local filtering")
	cmd.Flags().StringVar(&opts.WorkflowFile, "workflow-file", "", "Optional workflow file to execute and include as part of the evidence set")
	return cmd
}

func (c *CLI) collectDiagnosticEvidence(client *Client, app model.App, opts diagnosticCollectOptions) diagnosticEvidenceResult {
	result := diagnosticEvidenceResult{
		SchemaVersion: diagnosticEvidenceSchemaVersion,
		App:           strings.TrimSpace(app.Name),
		AppID:         strings.TrimSpace(app.ID),
		ObservedAt:    time.Now().UTC(),
		Since:         strings.TrimSpace(opts.Since),
		Until:         strings.TrimSpace(opts.Until),
		RequestID:     strings.TrimSpace(opts.RequestID),
		ResourceID:    strings.TrimSpace(opts.ResourceID),
		OperationID:   strings.TrimSpace(opts.OperationID),
		Redacted:      c.shouldRedact(),
		Summary:       "collected diagnostic evidence",
	}
	window, windowErr := resolveLogsQueryTimeWindow(opts.Since, opts.Until, result.ObservedAt)
	if windowErr != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("time window unavailable: %v", windowErr))
	}

	snapshot, err := c.loadAppOverview(client, app.ID)
	if err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("app overview unavailable: %v", err))
	} else {
		result.AppOverview = &snapshot
		if snapshot.PodInventory != nil {
			result.PodInventory = snapshot.PodInventory
		}
	}

	if result.PodInventory == nil {
		if inventory, err := client.GetAppRuntimePods(app.ID, "app"); err != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("runtime pod inventory unavailable: %v", err))
		} else {
			result.PodInventory = &inventory
		}
	}

	if diagnosis, err := client.TryGetAppDiagnosis(app.ID, "app"); err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("runtime diagnosis unavailable: %v", err))
	} else if diagnosis != nil {
		result.RuntimeDiagnosis = diagnosis
	}

	operations := resultOperations(result)
	if len(operations) == 0 {
		if fetched, err := client.ListOperations(app.ID); err != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("operation inventory unavailable: %v", err))
		} else {
			operations = fetched
		}
	}
	sortOperationsNewestFirst(operations)

	selectedOperation := selectDiagnosticOperation(operations, app, opts.OperationID)
	if selectedOperation != nil {
		result.OperationID = strings.TrimSpace(selectedOperation.ID)
		if diagnosis, err := client.TryGetOperationDiagnosis(selectedOperation.ID); err != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("operation diagnosis unavailable: %v", err))
		} else if diagnosis != nil {
			result.OperationDiagnosis = diagnosis
		}
	}

	if strings.TrimSpace(opts.WorkflowFile) != "" {
		spec, err := loadWorkflowSpec(opts.WorkflowFile)
		if err != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("workflow evidence unavailable: %v", err))
		} else {
			workflowResult, workflowErr := c.runWorkflowSpec(opts.WorkflowFile, spec)
			result.Workflow = &workflowResult
			if workflowErr != nil {
				result.Warnings = append(result.Warnings, fmt.Sprintf("workflow finished with failure evidence: %v", workflowErr))
			}
		}
	}

	filterTerms := buildDiagnosticFilterTerms(app, selectedOperation, opts)
	result.Logs = c.collectDiagnosticLogs(client, app, operations, selectedOperation, filterTerms, opts.TailLines, window, &result)
	result.Timeline = buildDiagnosticTimeline(app, operations, result.PodInventory, result.RuntimeDiagnosis)
	result.Summary = summarizeDiagnosticEvidence(result, selectedOperation)
	return result
}

func resultOperations(result diagnosticEvidenceResult) []model.Operation {
	if result.AppOverview == nil || len(result.AppOverview.Operations) == 0 {
		return nil
	}
	return append([]model.Operation(nil), result.AppOverview.Operations...)
}

func selectDiagnosticOperation(operations []model.Operation, app model.App, requested string) *model.Operation {
	requested = strings.TrimSpace(requested)
	if requested != "" {
		for _, operation := range operations {
			if strings.EqualFold(strings.TrimSpace(operation.ID), requested) {
				op := operation
				return &op
			}
		}
		return nil
	}
	if value := strings.TrimSpace(app.Status.LastOperationID); value != "" {
		for _, operation := range operations {
			if strings.EqualFold(strings.TrimSpace(operation.ID), value) {
				op := operation
				return &op
			}
		}
	}
	if latestDeploy := latestOperationOfType(operations, model.OperationTypeDeploy); latestDeploy != nil {
		return latestDeploy
	}
	if latestImport := latestOperationOfType(operations, model.OperationTypeImport); latestImport != nil {
		return latestImport
	}
	if len(operations) == 0 {
		return nil
	}
	op := operations[0]
	return &op
}

func buildDiagnosticFilterTerms(app model.App, selectedOperation *model.Operation, opts diagnosticCollectOptions) []string {
	terms := []string{
		strings.TrimSpace(app.ID),
		strings.TrimSpace(app.Name),
		strings.TrimSpace(opts.RequestID),
		strings.TrimSpace(opts.ResourceID),
		strings.TrimSpace(opts.OperationID),
	}
	if selectedOperation != nil {
		terms = append(terms, strings.TrimSpace(selectedOperation.ID))
	}
	out := make([]string, 0, len(terms))
	for _, term := range terms {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}
		out = appendUniqueString(out, term)
	}
	return out
}

func (c *CLI) collectDiagnosticLogs(client *Client, app model.App, operations []model.Operation, selectedOperation *model.Operation, terms []string, tail int, window logsQueryTimeWindow, result *diagnosticEvidenceResult) []diagnosticCollectedLog {
	sources := make([]diagnosticCollectedLog, 0, 5)
	sources = append(sources, collectRuntimeLogSource(client, app.ID, "app", "runtime-app", terms, tail, window))
	if app.Spec.Postgres != nil {
		sources = append(sources, collectRuntimeLogSource(client, app.ID, "postgres", "runtime-postgres", terms, tail, window))
	}
	if latestImport := latestOperationOfType(operations, model.OperationTypeImport); latestImport != nil {
		sources = append(sources, collectBuildLogSource(client, app.ID, latestImport.ID, terms, tail, window))
	}

	namespace, err := c.detectBuildEvidenceControlPlaneNamespace(client)
	if err != nil {
		if result != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("control-plane namespace unavailable: %v", err))
		}
		namespace = buildEvidenceDefaultControlPlaneNamespace
	}
	sources = append(sources,
		c.collectClusterComponentLogSource(client, namespace, "app.kubernetes.io/component=api", "api", "control-plane-api", terms, tail, window),
		c.collectClusterComponentLogSource(client, namespace, "app.kubernetes.io/component=controller", "controller", "control-plane-controller", terms, tail, window),
	)
	out := make([]diagnosticCollectedLog, 0, len(sources))
	for _, source := range sources {
		out = append(out, source)
	}
	return out
}

func collectRuntimeLogSource(client *Client, appID, component, name string, terms []string, tail int, window logsQueryTimeWindow) diagnosticCollectedLog {
	source := diagnosticCollectedLog{
		Name:     name,
		Category: "workload",
		Status:   "unavailable",
	}
	logs, err := client.GetRuntimeLogs(appID, runtimeLogsOptions{
		Component: component,
		TailLines: tail,
	})
	if err != nil {
		source.Summary = err.Error()
		return source
	}
	source.Status = "ok"
	source.Namespace = strings.TrimSpace(logs.Namespace)
	source.Pods = append([]string(nil), logs.Pods...)
	source.Container = strings.TrimSpace(logs.Container)
	source.Lines = filterDiagnosticLogLinesWithWindow(logs.Logs, terms, 20, window)
	source.Summary = summarizeCollectedLogSource(source, terms)
	source.Warnings = append([]string(nil), logs.Warnings...)
	return source
}

func collectBuildLogSource(client *Client, appID, operationID string, terms []string, tail int, window logsQueryTimeWindow) diagnosticCollectedLog {
	source := diagnosticCollectedLog{
		Name:     "build",
		Category: "operation",
		Status:   "unavailable",
	}
	logs, err := client.GetBuildLogs(appID, operationID, tail)
	if err != nil {
		source.Summary = err.Error()
		return source
	}
	source.Status = "ok"
	source.Lines = filterDiagnosticLogLinesWithWindow(logs.Logs, append(terms, operationID), 20, window)
	source.Summary = firstNonEmptyTrimmed(strings.TrimSpace(logs.Summary), summarizeCollectedLogSource(source, terms))
	return source
}

func (c *CLI) collectClusterComponentLogSource(client *Client, namespace, selector, preferredContainer, name string, terms []string, tail int, window logsQueryTimeWindow) diagnosticCollectedLog {
	source := diagnosticCollectedLog{
		Name:      name,
		Category:  "control-plane",
		Namespace: namespace,
		Status:    "unavailable",
	}
	pods, err := client.ListClusterPods(clusterPodsOptions{
		Namespace:         namespace,
		LabelSelector:     selector,
		IncludeTerminated: false,
	})
	if err != nil {
		source.Summary = err.Error()
		return source
	}
	if len(pods) == 0 {
		source.Summary = "no pods matched selector " + selector
		return source
	}
	sortClusterPodsForEvidence(pods)
	source.Pods = collectClusterPodNames(pods)
	for _, pod := range pods {
		container := selectEvidenceContainer(pod, preferredContainer)
		logs, err := client.GetClusterLogs(clusterLogsOptions{
			Namespace: namespace,
			Pod:       strings.TrimSpace(pod.Name),
			Container: container,
			TailLines: tail,
		})
		if err != nil {
			source.Warnings = append(source.Warnings, fmt.Sprintf("%s: %v", pod.Name, err))
			continue
		}
		source.Status = "ok"
		source.Container = container
		source.Lines = filterDiagnosticLogLinesWithWindow(logs.Logs, terms, 20, window)
		source.Summary = summarizeCollectedLogSource(source, terms)
		return source
	}
	if source.Summary == "" {
		source.Summary = "no readable pod logs were available"
	}
	return source
}

func collectClusterPodNames(pods []model.ClusterPod) []string {
	out := make([]string, 0, len(pods))
	for _, pod := range pods {
		name := strings.TrimSpace(pod.Name)
		if name == "" {
			continue
		}
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func filterDiagnosticLogLines(raw string, terms []string, limit int) []string {
	if len(terms) > 0 {
		if lines := extractRelevantLogLines(raw, terms, limit); len(lines) > 0 {
			return lines
		}
	}
	return tailNonEmptyLines(raw, limit)
}

func filterDiagnosticLogLinesWithWindow(raw string, terms []string, limit int, window logsQueryTimeWindow) []string {
	return filterDiagnosticLogLines(filterRawLogTextForTimeWindow(raw, window), terms, limit)
}

func summarizeCollectedLogSource(source diagnosticCollectedLog, terms []string) string {
	if len(source.Lines) == 0 {
		if len(terms) > 0 {
			return "no matching lines were found in the collected log tail"
		}
		return "log source returned no non-empty lines"
	}
	if len(terms) > 0 {
		return fmt.Sprintf("matched %d line(s) against the requested filters", len(source.Lines))
	}
	return fmt.Sprintf("captured the latest %d non-empty line(s)", len(source.Lines))
}

func buildDiagnosticTimeline(app model.App, operations []model.Operation, podInventory *model.AppRuntimePodInventory, diagnosis *appDiagnosis) []diagnosticTimelineEntry {
	entries := make([]diagnosticTimelineEntry, 0, len(operations)*3+8)
	if !app.CreatedAt.IsZero() {
		entries = append(entries, diagnosticTimelineEntry{
			At:      app.CreatedAt.UTC(),
			Kind:    "app",
			Ref:     strings.TrimSpace(app.ID),
			Status:  "created",
			Message: "app object created",
		})
	}
	if app.Status.CurrentReleaseStartedAt != nil && !app.Status.CurrentReleaseStartedAt.IsZero() {
		entries = append(entries, diagnosticTimelineEntry{
			At:      app.Status.CurrentReleaseStartedAt.UTC(),
			Kind:    "release",
			Ref:     strings.TrimSpace(app.ID),
			Status:  "started",
			Message: "current release started",
		})
	}
	if app.Status.CurrentReleaseReadyAt != nil && !app.Status.CurrentReleaseReadyAt.IsZero() {
		entries = append(entries, diagnosticTimelineEntry{
			At:      app.Status.CurrentReleaseReadyAt.UTC(),
			Kind:    "release",
			Ref:     strings.TrimSpace(app.ID),
			Status:  "ready",
			Message: "current release became ready",
		})
	}
	for _, operation := range operations {
		entries = append(entries, diagnosticTimelineEntry{
			At:      operation.CreatedAt.UTC(),
			Kind:    "operation",
			Ref:     strings.TrimSpace(operation.ID),
			Status:  strings.TrimSpace(operation.Status),
			Message: fmt.Sprintf("operation type=%s created", strings.TrimSpace(operation.Type)),
		})
		if operation.StartedAt != nil && !operation.StartedAt.IsZero() {
			entries = append(entries, diagnosticTimelineEntry{
				At:      operation.StartedAt.UTC(),
				Kind:    "operation",
				Ref:     strings.TrimSpace(operation.ID),
				Status:  "started",
				Message: fmt.Sprintf("operation type=%s started", strings.TrimSpace(operation.Type)),
			})
		}
		if operation.CompletedAt != nil && !operation.CompletedAt.IsZero() {
			entries = append(entries, diagnosticTimelineEntry{
				At:      operation.CompletedAt.UTC(),
				Kind:    "operation",
				Ref:     strings.TrimSpace(operation.ID),
				Status:  strings.TrimSpace(operation.Status),
				Message: firstNonEmptyTrimmed(strings.TrimSpace(operation.ResultMessage), strings.TrimSpace(operation.ErrorMessage), fmt.Sprintf("operation type=%s completed", strings.TrimSpace(operation.Type))),
			})
		}
	}
	if podInventory != nil {
		for _, pod := range flattenRuntimeInventoryPods(*podInventory) {
			if pod.StartTime == nil || pod.StartTime.IsZero() {
				continue
			}
			entries = append(entries, diagnosticTimelineEntry{
				At:      pod.StartTime.UTC(),
				Kind:    "pod",
				Ref:     strings.TrimSpace(pod.Name),
				Status:  strings.TrimSpace(pod.Phase),
				Message: fmt.Sprintf("pod ready=%t node=%s", pod.Ready, strings.TrimSpace(pod.NodeName)),
			})
		}
	}
	if diagnosis != nil {
		for _, event := range diagnosis.Events {
			entries = append(entries, diagnosticTimelineEntry{
				At:      clusterEventSortTimeForCLI(event),
				Kind:    "cluster-event",
				Ref:     strings.TrimSpace(event.ObjectName),
				Status:  strings.TrimSpace(event.Reason),
				Message: strings.TrimSpace(event.Message),
			})
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		if !entries[i].At.Equal(entries[j].At) {
			return entries[i].At.Before(entries[j].At)
		}
		if entries[i].Kind != entries[j].Kind {
			return entries[i].Kind < entries[j].Kind
		}
		return entries[i].Ref < entries[j].Ref
	})
	return entries
}

func summarizeDiagnosticEvidence(result diagnosticEvidenceResult, selectedOperation *model.Operation) string {
	switch {
	case result.Workflow != nil && strings.EqualFold(result.Workflow.Status, workflowResultStatusFail):
		return firstNonEmptyTrimmed(strings.TrimSpace(result.Workflow.Summary), "workflow captured failure evidence")
	case result.RuntimeDiagnosis != nil && strings.TrimSpace(result.RuntimeDiagnosis.Summary) != "":
		return strings.TrimSpace(result.RuntimeDiagnosis.Summary)
	case selectedOperation != nil && strings.TrimSpace(selectedOperation.ResultMessage) != "":
		return strings.TrimSpace(selectedOperation.ResultMessage)
	case len(result.Logs) > 0:
		return "collected correlated runtime, operation, and control-plane log evidence"
	default:
		return "collected diagnostic evidence"
	}
}

func sanitizeDiagnosticEvidenceResult(result diagnosticEvidenceResult, redact bool) diagnosticEvidenceResult {
	result.Redacted = redact
	if result.AppOverview != nil && redact {
		snapshot := redactOverviewSnapshotForOutput(*result.AppOverview)
		result.AppOverview = &snapshot
	}
	if result.RuntimeDiagnosis != nil && redact {
		diagnosis := *result.RuntimeDiagnosis
		diagnosis.Summary = redactDiagnosticString(diagnosis.Summary)
		diagnosis.Hint = redactDiagnosticString(diagnosis.Hint)
		diagnosis.Evidence = redactDiagnosticStringSlice(diagnosis.Evidence)
		diagnosis.Warnings = redactDiagnosticStringSlice(diagnosis.Warnings)
		for index := range diagnosis.Events {
			diagnosis.Events[index].Message = redactDiagnosticString(diagnosis.Events[index].Message)
		}
		result.RuntimeDiagnosis = &diagnosis
	}
	if result.OperationDiagnosis != nil && redact {
		diagnosis := *result.OperationDiagnosis
		diagnosis.Summary = redactDiagnosticString(diagnosis.Summary)
		diagnosis.Hint = redactDiagnosticString(diagnosis.Hint)
		diagnosis.Evidence = redactDiagnosticStringSlice(diagnosis.Evidence)
		result.OperationDiagnosis = &diagnosis
	}
	if result.Workflow != nil {
		workflow := sanitizeWorkflowRunResult(*result.Workflow, redact)
		result.Workflow = &workflow
	}
	result.Summary = redactDiagnosticString(result.Summary)
	result.Warnings = redactDiagnosticStringSlice(result.Warnings)
	for index := range result.Logs {
		result.Logs[index].Summary = redactDiagnosticString(result.Logs[index].Summary)
		result.Logs[index].Warnings = redactDiagnosticStringSlice(result.Logs[index].Warnings)
		result.Logs[index].Lines = redactDiagnosticStringSlice(result.Logs[index].Lines)
	}
	for index := range result.Timeline {
		result.Timeline[index].Message = redactDiagnosticString(result.Timeline[index].Message)
	}
	return result
}

func renderDiagnosticEvidenceResult(w io.Writer, result diagnosticEvidenceResult) error {
	pairs := []kvPair{
		{Key: "schema_version", Value: result.SchemaVersion},
		{Key: "app", Value: result.App},
		{Key: "app_id", Value: result.AppID},
		{Key: "observed_at", Value: formatTime(result.ObservedAt)},
		{Key: "since", Value: result.Since},
		{Key: "until", Value: result.Until},
		{Key: "request_id", Value: result.RequestID},
		{Key: "resource_id", Value: result.ResourceID},
		{Key: "operation_id", Value: result.OperationID},
		{Key: "summary", Value: result.Summary},
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
	if len(result.Timeline) > 0 {
		if _, err := fmt.Fprintln(w, "\ntimeline"); err != nil {
			return err
		}
		if err := writeDiagnosticTimelineTable(w, result.Timeline); err != nil {
			return err
		}
	}
	for _, source := range result.Logs {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "log_source %s\n", source.Name); err != nil {
			return err
		}
		if err := writeKeyValues(w,
			kvPair{Key: "category", Value: source.Category},
			kvPair{Key: "status", Value: source.Status},
			kvPair{Key: "namespace", Value: source.Namespace},
			kvPair{Key: "pods", Value: strings.Join(source.Pods, ",")},
			kvPair{Key: "container", Value: source.Container},
			kvPair{Key: "summary", Value: source.Summary},
		); err != nil {
			return err
		}
		for _, warning := range source.Warnings {
			if _, err := fmt.Fprintf(w, "warning=%s\n", warning); err != nil {
				return err
			}
		}
		if len(source.Lines) > 0 {
			if _, err := fmt.Fprintln(w, "\nlines:"); err != nil {
				return err
			}
			for _, line := range source.Lines {
				if _, err := fmt.Fprintln(w, line); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func writeDiagnosticTimelineTable(w io.Writer, entries []diagnosticTimelineEntry) error {
	tw := newTabWriter(w)
	if _, err := fmt.Fprintln(tw, "TIME\tKIND\tREF\tSTATUS\tMESSAGE"); err != nil {
		return err
	}
	for _, entry := range entries {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", formatTime(entry.At), entry.Kind, entry.Ref, entry.Status, entry.Message); err != nil {
			return err
		}
	}
	return tw.Flush()
}
