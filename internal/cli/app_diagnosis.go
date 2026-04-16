package cli

import (
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"text/tabwriter"

	"fugue/internal/model"
)

var queuedDeployOperationPattern = regexp.MustCompile(`queued deploy operation ([A-Za-z0-9_-]+)`)

type buildArtifactStage struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Detail string `json:"detail,omitempty"`
}

type appBuildArtifactReport struct {
	Service                 string               `json:"service,omitempty"`
	BuildOperationID        string               `json:"build_operation_id,omitempty"`
	BuildOperationStatus    string               `json:"build_operation_status,omitempty"`
	BuildStrategy           string               `json:"build_strategy,omitempty"`
	BuildMessage            string               `json:"build_message,omitempty"`
	BuildJobName            string               `json:"build_job_name,omitempty"`
	BuildLogSource          string               `json:"build_log_source,omitempty"`
	BuildLogsAvailable      *bool                `json:"build_logs_available,omitempty"`
	ManagedImageRef         string               `json:"managed_image_ref,omitempty"`
	RuntimeImageRef         string               `json:"runtime_image_ref,omitempty"`
	RegistryImageStatus     string               `json:"registry_image_status,omitempty"`
	RegistryImageCurrent    bool                 `json:"registry_image_current,omitempty"`
	LinkedDeployOperationID string               `json:"linked_deploy_operation_id,omitempty"`
	LinkedDeployStatus      string               `json:"linked_deploy_status,omitempty"`
	LinkedDeployMessage     string               `json:"linked_deploy_message,omitempty"`
	LatestPodGroup          string               `json:"latest_pod_group,omitempty"`
	LivePods                int                  `json:"live_pods,omitempty"`
	ReadyPods               int                  `json:"ready_pods,omitempty"`
	PodIssues               []string             `json:"pod_issues,omitempty"`
	Stages                  []buildArtifactStage `json:"stages,omitempty"`
	Warnings                []string             `json:"warnings,omitempty"`
}

type appOverviewDiagnosis struct {
	Category        string                  `json:"category"`
	Summary         string                  `json:"summary"`
	Hint            string                  `json:"hint,omitempty"`
	Evidence        []string                `json:"evidence,omitempty"`
	ArtifactSummary *appBuildArtifactReport `json:"artifact_summary,omitempty"`
}

func boolPtr(value bool) *bool {
	return &value
}

func (c *CLI) collectBuildArtifactReport(client *Client, appID string, logs buildLogsResponse) *appBuildArtifactReport {
	report := &appBuildArtifactReport{
		BuildOperationID:     strings.TrimSpace(logs.OperationID),
		BuildOperationStatus: strings.TrimSpace(logs.OperationStatus),
		BuildStrategy:        strings.TrimSpace(logs.BuildStrategy),
		BuildJobName:         strings.TrimSpace(logs.JobName),
		BuildLogSource:       strings.TrimSpace(logs.Source),
	}
	if report.BuildOperationID == "" && report.BuildJobName == "" && report.BuildLogSource == "" && report.BuildStrategy == "" {
		return nil
	}
	report.BuildLogsAvailable = boolPtr(logs.Available)
	report.BuildMessage = firstNonEmptyTrimmed(strings.TrimSpace(logs.ErrorMessage), strings.TrimSpace(logs.Summary), strings.TrimSpace(logs.ResultMessage))

	var (
		importOp   *model.Operation
		operations []model.Operation
	)
	if report.BuildOperationID != "" {
		if op, err := client.GetOperation(report.BuildOperationID); err != nil {
			report.Warnings = appendUniqueString(report.Warnings, fmt.Sprintf("build operation details unavailable: %v", err))
		} else {
			opCopy := op
			importOp = &opCopy
		}
	}
	if ops, err := client.ListOperations(appID); err != nil {
		report.Warnings = appendUniqueString(report.Warnings, fmt.Sprintf("operation inventory unavailable: %v", err))
	} else {
		operations = ops
		if importOp == nil && report.BuildOperationID != "" {
			if op := findOperationPtrByID(operations, report.BuildOperationID); op != nil {
				importOp = op
			}
		}
	}

	var images *appImageInventoryResponse
	if inventory, err := client.GetAppImages(appID); err != nil {
		report.Warnings = appendUniqueString(report.Warnings, fmt.Sprintf("image inventory unavailable: %v", err))
	} else {
		images = &inventory
	}

	var podInventory *model.AppRuntimePodInventory
	if inventory, err := client.GetAppRuntimePods(appID, "app"); err != nil {
		report.Warnings = appendUniqueString(report.Warnings, fmt.Sprintf("runtime pod inventory unavailable: %v", err))
	} else {
		podInventory = &inventory
	}

	enrichBuildArtifactReport(report, importOp, operations, images, podInventory)
	if report.BuildJobName == "" {
		report.BuildJobName = firstNonEmptyTrimmed(report.LatestPodGroup, report.LinkedDeployOperationID)
	}
	if !buildArtifactReportHasContent(report) {
		return nil
	}
	return report
}

func (c *CLI) buildAppOverviewDiagnosis(client *Client, snapshot appOverviewSnapshot) (*appOverviewDiagnosis, error) {
	report, latestImport, latestDeploy := summarizeAppBuildArtifact(snapshot.App, snapshot.Operations, snapshot.Images, snapshot.PodInventory)

	var deployDiagnosis *model.OperationDiagnosis
	if latestDeploy != nil {
		diagnosis, err := c.tryLoadOperationDiagnosis(client, *latestDeploy)
		if err != nil {
			if report != nil {
				report.Warnings = appendUniqueString(report.Warnings, fmt.Sprintf("deploy diagnosis unavailable: %v", err))
			}
		} else {
			deployDiagnosis = diagnosis
		}
	}

	return summarizeAppOverviewDiagnosis(snapshot.App, latestImport, latestDeploy, deployDiagnosis, report), nil
}

func summarizeAppBuildArtifact(app model.App, operations []model.Operation, images *appImageInventoryResponse, podInventory *model.AppRuntimePodInventory) (*appBuildArtifactReport, *model.Operation, *model.Operation) {
	if len(operations) == 0 && images == nil && podInventory == nil {
		return nil, nil, nil
	}

	sorted := append([]model.Operation(nil), operations...)
	sortOperationsNewestFirst(sorted)
	latestImport := latestOperationOfType(sorted, model.OperationTypeImport)
	latestDeploy := latestOperationOfType(sorted, model.OperationTypeDeploy)
	if latestImport != nil {
		if linked := linkedDeployOperation(*latestImport, sorted); linked != nil {
			latestDeploy = linked
		}
	}

	report := &appBuildArtifactReport{
		Service:              buildArtifactServiceName(app, latestImport, latestDeploy),
		BuildOperationID:     operationID(latestImport),
		BuildOperationStatus: operationStatus(latestImport),
		BuildStrategy:        buildArtifactStrategy(latestImport, latestDeploy),
		BuildMessage:         operationMessage(latestImport),
	}
	enrichBuildArtifactReport(report, latestImport, sorted, images, podInventory)
	if !buildArtifactReportHasContent(report) {
		return nil, latestImport, latestDeploy
	}
	return report, latestImport, latestDeploy
}

func enrichBuildArtifactReport(report *appBuildArtifactReport, importOp *model.Operation, operations []model.Operation, images *appImageInventoryResponse, podInventory *model.AppRuntimePodInventory) {
	if report == nil {
		return
	}

	var deployOp *model.Operation
	if importOp != nil {
		deployOp = linkedDeployOperation(*importOp, operations)
	}
	if deployOp == nil {
		deployOp = latestOperationOfType(operations, model.OperationTypeDeploy)
	}

	if service := buildArtifactServiceName(model.App{Name: report.Service}, importOp, deployOp); service != "" && report.Service == "" {
		report.Service = service
	}
	if report.BuildOperationID == "" {
		report.BuildOperationID = operationID(importOp)
	}
	if report.BuildOperationStatus == "" {
		report.BuildOperationStatus = operationStatus(importOp)
	}
	if report.BuildStrategy == "" {
		report.BuildStrategy = buildArtifactStrategy(importOp, deployOp)
	}
	if report.BuildMessage == "" {
		report.BuildMessage = operationMessage(importOp)
	}

	report.LinkedDeployOperationID = operationID(deployOp)
	report.LinkedDeployStatus = operationStatus(deployOp)
	report.LinkedDeployMessage = operationMessage(deployOp)

	report.ManagedImageRef = firstNonEmptyTrimmed(
		sourceResolvedImageRef(importOp),
		sourceResolvedImageRef(deployOp),
	)
	report.RuntimeImageRef = firstNonEmptyTrimmed(
		specImage(deployOp),
		specImage(importOp),
	)

	if version := matchImageVersion(images, report.ManagedImageRef, report.RuntimeImageRef); version != nil {
		report.RegistryImageStatus = normalizeImageInventoryStatus(version.Status)
		report.RegistryImageCurrent = version.Current
	}

	podState := summarizeLatestPodState(podInventory, report.RuntimeImageRef)
	report.LatestPodGroup = podState.Group
	report.LivePods = podState.LivePods
	report.ReadyPods = podState.ReadyPods
	report.PodIssues = podState.Issues

	report.Stages = buildArtifactStages(report)
}

func summarizeAppOverviewDiagnosis(app model.App, latestImport, latestDeploy *model.Operation, deployDiagnosis *model.OperationDiagnosis, report *appBuildArtifactReport) *appOverviewDiagnosis {
	if report != nil {
		status := normalizeImageInventoryStatus(report.RegistryImageStatus)
		switch {
		case status == "missing":
			return &appOverviewDiagnosis{
				Category:        "runtime-image-missing",
				Summary:         runtimeImageMissingSummary(report),
				Hint:            buildArtifactHint(app, report),
				Evidence:        buildArtifactEvidence(report, deployDiagnosis),
				ArtifactSummary: report,
			}
		case hasImagePullPodIssue(report.PodIssues):
			return &appOverviewDiagnosis{
				Category:        "runtime-image-pull-failed",
				Summary:         runtimeImagePullFailureSummary(report),
				Hint:            buildArtifactHint(app, report),
				Evidence:        buildArtifactEvidence(report, deployDiagnosis),
				ArtifactSummary: report,
			}
		}
	}

	if deployDiagnosis != nil {
		return &appOverviewDiagnosis{
			Category:        strings.TrimSpace(deployDiagnosis.Category),
			Summary:         strings.TrimSpace(deployDiagnosis.Summary),
			Hint:            strings.TrimSpace(deployDiagnosis.Hint),
			Evidence:        buildArtifactEvidence(report, deployDiagnosis),
			ArtifactSummary: report,
		}
	}

	if latestImport != nil && !isTerminalOperationStatus(latestImport.Status) {
		return &appOverviewDiagnosis{
			Category:        "import-" + strings.TrimSpace(latestImport.Status),
			Summary:         firstNonEmptyTrimmed(operationMessage(latestImport), "latest import operation is still in progress"),
			Hint:            buildArtifactHint(app, report),
			Evidence:        buildArtifactEvidence(report, nil),
			ArtifactSummary: report,
		}
	}

	if latestDeploy != nil && strings.EqualFold(strings.TrimSpace(latestDeploy.Status), model.OperationStatusFailed) {
		return &appOverviewDiagnosis{
			Category:        "deploy-failed",
			Summary:         firstNonEmptyTrimmed(operationMessage(latestDeploy), "latest deploy operation failed"),
			Hint:            buildArtifactHint(app, report),
			Evidence:        buildArtifactEvidence(report, nil),
			ArtifactSummary: report,
		}
	}

	if report == nil {
		return nil
	}
	if strings.EqualFold(report.LinkedDeployStatus, model.OperationStatusCompleted) &&
		normalizeImageInventoryStatus(report.RegistryImageStatus) == "available" &&
		len(report.PodIssues) == 0 &&
		report.ReadyPods > 0 {
		return nil
	}
	if len(report.PodIssues) == 0 && report.LinkedDeployOperationID == "" && report.BuildOperationID == "" {
		return nil
	}
	return &appOverviewDiagnosis{
		Category:        "state-summary",
		Summary:         fallbackArtifactSummary(report),
		Hint:            buildArtifactHint(app, report),
		Evidence:        buildArtifactEvidence(report, nil),
		ArtifactSummary: report,
	}
}

func renderAppOverviewDiagnosis(w io.Writer, diagnosis *appOverviewDiagnosis) error {
	if diagnosis == nil {
		return nil
	}
	pairs := []kvPair{
		{Key: "category", Value: strings.TrimSpace(diagnosis.Category)},
		{Key: "summary", Value: strings.TrimSpace(diagnosis.Summary)},
		{Key: "hint", Value: strings.TrimSpace(diagnosis.Hint)},
	}
	if diagnosis.ArtifactSummary != nil {
		artifact := diagnosis.ArtifactSummary
		pairs = append(pairs,
			kvPair{Key: "service", Value: strings.TrimSpace(artifact.Service)},
			kvPair{Key: "build_operation_id", Value: strings.TrimSpace(artifact.BuildOperationID)},
			kvPair{Key: "build_status", Value: strings.TrimSpace(artifact.BuildOperationStatus)},
			kvPair{Key: "deploy_operation_id", Value: strings.TrimSpace(artifact.LinkedDeployOperationID)},
			kvPair{Key: "deploy_status", Value: strings.TrimSpace(artifact.LinkedDeployStatus)},
			kvPair{Key: "managed_image_ref", Value: strings.TrimSpace(artifact.ManagedImageRef)},
			kvPair{Key: "runtime_image_ref", Value: strings.TrimSpace(artifact.RuntimeImageRef)},
			kvPair{Key: "registry_image_status", Value: strings.TrimSpace(artifact.RegistryImageStatus)},
			kvPair{Key: "latest_pod_group", Value: strings.TrimSpace(artifact.LatestPodGroup)},
		)
	}
	if err := writeKeyValues(w, pairs...); err != nil {
		return err
	}
	for _, evidence := range diagnosis.Evidence {
		if _, err := fmt.Fprintf(w, "evidence=%s\n", evidence); err != nil {
			return err
		}
	}
	return nil
}

func renderBuildLogsReport(w io.Writer, logs buildLogsResponse) error {
	buildMessage := firstNonEmptyTrimmed(strings.TrimSpace(logs.Summary), strings.TrimSpace(logs.ResultMessage), strings.TrimSpace(logs.ErrorMessage))
	summary := preferredBuildLogsSummary(logs)
	pairs := []kvPair{
		{Key: "operation_id", Value: strings.TrimSpace(logs.OperationID)},
		{Key: "operation_status", Value: strings.TrimSpace(logs.OperationStatus)},
		{Key: "build_strategy", Value: strings.TrimSpace(logs.BuildStrategy)},
		{Key: "job_name", Value: firstNonEmptyTrimmed(strings.TrimSpace(logs.JobName), buildLogsFallbackJobName(logs))},
		{Key: "log_source", Value: strings.TrimSpace(logs.Source)},
		{Key: "logs_available", Value: formatOptionalBool(boolPtr(logs.Available))},
		{Key: "summary", Value: summary},
	}
	if buildMessage != "" && buildMessage != summary {
		pairs = append(pairs, kvPair{Key: "build_message", Value: buildMessage})
	}
	if logs.ArtifactSummary != nil {
		artifact := logs.ArtifactSummary
		pairs = append(pairs,
			kvPair{Key: "service", Value: strings.TrimSpace(artifact.Service)},
			kvPair{Key: "managed_image_ref", Value: strings.TrimSpace(artifact.ManagedImageRef)},
			kvPair{Key: "runtime_image_ref", Value: strings.TrimSpace(artifact.RuntimeImageRef)},
			kvPair{Key: "registry_image_status", Value: strings.TrimSpace(artifact.RegistryImageStatus)},
			kvPair{Key: "deploy_operation_id", Value: strings.TrimSpace(artifact.LinkedDeployOperationID)},
			kvPair{Key: "deploy_status", Value: strings.TrimSpace(artifact.LinkedDeployStatus)},
			kvPair{Key: "latest_pod_group", Value: strings.TrimSpace(artifact.LatestPodGroup)},
		)
	}
	if err := writeKeyValues(w, pairs...); err != nil {
		return err
	}

	if logs.ArtifactSummary != nil && len(logs.ArtifactSummary.Stages) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "stages"); err != nil {
			return err
		}
		if err := writeBuildArtifactStageTable(w, logs.ArtifactSummary.Stages); err != nil {
			return err
		}
		for _, issue := range logs.ArtifactSummary.PodIssues {
			if _, err := fmt.Fprintf(w, "pod_issue=%s\n", issue); err != nil {
				return err
			}
		}
		for _, warning := range logs.ArtifactSummary.Warnings {
			if _, err := fmt.Fprintf(w, "warning=%s\n", warning); err != nil {
				return err
			}
		}
	}

	text := strings.TrimSpace(logs.Logs)
	if text == "" {
		text = strings.TrimSpace(logs.Summary)
	}
	if text == "" {
		text = strings.TrimSpace(logs.ResultMessage)
	}
	if text == "" {
		text = "no build logs available"
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}
	_, err := fmt.Fprintln(w, text)
	return err
}

func writeBuildArtifactStageTable(w io.Writer, stages []buildArtifactStage) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "STAGE\tSTATUS\tDETAIL"); err != nil {
		return err
	}
	for _, stage := range stages {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\n", stage.Name, stage.Status, stage.Detail); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func buildArtifactStages(report *appBuildArtifactReport) []buildArtifactStage {
	if report == nil {
		return nil
	}
	stages := make([]buildArtifactStage, 0, 5)

	if status := strings.TrimSpace(report.BuildOperationStatus); status != "" {
		stages = append(stages, buildArtifactStage{
			Name:   "build",
			Status: status,
			Detail: strings.TrimSpace(report.BuildMessage),
		})
	}

	if report.ManagedImageRef != "" || report.BuildOperationID != "" {
		pushStatus := "missing"
		pushDetail := "no managed image reference was recorded"
		if report.ManagedImageRef != "" {
			pushStatus = "recorded"
			pushDetail = report.ManagedImageRef
			if normalizeImageInventoryStatus(report.RegistryImageStatus) == "available" {
				pushStatus = "confirmed"
			}
		}
		stages = append(stages, buildArtifactStage{Name: "push", Status: pushStatus, Detail: pushDetail})
	}

	if publishStatus := normalizeImageInventoryStatus(report.RegistryImageStatus); publishStatus != "" || report.ManagedImageRef != "" {
		detail := strings.TrimSpace(report.ManagedImageRef)
		if detail == "" {
			detail = strings.TrimSpace(report.RuntimeImageRef)
		}
		if publishStatus == "" {
			publishStatus = "unknown"
			detail = firstNonEmptyTrimmed(detail, "registry inventory unavailable")
		}
		if report.RegistryImageCurrent {
			detail = strings.TrimSpace(detail + " (current)")
		}
		stages = append(stages, buildArtifactStage{Name: "publish", Status: publishStatus, Detail: detail})
	}

	if report.LinkedDeployOperationID != "" || report.BuildOperationID != "" {
		deployStatus := firstNonEmptyTrimmed(report.LinkedDeployStatus, "not-found")
		detail := strings.TrimSpace(report.LinkedDeployOperationID)
		if message := strings.TrimSpace(report.LinkedDeployMessage); message != "" {
			if detail != "" {
				detail += ": "
			}
			detail += message
		}
		stages = append(stages, buildArtifactStage{Name: "deploy", Status: deployStatus, Detail: detail})
	}

	runtimeStatus, runtimeDetail := buildArtifactRuntimeStage(report)
	if runtimeStatus != "" {
		stages = append(stages, buildArtifactStage{Name: "runtime", Status: runtimeStatus, Detail: runtimeDetail})
	}

	return stages
}

func buildArtifactRuntimeStage(report *appBuildArtifactReport) (string, string) {
	if report == nil {
		return "", ""
	}
	if len(report.PodIssues) > 0 {
		return "degraded", report.PodIssues[0]
	}
	if report.ReadyPods > 0 {
		detail := fmt.Sprintf("%d/%d pods ready", report.ReadyPods, maxInt(report.LivePods, report.ReadyPods))
		if report.LatestPodGroup != "" {
			detail = report.LatestPodGroup + ": " + detail
		}
		return "ready", detail
	}
	if report.LivePods > 0 {
		detail := fmt.Sprintf("%d live pods observed", report.LivePods)
		if report.LatestPodGroup != "" {
			detail = report.LatestPodGroup + ": " + detail
		}
		return "observed", detail
	}
	if report.LatestPodGroup != "" {
		return "empty", report.LatestPodGroup + ": no live pods"
	}
	return "", ""
}

type latestPodState struct {
	Group     string
	LivePods  int
	ReadyPods int
	Issues    []string
}

func summarizeLatestPodState(podInventory *model.AppRuntimePodInventory, expectedImage string) latestPodState {
	if podInventory == nil || len(podInventory.Groups) == 0 {
		return latestPodState{}
	}
	group := podInventory.Groups[0]
	state := latestPodState{
		Group:    strings.TrimSpace(group.OwnerKind) + "/" + strings.TrimSpace(group.OwnerName),
		LivePods: len(group.Pods),
	}
	for _, pod := range group.Pods {
		if pod.Ready {
			state.ReadyPods++
		}
		issue := describePodIssue(pod, expectedImage)
		if issue != "" {
			state.Issues = appendUniqueString(state.Issues, issue)
		}
	}
	if state.LivePods == 0 {
		state.Issues = appendUniqueString(state.Issues, fmt.Sprintf("latest rollout %s has no live pods", state.Group))
	}
	return state
}

func buildLogsFallbackJobName(logs buildLogsResponse) string {
	if logs.ArtifactSummary == nil {
		return ""
	}
	return firstNonEmptyTrimmed(
		strings.TrimSpace(logs.ArtifactSummary.BuildJobName),
		strings.TrimSpace(logs.ArtifactSummary.LatestPodGroup),
		strings.TrimSpace(logs.ArtifactSummary.LinkedDeployOperationID),
	)
}

func preferredBuildLogsSummary(logs buildLogsResponse) string {
	if logs.ArtifactSummary != nil {
		artifact := logs.ArtifactSummary
		switch {
		case normalizeImageInventoryStatus(artifact.RegistryImageStatus) == "missing":
			return runtimeImageMissingSummary(artifact)
		case hasImagePullPodIssue(artifact.PodIssues):
			return runtimeImagePullFailureSummary(artifact)
		case len(artifact.PodIssues) > 0:
			return strings.TrimSpace(artifact.PodIssues[0])
		}
	}
	return firstNonEmptyTrimmed(strings.TrimSpace(logs.Summary), strings.TrimSpace(logs.ResultMessage), strings.TrimSpace(logs.ErrorMessage))
}

func describePodIssue(pod model.ClusterPod, expectedImage string) string {
	if len(pod.Containers) == 0 {
		if !pod.Ready && !strings.EqualFold(strings.TrimSpace(pod.Phase), "Running") {
			return fmt.Sprintf("pod %s phase=%s", pod.Name, strings.TrimSpace(pod.Phase))
		}
		return ""
	}
	for _, container := range pod.Containers {
		if expected := strings.TrimSpace(expectedImage); expected != "" && strings.TrimSpace(container.Image) != "" && strings.TrimSpace(container.Image) != expected {
			return fmt.Sprintf("pod %s container %s is running image %s, expected %s", pod.Name, container.Name, container.Image, expected)
		}
		if container.Ready && strings.EqualFold(strings.TrimSpace(container.State), "running") && strings.TrimSpace(container.Reason) == "" && strings.TrimSpace(container.Message) == "" {
			continue
		}
		detail := strings.TrimSpace(container.Reason)
		if message := strings.TrimSpace(container.Message); message != "" {
			if detail != "" {
				detail += ": "
			}
			detail += message
		}
		detail = firstNonEmptyTrimmed(detail, strings.TrimSpace(container.State), strings.TrimSpace(pod.Phase))
		return fmt.Sprintf("pod %s container %s %s", pod.Name, container.Name, detail)
	}
	if !pod.Ready {
		return fmt.Sprintf("pod %s is not ready", pod.Name)
	}
	return ""
}

func buildArtifactEvidence(report *appBuildArtifactReport, deployDiagnosis *model.OperationDiagnosis) []string {
	evidence := make([]string, 0, 8)
	if report != nil {
		if report.BuildOperationID != "" {
			evidence = append(evidence, fmt.Sprintf("build %s %s", report.BuildOperationID, strings.TrimSpace(report.BuildOperationStatus)))
		}
		if report.LinkedDeployOperationID != "" {
			evidence = append(evidence, fmt.Sprintf("deploy %s %s", report.LinkedDeployOperationID, strings.TrimSpace(report.LinkedDeployStatus)))
		}
		if report.ManagedImageRef != "" {
			evidence = append(evidence, fmt.Sprintf("managed image %s", report.ManagedImageRef))
		}
		if report.RuntimeImageRef != "" {
			evidence = append(evidence, fmt.Sprintf("runtime image %s", report.RuntimeImageRef))
		}
		if status := strings.TrimSpace(report.RegistryImageStatus); status != "" {
			evidence = append(evidence, fmt.Sprintf("registry image status=%s", status))
		}
		for _, issue := range report.PodIssues {
			evidence = appendUniqueString(evidence, issue)
		}
		for _, warning := range report.Warnings {
			evidence = appendUniqueString(evidence, warning)
		}
	}
	if deployDiagnosis != nil {
		evidence = appendUniqueString(evidence, fmt.Sprintf("deploy diagnosis: %s", strings.TrimSpace(deployDiagnosis.Summary)))
	}
	return evidence
}

func runtimeImageMissingSummary(report *appBuildArtifactReport) string {
	if report == nil {
		return "managed image is missing from registry inventory"
	}
	summary := fmt.Sprintf("managed image %q is missing from registry inventory", strings.TrimSpace(report.ManagedImageRef))
	if report.BuildOperationID != "" && report.LinkedDeployOperationID != "" {
		summary = fmt.Sprintf(
			"build %s queued deploy %s, but managed image %q is missing from registry inventory",
			report.BuildOperationID,
			report.LinkedDeployOperationID,
			strings.TrimSpace(report.ManagedImageRef),
		)
	}
	if len(report.PodIssues) > 0 {
		summary += "; " + report.PodIssues[0]
	}
	return summary
}

func runtimeImagePullFailureSummary(report *appBuildArtifactReport) string {
	if report == nil || len(report.PodIssues) == 0 {
		return "latest runtime pod cannot pull the expected image"
	}
	summary := report.PodIssues[0]
	if report.RuntimeImageRef != "" {
		summary = fmt.Sprintf("runtime image %q is not becoming healthy: %s", report.RuntimeImageRef, report.PodIssues[0])
	}
	return summary
}

func fallbackArtifactSummary(report *appBuildArtifactReport) string {
	if report == nil {
		return "no diagnostic signal was detected"
	}
	switch {
	case report.LinkedDeployOperationID != "" && report.LinkedDeployStatus != "":
		return fmt.Sprintf("latest deploy %s is %s", report.LinkedDeployOperationID, report.LinkedDeployStatus)
	case report.BuildOperationID != "" && report.BuildOperationStatus != "":
		return fmt.Sprintf("latest build %s is %s", report.BuildOperationID, report.BuildOperationStatus)
	case len(report.PodIssues) > 0:
		return report.PodIssues[0]
	default:
		return "app state changed, but no single blocking signal was identified"
	}
}

func buildArtifactHint(app model.App, report *appBuildArtifactReport) string {
	if report == nil || report.BuildOperationID == "" {
		return ""
	}
	return fmt.Sprintf("run fugue app logs build %s --operation %s for build/publish details", strings.TrimSpace(app.Name), report.BuildOperationID)
}

func buildArtifactServiceName(app model.App, importOp, deployOp *model.Operation) string {
	for _, op := range []*model.Operation{importOp, deployOp} {
		if op == nil || op.DesiredSource == nil {
			continue
		}
		if service := strings.TrimSpace(op.DesiredSource.ComposeService); service != "" {
			return service
		}
	}
	return strings.TrimSpace(app.Name)
}

func buildArtifactStrategy(importOp, deployOp *model.Operation) string {
	for _, op := range []*model.Operation{importOp, deployOp} {
		if op == nil || op.DesiredSource == nil {
			continue
		}
		if strategy := strings.TrimSpace(op.DesiredSource.BuildStrategy); strategy != "" {
			return strategy
		}
		if strings.TrimSpace(op.DesiredSource.Type) == model.AppSourceTypeDockerImage {
			return model.AppSourceTypeDockerImage
		}
	}
	return ""
}

func linkedDeployOperation(importOp model.Operation, operations []model.Operation) *model.Operation {
	if id := queuedDeployOperationID(importOp.ResultMessage); id != "" {
		if op := findOperationPtrByID(operations, id); op != nil {
			return op
		}
	}
	sorted := append([]model.Operation(nil), operations...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].CreatedAt.Equal(sorted[j].CreatedAt) {
			return sorted[i].ID < sorted[j].ID
		}
		return sorted[i].CreatedAt.Before(sorted[j].CreatedAt)
	})
	for i := range sorted {
		op := &sorted[i]
		if !strings.EqualFold(strings.TrimSpace(op.Type), model.OperationTypeDeploy) {
			continue
		}
		if strings.TrimSpace(op.AppID) != strings.TrimSpace(importOp.AppID) {
			continue
		}
		if op.CreatedAt.Before(importOp.CreatedAt) {
			continue
		}
		return op
	}
	return nil
}

func queuedDeployOperationID(resultMessage string) string {
	match := queuedDeployOperationPattern.FindStringSubmatch(strings.TrimSpace(resultMessage))
	if len(match) != 2 {
		return ""
	}
	return strings.TrimSpace(match[1])
}

func latestOperationOfType(operations []model.Operation, opType string) *model.Operation {
	for i := range operations {
		if strings.EqualFold(strings.TrimSpace(operations[i].Type), strings.TrimSpace(opType)) {
			return &operations[i]
		}
	}
	return nil
}

func findOperationPtrByID(operations []model.Operation, id string) *model.Operation {
	id = strings.TrimSpace(id)
	for i := range operations {
		if strings.TrimSpace(operations[i].ID) == id {
			return &operations[i]
		}
	}
	return nil
}

func matchImageVersion(images *appImageInventoryResponse, managedImageRef, runtimeImageRef string) *appImageVersion {
	if images == nil {
		return nil
	}
	managedImageRef = strings.TrimSpace(managedImageRef)
	runtimeImageRef = strings.TrimSpace(runtimeImageRef)
	for i := range images.Versions {
		version := &images.Versions[i]
		if managedImageRef != "" && strings.EqualFold(strings.TrimSpace(version.ImageRef), managedImageRef) {
			return version
		}
	}
	for i := range images.Versions {
		version := &images.Versions[i]
		if runtimeImageRef != "" && strings.EqualFold(strings.TrimSpace(version.RuntimeImageRef), runtimeImageRef) {
			return version
		}
	}
	return nil
}

func normalizeImageInventoryStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "available", "ready", "present":
		return "available"
	case "missing", "absent":
		return "missing"
	default:
		return strings.TrimSpace(status)
	}
}

func hasImagePullPodIssue(issues []string) bool {
	for _, issue := range issues {
		normalized := strings.ToLower(strings.TrimSpace(issue))
		if strings.Contains(normalized, "imagepullbackoff") || strings.Contains(normalized, "errimagepull") || strings.Contains(normalized, "pull access denied") {
			return true
		}
	}
	return false
}

func buildArtifactReportHasContent(report *appBuildArtifactReport) bool {
	if report == nil {
		return false
	}
	return report.BuildOperationID != "" ||
		report.BuildJobName != "" ||
		report.ManagedImageRef != "" ||
		report.RuntimeImageRef != "" ||
		report.LinkedDeployOperationID != "" ||
		report.RegistryImageStatus != "" ||
		report.LatestPodGroup != "" ||
		len(report.PodIssues) > 0 ||
		len(report.Warnings) > 0
}

func sourceResolvedImageRef(op *model.Operation) string {
	if op == nil || op.DesiredSource == nil {
		return ""
	}
	return strings.TrimSpace(op.DesiredSource.ResolvedImageRef)
}

func specImage(op *model.Operation) string {
	if op == nil || op.DesiredSpec == nil {
		return ""
	}
	return strings.TrimSpace(op.DesiredSpec.Image)
}

func operationID(op *model.Operation) string {
	if op == nil {
		return ""
	}
	return strings.TrimSpace(op.ID)
}

func operationStatus(op *model.Operation) string {
	if op == nil {
		return ""
	}
	return strings.TrimSpace(op.Status)
}

func operationMessage(op *model.Operation) string {
	if op == nil {
		return ""
	}
	return firstNonEmptyTrimmed(strings.TrimSpace(op.ErrorMessage), strings.TrimSpace(op.ResultMessage))
}

func isTerminalOperationStatus(status string) bool {
	switch strings.TrimSpace(status) {
	case model.OperationStatusCompleted, model.OperationStatusFailed:
		return true
	default:
		return false
	}
}

func appendUniqueString(values []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return values
	}
	for _, existing := range values {
		if strings.EqualFold(strings.TrimSpace(existing), value) {
			return values
		}
	}
	return append(values, value)
}

func formatOptionalBool(value *bool) string {
	if value == nil {
		return ""
	}
	if *value {
		return "true"
	}
	return "false"
}
