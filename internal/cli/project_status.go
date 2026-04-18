package cli

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"

	"fugue/internal/model"
)

type projectServiceStatus struct {
	Service           string   `json:"service"`
	AppID             string   `json:"app_id,omitempty"`
	AppName           string   `json:"app_name,omitempty"`
	Phase             string   `json:"phase,omitempty"`
	PublicURL         string   `json:"public_url,omitempty"`
	Category          string   `json:"category,omitempty"`
	Summary           string   `json:"summary,omitempty"`
	Build             string   `json:"build,omitempty"`
	Push              string   `json:"push,omitempty"`
	Publish           string   `json:"publish,omitempty"`
	Deploy            string   `json:"deploy,omitempty"`
	Runtime           string   `json:"runtime,omitempty"`
	BuildOperationID  string   `json:"build_operation_id,omitempty"`
	DeployOperationID string   `json:"deploy_operation_id,omitempty"`
	Warnings          []string `json:"warnings,omitempty"`
}

type projectDeleteStatus struct {
	AppID       string `json:"app_id,omitempty"`
	AppName     string `json:"app_name,omitempty"`
	OperationID string `json:"operation_id,omitempty"`
	Status      string `json:"status,omitempty"`
	Summary     string `json:"summary,omitempty"`
}

type projectStatusResponse struct {
	Services []projectServiceStatus `json:"services,omitempty"`
	Deletes  []projectDeleteStatus  `json:"deletes,omitempty"`
	Warnings []string               `json:"warnings,omitempty"`
}

type projectStatusFilters struct {
	AppIDs       map[string]struct{}
	DeleteAppIDs map[string]struct{}
	Services     map[string]struct{}
}

func (c *CLI) loadProjectStatus(client *Client, detail consoleProjectDetailResponse) (*projectStatusResponse, error) {
	return c.loadProjectStatusFiltered(client, detail, projectStatusFilters{})
}

func (c *CLI) loadProjectStatusFiltered(client *Client, detail consoleProjectDetailResponse, filters projectStatusFilters) (*projectStatusResponse, error) {
	return c.buildProjectStatusFromAppsAndOperations(client, detail.Apps, detail.Operations, filters)
}

func (c *CLI) buildProjectStatusFromAppsAndOperations(client *Client, apps []model.App, operations []model.Operation, filters projectStatusFilters) (*projectStatusResponse, error) {
	apps = append([]model.App(nil), apps...)
	sort.Slice(apps, func(i, j int) bool {
		left := projectServiceLabel(apps[i])
		right := projectServiceLabel(apps[j])
		if left == right {
			return strings.TrimSpace(apps[i].Name) < strings.TrimSpace(apps[j].Name)
		}
		return left < right
	})

	opsByApp := make(map[string][]model.Operation, len(operations))
	for _, op := range operations {
		appID := strings.TrimSpace(op.AppID)
		if appID == "" {
			continue
		}
		opsByApp[appID] = append(opsByApp[appID], op)
	}

	status := &projectStatusResponse{}
	currentApps := make(map[string]model.App, len(apps))
	for _, app := range apps {
		currentApps[strings.TrimSpace(app.ID)] = app
		if !shouldIncludeProjectStatusApp(app, filters) {
			continue
		}

		appOps := append([]model.Operation(nil), opsByApp[strings.TrimSpace(app.ID)]...)
		sortOperationsNewestFirst(appOps)

		var (
			images       *appImageInventoryResponse
			podInventory *model.AppRuntimePodInventory
			appWarnings  []string
		)

		if projectStatusNeedsArtifactInventory(app, appOps) {
			if inventory, err := client.GetAppImages(app.ID); err == nil {
				images = &inventory
			} else {
				appWarnings = appendUniqueString(appWarnings, fmt.Sprintf("image inventory unavailable: %v", err))
			}
			if inventory, err := client.GetAppRuntimePods(app.ID, "app"); err == nil {
				podInventory = &inventory
			} else {
				appWarnings = appendUniqueString(appWarnings, fmt.Sprintf("runtime pod inventory unavailable: %v", err))
			}
		}

		report, latestImport, latestDeploy := summarizeAppBuildArtifact(app, appOps, images, podInventory)
		if report != nil {
			if projectStatusNeedsExtendedBuildEvidence(report) {
				c.enrichBuildLogsArtifactReport(client, app.ID, report)
			}
			for _, warning := range appWarnings {
				report.Warnings = appendUniqueString(report.Warnings, warning)
			}
		} else if len(appWarnings) > 0 {
			report = &appBuildArtifactReport{Warnings: append([]string(nil), appWarnings...)}
		}

		diagnosis := summarizeAppOverviewDiagnosis(app, latestImport, latestDeploy, nil, report)
		status.Services = append(status.Services, buildProjectServiceStatus(app, report, diagnosis))
	}

	deleteStatuses := make([]projectDeleteStatus, 0)
	for _, op := range operations {
		if !strings.EqualFold(strings.TrimSpace(op.Type), model.OperationTypeDelete) {
			continue
		}
		if !shouldIncludeProjectDeleteStatus(op, filters) {
			continue
		}
		appID := strings.TrimSpace(op.AppID)
		appName := strings.TrimSpace(op.AppID)
		if current, ok := currentApps[appID]; ok && strings.TrimSpace(current.Name) != "" {
			appName = strings.TrimSpace(current.Name)
		}
		deleteStatuses = append(deleteStatuses, projectDeleteStatus{
			AppID:       appID,
			AppName:     appName,
			OperationID: strings.TrimSpace(op.ID),
			Status:      strings.TrimSpace(op.Status),
			Summary:     projectDeleteSummary(op, currentApps[appID]),
		})
	}
	sort.Slice(deleteStatuses, func(i, j int) bool {
		if deleteStatuses[i].AppName == deleteStatuses[j].AppName {
			return deleteStatuses[i].OperationID < deleteStatuses[j].OperationID
		}
		return deleteStatuses[i].AppName < deleteStatuses[j].AppName
	})
	status.Deletes = deleteStatuses
	return status, nil
}

func buildProjectServiceStatus(app model.App, report *appBuildArtifactReport, diagnosis *appOverviewDiagnosis) projectServiceStatus {
	service := projectServiceLabel(app)
	if report != nil && strings.TrimSpace(report.Service) != "" {
		service = firstNonEmptyTrimmed(strings.TrimSpace(report.Service), service)
	}

	summary := strings.TrimSpace(app.Status.Phase)
	if diagnosis != nil && strings.TrimSpace(diagnosis.Summary) != "" {
		summary = strings.TrimSpace(diagnosis.Summary)
	} else if report != nil {
		summary = fallbackArtifactSummary(report)
	}
	if summary == "" {
		summary = "no live service signal was observed"
	}

	status := projectServiceStatus{
		Service:   service,
		AppID:     strings.TrimSpace(app.ID),
		AppName:   strings.TrimSpace(app.Name),
		Phase:     strings.TrimSpace(app.Status.Phase),
		Category:  strings.TrimSpace(firstNonEmptyTrimmed(categoryFromDiagnosis(diagnosis), statusCategoryFromReport(report))),
		Summary:   summary,
		Runtime:   strings.TrimSpace(firstNonEmptyTrimmed(projectStageStatus(report, "runtime"), strings.TrimSpace(app.Status.Phase))),
		Warnings:  nil,
		Build:     strings.TrimSpace(projectStageStatus(report, "build")),
		Push:      strings.TrimSpace(projectStageStatus(report, "push")),
		Publish:   strings.TrimSpace(projectStageStatus(report, "publish")),
		Deploy:    strings.TrimSpace(projectStageStatus(report, "deploy")),
		PublicURL: "",
	}
	if app.Route != nil {
		status.PublicURL = strings.TrimSpace(app.Route.PublicURL)
	}
	if report != nil {
		status.BuildOperationID = strings.TrimSpace(report.BuildOperationID)
		status.DeployOperationID = strings.TrimSpace(report.LinkedDeployOperationID)
		status.Warnings = append([]string(nil), report.Warnings...)
	}
	return status
}

func renderProjectStatus(w io.Writer, status *projectStatusResponse) error {
	if status == nil {
		return nil
	}
	if len(status.Services) > 0 {
		if _, err := fmt.Fprintln(w, "[service_status]"); err != nil {
			return err
		}
		if err := writeProjectServiceStatusTable(w, status.Services); err != nil {
			return err
		}
	}
	if len(status.Deletes) > 0 {
		if len(status.Services) > 0 {
			if _, err := fmt.Fprintln(w); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintln(w, "[delete_operations]"); err != nil {
			return err
		}
		if err := writeProjectDeleteStatusTable(w, status.Deletes); err != nil {
			return err
		}
	}
	for _, warning := range status.Warnings {
		if strings.TrimSpace(warning) == "" {
			continue
		}
		if _, err := fmt.Fprintf(w, "warning=%s\n", warning); err != nil {
			return err
		}
	}
	return nil
}

func writeProjectServiceStatusTable(w io.Writer, services []projectServiceStatus) error {
	sorted := append([]projectServiceStatus(nil), services...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Service == sorted[j].Service {
			return sorted[i].AppName < sorted[j].AppName
		}
		return sorted[i].Service < sorted[j].Service
	})

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "SERVICE\tAPP\tBUILD\tPUSH\tPUBLISH\tDEPLOY\tRUNTIME\tURL\tSUMMARY"); err != nil {
		return err
	}
	for _, service := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			displayProjectStatusValue(service.Service),
			displayProjectStatusValue(firstNonEmptyTrimmed(service.AppName, service.AppID)),
			displayProjectStatusValue(service.Build),
			displayProjectStatusValue(service.Push),
			displayProjectStatusValue(service.Publish),
			displayProjectStatusValue(service.Deploy),
			displayProjectStatusValue(service.Runtime),
			displayProjectStatusValue(service.PublicURL),
			displayProjectStatusValue(service.Summary),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeProjectDeleteStatusTable(w io.Writer, deletes []projectDeleteStatus) error {
	sorted := append([]projectDeleteStatus(nil), deletes...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].AppName == sorted[j].AppName {
			return sorted[i].OperationID < sorted[j].OperationID
		}
		return sorted[i].AppName < sorted[j].AppName
	})

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "APP\tOPERATION\tSTATUS\tSUMMARY"); err != nil {
		return err
	}
	for _, item := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\n",
			displayProjectStatusValue(firstNonEmptyTrimmed(item.AppName, item.AppID)),
			displayProjectStatusValue(item.OperationID),
			displayProjectStatusValue(item.Status),
			displayProjectStatusValue(item.Summary),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func shouldIncludeProjectStatusApp(app model.App, filters projectStatusFilters) bool {
	if len(filters.AppIDs) == 0 && len(filters.Services) == 0 {
		return true
	}
	if _, ok := filters.AppIDs[strings.TrimSpace(app.ID)]; ok {
		return true
	}
	service := projectServiceLabel(app)
	_, ok := filters.Services[service]
	return ok
}

func shouldIncludeProjectDeleteStatus(op model.Operation, filters projectStatusFilters) bool {
	if len(filters.DeleteAppIDs) == 0 && len(filters.AppIDs) == 0 {
		return true
	}
	appID := strings.TrimSpace(op.AppID)
	if _, ok := filters.DeleteAppIDs[appID]; ok {
		return true
	}
	_, ok := filters.AppIDs[appID]
	return ok
}

func projectDeleteSummary(op model.Operation, app model.App) string {
	if message := strings.TrimSpace(operationMessage(&op)); message != "" {
		return message
	}
	if strings.TrimSpace(app.ID) != "" && strings.TrimSpace(app.Status.Phase) != "" {
		return fmt.Sprintf("app phase=%s", strings.TrimSpace(app.Status.Phase))
	}
	switch strings.TrimSpace(op.Status) {
	case model.OperationStatusCompleted:
		return "delete completed"
	case model.OperationStatusFailed:
		return "delete failed"
	default:
		return "delete requested"
	}
}

func projectServiceLabel(app model.App) string {
	if app.Source != nil && strings.TrimSpace(app.Source.ComposeService) != "" {
		return strings.TrimSpace(app.Source.ComposeService)
	}
	return strings.TrimSpace(app.Name)
}

func projectStageStatus(report *appBuildArtifactReport, stageName string) string {
	if report == nil {
		return ""
	}
	stageName = strings.TrimSpace(stageName)
	for _, stage := range report.Stages {
		if !strings.EqualFold(strings.TrimSpace(stage.Name), stageName) {
			continue
		}
		return strings.TrimSpace(stage.Status)
	}
	return ""
}

func categoryFromDiagnosis(diagnosis *appOverviewDiagnosis) string {
	if diagnosis == nil {
		return ""
	}
	return strings.TrimSpace(diagnosis.Category)
}

func statusCategoryFromReport(report *appBuildArtifactReport) string {
	if report == nil {
		return ""
	}
	if normalizeImageInventoryStatus(report.RegistryImageStatus) == "missing" {
		return "runtime-image-missing"
	}
	if hasImagePullPodIssue(report.PodIssues) {
		return "runtime-image-pull-failed"
	}
	return ""
}

func displayProjectStatusValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "-"
	}
	return value
}

func topologyPlanStatusFilters(plan *model.TopologyDeployPlan, apps []model.App) projectStatusFilters {
	filters := projectStatusFilters{
		AppIDs:       map[string]struct{}{},
		DeleteAppIDs: map[string]struct{}{},
		Services:     map[string]struct{}{},
	}
	for _, service := range plan.Services {
		if value := strings.TrimSpace(service.Service); value != "" {
			filters.Services[value] = struct{}{}
		}
		if value := strings.TrimSpace(service.AppID); value != "" {
			filters.AppIDs[value] = struct{}{}
		}
		if value := strings.TrimSpace(service.ExistingAppID); value != "" {
			filters.AppIDs[value] = struct{}{}
		}
	}
	for _, app := range apps {
		if value := strings.TrimSpace(app.ID); value != "" {
			filters.AppIDs[value] = struct{}{}
		}
		if service := strings.TrimSpace(projectServiceLabel(app)); service != "" {
			filters.Services[service] = struct{}{}
		}
	}
	for _, item := range plan.DeleteCandidates {
		if value := strings.TrimSpace(item.AppID); value != "" {
			filters.DeleteAppIDs[value] = struct{}{}
		}
	}
	return filters
}

func projectStatusNeedsArtifactInventory(app model.App, operations []model.Operation) bool {
	phase := strings.ToLower(strings.TrimSpace(app.Status.Phase))
	switch phase {
	case "", "ready", "available":
	default:
		return true
	}
	if latestImport := latestOperationOfType(operations, model.OperationTypeImport); latestImport != nil {
		return true
	}
	if latestDeploy := latestOperationOfType(operations, model.OperationTypeDeploy); latestDeploy != nil &&
		!strings.EqualFold(strings.TrimSpace(latestDeploy.Status), model.OperationStatusCompleted) {
		return true
	}
	return false
}

func projectStatusNeedsExtendedBuildEvidence(report *appBuildArtifactReport) bool {
	if report == nil {
		return false
	}
	if strings.TrimSpace(report.BuildOperationID) != "" {
		return true
	}
	if normalizeImageInventoryStatus(report.RegistryImageStatus) == "missing" {
		return true
	}
	return len(report.PodIssues) > 0
}
