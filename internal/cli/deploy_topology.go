package cli

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"

	"fugue/internal/model"
)

func uploadInspectionHasTopology(response inspectUploadTemplateResponse) bool {
	return response.ComposeStack != nil || response.FugueManifest != nil
}

func githubInspectionHasTopology(response inspectGitHubTemplateResponse) bool {
	return response.ComposeStack != nil || response.FugueManifest != nil
}

func (c *CLI) maybeWarnTopologyCreateModeForUpload(client *Client, request importUploadRequest, archiveName string, archiveBytes []byte) error {
	inspection, err := client.InspectUploadTemplate(importUploadRequest{Name: request.Name}, archiveName, archiveBytes)
	if err != nil {
		if isOptionalTopologyPreflightError(err) {
			return nil
		}
		return err
	}
	if !uploadInspectionHasTopology(inspection) || request.UpdateExisting || request.DryRun {
		return nil
	}
	preview := request
	preview.DryRun = true
	response, err := client.ImportUpload(preview, archiveName, archiveBytes)
	if err != nil {
		if isOptionalTopologyPreflightError(err) {
			return nil
		}
		return err
	}
	emitTopologyPlanWarnings(c, response.Plan)
	return nil
}

func (c *CLI) maybeWarnTopologyCreateModeForGitHub(client *Client, request importGitHubRequest) error {
	inspection, err := client.InspectGitHubTemplate(inspectGitHubTemplateRequest{
		RepoURL:        request.RepoURL,
		RepoVisibility: request.RepoVisibility,
		RepoAuthToken:  request.RepoAuthToken,
		Branch:         request.Branch,
	})
	if err != nil {
		if isOptionalTopologyPreflightError(err) {
			return nil
		}
		return err
	}
	if !githubInspectionHasTopology(inspection) || request.UpdateExisting || request.DryRun {
		return nil
	}
	preview := request
	preview.DryRun = true
	response, err := client.ImportGitHub(preview)
	if err != nil {
		if isOptionalTopologyPreflightError(err) {
			return nil
		}
		return err
	}
	emitTopologyPlanWarnings(c, response.Plan)
	return nil
}

func isOptionalTopologyPreflightError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(message, "status=404") ||
		strings.Contains(message, "status=405") ||
		strings.Contains(message, "status=501") ||
		strings.Contains(message, "not found")
}

func emitTopologyPlanWarnings(c *CLI, plan *model.TopologyDeployPlan) {
	if c == nil || plan == nil {
		return
	}
	for _, warning := range plan.Warnings {
		if strings.TrimSpace(warning) == "" {
			continue
		}
		c.progressf("warning=%s", warning)
	}
}

func renderTopologyDeployPlan(w io.Writer, plan *model.TopologyDeployPlan) error {
	if plan == nil {
		return nil
	}
	pairs := []kvPair{
		{Key: "mode", Value: strings.TrimSpace(plan.Mode)},
		{Key: "project_id", Value: strings.TrimSpace(plan.ProjectID)},
		{Key: "project", Value: strings.TrimSpace(plan.ProjectName)},
		{Key: "primary_service", Value: strings.TrimSpace(plan.PrimaryService)},
		{Key: "service_count", Value: fmt.Sprintf("%d", len(plan.Services))},
		{Key: "delete_missing", Value: fmt.Sprintf("%t", plan.DeleteMissing)},
		{Key: "delete_candidate_count", Value: fmt.Sprintf("%d", len(plan.DeleteCandidates))},
		{Key: "dry_run", Value: fmt.Sprintf("%t", plan.DryRun)},
	}
	if err := writeKeyValues(w, pairs...); err != nil {
		return err
	}
	if len(plan.Services) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "[services]"); err != nil {
			return err
		}
		if err := writeTopologyDeployPlanTable(w, plan.Services); err != nil {
			return err
		}
	}
	for _, warning := range plan.Warnings {
		if strings.TrimSpace(warning) == "" {
			continue
		}
		if _, err := fmt.Fprintf(w, "warning=%s\n", warning); err != nil {
			return err
		}
	}
	if len(plan.DeleteCandidates) > 0 {
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "[delete_candidates]"); err != nil {
			return err
		}
		if err := writeTopologyDeleteCandidateTable(w, plan.DeleteCandidates); err != nil {
			return err
		}
	}
	return nil
}

func writeTopologyDeployPlanTable(w io.Writer, services []model.TopologyDeployPlanService) error {
	sorted := append([]model.TopologyDeployPlanService(nil), services...)
	sort.Slice(sorted, func(i, j int) bool {
		return strings.Compare(sorted[i].Service, sorted[j].Service) < 0
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "SERVICE\tACTION\tAPP\tBUILD\tROUTE"); err != nil {
		return err
	}
	for _, service := range sorted {
		appName := firstNonEmpty(strings.TrimSpace(service.AppName), strings.TrimSpace(service.ExistingAppName))
		route := firstNonEmpty(strings.TrimSpace(service.PublicURL), strings.TrimSpace(service.Hostname))
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\n",
			strings.TrimSpace(service.Service),
			strings.TrimSpace(service.Action),
			appName,
			strings.TrimSpace(service.BuildStrategy),
			route,
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeTopologyDeleteCandidateTable(w io.Writer, targets []model.TopologyDeployDeleteTarget) error {
	sorted := append([]model.TopologyDeployDeleteTarget(nil), targets...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].AppName == sorted[j].AppName {
			return sorted[i].AppID < sorted[j].AppID
		}
		return sorted[i].AppName < sorted[j].AppName
	})
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "APP\tSERVICE\tREASON"); err != nil {
		return err
	}
	for _, target := range sorted {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\n",
			firstNonEmpty(strings.TrimSpace(target.AppName), strings.TrimSpace(target.AppID)),
			strings.TrimSpace(target.Service),
			strings.TrimSpace(target.Reason),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}
