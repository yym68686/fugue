package cli

import (
	"strings"

	"fugue/internal/cli/viewmodel"
	"fugue/internal/model"
)

func buildAppHealthView(app model.App, activeOperations []model.Operation) viewmodel.AppHealthView {
	return viewmodel.NewAppHealth(app, activeOperations)
}

func buildAppOverviewHealthView(snapshot appOverviewSnapshot) viewmodel.AppHealthView {
	return viewmodel.NewAppHealth(snapshot.App, activeOperationsFromOperations(snapshot.Operations))
}

func buildAppDiagnosisEvidenceView(diagnosis appDiagnosis) viewmodel.DiagnosisEvidenceView {
	return viewmodel.DiagnosisEvidenceView{
		State:     viewmodel.ReadyState(),
		Category:  strings.TrimSpace(diagnosis.Category),
		Summary:   strings.TrimSpace(diagnosis.Summary),
		Hint:      strings.TrimSpace(diagnosis.Hint),
		Component: strings.TrimSpace(diagnosis.Component),
		Scope:     firstNonEmptyTrimmed(strings.TrimSpace(diagnosis.Namespace), strings.TrimSpace(diagnosis.Selector)),
		Tone:      viewmodel.ToneForDiagnosisCategory(diagnosis.Category),
		Evidence:  append([]string(nil), diagnosis.Evidence...),
		Warnings:  append([]string(nil), diagnosis.Warnings...),
	}
}

func buildAppOverviewDiagnosisEvidenceView(diagnosis *appOverviewDiagnosis) viewmodel.DiagnosisEvidenceView {
	if diagnosis == nil {
		return viewmodel.EmptyDiagnosisEvidence("no app overview diagnosis")
	}
	evidence := append([]string(nil), diagnosis.Evidence...)
	if diagnosis.ArtifactSummary != nil {
		if diagnosis.ArtifactSummary.BuildOperationID != "" {
			evidence = appendUniqueString(evidence, "build operation: "+diagnosis.ArtifactSummary.BuildOperationID)
		}
		if diagnosis.ArtifactSummary.LinkedDeployOperationID != "" {
			evidence = appendUniqueString(evidence, "deploy operation: "+diagnosis.ArtifactSummary.LinkedDeployOperationID)
		}
	}
	return viewmodel.DiagnosisEvidenceView{
		State:    viewmodel.ReadyState(),
		Category: strings.TrimSpace(diagnosis.Category),
		Summary:  strings.TrimSpace(diagnosis.Summary),
		Hint:     strings.TrimSpace(diagnosis.Hint),
		Tone:     viewmodel.ToneForDiagnosisCategory(diagnosis.Category),
		Evidence: evidence,
	}
}

func buildOperationTimelineView(operations []model.Operation) viewmodel.OperationTimelineView {
	return viewmodel.NewOperationTimeline(operations)
}

func buildOperationDiagnosisEvidenceView(diagnosis model.OperationDiagnosis) viewmodel.DiagnosisEvidenceView {
	return viewmodel.NewOperationDiagnosisEvidence(diagnosis)
}

func buildProjectWorkbenchView(project model.Project, apps []model.App, services []model.BackingService, operations []model.Operation) viewmodel.ProjectWorkbenchView {
	return viewmodel.NewProjectWorkbench(project, apps, services, operations)
}

func buildRuntimeCapacityView(runtime model.Runtime) viewmodel.RuntimeCapacityView {
	return viewmodel.NewRuntimeCapacity(runtime)
}

func buildActionPlanView(action, target, scope, apiCall, operationType string, destructive bool) viewmodel.ActionPlanView {
	return viewmodel.NewActionPlan(action, target, scope, apiCall, operationType, destructive)
}

func buildProjectOverviewWorkbenchView(detail consoleProjectDetailResponse, services []model.BackingService) viewmodel.ProjectWorkbenchView {
	var project model.Project
	if detail.Project != nil {
		project = *detail.Project
	}
	if strings.TrimSpace(project.ID) == "" {
		project = model.Project{
			ID:   strings.TrimSpace(detail.ProjectID),
			Name: strings.TrimSpace(detail.ProjectName),
		}
	}
	return viewmodel.NewProjectWorkbench(project, detail.Apps, services, detail.Operations)
}

func buildProjectStatusServiceStageViews(status *projectStatusResponse) []viewmodel.ServiceStageView {
	if status == nil || len(status.Services) == 0 {
		return nil
	}
	views := make([]viewmodel.ServiceStageView, 0, len(status.Services))
	for _, service := range status.Services {
		views = append(views, viewmodel.ServiceStageView{
			ID:         strings.TrimSpace(service.Service),
			Name:       strings.TrimSpace(service.Service),
			Type:       "app",
			Status:     firstNonEmptyTrimmed(service.Runtime, service.Phase),
			ProjectID:  "",
			OwnerAppID: strings.TrimSpace(service.AppID),
			RuntimeID:  "",
			Tone:       viewmodel.ToneForGenericStatus(firstNonEmptyTrimmed(service.Runtime, service.Phase, service.Category)),
		})
	}
	return views
}

func buildProjectStatusDiagnosisEvidenceViews(status *projectStatusResponse) []viewmodel.DiagnosisEvidenceView {
	if status == nil {
		return nil
	}
	views := make([]viewmodel.DiagnosisEvidenceView, 0, len(status.Services)+len(status.Deletes))
	for _, service := range status.Services {
		if strings.TrimSpace(service.Category) == "" && strings.TrimSpace(service.Summary) == "" && len(service.Warnings) == 0 {
			continue
		}
		views = append(views, viewmodel.DiagnosisEvidenceView{
			State:     viewmodel.ReadyState(),
			Category:  strings.TrimSpace(service.Category),
			Summary:   strings.TrimSpace(service.Summary),
			Component: "app",
			Scope:     firstNonEmptyTrimmed(service.Service, service.AppName, service.AppID),
			Tone:      viewmodel.ToneForDiagnosisCategory(service.Category),
			Evidence:  projectServiceEvidence(service),
			Warnings:  append([]string(nil), service.Warnings...),
		})
	}
	for _, deleteStatus := range status.Deletes {
		views = append(views, viewmodel.DiagnosisEvidenceView{
			State:     viewmodel.ReadyState(),
			Category:  "delete-" + strings.TrimSpace(deleteStatus.Status),
			Summary:   strings.TrimSpace(deleteStatus.Summary),
			Component: "app",
			Scope:     firstNonEmptyTrimmed(deleteStatus.AppName, deleteStatus.AppID),
			Tone:      viewmodel.ToneForOperationStatus(deleteStatus.Status),
			Evidence:  []string{"operation: " + strings.TrimSpace(deleteStatus.OperationID)},
		})
	}
	return views
}

func viewModelForError(err error) viewmodel.State {
	if viewmodel.IsPermissionError(err) {
		return viewmodel.PermissionState(err.Error())
	}
	return viewmodel.ErrorState(err)
}

func projectServiceEvidence(service projectServiceStatus) []string {
	evidence := make([]string, 0, 6)
	for _, item := range []string{
		"build: " + strings.TrimSpace(service.Build),
		"push: " + strings.TrimSpace(service.Push),
		"publish: " + strings.TrimSpace(service.Publish),
		"deploy: " + strings.TrimSpace(service.Deploy),
		"runtime: " + strings.TrimSpace(service.Runtime),
	} {
		if !strings.HasSuffix(item, ": ") {
			evidence = append(evidence, item)
		}
	}
	if strings.TrimSpace(service.BuildOperationID) != "" {
		evidence = append(evidence, "build operation: "+strings.TrimSpace(service.BuildOperationID))
	}
	if strings.TrimSpace(service.DeployOperationID) != "" {
		evidence = append(evidence, "deploy operation: "+strings.TrimSpace(service.DeployOperationID))
	}
	return evidence
}

func activeOperationsFromOperations(operations []model.Operation) []model.Operation {
	out := make([]model.Operation, 0)
	for _, operation := range operations {
		switch strings.ToLower(strings.TrimSpace(operation.Status)) {
		case model.OperationStatusPending, model.OperationStatusRunning:
			out = append(out, operation)
		}
	}
	return out
}
