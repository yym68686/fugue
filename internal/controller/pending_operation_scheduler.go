package controller

import (
	"fmt"
	"strings"

	"fugue/internal/model"
)

func pendingOperationMatchesLane(op model.Operation, lane operationLane) bool {
	requestedByGitHubSync := strings.TrimSpace(op.RequestedByID) == model.OperationRequestedByGitHubSyncController
	switch lane {
	case operationLaneForegroundImport:
		return op.Type == model.OperationTypeImport && !requestedByGitHubSync
	case operationLaneForegroundActivate:
		return op.Type != model.OperationTypeImport && !requestedByGitHubSync
	case operationLaneGitHubSync:
		return requestedByGitHubSync
	default:
		return false
	}
}

func indexAppsForPendingOperations(apps []model.App) (map[string]model.App, map[string]map[string]model.App) {
	appsByID := make(map[string]model.App, len(apps))
	composeAppsByProject := make(map[string]map[string]model.App)
	for _, app := range apps {
		appsByID[app.ID] = app
		if app.Source == nil {
			continue
		}
		projectID := strings.TrimSpace(app.ProjectID)
		composeService := strings.TrimSpace(app.Source.ComposeService)
		if projectID == "" || composeService == "" {
			continue
		}
		projectApps := composeAppsByProject[projectID]
		if projectApps == nil {
			projectApps = make(map[string]model.App)
			composeAppsByProject[projectID] = projectApps
		}
		projectApps[composeService] = app
	}
	return appsByID, composeAppsByProject
}

func indexActiveOperationsByApp(ops []model.Operation) map[string][]model.Operation {
	grouped := make(map[string][]model.Operation)
	for _, op := range ops {
		grouped[op.AppID] = append(grouped[op.AppID], op)
	}
	return grouped
}

func pendingOperationReadyForClaim(
	op model.Operation,
	app model.App,
	hasApp bool,
	activeOpsByApp map[string][]model.Operation,
	composeAppsByProject map[string]map[string]model.App,
) (bool, string) {
	if !pendingOperationHasClaimTurn(op, activeOpsByApp[op.AppID]) {
		return false, ""
	}
	if op.Type != model.OperationTypeDeploy || !hasApp {
		return true, ""
	}

	dependencies := composeDependenciesForOperation(op, app)
	if len(dependencies) == 0 {
		return true, ""
	}
	projectApps := composeAppsByProject[strings.TrimSpace(app.ProjectID)]
	if len(projectApps) == 0 {
		return false, missingComposeDependencyMessage(op, app, strings.TrimSpace(dependencies[0]))
	}
	for _, dependency := range dependencies {
		dependency = strings.TrimSpace(dependency)
		if dependency == "" {
			continue
		}
		dependencyApp, ok := projectApps[dependency]
		if !ok {
			return false, missingComposeDependencyMessage(op, app, dependency)
		}
		if dependencyApp.ID == app.ID {
			continue
		}
		if len(activeOpsByApp[dependencyApp.ID]) > 0 {
			return false, ""
		}
		if !strings.EqualFold(strings.TrimSpace(dependencyApp.Status.Phase), "deployed") {
			return false, unavailableComposeDependencyMessage(op, app, dependency, dependencyApp)
		}
	}
	return true, ""
}

func pendingOperationHasClaimTurn(op model.Operation, appOps []model.Operation) bool {
	for _, other := range appOps {
		if other.ID == op.ID {
			continue
		}
		if other.Status == model.OperationStatusRunning || other.Status == model.OperationStatusWaitingAgent {
			return false
		}
		if other.Status == model.OperationStatusPending && operationCreatedBefore(other, op) {
			return false
		}
	}
	return true
}

func composeDependenciesForOperation(op model.Operation, app model.App) []string {
	if op.DesiredSource != nil && len(op.DesiredSource.ComposeDependsOn) > 0 {
		return op.DesiredSource.ComposeDependsOn
	}
	if app.Source != nil && len(app.Source.ComposeDependsOn) > 0 {
		return app.Source.ComposeDependsOn
	}
	return nil
}

func operationCreatedBefore(left, right model.Operation) bool {
	if left.CreatedAt.Equal(right.CreatedAt) {
		return left.ID < right.ID
	}
	return left.CreatedAt.Before(right.CreatedAt)
}

func composeServiceNameForOperation(op model.Operation, app model.App) string {
	if op.DesiredSource != nil {
		if service := strings.TrimSpace(op.DesiredSource.ComposeService); service != "" {
			return service
		}
	}
	if app.Source != nil {
		if service := strings.TrimSpace(app.Source.ComposeService); service != "" {
			return service
		}
	}
	return strings.TrimSpace(app.Name)
}

func missingComposeDependencyMessage(op model.Operation, app model.App, dependency string) string {
	service := composeServiceNameForOperation(op, app)
	if service == "" {
		return fmt.Sprintf("compose dependency %q is missing from the project", dependency)
	}
	return fmt.Sprintf("compose dependency %q for service %q is missing from the project", dependency, service)
}

func unavailableComposeDependencyMessage(op model.Operation, app model.App, dependency string, dependencyApp model.App) string {
	service := composeServiceNameForOperation(op, app)
	phase := strings.TrimSpace(dependencyApp.Status.Phase)
	if phase == "" {
		phase = "unknown"
	}
	message := strings.TrimSpace(dependencyApp.Status.LastMessage)

	base := fmt.Sprintf("compose dependency %q", dependency)
	if service != "" {
		base = fmt.Sprintf("%s for service %q", base, service)
	}
	base = fmt.Sprintf("%s cannot proceed because app %q is %q", base, strings.TrimSpace(dependencyApp.Name), phase)
	if message == "" {
		return base
	}
	return fmt.Sprintf("%s: %s", base, message)
}
