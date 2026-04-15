package api

import (
	"fmt"
	"net/http"
	"sort"
	"strings"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/store"
)

func (s *Server) handleGetOperationDiagnosis(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	op, err := s.store.GetOperation(r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if !principal.IsPlatformAdmin() && op.TenantID != principal.TenantID {
		httpx.WriteError(w, http.StatusForbidden, "operation is not visible to this tenant")
		return
	}
	diagnosis, err := s.diagnoseOperation(op)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"diagnosis": diagnosis})
}

func (s *Server) diagnoseOperation(op model.Operation) (model.OperationDiagnosis, error) {
	app, appFound, err := s.getDiagnosisApp(op.AppID)
	if err != nil {
		return model.OperationDiagnosis{}, err
	}

	switch strings.TrimSpace(op.Status) {
	case model.OperationStatusPending:
		return s.diagnosePendingOperation(op, app, appFound)
	case model.OperationStatusWaitingAgent:
		runtimeID := firstNonEmpty(strings.TrimSpace(op.AssignedRuntimeID), strings.TrimSpace(op.TargetRuntimeID))
		summary := "waiting for an external runtime agent to pick up the task"
		if runtimeID != "" {
			summary = fmt.Sprintf("waiting for external runtime agent on runtime %q to pick up the task", runtimeID)
		}
		return model.OperationDiagnosis{
			Category: "waiting-agent",
			Summary:  summary,
			Hint:     "Check the target runtime connection and agent health with fugue runtime show or fugue runtime doctor.",
			AppName:  diagnosisAppName(app, appFound),
			Service:  diagnosisComposeService(op, app, appFound),
		}, nil
	case model.OperationStatusRunning:
		summary := firstNonEmpty(strings.TrimSpace(op.ResultMessage), "operation has been claimed and is running")
		return model.OperationDiagnosis{
			Category: "running",
			Summary:  summary,
			Hint:     "Inspect the relevant build or runtime logs if this stays in progress longer than expected.",
			AppName:  diagnosisAppName(app, appFound),
			Service:  diagnosisComposeService(op, app, appFound),
		}, nil
	case model.OperationStatusCompleted:
		summary := "operation completed"
		if message := strings.TrimSpace(op.ResultMessage); message != "" {
			summary = message
		}
		return model.OperationDiagnosis{
			Category: "completed",
			Summary:  summary,
			AppName:  diagnosisAppName(app, appFound),
			Service:  diagnosisComposeService(op, app, appFound),
		}, nil
	case model.OperationStatusFailed:
		summary := "operation failed"
		if message := firstNonEmpty(strings.TrimSpace(op.ErrorMessage), strings.TrimSpace(op.ResultMessage)); message != "" {
			summary = message
		}
		return model.OperationDiagnosis{
			Category: "failed",
			Summary:  summary,
			AppName:  diagnosisAppName(app, appFound),
			Service:  diagnosisComposeService(op, app, appFound),
		}, nil
	default:
		summary := firstNonEmpty(strings.TrimSpace(op.ResultMessage), "operation status is unknown")
		return model.OperationDiagnosis{
			Category: firstNonEmpty(strings.TrimSpace(op.Status), "unknown"),
			Summary:  summary,
			AppName:  diagnosisAppName(app, appFound),
			Service:  diagnosisComposeService(op, app, appFound),
		}, nil
	}
}

func (s *Server) diagnosePendingOperation(op model.Operation, app model.App, appFound bool) (model.OperationDiagnosis, error) {
	activeOps, err := s.store.ListActiveOperations()
	if err != nil {
		return model.OperationDiagnosis{}, err
	}
	activeOps = diagnosisFilterActiveOperationsByTenant(activeOps, op.TenantID)
	activeOpsByApp := diagnosisIndexActiveOperationsByApp(activeOps)

	sameAppBlockers := diagnosisClaimTurnBlockers(op, activeOpsByApp[strings.TrimSpace(op.AppID)])
	if len(sameAppBlockers) > 0 {
		category := "app-queue"
		summary := fmt.Sprintf("waiting for older queued operations on app %q", diagnosisAppName(app, appFound))
		hint := "Show the older operation first; this app only advances one active operation at a time."
		for _, blocker := range sameAppBlockers {
			switch blocker.Status {
			case model.OperationStatusRunning, model.OperationStatusWaitingAgent:
				category = "app-serial"
				summary = fmt.Sprintf("waiting for in-flight operation on app %q to finish", diagnosisAppName(app, appFound))
				hint = "Wait for the blocking operation to finish or resolve it before retrying this deploy."
				goto blockersReady
			}
		}
	blockersReady:
		return model.OperationDiagnosis{
			Category:  category,
			Summary:   summary,
			Hint:      hint,
			AppName:   diagnosisAppName(app, appFound),
			Service:   diagnosisComposeService(op, app, appFound),
			BlockedBy: diagnosisBuildBlockers(sameAppBlockers, map[string]model.App{strings.TrimSpace(app.ID): app}),
		}, nil
	}

	diagnosis := model.OperationDiagnosis{
		Category: "queue",
		Summary:  firstNonEmpty(strings.TrimSpace(op.ResultMessage), "operation is pending"),
		Hint:     "No direct app-local blocker was detected. If this persists, check controller health and logs.",
		AppName:  diagnosisAppName(app, appFound),
		Service:  diagnosisComposeService(op, app, appFound),
	}
	if op.Type != model.OperationTypeDeploy || !appFound {
		diagnosis.Summary = "no app-local blocker was detected; the controller has not claimed this operation yet"
		return diagnosis, nil
	}

	dependencies := diagnosisComposeDependencies(op, app)
	if len(dependencies) == 0 {
		diagnosis.Summary = "no app-local or compose dependency blocker was detected; the controller has not claimed this operation yet"
		return diagnosis, nil
	}

	projectID := strings.TrimSpace(app.ProjectID)
	if projectID == "" {
		diagnosis.Summary = "no compose dependency context was available for this deploy"
		return diagnosis, nil
	}
	projectApps, err := s.store.ListAppsMetadataByProjectIDs([]string{projectID})
	if err != nil {
		return model.OperationDiagnosis{}, err
	}
	projectApps = diagnosisFilterAppsByTenant(projectApps, op.TenantID)
	appsByService := diagnosisIndexAppsByComposeService(projectApps)

	if cycle := diagnosisDetectPendingDependencyCycle(op, app, activeOpsByApp, appsByService); len(cycle) > 0 {
		return model.OperationDiagnosis{
			Category:        "compose-dependency-cycle",
			Summary:         fmt.Sprintf("circular compose dependency wait detected: %s", strings.Join(cycle, " -> ")),
			Hint:            "Break the depends_on cycle or resolve one of the blocked service deploys so the queue can make progress.",
			AppName:         diagnosisAppName(app, appFound),
			Service:         diagnosisComposeService(op, app, appFound),
			DependencyChain: cycle,
			BlockedBy:       diagnosisBuildCycleBlockers(cycle, activeOpsByApp, appsByService),
		}, nil
	}

	for _, dependency := range dependencies {
		dependency = strings.TrimSpace(dependency)
		if dependency == "" {
			continue
		}
		dependencyApp, ok := appsByService[dependency]
		if !ok {
			return model.OperationDiagnosis{
				Category: "compose-dependency-missing",
				Summary:  diagnosisMissingComposeDependencyMessage(op, app, dependency),
				Hint:     "The pending deploy references a service that is not present in the project anymore.",
				AppName:  diagnosisAppName(app, appFound),
				Service:  diagnosisComposeService(op, app, appFound),
			}, nil
		}
		dependencyBlockers := activeOpsByApp[strings.TrimSpace(dependencyApp.ID)]
		if len(dependencyBlockers) > 0 {
			return model.OperationDiagnosis{
				Category:  "compose-dependency-active",
				Summary:   fmt.Sprintf("waiting for compose dependency %q because app %q still has active operation(s)", dependency, strings.TrimSpace(dependencyApp.Name)),
				Hint:      "Inspect the blocking dependency operation first; this deploy cannot be claimed until that dependency becomes ready.",
				AppName:   diagnosisAppName(app, appFound),
				Service:   diagnosisComposeService(op, app, appFound),
				BlockedBy: diagnosisBuildBlockers(dependencyBlockers, map[string]model.App{strings.TrimSpace(dependencyApp.ID): dependencyApp}),
			}, nil
		}
		if !strings.EqualFold(strings.TrimSpace(dependencyApp.Status.Phase), "deployed") {
			return model.OperationDiagnosis{
				Category: "compose-dependency-unavailable",
				Summary:  diagnosisUnavailableComposeDependencyMessage(op, app, dependency, dependencyApp),
				Hint:     "Bring the dependency service back to a deployed state before retrying this deploy.",
				AppName:  diagnosisAppName(app, appFound),
				Service:  diagnosisComposeService(op, app, appFound),
			}, nil
		}
	}

	diagnosis.Summary = "no app-local or compose dependency blocker was detected; the controller has not claimed this operation yet"
	return diagnosis, nil
}

func (s *Server) getDiagnosisApp(appID string) (model.App, bool, error) {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return model.App{}, false, nil
	}
	app, err := s.store.GetAppMetadata(appID)
	if err != nil {
		if err == store.ErrNotFound {
			return model.App{}, false, nil
		}
		return model.App{}, false, err
	}
	return app, true, nil
}

func diagnosisFilterActiveOperationsByTenant(ops []model.Operation, tenantID string) []model.Operation {
	if strings.TrimSpace(tenantID) == "" {
		return append([]model.Operation(nil), ops...)
	}
	filtered := make([]model.Operation, 0, len(ops))
	for _, op := range ops {
		if strings.TrimSpace(op.TenantID) != strings.TrimSpace(tenantID) {
			continue
		}
		filtered = append(filtered, op)
	}
	return filtered
}

func diagnosisFilterAppsByTenant(apps []model.App, tenantID string) []model.App {
	if strings.TrimSpace(tenantID) == "" {
		return append([]model.App(nil), apps...)
	}
	filtered := make([]model.App, 0, len(apps))
	for _, app := range apps {
		if strings.TrimSpace(app.TenantID) != strings.TrimSpace(tenantID) {
			continue
		}
		filtered = append(filtered, app)
	}
	return filtered
}

func diagnosisIndexActiveOperationsByApp(ops []model.Operation) map[string][]model.Operation {
	grouped := make(map[string][]model.Operation)
	for _, op := range ops {
		appID := strings.TrimSpace(op.AppID)
		if appID == "" {
			continue
		}
		grouped[appID] = append(grouped[appID], op)
	}
	for appID := range grouped {
		sort.Slice(grouped[appID], func(i, j int) bool {
			return diagnosisOperationCreatedBefore(grouped[appID][i], grouped[appID][j])
		})
	}
	return grouped
}

func diagnosisIndexAppsByComposeService(apps []model.App) map[string]model.App {
	indexed := make(map[string]model.App)
	for _, app := range apps {
		service := diagnosisComposeService(model.Operation{}, app, true)
		if service == "" {
			continue
		}
		indexed[service] = app
	}
	return indexed
}

func diagnosisClaimTurnBlockers(op model.Operation, appOps []model.Operation) []model.Operation {
	blockers := make([]model.Operation, 0, len(appOps))
	for _, other := range appOps {
		if other.ID == op.ID {
			continue
		}
		switch other.Status {
		case model.OperationStatusRunning, model.OperationStatusWaitingAgent:
			blockers = append(blockers, other)
		case model.OperationStatusPending:
			if diagnosisOperationCreatedBefore(other, op) {
				blockers = append(blockers, other)
			}
		}
	}
	return blockers
}

func diagnosisOperationCreatedBefore(left, right model.Operation) bool {
	if left.CreatedAt.Equal(right.CreatedAt) {
		return left.ID < right.ID
	}
	return left.CreatedAt.Before(right.CreatedAt)
}

func diagnosisComposeDependencies(op model.Operation, app model.App) []string {
	if op.DesiredSource != nil && len(op.DesiredSource.ComposeDependsOn) > 0 {
		return append([]string(nil), op.DesiredSource.ComposeDependsOn...)
	}
	if app.Source != nil && len(app.Source.ComposeDependsOn) > 0 {
		return append([]string(nil), app.Source.ComposeDependsOn...)
	}
	return nil
}

func diagnosisComposeService(op model.Operation, app model.App, appFound bool) string {
	if op.DesiredSource != nil {
		if service := model.SlugifyOptional(strings.TrimSpace(op.DesiredSource.ComposeService)); service != "" {
			return service
		}
	}
	if appFound && app.Source != nil {
		if service := model.SlugifyOptional(strings.TrimSpace(app.Source.ComposeService)); service != "" {
			return service
		}
	}
	return ""
}

func diagnosisAppName(app model.App, appFound bool) string {
	if !appFound {
		return ""
	}
	return strings.TrimSpace(app.Name)
}

func diagnosisBuildBlockers(ops []model.Operation, appsByID map[string]model.App) []model.OperationDiagnosisBlocker {
	if len(ops) == 0 {
		return nil
	}
	blockers := make([]model.OperationDiagnosisBlocker, 0, len(ops))
	for _, op := range ops {
		app := appsByID[strings.TrimSpace(op.AppID)]
		blockers = append(blockers, model.OperationDiagnosisBlocker{
			OperationID: strings.TrimSpace(op.ID),
			AppID:       strings.TrimSpace(op.AppID),
			AppName:     strings.TrimSpace(app.Name),
			Service:     diagnosisComposeService(op, app, app.ID != ""),
			Type:        strings.TrimSpace(op.Type),
			Status:      strings.TrimSpace(op.Status),
		})
	}
	return blockers
}

func diagnosisBuildCycleBlockers(cycle []string, activeOpsByApp map[string][]model.Operation, appsByService map[string]model.App) []model.OperationDiagnosisBlocker {
	if len(cycle) == 0 {
		return nil
	}
	blockers := make([]model.OperationDiagnosisBlocker, 0, len(cycle))
	seen := map[string]struct{}{}
	for _, service := range cycle {
		service = strings.TrimSpace(service)
		if service == "" {
			continue
		}
		app, ok := appsByService[service]
		if !ok {
			continue
		}
		active := activeOpsByApp[strings.TrimSpace(app.ID)]
		if len(active) == 0 {
			continue
		}
		op := active[0]
		if _, ok := seen[op.ID]; ok {
			continue
		}
		seen[op.ID] = struct{}{}
		blockers = append(blockers, model.OperationDiagnosisBlocker{
			OperationID: strings.TrimSpace(op.ID),
			AppID:       strings.TrimSpace(app.ID),
			AppName:     strings.TrimSpace(app.Name),
			Service:     service,
			Type:        strings.TrimSpace(op.Type),
			Status:      strings.TrimSpace(op.Status),
		})
	}
	return blockers
}

func diagnosisMissingComposeDependencyMessage(op model.Operation, app model.App, dependency string) string {
	service := diagnosisComposeService(op, app, app.ID != "")
	if service == "" {
		return fmt.Sprintf("compose dependency %q is missing from the project", dependency)
	}
	return fmt.Sprintf("compose dependency %q for service %q is missing from the project", dependency, service)
}

func diagnosisUnavailableComposeDependencyMessage(op model.Operation, app model.App, dependency string, dependencyApp model.App) string {
	service := diagnosisComposeService(op, app, app.ID != "")
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

func diagnosisDetectPendingDependencyCycle(
	op model.Operation,
	app model.App,
	activeOpsByApp map[string][]model.Operation,
	appsByService map[string]model.App,
) []string {
	startService := diagnosisComposeService(op, app, app.ID != "")
	if startService == "" {
		return nil
	}

	candidateApps := make(map[string]diagnosisPendingService, len(appsByService))
	for service, candidateApp := range appsByService {
		active := activeOpsByApp[strings.TrimSpace(candidateApp.ID)]
		if len(active) == 0 {
			continue
		}
		candidateOp, ok := diagnosisPendingDeployCandidate(active)
		if !ok {
			continue
		}
		candidateApps[service] = diagnosisPendingService{
			App: candidateApp,
			Op:  candidateOp,
		}
	}
	if _, ok := candidateApps[startService]; !ok {
		candidateApps[startService] = diagnosisPendingService{App: app, Op: op}
	}

	var (
		path    []string
		onStack = map[string]int{}
	)
	var visit func(service string) []string
	visit = func(service string) []string {
		candidate, ok := candidateApps[service]
		if !ok {
			return nil
		}
		onStack[service] = len(path)
		path = append(path, service)
		for _, dependency := range diagnosisComposeDependencies(candidate.Op, candidate.App) {
			dependency = strings.TrimSpace(dependency)
			if dependency == "" {
				continue
			}
			if _, ok := candidateApps[dependency]; !ok {
				continue
			}
			if index, ok := onStack[dependency]; ok {
				cycle := append([]string(nil), path[index:]...)
				cycle = append(cycle, dependency)
				return cycle
			}
			if cycle := visit(dependency); len(cycle) > 0 {
				return cycle
			}
		}
		delete(onStack, service)
		path = path[:len(path)-1]
		return nil
	}

	return visit(startService)
}

type diagnosisPendingService struct {
	App model.App
	Op  model.Operation
}

func diagnosisPendingDeployCandidate(appOps []model.Operation) (model.Operation, bool) {
	if len(appOps) == 0 {
		return model.Operation{}, false
	}
	first := appOps[0]
	if first.Status != model.OperationStatusPending || first.Type != model.OperationTypeDeploy {
		return model.Operation{}, false
	}
	return first, true
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			continue
		}
		return strings.TrimSpace(value)
	}
	return ""
}
