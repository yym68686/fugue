package store

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	ProjectMoveConflictFail        = "fail"
	ProjectMoveConflictRename      = "rename"
	ProjectMoveConflictUseExisting = "use-existing"
)

type AppProjectMoveOptions struct {
	TargetProjectID      string `json:"target_project_id,omitempty"`
	TargetProjectName    string `json:"target_project_name,omitempty"`
	CreateProject        bool   `json:"create_project,omitempty"`
	IncludeOwnedServices bool   `json:"include_owned_services"`
	IncludeBoundServices bool   `json:"include_bound_services,omitempty"`
	OnConflict           string `json:"on_conflict,omitempty"`
	DryRun               bool   `json:"dry_run,omitempty"`
}

type BackingServiceProjectMoveOptions struct {
	TargetProjectID   string `json:"target_project_id,omitempty"`
	TargetProjectName string `json:"target_project_name,omitempty"`
	CreateProject     bool   `json:"create_project,omitempty"`
	OnConflict        string `json:"on_conflict,omitempty"`
	DryRun            bool   `json:"dry_run,omitempty"`
}

type ProjectSplitTarget struct {
	AppID             string `json:"app_id,omitempty"`
	AppName           string `json:"app_name,omitempty"`
	TargetProjectID   string `json:"target_project_id,omitempty"`
	TargetProjectName string `json:"target_project_name,omitempty"`
}

type ProjectSplitOptions struct {
	Targets              []ProjectSplitTarget `json:"targets"`
	CreateProjects       bool                 `json:"create_projects,omitempty"`
	IncludeOwnedServices bool                 `json:"include_owned_services"`
	IncludeBoundServices bool                 `json:"include_bound_services,omitempty"`
	OnConflict           string               `json:"on_conflict,omitempty"`
	DryRun               bool                 `json:"dry_run,omitempty"`
}

type ProjectMovePlan struct {
	DryRun          bool                   `json:"dry_run"`
	SourceProject   model.Project          `json:"source_project"`
	TargetProjects  []model.Project        `json:"target_projects"`
	CreatedProjects []model.Project        `json:"created_projects,omitempty"`
	Apps            []model.App            `json:"apps,omitempty"`
	BackingServices []model.BackingService `json:"backing_services,omitempty"`
	Bindings        []model.ServiceBinding `json:"bindings,omitempty"`
	Warnings        []string               `json:"warnings,omitempty"`
	Blockers        []string               `json:"blockers,omitempty"`
}

type projectMoveAppTarget struct {
	AppID           string
	TargetProjectID string
}

type projectMoveServiceTarget struct {
	ServiceID         string
	TargetProjectID   string
	TargetProjectName string
	NewName           string
	ExistingServiceID string
}

type projectMoveStateMutation struct {
	appTargets     map[string]string
	serviceTargets map[string]projectMoveServiceTarget
	bindingTargets map[string]string
}

func (s *Store) MoveAppProject(id string, opts AppProjectMoveOptions) (ProjectMovePlan, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return ProjectMovePlan{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgMoveAppProject(id, opts)
	}
	var plan ProjectMovePlan
	err := s.withLockedState(!opts.DryRun, func(state *model.State) error {
		out, err := buildAppProjectMovePlanState(state, id, opts, !opts.DryRun)
		plan = out
		return err
	})
	return plan, err
}

func (s *Store) MoveBackingServiceProject(id string, opts BackingServiceProjectMoveOptions) (ProjectMovePlan, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return ProjectMovePlan{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgMoveBackingServiceProject(id, opts)
	}
	var plan ProjectMovePlan
	err := s.withLockedState(!opts.DryRun, func(state *model.State) error {
		out, err := buildBackingServiceProjectMovePlanState(state, id, opts, !opts.DryRun)
		plan = out
		return err
	})
	return plan, err
}

func (s *Store) PlanProjectSplit(projectID string, opts ProjectSplitOptions) (ProjectMovePlan, error) {
	opts.DryRun = true
	return s.SplitProject(projectID, opts)
}

func (s *Store) SplitProject(projectID string, opts ProjectSplitOptions) (ProjectMovePlan, error) {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return ProjectMovePlan{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgSplitProject(projectID, opts)
	}
	var plan ProjectMovePlan
	err := s.withLockedState(!opts.DryRun, func(state *model.State) error {
		out, err := buildProjectSplitPlanState(state, projectID, opts, !opts.DryRun)
		plan = out
		return err
	})
	return plan, err
}

func buildAppProjectMovePlanState(state *model.State, appID string, opts AppProjectMoveOptions, apply bool) (ProjectMovePlan, error) {
	if state == nil {
		return ProjectMovePlan{}, ErrInvalidInput
	}
	policy, err := normalizeProjectMoveConflictPolicy(opts.OnConflict)
	if err != nil {
		return ProjectMovePlan{}, err
	}
	appIndex := findApp(state, appID)
	if appIndex < 0 || isDeletedApp(state.Apps[appIndex]) {
		return ProjectMovePlan{}, ErrNotFound
	}
	app := state.Apps[appIndex]
	sourceProject, err := projectByIDState(state, app.ProjectID)
	if err != nil {
		return ProjectMovePlan{}, err
	}
	targetProject, created, err := resolveProjectMoveTargetState(state, app.TenantID, opts.TargetProjectID, opts.TargetProjectName, opts.CreateProject, apply)
	if err != nil {
		return ProjectMovePlan{}, err
	}

	plan := ProjectMovePlan{
		DryRun:         !apply,
		SourceProject:  sourceProject,
		TargetProjects: []model.Project{targetProject},
	}
	if created {
		plan.CreatedProjects = []model.Project{targetProject}
	}

	mutation := projectMoveStateMutation{
		appTargets:     map[string]string{app.ID: targetProject.ID},
		serviceTargets: map[string]projectMoveServiceTarget{},
		bindingTargets: map[string]string{},
	}
	if app.ProjectID == targetProject.ID {
		plan.Warnings = append(plan.Warnings, fmt.Sprintf("app %s is already in project %s", displayNameOrID(app.Name, app.ID), displayNameOrID(targetProject.Name, targetProject.ID)))
	} else if hasInFlightOperationForApp(state.Operations, app.ID) {
		plan.Blockers = append(plan.Blockers, fmt.Sprintf("app %s has an active operation", displayNameOrID(app.Name, app.ID)))
	}

	selectedServices := map[string]model.BackingService{}
	ownedServices := ownedBackingServicesForAppState(state, app.ID)
	if len(ownedServices) > 0 && !opts.IncludeOwnedServices {
		names := make([]string, 0, len(ownedServices))
		for _, service := range ownedServices {
			names = append(names, displayNameOrID(service.Name, service.ID))
		}
		sort.Strings(names)
		plan.Blockers = append(plan.Blockers, fmt.Sprintf("owned backing services must move with app %s: %s", displayNameOrID(app.Name, app.ID), strings.Join(names, ", ")))
	}
	if opts.IncludeOwnedServices {
		for _, service := range ownedServices {
			selectedServices[service.ID] = service
		}
	}

	boundServices := boundBackingServicesForAppState(state, app.ID)
	for _, service := range boundServices {
		if service.OwnerAppID == app.ID {
			continue
		}
		if opts.IncludeBoundServices {
			selectedServices[service.ID] = service
			continue
		}
		if service.ProjectID != targetProject.ID {
			plan.Warnings = append(plan.Warnings, fmt.Sprintf("binding from app %s to service %s will become cross-project; pass include_bound_services to move the service too", displayNameOrID(app.Name, app.ID), displayNameOrID(service.Name, service.ID)))
		}
	}

	for _, service := range sortedProjectMoveServices(selectedServices) {
		target, blockers, warnings := planServiceProjectTargetState(state, service, targetProject.ID, policy, map[string]string{app.ID: targetProject.ID})
		plan.Blockers = append(plan.Blockers, blockers...)
		plan.Warnings = append(plan.Warnings, warnings...)
		if target.ExistingServiceID != "" {
			updateMovingAppBindingsToExistingServiceState(state, &mutation, app.ID, service.ID, target.ExistingServiceID)
			continue
		}
		mutation.serviceTargets[service.ID] = target
	}

	if len(plan.Blockers) > 0 {
		sort.Strings(plan.Blockers)
		sort.Strings(plan.Warnings)
		return plan, projectMoveBlockedError(plan)
	}
	plan = applyProjectMoveMutationState(state, plan, mutation, apply)
	return plan, nil
}

func buildBackingServiceProjectMovePlanState(state *model.State, serviceID string, opts BackingServiceProjectMoveOptions, apply bool) (ProjectMovePlan, error) {
	if state == nil {
		return ProjectMovePlan{}, ErrInvalidInput
	}
	policy, err := normalizeProjectMoveConflictPolicy(opts.OnConflict)
	if err != nil {
		return ProjectMovePlan{}, err
	}
	serviceIndex := findBackingService(state, serviceID)
	if serviceIndex < 0 || isDeletedBackingService(state.BackingServices[serviceIndex]) {
		return ProjectMovePlan{}, ErrNotFound
	}
	service := state.BackingServices[serviceIndex]
	sourceProject, err := projectByIDState(state, service.ProjectID)
	if err != nil {
		return ProjectMovePlan{}, err
	}
	targetProject, created, err := resolveProjectMoveTargetState(state, service.TenantID, opts.TargetProjectID, opts.TargetProjectName, opts.CreateProject, apply)
	if err != nil {
		return ProjectMovePlan{}, err
	}

	plan := ProjectMovePlan{
		DryRun:         !apply,
		SourceProject:  sourceProject,
		TargetProjects: []model.Project{targetProject},
	}
	if created {
		plan.CreatedProjects = []model.Project{targetProject}
	}

	if service.ProjectID == targetProject.ID {
		plan.Warnings = append(plan.Warnings, fmt.Sprintf("service %s is already in project %s", displayNameOrID(service.Name, service.ID), displayNameOrID(targetProject.Name, targetProject.ID)))
	} else if strings.TrimSpace(service.OwnerAppID) != "" {
		ownerIndex := findApp(state, service.OwnerAppID)
		if ownerIndex < 0 || isDeletedApp(state.Apps[ownerIndex]) {
			plan.Blockers = append(plan.Blockers, fmt.Sprintf("owned service %s references a missing owner app", displayNameOrID(service.Name, service.ID)))
		} else if state.Apps[ownerIndex].ProjectID != targetProject.ID {
			plan.Blockers = append(plan.Blockers, fmt.Sprintf("owned service %s must move with owner app %s", displayNameOrID(service.Name, service.ID), displayNameOrID(state.Apps[ownerIndex].Name, state.Apps[ownerIndex].ID)))
		}
	}

	mutation := projectMoveStateMutation{
		appTargets:     map[string]string{},
		serviceTargets: map[string]projectMoveServiceTarget{},
		bindingTargets: map[string]string{},
	}
	if service.ProjectID != targetProject.ID {
		movingApps := map[string]string{}
		if ownerAppID := strings.TrimSpace(service.OwnerAppID); ownerAppID != "" {
			if ownerIndex := findApp(state, ownerAppID); ownerIndex >= 0 && !isDeletedApp(state.Apps[ownerIndex]) && state.Apps[ownerIndex].ProjectID == targetProject.ID {
				movingApps[ownerAppID] = targetProject.ID
			}
		}
		target, blockers, warnings := planServiceProjectTargetState(state, service, targetProject.ID, policy, movingApps)
		plan.Blockers = append(plan.Blockers, blockers...)
		plan.Warnings = append(plan.Warnings, warnings...)
		if target.ExistingServiceID != "" {
			plan.Blockers = append(plan.Blockers, fmt.Sprintf("service %s cannot use an existing target service without an app binding context", displayNameOrID(service.Name, service.ID)))
		} else {
			mutation.serviceTargets[service.ID] = target
		}
	}

	if len(plan.Blockers) > 0 {
		sort.Strings(plan.Blockers)
		sort.Strings(plan.Warnings)
		return plan, projectMoveBlockedError(plan)
	}
	plan = applyProjectMoveMutationState(state, plan, mutation, apply)
	return plan, nil
}

func buildProjectSplitPlanState(state *model.State, sourceProjectID string, opts ProjectSplitOptions, apply bool) (ProjectMovePlan, error) {
	if state == nil || len(opts.Targets) == 0 {
		return ProjectMovePlan{}, ErrInvalidInput
	}
	policy, err := normalizeProjectMoveConflictPolicy(opts.OnConflict)
	if err != nil {
		return ProjectMovePlan{}, err
	}
	sourceProject, err := projectByIDState(state, sourceProjectID)
	if err != nil {
		return ProjectMovePlan{}, err
	}
	if projectDeleteRequested(state, sourceProject.ID) {
		return ProjectMovePlan{}, ErrConflict
	}

	plan := ProjectMovePlan{
		DryRun:        !apply,
		SourceProject: sourceProject,
	}
	mutation := projectMoveStateMutation{
		appTargets:     map[string]string{},
		serviceTargets: map[string]projectMoveServiceTarget{},
		bindingTargets: map[string]string{},
	}
	targetProjectsByID := map[string]model.Project{}
	createdProjectsByID := map[string]model.Project{}
	appTargetsByID := map[string]string{}

	for _, rawTarget := range opts.Targets {
		app, err := resolveSplitTargetAppState(state, sourceProject, rawTarget)
		if err != nil {
			return plan, err
		}
		targetProject, created, err := resolveProjectMoveTargetState(state, sourceProject.TenantID, rawTarget.TargetProjectID, rawTarget.TargetProjectName, opts.CreateProjects, apply)
		if err != nil {
			return plan, err
		}
		if existingTarget, ok := appTargetsByID[app.ID]; ok && existingTarget != targetProject.ID {
			plan.Blockers = append(plan.Blockers, fmt.Sprintf("app %s is mapped to more than one target project", displayNameOrID(app.Name, app.ID)))
			continue
		}
		appTargetsByID[app.ID] = targetProject.ID
		mutation.appTargets[app.ID] = targetProject.ID
		targetProjectsByID[targetProject.ID] = targetProject
		if created {
			createdProjectsByID[targetProject.ID] = targetProject
		}
		if app.ProjectID == targetProject.ID {
			plan.Warnings = append(plan.Warnings, fmt.Sprintf("app %s is already in project %s", displayNameOrID(app.Name, app.ID), displayNameOrID(targetProject.Name, targetProject.ID)))
		}
		if hasInFlightOperationForApp(state.Operations, app.ID) {
			plan.Blockers = append(plan.Blockers, fmt.Sprintf("app %s has an active operation", displayNameOrID(app.Name, app.ID)))
		}
	}

	selectedServices := map[string]projectMoveServiceTarget{}
	for appID, targetProjectID := range appTargetsByID {
		appIndex := findApp(state, appID)
		if appIndex < 0 {
			continue
		}
		app := state.Apps[appIndex]
		ownedServices := ownedBackingServicesForAppState(state, app.ID)
		if len(ownedServices) > 0 && !opts.IncludeOwnedServices {
			names := make([]string, 0, len(ownedServices))
			for _, service := range ownedServices {
				names = append(names, displayNameOrID(service.Name, service.ID))
			}
			sort.Strings(names)
			plan.Blockers = append(plan.Blockers, fmt.Sprintf("owned backing services must move with app %s: %s", displayNameOrID(app.Name, app.ID), strings.Join(names, ", ")))
		}
		if opts.IncludeOwnedServices {
			for _, service := range ownedServices {
				selectedServices[service.ID] = projectMoveServiceTarget{ServiceID: service.ID, TargetProjectID: targetProjectID}
			}
		}

		for _, service := range boundBackingServicesForAppState(state, app.ID) {
			if service.OwnerAppID == app.ID {
				continue
			}
			if !opts.IncludeBoundServices {
				if service.ProjectID != targetProjectID {
					plan.Warnings = append(plan.Warnings, fmt.Sprintf("binding from app %s to service %s will become cross-project; pass include_bound_services to move the service too", displayNameOrID(app.Name, app.ID), displayNameOrID(service.Name, service.ID)))
				}
				continue
			}
			if existing, ok := selectedServices[service.ID]; ok && existing.TargetProjectID != targetProjectID {
				plan.Blockers = append(plan.Blockers, fmt.Sprintf("shared service %s is mapped to more than one target project", displayNameOrID(service.Name, service.ID)))
				continue
			}
			selectedServices[service.ID] = projectMoveServiceTarget{ServiceID: service.ID, TargetProjectID: targetProjectID}
		}
	}

	for _, selected := range sortedProjectMoveServiceTargets(selectedServices) {
		serviceIndex := findBackingService(state, selected.ServiceID)
		if serviceIndex < 0 {
			continue
		}
		service := state.BackingServices[serviceIndex]
		target, blockers, warnings := planServiceProjectTargetState(state, service, selected.TargetProjectID, policy, appTargetsByID)
		plan.Blockers = append(plan.Blockers, blockers...)
		plan.Warnings = append(plan.Warnings, warnings...)
		if target.ExistingServiceID != "" {
			for _, binding := range state.ServiceBindings {
				if binding.ServiceID != service.ID {
					continue
				}
				if appTargetsByID[binding.AppID] == selected.TargetProjectID {
					mutation.bindingTargets[binding.ID] = target.ExistingServiceID
				}
			}
			continue
		}
		mutation.serviceTargets[service.ID] = target
	}

	plan.TargetProjects = sortedProjectsFromMap(targetProjectsByID)
	plan.CreatedProjects = sortedProjectsFromMap(createdProjectsByID)
	if len(plan.Blockers) > 0 {
		sort.Strings(plan.Blockers)
		sort.Strings(plan.Warnings)
		return plan, projectMoveBlockedError(plan)
	}
	plan = applyProjectMoveMutationState(state, plan, mutation, apply)
	return plan, nil
}

func planServiceProjectTargetState(state *model.State, service model.BackingService, targetProjectID, policy string, movingApps map[string]string) (projectMoveServiceTarget, []string, []string) {
	target := projectMoveServiceTarget{
		ServiceID:       service.ID,
		TargetProjectID: targetProjectID,
		NewName:         service.Name,
	}
	targetProject, err := projectByIDState(state, targetProjectID)
	if err == nil {
		target.TargetProjectName = targetProject.Name
	}
	blockers := []string{}
	warnings := []string{}
	if service.ProjectID == targetProjectID {
		warnings = append(warnings, fmt.Sprintf("service %s is already in target project", displayNameOrID(service.Name, service.ID)))
		return target, blockers, warnings
	}
	if hasInFlightOperationForService(state.Operations, service.ID) {
		blockers = append(blockers, fmt.Sprintf("service %s has an active operation", displayNameOrID(service.Name, service.ID)))
	}
	if strings.TrimSpace(service.OwnerAppID) != "" {
		if movingApps == nil || movingApps[service.OwnerAppID] != targetProjectID {
			blockers = append(blockers, fmt.Sprintf("owned service %s must move with owner app", displayNameOrID(service.Name, service.ID)))
		}
	}
	if hasBindingsOutsideMovingTargetState(state, service.ID, targetProjectID, movingApps) {
		blockers = append(blockers, fmt.Sprintf("backing service %s is bound to apps that are not moving to the same project", displayNameOrID(service.Name, service.ID)))
	}
	if conflict := conflictingBackingServiceState(state, service.TenantID, targetProjectID, service.Name, service.ID); conflict != nil {
		switch policy {
		case ProjectMoveConflictRename:
			target.NewName = nextAvailableBackingServiceName(state, service.TenantID, targetProjectID, service.Name)
			warnings = append(warnings, fmt.Sprintf("service %s will be renamed to %s in the target project", displayNameOrID(service.Name, service.ID), target.NewName))
		case ProjectMoveConflictUseExisting:
			if strings.TrimSpace(service.OwnerAppID) != "" {
				blockers = append(blockers, fmt.Sprintf("owned service %s cannot use existing target service %s", displayNameOrID(service.Name, service.ID), displayNameOrID(conflict.Name, conflict.ID)))
			} else if !sameBackingServiceCompatibility(service, *conflict) {
				blockers = append(blockers, fmt.Sprintf("service %s conflicts with incompatible target service %s", displayNameOrID(service.Name, service.ID), displayNameOrID(conflict.Name, conflict.ID)))
			} else {
				target.ExistingServiceID = conflict.ID
				warnings = append(warnings, fmt.Sprintf("bindings for service %s will use existing target service %s", displayNameOrID(service.Name, service.ID), displayNameOrID(conflict.Name, conflict.ID)))
			}
		default:
			blockers = append(blockers, fmt.Sprintf("target project already has backing service named %s", service.Name))
		}
	}
	return target, blockers, warnings
}

func applyProjectMoveMutationState(state *model.State, plan ProjectMovePlan, mutation projectMoveStateMutation, apply bool) ProjectMovePlan {
	now := time.Now().UTC()
	apps := make([]model.App, 0, len(mutation.appTargets))
	for appID, targetProjectID := range mutation.appTargets {
		index := findApp(state, appID)
		if index < 0 {
			continue
		}
		app := cloneProjectMoveApp(state.Apps[index])
		app.ProjectID = targetProjectID
		app.UpdatedAt = now
		if apply && state.Apps[index].ProjectID != targetProjectID {
			state.Apps[index].ProjectID = targetProjectID
			state.Apps[index].UpdatedAt = now
			app = cloneProjectMoveApp(state.Apps[index])
		}
		app.Bindings = nil
		app.BackingServices = nil
		apps = append(apps, app)
	}
	sortAppsForProjectMove(apps)
	plan.Apps = apps

	services := make([]model.BackingService, 0, len(mutation.serviceTargets))
	for serviceID, target := range mutation.serviceTargets {
		index := findBackingService(state, serviceID)
		if index < 0 {
			continue
		}
		service := cloneBackingService(state.BackingServices[index])
		service.ProjectID = target.TargetProjectID
		service.Name = target.NewName
		service.UpdatedAt = now
		if apply {
			state.BackingServices[index].ProjectID = target.TargetProjectID
			state.BackingServices[index].Name = target.NewName
			state.BackingServices[index].UpdatedAt = now
			service = cloneBackingService(state.BackingServices[index])
		}
		services = append(services, service)
	}
	sortBackingServices(services)
	plan.BackingServices = services

	bindings := make([]model.ServiceBinding, 0, len(mutation.bindingTargets))
	for bindingID, serviceID := range mutation.bindingTargets {
		index := findServiceBinding(state, bindingID)
		if index < 0 {
			continue
		}
		binding := cloneServiceBinding(state.ServiceBindings[index])
		binding.ServiceID = serviceID
		binding.UpdatedAt = now
		if apply {
			state.ServiceBindings[index].ServiceID = serviceID
			state.ServiceBindings[index].UpdatedAt = now
			binding = cloneServiceBinding(state.ServiceBindings[index])
		}
		bindings = append(bindings, binding)
	}
	sortServiceBindings(bindings)
	plan.Bindings = bindings
	sort.Strings(plan.Warnings)
	sort.Strings(plan.Blockers)
	return plan
}

func resolveProjectMoveTargetState(state *model.State, tenantID, targetProjectID, targetProjectName string, create, apply bool) (model.Project, bool, error) {
	tenantID = strings.TrimSpace(tenantID)
	targetProjectID = strings.TrimSpace(targetProjectID)
	targetProjectName = strings.TrimSpace(targetProjectName)
	if tenantID == "" || (targetProjectID == "" && targetProjectName == "") {
		return model.Project{}, false, ErrInvalidInput
	}
	if findTenant(state, tenantID) < 0 {
		return model.Project{}, false, ErrNotFound
	}
	if targetProjectID != "" {
		project, err := projectByIDState(state, targetProjectID)
		if err != nil {
			return model.Project{}, false, err
		}
		if project.TenantID != tenantID {
			return model.Project{}, false, ErrNotFound
		}
		if projectDeleteRequested(state, project.ID) {
			return model.Project{}, false, ErrConflict
		}
		return project, false, nil
	}

	slug := model.Slugify(targetProjectName)
	if slug == "" {
		return model.Project{}, false, ErrInvalidInput
	}
	for _, project := range state.Projects {
		if project.TenantID == tenantID && project.Slug == slug {
			if projectDeleteRequested(state, project.ID) {
				return model.Project{}, false, ErrConflict
			}
			return project, false, nil
		}
	}
	if !create {
		return model.Project{}, false, ErrNotFound
	}
	now := time.Now().UTC()
	project := model.Project{
		ID:          model.NewID("project"),
		TenantID:    tenantID,
		Name:        targetProjectName,
		Slug:        slug,
		Description: "",
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if apply {
		state.Projects = append(state.Projects, project)
	}
	return project, true, nil
}

func normalizeProjectMoveConflictPolicy(raw string) (string, error) {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", ProjectMoveConflictFail:
		return ProjectMoveConflictFail, nil
	case ProjectMoveConflictRename:
		return ProjectMoveConflictRename, nil
	case ProjectMoveConflictUseExisting:
		return ProjectMoveConflictUseExisting, nil
	default:
		return "", ErrInvalidInput
	}
}

func projectMoveBlockedError(plan ProjectMovePlan) error {
	if len(plan.Blockers) == 0 {
		return nil
	}
	return ErrConflict
}

func projectByIDState(state *model.State, id string) (model.Project, error) {
	index := findProject(state, strings.TrimSpace(id))
	if index < 0 {
		return model.Project{}, ErrNotFound
	}
	return state.Projects[index], nil
}

func resolveSplitTargetAppState(state *model.State, sourceProject model.Project, target ProjectSplitTarget) (model.App, error) {
	appID := strings.TrimSpace(target.AppID)
	appName := strings.TrimSpace(target.AppName)
	if appID == "" && appName == "" {
		return model.App{}, ErrInvalidInput
	}
	if appID != "" {
		index := findApp(state, appID)
		if index < 0 || isDeletedApp(state.Apps[index]) {
			return model.App{}, ErrNotFound
		}
		app := state.Apps[index]
		if app.TenantID != sourceProject.TenantID || app.ProjectID != sourceProject.ID {
			return model.App{}, ErrNotFound
		}
		return app, nil
	}

	var matches []model.App
	for _, app := range state.Apps {
		if isDeletedApp(app) || app.TenantID != sourceProject.TenantID || app.ProjectID != sourceProject.ID {
			continue
		}
		if strings.EqualFold(app.Name, appName) || strings.EqualFold(model.Slugify(app.Name), model.Slugify(appName)) {
			matches = append(matches, app)
		}
	}
	if len(matches) == 0 {
		return model.App{}, ErrNotFound
	}
	if len(matches) > 1 {
		return model.App{}, ErrConflict
	}
	return matches[0], nil
}

func ownedBackingServicesForAppState(state *model.State, appID string) []model.BackingService {
	services := make([]model.BackingService, 0)
	for _, service := range state.BackingServices {
		if service.OwnerAppID != appID || isDeletedBackingService(service) {
			continue
		}
		services = append(services, cloneBackingService(service))
	}
	sortBackingServices(services)
	return services
}

func boundBackingServicesForAppState(state *model.State, appID string) []model.BackingService {
	servicesByID := map[string]model.BackingService{}
	for _, binding := range state.ServiceBindings {
		if binding.AppID != appID {
			continue
		}
		index := findBackingService(state, binding.ServiceID)
		if index < 0 || isDeletedBackingService(state.BackingServices[index]) {
			continue
		}
		servicesByID[state.BackingServices[index].ID] = cloneBackingService(state.BackingServices[index])
	}
	services := make([]model.BackingService, 0, len(servicesByID))
	for _, service := range servicesByID {
		services = append(services, service)
	}
	sortBackingServices(services)
	return services
}

func hasInFlightOperationForService(ops []model.Operation, serviceID string) bool {
	for _, op := range ops {
		if op.ServiceID != serviceID {
			continue
		}
		if isActiveOperationStatus(op.Status) {
			return true
		}
	}
	return false
}

func hasBindingsOutsideMovingTargetState(state *model.State, serviceID, targetProjectID string, movingApps map[string]string) bool {
	if movingApps == nil {
		return hasServiceBindings(state, serviceID)
	}
	for _, binding := range state.ServiceBindings {
		if binding.ServiceID != serviceID {
			continue
		}
		if movingApps[binding.AppID] == targetProjectID {
			continue
		}
		return true
	}
	return false
}

func updateMovingAppBindingsToExistingServiceState(state *model.State, mutation *projectMoveStateMutation, appID, sourceServiceID, targetServiceID string) {
	if mutation == nil {
		return
	}
	for _, binding := range state.ServiceBindings {
		if binding.AppID == appID && binding.ServiceID == sourceServiceID {
			mutation.bindingTargets[binding.ID] = targetServiceID
		}
	}
}

func conflictingBackingServiceState(state *model.State, tenantID, projectID, name, exceptID string) *model.BackingService {
	for _, service := range state.BackingServices {
		if service.ID == exceptID || isDeletedBackingService(service) {
			continue
		}
		if service.TenantID == tenantID && service.ProjectID == projectID && strings.EqualFold(service.Name, name) {
			copy := cloneBackingService(service)
			return &copy
		}
	}
	return nil
}

func sameBackingServiceCompatibility(left, right model.BackingService) bool {
	return strings.EqualFold(left.Type, right.Type) && strings.EqualFold(left.Provisioner, right.Provisioner)
}

func sortedProjectMoveServices(in map[string]model.BackingService) []model.BackingService {
	services := make([]model.BackingService, 0, len(in))
	for _, service := range in {
		services = append(services, service)
	}
	sortBackingServices(services)
	return services
}

func sortedProjectMoveServiceTargets(in map[string]projectMoveServiceTarget) []projectMoveServiceTarget {
	targets := make([]projectMoveServiceTarget, 0, len(in))
	for _, target := range in {
		targets = append(targets, target)
	}
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].ServiceID == targets[j].ServiceID {
			return targets[i].TargetProjectID < targets[j].TargetProjectID
		}
		return targets[i].ServiceID < targets[j].ServiceID
	})
	return targets
}

func sortedProjectsFromMap(in map[string]model.Project) []model.Project {
	projects := make([]model.Project, 0, len(in))
	for _, project := range in {
		projects = append(projects, project)
	}
	sort.Slice(projects, func(i, j int) bool {
		if projects[i].CreatedAt.Equal(projects[j].CreatedAt) {
			return projects[i].ID < projects[j].ID
		}
		return projects[i].CreatedAt.Before(projects[j].CreatedAt)
	})
	return projects
}

func sortAppsForProjectMove(apps []model.App) {
	sort.Slice(apps, func(i, j int) bool {
		if apps[i].CreatedAt.Equal(apps[j].CreatedAt) {
			return apps[i].ID < apps[j].ID
		}
		return apps[i].CreatedAt.Before(apps[j].CreatedAt)
	})
}

func cloneProjectMoveApp(app model.App) model.App {
	out := app
	out.Spec = *cloneAppSpec(&app.Spec)
	out.Route = cloneAppRoute(app.Route)
	model.SetAppSourceState(&out, app.OriginSource, app.BuildSource)
	out.Bindings = cloneServiceBindingsForProjectMove(app.Bindings)
	out.BackingServices = cloneBackingServicesForProjectMove(app.BackingServices)
	return out
}

func cloneServiceBindingsForProjectMove(bindings []model.ServiceBinding) []model.ServiceBinding {
	if len(bindings) == 0 {
		return nil
	}
	out := make([]model.ServiceBinding, 0, len(bindings))
	for _, binding := range bindings {
		out = append(out, cloneServiceBinding(binding))
	}
	return out
}

func cloneBackingServicesForProjectMove(services []model.BackingService) []model.BackingService {
	if len(services) == 0 {
		return nil
	}
	out := make([]model.BackingService, 0, len(services))
	for _, service := range services {
		out = append(out, cloneBackingService(service))
	}
	return out
}

func displayNameOrID(name, id string) string {
	if strings.TrimSpace(name) != "" {
		return strings.TrimSpace(name)
	}
	return strings.TrimSpace(id)
}

func (s *Store) pgMoveAppProject(id string, opts AppProjectMoveOptions) (ProjectMovePlan, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ProjectMovePlan{}, fmt.Errorf("begin move app project transaction: %w", err)
	}
	defer tx.Rollback()

	app, err := s.pgGetAppTx(ctx, tx, id, !opts.DryRun)
	if err != nil {
		return ProjectMovePlan{}, mapDBErr(err)
	}
	state, err := s.pgLoadProjectMoveTenantStateTx(ctx, tx, app.TenantID, !opts.DryRun)
	if err != nil {
		return ProjectMovePlan{}, err
	}
	before := cloneProjectMoveState(state)
	plan, err := buildAppProjectMovePlanState(&state, id, opts, !opts.DryRun)
	if err != nil {
		return plan, err
	}
	if opts.DryRun {
		return plan, nil
	}
	if err := s.pgPersistProjectMoveStateDiffTx(ctx, tx, before, state); err != nil {
		return plan, err
	}
	if err := tx.Commit(); err != nil {
		return plan, fmt.Errorf("commit move app project transaction: %w", err)
	}
	return plan, nil
}

func (s *Store) pgMoveBackingServiceProject(id string, opts BackingServiceProjectMoveOptions) (ProjectMovePlan, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ProjectMovePlan{}, fmt.Errorf("begin move backing service project transaction: %w", err)
	}
	defer tx.Rollback()

	service, err := s.pgGetBackingServiceTx(ctx, tx, id, !opts.DryRun)
	if err != nil {
		return ProjectMovePlan{}, mapDBErr(err)
	}
	state, err := s.pgLoadProjectMoveTenantStateTx(ctx, tx, service.TenantID, !opts.DryRun)
	if err != nil {
		return ProjectMovePlan{}, err
	}
	before := cloneProjectMoveState(state)
	plan, err := buildBackingServiceProjectMovePlanState(&state, id, opts, !opts.DryRun)
	if err != nil {
		return plan, err
	}
	if opts.DryRun {
		return plan, nil
	}
	if err := s.pgPersistProjectMoveStateDiffTx(ctx, tx, before, state); err != nil {
		return plan, err
	}
	if err := tx.Commit(); err != nil {
		return plan, fmt.Errorf("commit move backing service project transaction: %w", err)
	}
	return plan, nil
}

func (s *Store) pgSplitProject(projectID string, opts ProjectSplitOptions) (ProjectMovePlan, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ProjectMovePlan{}, fmt.Errorf("begin split project transaction: %w", err)
	}
	defer tx.Rollback()

	project, err := scanProject(tx.QueryRowContext(ctx, `
SELECT id, tenant_id, name, slug, description, default_runtime_id, created_at, updated_at
FROM fugue_projects
WHERE id = $1
`, projectID))
	if err != nil {
		return ProjectMovePlan{}, mapDBErr(err)
	}
	state, err := s.pgLoadProjectMoveTenantStateTx(ctx, tx, project.TenantID, !opts.DryRun)
	if err != nil {
		return ProjectMovePlan{}, err
	}
	before := cloneProjectMoveState(state)
	plan, err := buildProjectSplitPlanState(&state, projectID, opts, !opts.DryRun)
	if err != nil {
		return plan, err
	}
	if opts.DryRun {
		return plan, nil
	}
	if err := s.pgPersistProjectMoveStateDiffTx(ctx, tx, before, state); err != nil {
		return plan, err
	}
	if err := tx.Commit(); err != nil {
		return plan, fmt.Errorf("commit split project transaction: %w", err)
	}
	return plan, nil
}

func (s *Store) pgLoadProjectMoveTenantStateTx(ctx context.Context, tx *sql.Tx, tenantID string, forUpdate bool) (model.State, error) {
	state := model.State{}
	tenant, err := scanTenant(tx.QueryRowContext(ctx, `
SELECT id, name, slug, status, created_at, updated_at
FROM fugue_tenants
WHERE id = $1
`, tenantID))
	if err != nil {
		return state, mapDBErr(err)
	}
	state.Tenants = []model.Tenant{tenant}

	projectQuery := `
SELECT id, tenant_id, name, slug, description, default_runtime_id, created_at, updated_at, delete_requested_at
FROM fugue_projects
WHERE tenant_id = $1
ORDER BY created_at ASC
`
	if forUpdate {
		projectQuery += ` FOR UPDATE`
	}
	projectRows, err := tx.QueryContext(ctx, projectQuery, tenantID)
	if err != nil {
		return state, fmt.Errorf("load project move projects: %w", err)
	}
	defer projectRows.Close()
	for projectRows.Next() {
		project, deleteRequestedAt, err := scanProjectWithDeleteRequest(projectRows)
		if err != nil {
			return state, err
		}
		state.Projects = append(state.Projects, project)
		if deleteRequestedAt != nil {
			if state.ProjectDeleteRequests == nil {
				state.ProjectDeleteRequests = map[string]time.Time{}
			}
			state.ProjectDeleteRequests[project.ID] = *deleteRequestedAt
		}
	}
	if err := projectRows.Err(); err != nil {
		return state, fmt.Errorf("iterate project move projects: %w", err)
	}

	appQuery := `
SELECT id, tenant_id, project_id, name, description, source_json, route_json, spec_json, status_json, created_at, updated_at
FROM fugue_apps
WHERE tenant_id = $1
ORDER BY created_at ASC
`
	if forUpdate {
		appQuery += ` FOR UPDATE`
	}
	appRows, err := tx.QueryContext(ctx, appQuery, tenantID)
	if err != nil {
		return state, fmt.Errorf("load project move apps: %w", err)
	}
	defer appRows.Close()
	for appRows.Next() {
		app, err := scanApp(appRows)
		if err != nil {
			return state, err
		}
		normalizeAppStatusForRead(&app)
		state.Apps = append(state.Apps, app)
	}
	if err := appRows.Err(); err != nil {
		return state, fmt.Errorf("iterate project move apps: %w", err)
	}

	serviceQuery := `
SELECT id, tenant_id, project_id, owner_app_id, name, description, type, provisioner, status, spec_json, current_runtime_started_at, current_runtime_ready_at, created_at, updated_at
FROM fugue_backing_services
WHERE tenant_id = $1
ORDER BY created_at ASC
`
	if forUpdate {
		serviceQuery += ` FOR UPDATE`
	}
	serviceRows, err := tx.QueryContext(ctx, serviceQuery, tenantID)
	if err != nil {
		return state, fmt.Errorf("load project move backing services: %w", err)
	}
	defer serviceRows.Close()
	for serviceRows.Next() {
		service, err := scanBackingService(serviceRows)
		if err != nil {
			return state, err
		}
		state.BackingServices = append(state.BackingServices, service)
	}
	if err := serviceRows.Err(); err != nil {
		return state, fmt.Errorf("iterate project move backing services: %w", err)
	}

	bindingQuery := `
SELECT id, tenant_id, app_id, service_id, alias, env_json, created_at, updated_at
FROM fugue_service_bindings
WHERE tenant_id = $1
ORDER BY created_at ASC
`
	if forUpdate {
		bindingQuery += ` FOR UPDATE`
	}
	bindingRows, err := tx.QueryContext(ctx, bindingQuery, tenantID)
	if err != nil {
		return state, fmt.Errorf("load project move service bindings: %w", err)
	}
	defer bindingRows.Close()
	for bindingRows.Next() {
		binding, err := scanServiceBinding(bindingRows)
		if err != nil {
			return state, err
		}
		state.ServiceBindings = append(state.ServiceBindings, binding)
	}
	if err := bindingRows.Err(); err != nil {
		return state, fmt.Errorf("iterate project move service bindings: %w", err)
	}

	operationQuery := `
SELECT id, tenant_id, type, status, execution_mode, requested_by_type, requested_by_id, app_id, service_id, source_runtime_id, target_runtime_id, desired_replicas, desired_spec_json, desired_source_json, result_message, manifest_path, assigned_runtime_id, error_message, created_at, updated_at, started_at, completed_at
FROM fugue_operations
WHERE tenant_id = $1
ORDER BY created_at ASC
`
	if forUpdate {
		operationQuery += ` FOR UPDATE`
	}
	operationRows, err := tx.QueryContext(ctx, operationQuery, tenantID)
	if err != nil {
		return state, fmt.Errorf("load project move operations: %w", err)
	}
	defer operationRows.Close()
	for operationRows.Next() {
		op, err := scanOperation(operationRows)
		if err != nil {
			return state, err
		}
		state.Operations = append(state.Operations, op)
	}
	if err := operationRows.Err(); err != nil {
		return state, fmt.Errorf("iterate project move operations: %w", err)
	}
	return state, nil
}

func (s *Store) pgPersistProjectMoveStateDiffTx(ctx context.Context, tx *sql.Tx, before, after model.State) error {
	beforeProjects := mapProjectsByID(before.Projects)
	for _, project := range after.Projects {
		if _, ok := beforeProjects[project.ID]; ok {
			continue
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_projects (id, tenant_id, name, slug, description, default_runtime_id, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
`, project.ID, project.TenantID, project.Name, project.Slug, project.Description, nullIfEmpty(project.DefaultRuntimeID), project.CreatedAt, project.UpdatedAt); err != nil {
			return mapDBErr(err)
		}
	}

	beforeApps := mapAppsByID(before.Apps)
	for _, app := range after.Apps {
		previous, ok := beforeApps[app.ID]
		if !ok {
			continue
		}
		if previous.ProjectID == app.ProjectID && previous.UpdatedAt.Equal(app.UpdatedAt) {
			continue
		}
		if err := s.pgUpdateAppTx(ctx, tx, app); err != nil {
			return err
		}
	}

	beforeServices := mapBackingServicesByID(before.BackingServices)
	for _, service := range after.BackingServices {
		previous, ok := beforeServices[service.ID]
		if !ok {
			continue
		}
		if previous.ProjectID == service.ProjectID && previous.Name == service.Name && previous.UpdatedAt.Equal(service.UpdatedAt) {
			continue
		}
		if err := s.pgUpdateBackingServiceTx(ctx, tx, service); err != nil {
			return err
		}
	}

	beforeBindings := mapServiceBindingsByID(before.ServiceBindings)
	for _, binding := range after.ServiceBindings {
		previous, ok := beforeBindings[binding.ID]
		if !ok {
			continue
		}
		if previous.ServiceID == binding.ServiceID && previous.UpdatedAt.Equal(binding.UpdatedAt) && previous.Alias == binding.Alias && reflect.DeepEqual(previous.Env, binding.Env) {
			continue
		}
		if err := s.pgUpdateServiceBindingTx(ctx, tx, binding); err != nil {
			return err
		}
	}
	return nil
}

func scanProjectWithDeleteRequest(scanner sqlScanner) (model.Project, *time.Time, error) {
	var project model.Project
	var tenantID sql.NullString
	var defaultRuntimeID sql.NullString
	var deleteRequestedAt sql.NullTime
	if err := scanner.Scan(&project.ID, &tenantID, &project.Name, &project.Slug, &project.Description, &defaultRuntimeID, &project.CreatedAt, &project.UpdatedAt, &deleteRequestedAt); err != nil {
		return model.Project{}, nil, err
	}
	project.TenantID = tenantID.String
	project.DefaultRuntimeID = defaultRuntimeID.String
	if deleteRequestedAt.Valid {
		return project, &deleteRequestedAt.Time, nil
	}
	return project, nil, nil
}

func cloneProjectMoveState(state model.State) model.State {
	out := model.State{
		Tenants:               append([]model.Tenant(nil), state.Tenants...),
		Projects:              append([]model.Project(nil), state.Projects...),
		ProjectDeleteRequests: cloneTimeMap(state.ProjectDeleteRequests),
		Operations:            append([]model.Operation(nil), state.Operations...),
	}
	out.Apps = make([]model.App, 0, len(state.Apps))
	for _, app := range state.Apps {
		out.Apps = append(out.Apps, cloneProjectMoveApp(app))
	}
	out.BackingServices = cloneBackingServicesForProjectMove(state.BackingServices)
	out.ServiceBindings = cloneServiceBindingsForProjectMove(state.ServiceBindings)
	return out
}

func cloneTimeMap(in map[string]time.Time) map[string]time.Time {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]time.Time, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func mapProjectsByID(projects []model.Project) map[string]model.Project {
	out := make(map[string]model.Project, len(projects))
	for _, project := range projects {
		out[project.ID] = project
	}
	return out
}

func mapAppsByID(apps []model.App) map[string]model.App {
	out := make(map[string]model.App, len(apps))
	for _, app := range apps {
		out[app.ID] = app
	}
	return out
}

func mapBackingServicesByID(services []model.BackingService) map[string]model.BackingService {
	out := make(map[string]model.BackingService, len(services))
	for _, service := range services {
		out[service.ID] = service
	}
	return out
}

func mapServiceBindingsByID(bindings []model.ServiceBinding) map[string]model.ServiceBinding {
	out := make(map[string]model.ServiceBinding, len(bindings))
	for _, binding := range bindings {
		out[binding.ID] = binding
	}
	return out
}
