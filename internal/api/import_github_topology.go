package api

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/sourceimport"
	"fugue/internal/store"
)

type importedGitHubTopology struct {
	PrimaryApp      model.App
	PrimaryOp       model.Operation
	Apps            []model.App
	Operations      []model.Operation
	PrimaryService  string
	ServiceDetails  []map[string]any
	Warnings        []string
	InferenceReport []sourceimport.TopologyInference
	Plan            *model.TopologyDeployPlan
}

type topologySourceBuilder func(service sourceimport.ComposeService, composeDependsOn []string) (model.AppSource, error)

type topologyAuditMetadataBuilder func(source model.AppSource, route model.AppRoute) map[string]string

type topologyImportOptions struct {
	ProjectID                string
	ProjectName              string
	RuntimeID                string
	Replicas                 int
	ServicePort              int
	Description              string
	BaseName                 string
	Env                      map[string]string
	ServiceEnv               map[string]map[string]string
	ServicePersistentStorage map[string]model.ServicePersistentStorageOverride
	AuditAction              string
	BuildSource              topologySourceBuilder
	DesiredOriginSource      func(existing *model.App, source model.AppSource) *model.AppSource
	BuildAuditMetadata       topologyAuditMetadataBuilder
	UpdateExisting           bool
	DeleteMissing            bool
	DryRun                   bool
	Family                   topologyImportFamily
}

type topologyImportFamily struct {
	Kind string
	Key  string
}

type plannedTopologyService struct {
	Service sourceimport.ComposeService
	AppName string
	Route   *model.AppRoute
	Source  model.AppSource
	Spec    model.AppSpec
	Action  string
	Match   *model.App
}

type composeImportNamingStrategy struct {
	MaxAttempts       int
	MaxServiceNameLen int
}

var defaultComposeImportNamingStrategy = composeImportNamingStrategy{
	MaxAttempts:       8,
	MaxServiceNameLen: 50,
}

func (s *Server) importResolvedGitHubTopology(principal model.Principal, tenantID string, req importGitHubRequest, runtimeID string, replicas int, description string, baseName string, topology sourceimport.NormalizedTopology) (importedGitHubTopology, error) {
	services, err := applyImportGitHubPersistentStorageSeedFiles(topology.Services, req.PersistentStorageSeedFiles)
	if err != nil {
		return importedGitHubTopology{}, invalidComposeImport(err)
	}
	topology.Services = services

	return s.importResolvedTopology(principal, tenantID, topologyImportOptions{
		ProjectID:                req.ProjectID,
		RuntimeID:                runtimeID,
		Replicas:                 replicas,
		ServicePort:              req.ServicePort,
		Description:              description,
		BaseName:                 baseName,
		Env:                      req.Env,
		ServiceEnv:               normalizedImportServiceEnv(req.ServiceEnv),
		ServicePersistentStorage: normalizedImportServicePersistentStorage(req.ServicePersistentStorage),
		AuditAction:              "app.import_github",
		BuildSource: func(service sourceimport.ComposeService, composeDependsOn []string) (model.AppSource, error) {
			return buildQueuedComposeServiceSource(req, service, composeDependsOn)
		},
		DesiredOriginSource: func(_ *model.App, source model.AppSource) *model.AppSource {
			return model.CloneAppSource(&source)
		},
		BuildAuditMetadata: func(source model.AppSource, _ model.AppRoute) map[string]string {
			return map[string]string{
				"repo_url": strings.TrimSpace(req.RepoURL),
			}
		},
		UpdateExisting: req.UpdateExisting,
		DeleteMissing:  req.DeleteMissing,
		DryRun:         req.DryRun,
		Family: topologyImportFamily{
			Kind: "github",
			Key:  normalizeTopologyGitHubFamilyKey(req.RepoURL),
		},
	}, topology)
}

func applyImportGitHubPersistentStorageSeedFiles(services []sourceimport.ComposeService, overrides []importGitHubPersistentStorageSeedFile) ([]sourceimport.ComposeService, error) {
	if len(overrides) == 0 {
		return services, nil
	}

	clonedServices := cloneComposeServicesForImport(services)
	serviceIndexes := make(map[string]int, len(clonedServices))
	for index, service := range clonedServices {
		name := strings.TrimSpace(service.Name)
		if name == "" {
			continue
		}
		serviceIndexes[name] = index
	}

	seen := make(map[string]struct{}, len(overrides))
	for index, override := range overrides {
		serviceName := strings.TrimSpace(override.Service)
		if serviceName == "" {
			return nil, fmt.Errorf("persistent_storage_seed_files[%d].service is required", index)
		}

		path := strings.TrimSpace(override.Path)
		if path == "" {
			return nil, fmt.Errorf("persistent_storage_seed_files[%d].path is required", index)
		}

		key := serviceName + "\x00" + path
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("persistent_storage_seed_files contains duplicate entry for service %q path %q", serviceName, path)
		}
		seen[key] = struct{}{}

		serviceIndex, ok := serviceIndexes[serviceName]
		if !ok {
			return nil, fmt.Errorf("persistent_storage_seed_files references unknown service %q", serviceName)
		}

		service := &clonedServices[serviceIndex]
		if service.PersistentStorage == nil {
			return nil, fmt.Errorf("persistent_storage_seed_files references service %q without persistent storage", serviceName)
		}

		seedFileIndex := -1
		for candidateIndex, seedFile := range service.PersistentStorageSeedFiles {
			if strings.TrimSpace(seedFile.Path) == path {
				seedFileIndex = candidateIndex
				break
			}
		}
		if seedFileIndex < 0 {
			return nil, fmt.Errorf("persistent_storage_seed_files references unknown editable file %q for service %q", path, serviceName)
		}

		mountIndex := -1
		for candidateIndex, mount := range service.PersistentStorage.Mounts {
			if strings.TrimSpace(mount.Path) == path {
				mountIndex = candidateIndex
				break
			}
		}
		if mountIndex < 0 {
			return nil, fmt.Errorf("persistent_storage_seed_files references persistent file %q for service %q that is no longer available", path, serviceName)
		}
		if service.PersistentStorage.Mounts[mountIndex].Kind != model.AppPersistentStorageMountKindFile {
			return nil, fmt.Errorf("persistent_storage_seed_files references non-file mount %q for service %q", path, serviceName)
		}

		service.PersistentStorage.Mounts[mountIndex].SeedContent = override.SeedContent
		service.PersistentStorageSeedFiles[seedFileIndex].SeedContent = override.SeedContent
	}

	return clonedServices, nil
}

func cloneComposeServicesForImport(services []sourceimport.ComposeService) []sourceimport.ComposeService {
	if len(services) == 0 {
		return nil
	}

	clonedServices := make([]sourceimport.ComposeService, len(services))
	for index, service := range services {
		cloned := service

		if service.PersistentStorage != nil {
			storage := *service.PersistentStorage
			storage.Mounts = append([]model.AppPersistentStorageMount(nil), service.PersistentStorage.Mounts...)
			cloned.PersistentStorage = &storage
		}

		cloned.PersistentStorageSeedFiles = append(
			[]sourceimport.PersistentStorageSeedFile(nil),
			service.PersistentStorageSeedFiles...,
		)
		clonedServices[index] = cloned
	}

	return clonedServices
}

func validateTopologyServiceEnvOverrides(services []sourceimport.ComposeService, serviceEnv map[string]map[string]string) error {
	if len(serviceEnv) == 0 {
		return nil
	}

	knownServices := make(map[string]struct{}, len(services))
	for _, service := range services {
		name := strings.TrimSpace(service.Name)
		if name == "" {
			continue
		}
		knownServices[name] = struct{}{}
	}
	for serviceName := range serviceEnv {
		serviceName = strings.TrimSpace(serviceName)
		if serviceName == "" {
			continue
		}
		if _, ok := knownServices[serviceName]; ok {
			continue
		}
		return fmt.Errorf("service_env references unknown service %q", serviceName)
	}
	return nil
}

func applyTopologyServicePersistentStorageOverrides(services []sourceimport.ComposeService, overrides map[string]model.ServicePersistentStorageOverride) ([]sourceimport.ComposeService, error) {
	if len(overrides) == 0 {
		return services, nil
	}

	clonedServices := cloneComposeServicesForImport(services)
	serviceIndexes := make(map[string]int, len(clonedServices))
	for index, service := range clonedServices {
		name := strings.TrimSpace(service.Name)
		if name == "" {
			continue
		}
		serviceIndexes[name] = index
	}

	for rawService, rawOverride := range overrides {
		serviceName := model.SlugifyOptional(strings.TrimSpace(rawService))
		if serviceName == "" {
			continue
		}
		serviceIndex, ok := serviceIndexes[serviceName]
		if !ok {
			return nil, fmt.Errorf("service_persistent_storage references unknown service %q", serviceName)
		}
		service := &clonedServices[serviceIndex]
		if service.PersistentStorage == nil {
			return nil, fmt.Errorf("service_persistent_storage references service %q without persistent storage", serviceName)
		}
		storageSize := strings.TrimSpace(rawOverride.StorageSize)
		if storageSize == "" {
			return nil, fmt.Errorf("service_persistent_storage.%s.storage_size is required", serviceName)
		}
		service.PersistentStorage.StorageSize = storageSize
	}

	return clonedServices, nil
}

func (s *Server) importResolvedUploadTopology(principal model.Principal, tenantID string, req importUploadRequest, upload model.SourceUpload, runtimeID string, replicas int, description string, baseName string, topology sourceimport.NormalizedTopology) (importedGitHubTopology, error) {
	return s.importResolvedTopology(principal, tenantID, topologyImportOptions{
		ProjectID:                req.ProjectID,
		RuntimeID:                runtimeID,
		Replicas:                 replicas,
		ServicePort:              req.ServicePort,
		Description:              description,
		BaseName:                 baseName,
		Env:                      req.Env,
		ServiceEnv:               normalizedImportServiceEnv(req.ServiceEnv),
		ServicePersistentStorage: normalizedImportServicePersistentStorage(req.ServicePersistentStorage),
		AuditAction:              "app.import_upload",
		BuildSource: func(service sourceimport.ComposeService, composeDependsOn []string) (model.AppSource, error) {
			return buildQueuedUploadComposeServiceSource(upload, service, composeDependsOn)
		},
		DesiredOriginSource: func(existing *model.App, source model.AppSource) *model.AppSource {
			if req.ReplaceSource || existing == nil {
				return model.CloneAppSource(&source)
			}
			return model.AppOriginSource(*existing)
		},
		BuildAuditMetadata: func(source model.AppSource, _ model.AppRoute) map[string]string {
			return map[string]string{
				"upload_id":      strings.TrimSpace(upload.ID),
				"archive_sha256": strings.TrimSpace(upload.SHA256),
			}
		},
		UpdateExisting: req.UpdateExisting,
		DeleteMissing:  req.DeleteMissing,
		DryRun:         req.DryRun,
		Family: topologyImportFamily{
			Kind: model.AppSourceTypeUpload,
			Key:  strings.TrimSpace(upload.Filename),
		},
	}, topology)
}

func (s *Server) importResolvedTopology(principal model.Principal, tenantID string, options topologyImportOptions, topology sourceimport.NormalizedTopology) (importedGitHubTopology, error) {
	if options.BuildSource == nil {
		return importedGitHubTopology{}, fmt.Errorf("topology source builder is required")
	}
	if err := validateTopologyServiceEnvOverrides(topology.Services, options.ServiceEnv); err != nil {
		return importedGitHubTopology{}, invalidComposeImport(err)
	}
	services, err := applyTopologyServicePersistentStorageOverrides(topology.Services, options.ServicePersistentStorage)
	if err != nil {
		return importedGitHubTopology{}, invalidComposeImport(err)
	}
	topology.Services = services

	topologyPlan, err := sourceimport.AnalyzeNormalizedTopology(topology, topology.PrimaryService)
	if err != nil {
		return importedGitHubTopology{}, invalidComposeImport(err)
	}

	projectName := strings.TrimSpace(options.ProjectName)
	if projectName == "" && strings.TrimSpace(options.ProjectID) != "" {
		if project, err := s.store.GetProject(options.ProjectID); err == nil {
			projectName = strings.TrimSpace(project.Name)
		}
	}
	if options.DeleteMissing && !options.UpdateExisting {
		return importedGitHubTopology{}, invalidComposeImportf("delete_missing requires update_existing")
	}
	projectApps, err := s.store.ListAppsMetadataByProjectIDs([]string{options.ProjectID})
	if err != nil {
		return importedGitHubTopology{}, err
	}
	existingMatches, familyApps, matchWarnings := matchTopologyExistingApps(projectApps, topologyPlan.Deployable, options.Family, options.UpdateExisting)

	appNames, routeHosts, routeWarnings, err := s.allocateComposeAppNamesWithExisting(tenantID, options.ProjectID, options.BaseName, topologyPlan.Deployable, topologyPlan.PrimaryService, existingMatches)
	if err != nil {
		return importedGitHubTopology{}, err
	}
	serviceHosts := make(map[string]string, len(appNames))
	for serviceName, appName := range appNames {
		aliasName := runtime.ComposeServiceAliasName(options.ProjectID, serviceName)
		if aliasName == "" {
			aliasName = appName
		}
		serviceHosts[serviceName] = aliasName
	}

	deployment := sourceimport.TopologyDeployment{
		ServiceHosts:           cloneStringMap(serviceHosts),
		ManagedPostgresByOwner: map[string]model.AppPostgresSpec{},
	}
	for _, backing := range topologyPlan.ManagedBackings {
		ownerAppName, ok := appNames[backing.OwnerService]
		if !ok {
			return importedGitHubTopology{}, invalidComposeImportf("managed backing service %q resolved to unknown owner %q", backing.Service.Name, backing.OwnerService)
		}
		spec, specErr := sourceimport.ManagedPostgresSpec(backing.Service, ownerAppName)
		if specErr != nil {
			return importedGitHubTopology{}, invalidComposeImport(specErr)
		}
		deployment.ManagedPostgresByOwner[backing.OwnerService] = spec
	}

	plans := make([]plannedTopologyService, 0, len(topologyPlan.Deployable))
	inferenceReport := append([]sourceimport.TopologyInference(nil), topologyPlan.InferenceReport...)
	for _, service := range topologyPlan.Deployable {
		suggestedEnv, envInferences, resolveErr := sourceimport.ResolveTopologyServiceEnvironment(topologyPlan, service.Name, deployment)
		if resolveErr != nil {
			return importedGitHubTopology{}, invalidComposeImport(resolveErr)
		}
		inferenceReport = append(inferenceReport, envInferences...)
		var route *model.AppRoute
		requestedPort := 0
		if host, ok := routeHosts[service.Name]; ok {
			if service.Name == topologyPlan.PrimaryService {
				requestedPort = options.ServicePort
			}
			route = &model.AppRoute{
				Hostname:    host,
				BaseDomain:  s.appBaseDomain,
				PublicURL:   "https://" + host,
				ServicePort: effectiveImportServicePort(requestedPort, service.InternalPort),
			}
		}

		source, err := options.BuildSource(service, composeDeployDependencies(topologyPlan, service.Name))
		if err != nil {
			return importedGitHubTopology{}, invalidComposeImport(err)
		}
		source = hydrateTopologySourceRevision(source, topology)

		var postgres *model.AppPostgresSpec
		if spec, ok := deployment.ManagedPostgresByOwner[service.Name]; ok {
			specCopy := spec
			postgres = &specCopy
		}
		suggestedEnv = mergeImportedEnv(suggestedEnv, options.Env)
		suggestedEnv = mergeImportedEnv(suggestedEnv, options.ServiceEnv[service.Name])

		spec, err := s.buildImportedAppSpec(
			service.BuildStrategy,
			appNames[service.Name],
			"",
			options.RuntimeID,
			options.Replicas,
			effectiveImportServicePort(requestedPort, service.InternalPort),
			"",
			nil,
			service.PersistentStorage,
			postgres,
			suggestedEnv,
		)
		if err != nil {
			return importedGitHubTopology{}, invalidComposeImport(err)
		}
		applyImportedNetworkMode(&spec, service.NetworkMode)
		if route != nil {
			route.ServicePort = firstServicePort(spec)
		}

		action := "create"
		var matchedApp *model.App
		if existing, ok := existingMatches[service.Name]; ok {
			action = "update"
			existingCopy := existing
			matchedApp = &existingCopy
		}
		plans = append(plans, plannedTopologyService{
			Service: service,
			AppName: appNames[service.Name],
			Route:   route,
			Source:  source,
			Spec:    spec,
			Action:  action,
			Match:   matchedApp,
		})
	}

	deleteCandidates := topologyDeleteCandidates(familyApps, topologyPlan.Deployable, existingMatches, options.DeleteMissing)
	planWarnings := append([]string(nil), topologyPlan.Warnings...)
	planWarnings = append(planWarnings, matchWarnings...)
	planWarnings = append(planWarnings, routeWarnings...)
	if !options.UpdateExisting && len(familyApps) > 0 {
		planWarnings = append(planWarnings, fmt.Sprintf("project %q already has %d imported service app(s) from the same topology family; this deploy will create a second copy unless you rerun with update_existing or replace", firstNonEmpty(strings.TrimSpace(projectName), strings.TrimSpace(options.ProjectID)), len(familyApps)))
	}
	if options.DeleteMissing && len(deleteCandidates) == 0 {
		planWarnings = append(planWarnings, "delete_missing was requested, but no previously managed services are eligible for deletion in this topology family")
	}
	deployPlan := buildTopologyDeployPlan(projectName, options, topologyPlan, plans, deleteCandidates, planWarnings)

	serviceDetails := make([]map[string]any, 0, len(plans))
	for _, planned := range plans {
		buildStrategy := strings.TrimSpace(planned.Service.BuildStrategy)
		if buildStrategy == "" && planned.Source.Type == model.AppSourceTypeDockerImage {
			buildStrategy = model.AppSourceTypeDockerImage
		}
		serviceInfo := map[string]any{
			"service":         planned.Service.Name,
			"kind":            planned.Service.Kind,
			"app_name":        planned.AppName,
			"build_strategy":  buildStrategy,
			"internal_port":   firstServicePort(planned.Spec),
			"compose_service": planned.Service.Name,
			"service_type":    strings.TrimSpace(planned.Service.ServiceType),
			"action":          planned.Action,
		}
		if planned.Match != nil {
			serviceInfo["existing_app_id"] = planned.Match.ID
			serviceInfo["existing_app_name"] = planned.Match.Name
			serviceInfo["app_id"] = planned.Match.ID
		}
		if planned.Route != nil && strings.TrimSpace(planned.Route.PublicURL) != "" {
			serviceInfo["public_url"] = planned.Route.PublicURL
		}
		if bindings := topologyPlan.BindingsBySource[planned.Service.Name]; len(bindings) > 0 {
			targets := make([]string, 0, len(bindings))
			for _, binding := range bindings {
				targets = append(targets, binding.Service)
			}
			sort.Strings(targets)
			serviceInfo["binding_targets"] = targets
		}
		if _, ok := deployment.ManagedPostgresByOwner[planned.Service.Name]; ok {
			serviceInfo["owns_postgres"] = true
		}
		serviceDetails = append(serviceDetails, serviceInfo)
	}

	if options.DryRun {
		return importedGitHubTopology{
			PrimaryService:  topologyPlan.PrimaryService,
			ServiceDetails:  serviceDetails,
			Warnings:        append([]string(nil), planWarnings...),
			InferenceReport: inferenceReport,
			Plan:            deployPlan,
		}, nil
	}

	apps := make([]model.App, 0, len(plans))
	ops := make([]model.Operation, 0, len(plans))
	appsByService := make(map[string]model.App, len(plans))
	opsByService := make(map[string]model.Operation, len(plans))
	createdApps := make([]model.App, 0, len(plans))
	rollback := func(baseErr error) error {
		if rollbackErr := s.rollbackImportedApps(createdApps); rollbackErr != nil {
			return errors.Join(baseErr, rollbackErr)
		}
		return baseErr
	}
	for _, plan := range plans {
		appDescription := options.Description
		if len(plans) > 1 {
			appDescription = fmt.Sprintf("%s (service %s)", options.Description, plan.Service.Name)
		}

		route := model.AppRoute{}
		if plan.Route != nil {
			route = *plan.Route
		}
		app := model.App{}
		if plan.Match != nil {
			app = *plan.Match
		} else {
			app, err = s.store.CreateImportedApp(tenantID, options.ProjectID, plan.AppName, appDescription, plan.Spec, plan.Source, route)
			if err != nil {
				if err == store.ErrConflict {
					return importedGitHubTopology{}, rollback(fmt.Errorf("topology import naming conflict for service %q: %w", plan.Service.Name, store.ErrConflict))
				}
				return importedGitHubTopology{}, rollback(err)
			}
			createdApps = append(createdApps, app)
		}
		apps = append(apps, app)

		specCopy := cloneAppSpec(plan.Spec)
		sourceCopy := plan.Source
		desiredOriginSource := model.CloneAppSource(&sourceCopy)
		if options.DesiredOriginSource != nil {
			desiredOriginSource = options.DesiredOriginSource(plan.Match, sourceCopy)
		}
		op, err := s.store.CreateOperation(model.Operation{
			TenantID:            app.TenantID,
			Type:                model.OperationTypeImport,
			RequestedByType:     principal.ActorType,
			RequestedByID:       principal.ActorID,
			AppID:               app.ID,
			DesiredSpec:         &specCopy,
			DesiredSource:       &sourceCopy,
			DesiredOriginSource: desiredOriginSource,
		})
		if err != nil {
			return importedGitHubTopology{}, rollback(err)
		}

		auditMetadata := map[string]string{
			"compose_service": sourceCopy.ComposeService,
			"hostname":        route.Hostname,
			"action":          plan.Action,
		}
		if strings.TrimSpace(sourceCopy.BuildStrategy) != "" {
			auditMetadata["build_strategy"] = sourceCopy.BuildStrategy
		}
		if strings.TrimSpace(sourceCopy.ImageRef) != "" {
			auditMetadata["image_ref"] = sourceCopy.ImageRef
		}
		if options.BuildAuditMetadata != nil {
			for key, value := range options.BuildAuditMetadata(sourceCopy, route) {
				if strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" {
					continue
				}
				auditMetadata[key] = value
			}
		}
		if strings.TrimSpace(options.AuditAction) != "" {
			s.appendAudit(principal, options.AuditAction, "app", app.ID, app.TenantID, auditMetadata)
		}

		ops = append(ops, op)
		appsByService[plan.Service.Name] = app
		opsByService[plan.Service.Name] = op
	}

	for index, planned := range plans {
		if app, ok := appsByService[planned.Service.Name]; ok {
			serviceDetails[index]["app_id"] = app.ID
			serviceDetails[index]["app_name"] = app.Name
			if app.Route != nil && strings.TrimSpace(app.Route.PublicURL) != "" {
				serviceDetails[index]["public_url"] = app.Route.PublicURL
			}
			serviceDetails[index]["internal_port"] = firstServicePort(app.Spec)
		}
		if op, ok := opsByService[planned.Service.Name]; ok {
			serviceDetails[index]["operation_id"] = op.ID
		}
	}
	for _, candidate := range deleteCandidates {
		deleteOp, err := s.store.CreateOperation(model.Operation{
			TenantID:        candidate.TenantID,
			Type:            model.OperationTypeDelete,
			RequestedByType: principal.ActorType,
			RequestedByID:   principal.ActorID,
			AppID:           candidate.ID,
		})
		if err != nil {
			if errors.Is(err, store.ErrConflict) || errors.Is(err, store.ErrNotFound) {
				continue
			}
			return importedGitHubTopology{}, rollback(err)
		}
		ops = append(ops, deleteOp)
	}
	if deployPlan != nil {
		for index, servicePlan := range deployPlan.Services {
			if app, ok := appsByService[servicePlan.Service]; ok {
				deployPlan.Services[index].AppID = app.ID
				deployPlan.Services[index].AppName = app.Name
				if app.Route != nil {
					deployPlan.Services[index].Hostname = firstNonEmpty(strings.TrimSpace(app.Route.Hostname), deployPlan.Services[index].Hostname)
					deployPlan.Services[index].PublicURL = firstNonEmpty(strings.TrimSpace(app.Route.PublicURL), deployPlan.Services[index].PublicURL)
				}
			}
		}
	}

	return importedGitHubTopology{
		PrimaryApp:      appsByService[topologyPlan.PrimaryService],
		PrimaryOp:       opsByService[topologyPlan.PrimaryService],
		Apps:            apps,
		Operations:      ops,
		PrimaryService:  topologyPlan.PrimaryService,
		ServiceDetails:  serviceDetails,
		Warnings:        append([]string(nil), planWarnings...),
		InferenceReport: inferenceReport,
		Plan:            deployPlan,
	}, nil
}

func (s *Server) allocateComposeAppNamesWithExisting(tenantID, projectID, baseName string, services []sourceimport.ComposeService, primaryService string, existingByService map[string]model.App) (map[string]string, map[string]string, []string, error) {
	apps, err := s.store.ListApps(tenantID, false)
	if err != nil {
		return nil, nil, nil, err
	}
	fixedAppIDs := make(map[string]struct{}, len(existingByService))
	warnings := make([]string, 0, len(existingByService))
	for serviceName, app := range existingByService {
		fixedAppIDs[strings.TrimSpace(app.ID)] = struct{}{}
		if !servicePublishedByName(services, serviceName) && app.Route != nil && strings.TrimSpace(app.Route.Hostname) != "" {
			warnings = append(warnings, fmt.Sprintf("service %q keeps its existing public route %q during update_existing; route removal still requires an explicit route/domain change or delete/recreate", serviceName, app.Route.Hostname))
		}
		if servicePublishedByName(services, serviceName) && (app.Route == nil || strings.TrimSpace(app.Route.Hostname) == "") {
			warnings = append(warnings, fmt.Sprintf("service %q is published in the topology, but existing app %q has no current route; update_existing preserves the current route state", serviceName, app.Name))
		}
	}

	for attempt := 0; attempt < defaultComposeImportNamingStrategy.MaxAttempts; attempt++ {
		primaryName, primaryHost := buildImportIdentity(baseName, s.appBaseDomain, attempt)
		if s.isReservedAppHostname(primaryHost) {
			continue
		}

		names := make(map[string]string, len(services))
		usedNames := make(map[string]struct{}, len(services))
		conflict := false
		for _, service := range services {
			if existing, ok := existingByService[service.Name]; ok {
				name := strings.TrimSpace(existing.Name)
				if name == "" {
					conflict = true
					break
				}
				if _, exists := usedNames[name]; exists {
					conflict = true
					break
				}
				names[service.Name] = name
				usedNames[name] = struct{}{}
				continue
			}
			name := primaryName
			if service.Name != primaryService {
				name = truncateSlug(primaryName+"-"+service.Name, defaultComposeImportNamingStrategy.MaxServiceNameLen)
			}
			if name == "" {
				conflict = true
				break
			}
			if _, exists := usedNames[name]; exists {
				conflict = true
				break
			}
			names[service.Name] = name
			usedNames[name] = struct{}{}
		}
		if conflict {
			continue
		}

		routeHosts := make(map[string]string)
		usedHosts := make(map[string]struct{})
		for _, service := range services {
			if !service.Published {
				continue
			}
			if existing, ok := existingByService[service.Name]; ok {
				if existing.Route != nil {
					host := strings.TrimSpace(strings.ToLower(existing.Route.Hostname))
					if host != "" {
						if _, exists := usedHosts[host]; exists {
							conflict = true
							break
						}
						routeHosts[service.Name] = host
						usedHosts[host] = struct{}{}
					}
				}
				continue
			}
			host := primaryHost
			if service.Name != primaryService {
				host = names[service.Name] + "." + s.appBaseDomain
			}
			host = strings.TrimSpace(strings.ToLower(host))
			if host == "" || s.isReservedAppHostname(host) {
				conflict = true
				break
			}
			if _, exists := usedHosts[host]; exists {
				conflict = true
				break
			}
			routeHosts[service.Name] = host
			usedHosts[host] = struct{}{}
		}
		if conflict {
			continue
		}

		hostConflict := false
		for _, existing := range apps {
			if _, ok := fixedAppIDs[strings.TrimSpace(existing.ID)]; ok {
				continue
			}
			if existing.ProjectID != projectID {
				continue
			}
			for _, planned := range names {
				if strings.EqualFold(strings.TrimSpace(existing.Name), planned) {
					hostConflict = true
					break
				}
			}
			if hostConflict {
				break
			}
		}
		if hostConflict {
			continue
		}
		for _, existing := range apps {
			if _, ok := fixedAppIDs[strings.TrimSpace(existing.ID)]; ok {
				continue
			}
			if existing.Route == nil {
				continue
			}
			existingHost := strings.TrimSpace(strings.ToLower(existing.Route.Hostname))
			for _, plannedHost := range routeHosts {
				if strings.EqualFold(existingHost, plannedHost) {
					hostConflict = true
					break
				}
			}
			if hostConflict {
				break
			}
		}
		if hostConflict {
			continue
		}
		for _, plannedHost := range routeHosts {
			if _, err := s.store.GetAppByHostname(plannedHost); err == nil {
				occupiedByFixed := false
				for _, existing := range existingByService {
					if existing.Route != nil && strings.EqualFold(strings.TrimSpace(existing.Route.Hostname), plannedHost) {
						occupiedByFixed = true
						break
					}
				}
				if occupiedByFixed {
					continue
				}
				hostConflict = true
				break
			} else if err != nil && !errors.Is(err, store.ErrNotFound) {
				return nil, nil, nil, err
			}
		}
		if hostConflict {
			continue
		}
		return names, routeHosts, warnings, nil
	}

	return nil, nil, warnings, store.ErrConflict
}

func matchTopologyExistingApps(projectApps []model.App, services []sourceimport.ComposeService, family topologyImportFamily, updateExisting bool) (map[string]model.App, map[string]model.App, []string) {
	candidatesByService := make(map[string][]model.App)
	familyApps := make(map[string]model.App)
	for _, app := range projectApps {
		if !topologyImportFamilyMatchesApp(app, family) || topologyAppIsDeleting(app) {
			continue
		}
		source := model.AppOriginSource(app)
		if source == nil {
			continue
		}
		service := strings.TrimSpace(source.ComposeService)
		if service == "" {
			continue
		}
		candidatesByService[service] = append(candidatesByService[service], app)
		familyApps[strings.TrimSpace(app.ID)] = app
	}

	warnings := make([]string, 0)
	matches := make(map[string]model.App)
	if !updateExisting {
		return matches, familyApps, warnings
	}
	for serviceName := range serviceNameSet(services) {
		candidates := candidatesByService[serviceName]
		if len(candidates) == 0 {
			continue
		}
		sort.Slice(candidates, func(i, j int) bool {
			if candidates[i].UpdatedAt.Equal(candidates[j].UpdatedAt) {
				return candidates[i].CreatedAt.After(candidates[j].CreatedAt)
			}
			return candidates[i].UpdatedAt.After(candidates[j].UpdatedAt)
		})
		matches[serviceName] = candidates[0]
		if len(candidates) > 1 {
			warnings = append(warnings, fmt.Sprintf("service %q matched %d existing apps in the project; update_existing will reuse the most recently updated app %q", serviceName, len(candidates), candidates[0].Name))
		}
	}
	return matches, familyApps, warnings
}

func topologyDeleteCandidates(familyApps map[string]model.App, services []sourceimport.ComposeService, matched map[string]model.App, enabled bool) []model.App {
	if !enabled || len(familyApps) == 0 {
		return nil
	}
	deployableServices := serviceNameSet(services)
	matchedIDs := make(map[string]struct{}, len(matched))
	for _, app := range matched {
		matchedIDs[strings.TrimSpace(app.ID)] = struct{}{}
	}
	candidates := make([]model.App, 0)
	for _, app := range familyApps {
		if _, ok := matchedIDs[strings.TrimSpace(app.ID)]; ok {
			continue
		}
		source := model.AppOriginSource(app)
		service := ""
		if source != nil {
			service = strings.TrimSpace(source.ComposeService)
		}
		if service != "" {
			if _, ok := deployableServices[service]; ok {
				continue
			}
		}
		candidates = append(candidates, app)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Name == candidates[j].Name {
			return candidates[i].ID < candidates[j].ID
		}
		return candidates[i].Name < candidates[j].Name
	})
	return candidates
}

func buildTopologyDeployPlan(projectName string, options topologyImportOptions, topologyPlan sourceimport.TopologyPlan, services []plannedTopologyService, deleteCandidates []model.App, warnings []string) *model.TopologyDeployPlan {
	plan := &model.TopologyDeployPlan{
		Mode:           "create",
		DryRun:         options.DryRun,
		UpdateExisting: options.UpdateExisting,
		DeleteMissing:  options.DeleteMissing,
		ProjectID:      strings.TrimSpace(options.ProjectID),
		ProjectName:    strings.TrimSpace(projectName),
		PrimaryService: strings.TrimSpace(topologyPlan.PrimaryService),
		Warnings:       append([]string(nil), warnings...),
	}
	if options.UpdateExisting {
		plan.Mode = "update-existing"
	}
	for _, service := range services {
		buildStrategy := strings.TrimSpace(service.Service.BuildStrategy)
		if buildStrategy == "" && service.Source.Type == model.AppSourceTypeDockerImage {
			buildStrategy = model.AppSourceTypeDockerImage
		}
		entry := model.TopologyDeployPlanService{
			Service:        strings.TrimSpace(service.Service.Name),
			Kind:           strings.TrimSpace(service.Service.Kind),
			ServiceType:    strings.TrimSpace(service.Service.ServiceType),
			ComposeService: strings.TrimSpace(service.Source.ComposeService),
			BuildStrategy:  buildStrategy,
			Action:         strings.TrimSpace(service.Action),
			AppName:        strings.TrimSpace(service.AppName),
			InternalPort:   firstServicePort(service.Spec),
		}
		if service.Match != nil {
			entry.ExistingAppID = strings.TrimSpace(service.Match.ID)
			entry.ExistingAppName = strings.TrimSpace(service.Match.Name)
			entry.AppID = entry.ExistingAppID
			entry.AppName = firstNonEmpty(strings.TrimSpace(service.Match.Name), entry.AppName)
			if service.Match.Route != nil {
				entry.Hostname = strings.TrimSpace(service.Match.Route.Hostname)
				entry.PublicURL = strings.TrimSpace(service.Match.Route.PublicURL)
			}
		}
		if service.Route != nil {
			entry.Hostname = firstNonEmpty(strings.TrimSpace(entry.Hostname), strings.TrimSpace(service.Route.Hostname))
			entry.PublicURL = firstNonEmpty(strings.TrimSpace(entry.PublicURL), strings.TrimSpace(service.Route.PublicURL))
		}
		plan.Services = append(plan.Services, entry)
	}
	for _, app := range deleteCandidates {
		source := model.AppOriginSource(app)
		service := ""
		if source != nil {
			service = strings.TrimSpace(source.ComposeService)
		}
		plan.DeleteCandidates = append(plan.DeleteCandidates, model.TopologyDeployDeleteTarget{
			AppID:   strings.TrimSpace(app.ID),
			AppName: strings.TrimSpace(app.Name),
			Service: service,
			Reason:  "service is no longer present in the imported topology",
		})
	}
	return plan
}

func serviceNameSet(services []sourceimport.ComposeService) map[string]struct{} {
	out := make(map[string]struct{}, len(services))
	for _, service := range services {
		name := strings.TrimSpace(service.Name)
		if name == "" {
			continue
		}
		out[name] = struct{}{}
	}
	return out
}

func servicePublishedByName(services []sourceimport.ComposeService, name string) bool {
	name = strings.TrimSpace(name)
	for _, service := range services {
		if strings.EqualFold(strings.TrimSpace(service.Name), name) {
			return service.Published
		}
	}
	return false
}

func topologyImportFamilyMatchesApp(app model.App, family topologyImportFamily) bool {
	source := model.AppOriginSource(app)
	if source == nil {
		return false
	}
	switch strings.TrimSpace(family.Kind) {
	case model.AppSourceTypeGitHubPublic, model.AppSourceTypeGitHubPrivate, "github":
		kind := strings.TrimSpace(source.Type)
		if kind != model.AppSourceTypeGitHubPublic && kind != model.AppSourceTypeGitHubPrivate {
			return false
		}
		return strings.EqualFold(normalizeTopologyGitHubFamilyKey(source.RepoURL), normalizeTopologyGitHubFamilyKey(family.Key))
	case model.AppSourceTypeUpload:
		return strings.EqualFold(strings.TrimSpace(source.Type), model.AppSourceTypeUpload) &&
			strings.EqualFold(strings.TrimSpace(source.UploadFilename), strings.TrimSpace(family.Key))
	default:
		return false
	}
}

func normalizeTopologyGitHubFamilyKey(raw string) string {
	raw = strings.TrimSpace(strings.TrimSuffix(raw, ".git"))
	lower := strings.ToLower(raw)
	lower = strings.TrimPrefix(lower, "https://")
	lower = strings.TrimPrefix(lower, "http://")
	lower = strings.TrimPrefix(lower, "ssh://")
	lower = strings.TrimPrefix(lower, "git@github.com:")
	lower = strings.TrimPrefix(lower, "github.com/")
	parts := strings.Split(strings.Trim(lower, "/"), "/")
	if len(parts) >= 2 {
		return strings.Join(parts[len(parts)-2:], "/")
	}
	return strings.Trim(lower, "/")
}

func topologyAppIsDeleting(app model.App) bool {
	phase := strings.TrimSpace(strings.ToLower(app.Status.Phase))
	return phase != "" && strings.Contains(phase, "deleting")
}

func resolveTopologyPrimaryService(services []sourceimport.ComposeService, preferred string) (sourceimport.ComposeService, error) {
	return sourceimport.SelectPrimaryTopologyService(services, preferred)
}

func composeDeployDependencies(plan sourceimport.TopologyPlan, serviceName string) []string {
	deployable := make(map[string]struct{}, len(plan.Deployable))
	for _, service := range plan.Deployable {
		name := strings.TrimSpace(service.Name)
		if name == "" {
			continue
		}
		deployable[name] = struct{}{}
	}

	serviceName = strings.TrimSpace(serviceName)
	dependencies := make([]string, 0, len(plan.BindingsBySource[serviceName]))
	seen := make(map[string]struct{}, len(plan.BindingsBySource[serviceName]))
	for _, binding := range plan.BindingsBySource[serviceName] {
		switch strings.TrimSpace(binding.Source) {
		case "", sourceimport.BindingSourceExplicit, sourceimport.BindingSourceDependsOn:
		default:
			continue
		}
		dependency := strings.TrimSpace(binding.Service)
		if dependency == "" || dependency == serviceName {
			continue
		}
		if _, ok := deployable[dependency]; !ok {
			continue
		}
		if _, ok := seen[dependency]; ok {
			continue
		}
		seen[dependency] = struct{}{}
		dependencies = append(dependencies, dependency)
	}
	sort.Strings(dependencies)
	if len(dependencies) == 0 {
		return nil
	}
	return dependencies
}

func hydrateTopologySourceRevision(source model.AppSource, topology sourceimport.NormalizedTopology) model.AppSource {
	if !model.IsGitHubAppSourceType(source.Type) {
		return source
	}
	if strings.TrimSpace(source.CommitSHA) == "" {
		source.CommitSHA = strings.TrimSpace(topology.CommitSHA)
	}
	if strings.TrimSpace(source.CommitCommittedAt) == "" {
		source.CommitCommittedAt = strings.TrimSpace(topology.CommitCommittedAt)
	}
	return source
}

func buildQueuedComposeServiceSource(req importGitHubRequest, service sourceimport.ComposeService, composeDependsOn []string) (model.AppSource, error) {
	var (
		source model.AppSource
		err    error
	)
	if strings.TrimSpace(service.Image) != "" && strings.TrimSpace(service.BuildStrategy) == "" {
		source, err = buildQueuedImageSource(service.Image, service.Name, service.Name)
	} else {
		source, err = buildQueuedGitHubSource(
			req.RepoURL,
			req.RepoVisibility,
			req.RepoAuthToken,
			req.Branch,
			service.SourceDir,
			service.DockerfilePath,
			service.BuildContextDir,
			service.BuildStrategy,
			service.Name,
			service.Name,
		)
	}
	if err != nil {
		return model.AppSource{}, err
	}
	if len(composeDependsOn) > 0 {
		source.ComposeDependsOn = append([]string(nil), composeDependsOn...)
	}
	return source, nil
}

func buildQueuedUploadComposeServiceSource(upload model.SourceUpload, service sourceimport.ComposeService, composeDependsOn []string) (model.AppSource, error) {
	var (
		source model.AppSource
		err    error
	)
	if strings.TrimSpace(service.Image) != "" && strings.TrimSpace(service.BuildStrategy) == "" {
		source, err = buildQueuedImageSource(service.Image, service.Name, service.Name)
	} else {
		source, err = buildQueuedUploadSource(
			upload,
			service.SourceDir,
			service.DockerfilePath,
			service.BuildContextDir,
			service.BuildStrategy,
			service.Name,
			service.Name,
		)
	}
	if err != nil {
		return model.AppSource{}, err
	}
	if len(composeDependsOn) > 0 {
		source.ComposeDependsOn = append([]string(nil), composeDependsOn...)
	}
	return source, nil
}
