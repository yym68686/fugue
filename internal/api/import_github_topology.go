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
}

type topologySourceBuilder func(service sourceimport.ComposeService, composeDependsOn []string) (model.AppSource, error)

type topologyAuditMetadataBuilder func(source model.AppSource, route model.AppRoute) map[string]string

type topologyImportOptions struct {
	ProjectID          string
	RuntimeID          string
	Replicas           int
	ServicePort        int
	Description        string
	BaseName           string
	Env                map[string]string
	AuditAction        string
	BuildSource        topologySourceBuilder
	BuildAuditMetadata topologyAuditMetadataBuilder
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
		ProjectID:   req.ProjectID,
		RuntimeID:   runtimeID,
		Replicas:    replicas,
		ServicePort: req.ServicePort,
		Description: description,
		BaseName:    baseName,
		Env:         req.Env,
		AuditAction: "app.import_github",
		BuildSource: func(service sourceimport.ComposeService, composeDependsOn []string) (model.AppSource, error) {
			return buildQueuedComposeServiceSource(req, service, composeDependsOn)
		},
		BuildAuditMetadata: func(source model.AppSource, _ model.AppRoute) map[string]string {
			return map[string]string{
				"repo_url": strings.TrimSpace(req.RepoURL),
			}
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

func (s *Server) importResolvedUploadTopology(principal model.Principal, tenantID string, req importUploadRequest, upload model.SourceUpload, runtimeID string, replicas int, description string, baseName string, topology sourceimport.NormalizedTopology) (importedGitHubTopology, error) {
	return s.importResolvedTopology(principal, tenantID, topologyImportOptions{
		ProjectID:   req.ProjectID,
		RuntimeID:   runtimeID,
		Replicas:    replicas,
		ServicePort: req.ServicePort,
		Description: description,
		BaseName:    baseName,
		Env:         req.Env,
		AuditAction: "app.import_upload",
		BuildSource: func(service sourceimport.ComposeService, composeDependsOn []string) (model.AppSource, error) {
			return buildQueuedUploadComposeServiceSource(upload, service, composeDependsOn)
		},
		BuildAuditMetadata: func(source model.AppSource, _ model.AppRoute) map[string]string {
			return map[string]string{
				"upload_id":      strings.TrimSpace(upload.ID),
				"archive_sha256": strings.TrimSpace(upload.SHA256),
			}
		},
	}, topology)
}

func (s *Server) importResolvedTopology(principal model.Principal, tenantID string, options topologyImportOptions, topology sourceimport.NormalizedTopology) (importedGitHubTopology, error) {
	if options.BuildSource == nil {
		return importedGitHubTopology{}, fmt.Errorf("topology source builder is required")
	}

	topologyPlan, err := sourceimport.AnalyzeNormalizedTopology(topology, topology.PrimaryService)
	if err != nil {
		return importedGitHubTopology{}, invalidComposeImport(err)
	}

	appNames, primaryHost, err := s.allocateComposeAppNames(tenantID, options.ProjectID, options.BaseName, topologyPlan.Deployable, topologyPlan.PrimaryService)
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

	plans := make([]composeAppPlan, 0, len(topologyPlan.Deployable))
	inferenceReport := append([]sourceimport.TopologyInference(nil), topologyPlan.InferenceReport...)
	for _, service := range topologyPlan.Deployable {
		suggestedEnv, envInferences, resolveErr := sourceimport.ResolveTopologyServiceEnvironment(topologyPlan, service.Name, deployment)
		if resolveErr != nil {
			return importedGitHubTopology{}, invalidComposeImport(resolveErr)
		}
		inferenceReport = append(inferenceReport, envInferences...)
		var route *model.AppRoute
		requestedPort := 0
		if service.Name == topologyPlan.PrimaryService {
			requestedPort = options.ServicePort
			route = &model.AppRoute{
				Hostname:    primaryHost,
				BaseDomain:  s.appBaseDomain,
				PublicURL:   "https://" + primaryHost,
				ServicePort: effectiveImportServicePort(requestedPort, service.InternalPort),
			}
		}

		source, err := options.BuildSource(service, composeDeployDependencies(topologyPlan, service.Name))
		if err != nil {
			return importedGitHubTopology{}, invalidComposeImport(err)
		}

		var postgres *model.AppPostgresSpec
		if spec, ok := deployment.ManagedPostgresByOwner[service.Name]; ok {
			specCopy := spec
			postgres = &specCopy
		}
		suggestedEnv = mergeImportedEnv(suggestedEnv, options.Env)

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
		if route != nil {
			route.ServicePort = firstServicePort(spec)
		}

		plans = append(plans, composeAppPlan{
			Service: service,
			AppName: appNames[service.Name],
			Route:   route,
			Source:  source,
			Spec:    spec,
		})
	}

	apps := make([]model.App, 0, len(plans))
	ops := make([]model.Operation, 0, len(plans))
	appsByService := make(map[string]model.App, len(plans))
	opsByService := make(map[string]model.Operation, len(plans))
	rollback := func(baseErr error) error {
		if rollbackErr := s.rollbackImportedApps(apps); rollbackErr != nil {
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
		app, err := s.store.CreateImportedApp(tenantID, options.ProjectID, plan.AppName, appDescription, plan.Spec, plan.Source, route)
		if err != nil {
			if err == store.ErrConflict {
				return importedGitHubTopology{}, rollback(fmt.Errorf("topology import naming conflict for service %q: %w", plan.Service.Name, store.ErrConflict))
			}
			return importedGitHubTopology{}, rollback(err)
		}
		apps = append(apps, app)

		specCopy := cloneAppSpec(app.Spec)
		sourceCopy := plan.Source
		op, err := s.store.CreateOperation(model.Operation{
			TenantID:        app.TenantID,
			Type:            model.OperationTypeImport,
			RequestedByType: principal.ActorType,
			RequestedByID:   principal.ActorID,
			AppID:           app.ID,
			DesiredSpec:     &specCopy,
			DesiredSource:   &sourceCopy,
		})
		if err != nil {
			return importedGitHubTopology{}, rollback(err)
		}

		auditMetadata := map[string]string{
			"compose_service": sourceCopy.ComposeService,
			"hostname":        route.Hostname,
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

	serviceDetails := make([]map[string]any, 0, len(plans))
	for _, appPlan := range plans {
		app := appsByService[appPlan.Service.Name]
		op := opsByService[appPlan.Service.Name]
		buildStrategy := strings.TrimSpace(appPlan.Service.BuildStrategy)
		if buildStrategy == "" && appPlan.Source.Type == model.AppSourceTypeDockerImage {
			buildStrategy = model.AppSourceTypeDockerImage
		}
		serviceInfo := map[string]any{
			"service":         appPlan.Service.Name,
			"kind":            appPlan.Service.Kind,
			"app_id":          app.ID,
			"app_name":        app.Name,
			"operation_id":    op.ID,
			"build_strategy":  buildStrategy,
			"internal_port":   firstServicePort(app.Spec),
			"compose_service": appPlan.Service.Name,
		}
		if app.Route != nil && strings.TrimSpace(app.Route.PublicURL) != "" {
			serviceInfo["public_url"] = app.Route.PublicURL
		}
		serviceInfo["service_type"] = strings.TrimSpace(appPlan.Service.ServiceType)
		if bindings := topologyPlan.BindingsBySource[appPlan.Service.Name]; len(bindings) > 0 {
			targets := make([]string, 0, len(bindings))
			for _, binding := range bindings {
				targets = append(targets, binding.Service)
			}
			sort.Strings(targets)
			serviceInfo["binding_targets"] = targets
		}
		if _, ok := deployment.ManagedPostgresByOwner[appPlan.Service.Name]; ok {
			serviceInfo["owns_postgres"] = true
		}
		serviceDetails = append(serviceDetails, serviceInfo)
	}

	return importedGitHubTopology{
		PrimaryApp:      appsByService[topologyPlan.PrimaryService],
		PrimaryOp:       opsByService[topologyPlan.PrimaryService],
		Apps:            apps,
		Operations:      ops,
		PrimaryService:  topologyPlan.PrimaryService,
		ServiceDetails:  serviceDetails,
		Warnings:        append([]string(nil), topologyPlan.Warnings...),
		InferenceReport: inferenceReport,
	}, nil
}

func (s *Server) allocateComposeAppNames(tenantID, projectID, baseName string, services []sourceimport.ComposeService, primaryService string) (map[string]string, string, error) {
	apps, err := s.store.ListApps(tenantID, false)
	if err != nil {
		return nil, "", err
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

		hostConflict := false
		for _, existing := range apps {
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
			if existing.Route == nil {
				continue
			}
			if strings.EqualFold(strings.TrimSpace(existing.Route.Hostname), primaryHost) {
				hostConflict = true
				break
			}
		}
		if hostConflict {
			continue
		}
		if _, err := s.store.GetAppByHostname(primaryHost); err == nil {
			continue
		} else if err != nil && !errors.Is(err, store.ErrNotFound) {
			return nil, "", err
		}
		return names, primaryHost, nil
	}

	return nil, "", store.ErrConflict
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
