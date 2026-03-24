package api

import (
	"fmt"
	"strings"

	"fugue/internal/model"
	"fugue/internal/sourceimport"
	"fugue/internal/store"
)

type importedGitHubTopology struct {
	PrimaryApp     model.App
	PrimaryOp      model.Operation
	Apps           []model.App
	Operations     []model.Operation
	PrimaryService string
	ServiceDetails []map[string]any
	Warnings       []string
}

func (s *Server) importResolvedGitHubTopology(principal model.Principal, tenantID string, req importGitHubRequest, runtimeID string, replicas int, description string, baseName string, services []sourceimport.ComposeService, preferredPrimary string, warnings []string) (importedGitHubTopology, error) {
	appServices, postgresServices := splitComposeServices(services)
	if len(appServices) == 0 {
		return importedGitHubTopology{}, invalidComposeImportf("topology does not define any buildable application services")
	}
	if err := validateComposeDependencies(appServices, postgresServices); err != nil {
		return importedGitHubTopology{}, invalidComposeImport(err)
	}

	primaryService, err := resolveTopologyPrimaryService(appServices, preferredPrimary)
	if err != nil {
		return importedGitHubTopology{}, invalidComposeImport(err)
	}

	appNames, primaryHost, err := s.allocateComposeAppNames(tenantID, req.ProjectID, baseName, appServices, primaryService.Name)
	if err != nil {
		return importedGitHubTopology{}, err
	}

	postgresByOwner, postgresHosts, postgresWarnings, err := buildComposePostgresPlan(appServices, postgresServices, appNames)
	if err != nil {
		return importedGitHubTopology{}, invalidComposeImport(err)
	}

	serviceHosts := make(map[string]string, len(appNames)+len(postgresHosts))
	for serviceName, appName := range appNames {
		serviceHosts[serviceName] = appName
	}
	for serviceName, host := range postgresHosts {
		serviceHosts[serviceName] = host
	}

	plans := make([]composeAppPlan, 0, len(appServices))
	sortedServices := orderComposeServicesForCreation(appServices, primaryService.Name)
	for _, service := range sortedServices {
		suggestedEnv := rewriteComposeEnvironment(service.Environment, serviceHosts)
		var route *model.AppRoute
		requestedPort := 0
		if service.Name == primaryService.Name {
			requestedPort = req.ServicePort
			route = &model.AppRoute{
				Hostname:    primaryHost,
				BaseDomain:  s.appBaseDomain,
				PublicURL:   "https://" + primaryHost,
				ServicePort: effectiveImportServicePort(requestedPort, service.InternalPort),
			}
		}

		source, err := buildQueuedGitHubSource(
			req.RepoURL,
			req.Branch,
			service.SourceDir,
			service.DockerfilePath,
			service.BuildContextDir,
			service.BuildStrategy,
			service.Name,
			service.Name,
		)
		if err != nil {
			return importedGitHubTopology{}, invalidComposeImport(err)
		}

		var postgres *model.AppPostgresSpec
		if spec, ok := postgresByOwner[service.Name]; ok {
			specCopy := spec
			postgres = &specCopy
			suggestedEnv = applyManagedPostgresEnvironment(suggestedEnv, specCopy)
		}

		spec, err := s.buildImportedAppSpec(
			service.BuildStrategy,
			appNames[service.Name],
			"",
			runtimeID,
			replicas,
			effectiveImportServicePort(requestedPort, service.InternalPort),
			"",
			nil,
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
	for _, plan := range plans {
		appDescription := description
		if len(plans) > 1 {
			appDescription = fmt.Sprintf("%s (service %s)", description, plan.Service.Name)
		}

		route := model.AppRoute{}
		if plan.Route != nil {
			route = *plan.Route
		}
		app, err := s.store.CreateImportedApp(tenantID, req.ProjectID, plan.AppName, appDescription, plan.Spec, plan.Source, route)
		if err != nil {
			if err == store.ErrConflict {
				return importedGitHubTopology{}, fmt.Errorf("topology import naming conflict for service %q: %w", plan.Service.Name, store.ErrConflict)
			}
			return importedGitHubTopology{}, err
		}

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
			return importedGitHubTopology{}, err
		}

		s.appendAudit(principal, "app.import_github", "app", app.ID, app.TenantID, map[string]string{
			"repo_url":        sourceCopy.RepoURL,
			"build_strategy":  sourceCopy.BuildStrategy,
			"compose_service": sourceCopy.ComposeService,
			"hostname":        route.Hostname,
		})

		apps = append(apps, app)
		ops = append(ops, op)
		appsByService[plan.Service.Name] = app
		opsByService[plan.Service.Name] = op
	}

	serviceDetails := make([]map[string]any, 0, len(plans))
	for _, plan := range plans {
		app := appsByService[plan.Service.Name]
		op := opsByService[plan.Service.Name]
		serviceInfo := map[string]any{
			"service":         plan.Service.Name,
			"kind":            plan.Service.Kind,
			"app_id":          app.ID,
			"app_name":        app.Name,
			"operation_id":    op.ID,
			"build_strategy":  plan.Service.BuildStrategy,
			"internal_port":   firstServicePort(app.Spec),
			"compose_service": plan.Service.Name,
		}
		if app.Route != nil && strings.TrimSpace(app.Route.PublicURL) != "" {
			serviceInfo["public_url"] = app.Route.PublicURL
		}
		if _, ok := postgresByOwner[plan.Service.Name]; ok {
			serviceInfo["owns_postgres"] = true
		}
		serviceDetails = append(serviceDetails, serviceInfo)
	}

	return importedGitHubTopology{
		PrimaryApp:     appsByService[primaryService.Name],
		PrimaryOp:      opsByService[primaryService.Name],
		Apps:           apps,
		Operations:     ops,
		PrimaryService: primaryService.Name,
		ServiceDetails: serviceDetails,
		Warnings:       append(append([]string(nil), warnings...), postgresWarnings...),
	}, nil
}

func resolveTopologyPrimaryService(services []sourceimport.ComposeService, preferred string) (sourceimport.ComposeService, error) {
	preferred = strings.TrimSpace(preferred)
	if preferred == "" {
		return pickPrimaryComposeService(services), nil
	}
	preferred = model.Slugify(preferred)
	for _, service := range services {
		if service.Name == preferred {
			return service, nil
		}
	}
	return sourceimport.ComposeService{}, fmt.Errorf("primary service %q does not exist", preferred)
}
