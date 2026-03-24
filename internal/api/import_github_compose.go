package api

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strings"

	"fugue/internal/model"
	"fugue/internal/sourceimport"
	"fugue/internal/store"
)

var errInvalidComposeImport = errors.New("invalid compose import")

type composeAppPlan struct {
	Service sourceimport.ComposeService
	AppName string
	Route   *model.AppRoute
	Source  model.AppSource
	Spec    model.AppSpec
}

func invalidComposeImportf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errInvalidComposeImport, fmt.Sprintf(format, args...))
}

func invalidComposeImport(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %v", errInvalidComposeImport, err)
}

func shouldInspectComposeImport(req importGitHubRequest, buildStrategy, profile string) bool {
	if normalizeBuildStrategy(buildStrategy) != model.AppBuildStrategyAuto {
		return false
	}
	if strings.TrimSpace(profile) != "" {
		return false
	}
	if strings.TrimSpace(req.SourceDir) != "" || strings.TrimSpace(req.DockerfilePath) != "" || strings.TrimSpace(req.BuildContextDir) != "" {
		return false
	}
	if strings.TrimSpace(req.ConfigContent) != "" || len(req.Files) > 0 || req.Postgres != nil {
		return false
	}
	return true
}

func (s *Server) importComposeGitHubStack(principal model.Principal, tenantID string, req importGitHubRequest, runtimeID string, replicas int, description string, baseName string, stack sourceimport.GitHubComposeStack) (map[string]any, model.App, model.Operation, error) {
	appServices, postgresServices := splitComposeServices(stack.Services)
	if len(appServices) == 0 {
		return nil, model.App{}, model.Operation{}, invalidComposeImportf("compose file %q does not define any buildable application services", stack.ComposePath)
	}
	if err := validateComposeDependencies(appServices, postgresServices); err != nil {
		return nil, model.App{}, model.Operation{}, invalidComposeImport(err)
	}

	primaryService := pickPrimaryComposeService(appServices)
	appNames, primaryHost, err := s.allocateComposeAppNames(tenantID, req.ProjectID, baseName, appServices, primaryService.Name)
	if err != nil {
		return nil, model.App{}, model.Operation{}, err
	}

	postgresByOwner, postgresHosts, postgresWarnings, err := buildComposePostgresPlan(appServices, postgresServices, appNames)
	if err != nil {
		return nil, model.App{}, model.Operation{}, invalidComposeImport(err)
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
			"",
			service.Name,
			service.Name,
		)
		if err != nil {
			return nil, model.App{}, model.Operation{}, invalidComposeImport(err)
		}

		var postgres *model.AppPostgresSpec
		if spec, ok := postgresByOwner[service.Name]; ok {
			specCopy := spec
			postgres = &specCopy
		}

		spec, err := s.buildImportedAppSpec(
			"",
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
			return nil, model.App{}, model.Operation{}, invalidComposeImport(err)
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
			appDescription = fmt.Sprintf("%s (compose service %s)", description, plan.Service.Name)
		}

		route := model.AppRoute{}
		if plan.Route != nil {
			route = *plan.Route
		}
		app, err := s.store.CreateImportedApp(tenantID, req.ProjectID, plan.AppName, appDescription, plan.Spec, plan.Source, route)
		if err != nil {
			if err == store.ErrConflict {
				return nil, model.App{}, model.Operation{}, fmt.Errorf("compose import naming conflict for service %q: %w", plan.Service.Name, store.ErrConflict)
			}
			return nil, model.App{}, model.Operation{}, err
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
			return nil, model.App{}, model.Operation{}, err
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

	primaryApp := appsByService[primaryService.Name]
	primaryOp := opsByService[primaryService.Name]

	composeServices := make([]map[string]any, 0, len(plans))
	for _, plan := range plans {
		app := appsByService[plan.Service.Name]
		op := opsByService[plan.Service.Name]
		serviceInfo := map[string]any{
			"compose_service": plan.Service.Name,
			"kind":            plan.Service.Kind,
			"app_id":          app.ID,
			"app_name":        app.Name,
			"operation_id":    op.ID,
			"build_strategy":  plan.Service.BuildStrategy,
			"internal_port":   firstServicePort(app.Spec),
		}
		if app.Route != nil && strings.TrimSpace(app.Route.PublicURL) != "" {
			serviceInfo["public_url"] = app.Route.PublicURL
		}
		if _, ok := postgresByOwner[plan.Service.Name]; ok {
			serviceInfo["owns_postgres"] = true
		}
		composeServices = append(composeServices, serviceInfo)
	}

	warnings := append([]string(nil), stack.Warnings...)
	warnings = append(warnings, postgresWarnings...)

	return map[string]any{
		"app":        sanitizeAppForAPI(primaryApp),
		"operation":  sanitizeOperationForAPI(primaryOp),
		"apps":       sanitizeAppsForAPI(apps),
		"operations": sanitizeOperationsForAPI(ops),
		"compose_stack": map[string]any{
			"compose_path":    stack.ComposePath,
			"primary_service": primaryService.Name,
			"services":        composeServices,
			"warnings":        warnings,
		},
	}, primaryApp, primaryOp, nil
}

func splitComposeServices(services []sourceimport.ComposeService) ([]sourceimport.ComposeService, []sourceimport.ComposeService) {
	apps := make([]sourceimport.ComposeService, 0, len(services))
	postgres := make([]sourceimport.ComposeService, 0)
	for _, service := range services {
		switch service.Kind {
		case sourceimport.ComposeServiceKindApp:
			apps = append(apps, service)
		case sourceimport.ComposeServiceKindPostgres:
			postgres = append(postgres, service)
		}
	}
	return apps, postgres
}

func validateComposeDependencies(appServices, postgresServices []sourceimport.ComposeService) error {
	supported := make(map[string]struct{}, len(appServices)+len(postgresServices))
	for _, service := range appServices {
		supported[service.Name] = struct{}{}
	}
	for _, service := range postgresServices {
		supported[service.Name] = struct{}{}
	}

	for _, service := range appServices {
		for _, dep := range service.DependsOn {
			if _, ok := supported[dep]; ok {
				continue
			}
			return fmt.Errorf("compose service %q depends_on unsupported service %q; Fugue currently imports buildable app services and managed postgres only", service.Name, dep)
		}
	}
	return nil
}

func pickPrimaryComposeService(services []sourceimport.ComposeService) sourceimport.ComposeService {
	best := services[0]
	bestScore := composePrimaryServiceScore(best)
	for _, service := range services[1:] {
		score := composePrimaryServiceScore(service)
		if score > bestScore || (score == bestScore && service.Name < best.Name) {
			best = service
			bestScore = score
		}
	}
	return best
}

func composePrimaryServiceScore(service sourceimport.ComposeService) int {
	score := 0
	if service.Published {
		score += 100
	}
	switch service.Name {
	case "web":
		score += 90
	case "frontend":
		score += 80
	case "app":
		score += 70
	case "site":
		score += 60
	case "ui":
		score += 50
	case "api":
		score += 40
	}
	if strings.Contains(service.Name, "front") || strings.Contains(service.Name, "web") {
		score += 20
	}
	if service.InternalPort == 80 || service.InternalPort == 3000 || service.InternalPort == 8080 || service.InternalPort == 443 {
		score += 10
	}
	return score
}

func (s *Server) allocateComposeAppNames(tenantID, projectID, baseName string, services []sourceimport.ComposeService, primaryService string) (map[string]string, string, error) {
	apps, err := s.store.ListApps(tenantID, false)
	if err != nil {
		return nil, "", err
	}

	for attempt := 0; attempt < 8; attempt++ {
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
				name = truncateSlug(primaryName+"-"+service.Name, 50)
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

func orderComposeServicesForCreation(services []sourceimport.ComposeService, primaryService string) []sourceimport.ComposeService {
	ordered := make([]sourceimport.ComposeService, 0, len(services))
	for _, service := range services {
		if service.Name == primaryService {
			ordered = append(ordered, service)
			break
		}
	}
	for _, service := range services {
		if service.Name == primaryService {
			continue
		}
		ordered = append(ordered, service)
	}
	return ordered
}

func buildComposePostgresPlan(appServices, postgresServices []sourceimport.ComposeService, appNames map[string]string) (map[string]model.AppPostgresSpec, map[string]string, []string, error) {
	postgresByOwner := make(map[string]model.AppPostgresSpec)
	hosts := make(map[string]string)
	warnings := make([]string, 0)

	for _, postgres := range postgresServices {
		consumers := composePostgresConsumers(appServices, postgres.Name)
		if len(consumers) == 0 {
			warnings = append(warnings, fmt.Sprintf("compose postgres service %q has no detected consumer and will not be provisioned", postgres.Name))
			continue
		}

		owner := pickComposePostgresOwner(consumers)
		if _, exists := postgresByOwner[owner.Name]; exists {
			return nil, nil, nil, fmt.Errorf("compose service %q and another managed postgres service would both attach to app service %q; this is not supported yet", postgres.Name, owner.Name)
		}

		spec, err := composePostgresSpec(postgres, appNames[owner.Name])
		if err != nil {
			return nil, nil, nil, err
		}
		postgresByOwner[owner.Name] = spec
		hosts[postgres.Name] = spec.ServiceName

		if len(consumers) > 1 {
			names := make([]string, 0, len(consumers))
			for _, consumer := range consumers {
				names = append(names, consumer.Name)
			}
			sort.Strings(names)
			warnings = append(warnings, fmt.Sprintf("compose postgres service %q is referenced by multiple app services (%s); Fugue will let %q own the database resources", postgres.Name, strings.Join(names, ", "), owner.Name))
		}
	}

	return postgresByOwner, hosts, warnings, nil
}

func composePostgresConsumers(appServices []sourceimport.ComposeService, postgresService string) []sourceimport.ComposeService {
	consumers := make([]sourceimport.ComposeService, 0)
	for _, service := range appServices {
		if composeServiceDependsOn(service, postgresService) || composeEnvironmentReferencesService(service.Environment, postgresService) {
			consumers = append(consumers, service)
		}
	}
	return consumers
}

func pickComposePostgresOwner(consumers []sourceimport.ComposeService) sourceimport.ComposeService {
	best := consumers[0]
	bestScore := composePostgresOwnerScore(best)
	for _, service := range consumers[1:] {
		score := composePostgresOwnerScore(service)
		if score > bestScore || (score == bestScore && service.Name < best.Name) {
			best = service
			bestScore = score
		}
	}
	return best
}

func composePostgresOwnerScore(service sourceimport.ComposeService) int {
	score := 0
	switch service.Name {
	case "api":
		score += 100
	case "backend":
		score += 90
	case "server":
		score += 80
	case "app":
		score += 60
	}
	if strings.Contains(service.Name, "api") || strings.Contains(service.Name, "back") {
		score += 30
	}
	return score
}

func composePostgresSpec(service sourceimport.ComposeService, ownerAppName string) (model.AppPostgresSpec, error) {
	spec := model.AppPostgresSpec{
		Image:       strings.TrimSpace(service.Image),
		Database:    firstNonEmptyComposeValue(service.Environment, "POSTGRES_DB", "POSTGRES_DATABASE", "DB_NAME"),
		User:        firstNonEmptyComposeValue(service.Environment, "POSTGRES_USER", "DB_USER"),
		Password:    firstNonEmptyComposeValue(service.Environment, "POSTGRES_PASSWORD", "DB_PASSWORD"),
		ServiceName: model.Slugify(ownerAppName + "-" + service.Name + "-postgres"),
	}
	if spec.Image == "" {
		spec.Image = "postgres:17.6-alpine"
	}
	if spec.Database == "" {
		spec.Database = ownerAppName
	}
	if spec.User == "" {
		spec.User = "postgres"
	}
	if spec.ServiceName == "" {
		spec.ServiceName = model.Slugify(ownerAppName + "-postgres")
	}
	if spec.Password == "" {
		password, err := randomHex(24)
		if err != nil {
			return model.AppPostgresSpec{}, fmt.Errorf("generate postgres password for compose service %q: %w", service.Name, err)
		}
		spec.Password = password
	}
	return spec, nil
}

func firstNonEmptyComposeValue(env map[string]string, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(env[key]); value != "" {
			return value
		}
	}
	return ""
}

func composeServiceDependsOn(service sourceimport.ComposeService, target string) bool {
	for _, dep := range service.DependsOn {
		if dep == target {
			return true
		}
	}
	return false
}

func composeEnvironmentReferencesService(env map[string]string, service string) bool {
	for _, value := range env {
		if composeEnvValueReferencesService(value, service) {
			return true
		}
	}
	return false
}

func rewriteComposeEnvironment(env map[string]string, hosts map[string]string) map[string]string {
	if len(env) == 0 {
		return nil
	}
	out := make(map[string]string, len(env))
	for key, value := range env {
		out[key] = rewriteComposeEnvValue(value, hosts)
	}
	return out
}

func rewriteComposeEnvValue(value string, hosts map[string]string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return value
	}
	if replacement, ok := hosts[value]; ok {
		return replacement
	}

	if parsed, err := url.Parse(value); err == nil && parsed.Scheme != "" && parsed.Host != "" {
		host := parsed.Hostname()
		if replacement, ok := hosts[host]; ok {
			if port := parsed.Port(); port != "" {
				parsed.Host = net.JoinHostPort(replacement, port)
			} else {
				parsed.Host = replacement
			}
			return parsed.String()
		}
	}

	if host, port, err := net.SplitHostPort(value); err == nil {
		if replacement, ok := hosts[host]; ok {
			return net.JoinHostPort(replacement, port)
		}
	}

	for service, replacement := range hosts {
		value = strings.ReplaceAll(value, "://"+service+":", "://"+replacement+":")
		value = strings.ReplaceAll(value, "://"+service+"/", "://"+replacement+"/")
		value = strings.ReplaceAll(value, "@"+service+":", "@"+replacement+":")
		value = strings.ReplaceAll(value, "@"+service+"/", "@"+replacement+"/")
	}
	return value
}

func composeEnvValueReferencesService(value, service string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	if value == service {
		return true
	}
	if parsed, err := url.Parse(value); err == nil && parsed.Scheme != "" && parsed.Hostname() == service {
		return true
	}
	if host, _, err := net.SplitHostPort(value); err == nil && host == service {
		return true
	}
	return strings.Contains(value, "://"+service+":") ||
		strings.Contains(value, "://"+service+"/") ||
		strings.Contains(value, "@"+service+":") ||
		strings.Contains(value, "@"+service+"/")
}
