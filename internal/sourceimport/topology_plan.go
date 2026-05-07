package sourceimport

import (
	"crypto/rand"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strings"

	"fugue/internal/model"
)

type primaryServiceSelectionPolicy struct{}

type backingOwnerSelectionPolicy struct{}

var defaultPrimaryServiceSelectionPolicy = primaryServiceSelectionPolicy{}
var defaultBackingOwnerSelectionPolicy = backingOwnerSelectionPolicy{}

func SelectPrimaryTopologyService(services []ComposeService, preferred string) (ComposeService, error) {
	service, _, err := resolveTopologyPrimaryServiceFromPlan(services, preferred)
	return service, err
}

func RewriteServiceEnvironment(env map[string]string, hosts map[string]string) map[string]string {
	return rewriteTopologyEnvironment(env, hosts)
}

func ApplyManagedPostgresEnvironment(env map[string]string, spec model.AppPostgresSpec) map[string]string {
	out, _ := applyManagedPostgresBindingEnvironment("", env, spec)
	return out
}

func (primaryServiceSelectionPolicy) score(service ComposeService) int {
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
	switch service.InternalPort {
	case 80, 3000, 8080, 443:
		score += 10
	}
	return score
}

func (backingOwnerSelectionPolicy) score(service ComposeService) int {
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

func AnalyzeNormalizedTopology(topology NormalizedTopology, preferredPrimary string) (TopologyPlan, error) {
	servicesByName := make(map[string]ComposeService, len(topology.Services))
	deployable := make([]ComposeService, 0, len(topology.Services))
	for _, service := range topology.Services {
		servicesByName[service.Name] = service
		if service.Kind == ComposeServiceKindApp {
			deployable = append(deployable, service)
		}
	}
	if len(deployable) == 0 {
		return TopologyPlan{}, fmt.Errorf("topology does not define any deployable application services")
	}
	if err := validateTopologyDependencies(deployable, servicesByName); err != nil {
		return TopologyPlan{}, err
	}

	bindingsBySource := make(map[string][]ServiceBinding, len(topology.Services))
	inferenceReport := append([]TopologyInference(nil), topology.InferenceReport...)
	for _, service := range topology.Services {
		bindings, inferred := inferBindingsForService(service, servicesByName)
		bindingsBySource[service.Name] = bindings
		inferenceReport = append(inferenceReport, inferred...)
	}

	primaryService, primaryInferences, err := resolveTopologyPrimaryServiceFromPlan(deployable, firstNonEmptyString(strings.TrimSpace(preferredPrimary), strings.TrimSpace(topology.PrimaryService)))
	if err != nil {
		return TopologyPlan{}, err
	}
	inferenceReport = append(inferenceReport, primaryInferences...)

	managedBackings := make(map[string]ManagedBackingPlan)
	warnings := append([]string(nil), topology.Warnings...)
	for _, service := range topology.Services {
		if !managedBackingService(service) {
			continue
		}
		consumers := bindingConsumersForService(deployable, bindingsBySource, service.Name)
		if len(consumers) == 0 {
			warnings = append(warnings, fmt.Sprintf("backing service %q has no detected consumer and will not be provisioned as managed %s", service.Name, service.ServiceType))
			inferenceReport = appendInference(inferenceReport, InferenceLevelWarning, "binding", service.Name, "no consumer was found for managed backing service %q", service.Name)
			continue
		}

		ownerService, ownerInferences, err := resolveManagedBackingOwner(service, consumers)
		if err != nil {
			return TopologyPlan{}, err
		}
		inferenceReport = append(inferenceReport, ownerInferences...)
		managedBackings[service.Name] = ManagedBackingPlan{
			Service:      service,
			OwnerService: ownerService,
			Consumers:    consumerNames(consumers),
		}
	}

	plan := TopologyPlan{
		Topology:         topology,
		PrimaryService:   primaryService.Name,
		Deployable:       orderDeployableServices(deployable, primaryService.Name),
		BindingsBySource: bindingsBySource,
		ManagedBackings:  managedBackings,
		Warnings:         warnings,
		InferenceReport:  inferenceReport,
	}
	return plan, nil
}

func ResolveTopologyServiceEnvironment(plan TopologyPlan, serviceName string, deployment TopologyDeployment) (map[string]string, []TopologyInference, error) {
	serviceName = slugifyOptional(serviceName)
	if serviceName == "" {
		return nil, nil, nil
	}
	service, ok := findTopologyService(plan.Topology.Services, serviceName)
	if !ok {
		return nil, nil, fmt.Errorf("compose service %q not found in source topology", serviceName)
	}

	hosts := cloneStringMapLocal(deployment.ServiceHosts)
	if hosts == nil {
		hosts = map[string]string{}
	}
	for backingService, backing := range plan.ManagedBackings {
		spec, ok := deployment.ManagedPostgresByOwner[backing.OwnerService]
		if !ok || strings.TrimSpace(spec.ServiceName) == "" {
			continue
		}
		hosts[backingService] = model.PostgresRWServiceName(spec.ServiceName)
	}

	original := cloneStringMapLocal(service.Environment)
	rewritten := rewriteTopologyEnvironment(service.Environment, hosts)
	inferenceReport := buildEnvRewriteInference(service.Name, original, rewritten)
	if spec, ok := deployment.ManagedPostgresByOwner[service.Name]; ok {
		managedEnv, managedInference := applyManagedPostgresBindingEnvironment(service.Name, rewritten, spec)
		rewritten = managedEnv
		inferenceReport = append(inferenceReport, managedInference...)
	}
	if len(rewritten) == 0 {
		return nil, inferenceReport, nil
	}
	return rewritten, inferenceReport, nil
}

func ManagedPostgresSpec(service ComposeService, ownerAppName string) (model.AppPostgresSpec, error) {
	spec := model.AppPostgresSpec{}
	if service.Postgres != nil {
		spec = *service.Postgres
	}
	if strings.TrimSpace(spec.Image) == "" {
		spec.Image = model.NormalizeManagedPostgresImage(service.Image)
	}
	if strings.TrimSpace(spec.Database) == "" {
		spec.Database = firstNonEmptyComposeValue(service.Environment, "POSTGRES_DB", "POSTGRES_DATABASE", "DB_NAME")
	}
	if strings.TrimSpace(spec.User) == "" {
		spec.User = firstNonEmptyComposeValue(service.Environment, "POSTGRES_USER", "DB_USER")
	}
	if service.Postgres == nil && strings.EqualFold(strings.TrimSpace(spec.User), "postgres") {
		spec.User = ""
	}
	if strings.TrimSpace(spec.Password) == "" {
		spec.Password = firstNonEmptyComposeValue(service.Environment, "POSTGRES_PASSWORD", "DB_PASSWORD")
	}
	if strings.TrimSpace(spec.ServiceName) == "" {
		spec.ServiceName = model.Slugify(ownerAppName + "-" + service.Name + "-postgres")
	}
	spec.Image = model.NormalizeManagedPostgresImage(spec.Image)
	if spec.Database == "" {
		spec.Database = ownerAppName
	}
	if spec.User == "" {
		spec.User = model.DefaultManagedPostgresUser(ownerAppName)
	}
	if err := model.ValidateManagedPostgresUser(ownerAppName, spec); err != nil {
		return model.AppPostgresSpec{}, fmt.Errorf("invalid postgres config for compose service %q: %w", service.Name, err)
	}
	if spec.Password == "" {
		password, err := randomHex(24)
		if err != nil {
			return model.AppPostgresSpec{}, fmt.Errorf("generate postgres password for compose service %q: %w", service.Name, err)
		}
		spec.Password = password
	}
	if spec.ServiceName == "" {
		spec.ServiceName = model.Slugify(ownerAppName + "-postgres")
	}
	return spec, nil
}

func firstNonEmptyComposeValue(env map[string]string, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(env[key]); value != "" {
			if isComposeMissingRequiredEnvValue(value) {
				continue
			}
			return value
		}
	}
	return ""
}

func randomHex(numBytes int) (string, error) {
	if numBytes <= 0 {
		return "", nil
	}
	buf := make([]byte, numBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", buf), nil
}

func validateTopologyDependencies(deployable []ComposeService, servicesByName map[string]ComposeService) error {
	for _, service := range deployable {
		for _, dep := range service.DependsOn {
			if _, ok := servicesByName[dep]; ok {
				continue
			}
			return fmt.Errorf("compose service %q depends_on unsupported service %q", service.Name, dep)
		}
	}
	return nil
}

func findTopologyService(services []ComposeService, name string) (ComposeService, bool) {
	for _, service := range services {
		if service.Name == name {
			return service, true
		}
	}
	return ComposeService{}, false
}

func resolveTopologyPrimaryServiceFromPlan(services []ComposeService, preferred string) (ComposeService, []TopologyInference, error) {
	preferred = strings.TrimSpace(preferred)
	if preferred != "" {
		preferred = slugifyOptional(preferred)
		for _, service := range services {
			if service.Name == preferred {
				return service, appendInference(nil, InferenceLevelInfo, "primary_service", service.Name, "selected primary service %q from explicit metadata", service.Name), nil
			}
		}
		return ComposeService{}, nil, fmt.Errorf("primary service %q does not exist", preferred)
	}
	best := services[0]
	bestScore := defaultPrimaryServiceSelectionPolicy.score(best)
	for _, service := range services[1:] {
		score := defaultPrimaryServiceSelectionPolicy.score(service)
		if score > bestScore || (score == bestScore && service.Name < best.Name) {
			best = service
			bestScore = score
		}
	}
	return best, appendInference(nil, InferenceLevelInfo, "primary_service", best.Name, "selected primary service %q using naming and port heuristics", best.Name), nil
}

func inferBindingsForService(service ComposeService, servicesByName map[string]ComposeService) ([]ServiceBinding, []TopologyInference) {
	bindings := append([]ServiceBinding(nil), service.Bindings...)
	inferenceReport := make([]TopologyInference, 0)
	for _, binding := range service.Bindings {
		inferenceReport = appendInference(inferenceReport, InferenceLevelInfo, "binding", service.Name, "kept explicit binding from %q to %q", service.Name, binding.Service)
	}
	for _, dep := range service.DependsOn {
		if _, ok := servicesByName[dep]; !ok {
			continue
		}
		bindings = append(bindings, ServiceBinding{Service: dep, Source: BindingSourceDependsOn})
	}
	for candidate := range servicesByName {
		if candidate == service.Name {
			continue
		}
		if environmentReferencesService(service.Environment, candidate) {
			bindings = append(bindings, ServiceBinding{Service: candidate, Source: BindingSourceEnv})
		}
	}
	bindings = uniqueBindings(bindings)
	for _, binding := range bindings {
		if binding.Source == BindingSourceExplicit {
			continue
		}
		inferenceReport = appendInference(inferenceReport, InferenceLevelInfo, "binding", service.Name, "inferred binding from %q to %q via %s", service.Name, binding.Service, binding.Source)
	}
	return bindings, inferenceReport
}

func bindingConsumersForService(services []ComposeService, bindingsBySource map[string][]ServiceBinding, target string) []ComposeService {
	consumers := make([]ComposeService, 0)
	for _, service := range services {
		for _, binding := range bindingsBySource[service.Name] {
			if binding.Service == target {
				consumers = append(consumers, service)
				break
			}
		}
	}
	return consumers
}

func resolveManagedBackingOwner(service ComposeService, consumers []ComposeService) (string, []TopologyInference, error) {
	if owner := slugifyOptional(service.OwnerService); owner != "" {
		for _, consumer := range consumers {
			if consumer.Name == owner {
				return owner, appendInference(nil, InferenceLevelInfo, "owner", service.Name, "assigned managed backing service %q to explicit owner %q", service.Name, owner), nil
			}
		}
		return "", nil, fmt.Errorf("managed backing service %q declares owner_service %q but no matching consumer exists", service.Name, service.OwnerService)
	}
	best := consumers[0]
	bestScore := defaultBackingOwnerSelectionPolicy.score(best)
	for _, consumer := range consumers[1:] {
		score := defaultBackingOwnerSelectionPolicy.score(consumer)
		if score > bestScore || (score == bestScore && consumer.Name < best.Name) {
			best = consumer
			bestScore = score
		}
	}
	return best.Name, appendInference(nil, InferenceLevelInfo, "owner", service.Name, "assigned managed backing service %q to inferred owner %q", service.Name, best.Name), nil
}

func consumerNames(consumers []ComposeService) []string {
	names := make([]string, 0, len(consumers))
	for _, consumer := range consumers {
		names = append(names, consumer.Name)
	}
	sort.Strings(names)
	return names
}

func orderDeployableServices(services []ComposeService, primary string) []ComposeService {
	ordered := make([]ComposeService, 0, len(services))
	for _, service := range services {
		if service.Name == primary {
			ordered = append(ordered, service)
			break
		}
	}
	for _, service := range services {
		if service.Name == primary {
			continue
		}
		ordered = append(ordered, service)
	}
	return ordered
}

func rewriteTopologyEnvironment(env map[string]string, hosts map[string]string) map[string]string {
	if len(env) == 0 {
		return nil
	}
	out := make(map[string]string, len(env))
	for key, value := range env {
		if topologyEnvKeyUsesLogicalServiceName(key) {
			out[key] = strings.TrimSpace(value)
			continue
		}
		out[key] = rewriteTopologyEnvValue(value, hosts)
	}
	return out
}

func applyManagedPostgresBindingEnvironment(ownerService string, env map[string]string, spec model.AppPostgresSpec) (map[string]string, []TopologyInference) {
	if len(env) == 0 {
		return nil, nil
	}
	out := cloneStringMapLocal(env)
	host := model.PostgresRWServiceName(spec.ServiceName)
	overrideManagedEnvIfPresent(out, "DB_HOST", host)
	overrideManagedEnvIfPresent(out, "POSTGRES_HOST", host)
	overrideManagedEnvIfPresent(out, "DATABASE_HOST", host)
	overrideManagedEnvIfPresent(out, "DB_PORT", "5432")
	overrideManagedEnvIfPresent(out, "POSTGRES_PORT", "5432")
	overrideManagedEnvIfPresent(out, "DATABASE_PORT", "5432")
	overrideManagedEnvIfPresent(out, "DB_NAME", spec.Database)
	overrideManagedEnvIfPresent(out, "POSTGRES_DB", spec.Database)
	overrideManagedEnvIfPresent(out, "POSTGRES_DATABASE", spec.Database)
	overrideManagedEnvIfPresent(out, "DATABASE_DBNAME", spec.Database)
	overrideManagedEnvIfPresent(out, "DATABASE_NAME", spec.Database)
	overrideManagedEnvIfPresent(out, "DB_USER", spec.User)
	overrideManagedEnvIfPresent(out, "POSTGRES_USER", spec.User)
	overrideManagedEnvIfPresent(out, "DATABASE_USER", spec.User)
	overrideManagedEnvIfPresent(out, "DB_PASSWORD", spec.Password)
	overrideManagedEnvIfPresent(out, "POSTGRES_PASSWORD", spec.Password)
	overrideManagedEnvIfPresent(out, "DATABASE_PASSWORD", spec.Password)
	for key, value := range out {
		if rewritten, ok := rewriteManagedPostgresURL(value, spec); ok {
			out[key] = rewritten
		}
	}
	return out, buildManagedEnvInference(ownerService, env, out)
}

func overrideManagedEnvIfPresent(env map[string]string, key, value string) {
	if _, ok := env[key]; ok {
		env[key] = value
	}
}

func rewriteManagedPostgresURL(value string, spec model.AppPostgresSpec) (string, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return value, false
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme == "" || parsed.Hostname() == "" {
		return value, false
	}
	if !strings.Contains(strings.ToLower(parsed.Scheme), "postgres") {
		return value, false
	}
	legacyHost := strings.TrimSpace(spec.ServiceName)
	host := model.PostgresRWServiceName(spec.ServiceName)
	if !strings.EqualFold(parsed.Hostname(), legacyHost) && !strings.EqualFold(parsed.Hostname(), host) {
		return value, false
	}
	port := parsed.Port()
	if port == "" {
		port = "5432"
	}
	parsed.Host = net.JoinHostPort(host, port)
	parsed.User = url.UserPassword(spec.User, spec.Password)
	if db := strings.TrimSpace(spec.Database); db != "" {
		parsed.Path = "/" + strings.TrimPrefix(db, "/")
	}
	return parsed.String(), true
}

func rewriteTopologyEnvValue(value string, hosts map[string]string) string {
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

func environmentReferencesService(env map[string]string, service string) bool {
	for key, value := range env {
		if topologyEnvKeyUsesLogicalServiceName(key) {
			continue
		}
		if envValueReferencesService(value, service) {
			return true
		}
	}
	return false
}

func topologyEnvKeyUsesLogicalServiceName(key string) bool {
	key = strings.TrimSpace(strings.ToUpper(key))
	if key == "" {
		return false
	}
	return strings.HasSuffix(key, "_COMPOSE_SERVICE")
}

func envValueReferencesService(value, service string) bool {
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

func buildEnvRewriteInference(serviceName string, before, after map[string]string) []TopologyInference {
	if len(before) == 0 || len(after) == 0 {
		return nil
	}
	keys := make([]string, 0, len(after))
	for key := range after {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	report := make([]TopologyInference, 0)
	for _, key := range keys {
		beforeValue, beforeOK := before[key]
		afterValue, afterOK := after[key]
		if !beforeOK || !afterOK || beforeValue == afterValue {
			continue
		}
		report = appendInference(report, InferenceLevelInfo, "env_rewrite", serviceName, "rewrote env key %q to target the deployed topology", key)
	}
	return report
}

func buildManagedEnvInference(serviceName string, before, after map[string]string) []TopologyInference {
	if len(before) == 0 || len(after) == 0 {
		return nil
	}
	keys := make([]string, 0, len(after))
	for key := range after {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	report := make([]TopologyInference, 0)
	for _, key := range keys {
		beforeValue, beforeOK := before[key]
		afterValue, afterOK := after[key]
		if !beforeOK || !afterOK || beforeValue == afterValue {
			continue
		}
		report = appendInference(report, InferenceLevelInfo, "managed_env", serviceName, "updated env key %q from managed postgres binding", key)
	}
	return report
}
