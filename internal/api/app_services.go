package api

import (
	"sort"
	"strings"

	"fugue/internal/model"
)

type appBoundService struct {
	Binding model.ServiceBinding
	Service model.BackingService
}

func appBoundServices(app model.App) []appBoundService {
	servicesByID := make(map[string]model.BackingService, len(app.BackingServices))
	for _, service := range app.BackingServices {
		servicesByID[service.ID] = service
	}

	bound := make([]appBoundService, 0, len(app.Bindings))
	for _, binding := range app.Bindings {
		service, ok := servicesByID[binding.ServiceID]
		if !ok {
			continue
		}
		bound = append(bound, appBoundService{
			Binding: binding,
			Service: service,
		})
	}

	return bound
}

func mergedAppEnv(app model.App) map[string]string {
	return mergedAppEnvWithSpec(app, app.Spec)
}

func mergedAppEnvWithBindings(spec model.AppSpec, bindings []model.ServiceBinding) map[string]string {
	app := model.App{
		Bindings: bindings,
		Spec:     spec,
	}
	return mergedAppEnvWithSpec(app, spec)
}

func mergedAppEnvWithSpec(app model.App, spec model.AppSpec) map[string]string {
	return mergedAppEnvDetails(app, spec).Env
}

func mergedAppEnvEntries(app model.App, spec model.AppSpec) []model.AppEnvEntry {
	return mergedAppEnvDetails(app, spec).Entries
}

type appEnvDetails struct {
	Env     map[string]string
	Entries []model.AppEnvEntry
}

type appEnvOrigin struct {
	Source    string
	SourceRef string
}

func mergedAppEnvDetails(app model.App, spec model.AppSpec) appEnvDetails {
	merged := make(map[string]string)
	origins := make(map[string]appEnvOrigin)
	overrides := make(map[string][]string)
	hasManagedPostgresBinding := false
	for _, bound := range appBoundServices(app) {
		bindingEnv := cloneStringMap(bound.Binding.Env)
		if strings.EqualFold(strings.TrimSpace(bound.Service.Type), model.BackingServiceTypePostgres) && bound.Service.Spec.Postgres != nil {
			hasManagedPostgresBinding = true
			if bindingEnv == nil {
				bindingEnv = map[string]string{}
			}
			for key, value := range defaultAPIBindingPostgresEnv(*bound.Service.Spec.Postgres) {
				bindingEnv[key] = value
			}
		}
		sourceRef := firstNonEmptyString(
			strings.TrimSpace(bound.Binding.Alias),
			strings.TrimSpace(bound.Service.Name),
			strings.TrimSpace(bound.Service.ID),
		)
		for key, value := range bindingEnv {
			recordAppEnvValue(merged, origins, overrides, key, value, appEnvOrigin{
				Source:    "binding",
				SourceRef: sourceRef,
			})
		}
	}
	if !hasManagedPostgresBinding && spec.Postgres != nil {
		postgresEnv := defaultAppManagedPostgresEnv(app.Name, *spec.Postgres)
		for key, value := range postgresEnv {
			if _, exists := merged[key]; exists {
				continue
			}
			recordAppEnvValue(merged, origins, overrides, key, value, appEnvOrigin{
				Source:    "managed-postgres",
				SourceRef: firstNonEmptyString(strings.TrimSpace(spec.Postgres.ServiceName), strings.TrimSpace(app.Name)),
			})
		}
	}
	for key, value := range spec.Env {
		recordAppEnvValue(merged, origins, overrides, key, value, appEnvOrigin{
			Source:    "app",
			SourceRef: "spec.env",
		})
	}
	if len(merged) == 0 {
		return appEnvDetails{}
	}
	keys := make([]string, 0, len(merged))
	for key := range merged {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	entries := make([]model.AppEnvEntry, 0, len(keys))
	for _, key := range keys {
		entry := model.AppEnvEntry{
			Key:   key,
			Value: merged[key],
		}
		if origin, ok := origins[key]; ok {
			entry.Source = origin.Source
			entry.SourceRef = origin.SourceRef
		}
		if len(overrides[key]) > 0 {
			entry.Overrides = append([]string(nil), overrides[key]...)
		}
		entries = append(entries, entry)
	}
	return appEnvDetails{
		Env:     merged,
		Entries: entries,
	}
}

func recordAppEnvValue(merged map[string]string, origins map[string]appEnvOrigin, overrides map[string][]string, key, value string, origin appEnvOrigin) {
	if previous, exists := origins[key]; exists {
		label := describeAppEnvOrigin(previous)
		if label != "" {
			overrides[key] = appendUniqueString(overrides[key], label)
		}
	}
	merged[key] = value
	origins[key] = origin
}

func describeAppEnvOrigin(origin appEnvOrigin) string {
	switch {
	case strings.TrimSpace(origin.SourceRef) == "":
		return strings.TrimSpace(origin.Source)
	case strings.TrimSpace(origin.Source) == "":
		return strings.TrimSpace(origin.SourceRef)
	default:
		return strings.TrimSpace(origin.Source) + ":" + strings.TrimSpace(origin.SourceRef)
	}
}

func appendUniqueString(values []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return values
	}
	for _, existing := range values {
		if strings.EqualFold(strings.TrimSpace(existing), value) {
			return values
		}
	}
	return append(values, value)
}

func hasManagedStatefulBinding(app model.App) bool {
	for _, bound := range appBoundServices(app) {
		if !strings.EqualFold(strings.TrimSpace(bound.Service.Type), model.BackingServiceTypePostgres) {
			continue
		}
		if isManagedBackingService(bound.Service) {
			return true
		}
	}
	return false
}

func firstManagedPostgresBinding(app model.App) (appBoundService, bool) {
	var fallback appBoundService
	for _, bound := range appBoundServices(app) {
		if !strings.EqualFold(strings.TrimSpace(bound.Service.Type), model.BackingServiceTypePostgres) {
			continue
		}
		if !isManagedBackingService(bound.Service) {
			continue
		}
		if bound.Service.OwnerAppID == app.ID {
			return bound, true
		}
		if fallback.Service.ID == "" {
			fallback = bound
		}
	}
	if fallback.Service.ID != "" {
		return fallback, true
	}
	return appBoundService{}, false
}

func isManagedBackingService(service model.BackingService) bool {
	provisioner := strings.TrimSpace(service.Provisioner)
	return provisioner == "" || strings.EqualFold(provisioner, model.BackingServiceProvisionerManaged)
}

func defaultAPIBindingPostgresEnv(spec model.AppPostgresSpec) map[string]string {
	return map[string]string{
		"DB_TYPE":     "postgres",
		"DB_HOST":     model.PostgresRWServiceName(spec.ServiceName),
		"DB_PORT":     "5432",
		"DB_USER":     spec.User,
		"DB_PASSWORD": spec.Password,
		"DB_NAME":     spec.Database,
	}
}

func defaultAppManagedPostgresEnv(appName string, spec model.AppPostgresSpec) map[string]string {
	baseName := strings.TrimSpace(model.Slugify(appName))
	if baseName == "" {
		baseName = "app"
	}
	if strings.TrimSpace(spec.Database) == "" {
		spec.Database = baseName
	}
	if strings.TrimSpace(spec.User) == "" {
		spec.User = model.DefaultManagedPostgresUser(baseName)
	}
	if strings.TrimSpace(spec.ServiceName) == "" {
		spec.ServiceName = model.Slugify(baseName + "-postgres")
	}
	if len(spec.ServiceName) > 63 {
		spec.ServiceName = spec.ServiceName[:63]
	}
	return defaultAPIBindingPostgresEnv(spec)
}
