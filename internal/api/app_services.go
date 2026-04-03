package api

import (
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

	if len(bound) == 0 && app.Spec.Postgres != nil {
		spec := normalizeLegacyAPIAppPostgresSpec(app)
		bound = append(bound, appBoundService{
			Binding: model.ServiceBinding{
				ID:        "legacy-postgres-binding-" + app.ID,
				TenantID:  app.TenantID,
				AppID:     app.ID,
				ServiceID: "legacy-postgres-" + app.ID,
				Alias:     "postgres",
				Env:       defaultAPIBindingPostgresEnv(spec),
			},
			Service: model.BackingService{
				ID:          "legacy-postgres-" + app.ID,
				TenantID:    app.TenantID,
				ProjectID:   app.ProjectID,
				OwnerAppID:  app.ID,
				Name:        legacyAPIBackingServiceName(app.Name),
				Type:        model.BackingServiceTypePostgres,
				Provisioner: model.BackingServiceProvisionerManaged,
				Status:      model.BackingServiceStatusActive,
				Spec: model.BackingServiceSpec{
					Postgres: &spec,
				},
			},
		})
	}

	return bound
}

func mergedAppEnv(app model.App) map[string]string {
	return mergedAppEnvWithSpec(app, app.Spec)
}

func mergedAppEnvWithSpec(app model.App, spec model.AppSpec) map[string]string {
	merged := make(map[string]string)
	for _, bound := range appBoundServices(app) {
		bindingEnv := cloneStringMap(bound.Binding.Env)
		if strings.EqualFold(strings.TrimSpace(bound.Service.Type), model.BackingServiceTypePostgres) && bound.Service.Spec.Postgres != nil {
			if bindingEnv == nil {
				bindingEnv = map[string]string{}
			}
			for key, value := range defaultAPIBindingPostgresEnv(*bound.Service.Spec.Postgres) {
				bindingEnv[key] = value
			}
		}
		for key, value := range bindingEnv {
			merged[key] = value
		}
	}
	for key, value := range spec.Env {
		merged[key] = value
	}
	if len(merged) == 0 {
		return nil
	}
	return merged
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

func normalizeLegacyAPIAppPostgresSpec(app model.App) model.AppPostgresSpec {
	spec := *app.Spec.Postgres
	baseName := legacyAPIBackingServiceName(app.Name)
	if strings.TrimSpace(spec.Database) == "" {
		spec.Database = baseName
	}
	if strings.TrimSpace(spec.User) == "" {
		spec.User = model.DefaultManagedPostgresUser(app.Name, spec.StoragePath)
	}
	if strings.TrimSpace(spec.ServiceName) == "" {
		spec.ServiceName = baseName + "-postgres"
	}
	return spec
}

func legacyAPIBackingServiceName(appName string) string {
	name := model.Slugify(appName)
	if len(name) > 50 {
		name = name[:50]
	}
	if strings.TrimSpace(name) == "" {
		return "app"
	}
	return name
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
