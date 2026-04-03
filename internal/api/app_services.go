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
