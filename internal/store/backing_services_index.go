package store

import "fugue/internal/model"

type appBackingServiceIndex struct {
	bindingsByAppID map[string][]model.ServiceBinding
	servicesByAppID map[string]map[string]model.BackingService
}

func newAppBackingServiceIndex(state *model.State) appBackingServiceIndex {
	index := appBackingServiceIndex{
		bindingsByAppID: map[string][]model.ServiceBinding{},
		servicesByAppID: map[string]map[string]model.BackingService{},
	}
	if state == nil {
		return index
	}

	servicesByID := make(map[string]model.BackingService, len(state.BackingServices))
	for _, service := range state.BackingServices {
		if _, exists := servicesByID[service.ID]; exists {
			continue
		}
		servicesByID[service.ID] = service
	}

	for _, binding := range state.ServiceBindings {
		service, ok := servicesByID[binding.ServiceID]
		if !ok || isDeletedBackingService(service) {
			continue
		}
		index.bindingsByAppID[binding.AppID] = append(index.bindingsByAppID[binding.AppID], binding)
		if index.servicesByAppID[binding.AppID] == nil {
			index.servicesByAppID[binding.AppID] = map[string]model.BackingService{}
		}
		index.servicesByAppID[binding.AppID][service.ID] = service
	}

	return index
}

func hydrateAppBackingServicesWithIndex(index appBackingServiceIndex, app *model.App) {
	if app == nil {
		return
	}

	bindings := make([]model.ServiceBinding, 0, len(index.bindingsByAppID[app.ID]))
	for _, binding := range index.bindingsByAppID[app.ID] {
		bindings = append(bindings, cloneServiceBinding(binding))
	}
	sortServiceBindings(bindings)

	serviceMap := index.servicesByAppID[app.ID]
	services := make([]model.BackingService, 0, len(serviceMap))
	for _, service := range serviceMap {
		services = append(services, cloneBackingService(service))
	}
	sortBackingServices(services)

	app.Bindings = bindings
	app.BackingServices = services
}
