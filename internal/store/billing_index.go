package store

import (
	"sort"
	"strings"

	"fugue/internal/model"
)

type billingStateIndex struct {
	runtimesByID                   map[string]model.Runtime
	servicesByID                   map[string]model.BackingService
	billableManagedServicesByAppID map[string][]model.BackingService
}

func newBillingStateIndex(state *model.State) billingStateIndex {
	index := billingStateIndex{
		runtimesByID:                   map[string]model.Runtime{},
		servicesByID:                   map[string]model.BackingService{},
		billableManagedServicesByAppID: map[string][]model.BackingService{},
	}
	if state == nil {
		return index
	}

	for _, runtime := range state.Runtimes {
		if _, exists := index.runtimesByID[runtime.ID]; exists {
			continue
		}
		index.runtimesByID[runtime.ID] = runtime
	}
	for _, service := range state.BackingServices {
		if _, exists := index.servicesByID[service.ID]; exists {
			continue
		}
		index.servicesByID[service.ID] = service
	}

	seenServiceIDsByAppID := map[string]map[string]struct{}{}
	for _, binding := range state.ServiceBindings {
		service, ok := index.servicesByID[binding.ServiceID]
		if !ok || !isBillableManagedBackingService(service) {
			continue
		}
		seenServiceIDs := seenServiceIDsByAppID[binding.AppID]
		if seenServiceIDs == nil {
			seenServiceIDs = map[string]struct{}{}
			seenServiceIDsByAppID[binding.AppID] = seenServiceIDs
		}
		if _, seen := seenServiceIDs[service.ID]; seen {
			continue
		}
		seenServiceIDs[service.ID] = struct{}{}
		index.billableManagedServicesByAppID[binding.AppID] = append(index.billableManagedServicesByAppID[binding.AppID], service)
	}

	return index
}

func (index billingStateIndex) runtime(runtimeID string) (model.Runtime, bool) {
	if strings.TrimSpace(runtimeID) == "" {
		return model.Runtime{}, false
	}
	runtime, ok := index.runtimesByID[runtimeID]
	return runtime, ok
}

func (index billingStateIndex) runtimeType(runtimeID string) string {
	runtime, ok := index.runtime(runtimeID)
	if !ok {
		return ""
	}
	return runtime.Type
}

func (index billingStateIndex) service(serviceID string) (model.BackingService, bool) {
	if strings.TrimSpace(serviceID) == "" {
		return model.BackingService{}, false
	}
	service, ok := index.servicesByID[serviceID]
	return service, ok
}

func (index billingStateIndex) managedServicesForApp(appID string) []model.BackingService {
	if strings.TrimSpace(appID) == "" {
		return nil
	}
	return index.billableManagedServicesByAppID[appID]
}

func tenantManagedCommittedResourcesWithIndex(state *model.State, index billingStateIndex, tenantID string) model.BillingResourceSpec {
	total := model.BillingResourceSpec{}
	if state == nil {
		return total
	}
	countedServices := make(map[string]struct{})
	for _, app := range state.Apps {
		if app.TenantID != tenantID || isDeletedApp(app) {
			continue
		}
		total = addResourceSpec(total, appManagedBundleCommitmentWithIndex(index, app, app.Status.CurrentRuntimeID, app.Status.CurrentReplicas))
		for _, service := range index.managedServicesForApp(app.ID) {
			countedServices[service.ID] = struct{}{}
		}
	}
	for _, service := range state.BackingServices {
		if service.TenantID != tenantID || isDeletedBackingService(service) {
			continue
		}
		if _, counted := countedServices[service.ID]; counted {
			continue
		}
		if !isBillableManagedBackingService(service) {
			continue
		}
		if !isBillableManagedRuntimeType(index.runtimeType(backingServiceRuntimeID(service, ""))) {
			continue
		}
		total = addResourceSpec(total, backingServiceResources(service))
	}
	return total
}

func tenantManagedCommittedResourcesForBillingWithIndex(state *model.State, index billingStateIndex, record model.TenantBilling) model.BillingResourceSpec {
	return addManagedImageStorageCommitment(tenantManagedCommittedResourcesWithIndex(state, index, record.TenantID), record.ManagedImageStorageGibibytes)
}

func appManagedBundleCommitmentWithIndex(index billingStateIndex, app model.App, runtimeID string, replicas int) model.BillingResourceSpec {
	total := model.BillingResourceSpec{}
	if replicas > 0 && isBillableManagedRuntimeType(index.runtimeType(runtimeID)) {
		total = multiplyResourceSpec(appEffectiveResources(app.Spec), int64(replicas))
	}
	for _, service := range index.managedServicesForApp(app.ID) {
		if !isBillableManagedRuntimeType(index.runtimeType(backingServiceRuntimeID(service, runtimeID))) {
			continue
		}
		total = addResourceSpec(total, backingServiceResources(service))
	}
	return total
}

func publicRuntimeChargeComponentForResourcesWithIndex(index billingStateIndex, consumerTenantID, runtimeID string, resources model.BillingResourceSpec) (publicRuntimeChargeComponent, bool) {
	if strings.TrimSpace(runtimeID) == "" {
		return publicRuntimeChargeComponent{}, false
	}
	if resources.CPUMilliCores <= 0 && resources.MemoryMebibytes <= 0 && resources.StorageGibibytes <= 0 {
		return publicRuntimeChargeComponent{}, false
	}

	runtime, ok := index.runtime(runtimeID)
	if !ok {
		return publicRuntimeChargeComponent{}, false
	}
	if strings.TrimSpace(runtime.TenantID) == "" || runtime.TenantID == consumerTenantID {
		return publicRuntimeChargeComponent{}, false
	}
	if normalizeRuntimeAccessMode(runtime.Type, runtime.AccessMode) != model.RuntimeAccessModePublic || runtime.PublicOffer == nil {
		return publicRuntimeChargeComponent{}, false
	}

	hourlyRate := publicRuntimeOfferHourlyRateMicroCents(*runtime.PublicOffer, resources)
	if hourlyRate <= 0 {
		return publicRuntimeChargeComponent{}, false
	}
	return publicRuntimeChargeComponent{
		OwnerTenantID:        runtime.TenantID,
		RuntimeID:            runtime.ID,
		RuntimeName:          runtime.Name,
		HourlyRateMicroCents: hourlyRate,
	}, true
}

func tenantPublicRuntimeChargeComponentsWithIndex(state *model.State, index billingStateIndex, tenantID string) []publicRuntimeChargeComponent {
	if state == nil || strings.TrimSpace(tenantID) == "" {
		return nil
	}
	aggregated := make(map[string]publicRuntimeChargeComponent)
	for _, app := range state.Apps {
		if app.TenantID != tenantID || isDeletedApp(app) {
			continue
		}
		if app.Status.CurrentReplicas <= 0 || strings.TrimSpace(app.Status.CurrentRuntimeID) == "" {
			continue
		}
		if component, ok := publicRuntimeChargeComponentForResourcesWithIndex(
			index,
			tenantID,
			app.Status.CurrentRuntimeID,
			multiplyResourceSpec(appEffectiveResources(app.Spec), int64(app.Status.CurrentReplicas)),
		); ok {
			mergePublicRuntimeChargeComponent(aggregated, component)
		}
		for _, service := range index.managedServicesForApp(app.ID) {
			if component, ok := publicRuntimeChargeComponentForResourcesWithIndex(
				index,
				tenantID,
				backingServiceRuntimeID(service, app.Status.CurrentRuntimeID),
				backingServiceResources(service),
			); ok {
				mergePublicRuntimeChargeComponent(aggregated, component)
			}
		}
	}
	components := make([]publicRuntimeChargeComponent, 0, len(aggregated))
	for _, component := range aggregated {
		components = append(components, component)
	}
	sortPublicRuntimeChargeComponents(components)
	return components
}

func tenantPublicRuntimeOutgoingHourlyRateMicroCentsWithIndex(state *model.State, index billingStateIndex, tenantID string) int64 {
	total := int64(0)
	for _, component := range tenantPublicRuntimeChargeComponentsWithIndex(state, index, tenantID) {
		total += component.HourlyRateMicroCents
	}
	return total
}

func sortPublicRuntimeChargeComponents(components []publicRuntimeChargeComponent) {
	sort.Slice(components, func(i, j int) bool {
		if components[i].OwnerTenantID == components[j].OwnerTenantID {
			if components[i].RuntimeID == components[j].RuntimeID {
				return components[i].RuntimeName < components[j].RuntimeName
			}
			return components[i].RuntimeID < components[j].RuntimeID
		}
		return components[i].OwnerTenantID < components[j].OwnerTenantID
	})
}
