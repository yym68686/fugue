package store

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	defaultManagedBackingPostgresImage               = ""
	defaultManagedBackingPostgresStorage             = "1Gi"
	defaultManagedBackingPostgresInstances           = 3
	defaultManagedBackingPostgresSynchronousReplicas = 1
)

func (s *Store) ListBackingServices(tenantID string, platformAdmin bool) ([]model.BackingService, error) {
	if s.usingDatabase() {
		return s.pgListBackingServices(tenantID, platformAdmin)
	}
	var services []model.BackingService
	err := s.withLockedState(false, func(state *model.State) error {
		for _, service := range state.BackingServices {
			if isDeletedBackingService(service) {
				continue
			}
			if platformAdmin || service.TenantID == tenantID {
				services = append(services, cloneBackingService(service))
			}
		}
		sort.Slice(services, func(i, j int) bool {
			return services[i].CreatedAt.Before(services[j].CreatedAt)
		})
		return nil
	})
	return services, err
}

func (s *Store) GetBackingService(id string) (model.BackingService, error) {
	if s.usingDatabase() {
		return s.pgGetBackingService(id)
	}
	var service model.BackingService
	err := s.withLockedState(false, func(state *model.State) error {
		index := findBackingService(state, id)
		if index < 0 {
			return ErrNotFound
		}
		service = cloneBackingService(state.BackingServices[index])
		if isDeletedBackingService(service) {
			return ErrNotFound
		}
		return nil
	})
	return service, err
}

func (s *Store) CreateBackingService(tenantID, projectID, name, description string, spec model.BackingServiceSpec) (model.BackingService, error) {
	if tenantID == "" || projectID == "" || strings.TrimSpace(name) == "" {
		return model.BackingService{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgCreateBackingService(tenantID, projectID, name, description, spec)
	}
	var service model.BackingService
	err := s.withLockedState(true, func(state *model.State) error {
		if findTenant(state, tenantID) < 0 {
			return ErrNotFound
		}
		if !projectBelongsToTenant(state, projectID, tenantID) {
			return ErrNotFound
		}
		now := time.Now().UTC()
		service = model.BackingService{
			ID:          model.NewID("service"),
			TenantID:    tenantID,
			ProjectID:   projectID,
			Name:        nextAvailableBackingServiceName(state, tenantID, projectID, name),
			Description: strings.TrimSpace(description),
			Type:        backingServiceTypeFromSpec(spec),
			Provisioner: model.BackingServiceProvisionerManaged,
			Status:      model.BackingServiceStatusActive,
			Spec:        cloneBackingServiceSpec(spec),
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		if err := normalizeBackingServiceForPersist(&service, nil); err != nil {
			return err
		}
		if backingServiceNameExists(state, tenantID, projectID, service.Name, "") {
			return ErrConflict
		}
		state.BackingServices = append(state.BackingServices, service)
		return nil
	})
	return service, err
}

func (s *Store) DeleteBackingService(id string) (model.BackingService, error) {
	if s.usingDatabase() {
		return s.pgDeleteBackingService(id)
	}
	var service model.BackingService
	err := s.withLockedState(true, func(state *model.State) error {
		index := findBackingService(state, id)
		if index < 0 {
			return ErrNotFound
		}
		service = cloneBackingService(state.BackingServices[index])
		if isDeletedBackingService(service) {
			return ErrNotFound
		}
		if hasServiceBindings(state, id) {
			return ErrConflict
		}
		state.BackingServices = append(state.BackingServices[:index], state.BackingServices[index+1:]...)
		return nil
	})
	return service, err
}

func (s *Store) ListServiceBindings(appID string) ([]model.ServiceBinding, error) {
	if strings.TrimSpace(appID) == "" {
		return nil, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgListServiceBindings(appID)
	}
	var bindings []model.ServiceBinding
	err := s.withLockedState(false, func(state *model.State) error {
		if findApp(state, appID) < 0 {
			return ErrNotFound
		}
		for _, binding := range state.ServiceBindings {
			if binding.AppID != appID {
				continue
			}
			bindings = append(bindings, cloneServiceBinding(binding))
		}
		sortServiceBindings(bindings)
		return nil
	})
	return bindings, err
}

func (s *Store) BindBackingService(tenantID, appID, serviceID, alias string, env map[string]string) (model.ServiceBinding, error) {
	if tenantID == "" || appID == "" || serviceID == "" {
		return model.ServiceBinding{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgBindBackingService(tenantID, appID, serviceID, alias, env)
	}
	var binding model.ServiceBinding
	err := s.withLockedState(true, func(state *model.State) error {
		appIndex := findApp(state, appID)
		if appIndex < 0 {
			return ErrNotFound
		}
		app := state.Apps[appIndex]
		if app.TenantID != tenantID || isDeletedApp(app) {
			return ErrNotFound
		}
		serviceIndex := findBackingService(state, serviceID)
		if serviceIndex < 0 {
			return ErrNotFound
		}
		service := state.BackingServices[serviceIndex]
		if isDeletedBackingService(service) || service.TenantID != tenantID {
			return ErrNotFound
		}
		if findServiceBindingByAppAndService(state, appID, serviceID) >= 0 {
			return ErrConflict
		}
		if requiresExclusiveBinding(service) && hasBindingsOnOtherApps(state.ServiceBindings, serviceID, appID) {
			return ErrConflict
		}
		now := time.Now().UTC()
		binding = model.ServiceBinding{
			ID:        model.NewID("binding"),
			TenantID:  tenantID,
			AppID:     appID,
			ServiceID: serviceID,
			Alias:     defaultServiceBindingAlias(alias, service),
			Env:       cloneMap(env),
			CreatedAt: now,
			UpdatedAt: now,
		}
		if err := normalizeBindingForPersist(&binding, service); err != nil {
			return err
		}
		state.ServiceBindings = append(state.ServiceBindings, binding)
		return nil
	})
	return binding, err
}

func (s *Store) UnbindBackingService(bindingID string) (model.ServiceBinding, error) {
	if strings.TrimSpace(bindingID) == "" {
		return model.ServiceBinding{}, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgUnbindBackingService(bindingID)
	}
	var binding model.ServiceBinding
	err := s.withLockedState(true, func(state *model.State) error {
		index := findServiceBinding(state, bindingID)
		if index < 0 {
			return ErrNotFound
		}
		binding = cloneServiceBinding(state.ServiceBindings[index])
		state.ServiceBindings = append(state.ServiceBindings[:index], state.ServiceBindings[index+1:]...)
		return nil
	})
	return binding, err
}

func hydrateAppBackingServices(state *model.State, app *model.App) {
	if app == nil {
		return
	}
	bindings := make([]model.ServiceBinding, 0)
	servicesByID := make(map[string]model.BackingService)
	for _, binding := range state.ServiceBindings {
		if binding.AppID != app.ID {
			continue
		}
		serviceIndex := findBackingService(state, binding.ServiceID)
		if serviceIndex < 0 {
			continue
		}
		service := state.BackingServices[serviceIndex]
		if isDeletedBackingService(service) {
			continue
		}
		bindings = append(bindings, cloneServiceBinding(binding))
		servicesByID[service.ID] = cloneBackingService(service)
	}
	sortServiceBindings(bindings)

	services := make([]model.BackingService, 0, len(servicesByID))
	for _, service := range servicesByID {
		services = append(services, service)
	}
	sortBackingServices(services)

	if app.Spec.Postgres != nil && !hasBackingServiceType(services, model.BackingServiceTypePostgres) {
		service, binding := legacyInlinePostgresResources(*app)
		services = append(services, service)
		bindings = append(bindings, binding)
		sortBackingServices(services)
		sortServiceBindings(bindings)
	}

	app.Bindings = bindings
	app.BackingServices = services
}

func migrateLegacyAppBackingServicesState(state *model.State) error {
	changed := false
	for index := range state.Apps {
		app := &state.Apps[index]
		if app.Spec.Postgres == nil || isDeletedApp(*app) {
			continue
		}
		if hasAppOwnedPostgresService(state, app.ID) || appHasBindingToServiceType(state, app.ID, model.BackingServiceTypePostgres) {
			app.Spec.Postgres = nil
			changed = true
			continue
		}
		service, binding := ownedLegacyPostgresResources(*app)
		service.Name = nextAvailableBackingServiceName(state, app.TenantID, app.ProjectID, service.Name)
		service.Spec.Postgres.ServiceName = postgresServiceNameForApp(app.Name)
		state.BackingServices = append(state.BackingServices, service)
		state.ServiceBindings = append(state.ServiceBindings, binding)
		app.Spec.Postgres = nil
		changed = true
	}
	if changed {
		repairAllAppStatuses(state)
	}
	return nil
}

func applyDesiredSpecBackingServicesState(state *model.State, app *model.App, desiredSpec *model.AppSpec) error {
	if state == nil || app == nil || desiredSpec == nil || desiredSpec.Postgres == nil {
		return nil
	}
	if err := validateManagedPostgresSpecForAppName(app.Name, desiredSpec.Postgres); err != nil {
		return err
	}

	if serviceIndex := findOwnedBackingServiceByAppAndType(state, app.ID, model.BackingServiceTypePostgres); serviceIndex >= 0 {
		now := time.Now().UTC()
		service := cloneBackingService(state.BackingServices[serviceIndex])
		normalized := normalizeManagedPostgresSpec(app.TenantID, appNameForService(&service, app), *desiredSpec.Postgres)
		service.Type = model.BackingServiceTypePostgres
		service.Provisioner = model.BackingServiceProvisionerManaged
		service.Status = model.BackingServiceStatusActive
		service.Spec.Postgres = &normalized
		service.UpdatedAt = now
		state.BackingServices[serviceIndex] = service
		ensureAppServiceBindingState(state, *app, service, defaultPostgresBindingEnv(normalized), now)
		desiredSpec.Postgres = nil
		return nil
	}

	if appHasBindingToServiceType(state, app.ID, model.BackingServiceTypePostgres) {
		desiredSpec.Postgres = nil
		return nil
	}

	appCopy := *app
	if cloned := cloneAppSpec(desiredSpec); cloned != nil {
		appCopy.Spec = *cloned
	}
	service, binding := ownedLegacyPostgresResources(appCopy)
	service.Name = nextAvailableBackingServiceName(state, app.TenantID, app.ProjectID, service.Name)
	state.BackingServices = append(state.BackingServices, service)
	state.ServiceBindings = append(state.ServiceBindings, binding)
	desiredSpec.Postgres = nil
	return nil
}

func isDeletedBackingService(service model.BackingService) bool {
	return strings.EqualFold(strings.TrimSpace(service.Status), model.BackingServiceStatusDeleted)
}

func cloneBackingService(service model.BackingService) model.BackingService {
	out := service
	out.Spec = cloneBackingServiceSpec(service.Spec)
	return out
}

func cloneBackingServiceSpec(spec model.BackingServiceSpec) model.BackingServiceSpec {
	out := spec
	if spec.Postgres != nil {
		postgres := *spec.Postgres
		if spec.Postgres.Resources != nil {
			resources := *spec.Postgres.Resources
			postgres.Resources = &resources
		}
		out.Postgres = &postgres
	}
	return out
}

func cloneServiceBinding(binding model.ServiceBinding) model.ServiceBinding {
	out := binding
	out.Env = cloneMap(binding.Env)
	return out
}

func backingServiceTypeFromSpec(spec model.BackingServiceSpec) string {
	switch {
	case spec.Postgres != nil:
		return model.BackingServiceTypePostgres
	default:
		return ""
	}
}

func normalizeBackingServiceForPersist(service *model.BackingService, app *model.App) error {
	if service == nil {
		return ErrInvalidInput
	}
	service.Name = model.Slugify(service.Name)
	if service.Name == "" {
		service.Name = "service"
	}
	if service.Provisioner == "" {
		service.Provisioner = model.BackingServiceProvisionerManaged
	}
	if service.Status == "" {
		service.Status = model.BackingServiceStatusActive
	}
	if service.Type == "" {
		service.Type = backingServiceTypeFromSpec(service.Spec)
	}
	switch service.Type {
	case model.BackingServiceTypePostgres:
		if service.Spec.Postgres == nil {
			return ErrInvalidInput
		}
		if err := normalizePostgresSpecResources(service.Spec.Postgres); err != nil {
			return err
		}
		if err := validateManagedPostgresSpecForAppName(appNameForService(service, app), service.Spec.Postgres); err != nil {
			return err
		}
		normalized := normalizeManagedPostgresSpec(service.TenantID, appNameForService(service, app), *service.Spec.Postgres)
		service.Spec.Postgres = &normalized
		if strings.TrimSpace(service.Description) == "" {
			service.Description = "Managed postgres service"
			if app != nil {
				service.Description = "Managed postgres for " + app.Name
			}
		}
	default:
		return ErrInvalidInput
	}
	return nil
}

func validateManagedPostgresSpecForAppName(appName string, spec *model.AppPostgresSpec) error {
	if spec == nil {
		return nil
	}
	if err := model.ValidateManagedPostgresUser(appName, *spec); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidInput, err)
	}
	return nil
}

func normalizeBindingForPersist(binding *model.ServiceBinding, service model.BackingService) error {
	if binding == nil {
		return ErrInvalidInput
	}
	binding.Alias = defaultServiceBindingAlias(binding.Alias, service)
	if len(binding.Env) == 0 {
		switch service.Type {
		case model.BackingServiceTypePostgres:
			if service.Spec.Postgres == nil {
				return ErrInvalidInput
			}
			binding.Env = defaultPostgresBindingEnv(*service.Spec.Postgres)
		default:
			return ErrInvalidInput
		}
	}
	return nil
}

func normalizeManagedPostgresSpec(_ string, appName string, spec model.AppPostgresSpec) model.AppPostgresSpec {
	out := spec
	resourceName := postgresServiceNameForApp(appName)
	if strings.TrimSpace(out.Image) == "" {
		out.Image = defaultManagedBackingPostgresImage
	}
	if strings.TrimSpace(out.Database) == "" {
		out.Database = serviceResourceName(appName)
	}
	if strings.TrimSpace(out.User) == "" {
		out.User = model.DefaultManagedPostgresUser(appName, out.StoragePath)
	}
	if strings.TrimSpace(out.ServiceName) == "" {
		out.ServiceName = resourceName
	}
	if strings.TrimSpace(out.StorageSize) == "" {
		out.StorageSize = defaultManagedBackingPostgresStorage
	}
	out.StorageClassName = strings.TrimSpace(out.StorageClassName)
	if out.Instances <= 0 {
		out.Instances = defaultManagedBackingPostgresInstances
	}
	if out.SynchronousReplicas < 0 {
		out.SynchronousReplicas = 0
	}
	if out.SynchronousReplicas == 0 && out.Instances > 1 {
		out.SynchronousReplicas = defaultManagedBackingPostgresSynchronousReplicas
	}
	if out.SynchronousReplicas >= out.Instances {
		out.SynchronousReplicas = out.Instances - 1
	}
	resources, err := normalizeWorkloadResources(out.Resources, model.DefaultManagedPostgresResources())
	if err != nil {
		fallback := model.DefaultManagedPostgresResources()
		resources = &fallback
	}
	out.Resources = resources
	return out
}

func ownedLegacyPostgresResources(app model.App) (model.BackingService, model.ServiceBinding) {
	service, binding := legacyInlinePostgresResources(app)
	now := time.Now().UTC()
	service.ID = model.NewID("service")
	service.TenantID = app.TenantID
	service.ProjectID = app.ProjectID
	service.OwnerAppID = app.ID
	service.Name = defaultOwnedBackingServiceName(app.Name, service.Type)
	service.Description = "Managed postgres for " + app.Name
	service.Provisioner = model.BackingServiceProvisionerManaged
	service.Status = model.BackingServiceStatusActive
	service.CreatedAt = now
	service.UpdatedAt = now

	binding.ID = model.NewID("binding")
	binding.TenantID = app.TenantID
	binding.AppID = app.ID
	binding.ServiceID = service.ID
	binding.Alias = defaultServiceBindingAlias("", service)
	binding.CreatedAt = now
	binding.UpdatedAt = now
	return service, binding
}

func legacyInlinePostgresResources(app model.App) (model.BackingService, model.ServiceBinding) {
	spec := normalizeManagedPostgresSpec(app.TenantID, app.Name, *app.Spec.Postgres)
	service := model.BackingService{
		ID:          "legacy-postgres-" + app.ID,
		TenantID:    app.TenantID,
		ProjectID:   app.ProjectID,
		OwnerAppID:  app.ID,
		Name:        defaultOwnedBackingServiceName(app.Name, model.BackingServiceTypePostgres),
		Description: "Managed postgres for " + app.Name,
		Type:        model.BackingServiceTypePostgres,
		Provisioner: model.BackingServiceProvisionerManaged,
		Status:      model.BackingServiceStatusActive,
		Spec: model.BackingServiceSpec{
			Postgres: &spec,
		},
	}
	binding := model.ServiceBinding{
		ID:        "legacy-postgres-binding-" + app.ID,
		TenantID:  app.TenantID,
		AppID:     app.ID,
		ServiceID: service.ID,
		Alias:     "postgres",
		Env:       defaultPostgresBindingEnv(spec),
	}
	return service, binding
}

func defaultOwnedBackingServiceName(appName, serviceType string) string {
	switch serviceType {
	case model.BackingServiceTypePostgres:
		return serviceResourceName(appName)
	default:
		return serviceResourceName(appName) + "-service"
	}
}

func defaultServiceBindingAlias(alias string, service model.BackingService) string {
	alias = strings.TrimSpace(alias)
	if alias != "" {
		return alias
	}
	switch service.Type {
	case model.BackingServiceTypePostgres:
		return "postgres"
	default:
		return service.Name
	}
}

func defaultPostgresBindingEnv(spec model.AppPostgresSpec) map[string]string {
	return map[string]string{
		"DB_TYPE":     "postgres",
		"DB_HOST":     model.PostgresRWServiceName(spec.ServiceName),
		"DB_PORT":     "5432",
		"DB_USER":     spec.User,
		"DB_PASSWORD": spec.Password,
		"DB_NAME":     spec.Database,
	}
}

func serviceNamespaceForTenant(tenantID string) string {
	tenantID = model.Slugify(strings.ReplaceAll(tenantID, "_", "-"))
	if len(tenantID) > 32 {
		tenantID = tenantID[:32]
	}
	return "fg-" + tenantID
}

func serviceResourceName(name string) string {
	name = model.Slugify(name)
	if len(name) > 50 {
		return name[:50]
	}
	return name
}

func postgresServiceNameForApp(appName string) string {
	return serviceResourceName(appName) + "-postgres"
}

func appNameForService(service *model.BackingService, app *model.App) string {
	if app != nil {
		return app.Name
	}
	if strings.TrimSpace(service.Name) != "" {
		name := service.Name
		if strings.HasSuffix(name, "-postgres") {
			return strings.TrimSuffix(name, "-postgres")
		}
		return name
	}
	return "service"
}

func sortBackingServices(services []model.BackingService) {
	sort.Slice(services, func(i, j int) bool {
		if services[i].CreatedAt.Equal(services[j].CreatedAt) {
			return services[i].ID < services[j].ID
		}
		return services[i].CreatedAt.Before(services[j].CreatedAt)
	})
}

func sortServiceBindings(bindings []model.ServiceBinding) {
	sort.Slice(bindings, func(i, j int) bool {
		if bindings[i].CreatedAt.Equal(bindings[j].CreatedAt) {
			return bindings[i].ID < bindings[j].ID
		}
		return bindings[i].CreatedAt.Before(bindings[j].CreatedAt)
	})
}

func hasBackingServiceType(services []model.BackingService, serviceType string) bool {
	for _, service := range services {
		if strings.EqualFold(service.Type, serviceType) {
			return true
		}
	}
	return false
}

func backingServiceNameExists(state *model.State, tenantID, projectID, name, exceptID string) bool {
	for _, service := range state.BackingServices {
		if service.ID == exceptID || isDeletedBackingService(service) {
			continue
		}
		if service.TenantID == tenantID && service.ProjectID == projectID && strings.EqualFold(service.Name, name) {
			return true
		}
	}
	return false
}

func nextAvailableBackingServiceName(state *model.State, tenantID, projectID, base string) string {
	base = model.Slugify(base)
	if base == "" {
		base = "service"
	}
	candidate := base
	for attempt := 1; backingServiceNameExists(state, tenantID, projectID, candidate, ""); attempt++ {
		candidate = fmt.Sprintf("%s-%d", base, attempt+1)
	}
	return candidate
}

func hasServiceBindings(state *model.State, serviceID string) bool {
	for _, binding := range state.ServiceBindings {
		if binding.ServiceID == serviceID {
			return true
		}
	}
	return false
}

func hasAppOwnedPostgresService(state *model.State, appID string) bool {
	for _, service := range state.BackingServices {
		if service.OwnerAppID == appID && service.Type == model.BackingServiceTypePostgres && !isDeletedBackingService(service) {
			return true
		}
	}
	return false
}

func appHasBindingToServiceType(state *model.State, appID, serviceType string) bool {
	for _, binding := range state.ServiceBindings {
		if binding.AppID != appID {
			continue
		}
		serviceIndex := findBackingService(state, binding.ServiceID)
		if serviceIndex < 0 {
			continue
		}
		if state.BackingServices[serviceIndex].Type == serviceType && !isDeletedBackingService(state.BackingServices[serviceIndex]) {
			return true
		}
	}
	return false
}

func ensureAppServiceBindingState(state *model.State, app model.App, service model.BackingService, env map[string]string, now time.Time) {
	index := findServiceBindingByAppAndService(state, app.ID, service.ID)
	if index >= 0 {
		state.ServiceBindings[index].Alias = defaultServiceBindingAlias(state.ServiceBindings[index].Alias, service)
		state.ServiceBindings[index].Env = cloneMap(env)
		state.ServiceBindings[index].UpdatedAt = now
		return
	}

	state.ServiceBindings = append(state.ServiceBindings, model.ServiceBinding{
		ID:        model.NewID("binding"),
		TenantID:  app.TenantID,
		AppID:     app.ID,
		ServiceID: service.ID,
		Alias:     defaultServiceBindingAlias("", service),
		Env:       cloneMap(env),
		CreatedAt: now,
		UpdatedAt: now,
	})
}

func deleteBackingServicesByTenant(services []model.BackingService, tenantID string) []model.BackingService {
	filtered := services[:0]
	for _, service := range services {
		if service.TenantID == tenantID {
			continue
		}
		filtered = append(filtered, service)
	}
	return filtered
}

func deleteBackingServicesByProject(services []model.BackingService, projectID string) []model.BackingService {
	filtered := services[:0]
	for _, service := range services {
		if service.ProjectID == projectID {
			continue
		}
		filtered = append(filtered, service)
	}
	return filtered
}

func deleteServiceBindingsByTenant(bindings []model.ServiceBinding, tenantID string) []model.ServiceBinding {
	filtered := bindings[:0]
	for _, binding := range bindings {
		if binding.TenantID == tenantID {
			continue
		}
		filtered = append(filtered, binding)
	}
	return filtered
}

func deleteServiceBindingsByApp(bindings []model.ServiceBinding, appID string) []model.ServiceBinding {
	filtered := bindings[:0]
	for _, binding := range bindings {
		if binding.AppID == appID {
			continue
		}
		filtered = append(filtered, binding)
	}
	return filtered
}

func deleteServiceBindingsByAppIDs(bindings []model.ServiceBinding, appIDs []string) []model.ServiceBinding {
	if len(appIDs) == 0 {
		return bindings
	}
	remove := make(map[string]struct{}, len(appIDs))
	for _, appID := range appIDs {
		if strings.TrimSpace(appID) == "" {
			continue
		}
		remove[appID] = struct{}{}
	}
	filtered := bindings[:0]
	for _, binding := range bindings {
		if _, ok := remove[binding.AppID]; ok {
			continue
		}
		filtered = append(filtered, binding)
	}
	return filtered
}

func deleteOwnedBackingServicesByApp(services []model.BackingService, appID string) []model.BackingService {
	filtered := services[:0]
	for _, service := range services {
		if service.OwnerAppID == appID {
			continue
		}
		filtered = append(filtered, service)
	}
	return filtered
}

func findBackingService(state *model.State, id string) int {
	for index, service := range state.BackingServices {
		if service.ID == id {
			return index
		}
	}
	return -1
}

func findOwnedBackingServiceByAppAndType(state *model.State, appID, serviceType string) int {
	for index, service := range state.BackingServices {
		if service.OwnerAppID != appID {
			continue
		}
		if isDeletedBackingService(service) {
			continue
		}
		if strings.EqualFold(service.Type, serviceType) {
			return index
		}
	}
	return -1
}

func findServiceBinding(state *model.State, id string) int {
	for index, binding := range state.ServiceBindings {
		if binding.ID == id {
			return index
		}
	}
	return -1
}

func findServiceBindingByAppAndService(state *model.State, appID, serviceID string) int {
	for index, binding := range state.ServiceBindings {
		if binding.AppID == appID && binding.ServiceID == serviceID {
			return index
		}
	}
	return -1
}

func hasBindingsOnOtherApps(bindings []model.ServiceBinding, serviceID, appID string) bool {
	for _, binding := range bindings {
		if binding.ServiceID == serviceID && binding.AppID != appID {
			return true
		}
	}
	return false
}

func requiresExclusiveBinding(service model.BackingService) bool {
	if !strings.EqualFold(strings.TrimSpace(service.Type), model.BackingServiceTypePostgres) {
		return false
	}
	provisioner := strings.TrimSpace(strings.ToLower(service.Provisioner))
	return provisioner == "" || provisioner == model.BackingServiceProvisionerManaged
}
