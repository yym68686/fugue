package store

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"fugue/internal/storagerecovery"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	defaultManagedBackingPostgresImage     = ""
	defaultManagedBackingPostgresInstances = 1
)

func (s *Store) ListBackingServices(tenantID string, platformAdmin bool) ([]model.BackingService, error) {
	return s.pgListBackingServices(tenantID, platformAdmin)
}

func (s *Store) GetBackingService(id string) (model.BackingService, error) {
	return s.pgGetBackingService(id)
}

func (s *Store) CreateBackingService(tenantID, projectID, name, description string, spec model.BackingServiceSpec) (model.BackingService, error) {
	if tenantID == "" || projectID == "" || strings.TrimSpace(name) == "" {
		return model.BackingService{}, ErrInvalidInput
	}
	if err := reconcileManagedPostgresRuntimeResources(spec.Postgres, nil); err != nil {
		return model.BackingService{}, err
	}
	if spec.Postgres != nil && spec.Postgres.Suspended {
		return model.BackingService{}, ErrInvalidInput
	}
	return s.pgCreateBackingService(tenantID, projectID, name, description, spec)
}

func (s *Store) DeleteBackingService(id string) (model.BackingService, error) {
	return s.pgDeleteBackingService(id)
}

func (s *Store) UpdateBackingServiceSpec(id string, spec model.BackingServiceSpec) (model.BackingService, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return model.BackingService{}, ErrInvalidInput
	}
	return s.pgUpdateBackingServiceSpec(id, spec)
}

func (s *Store) ListServiceBindings(appID string) ([]model.ServiceBinding, error) {
	if strings.TrimSpace(appID) == "" {
		return nil, ErrInvalidInput
	}
	return s.pgListServiceBindings(appID)
}

func (s *Store) BindBackingService(tenantID, appID, serviceID, alias string, env map[string]string) (model.ServiceBinding, error) {
	if tenantID == "" || appID == "" || serviceID == "" {
		return model.ServiceBinding{}, ErrInvalidInput
	}
	return s.pgBindBackingService(tenantID, appID, serviceID, alias, env)
}

func (s *Store) UnbindBackingService(bindingID string) (model.ServiceBinding, error) {
	if strings.TrimSpace(bindingID) == "" {
		return model.ServiceBinding{}, ErrInvalidInput
	}
	return s.pgUnbindBackingService(bindingID)
}

func hydrateAppBackingServices(state *storeProjection, app *model.App) {
	hydrateAppBackingServicesWithIndex(newAppBackingServiceIndex(state), app)
}

func applyDesiredSpecBackingServicesState(state *storeProjection, app *model.App, desiredSpec *model.AppSpec) error {
	if state == nil || app == nil || desiredSpec == nil || desiredSpec.Postgres == nil {
		return nil
	}
	if serviceIndex := findManagedBackingServiceByAppAndType(state, app.ID, model.BackingServiceTypePostgres); serviceIndex >= 0 {
		now := time.Now().UTC()
		service := cloneBackingService(state.BackingServices[serviceIndex])
		if err := reconcileManagedPostgresRuntimeResources(desiredSpec.Postgres, service.Spec.Postgres); err != nil {
			return err
		}
		if err := ensureManagedPostgresPasswordWithExisting(desiredSpec.Postgres, service.Spec.Postgres); err != nil {
			return err
		}
		if err := validateManagedPostgresSuspensionTransition(desiredSpec.Postgres, service.Spec.Postgres); err != nil {
			return err
		}
		if err := validateManagedPostgresSpecForAppName(app.Name, desiredSpec.Postgres); err != nil {
			return err
		}
		normalized := normalizeManagedPostgresSpec(appNameForService(&service, app), app.Spec.RuntimeID, *desiredSpec.Postgres)
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

	if err := reconcileManagedPostgresRuntimeResources(desiredSpec.Postgres, nil); err != nil {
		return err
	}
	if err := ensureManagedPostgresPassword(desiredSpec.Postgres); err != nil {
		return err
	}
	if desiredSpec.Postgres.Suspended {
		return ErrInvalidInput
	}
	if err := validateManagedPostgresSpecForAppName(app.Name, desiredSpec.Postgres); err != nil {
		return err
	}

	appCopy := *app
	if cloned := cloneAppSpec(desiredSpec); cloned != nil {
		appCopy.Spec = *cloned
	}
	service, binding := ownedManagedPostgresResources(appCopy)
	service.Name = nextAvailableBackingServiceName(state, app.TenantID, app.ProjectID, service.Name)
	state.BackingServices = append(state.BackingServices, service)
	state.ServiceBindings = append(state.ServiceBindings, binding)
	desiredSpec.Postgres = nil
	return nil
}

func applyManagedPostgresLifecycleState(state *storeProjection, app *model.App, op *model.Operation) error {
	if state == nil || app == nil || op == nil || op.DesiredSpec == nil || op.DesiredSpec.Postgres == nil {
		return ErrInvalidInput
	}
	serviceID := strings.TrimSpace(op.ServiceID)
	serviceIndex := findBackingService(state, serviceID)
	if serviceID == "" || serviceIndex < 0 {
		return ErrInvalidInput
	}
	service := cloneBackingService(state.BackingServices[serviceIndex])
	if service.TenantID != app.TenantID || service.ProjectID != app.ProjectID || !isManagedPostgresService(service) || service.Spec.Postgres == nil {
		return ErrInvalidInput
	}
	ownerAppID := strings.TrimSpace(service.OwnerAppID)
	if ownerAppID != strings.TrimSpace(app.ID) {
		if ownerAppID != "" || findServiceBindingByAppAndService(state, app.ID, service.ID) < 0 {
			return ErrInvalidInput
		}
	}
	wantSuspended := op.Type == model.OperationTypeDatabaseSuspend
	if op.Type != model.OperationTypeDatabaseSuspend && op.Type != model.OperationTypeDatabaseResume {
		return ErrInvalidInput
	}
	if op.DesiredSpec.Postgres.Suspended != wantSuspended {
		return ErrInvalidInput
	}
	postgres := *model.CloneAppPostgresSpec(service.Spec.Postgres)
	postgres.Suspended = wantSuspended
	service.Spec.Postgres = &postgres
	service.UpdatedAt = time.Now().UTC()
	state.BackingServices[serviceIndex] = service
	// PostgreSQL remains externalized from AppSpec. Only the exact backing
	// service selected by ServiceID is changed by this operation.
	op.DesiredSpec.Postgres = nil
	return nil
}

func OverlayDesiredManagedPostgres(app model.App) (model.App, error) {
	if app.Spec.Postgres == nil {
		return app, nil
	}
	if err := validateManagedPostgresSpecForAppName(app.Name, app.Spec.Postgres); err != nil {
		return model.App{}, err
	}

	out := app
	normalized := normalizeManagedPostgresSpec(out.Name, out.Spec.RuntimeID, *out.Spec.Postgres)

	if index := findAppManagedBackingServiceByType(out, model.BackingServiceTypePostgres); index >= 0 {
		service := out.BackingServices[index]
		serviceCopy := cloneBackingService(service)
		serviceCopy.Spec.Postgres = &normalized
		out.BackingServices[index] = serviceCopy
		ensureBoundPostgresViewBinding(&out, serviceCopy, normalized)
		out.Spec.Postgres = nil
		return out, nil
	}

	if appHasBoundServiceType(out, model.BackingServiceTypePostgres) {
		out.Spec.Postgres = nil
		return out, nil
	}

	out.Spec.Postgres = &normalized
	service, binding := appManagedPostgresResources(out)
	out.BackingServices = append(out.BackingServices, service)
	out.Bindings = append(out.Bindings, binding)
	sortBackingServices(out.BackingServices)
	sortServiceBindings(out.Bindings)
	out.Spec.Postgres = nil
	return out, nil
}

// findAppManagedBackingServiceByType keeps the app projection compatible with
// legacy backing-service rows that have no owner_app_id. A valid binding is
// the relationship of record in that case, so desired Postgres changes must
// update the bound service instead of being silently discarded.
func findAppManagedBackingServiceByType(app model.App, serviceType string) int {
	for index, service := range app.BackingServices {
		if isDeletedBackingService(service) || service.Spec.Postgres == nil && serviceType == model.BackingServiceTypePostgres {
			continue
		}
		if strings.EqualFold(service.Type, serviceType) && strings.TrimSpace(service.OwnerAppID) == strings.TrimSpace(app.ID) {
			return index
		}
	}
	for _, binding := range app.Bindings {
		if strings.TrimSpace(binding.AppID) != strings.TrimSpace(app.ID) {
			continue
		}
		for index, service := range app.BackingServices {
			if service.ID != binding.ServiceID || isDeletedBackingService(service) {
				continue
			}
			if serviceType == model.BackingServiceTypePostgres && service.Spec.Postgres == nil {
				continue
			}
			if strings.EqualFold(service.Type, serviceType) {
				return index
			}
		}
	}
	return -1
}

func isDeletedBackingService(service model.BackingService) bool {
	return strings.EqualFold(strings.TrimSpace(service.Status), model.BackingServiceStatusDeleted)
}

func cloneBackingService(service model.BackingService) model.BackingService {
	out := service
	out.Spec = cloneBackingServiceSpec(service.Spec)
	if service.RuntimeStatus != nil {
		status := *service.RuntimeStatus
		out.RuntimeStatus = &status
	}
	return out
}

func cloneBackingServiceSpec(spec model.BackingServiceSpec) model.BackingServiceSpec {
	out := spec
	out.Postgres = model.CloneAppPostgresSpec(spec.Postgres)
	return out
}

func backingServiceSpecRuntimeID(service model.BackingService) string {
	if service.Spec.Postgres != nil {
		return strings.TrimSpace(service.Spec.Postgres.RuntimeID)
	}
	return ""
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
		if err := ensureManagedPostgresPassword(service.Spec.Postgres); err != nil {
			return err
		}
		if err := normalizePostgresSpecResources(service.Spec.Postgres); err != nil {
			return err
		}
		if err := validateManagedPostgresSpecForAppName(appNameForService(service, app), service.Spec.Postgres); err != nil {
			return err
		}
		runtimeID := ""
		if app != nil {
			runtimeID = app.Spec.RuntimeID
		}
		normalized := normalizeManagedPostgresSpec(appNameForService(service, app), runtimeID, *service.Spec.Postgres)
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

func ensureManagedPostgresPassword(spec *model.AppPostgresSpec) error {
	if spec == nil || strings.TrimSpace(spec.Password) != "" {
		return nil
	}
	password, err := randomHexString(24)
	if err != nil {
		return fmt.Errorf("generate managed postgres password: %w", err)
	}
	spec.Password = password
	return nil
}

func ensureManagedPostgresPasswordWithExisting(spec *model.AppPostgresSpec, existing *model.AppPostgresSpec) error {
	if spec == nil || strings.TrimSpace(spec.Password) != "" {
		return nil
	}
	if existing != nil && strings.TrimSpace(existing.Password) != "" {
		spec.Password = existing.Password
		return nil
	}
	return ensureManagedPostgresPassword(spec)
}

func randomHexString(numBytes int) (string, error) {
	if numBytes <= 0 {
		numBytes = 16
	}
	buf := make([]byte, numBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
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

// reconcileManagedPostgresRuntimeResources makes RuntimeResources a
// server-owned field for every ordinary app and backing-service mutation. A
// caller may round-trip the exact current value, but cannot create or change
// it. Omitting the read-only field preserves the persisted target. The
// dedicated in-place resize operation updates it through a narrower store
// transaction instead of this helper.
func reconcileManagedPostgresRuntimeResources(desired, current *model.AppPostgresSpec) error {
	if desired == nil {
		return nil
	}
	if desired.EndpointServiceName != "" && (current == nil || desired.EndpointServiceName != current.EndpointServiceName) {
		return ErrInvalidInput
	}
	if current != nil {
		desired.EndpointServiceName = current.EndpointServiceName
	}
	if desired.CredentialSecretName != "" && (current == nil || desired.CredentialSecretName != current.CredentialSecretName) {
		return ErrInvalidInput
	}
	if current != nil {
		desired.CredentialSecretName = current.CredentialSecretName
	}
	if desired.RuntimeResources != nil {
		if current == nil || current.RuntimeResources == nil || *desired.RuntimeResources != *current.RuntimeResources {
			return ErrInvalidInput
		}
	}
	if current == nil {
		desired.RuntimeResources = nil
		return nil
	}
	desired.RuntimeResources = model.CloneResourceSpec(current.RuntimeResources)
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

func normalizeManagedPostgresSpec(appName, appRuntimeID string, spec model.AppPostgresSpec) model.AppPostgresSpec {
	out := *model.CloneAppPostgresSpec(&spec)
	resourceName := postgresServiceNameForApp(appName)
	out.Image = model.NormalizeManagedPostgresImage(out.Image)
	if strings.TrimSpace(out.Image) == "" {
		out.Image = defaultManagedBackingPostgresImage
	}
	if strings.TrimSpace(out.Database) == "" {
		out.Database = serviceResourceName(appName)
	}
	if strings.TrimSpace(out.User) == "" {
		out.User = model.DefaultManagedPostgresUser(appName)
	}
	out.ServiceName = model.NormalizePostgresServiceName(out.ServiceName, resourceName)
	out.RuntimeID = strings.TrimSpace(out.RuntimeID)
	if out.RuntimeID == "" {
		out.RuntimeID = strings.TrimSpace(appRuntimeID)
	}
	out.FailoverTargetRuntimeID = strings.TrimSpace(out.FailoverTargetRuntimeID)
	out.PrimaryNodeName = strings.TrimSpace(out.PrimaryNodeName)
	if strings.TrimSpace(out.StorageSize) == "" {
		out.StorageSize = model.DefaultManagedPostgresStorageSize
	}
	out.StorageClassName = strings.TrimSpace(out.StorageClassName)
	if out.Instances <= 0 {
		out.Instances = defaultManagedBackingPostgresInstances
	}
	if out.FailoverTargetRuntimeID != "" && out.Instances < 2 {
		out.Instances = 2
	}
	if out.SynchronousReplicas < 0 {
		out.SynchronousReplicas = 0
	}
	if out.FailoverTargetRuntimeID != "" && out.SynchronousReplicas < 1 {
		out.SynchronousReplicas = 1
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

func ownedManagedPostgresResources(app model.App) (model.BackingService, model.ServiceBinding) {
	service, binding := appManagedPostgresResources(app)
	now := time.Now().UTC()
	service.TenantID = app.TenantID
	service.ProjectID = app.ProjectID
	service.OwnerAppID = app.ID
	service.Name = defaultOwnedBackingServiceName(app.Name, service.Type)
	service.Description = "Managed postgres for " + app.Name
	service.Provisioner = model.BackingServiceProvisionerManaged
	service.Status = model.BackingServiceStatusActive
	service.CreatedAt = now
	service.UpdatedAt = now

	binding.TenantID = app.TenantID
	binding.AppID = app.ID
	binding.ServiceID = service.ID
	binding.Alias = defaultServiceBindingAlias("", service)
	binding.CreatedAt = now
	binding.UpdatedAt = now
	return service, binding
}

func appManagedPostgresResources(app model.App) (model.BackingService, model.ServiceBinding) {
	spec := normalizeManagedPostgresSpec(app.Name, app.Spec.RuntimeID, *app.Spec.Postgres)
	service := model.BackingService{
		ID:          "app-postgres-" + app.ID,
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
		ID:        "app-postgres-binding-" + app.ID,
		TenantID:  app.TenantID,
		AppID:     app.ID,
		ServiceID: service.ID,
		Alias:     "postgres",
		Env:       defaultPostgresBindingEnv(spec),
	}
	return service, binding
}

func appHasBoundServiceType(app model.App, serviceType string) bool {
	servicesByID := make(map[string]model.BackingService, len(app.BackingServices))
	for _, service := range app.BackingServices {
		servicesByID[service.ID] = service
	}

	for _, binding := range app.Bindings {
		service, ok := servicesByID[binding.ServiceID]
		if !ok {
			continue
		}
		if service.Type == serviceType && !isDeletedBackingService(service) {
			return true
		}
	}
	return false
}

func appHasManagedPostgresService(app model.App) bool {
	if app.Spec.Postgres != nil {
		return true
	}

	servicesByID := make(map[string]model.BackingService, len(app.BackingServices))
	for _, service := range app.BackingServices {
		servicesByID[service.ID] = service
	}

	for _, binding := range app.Bindings {
		service, ok := servicesByID[binding.ServiceID]
		if !ok {
			continue
		}
		if isManagedPostgresService(service) {
			return true
		}
	}

	for _, service := range app.BackingServices {
		if strings.TrimSpace(service.OwnerAppID) == strings.TrimSpace(app.ID) && isManagedPostgresService(service) {
			return true
		}
	}

	return false
}

func OwnedManagedPostgresSpec(app model.App) *model.AppPostgresSpec {
	if app.Spec.Postgres != nil {
		normalized := normalizeManagedPostgresSpec(app.Name, app.Spec.RuntimeID, *app.Spec.Postgres)
		return &normalized
	}

	for _, service := range app.BackingServices {
		if strings.TrimSpace(service.OwnerAppID) != strings.TrimSpace(app.ID) {
			continue
		}
		if !isManagedPostgresService(service) || service.Spec.Postgres == nil {
			continue
		}
		normalized := normalizeManagedPostgresSpec(appNameForService(&service, &app), app.Spec.RuntimeID, *service.Spec.Postgres)
		return &normalized
	}

	for _, service := range app.BackingServices {
		ownerAppID := strings.TrimSpace(service.OwnerAppID)
		if ownerAppID != "" && ownerAppID != strings.TrimSpace(app.ID) {
			continue
		}
		if !app.HasServiceBinding(service.ID) {
			continue
		}
		if !isManagedPostgresService(service) || service.Spec.Postgres == nil {
			continue
		}
		normalized := normalizeManagedPostgresSpec(appNameForService(&service, &app), app.Spec.RuntimeID, *service.Spec.Postgres)
		return &normalized
	}

	return nil
}

func prepareManagedPostgresLifecycleOperation(app model.App, op *model.Operation, suspended bool) error {
	if op == nil {
		return ErrInvalidInput
	}
	target, err := ManagedPostgresOperationTargetForApp(app, op.ServiceID)
	if err != nil || target == nil || target.Service == nil || strings.TrimSpace(target.ServiceID) == "" {
		return ErrInvalidInput
	}
	ownerAppID := strings.TrimSpace(target.Service.OwnerAppID)
	if ownerAppID != "" && ownerAppID != strings.TrimSpace(app.ID) {
		return ErrInvalidInput
	}
	// Lifecycle operations are deliberately narrow. Ignore a caller-supplied
	// app spec so suspend/resume cannot smuggle unrelated workload changes into
	// the completion transaction.
	op.DesiredSpec = cloneAppSpec(&app.Spec)
	if op.DesiredSpec == nil {
		return ErrInvalidInput
	}
	postgresCopy := *model.CloneAppPostgresSpec(&target.Postgres)
	postgresCopy.Suspended = suspended
	op.DesiredSpec.Postgres = &postgresCopy
	op.ServiceID = strings.TrimSpace(target.ServiceID)
	runtimeID := strings.TrimSpace(postgresCopy.RuntimeID)
	if runtimeID == "" {
		runtimeID = strings.TrimSpace(app.Spec.RuntimeID)
	}
	op.SourceRuntimeID = runtimeID
	op.TargetRuntimeID = runtimeID
	if err := validateManagedPostgresSpecForAppName(app.Name, op.DesiredSpec.Postgres); err != nil {
		return err
	}
	return nil
}

func validateManagedPostgresSuspensionTransition(desired, current *model.AppPostgresSpec) error {
	if desired == nil {
		return nil
	}
	if current == nil {
		if desired.Suspended {
			return ErrInvalidInput
		}
		return nil
	}
	if desired.Suspended != current.Suspended {
		return ErrInvalidInput
	}
	return nil
}

func validateManagedPostgresSuspensionForOperation(operationType string, desired, current *model.AppPostgresSpec) error {
	if operationType == model.OperationTypeDatabaseSuspend || operationType == model.OperationTypeDatabaseResume {
		return nil
	}
	return validateManagedPostgresSuspensionTransition(desired, current)
}

func managedPostgresServiceIsSuspended(service model.BackingService) bool {
	return isManagedPostgresService(service) && service.Spec.Postgres != nil && service.Spec.Postgres.Suspended
}

func appHasSuspendedManagedPostgres(app model.App) bool {
	if app.Spec.Postgres != nil && app.Spec.Postgres.Suspended {
		return true
	}
	for _, service := range app.BackingServices {
		if managedPostgresServiceIsSuspended(service) {
			return true
		}
	}
	return false
}

func appHasDesiredOrCurrentReplicas(app model.App) bool {
	return app.Spec.Replicas > 0 || app.Status.CurrentReplicas > 0
}

func operationWouldRunApp(app model.App, op model.Operation) bool {
	switch op.Type {
	case model.OperationTypeImport, model.OperationTypeDeploy:
		return op.DesiredSpec != nil && op.DesiredSpec.Replicas > 0
	case model.OperationTypeScale:
		return op.DesiredReplicas != nil && *op.DesiredReplicas > 0
	case model.OperationTypeMigrate, model.OperationTypeFailover:
		if op.DesiredSpec != nil {
			return op.DesiredSpec.Replicas > 0
		}
		return app.Spec.Replicas > 0 || app.Status.CurrentReplicas > 0
	default:
		return false
	}
}

func validateManagedPostgresActiveForOperation(app model.App, op model.Operation) error {
	switch op.Type {
	case model.OperationTypeMigrate,
		model.OperationTypeFailover,
		model.OperationTypeDatabaseSwitchover,
		model.OperationTypeDatabaseLocalize, storagerecovery.OperationType:
	default:
		return nil
	}
	postgres := ManagedPostgresSpecForOperation(app, op.ServiceID)
	if postgres != nil && postgres.Suspended {
		return ErrConflict
	}
	return nil
}

type ManagedPostgresOperationTarget struct {
	ServiceID string
	Service   *model.BackingService
	Postgres  model.AppPostgresSpec
	AppOwned  bool
}

func ManagedPostgresOperationTargetForApp(app model.App, serviceID string) (*ManagedPostgresOperationTarget, error) {
	serviceID = strings.TrimSpace(serviceID)
	if serviceID == "" {
		if app.Spec.Postgres != nil {
			normalized := normalizeManagedPostgresSpec(app.Name, app.Spec.RuntimeID, *app.Spec.Postgres)
			return &ManagedPostgresOperationTarget{
				Postgres: normalized,
				AppOwned: true,
			}, nil
		}
		for _, service := range app.BackingServices {
			if strings.TrimSpace(service.OwnerAppID) != strings.TrimSpace(app.ID) {
				continue
			}
			if !isManagedPostgresService(service) || service.Spec.Postgres == nil {
				continue
			}
			serviceCopy := cloneBackingService(service)
			normalized := normalizeManagedPostgresSpec(appNameForService(&serviceCopy, &app), app.Spec.RuntimeID, *serviceCopy.Spec.Postgres)
			serviceCopy.Spec.Postgres = &normalized
			return &ManagedPostgresOperationTarget{
				ServiceID: serviceCopy.ID,
				Service:   &serviceCopy,
				Postgres:  normalized,
				AppOwned:  true,
			}, nil
		}
		for _, service := range app.BackingServices {
			ownerAppID := strings.TrimSpace(service.OwnerAppID)
			if ownerAppID != "" && ownerAppID != strings.TrimSpace(app.ID) {
				continue
			}
			if !app.HasServiceBinding(service.ID) {
				continue
			}
			if !isManagedPostgresService(service) || service.Spec.Postgres == nil {
				continue
			}
			serviceCopy := cloneBackingService(service)
			normalized := normalizeManagedPostgresSpec(appNameForService(&serviceCopy, &app), app.Spec.RuntimeID, *serviceCopy.Spec.Postgres)
			serviceCopy.Spec.Postgres = &normalized
			return &ManagedPostgresOperationTarget{
				ServiceID: serviceCopy.ID,
				Service:   &serviceCopy,
				Postgres:  normalized,
				AppOwned:  strings.TrimSpace(serviceCopy.OwnerAppID) == strings.TrimSpace(app.ID),
			}, nil
		}
		return nil, nil
	}

	for _, service := range app.BackingServices {
		if strings.TrimSpace(service.ID) != serviceID {
			continue
		}
		if !isManagedPostgresService(service) || service.Spec.Postgres == nil {
			return nil, ErrInvalidInput
		}
		appOwned := strings.TrimSpace(service.OwnerAppID) == strings.TrimSpace(app.ID)
		if !appOwned && !app.HasServiceBinding(serviceID) {
			return nil, ErrInvalidInput
		}
		serviceCopy := cloneBackingService(service)
		normalized := normalizeManagedPostgresSpec(appNameForService(&serviceCopy, &app), app.Spec.RuntimeID, *serviceCopy.Spec.Postgres)
		serviceCopy.Spec.Postgres = &normalized
		return &ManagedPostgresOperationTarget{
			ServiceID: serviceCopy.ID,
			Service:   &serviceCopy,
			Postgres:  normalized,
			AppOwned:  appOwned,
		}, nil
	}
	return nil, ErrInvalidInput
}

func ManagedPostgresSpecForOperation(app model.App, serviceID string) *model.AppPostgresSpec {
	target, err := ManagedPostgresOperationTargetForApp(app, serviceID)
	if err != nil || target == nil {
		return nil
	}
	return model.CloneAppPostgresSpec(&target.Postgres)
}

func ensureBoundPostgresViewBinding(app *model.App, service model.BackingService, spec model.AppPostgresSpec) {
	if app == nil {
		return
	}
	for index, binding := range app.Bindings {
		if binding.ServiceID != service.ID {
			continue
		}
		bindingCopy := cloneServiceBinding(binding)
		bindingCopy.Alias = defaultServiceBindingAlias(bindingCopy.Alias, service)
		bindingCopy.Env = defaultPostgresBindingEnv(spec)
		app.Bindings[index] = bindingCopy
		return
	}

	app.Bindings = append(app.Bindings, model.ServiceBinding{
		ID:        "app-postgres-binding-" + app.ID,
		TenantID:  app.TenantID,
		AppID:     app.ID,
		ServiceID: service.ID,
		Alias:     defaultServiceBindingAlias("", service),
		Env:       defaultPostgresBindingEnv(spec),
	})
	sortServiceBindings(app.Bindings)
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
		"DB_HOST":     model.PostgresEndpointName(spec),
		"DB_PORT":     "5432",
		"DB_USER":     spec.User,
		"DB_PASSWORD": spec.Password,
		"DB_NAME":     spec.Database,
	}
}

func serviceResourceName(name string) string {
	name = model.Slugify(name)
	if len(name) > 50 {
		return name[:50]
	}
	return name
}

func postgresServiceNameForApp(appName string) string {
	return model.DNS1035Label(serviceResourceName(appName)+"-postgres", "postgres")
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

func backingServiceNameExists(state *storeProjection, tenantID, projectID, name, exceptID string) bool {
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

func nextAvailableBackingServiceName(state *storeProjection, tenantID, projectID, base string) string {
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

func hasServiceBindings(state *storeProjection, serviceID string) bool {
	for _, binding := range state.ServiceBindings {
		if binding.ServiceID == serviceID {
			return true
		}
	}
	return false
}

func appHasBindingToServiceType(state *storeProjection, appID, serviceType string) bool {
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

func ensureAppServiceBindingState(state *storeProjection, app model.App, service model.BackingService, env map[string]string, now time.Time) {
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

func findBackingService(state *storeProjection, id string) int {
	for index, service := range state.BackingServices {
		if service.ID == id {
			return index
		}
	}
	return -1
}

func findOwnedBackingServiceByAppAndType(state *storeProjection, appID, serviceType string) int {
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

// findManagedBackingServiceByAppAndType also accepts a valid service binding
// for legacy rows that predate owner_app_id. The binding is the relationship
// of record for those rows; silently discarding a desired update would leave
// the app and its backing service permanently divergent.
func findManagedBackingServiceByAppAndType(state *storeProjection, appID, serviceType string) int {
	if index := findOwnedBackingServiceByAppAndType(state, appID, serviceType); index >= 0 {
		return index
	}
	for _, binding := range state.ServiceBindings {
		if binding.AppID != appID {
			continue
		}
		index := findBackingService(state, binding.ServiceID)
		if index < 0 {
			continue
		}
		service := state.BackingServices[index]
		if isDeletedBackingService(service) || !strings.EqualFold(service.Type, serviceType) {
			continue
		}
		return index
	}
	return -1
}

func findServiceBinding(state *storeProjection, id string) int {
	for index, binding := range state.ServiceBindings {
		if binding.ID == id {
			return index
		}
	}
	return -1
}

func findServiceBindingByAppAndService(state *storeProjection, appID, serviceID string) int {
	for index, binding := range state.ServiceBindings {
		if binding.AppID == appID && binding.ServiceID == serviceID {
			return index
		}
	}
	return -1
}

func requiresExclusiveBinding(service model.BackingService) bool {
	return isManagedPostgresService(service)
}

func isManagedPostgresService(service model.BackingService) bool {
	if !strings.EqualFold(strings.TrimSpace(service.Type), model.BackingServiceTypePostgres) {
		return false
	}
	if isDeletedBackingService(service) {
		return false
	}
	provisioner := strings.TrimSpace(strings.ToLower(service.Provisioner))
	return provisioner == "" || provisioner == model.BackingServiceProvisionerManaged
}
