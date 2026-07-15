package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

type orphanManagedAppAdoption struct {
	App      model.App
	Services []model.BackingService
	Bindings []model.ServiceBinding
}

// AdoptOrphanManagedApp restores the control-plane records for a retained
// ManagedApp snapshot. The restored application is always disabled; this
// operation only re-establishes ownership of the existing managed PostgreSQL
// resource and never starts an application workload.
func (s *Store) AdoptOrphanManagedApp(snapshot model.App) (model.App, bool, error) {
	prepared, err := prepareOrphanManagedAppAdoption(snapshot, time.Now().UTC())
	if err != nil {
		return model.App{}, false, err
	}
	if s.usingDatabase() {
		return s.pgAdoptOrphanManagedApp(prepared)
	}

	var (
		adopted model.App
		already bool
	)
	err = s.withLockedState(true, func(state *model.State) error {
		if findTenant(state, prepared.App.TenantID) < 0 ||
			!projectBelongsToTenant(state, prepared.App.ProjectID, prepared.App.TenantID) {
			return ErrNotFound
		}
		if projectDeleteRequested(state, prepared.App.ProjectID) {
			return ErrConflict
		}
		if err := validateOrphanAdoptionRuntimeVisibilityState(state, prepared); err != nil {
			return err
		}
		if hasInFlightOperationForApp(state.Operations, prepared.App.ID) {
			return ErrConflict
		}

		appIndex := findApp(state, prepared.App.ID)
		if appIndex >= 0 {
			existing := state.Apps[appIndex]
			hydrateAppBackingServices(state, &existing)
			if !isDeletedApp(existing) {
				if orphanAdoptionEquivalent(existing, prepared) {
					adopted = cloneOrphanAdoptedApp(existing)
					already = true
					return nil
				}
				return ErrConflict
			}
			mergeOrphanTombstoneMetadata(&prepared.App, existing)
		}

		if orphanAdoptionConflictsInState(state, prepared) {
			return ErrConflict
		}
		// Settle the pre-adoption interval before the retained database becomes
		// part of committed resources, avoiding retroactive metering.
		if billing := accrueTenantBillingLedger(state, prepared.App.TenantID, time.Now().UTC()); billing == nil {
			return ErrNotFound
		}
		if appIndex >= 0 {
			state.Apps[appIndex] = orphanAdoptionAppForPersist(prepared.App)
		} else {
			state.Apps = append(state.Apps, orphanAdoptionAppForPersist(prepared.App))
		}
		for _, service := range prepared.Services {
			state.BackingServices = append(state.BackingServices, cloneBackingService(service))
		}
		for _, binding := range prepared.Bindings {
			state.ServiceBindings = append(state.ServiceBindings, cloneServiceBinding(binding))
		}
		adopted = orphanAdoptionResult(prepared)
		return nil
	})
	return adopted, already, err
}

// VerifyAdoptedOrphanManagedApp is a read-only idempotency check for callers
// that no longer observe the orphan marker after a successful adoption. It
// never creates or restores records: the existing live app and its exact
// service/binding identity must already match the retained snapshot.
func (s *Store) VerifyAdoptedOrphanManagedApp(snapshot model.App) (model.App, error) {
	prepared, err := prepareOrphanManagedAppAdoption(snapshot, time.Now().UTC())
	if err != nil {
		return model.App{}, err
	}
	if s.usingDatabase() {
		return s.pgVerifyAdoptedOrphanManagedApp(prepared)
	}
	var verified model.App
	err = s.withLockedState(false, func(state *model.State) error {
		index := findApp(state, prepared.App.ID)
		if index < 0 || isDeletedApp(state.Apps[index]) {
			return ErrConflict
		}
		existing := state.Apps[index]
		hydrateAppBackingServices(state, &existing)
		if !orphanAdoptionEquivalent(existing, prepared) {
			return ErrConflict
		}
		verified = cloneOrphanAdoptedApp(existing)
		return nil
	})
	return verified, err
}

func prepareOrphanManagedAppAdoption(snapshot model.App, now time.Time) (orphanManagedAppAdoption, error) {
	appID := strings.TrimSpace(snapshot.ID)
	tenantID := strings.TrimSpace(snapshot.TenantID)
	projectID := strings.TrimSpace(snapshot.ProjectID)
	name := model.Slugify(snapshot.Name)
	if appID == "" || tenantID == "" || projectID == "" || name == "" {
		return orphanManagedAppAdoption{}, ErrInvalidInput
	}
	if hasDeletedAppTombstoneName(name) {
		return orphanManagedAppAdoption{}, ErrInvalidInput
	}

	spec := cloneAppSpec(&snapshot.Spec)
	if spec == nil || strings.TrimSpace(spec.RuntimeID) == "" {
		return orphanManagedAppAdoption{}, ErrInvalidInput
	}
	if err := normalizeAppSpecResources(spec); err != nil {
		return orphanManagedAppAdoption{}, err
	}
	if err := validateAppNetworkMode(*spec); err != nil {
		return orphanManagedAppAdoption{}, err
	}
	if err := validateFailoverSpec(*spec); err != nil {
		return orphanManagedAppAdoption{}, err
	}
	spec.Replicas = 0
	// Managed PostgreSQL is persisted as a backing service. Keeping an inline
	// copy would create two competing desired-state sources after adoption.
	spec.Postgres = nil

	app := model.App{
		ID:          appID,
		TenantID:    tenantID,
		ProjectID:   projectID,
		Name:        name,
		Description: strings.TrimSpace(snapshot.Description),
		Spec:        *spec,
		Status: model.AppStatus{
			Phase:            "disabled",
			CurrentRuntimeID: strings.TrimSpace(spec.RuntimeID),
			CurrentReplicas:  0,
			UpdatedAt:        now,
		},
		CreatedAt: snapshot.CreatedAt,
		UpdatedAt: now,
	}
	if app.CreatedAt.IsZero() {
		app.CreatedAt = now
	}
	origin := model.AppOriginSource(snapshot)
	build := model.AppBuildSource(snapshot)
	if origin == nil {
		origin = snapshot.Source
	}
	if build == nil {
		build = snapshot.Source
	}
	model.SetAppSourceState(&app, origin, build)

	if len(snapshot.BackingServices) == 0 || len(snapshot.Bindings) == 0 {
		return orphanManagedAppAdoption{}, ErrInvalidInput
	}
	services := make([]model.BackingService, 0, len(snapshot.BackingServices))
	servicesByID := make(map[string]model.BackingService, len(snapshot.BackingServices))
	serviceNames := make(map[string]struct{}, len(snapshot.BackingServices))
	for _, raw := range snapshot.BackingServices {
		service := cloneBackingService(raw)
		service.ID = strings.TrimSpace(service.ID)
		service.TenantID = strings.TrimSpace(service.TenantID)
		service.ProjectID = strings.TrimSpace(service.ProjectID)
		service.OwnerAppID = strings.TrimSpace(service.OwnerAppID)
		if service.ID == "" ||
			service.TenantID != tenantID ||
			service.ProjectID != projectID ||
			(service.OwnerAppID != "" && service.OwnerAppID != appID) ||
			!strings.EqualFold(strings.TrimSpace(service.Type), model.BackingServiceTypePostgres) ||
			!strings.EqualFold(strings.TrimSpace(service.Provisioner), model.BackingServiceProvisionerManaged) ||
			service.Spec.Postgres == nil ||
			strings.TrimSpace(service.Spec.Postgres.Password) == "" {
			return orphanManagedAppAdoption{}, ErrInvalidInput
		}
		if _, exists := servicesByID[service.ID]; exists {
			return orphanManagedAppAdoption{}, ErrInvalidInput
		}
		service.RuntimeStatus = nil
		service.CurrentResourceUsage = nil
		service.CurrentRuntimeStartedAt = nil
		service.CurrentRuntimeReadyAt = nil
		service.Type = model.BackingServiceTypePostgres
		service.Provisioner = model.BackingServiceProvisionerManaged
		service.Status = model.BackingServiceStatusActive
		service.CreatedAt = raw.CreatedAt
		if service.CreatedAt.IsZero() {
			service.CreatedAt = now
		}
		service.UpdatedAt = now
		if err := normalizeBackingServiceForPersist(&service, &app); err != nil {
			return orphanManagedAppAdoption{}, err
		}
		if err := validateManagedPostgresRuntimeSpec(app.Spec.RuntimeID, *service.Spec.Postgres); err != nil {
			return orphanManagedAppAdoption{}, err
		}
		nameKey := strings.ToLower(service.Name)
		if _, exists := serviceNames[nameKey]; exists {
			return orphanManagedAppAdoption{}, ErrInvalidInput
		}
		serviceNames[nameKey] = struct{}{}
		servicesByID[service.ID] = service
		services = append(services, service)
	}

	bindings := make([]model.ServiceBinding, 0, len(snapshot.Bindings))
	bindingIDs := make(map[string]struct{}, len(snapshot.Bindings))
	boundServiceIDs := make(map[string]struct{}, len(snapshot.Bindings))
	for _, raw := range snapshot.Bindings {
		binding := cloneServiceBinding(raw)
		binding.ID = strings.TrimSpace(binding.ID)
		binding.TenantID = strings.TrimSpace(binding.TenantID)
		binding.AppID = strings.TrimSpace(binding.AppID)
		binding.ServiceID = strings.TrimSpace(binding.ServiceID)
		service, exists := servicesByID[binding.ServiceID]
		if binding.ID == "" ||
			binding.TenantID != tenantID ||
			binding.AppID != appID ||
			!exists {
			return orphanManagedAppAdoption{}, ErrInvalidInput
		}
		if _, exists := bindingIDs[binding.ID]; exists {
			return orphanManagedAppAdoption{}, ErrInvalidInput
		}
		if _, exists := boundServiceIDs[binding.ServiceID]; exists {
			return orphanManagedAppAdoption{}, ErrInvalidInput
		}
		bindingIDs[binding.ID] = struct{}{}
		boundServiceIDs[binding.ServiceID] = struct{}{}
		binding.Env = defaultPostgresBindingEnv(*service.Spec.Postgres)
		if err := normalizeBindingForPersist(&binding, service); err != nil {
			return orphanManagedAppAdoption{}, err
		}
		binding.CreatedAt = raw.CreatedAt
		if binding.CreatedAt.IsZero() {
			binding.CreatedAt = now
		}
		binding.UpdatedAt = now
		bindings = append(bindings, binding)
	}
	if len(boundServiceIDs) != len(servicesByID) {
		return orphanManagedAppAdoption{}, ErrInvalidInput
	}
	// Adoption is the explicit ownership transfer. A standalone managed
	// PostgreSQL service is accepted only through the exact exclusive binding
	// validated above, then becomes owned by the restored disabled app.
	for index := range services {
		services[index].OwnerAppID = appID
	}

	sort.Slice(services, func(i, j int) bool { return services[i].ID < services[j].ID })
	sort.Slice(bindings, func(i, j int) bool { return bindings[i].ID < bindings[j].ID })
	return orphanManagedAppAdoption{App: app, Services: services, Bindings: bindings}, nil
}

func validateOrphanAdoptionRuntimeVisibilityState(state *model.State, prepared orphanManagedAppAdoption) error {
	runtimeIDs := map[string]struct{}{strings.TrimSpace(prepared.App.Spec.RuntimeID): {}}
	for _, service := range prepared.Services {
		for _, runtimeID := range managedPostgresReferencedRuntimeIDs(prepared.App.Spec.RuntimeID, *service.Spec.Postgres) {
			runtimeIDs[strings.TrimSpace(runtimeID)] = struct{}{}
		}
	}
	for runtimeID := range runtimeIDs {
		if runtimeID == "" || !runtimeVisibleToTenant(state, runtimeID, prepared.App.TenantID) {
			return ErrNotFound
		}
	}
	return nil
}

func orphanAdoptionConflictsInState(state *model.State, prepared orphanManagedAppAdoption) bool {
	for _, app := range state.Apps {
		if app.ID == prepared.App.ID {
			continue
		}
		if app.TenantID == prepared.App.TenantID &&
			app.ProjectID == prepared.App.ProjectID &&
			strings.EqualFold(app.Name, prepared.App.Name) {
			return true
		}
	}
	for _, desired := range prepared.Services {
		if findBackingService(state, desired.ID) >= 0 ||
			backingServiceNameExists(state, desired.TenantID, desired.ProjectID, desired.Name, "") {
			return true
		}
	}
	for _, desired := range prepared.Bindings {
		if findServiceBinding(state, desired.ID) >= 0 ||
			findServiceBindingByAppAndService(state, desired.AppID, desired.ServiceID) >= 0 {
			return true
		}
	}
	return false
}

func mergeOrphanTombstoneMetadata(app *model.App, tombstone model.App) {
	if app == nil {
		return
	}
	if app.Description == "" {
		app.Description = tombstone.Description
	}
	if app.CreatedAt.IsZero() || (!tombstone.CreatedAt.IsZero() && tombstone.CreatedAt.Before(app.CreatedAt)) {
		app.CreatedAt = tombstone.CreatedAt
	}
	if model.AppOriginSource(*app) == nil && model.AppBuildSource(*app) == nil {
		model.SetAppSourceState(app, model.AppOriginSource(tombstone), model.AppBuildSource(tombstone))
	}
}

func orphanAdoptionAppForPersist(app model.App) model.App {
	app.Bindings = nil
	app.BackingServices = nil
	app.CurrentResourceUsage = nil
	app.InternalService = nil
	app.TechStack = nil
	return app
}

func orphanAdoptionResult(prepared orphanManagedAppAdoption) model.App {
	app := cloneOrphanAdoptedApp(orphanAdoptionAppForPersist(prepared.App))
	setOrphanAdoptionServicesAndBindings(&app, prepared.Services, prepared.Bindings)
	return app
}

func cloneOrphanAdoptedApp(app model.App) model.App {
	out := app
	out.Source = cloneAppSource(app.Source)
	out.OriginSource = cloneAppSource(app.OriginSource)
	out.BuildSource = cloneAppSource(app.BuildSource)
	out.Route = cloneAppRoute(app.Route)
	if spec := cloneAppSpec(&app.Spec); spec != nil {
		out.Spec = *spec
	}
	out.CurrentResourceUsage = nil
	out.Bindings = make([]model.ServiceBinding, 0, len(app.Bindings))
	for _, binding := range app.Bindings {
		out.Bindings = append(out.Bindings, cloneServiceBinding(binding))
	}
	out.BackingServices = make([]model.BackingService, 0, len(app.BackingServices))
	for _, service := range app.BackingServices {
		out.BackingServices = append(out.BackingServices, cloneBackingService(service))
	}
	return out
}

func setOrphanAdoptionServicesAndBindings(app *model.App, services []model.BackingService, bindings []model.ServiceBinding) {
	if app == nil {
		return
	}
	app.BackingServices = make([]model.BackingService, 0, len(services))
	for _, service := range services {
		app.BackingServices = append(app.BackingServices, cloneBackingService(service))
	}
	app.Bindings = make([]model.ServiceBinding, 0, len(bindings))
	for _, binding := range bindings {
		app.Bindings = append(app.Bindings, cloneServiceBinding(binding))
	}
}

func orphanAdoptionEquivalent(existing model.App, prepared orphanManagedAppAdoption) bool {
	if existing.ID != prepared.App.ID ||
		existing.TenantID != prepared.App.TenantID ||
		existing.ProjectID != prepared.App.ProjectID ||
		!strings.EqualFold(existing.Name, prepared.App.Name) ||
		existing.Route != nil ||
		!reflect.DeepEqual(existing.Spec, prepared.App.Spec) ||
		!strings.EqualFold(strings.TrimSpace(existing.Status.Phase), "disabled") ||
		existing.Status.CurrentReplicas != 0 ||
		strings.TrimSpace(existing.Status.CurrentRuntimeID) != strings.TrimSpace(prepared.App.Spec.RuntimeID) {
		return false
	}
	if len(existing.BackingServices) != len(prepared.Services) || len(existing.Bindings) != len(prepared.Bindings) {
		return false
	}
	existingServices := make(map[string]model.BackingService, len(existing.BackingServices))
	for _, service := range existing.BackingServices {
		existingServices[service.ID] = canonicalOrphanAdoptedService(service)
	}
	for _, service := range prepared.Services {
		if !reflect.DeepEqual(existingServices[service.ID], canonicalOrphanAdoptedService(service)) {
			return false
		}
	}
	existingBindings := make(map[string]model.ServiceBinding, len(existing.Bindings))
	for _, binding := range existing.Bindings {
		existingBindings[binding.ID] = canonicalOrphanAdoptedBinding(binding)
	}
	for _, binding := range prepared.Bindings {
		if !reflect.DeepEqual(existingBindings[binding.ID], canonicalOrphanAdoptedBinding(binding)) {
			return false
		}
	}
	return true
}

func canonicalOrphanAdoptedService(service model.BackingService) model.BackingService {
	service = cloneBackingService(service)
	service.RuntimeStatus = nil
	service.CurrentResourceUsage = nil
	service.CurrentRuntimeStartedAt = nil
	service.CurrentRuntimeReadyAt = nil
	service.CreatedAt = time.Time{}
	service.UpdatedAt = time.Time{}
	return service
}

func canonicalOrphanAdoptedBinding(binding model.ServiceBinding) model.ServiceBinding {
	binding = cloneServiceBinding(binding)
	binding.CreatedAt = time.Time{}
	binding.UpdatedAt = time.Time{}
	return binding
}

func (s *Store) pgAdoptOrphanManagedApp(prepared orphanManagedAppAdoption) (model.App, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.App{}, false, fmt.Errorf("begin adopt orphan managed app transaction: %w", err)
	}
	defer tx.Rollback()

	exists, err := s.pgTenantExistsTx(ctx, tx, prepared.App.TenantID)
	if err != nil {
		return model.App{}, false, err
	}
	if !exists {
		return model.App{}, false, ErrNotFound
	}
	belongs, err := s.pgProjectBelongsToTenantTx(ctx, tx, prepared.App.ProjectID, prepared.App.TenantID)
	if err != nil {
		return model.App{}, false, err
	}
	if !belongs {
		return model.App{}, false, ErrNotFound
	}
	deleteRequested, err := s.pgProjectDeleteRequestedTx(ctx, tx, prepared.App.ProjectID)
	if err != nil {
		return model.App{}, false, err
	}
	if deleteRequested {
		return model.App{}, false, ErrConflict
	}
	if err := s.pgValidateOrphanAdoptionRuntimeVisibilityTx(ctx, tx, prepared); err != nil {
		return model.App{}, false, err
	}
	var inFlightCount int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(1)
FROM fugue_operations
WHERE app_id = $1
  AND status IN ($2, $3, $4)
`, prepared.App.ID, model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent).Scan(&inFlightCount); err != nil {
		return model.App{}, false, fmt.Errorf("count in-flight orphan app operations: %w", err)
	}
	if inFlightCount > 0 {
		return model.App{}, false, ErrConflict
	}

	existing, err := s.pgGetAppTx(ctx, tx, prepared.App.ID, true)
	appExists := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return model.App{}, false, err
	}
	if appExists {
		if !isDeletedApp(existing) {
			if orphanAdoptionEquivalent(existing, prepared) {
				return cloneOrphanAdoptedApp(existing), true, nil
			}
			return model.App{}, false, ErrConflict
		}
		mergeOrphanTombstoneMetadata(&prepared.App, existing)
	}
	// Settle the pre-adoption interval before inserting the retained database
	// into the billing projection, so adoption does not meter it retroactively.
	if _, _, _, err := s.pgAccrueTenantBillingTx(ctx, tx, prepared.App.TenantID, time.Now().UTC()); err != nil {
		return model.App{}, false, err
	}

	if appExists {
		if err := s.pgUpdateAppTx(ctx, tx, orphanAdoptionAppForPersist(prepared.App)); err != nil {
			return model.App{}, false, mapDBErr(err)
		}
	} else if err := s.pgInsertOrphanAdoptedAppTx(ctx, tx, prepared.App); err != nil {
		return model.App{}, false, err
	}
	for _, service := range prepared.Services {
		if err := s.pgInsertBackingServiceTx(ctx, tx, service); err != nil {
			return model.App{}, false, err
		}
	}
	for _, binding := range prepared.Bindings {
		if err := s.pgInsertServiceBindingTx(ctx, tx, binding); err != nil {
			return model.App{}, false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return model.App{}, false, fmt.Errorf("commit adopt orphan managed app transaction: %w", err)
	}
	return orphanAdoptionResult(prepared), false, nil
}

func (s *Store) pgVerifyAdoptedOrphanManagedApp(prepared orphanManagedAppAdoption) (model.App, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.App{}, fmt.Errorf("begin verify adopted orphan managed app transaction: %w", err)
	}
	defer tx.Rollback()
	existing, err := s.pgGetAppTx(ctx, tx, prepared.App.ID, true)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.App{}, ErrConflict
		}
		return model.App{}, err
	}
	if isDeletedApp(existing) || !orphanAdoptionEquivalent(existing, prepared) {
		return model.App{}, ErrConflict
	}
	if err := tx.Commit(); err != nil {
		return model.App{}, fmt.Errorf("commit verify adopted orphan managed app transaction: %w", err)
	}
	return cloneOrphanAdoptedApp(existing), nil
}

func (s *Store) pgValidateOrphanAdoptionRuntimeVisibilityTx(
	ctx context.Context,
	tx *sql.Tx,
	prepared orphanManagedAppAdoption,
) error {
	runtimeIDs := map[string]struct{}{strings.TrimSpace(prepared.App.Spec.RuntimeID): {}}
	for _, service := range prepared.Services {
		for _, runtimeID := range managedPostgresReferencedRuntimeIDs(prepared.App.Spec.RuntimeID, *service.Spec.Postgres) {
			runtimeIDs[strings.TrimSpace(runtimeID)] = struct{}{}
		}
	}
	for runtimeID := range runtimeIDs {
		if runtimeID == "" {
			return ErrNotFound
		}
		visible, err := s.pgRuntimeVisibleToTenantTx(ctx, tx, runtimeID, prepared.App.TenantID)
		if err != nil {
			return err
		}
		if !visible {
			return ErrNotFound
		}
	}
	return nil
}

func (s *Store) pgInsertOrphanAdoptedAppTx(ctx context.Context, tx *sql.Tx, app model.App) error {
	app = orphanAdoptionAppForPersist(app)
	sourceJSON, err := marshalAppSourceState(app)
	if err != nil {
		return err
	}
	routeJSON, err := marshalNullableJSON(app.Route)
	if err != nil {
		return err
	}
	specJSON, err := marshalJSON(app.Spec)
	if err != nil {
		return err
	}
	statusJSON, err := marshalJSON(app.Status)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_apps (id, tenant_id, project_id, name, description, source_json, route_json, spec_json, status_json, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
`, app.ID, app.TenantID, app.ProjectID, app.Name, app.Description, sourceJSON, routeJSON, specJSON, statusJSON, app.CreatedAt, app.UpdatedAt); err != nil {
		return mapDBErr(err)
	}
	return nil
}
