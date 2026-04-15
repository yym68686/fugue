package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

type sqlQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (s *Store) pgListBackingServices(tenantID string, platformAdmin bool) ([]model.BackingService, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
SELECT id, tenant_id, project_id, owner_app_id, name, description, type, provisioner, status, spec_json, current_runtime_started_at, current_runtime_ready_at, created_at, updated_at
FROM fugue_backing_services
`
	args := make([]any, 0, 1)
	if !platformAdmin {
		query += ` WHERE tenant_id = $1`
		args = append(args, tenantID)
	}
	query += ` ORDER BY created_at ASC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list backing services: %w", err)
	}
	defer rows.Close()

	services := make([]model.BackingService, 0)
	for rows.Next() {
		service, err := scanBackingService(rows)
		if err != nil {
			return nil, err
		}
		if isDeletedBackingService(service) {
			continue
		}
		services = append(services, service)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate backing services: %w", err)
	}
	return services, nil
}

func (s *Store) pgGetBackingService(id string) (model.BackingService, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	service, err := scanBackingService(s.db.QueryRowContext(ctx, `
SELECT id, tenant_id, project_id, owner_app_id, name, description, type, provisioner, status, spec_json, current_runtime_started_at, current_runtime_ready_at, created_at, updated_at
FROM fugue_backing_services
WHERE id = $1
`, id))
	if err != nil {
		return model.BackingService{}, mapDBErr(err)
	}
	if isDeletedBackingService(service) {
		return model.BackingService{}, ErrNotFound
	}
	return service, nil
}

func (s *Store) pgCreateBackingService(tenantID, projectID, name, description string, spec model.BackingServiceSpec) (model.BackingService, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.BackingService{}, fmt.Errorf("begin create backing service transaction: %w", err)
	}
	defer tx.Rollback()

	exists, err := s.pgTenantExistsTx(ctx, tx, tenantID)
	if err != nil {
		return model.BackingService{}, err
	}
	if !exists {
		return model.BackingService{}, ErrNotFound
	}
	projectOK, err := s.pgProjectBelongsToTenantTx(ctx, tx, projectID, tenantID)
	if err != nil {
		return model.BackingService{}, err
	}
	if !projectOK {
		return model.BackingService{}, ErrNotFound
	}
	deleteRequested, err := s.pgProjectDeleteRequestedTx(ctx, tx, projectID)
	if err != nil {
		return model.BackingService{}, err
	}
	if deleteRequested {
		return model.BackingService{}, ErrConflict
	}

	now := time.Now().UTC()
	billing, billingState, err := s.pgAccrueTenantBillingTx(ctx, tx, tenantID, now)
	if err != nil {
		return model.BackingService{}, err
	}
	service := model.BackingService{
		ID:          model.NewID("service"),
		TenantID:    tenantID,
		ProjectID:   projectID,
		Name:        strings.TrimSpace(name),
		Description: strings.TrimSpace(description),
		Type:        backingServiceTypeFromSpec(spec),
		Provisioner: model.BackingServiceProvisionerManaged,
		Status:      model.BackingServiceStatusActive,
		Spec:        cloneBackingServiceSpec(spec),
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if err := normalizeBackingServiceForPersist(&service, nil); err != nil {
		return model.BackingService{}, err
	}
	service.Name = s.pgNextAvailableBackingServiceNameTx(ctx, tx, tenantID, projectID, service.Name)
	if err := validateTenantManagedCapacityProjection(&billingState, billing, func(projection *model.State) {
		projection.BackingServices = append(projection.BackingServices, cloneBackingService(service))
	}); err != nil {
		return model.BackingService{}, err
	}
	if err := s.pgInsertBackingServiceTx(ctx, tx, service); err != nil {
		return model.BackingService{}, err
	}

	if err := tx.Commit(); err != nil {
		return model.BackingService{}, fmt.Errorf("commit create backing service transaction: %w", err)
	}
	return service, nil
}

func (s *Store) pgDeleteBackingService(id string) (model.BackingService, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.BackingService{}, fmt.Errorf("begin delete backing service transaction: %w", err)
	}
	defer tx.Rollback()

	service, err := s.pgGetBackingServiceTx(ctx, tx, id, true)
	if err != nil {
		return model.BackingService{}, mapDBErr(err)
	}
	if isDeletedBackingService(service) {
		return model.BackingService{}, ErrNotFound
	}
	bindingCount, err := s.pgCountBindingsForServiceTx(ctx, tx, id)
	if err != nil {
		return model.BackingService{}, err
	}
	if bindingCount > 0 {
		return model.BackingService{}, ErrConflict
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM fugue_backing_services WHERE id = $1`, id); err != nil {
		return model.BackingService{}, fmt.Errorf("delete backing service %s: %w", id, err)
	}
	if err := s.pgTryFinalizeRequestedProjectDeleteTx(ctx, tx, service.ProjectID); err != nil {
		return model.BackingService{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.BackingService{}, fmt.Errorf("commit delete backing service transaction: %w", err)
	}
	return service, nil
}

func (s *Store) pgListServiceBindings(appID string) ([]model.ServiceBinding, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := s.db.QueryContext(ctx, `
SELECT id, tenant_id, app_id, service_id, alias, env_json, created_at, updated_at
FROM fugue_service_bindings
WHERE app_id = $1
ORDER BY created_at ASC
`, appID)
	if err != nil {
		return nil, fmt.Errorf("list service bindings: %w", err)
	}
	defer rows.Close()

	bindings := make([]model.ServiceBinding, 0)
	for rows.Next() {
		binding, err := scanServiceBinding(rows)
		if err != nil {
			return nil, err
		}
		bindings = append(bindings, binding)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate service bindings: %w", err)
	}
	return bindings, nil
}

func (s *Store) pgBindBackingService(tenantID, appID, serviceID, alias string, env map[string]string) (model.ServiceBinding, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.ServiceBinding{}, fmt.Errorf("begin bind backing service transaction: %w", err)
	}
	defer tx.Rollback()

	app, err := s.pgGetAppTx(ctx, tx, appID, true)
	if err != nil {
		return model.ServiceBinding{}, mapDBErr(err)
	}
	if app.TenantID != tenantID || isDeletedApp(app) {
		return model.ServiceBinding{}, ErrNotFound
	}

	service, err := s.pgGetBackingServiceTx(ctx, tx, serviceID, true)
	if err != nil {
		return model.ServiceBinding{}, mapDBErr(err)
	}
	if isDeletedBackingService(service) || service.TenantID != tenantID {
		return model.ServiceBinding{}, ErrNotFound
	}

	if _, exists, err := s.pgGetServiceBindingByAppAndServiceTx(ctx, tx, appID, serviceID); err != nil {
		return model.ServiceBinding{}, err
	} else if exists {
		return model.ServiceBinding{}, ErrConflict
	}
	if requiresExclusiveBinding(service) {
		bindingCount, err := s.pgCountBindingsForServiceTx(ctx, tx, serviceID)
		if err != nil {
			return model.ServiceBinding{}, err
		}
		if bindingCount > 0 {
			return model.ServiceBinding{}, ErrConflict
		}
	}

	now := time.Now().UTC()
	binding := model.ServiceBinding{
		ID:        model.NewID("binding"),
		TenantID:  tenantID,
		AppID:     appID,
		ServiceID: serviceID,
		Alias:     alias,
		Env:       cloneMap(env),
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := normalizeBindingForPersist(&binding, service); err != nil {
		return model.ServiceBinding{}, err
	}
	billing, billingState, err := s.pgAccrueTenantBillingTx(ctx, tx, tenantID, now)
	if err != nil {
		return model.ServiceBinding{}, err
	}
	if err := validateTenantManagedCapacityProjection(&billingState, billing, func(projection *model.State) {
		projection.ServiceBindings = append(projection.ServiceBindings, cloneServiceBinding(binding))
	}); err != nil {
		return model.ServiceBinding{}, err
	}
	if err := s.pgInsertServiceBindingTx(ctx, tx, binding); err != nil {
		return model.ServiceBinding{}, err
	}

	if err := tx.Commit(); err != nil {
		return model.ServiceBinding{}, fmt.Errorf("commit bind backing service transaction: %w", err)
	}
	return binding, nil
}

func (s *Store) pgUnbindBackingService(bindingID string) (model.ServiceBinding, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.ServiceBinding{}, fmt.Errorf("begin unbind backing service transaction: %w", err)
	}
	defer tx.Rollback()

	binding, err := s.pgGetServiceBindingTx(ctx, tx, bindingID, true)
	if err != nil {
		return model.ServiceBinding{}, mapDBErr(err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM fugue_service_bindings WHERE id = $1`, bindingID); err != nil {
		return model.ServiceBinding{}, fmt.Errorf("delete service binding %s: %w", bindingID, err)
	}
	if err := tx.Commit(); err != nil {
		return model.ServiceBinding{}, fmt.Errorf("commit unbind backing service transaction: %w", err)
	}
	return binding, nil
}

func (s *Store) pgHydrateAppBackingServices(ctx context.Context, app *model.App) error {
	return s.pgHydrateAppBackingServicesWithQueryer(ctx, s.db, app)
}

func (s *Store) pgHydrateAppsBackingServices(ctx context.Context, apps []model.App) error {
	return s.pgHydrateAppsBackingServicesWithQueryer(ctx, s.db, apps)
}

func (s *Store) pgHydrateAppBackingServicesWithQueryer(ctx context.Context, queryer sqlQueryer, app *model.App) error {
	if app == nil {
		return nil
	}
	rows, err := queryer.QueryContext(ctx, `
	SELECT b.id, b.tenant_id, b.app_id, b.service_id, b.alias, b.env_json, b.created_at, b.updated_at,
	       s.id, s.tenant_id, s.project_id, s.owner_app_id, s.name, s.description, s.type, s.provisioner, s.status, s.spec_json, s.current_runtime_started_at, s.current_runtime_ready_at, s.created_at, s.updated_at
	FROM fugue_service_bindings AS b
	JOIN fugue_backing_services AS s ON s.id = b.service_id
WHERE b.app_id = $1
ORDER BY b.created_at ASC, s.created_at ASC
`, app.ID)
	if err != nil {
		return fmt.Errorf("query app backing services: %w", err)
	}
	defer rows.Close()

	bindings := make([]model.ServiceBinding, 0)
	servicesByID := make(map[string]model.BackingService)
	for rows.Next() {
		binding, service, err := scanBoundBackingService(rows)
		if err != nil {
			return err
		}
		if isDeletedBackingService(service) {
			continue
		}
		bindings = append(bindings, binding)
		servicesByID[service.ID] = service
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate app backing services: %w", err)
	}

	services := make([]model.BackingService, 0, len(servicesByID))
	for _, service := range servicesByID {
		services = append(services, service)
	}
	sortBackingServices(services)
	sortServiceBindings(bindings)

	app.Bindings = bindings
	app.BackingServices = services
	return nil
}

func (s *Store) pgHydrateAppsBackingServicesWithQueryer(
	ctx context.Context,
	queryer sqlQueryer,
	apps []model.App,
) error {
	if len(apps) == 0 {
		return nil
	}

	appIDs := make([]string, 0, len(apps))
	appsByID := make(map[string]*model.App, len(apps))
	for index := range apps {
		appID := strings.TrimSpace(apps[index].ID)
		if appID == "" {
			continue
		}
		apps[index].Bindings = nil
		apps[index].BackingServices = nil
		appIDs = append(appIDs, appID)
		appsByID[appID] = &apps[index]
	}

	appIDs = sortedTrimmedStringKeys(trimmedStringSet(appIDs))
	if len(appIDs) == 0 {
		return nil
	}

	args := make([]any, 0, len(appIDs))
	for _, appID := range appIDs {
		args = append(args, appID)
	}

	rows, err := queryer.QueryContext(ctx, fmt.Sprintf(`
	SELECT b.id, b.tenant_id, b.app_id, b.service_id, b.alias, b.env_json, b.created_at, b.updated_at,
	       s.id, s.tenant_id, s.project_id, s.owner_app_id, s.name, s.description, s.type, s.provisioner, s.status, s.spec_json, s.current_runtime_started_at, s.current_runtime_ready_at, s.created_at, s.updated_at
	FROM fugue_service_bindings AS b
	JOIN fugue_backing_services AS s ON s.id = b.service_id
	WHERE b.app_id IN (%s)
	ORDER BY b.app_id ASC, b.created_at ASC, s.created_at ASC
`, sqlPlaceholderList(1, len(appIDs))), args...)
	if err != nil {
		return fmt.Errorf("query app backing services: %w", err)
	}
	defer rows.Close()

	bindingsByAppID := make(map[string][]model.ServiceBinding, len(appIDs))
	servicesByAppID := make(map[string]map[string]model.BackingService, len(appIDs))
	for rows.Next() {
		binding, service, err := scanBoundBackingService(rows)
		if err != nil {
			return err
		}
		if isDeletedBackingService(service) {
			continue
		}

		appID := strings.TrimSpace(binding.AppID)
		if appID == "" {
			continue
		}

		bindingsByAppID[appID] = append(bindingsByAppID[appID], binding)
		if _, ok := servicesByAppID[appID]; !ok {
			servicesByAppID[appID] = make(map[string]model.BackingService)
		}
		servicesByAppID[appID][service.ID] = service
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate app backing services: %w", err)
	}

	for appID, app := range appsByID {
		if bindings := bindingsByAppID[appID]; len(bindings) > 0 {
			app.Bindings = sortAndNormalizeBindings(bindings)
		}

		if serviceMap := servicesByAppID[appID]; len(serviceMap) > 0 {
			services := make([]model.BackingService, 0, len(serviceMap))
			for _, service := range serviceMap {
				services = append(services, service)
			}
			app.BackingServices = sortAndNormalizeServices(services)
		}
	}

	return nil
}

func (s *Store) pgApplyDesiredSpecBackingServicesTx(ctx context.Context, tx *sql.Tx, app *model.App, desiredSpec *model.AppSpec) error {
	if app == nil || desiredSpec == nil || desiredSpec.Postgres == nil {
		return nil
	}
	if err := validateManagedPostgresSpecForAppName(app.Name, desiredSpec.Postgres); err != nil {
		return err
	}

	if service, found, err := s.pgGetOwnedBackingServiceByAppAndTypeTx(ctx, tx, app.ID, model.BackingServiceTypePostgres, true); err != nil {
		return err
	} else if found {
		now := time.Now().UTC()
		normalized := normalizeManagedPostgresSpec(appNameForService(&service, app), app.Spec.RuntimeID, *desiredSpec.Postgres)
		service.Type = model.BackingServiceTypePostgres
		service.Provisioner = model.BackingServiceProvisionerManaged
		service.Status = model.BackingServiceStatusActive
		service.Spec.Postgres = &normalized
		service.UpdatedAt = now
		if err := s.pgUpdateBackingServiceTx(ctx, tx, service); err != nil {
			return err
		}
		if err := s.pgEnsureAppServiceBindingTx(ctx, tx, *app, service, defaultPostgresBindingEnv(normalized), now); err != nil {
			return err
		}
		desiredSpec.Postgres = nil
		return nil
	}

	if bound, err := s.pgAppHasBindingToServiceTypeTx(ctx, tx, app.ID, model.BackingServiceTypePostgres); err != nil {
		return err
	} else if bound {
		desiredSpec.Postgres = nil
		return nil
	}

	appCopy := *app
	if cloned := cloneAppSpec(desiredSpec); cloned != nil {
		appCopy.Spec = *cloned
	}
	service, binding := ownedManagedPostgresResources(appCopy)
	service.Name = s.pgNextAvailableBackingServiceNameTx(ctx, tx, app.TenantID, app.ProjectID, service.Name)
	if err := s.pgInsertBackingServiceTx(ctx, tx, service); err != nil {
		return err
	}
	if err := s.pgInsertServiceBindingTx(ctx, tx, binding); err != nil {
		return err
	}
	desiredSpec.Postgres = nil
	return nil
}

func (s *Store) pgInsertBackingServiceTx(ctx context.Context, tx *sql.Tx, service model.BackingService) error {
	specJSON, err := marshalJSON(service.Spec)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_backing_services (id, tenant_id, project_id, owner_app_id, name, description, type, provisioner, status, spec_json, current_runtime_started_at, current_runtime_ready_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
`, service.ID, nullIfEmpty(service.TenantID), service.ProjectID, nullIfEmpty(service.OwnerAppID), service.Name, service.Description, service.Type, service.Provisioner, service.Status, specJSON, service.CurrentRuntimeStartedAt, service.CurrentRuntimeReadyAt, service.CreatedAt, service.UpdatedAt); err != nil {
		return mapDBErr(err)
	}
	return nil
}

func (s *Store) pgInsertServiceBindingTx(ctx context.Context, tx *sql.Tx, binding model.ServiceBinding) error {
	envJSON, err := marshalNullableJSON(binding.Env)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_service_bindings (id, tenant_id, app_id, service_id, alias, env_json, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
`, binding.ID, nullIfEmpty(binding.TenantID), binding.AppID, binding.ServiceID, binding.Alias, envJSON, binding.CreatedAt, binding.UpdatedAt); err != nil {
		return mapDBErr(err)
	}
	return nil
}

func (s *Store) pgUpdateBackingServiceTx(ctx context.Context, tx *sql.Tx, service model.BackingService) error {
	specJSON, err := marshalJSON(service.Spec)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE fugue_backing_services
SET tenant_id = $2,
	project_id = $3,
	owner_app_id = $4,
	name = $5,
	description = $6,
	type = $7,
	provisioner = $8,
	status = $9,
	spec_json = $10,
	current_runtime_started_at = $11,
	current_runtime_ready_at = $12,
	created_at = $13,
	updated_at = $14
WHERE id = $1
`, service.ID, nullIfEmpty(service.TenantID), service.ProjectID, nullIfEmpty(service.OwnerAppID), service.Name, service.Description, service.Type, service.Provisioner, service.Status, specJSON, service.CurrentRuntimeStartedAt, service.CurrentRuntimeReadyAt, service.CreatedAt, service.UpdatedAt); err != nil {
		return mapDBErr(err)
	}
	return nil
}

func (s *Store) pgUpdateServiceBindingTx(ctx context.Context, tx *sql.Tx, binding model.ServiceBinding) error {
	envJSON, err := marshalNullableJSON(binding.Env)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE fugue_service_bindings
SET tenant_id = $2,
	app_id = $3,
	service_id = $4,
	alias = $5,
	env_json = $6,
	created_at = $7,
	updated_at = $8
WHERE id = $1
`, binding.ID, nullIfEmpty(binding.TenantID), binding.AppID, binding.ServiceID, binding.Alias, envJSON, binding.CreatedAt, binding.UpdatedAt); err != nil {
		return mapDBErr(err)
	}
	return nil
}

func (s *Store) pgGetBackingServiceTx(ctx context.Context, tx *sql.Tx, id string, forUpdate bool) (model.BackingService, error) {
	query := `
SELECT id, tenant_id, project_id, owner_app_id, name, description, type, provisioner, status, spec_json, current_runtime_started_at, current_runtime_ready_at, created_at, updated_at
FROM fugue_backing_services
WHERE id = $1
`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	service, err := scanBackingService(tx.QueryRowContext(ctx, query, id))
	if err != nil {
		return model.BackingService{}, err
	}
	return service, nil
}

func (s *Store) pgGetServiceBindingTx(ctx context.Context, tx *sql.Tx, id string, forUpdate bool) (model.ServiceBinding, error) {
	query := `
SELECT id, tenant_id, app_id, service_id, alias, env_json, created_at, updated_at
FROM fugue_service_bindings
WHERE id = $1
`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	binding, err := scanServiceBinding(tx.QueryRowContext(ctx, query, id))
	if err != nil {
		return model.ServiceBinding{}, err
	}
	return binding, nil
}

func (s *Store) pgGetServiceBindingByAppAndServiceTx(ctx context.Context, tx *sql.Tx, appID, serviceID string) (model.ServiceBinding, bool, error) {
	binding, err := scanServiceBinding(tx.QueryRowContext(ctx, `
SELECT id, tenant_id, app_id, service_id, alias, env_json, created_at, updated_at
FROM fugue_service_bindings
WHERE app_id = $1
  AND service_id = $2
FOR UPDATE
`, appID, serviceID))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.ServiceBinding{}, false, nil
		}
		return model.ServiceBinding{}, false, err
	}
	return binding, true, nil
}

func (s *Store) pgGetOwnedBackingServiceByAppAndTypeTx(ctx context.Context, tx *sql.Tx, appID, serviceType string, forUpdate bool) (model.BackingService, bool, error) {
	query := `
SELECT id, tenant_id, project_id, owner_app_id, name, description, type, provisioner, status, spec_json, current_runtime_started_at, current_runtime_ready_at, created_at, updated_at
FROM fugue_backing_services
WHERE owner_app_id = $1
  AND type = $2
ORDER BY created_at ASC
LIMIT 1
`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	service, err := scanBackingService(tx.QueryRowContext(ctx, query, appID, serviceType))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.BackingService{}, false, nil
		}
		return model.BackingService{}, false, err
	}
	return service, true, nil
}

func (s *Store) pgCountBindingsForServiceTx(ctx context.Context, tx *sql.Tx, serviceID string) (int, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(1)
FROM fugue_service_bindings
WHERE service_id = $1
`, serviceID).Scan(&count); err != nil {
		return 0, fmt.Errorf("count bindings for service %s: %w", serviceID, err)
	}
	return count, nil
}

func (s *Store) pgEnsureAppServiceBindingTx(ctx context.Context, tx *sql.Tx, app model.App, service model.BackingService, env map[string]string, now time.Time) error {
	binding, exists, err := s.pgGetServiceBindingByAppAndServiceTx(ctx, tx, app.ID, service.ID)
	if err != nil {
		return err
	}
	if exists {
		binding.Alias = defaultServiceBindingAlias(binding.Alias, service)
		binding.Env = cloneMap(env)
		binding.UpdatedAt = now
		return s.pgUpdateServiceBindingTx(ctx, tx, binding)
	}

	return s.pgInsertServiceBindingTx(ctx, tx, model.ServiceBinding{
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

func (s *Store) pgDeleteServiceBindingsByAppTx(ctx context.Context, tx *sql.Tx, appID string) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM fugue_service_bindings WHERE app_id = $1`, appID); err != nil {
		return fmt.Errorf("delete bindings for app %s: %w", appID, err)
	}
	return nil
}

func (s *Store) pgDeleteOwnedBackingServicesByAppTx(ctx context.Context, tx *sql.Tx, appID string) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM fugue_backing_services WHERE owner_app_id = $1`, appID); err != nil {
		return fmt.Errorf("delete owned backing services for app %s: %w", appID, err)
	}
	return nil
}

func (s *Store) pgHasOwnedPostgresServiceTx(ctx context.Context, tx *sql.Tx, appID string) (bool, error) {
	var exists bool
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS (
	SELECT 1
	FROM fugue_backing_services
	WHERE owner_app_id = $1
	  AND type = $2
)
`, appID, model.BackingServiceTypePostgres).Scan(&exists); err != nil {
		return false, fmt.Errorf("check owned postgres service for app %s: %w", appID, err)
	}
	return exists, nil
}

func (s *Store) pgAppHasBindingToServiceTypeTx(ctx context.Context, tx *sql.Tx, appID, serviceType string) (bool, error) {
	var exists bool
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS (
	SELECT 1
	FROM fugue_service_bindings AS b
	JOIN fugue_backing_services AS s ON s.id = b.service_id
	WHERE b.app_id = $1
	  AND s.type = $2
)
`, appID, serviceType).Scan(&exists); err != nil {
		return false, fmt.Errorf("check service binding for app %s: %w", appID, err)
	}
	return exists, nil
}

func (s *Store) pgNextAvailableBackingServiceNameTx(ctx context.Context, tx *sql.Tx, tenantID, projectID, base string) string {
	base = model.Slugify(base)
	if base == "" {
		base = "service"
	}
	candidate := base
	for attempt := 1; ; attempt++ {
		var exists bool
		err := tx.QueryRowContext(ctx, `
SELECT EXISTS (
	SELECT 1
	FROM fugue_backing_services
	WHERE tenant_id = $1
	  AND project_id = $2
	  AND lower(name) = lower($3)
)
`, tenantID, projectID, candidate).Scan(&exists)
		if err != nil || !exists {
			return candidate
		}
		candidate = fmt.Sprintf("%s-%d", base, attempt+1)
	}
}

func scanBackingService(scanner sqlScanner) (model.BackingService, error) {
	var service model.BackingService
	var tenantID sql.NullString
	var ownerAppID sql.NullString
	var specRaw []byte
	if err := scanner.Scan(&service.ID, &tenantID, &service.ProjectID, &ownerAppID, &service.Name, &service.Description, &service.Type, &service.Provisioner, &service.Status, &specRaw, &service.CurrentRuntimeStartedAt, &service.CurrentRuntimeReadyAt, &service.CreatedAt, &service.UpdatedAt); err != nil {
		return model.BackingService{}, err
	}
	service.TenantID = tenantID.String
	service.OwnerAppID = ownerAppID.String
	spec, err := decodeJSONValue[model.BackingServiceSpec](specRaw)
	if err != nil {
		return model.BackingService{}, err
	}
	service.Spec = spec
	return service, nil
}

func scanServiceBinding(scanner sqlScanner) (model.ServiceBinding, error) {
	var binding model.ServiceBinding
	var tenantID sql.NullString
	var envRaw []byte
	if err := scanner.Scan(&binding.ID, &tenantID, &binding.AppID, &binding.ServiceID, &binding.Alias, &envRaw, &binding.CreatedAt, &binding.UpdatedAt); err != nil {
		return model.ServiceBinding{}, err
	}
	binding.TenantID = tenantID.String
	env, err := decodeJSONValue[map[string]string](envRaw)
	if err != nil {
		return model.ServiceBinding{}, err
	}
	binding.Env = env
	return binding, nil
}

func scanBoundBackingService(scanner sqlScanner) (model.ServiceBinding, model.BackingService, error) {
	var binding model.ServiceBinding
	var service model.BackingService
	var bindingTenantID sql.NullString
	var bindingEnvRaw []byte
	var serviceTenantID sql.NullString
	var ownerAppID sql.NullString
	var specRaw []byte
	if err := scanner.Scan(
		&binding.ID,
		&bindingTenantID,
		&binding.AppID,
		&binding.ServiceID,
		&binding.Alias,
		&bindingEnvRaw,
		&binding.CreatedAt,
		&binding.UpdatedAt,
		&service.ID,
		&serviceTenantID,
		&service.ProjectID,
		&ownerAppID,
		&service.Name,
		&service.Description,
		&service.Type,
		&service.Provisioner,
		&service.Status,
		&specRaw,
		&service.CurrentRuntimeStartedAt,
		&service.CurrentRuntimeReadyAt,
		&service.CreatedAt,
		&service.UpdatedAt,
	); err != nil {
		return model.ServiceBinding{}, model.BackingService{}, err
	}
	binding.TenantID = bindingTenantID.String
	env, err := decodeJSONValue[map[string]string](bindingEnvRaw)
	if err != nil {
		return model.ServiceBinding{}, model.BackingService{}, err
	}
	binding.Env = env
	service.TenantID = serviceTenantID.String
	service.OwnerAppID = ownerAppID.String
	spec, err := decodeJSONValue[model.BackingServiceSpec](specRaw)
	if err != nil {
		return model.ServiceBinding{}, model.BackingService{}, err
	}
	service.Spec = spec
	return binding, service, nil
}

func sortAndNormalizeBindings(bindings []model.ServiceBinding) []model.ServiceBinding {
	out := make([]model.ServiceBinding, len(bindings))
	for index, binding := range bindings {
		out[index] = cloneServiceBinding(binding)
	}
	sortServiceBindings(out)
	return out
}

func sortAndNormalizeServices(services []model.BackingService) []model.BackingService {
	out := make([]model.BackingService, len(services))
	for index, service := range services {
		out[index] = cloneBackingService(service)
	}
	sortBackingServices(out)
	return out
}
