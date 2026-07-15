package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

// GetActiveManagedPostgresLifecycleOperation returns the sole active
// suspend/resume operation only when it exactly matches the requested tenant,
// app, backing service, and direction. The backing-service topology is
// validated under the same store lock used by lifecycle creation. Any active
// operation for the app or service that is not the exact sole match is a
// conflict and is never returned.
func (s *Store) GetActiveManagedPostgresLifecycleOperation(
	tenantID, appID, serviceID, operationType string,
) (model.Operation, bool, error) {
	tenantID = strings.TrimSpace(tenantID)
	appID = strings.TrimSpace(appID)
	serviceID = strings.TrimSpace(serviceID)
	operationType = strings.TrimSpace(operationType)
	if tenantID == "" || appID == "" || serviceID == "" || !isManagedPostgresLifecycleOperationType(operationType) {
		return model.Operation{}, false, ErrInvalidInput
	}
	if s.usingDatabase() {
		return s.pgGetActiveManagedPostgresLifecycleOperation(tenantID, appID, serviceID, operationType)
	}

	var (
		existing model.Operation
		found    bool
	)
	err := s.withLockedState(false, func(state *model.State) error {
		appIndex := findApp(state, appID)
		if appIndex < 0 {
			return ErrNotFound
		}
		app := state.Apps[appIndex]
		if strings.TrimSpace(app.TenantID) != tenantID {
			return ErrNotFound
		}
		hydrateAppBackingServices(state, &app)
		if err := validateManagedPostgresLifecycleTargetState(state, app, serviceID); err != nil {
			return err
		}
		active := activeOperationsForLifecycleTarget(state.Operations, appID, serviceID)
		if len(active) == 0 {
			return nil
		}
		candidate := model.Operation{
			TenantID:  tenantID,
			AppID:     appID,
			ServiceID: serviceID,
			Type:      operationType,
		}
		if err := prepareManagedPostgresLifecycleOperation(app, &candidate, operationType == model.OperationTypeDatabaseSuspend); err != nil {
			return err
		}
		if len(active) != 1 || !managedPostgresLifecycleRetryMatches(active[0], candidate) {
			return ErrConflict
		}
		existing = cloneOperation(active[0])
		found = true
		return nil
	})
	if err != nil {
		return model.Operation{}, false, err
	}
	return existing, found, nil
}

func (s *Store) pgGetActiveManagedPostgresLifecycleOperation(
	tenantID, appID, serviceID, operationType string,
) (model.Operation, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Operation{}, false, fmt.Errorf("begin lookup managed postgres lifecycle transaction: %w", err)
	}
	defer tx.Rollback()

	app, err := s.pgGetAppTx(ctx, tx, appID, true)
	if err != nil {
		return model.Operation{}, false, mapDBErr(err)
	}
	if strings.TrimSpace(app.TenantID) != tenantID {
		return model.Operation{}, false, ErrNotFound
	}
	if err := s.pgValidateManagedPostgresLifecycleTargetTx(ctx, tx, &app, serviceID); err != nil {
		return model.Operation{}, false, err
	}
	active, err := s.pgActiveOperationsForLifecycleTargetTx(ctx, tx, appID, serviceID)
	if err != nil {
		return model.Operation{}, false, err
	}
	if len(active) == 0 {
		return model.Operation{}, false, nil
	}
	candidate := model.Operation{
		TenantID:  tenantID,
		AppID:     appID,
		ServiceID: serviceID,
		Type:      operationType,
	}
	if err := prepareManagedPostgresLifecycleOperation(app, &candidate, operationType == model.OperationTypeDatabaseSuspend); err != nil {
		return model.Operation{}, false, err
	}
	if len(active) != 1 || !managedPostgresLifecycleRetryMatches(active[0], candidate) {
		return model.Operation{}, false, ErrConflict
	}
	return active[0], true, nil
}
