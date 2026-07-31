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

type pgManagedPostgresPlacementPersistTarget struct {
	service  *model.BackingService
	postgres model.AppPostgresSpec
}

func (s *Store) pgCountActiveOperationsForManagedPostgresPlacementTargetTx(
	ctx context.Context,
	tx *sql.Tx,
	appID, serviceID string,
) (int, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(1)
FROM fugue_operations
WHERE (app_id = $1 OR ($2 <> '' AND service_id = $2))
  AND status IN ($3, $4, $5)
`, strings.TrimSpace(appID), strings.TrimSpace(serviceID),
		model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent,
	).Scan(&count); err != nil {
		return 0, fmt.Errorf("count in-flight managed postgres placement operations: %w", err)
	}
	return count, nil
}

func replaceHydratedPlacementService(app *model.App, service model.BackingService) {
	if app == nil {
		return
	}
	for index := range app.BackingServices {
		if strings.TrimSpace(app.BackingServices[index].ID) == strings.TrimSpace(service.ID) {
			app.BackingServices[index] = cloneBackingService(service)
			return
		}
	}
	app.BackingServices = append(app.BackingServices, cloneBackingService(service))
}

func replaceHydratedPlacementBinding(app *model.App, binding model.ServiceBinding) {
	if app == nil {
		return
	}
	for index := range app.Bindings {
		if strings.TrimSpace(app.Bindings[index].ID) == strings.TrimSpace(binding.ID) {
			app.Bindings[index] = cloneServiceBinding(binding)
			return
		}
	}
	app.Bindings = append(app.Bindings, cloneServiceBinding(binding))
}

func (s *Store) pgLockedManagedPostgresPlacementTargetTx(
	ctx context.Context,
	tx *sql.Tx,
	app *model.App,
	witness ManagedPostgresPlacementWitness,
) (pgManagedPostgresPlacementPersistTarget, error) {
	if app == nil {
		return pgManagedPostgresPlacementPersistTarget{}, ErrInvalidInput
	}

	bindingCount := 0
	if witness.ServiceID != "" {
		service, err := s.pgGetBackingServiceTx(ctx, tx, witness.ServiceID, true)
		if err != nil {
			return pgManagedPostgresPlacementPersistTarget{}, mapDBErr(err)
		}
		binding, found, err := s.pgGetServiceBindingByAppAndServiceTx(ctx, tx, witness.AppID, witness.ServiceID)
		if err != nil {
			return pgManagedPostgresPlacementPersistTarget{}, mapDBErr(err)
		}
		if !found {
			return pgManagedPostgresPlacementPersistTarget{}, ErrNotFound
		}
		bindingCount, err = s.pgCountBindingsForServiceTx(ctx, tx, witness.ServiceID)
		if err != nil {
			return pgManagedPostgresPlacementPersistTarget{}, err
		}
		replaceHydratedPlacementService(app, service)
		replaceHydratedPlacementBinding(app, binding)
	}

	target, err := ManagedPostgresOperationTargetForApp(*app, witness.ServiceID)
	if err != nil {
		return pgManagedPostgresPlacementPersistTarget{}, err
	}
	if err := validateManagedPostgresPlacementIdentity(*app, target, witness, bindingCount); err != nil {
		return pgManagedPostgresPlacementPersistTarget{}, err
	}
	if target == nil {
		return pgManagedPostgresPlacementPersistTarget{}, ErrNotFound
	}

	if witness.ServiceID == "" {
		if app.Spec.Postgres == nil {
			return pgManagedPostgresPlacementPersistTarget{}, ErrNotFound
		}
		return pgManagedPostgresPlacementPersistTarget{
			postgres: *model.CloneAppPostgresSpec(app.Spec.Postgres),
		}, nil
	}
	for index := range app.BackingServices {
		service := &app.BackingServices[index]
		if strings.TrimSpace(service.ID) != witness.ServiceID {
			continue
		}
		if service.Spec.Postgres == nil {
			return pgManagedPostgresPlacementPersistTarget{}, ErrNotFound
		}
		return pgManagedPostgresPlacementPersistTarget{
			service:  service,
			postgres: *model.CloneAppPostgresSpec(service.Spec.Postgres),
		}, nil
	}
	return pgManagedPostgresPlacementPersistTarget{}, ErrNotFound
}

func (s *Store) pgApplyManagedPostgresPlacementStateTx(
	ctx context.Context,
	tx *sql.Tx,
	app *model.App,
	target pgManagedPostgresPlacementPersistTarget,
	desired ManagedPostgresPlacementState,
	now time.Time,
) error {
	postgres := *model.CloneAppPostgresSpec(&target.postgres)
	applyManagedPostgresPlacementState(&postgres, desired)
	if target.service != nil {
		service := cloneBackingService(*target.service)
		if strings.TrimSpace(target.postgres.RuntimeID) != strings.TrimSpace(postgres.RuntimeID) {
			service.CurrentRuntimeStartedAt = nil
			service.CurrentRuntimeReadyAt = nil
		}
		service.Spec.Postgres = &postgres
		service.UpdatedAt = now
		if err := s.pgUpdateBackingServiceTx(ctx, tx, service); err != nil {
			return err
		}
		replaceHydratedPlacementService(app, service)
	} else {
		app.Spec.Postgres = &postgres
	}
	app.UpdatedAt = now
	return nil
}

func (s *Store) pgSyncObservedManagedPostgresPlacement(
	mutation ManagedPostgresPlacementMutation,
) (model.App, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.App{}, fmt.Errorf("begin sync managed postgres placement transaction: %w", err)
	}
	defer tx.Rollback()

	app, err := s.pgGetAppTx(ctx, tx, mutation.Witness.AppID, true)
	if err != nil {
		return model.App{}, mapDBErr(err)
	}
	if isDeletedApp(app) || strings.TrimSpace(app.TenantID) != mutation.Witness.TenantID ||
		strings.TrimSpace(app.ProjectID) != mutation.Witness.ProjectID {
		return model.App{}, ErrNotFound
	}
	activeCount, err := s.pgCountActiveOperationsForManagedPostgresPlacementTargetTx(
		ctx, tx, mutation.Witness.AppID, mutation.Witness.ServiceID,
	)
	if err != nil {
		return model.App{}, err
	}
	if activeCount != 0 {
		return model.App{}, ErrConflict
	}
	target, err := s.pgLockedManagedPostgresPlacementTargetTx(ctx, tx, &app, mutation.Witness)
	if err != nil {
		return model.App{}, err
	}
	if !managedPostgresPlacementStateEqual(
		ManagedPostgresPlacementStateFromSpec(target.postgres), mutation.Expected,
	) {
		return model.App{}, ErrConflict
	}
	consumeFailover, err := managedPostgresIdlePlacementConsumesFailover(mutation.Expected, mutation.Desired)
	if err != nil {
		return model.App{}, err
	}
	if consumeFailover {
		sourceRuntime, err := s.pgGetRuntimeTx(ctx, tx, mutation.Expected.RuntimeID, true)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return model.App{}, err
		}
		if err == nil && !strings.EqualFold(strings.TrimSpace(sourceRuntime.Status), model.RuntimeStatusOffline) {
			return model.App{}, ErrConflict
		}
	}

	if err := s.pgApplyManagedPostgresPlacementStateTx(ctx, tx, &app, target, mutation.Desired, time.Now().UTC()); err != nil {
		return model.App{}, err
	}
	if err := s.pgUpdateAppTx(ctx, tx, app); err != nil {
		return model.App{}, mapDBErr(err)
	}
	if err := tx.Commit(); err != nil {
		return model.App{}, fmt.Errorf("commit sync managed postgres placement transaction: %w", err)
	}
	normalizeAppStatusForRead(&app)
	if err := s.pgHydrateAppBackingServices(context.Background(), &app); err != nil {
		return model.App{}, err
	}
	return app, nil
}

func (s *Store) pgCompleteManagedPostgresSwitchoverWithPlacement(
	id, manifestPath, message string,
	mutation ManagedPostgresPlacementMutation,
) (model.Operation, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Operation{}, fmt.Errorf("begin complete managed postgres switchover placement transaction: %w", err)
	}
	defer tx.Rollback()

	op, err := s.pgGetOperationTx(ctx, tx, id, true)
	if err != nil {
		return model.Operation{}, mapDBErr(err)
	}
	if err := validateManagedPostgresSwitchoverPlacementOperation(op, mutation); err != nil {
		return model.Operation{}, err
	}
	app, err := s.pgGetAppTx(ctx, tx, op.AppID, true)
	if err != nil {
		return model.Operation{}, mapDBErr(err)
	}
	if isDeletedApp(app) || strings.TrimSpace(app.TenantID) != mutation.Witness.TenantID ||
		strings.TrimSpace(app.ProjectID) != mutation.Witness.ProjectID {
		return model.Operation{}, ErrNotFound
	}
	activeCount, err := s.pgCountActiveOperationsForManagedPostgresPlacementTargetTx(
		ctx, tx, mutation.Witness.AppID, mutation.Witness.ServiceID,
	)
	if err != nil {
		return model.Operation{}, err
	}
	if activeCount != 1 {
		return model.Operation{}, ErrConflict
	}
	target, err := s.pgLockedManagedPostgresPlacementTargetTx(ctx, tx, &app, mutation.Witness)
	if err != nil {
		return model.Operation{}, err
	}
	if !managedPostgresPlacementStateEqual(
		ManagedPostgresPlacementStateFromSpec(target.postgres), mutation.Expected,
	) {
		return model.Operation{}, ErrConflict
	}

	now := time.Now().UTC()
	if err := s.pgApplyManagedPostgresPlacementStateTx(ctx, tx, &app, target, mutation.Desired, now); err != nil {
		return model.Operation{}, err
	}
	if err := completeManagedPostgresSwitchoverOperationModel(&op, &app, manifestPath, message, now); err != nil {
		return model.Operation{}, err
	}
	if err := s.pgUpdateOperationTx(ctx, tx, op); err != nil {
		return model.Operation{}, err
	}
	if err := s.pgUpdateAppTx(ctx, tx, app); err != nil {
		return model.Operation{}, err
	}
	if err := s.pgUpdateAppImageTrackingDeployedTx(ctx, tx, op, now); err != nil {
		return model.Operation{}, err
	}
	if err := s.pgSyncStableReleaseForCompletedDeployTx(ctx, tx, app, op, now); err != nil {
		return model.Operation{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.Operation{}, fmt.Errorf("commit complete managed postgres switchover placement transaction: %w", err)
	}
	return op, nil
}
