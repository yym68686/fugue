package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) pgValidateManagedPostgresResizeTargetTx(
	ctx context.Context,
	tx *sql.Tx,
	app *model.App,
	serviceID string,
) (model.BackingService, error) {
	if app == nil {
		return model.BackingService{}, ErrInvalidInput
	}
	serviceID = strings.TrimSpace(serviceID)
	if serviceID == "" {
		return model.BackingService{}, ErrInvalidInput
	}
	service, err := s.pgGetBackingServiceTx(ctx, tx, serviceID, true)
	if err != nil {
		return model.BackingService{}, mapDBErr(err)
	}
	if service.TenantID != app.TenantID || service.ProjectID != app.ProjectID ||
		!isManagedPostgresService(service) || service.Spec.Postgres == nil {
		return model.BackingService{}, ErrInvalidInput
	}
	if strings.TrimSpace(service.OwnerAppID) != strings.TrimSpace(app.ID) {
		return model.BackingService{}, ErrConflict
	}
	if service.Spec.Postgres.Suspended {
		return model.BackingService{}, ErrConflict
	}

	rows, err := tx.QueryContext(ctx, `
SELECT tenant_id, app_id
FROM fugue_service_bindings
WHERE service_id = $1
FOR UPDATE
`, serviceID)
	if err != nil {
		return model.BackingService{}, fmt.Errorf("list resize target bindings: %w", err)
	}
	defer rows.Close()
	bindingCount := 0
	for rows.Next() {
		var tenantID, appID string
		if err := rows.Scan(&tenantID, &appID); err != nil {
			return model.BackingService{}, fmt.Errorf("scan resize target binding: %w", err)
		}
		bindingCount++
		if tenantID != app.TenantID || strings.TrimSpace(appID) != strings.TrimSpace(app.ID) {
			return model.BackingService{}, ErrConflict
		}
	}
	if err := rows.Err(); err != nil {
		return model.BackingService{}, fmt.Errorf("iterate resize target bindings: %w", err)
	}
	if bindingCount != 1 {
		return model.BackingService{}, ErrConflict
	}
	return service, nil
}

func (s *Store) pgApplyManagedPostgresResizeTx(
	ctx context.Context,
	tx *sql.Tx,
	app *model.App,
	op *model.Operation,
) error {
	if app == nil || op == nil || op.Type != model.OperationTypeDatabaseResize ||
		op.DesiredSpec == nil || op.DesiredSpec.Postgres == nil {
		return ErrInvalidInput
	}
	targetResources, err := normalizeManagedPostgresResizeTarget(op.DesiredSpec.Postgres.RuntimeResources)
	if err != nil {
		return err
	}
	service, err := s.pgValidateManagedPostgresResizeTargetTx(ctx, tx, app, op.ServiceID)
	if err != nil {
		return err
	}
	postgres := model.CloneAppPostgresSpec(service.Spec.Postgres)
	postgres.RuntimeResources = model.CloneResourceSpec(targetResources)
	service.Spec.Postgres = postgres
	service.UpdatedAt = time.Now().UTC()
	return s.pgUpdateBackingServiceTx(ctx, tx, service)
}

func (s *Store) pgHasActiveAppDatabaseRestoreRunForManagedPostgresTx(
	ctx context.Context,
	tx *sql.Tx,
	app model.App,
) (bool, error) {
	var active bool
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS (
	SELECT 1
	FROM fugue_backup_restore_runs
	WHERE app_id = $1
	  AND status IN ($2, $3)
)
`, strings.TrimSpace(app.ID), model.BackupRestoreStatusPlanned, model.BackupRestoreStatusRunning).Scan(&active); err != nil {
		return false, fmt.Errorf("check active app database restore for managed postgres: %w", err)
	}
	return active, nil
}
