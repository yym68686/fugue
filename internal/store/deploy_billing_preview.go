package store

import (
	"context"
	"database/sql"
	"time"

	"fugue/internal/model"
)

// CheckDeployBilling previews the same resource and price projections used at
// deploy acceptance, without creating an operation or changing billing records.
// Acceptance still rechecks under its normal transaction after a long build.
func (s *Store) CheckDeployBilling(ctx context.Context, appID string, spec model.AppSpec) error {
	check := func(state *model.State) error {
		i := findApp(state, appID)
		if i < 0 || isDeletedApp(state.Apps[i]) {
			return ErrNotFound
		}
		app := state.Apps[i]
		op := model.Operation{AppID: appID, TenantID: app.TenantID, Type: model.OperationTypeDeploy, DesiredSpec: cloneAppSpec(&spec)}
		record := accrueTenantBillingLedger(state, app.TenantID, time.Now().UTC())
		if record == nil {
			return ErrNotFound
		}
		current, next, err := projectedTenantManagedTotalsWithBilling(state, app, op, *record)
		if err != nil {
			return err
		}
		currentPublic, nextPublic, err := projectedTenantPublicRuntimeHourlyRates(state, app, op)
		if err != nil {
			return err
		}
		return validateTenantOperationBilling(*record, current, next, currentPublic, nextPublic)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !s.usingDatabase() {
		return s.withLockedState(false, check)
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	app, err := s.pgGetAppTx(ctx, tx, appID, false)
	if err != nil {
		return mapDBErr(err)
	}
	state, err := s.pgLoadTenantBillingStateTx(ctx, tx, app.TenantID)
	if err != nil {
		return err
	}
	record, err := s.pgGetTenantBillingRecordTx(ctx, tx, app.TenantID, false)
	if err != nil {
		return err
	}
	state.TenantBilling = append(state.TenantBilling, record)
	return check(&state)
}
