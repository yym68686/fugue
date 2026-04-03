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

func (s *Store) pgGetTenantBillingSummary(tenantID string) (model.TenantBillingSummary, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.TenantBillingSummary{}, fmt.Errorf("begin billing summary transaction: %w", err)
	}
	defer tx.Rollback()

	exists, err := s.pgTenantExistsTx(ctx, tx, tenantID)
	if err != nil {
		return model.TenantBillingSummary{}, err
	}
	if !exists {
		return model.TenantBillingSummary{}, ErrNotFound
	}

	now := time.Now().UTC()
	record, err := s.pgEnsureTenantBillingRecordTx(ctx, tx, tenantID, true, now)
	if err != nil {
		return model.TenantBillingSummary{}, err
	}
	accrueTenantBilling(&record, now)
	if err := s.pgUpdateTenantBillingRecordTx(ctx, tx, record); err != nil {
		return model.TenantBillingSummary{}, err
	}

	state, err := s.pgLoadTenantBillingStateTx(ctx, tx, tenantID)
	if err != nil {
		return model.TenantBillingSummary{}, err
	}
	summary := buildTenantBillingSummary(&state, record)

	if err := tx.Commit(); err != nil {
		return model.TenantBillingSummary{}, fmt.Errorf("commit billing summary transaction: %w", err)
	}
	return summary, nil
}

func (s *Store) pgUpdateTenantBilling(tenantID string, managedCap model.BillingResourceSpec) (model.TenantBillingSummary, error) {
	normalizedCap, err := normalizeBillingCap(managedCap)
	if err != nil {
		return model.TenantBillingSummary{}, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.TenantBillingSummary{}, fmt.Errorf("begin billing update transaction: %w", err)
	}
	defer tx.Rollback()

	exists, err := s.pgTenantExistsTx(ctx, tx, tenantID)
	if err != nil {
		return model.TenantBillingSummary{}, err
	}
	if !exists {
		return model.TenantBillingSummary{}, ErrNotFound
	}

	now := time.Now().UTC()
	record, err := s.pgEnsureTenantBillingRecordTx(ctx, tx, tenantID, true, now)
	if err != nil {
		return model.TenantBillingSummary{}, err
	}
	accrueTenantBilling(&record, now)
	record.ManagedCap = normalizedCap
	record.UpdatedAt = now
	if err := s.pgUpdateTenantBillingRecordTx(ctx, tx, record); err != nil {
		return model.TenantBillingSummary{}, err
	}
	if err := s.pgInsertTenantBillingEventTx(ctx, tx, newTenantBillingConfigUpdatedEvent(
		tenantID,
		normalizedCap,
		record.BalanceMicroCents,
		now,
		nil,
	)); err != nil {
		return model.TenantBillingSummary{}, err
	}

	state, err := s.pgLoadTenantBillingStateTx(ctx, tx, tenantID)
	if err != nil {
		return model.TenantBillingSummary{}, err
	}
	summary := buildTenantBillingSummary(&state, record)

	if err := tx.Commit(); err != nil {
		return model.TenantBillingSummary{}, fmt.Errorf("commit billing update transaction: %w", err)
	}
	return summary, nil
}

func (s *Store) pgTopUpTenantBilling(tenantID string, amountCents int64, note string) (model.TenantBillingSummary, error) {
	if amountCents <= 0 {
		return model.TenantBillingSummary{}, ErrInvalidInput
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.TenantBillingSummary{}, fmt.Errorf("begin billing top-up transaction: %w", err)
	}
	defer tx.Rollback()

	exists, err := s.pgTenantExistsTx(ctx, tx, tenantID)
	if err != nil {
		return model.TenantBillingSummary{}, err
	}
	if !exists {
		return model.TenantBillingSummary{}, ErrNotFound
	}

	now := time.Now().UTC()
	record, err := s.pgEnsureTenantBillingRecordTx(ctx, tx, tenantID, true, now)
	if err != nil {
		return model.TenantBillingSummary{}, err
	}
	accrueTenantBilling(&record, now)
	record.BalanceMicroCents += amountCents * microCentsPerCent
	record.UpdatedAt = now
	if err := s.pgUpdateTenantBillingRecordTx(ctx, tx, record); err != nil {
		return model.TenantBillingSummary{}, err
	}

	metadata := map[string]string{}
	if note = strings.TrimSpace(note); note != "" {
		metadata["note"] = note
	}
	if err := s.pgInsertTenantBillingEventTx(ctx, tx, model.TenantBillingEvent{
		ID:                     model.NewID("billingevt"),
		TenantID:               tenantID,
		Type:                   model.BillingEventTypeTopUp,
		AmountMicroCents:       amountCents * microCentsPerCent,
		BalanceAfterMicroCents: record.BalanceMicroCents,
		Metadata:               metadata,
		CreatedAt:              now,
	}); err != nil {
		return model.TenantBillingSummary{}, err
	}

	state, err := s.pgLoadTenantBillingStateTx(ctx, tx, tenantID)
	if err != nil {
		return model.TenantBillingSummary{}, err
	}
	summary := buildTenantBillingSummary(&state, record)

	if err := tx.Commit(); err != nil {
		return model.TenantBillingSummary{}, fmt.Errorf("commit billing top-up transaction: %w", err)
	}
	return summary, nil
}

func (s *Store) pgSetTenantBillingBalance(tenantID string, balanceCents int64, metadata map[string]string) (model.TenantBillingSummary, error) {
	if balanceCents < 0 {
		return model.TenantBillingSummary{}, ErrInvalidInput
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.TenantBillingSummary{}, fmt.Errorf("begin billing balance transaction: %w", err)
	}
	defer tx.Rollback()

	exists, err := s.pgTenantExistsTx(ctx, tx, tenantID)
	if err != nil {
		return model.TenantBillingSummary{}, err
	}
	if !exists {
		return model.TenantBillingSummary{}, ErrNotFound
	}

	now := time.Now().UTC()
	record, err := s.pgEnsureTenantBillingRecordTx(ctx, tx, tenantID, true, now)
	if err != nil {
		return model.TenantBillingSummary{}, err
	}
	accrueTenantBilling(&record, now)
	previousBalanceMicroCents := record.BalanceMicroCents
	targetBalanceMicroCents := balanceCents * microCentsPerCent
	if previousBalanceMicroCents != targetBalanceMicroCents {
		record.BalanceMicroCents = targetBalanceMicroCents
		record.UpdatedAt = now
	}
	if err := s.pgUpdateTenantBillingRecordTx(ctx, tx, record); err != nil {
		return model.TenantBillingSummary{}, err
	}
	if previousBalanceMicroCents != targetBalanceMicroCents {
		if err := s.pgInsertTenantBillingEventTx(ctx, tx, newTenantBillingBalanceAdjustedEvent(
			tenantID,
			targetBalanceMicroCents-previousBalanceMicroCents,
			record.BalanceMicroCents,
			now,
			metadata,
		)); err != nil {
			return model.TenantBillingSummary{}, err
		}
	}

	state, err := s.pgLoadTenantBillingStateTx(ctx, tx, tenantID)
	if err != nil {
		return model.TenantBillingSummary{}, err
	}
	summary := buildTenantBillingSummary(&state, record)

	if err := tx.Commit(); err != nil {
		return model.TenantBillingSummary{}, fmt.Errorf("commit billing balance transaction: %w", err)
	}
	return summary, nil
}

func (s *Store) ensureTenantBillingRecordsTx(ctx context.Context, tx *sql.Tx) error {
	now := time.Now().UTC()
	defaultRecord := defaultTenantBilling("", now)
	managedCapJSON, err := marshalJSON(defaultRecord.ManagedCap)
	if err != nil {
		return err
	}
	priceBookJSON, err := marshalJSON(defaultRecord.PriceBook)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_tenant_billing (tenant_id, managed_cap_json, balance_microcents, price_book_json, last_accrued_at, created_at, updated_at)
SELECT t.id, $1, $2, $3, $4, $4, $4
FROM fugue_tenants AS t
LEFT JOIN fugue_tenant_billing AS b
	ON b.tenant_id = t.id
WHERE b.tenant_id IS NULL
`, managedCapJSON, defaultRecord.BalanceMicroCents, priceBookJSON, now); err != nil {
		return fmt.Errorf("ensure tenant billing defaults: %w", err)
	}
	return nil
}

func (s *Store) pgLoadTenantBillingStateTx(ctx context.Context, tx *sql.Tx, tenantID string) (model.State, error) {
	appRows, err := tx.QueryContext(ctx, `
SELECT id, tenant_id, project_id, name, description, source_json, route_json, spec_json, status_json, created_at, updated_at
FROM fugue_apps
WHERE tenant_id = $1
ORDER BY created_at ASC
`, tenantID)
	if err != nil {
		return model.State{}, fmt.Errorf("list billing apps: %w", err)
	}
	defer appRows.Close()

	apps := make([]model.App, 0)
	for appRows.Next() {
		app, err := scanApp(appRows)
		if err != nil {
			return model.State{}, err
		}
		apps = append(apps, app)
	}
	if err := appRows.Err(); err != nil {
		return model.State{}, fmt.Errorf("iterate billing apps: %w", err)
	}

	serviceRows, err := tx.QueryContext(ctx, `
SELECT id, tenant_id, project_id, owner_app_id, name, description, type, provisioner, status, spec_json, current_runtime_started_at, current_runtime_ready_at, created_at, updated_at
FROM fugue_backing_services
WHERE tenant_id = $1
ORDER BY created_at ASC
`, tenantID)
	if err != nil {
		return model.State{}, fmt.Errorf("list billing services: %w", err)
	}
	defer serviceRows.Close()

	services := make([]model.BackingService, 0)
	for serviceRows.Next() {
		service, err := scanBackingService(serviceRows)
		if err != nil {
			return model.State{}, err
		}
		services = append(services, service)
	}
	if err := serviceRows.Err(); err != nil {
		return model.State{}, fmt.Errorf("iterate billing services: %w", err)
	}

	bindingRows, err := tx.QueryContext(ctx, `
SELECT id, tenant_id, app_id, service_id, alias, env_json, created_at, updated_at
FROM fugue_service_bindings
WHERE tenant_id = $1
ORDER BY created_at ASC
`, tenantID)
	if err != nil {
		return model.State{}, fmt.Errorf("list billing bindings: %w", err)
	}
	defer bindingRows.Close()

	bindings := make([]model.ServiceBinding, 0)
	for bindingRows.Next() {
		binding, err := scanServiceBinding(bindingRows)
		if err != nil {
			return model.State{}, err
		}
		bindings = append(bindings, binding)
	}
	if err := bindingRows.Err(); err != nil {
		return model.State{}, fmt.Errorf("iterate billing bindings: %w", err)
	}

	runtimeRows, err := tx.QueryContext(ctx, `
SELECT id, tenant_id, name, machine_name, type, access_mode, pool_mode, connection_mode, status, endpoint, labels_json, node_key_id, cluster_node_name, fingerprint_prefix, fingerprint_hash, agent_key_prefix, agent_key_hash, last_seen_at, last_heartbeat_at, created_at, updated_at
FROM fugue_runtimes
ORDER BY created_at ASC
`)
	if err != nil {
		return model.State{}, fmt.Errorf("list billing runtimes: %w", err)
	}
	defer runtimeRows.Close()

	runtimes := make([]model.Runtime, 0)
	for runtimeRows.Next() {
		runtime, err := scanRuntime(runtimeRows)
		if err != nil {
			return model.State{}, err
		}
		runtimes = append(runtimes, runtime)
	}
	if err := runtimeRows.Err(); err != nil {
		return model.State{}, fmt.Errorf("iterate billing runtimes: %w", err)
	}

	eventRows, err := tx.QueryContext(ctx, `
SELECT id, tenant_id, type, amount_microcents, balance_after_microcents, metadata_json, created_at
FROM fugue_billing_events
WHERE tenant_id = $1
ORDER BY created_at DESC, id DESC
LIMIT $2
`, tenantID, billingHistoryLimit)
	if err != nil {
		return model.State{}, fmt.Errorf("list billing events: %w", err)
	}
	defer eventRows.Close()

	events := make([]model.TenantBillingEvent, 0)
	for eventRows.Next() {
		event, err := scanTenantBillingEvent(eventRows)
		if err != nil {
			return model.State{}, err
		}
		events = append(events, event)
	}
	if err := eventRows.Err(); err != nil {
		return model.State{}, fmt.Errorf("iterate billing events: %w", err)
	}

	return model.State{
		Apps:            apps,
		BackingServices: services,
		ServiceBindings: bindings,
		Runtimes:        runtimes,
		BillingEvents:   events,
	}, nil
}

func (s *Store) pgEnsureTenantBillingRecordTx(ctx context.Context, tx *sql.Tx, tenantID string, forUpdate bool, now time.Time) (model.TenantBilling, error) {
	record, err := s.pgGetTenantBillingRecordTx(ctx, tx, tenantID, forUpdate)
	if err == nil {
		normalizeTenantBillingRecord(&record, now)
		changed := false
		if shouldRecalibrateTenantBillingPriceBook(record) {
			recalibrateTenantBillingPriceBook(&record, now)
			changed = true
		}
		if shouldBackfillLegacyTenantBillingRecord(record) {
			hasEvents, err := s.pgTenantHasBillingEventsTx(ctx, tx, tenantID)
			if err != nil {
				return model.TenantBilling{}, err
			}
			if !hasEvents {
				backfillLegacyTenantBillingRecord(&record, now)
				changed = true
			}
		}
		if changed {
			if err := s.pgUpdateTenantBillingRecordTx(ctx, tx, record); err != nil {
				return model.TenantBilling{}, err
			}
		}
		return record, nil
	}
	if !errors.Is(mapDBErr(err), ErrNotFound) {
		return model.TenantBilling{}, mapDBErr(err)
	}

	record = defaultTenantBilling(tenantID, now)
	if err := s.pgInsertTenantBillingRecordTx(ctx, tx, record); err != nil {
		return model.TenantBilling{}, err
	}
	if forUpdate {
		record, err = s.pgGetTenantBillingRecordTx(ctx, tx, tenantID, true)
		if err != nil {
			return model.TenantBilling{}, mapDBErr(err)
		}
	}
	normalizeTenantBillingRecord(&record, now)
	return record, nil
}

func (s *Store) pgGetTenantBillingRecordTx(ctx context.Context, tx *sql.Tx, tenantID string, forUpdate bool) (model.TenantBilling, error) {
	query := `
SELECT tenant_id, managed_cap_json, balance_microcents, price_book_json, last_accrued_at, created_at, updated_at
FROM fugue_tenant_billing
WHERE tenant_id = $1
`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	record, err := scanTenantBilling(tx.QueryRowContext(ctx, query, tenantID))
	if err != nil {
		return model.TenantBilling{}, err
	}
	return record, nil
}

func (s *Store) pgTenantHasBillingEventsTx(ctx context.Context, tx *sql.Tx, tenantID string) (bool, error) {
	var exists bool
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS (
	SELECT 1
	FROM fugue_billing_events
	WHERE tenant_id = $1
)
`, tenantID).Scan(&exists); err != nil {
		return false, fmt.Errorf("check tenant billing events %s: %w", tenantID, err)
	}
	return exists, nil
}

func (s *Store) pgInsertTenantBillingRecordTx(ctx context.Context, tx *sql.Tx, record model.TenantBilling) error {
	managedCapJSON, err := marshalJSON(record.ManagedCap)
	if err != nil {
		return err
	}
	priceBookJSON, err := marshalJSON(record.PriceBook)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_tenant_billing (tenant_id, managed_cap_json, balance_microcents, price_book_json, last_accrued_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (tenant_id) DO NOTHING
`, record.TenantID, managedCapJSON, record.BalanceMicroCents, priceBookJSON, record.LastAccruedAt, record.CreatedAt, record.UpdatedAt); err != nil {
		return fmt.Errorf("insert tenant billing %s: %w", record.TenantID, err)
	}
	return nil
}

func (s *Store) pgUpdateTenantBillingRecordTx(ctx context.Context, tx *sql.Tx, record model.TenantBilling) error {
	managedCapJSON, err := marshalJSON(record.ManagedCap)
	if err != nil {
		return err
	}
	priceBookJSON, err := marshalJSON(record.PriceBook)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE fugue_tenant_billing
SET managed_cap_json = $2,
	balance_microcents = $3,
	price_book_json = $4,
	last_accrued_at = $5,
	created_at = $6,
	updated_at = $7
WHERE tenant_id = $1
`, record.TenantID, managedCapJSON, record.BalanceMicroCents, priceBookJSON, record.LastAccruedAt, record.CreatedAt, record.UpdatedAt); err != nil {
		return fmt.Errorf("update tenant billing %s: %w", record.TenantID, err)
	}
	return nil
}

func (s *Store) pgInsertTenantBillingEventTx(ctx context.Context, tx *sql.Tx, event model.TenantBillingEvent) error {
	metadataJSON, err := marshalNullableJSON(event.Metadata)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO fugue_billing_events (id, tenant_id, type, amount_microcents, balance_after_microcents, metadata_json, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
`, event.ID, event.TenantID, event.Type, event.AmountMicroCents, event.BalanceAfterMicroCents, metadataJSON, event.CreatedAt); err != nil {
		return fmt.Errorf("insert tenant billing event %s: %w", event.ID, err)
	}
	return nil
}

func scanTenantBilling(scanner sqlScanner) (model.TenantBilling, error) {
	var record model.TenantBilling
	var managedCapRaw []byte
	var priceBookRaw []byte
	if err := scanner.Scan(&record.TenantID, &managedCapRaw, &record.BalanceMicroCents, &priceBookRaw, &record.LastAccruedAt, &record.CreatedAt, &record.UpdatedAt); err != nil {
		return model.TenantBilling{}, err
	}
	managedCap, err := decodeJSONValue[model.BillingResourceSpec](managedCapRaw)
	if err != nil {
		return model.TenantBilling{}, err
	}
	priceBook, err := decodeJSONValue[model.BillingPriceBook](priceBookRaw)
	if err != nil {
		return model.TenantBilling{}, err
	}
	record.ManagedCap = managedCap
	record.PriceBook = priceBook
	return record, nil
}

func scanTenantBillingEvent(scanner sqlScanner) (model.TenantBillingEvent, error) {
	var event model.TenantBillingEvent
	var metadataRaw []byte
	if err := scanner.Scan(&event.ID, &event.TenantID, &event.Type, &event.AmountMicroCents, &event.BalanceAfterMicroCents, &metadataRaw, &event.CreatedAt); err != nil {
		return model.TenantBillingEvent{}, err
	}
	metadata, err := decodeJSONValue[map[string]string](metadataRaw)
	if err != nil {
		return model.TenantBillingEvent{}, err
	}
	event.Metadata = metadata
	return event, nil
}
